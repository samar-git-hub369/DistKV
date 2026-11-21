#include "network/tcp_server.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include <boost/asio.hpp>

#include "server/command_handler.h"
#include "server/kv_backend.h"
#include "storage/storage_engine.h"

namespace distkv {
namespace {

namespace asio = boost::asio;
namespace fs = std::filesystem;
using asio::ip::tcp;

// Blocking client speaking the text protocol.
class Client {
  public:
    explicit Client(uint16_t port) : socket_(io_) {
        socket_.connect(tcp::endpoint(asio::ip::make_address("127.0.0.1"), port));
    }

    void Send(const std::string& bytes) { asio::write(socket_, asio::buffer(bytes)); }

    // Reads one response line without its newline. Returns "<closed>" on EOF.
    std::string ReadLine() {
        boost::system::error_code ec;
        const size_t n = asio::read_until(socket_, buffer_, '\n', ec);
        if (ec) return "<closed>";
        const auto begin = asio::buffers_begin(buffer_.data());
        std::string line(begin, begin + static_cast<std::ptrdiff_t>(n - 1));
        buffer_.consume(n);
        return line;
    }

    void Close() {
        boost::system::error_code ignored;
        socket_.close(ignored);
    }

    std::string Call(const std::string& request) {
        Send(request + "\n");
        return ReadLine();
    }

  private:
    asio::io_context io_;
    tcp::socket socket_;
    asio::streambuf buffer_;
};

// Parameterised on the number of io threads.
class TcpServerTest : public ::testing::TestWithParam<size_t> {
  protected:
    void SetUp() override {
        std::random_device rd;
        dir_ = (fs::temp_directory_path() / ("distkv_server_test_" + std::to_string(rd()))).string();
        EngineOptions eo;
        eo.wal_sync = WalSyncMode::kNone;
        eo.memtable_bytes = 64 * 1024;
        ASSERT_TRUE(StorageEngine::Open(dir_, eo, &engine_).ok());
        backend_ = std::make_unique<StandaloneBackend>(engine_.get());
        handler_ = std::make_unique<CommandHandler>(backend_.get());
    }

    void TearDown() override {
        server_.reset();
        engine_.reset();
        std::error_code ec;
        fs::remove_all(dir_, ec);
    }

    void StartServer(size_t max_connections = 1024) {
        ServerOptions so;
        so.host = "127.0.0.1";
        so.port = 0;
        so.threads = GetParam();
        so.max_connections = max_connections;
        server_ = std::make_unique<TcpServer>(so, handler_.get());
        ASSERT_TRUE(server_->Start().ok());
        ASSERT_NE(server_->port(), 0);
    }

    // Waits for a condition that the server reaches asynchronously.
    template <typename Pred>
    bool Eventually(Pred pred) {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while (!pred()) {
            if (std::chrono::steady_clock::now() > deadline) return false;
            std::this_thread::sleep_for(std::chrono::milliseconds(2));
        }
        return true;
    }

    std::string dir_;
    std::unique_ptr<StorageEngine> engine_;
    std::unique_ptr<StandaloneBackend> backend_;
    std::unique_ptr<CommandHandler> handler_;
    std::unique_ptr<TcpServer> server_;
};

TEST_P(TcpServerTest, RoundTripsEveryCommand) {
    StartServer();
    Client c(server_->port());
    EXPECT_EQ(c.Call("PING"), "PONG");
    EXPECT_EQ(c.Call("PUT city Jaipur, Rajasthan"), "OK");
    EXPECT_EQ(c.Call("GET city"), "VALUE Jaipur, Rajasthan");
    EXPECT_EQ(c.Call("DELETE city"), "OK");
    EXPECT_EQ(c.Call("GET city"), "NOT_FOUND");
    EXPECT_EQ(c.Call("GET never"), "NOT_FOUND");
    EXPECT_EQ(c.Call("NOPE"), "ERR unknown command 'NOPE'");
    EXPECT_EQ(c.Call("GET"), "ERR missing key");

    const std::string status = c.Call("STATUS");
    EXPECT_EQ(status.rfind("STATUS role=standalone last_seq=2 ", 0), 0u) << status;
}

TEST_P(TcpServerTest, PipelinedRequestsAnsweredInOrder) {
    StartServer();
    Client c(server_->port());
    std::string batch;
    for (int i = 0; i < 200; ++i) {
        batch += "PUT k" + std::to_string(i) + " v" + std::to_string(i) + "\r\n";
        batch += "GET k" + std::to_string(i) + "\n";
    }
    c.Send(batch);  // one write, many requests
    for (int i = 0; i < 200; ++i) {
        ASSERT_EQ(c.ReadLine(), "OK") << i;
        ASSERT_EQ(c.ReadLine(), "VALUE v" + std::to_string(i)) << i;
    }
}

TEST_P(TcpServerTest, RequestSplitAcrossWritesIsReassembled) {
    StartServer();
    Client c(server_->port());
    c.Send("PU");
    c.Send("T sp");
    c.Send("lit val");
    c.Send("ue\nGET split\n");
    EXPECT_EQ(c.ReadLine(), "OK");
    EXPECT_EQ(c.ReadLine(), "VALUE value");
}

TEST_P(TcpServerTest, ManyConcurrentClients) {
    StartServer();
    constexpr int kClients = 16;
    constexpr int kOps = 200;
    std::atomic<int> bad{0};
    std::vector<std::thread> threads;
    for (int t = 0; t < kClients; ++t) {
        threads.emplace_back([&, t] {
            Client c(server_->port());
            for (int i = 0; i < kOps; ++i) {
                const std::string key = "c" + std::to_string(t) + ":" + std::to_string(i);
                if (c.Call("PUT " + key + " " + std::to_string(i)) != "OK") ++bad;
                if (c.Call("GET " + key) != "VALUE " + std::to_string(i)) ++bad;
            }
        });
    }
    for (auto& th : threads) th.join();
    EXPECT_EQ(bad.load(), 0);
    EXPECT_EQ(server_->accepted_connections(), static_cast<uint64_t>(kClients));
    EXPECT_EQ(engine_->stats().last_seq, static_cast<uint64_t>(kClients * kOps));
}

TEST_P(TcpServerTest, OverlongLineGetsErrorAndDisconnect) {
    StartServer();
    Client c(server_->port());
    EXPECT_EQ(c.Call("PUT before ok"), "OK");
    c.Send("PUT big " + std::string(kMaxLineBytes + 10, 'x') + "\n");
    EXPECT_EQ(c.ReadLine(), "ERR request line too long");
    EXPECT_EQ(c.ReadLine(), "<closed>");

    // The server is unaffected for other clients.
    Client other(server_->port());
    EXPECT_EQ(other.Call("GET before"), "VALUE ok");
}

TEST_P(TcpServerTest, ConnectionLimitRejectsExtraClients) {
    StartServer(/*max_connections=*/2);
    Client a(server_->port());
    Client b(server_->port());
    EXPECT_EQ(a.Call("PING"), "PONG");
    EXPECT_EQ(b.Call("PING"), "PONG");

    Client extra(server_->port());
    EXPECT_EQ(extra.ReadLine(), "ERR too many connections");
    EXPECT_EQ(extra.ReadLine(), "<closed>");
    EXPECT_EQ(server_->rejected_connections(), 1u);

    // Closing a client frees a slot.
    a.Close();
    ASSERT_TRUE(Eventually([&] { return server_->active_connections() == 1; }));
    Client again(server_->port());
    EXPECT_EQ(again.Call("PING"), "PONG");
}

TEST_P(TcpServerTest, ClientDisconnectReleasesSession) {
    StartServer();
    {
        Client c(server_->port());
        EXPECT_EQ(c.Call("PING"), "PONG");
        EXPECT_EQ(server_->active_connections(), 1u);
    }
    EXPECT_TRUE(Eventually([&] { return server_->active_connections() == 0; }));
}

TEST_P(TcpServerTest, StopClosesIdleAndBusyClients) {
    StartServer();
    Client idle(server_->port());
    Client busy(server_->port());
    EXPECT_EQ(idle.Call("PING"), "PONG");

    std::atomic<bool> keep_going{true};
    std::thread load([&] {
        while (keep_going.load()) {
            if (busy.Call("PUT k v") != "OK") break;  // stops once the server closes
        }
    });
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    server_->Stop();  // must return even with a client mid-request
    EXPECT_EQ(idle.ReadLine(), "<closed>");
    keep_going = false;
    load.join();
    EXPECT_EQ(server_->active_connections(), 0u);
    server_->Stop();  // idempotent
}

TEST_P(TcpServerTest, DataWrittenOverTcpSurvivesRestart) {
    StartServer();
    {
        Client c(server_->port());
        for (int i = 0; i < 50; ++i) {
            ASSERT_EQ(c.Call("PUT k" + std::to_string(i) + " v" + std::to_string(i)), "OK");
        }
    }
    server_.reset();
    handler_.reset();
    backend_.reset();
    engine_.reset();

    EngineOptions eo;
    eo.wal_sync = WalSyncMode::kNone;
    ASSERT_TRUE(StorageEngine::Open(dir_, eo, &engine_).ok());
    backend_ = std::make_unique<StandaloneBackend>(engine_.get());
    handler_ = std::make_unique<CommandHandler>(backend_.get());
    StartServer();
    Client c(server_->port());
    for (int i = 0; i < 50; ++i) {
        ASSERT_EQ(c.Call("GET k" + std::to_string(i)), "VALUE v" + std::to_string(i));
    }
}

TEST(TcpServerStartTest, RejectsBadHostAndBusyPort) {
    StandaloneBackend backend(nullptr);
    CommandHandler handler(&backend);

    ServerOptions bad;
    bad.host = "not-an-ip";
    bad.port = 0;
    TcpServer s1(bad, &handler);
    EXPECT_EQ(s1.Start().code(), StatusCode::kInvalidArgument);

    ServerOptions ok;
    ok.host = "127.0.0.1";
    ok.port = 0;
    TcpServer first(ok, &handler);
    ASSERT_TRUE(first.Start().ok());

    ServerOptions same = ok;
    same.port = first.port();
    TcpServer second(same, &handler);
    // reuse_address still forbids two listeners on one port.
    EXPECT_EQ(second.Start().code(), StatusCode::kIOError);
}

INSTANTIATE_TEST_SUITE_P(IoThreads, TcpServerTest, ::testing::Values(1, 4),
                         [](const auto& info) { return std::to_string(info.param) + "Threads"; });

}  // namespace
}  // namespace distkv

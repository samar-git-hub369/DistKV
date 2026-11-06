// End-to-end tests: a real Server on an ephemeral loopback port, driven by an
// io_context on its own thread, talking to blocking clients over TCP.

#include <gtest/gtest.h>
#include <sys/socket.h>
#include <sys/time.h>

#include <asio.hpp>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <future>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "network/server.hpp"
#include "protocol/resp.hpp"
#include "replication/local_replicator.hpp"
#include "server/command_dispatcher.hpp"
#include "server/kv_service.hpp"
#include "storage/kv_store.hpp"

namespace distkv {
namespace {

using namespace std::chrono_literals;
using Args = std::vector<std::string>;

constexpr auto kTimeout = 5s;

// Length of the complete RESP reply at the start of `buf`, if there is one.
std::optional<std::size_t> reply_length(std::string_view buf, std::size_t pos = 0) {
    if (pos >= buf.size()) return std::nullopt;
    const std::size_t eol = buf.find("\r\n", pos);
    if (eol == std::string_view::npos) return std::nullopt;
    const std::string_view header = buf.substr(pos + 1, eol - pos - 1);
    const std::size_t after_header = eol + 2;
    switch (buf[pos]) {
        case '+':
        case '-':
        case ':':
            return after_header - pos;
        case '$': {
            const long len = std::stol(std::string(header));
            if (len < 0) return after_header - pos;
            const std::size_t end = after_header + static_cast<std::size_t>(len) + 2;
            if (end > buf.size()) return std::nullopt;
            return end - pos;
        }
        case '*': {
            const long count = std::stol(std::string(header));
            std::size_t cursor = after_header;
            for (long i = 0; i < count; ++i) {
                const auto element = reply_length(buf, cursor);
                if (!element) return std::nullopt;
                cursor += *element;
            }
            return cursor - pos;
        }
        default:
            ADD_FAILURE() << "bad reply type byte: " << buf[pos];
            return std::nullopt;
    }
}

// Blocking test client. Reads time out so a server bug fails instead of hanging.
class Client {
public:
    explicit Client(std::uint16_t port) : socket_(io_) {
        socket_.connect({asio::ip::address_v4::loopback(), port});
        timeval tv{};
        tv.tv_sec = static_cast<decltype(tv.tv_sec)>(
            std::chrono::duration_cast<std::chrono::seconds>(kTimeout).count());
        ::setsockopt(socket_.native_handle(), SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    }

    void send(std::string_view bytes) { asio::write(socket_, asio::buffer(bytes)); }
    void send_command(const Args& args) { send(resp::command(args)); }

    // Returns the next complete reply, or "" on timeout/EOF (with a failure).
    std::string read_reply() {
        while (true) {
            if (const auto len = reply_length(buffer_)) {
                std::string reply = buffer_.substr(0, *len);
                buffer_.erase(0, *len);
                return reply;
            }
            if (!read_more()) {
                ADD_FAILURE() << "connection ended while waiting for a reply";
                return {};
            }
        }
    }

    std::string call(const Args& args) {
        send_command(args);
        return read_reply();
    }

    // True if the server closed the connection (EOF) before any more data.
    bool closed_by_server() {
        return buffer_.empty() && !read_more() && last_error_ == asio::error::eof;
    }

private:
    bool read_more() {
        char chunk[4096];
        const std::size_t n = socket_.read_some(asio::buffer(chunk), last_error_);
        if (last_error_) return false;
        buffer_.append(chunk, n);
        return true;
    }

    asio::io_context io_;
    asio::ip::tcp::socket socket_;
    std::string buffer_;
    asio::error_code last_error_;
};

// A Server and its io_context thread over the given replicator and store.
class Harness {
public:
    Harness(Replicator& replicator, KvStore& store, Session::Limits limits = {})
        : service_(replicator, store), dispatcher_(service_), server_(io_, dispatcher_, limits) {
        const std::error_code ec = server_.listen(asio::ip::address_v4::loopback(), 0);
        EXPECT_FALSE(ec) << ec.message();
        EXPECT_NE(server_.port(), 0);
        server_.start();
        thread_ = std::thread([this] {
            io_.run();
            stopped_.set_value();
        });
    }

    ~Harness() {
        if (!joined_) stop_and_wait();
    }

    std::uint16_t port() const { return server_.port(); }

    // Stops the server; returns whether io_context::run() returned in time.
    bool stop_and_wait() {
        server_.stop();
        const bool returned = stopped_future_.wait_for(kTimeout) == std::future_status::ready;
        if (!returned) io_.stop();  // Unblock the thread so the test can fail cleanly.
        thread_.join();
        joined_ = true;
        return returned;
    }

private:
    KvService service_;
    CommandDispatcher dispatcher_;
    asio::io_context io_;
    Server server_;
    std::promise<void> stopped_;
    std::future<void> stopped_future_ = stopped_.get_future();
    std::thread thread_;
    bool joined_{false};
};

class ServerIntegrationTest : public ::testing::Test {
protected:
    KvStore store_;
    LocalReplicator replicator_{store_};
    Harness harness_{replicator_, store_};
};

TEST_F(ServerIntegrationTest, SetGetDelRoundTrip) {
    Client client(harness_.port());
    EXPECT_EQ(client.call({"PING"}), "+PONG\r\n");
    EXPECT_EQ(client.call({"SET", "k", "v"}), "+OK\r\n");
    EXPECT_EQ(client.call({"GET", "k"}), "$1\r\nv\r\n");
    EXPECT_EQ(client.call({"DEL", "k"}), ":1\r\n");
    EXPECT_EQ(client.call({"GET", "k"}), "$-1\r\n");
}

TEST_F(ServerIntegrationTest, InlineCommands) {
    Client client(harness_.port());
    client.send("SET a b\r\nGET a\r\n");
    EXPECT_EQ(client.read_reply(), "+OK\r\n");
    EXPECT_EQ(client.read_reply(), "$1\r\nb\r\n");
}

TEST_F(ServerIntegrationTest, BinaryValueRoundTrip) {
    Client client(harness_.port());
    const std::string value("\0\r\n\xff", 4);
    EXPECT_EQ(client.call({"SET", "bin", value}), "+OK\r\n");
    EXPECT_EQ(client.call({"GET", "bin"}), "$4\r\n" + value + "\r\n");
}

TEST_F(ServerIntegrationTest, HundredPipelinedCommandsGetRepliesInOrder) {
    Client client(harness_.port());
    std::string batch;
    std::vector<std::string> expected;
    for (int i = 0; i < 50; ++i) {
        const std::string key = "key" + std::to_string(i);
        const std::string value = "value" + std::to_string(i);
        batch += resp::command({"SET", key, value});
        expected.push_back("+OK\r\n");
        batch += resp::command({"GET", key});
        expected.push_back(resp::bulk_string(value));
    }
    client.send(batch);  // One write.
    for (std::size_t i = 0; i < expected.size(); ++i) {
        EXPECT_EQ(client.read_reply(), expected[i]) << "reply " << i;
    }
}

TEST_F(ServerIntegrationTest, RequestSplitAcrossWrites) {
    Client client(harness_.port());
    client.send("*3\r\n$3\r\nSE");
    std::this_thread::sleep_for(20ms);
    client.send("T\r\n$1\r\nk\r\n$5\r\nhel");
    std::this_thread::sleep_for(20ms);
    client.send("lo\r\n");
    EXPECT_EQ(client.read_reply(), "+OK\r\n");
    EXPECT_EQ(client.call({"GET", "k"}), "$5\r\nhello\r\n");
}

TEST_F(ServerIntegrationTest, TwoClientsAtOnce) {
    Client a(harness_.port());
    Client b(harness_.port());
    a.send_command({"SET", "a", "1"});
    b.send_command({"SET", "b", "2"});
    EXPECT_EQ(b.read_reply(), "+OK\r\n");
    EXPECT_EQ(a.read_reply(), "+OK\r\n");
    EXPECT_EQ(a.call({"GET", "b"}), "$1\r\n2\r\n");
    EXPECT_EQ(b.call({"GET", "a"}), "$1\r\n1\r\n");
}

TEST_F(ServerIntegrationTest, ProtocolErrorRepliesThenCloses) {
    Client client(harness_.port());
    client.send("*1\r\n$x\r\n");
    EXPECT_EQ(client.read_reply(), "-ERR Protocol error: invalid bulk length 'x'\r\n");
    EXPECT_TRUE(client.closed_by_server());
}

TEST_F(ServerIntegrationTest, RequestsBeforeAProtocolErrorAreStillAnswered) {
    Client client(harness_.port());
    client.send("PING\r\nECHO hi\r\n+bad\r\nPING\r\n");
    EXPECT_EQ(client.read_reply(), "+PONG\r\n");
    EXPECT_EQ(client.read_reply(), "$2\r\nhi\r\n");
    EXPECT_EQ(client.read_reply().rfind("-ERR Protocol error: unexpected type byte", 0), 0U);
    EXPECT_TRUE(client.closed_by_server());
}

TEST_F(ServerIntegrationTest, HalfClosedClientStillGetsReplies) {
    asio::io_context io;
    asio::ip::tcp::socket raw(io);
    raw.connect({asio::ip::address_v4::loopback(), harness_.port()});
    asio::write(raw, asio::buffer(std::string("PING\r\nPING\r\n")));
    raw.shutdown(asio::ip::tcp::socket::shutdown_send);  // Like `printf ... | nc`.
    std::string received;
    asio::error_code ec;
    while (!ec) {
        char chunk[64];
        const std::size_t n = raw.read_some(asio::buffer(chunk), ec);
        received.append(chunk, n);
    }
    EXPECT_EQ(ec, asio::error::eof);
    EXPECT_EQ(received, "+PONG\r\n+PONG\r\n");
}

TEST_F(ServerIntegrationTest, StopWithIdleClientConnectedMakesRunReturn) {
    Client idle(harness_.port());
    EXPECT_EQ(idle.call({"PING"}), "+PONG\r\n");  // The session definitely exists.
    EXPECT_TRUE(harness_.stop_and_wait()) << "io_context::run() did not return after stop()";
    EXPECT_TRUE(idle.closed_by_server());
}

TEST(ServerIntegrationBackpressureTest, PipelinedFloodIsAnsweredInOrderUnderTinyLimits) {
    KvStore store;
    LocalReplicator replicator(store);
    Session::Limits limits;
    limits.max_pending_replies = 4;
    limits.max_output_bytes = 64;
    limits.read_chunk_bytes = 16;
    Harness harness(replicator, store, limits);

    // Small enough to sit in kernel socket buffers, so writing it all before
    // reading cannot deadlock.
    constexpr int kCommands = 2000;
    std::string batch;
    for (int i = 0; i < kCommands; ++i) batch += resp::command({"ECHO", std::to_string(i)});
    Client client(harness.port());
    client.send(batch);
    for (int i = 0; i < kCommands; ++i) {
        ASSERT_EQ(client.read_reply(), resp::bulk_string(std::to_string(i))) << "reply " << i;
    }
}

// Holds every proposal until the test completes it, from the test thread and
// in any order, to exercise reply reordering across threads.
class DeferredReplicator final : public Replicator {
public:
    void propose(Command /*command*/, CommitCallback on_commit) override {
        std::lock_guard lock(mutex_);
        pending_.push_back(std::move(on_commit));
    }
    [[nodiscard]] bool is_leader() const override { return true; }
    [[nodiscard]] std::uint64_t commit_index() const override { return 0; }
    [[nodiscard]] std::uint64_t current_term() const override { return 1; }

    // Waits until `n` proposals are pending.
    bool wait_for_pending(std::size_t n) {
        const auto deadline = std::chrono::steady_clock::now() + kTimeout;
        while (std::chrono::steady_clock::now() < deadline) {
            {
                std::lock_guard lock(mutex_);
                if (pending_.size() >= n) return true;
            }
            std::this_thread::sleep_for(1ms);
        }
        return false;
    }

    void complete(std::size_t i, const Status& status) {
        CommitCallback callback;
        {
            std::lock_guard lock(mutex_);
            callback = std::move(pending_.at(i));
        }
        callback(status, status.is_ok() ? i + 1 : 0);
    }

private:
    std::mutex mutex_;
    std::vector<CommitCallback> pending_;
};

TEST(ServerIntegrationOrderingTest, LaterCompletionsWaitForEarlierRequests) {
    KvStore store;
    DeferredReplicator replicator;
    Harness harness(replicator, store);
    Client client(harness.port());

    client.send(resp::command({"SET", "a", "1"}) + resp::command({"SET", "b", "2"}) +
                resp::command({"PING"}) + resp::command({"SET", "c", "3"}));
    ASSERT_TRUE(replicator.wait_for_pending(3));

    // Complete in reverse order from this (non-io) thread.
    replicator.complete(2, Status::ok());
    replicator.complete(1, Status::not_leader("b"));
    replicator.complete(0, Status::unavailable("a"));

    EXPECT_EQ(client.read_reply(), "-TRYAGAIN a\r\n");
    EXPECT_EQ(client.read_reply(), "-NOTLEADER b\r\n");
    EXPECT_EQ(client.read_reply(), "+PONG\r\n");
    EXPECT_EQ(client.read_reply(), "+OK\r\n");
}

}  // namespace
}  // namespace distkv

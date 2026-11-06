// End-to-end tests: a real Server on an ephemeral loopback port, driven by an
// io_context on one or more threads, talking to blocking clients over TCP.

#include <gtest/gtest.h>

#include <asio.hpp>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "network/session.hpp"
#include "protocol/resp.hpp"
#include "replication/local_replicator.hpp"
#include "server_test_support.hpp"
#include "storage/kv_store.hpp"

namespace distkv {
namespace {

using namespace std::chrono_literals;
using test::Args;
using test::Client;
using test::Harness;
using test::HarnessOptions;
using test::kTimeout;

// Runs each test with the io_context on 1 and on 4 threads.
class ServerIntegrationTest : public ::testing::TestWithParam<int> {
protected:
    KvStore store_;
    LocalReplicator replicator_{store_};
    Harness harness_{replicator_, store_, test::with_threads(GetParam())};
};

INSTANTIATE_TEST_SUITE_P(IoThreads, ServerIntegrationTest, ::testing::Values(1, 4),
                         [](const auto& param_info) {
                             return std::to_string(param_info.param) + "Threads";
                         });

TEST_P(ServerIntegrationTest, SetGetDelRoundTrip) {
    Client client(harness_.port());
    EXPECT_EQ(client.call({"PING"}), "+PONG\r\n");
    EXPECT_EQ(client.call({"SET", "k", "v"}), "+OK\r\n");
    EXPECT_EQ(client.call({"GET", "k"}), "$1\r\nv\r\n");
    EXPECT_EQ(client.call({"DEL", "k"}), ":1\r\n");
    EXPECT_EQ(client.call({"GET", "k"}), "$-1\r\n");
}

TEST_P(ServerIntegrationTest, InlineCommands) {
    Client client(harness_.port());
    client.send("SET a b\r\nGET a\r\n");
    EXPECT_EQ(client.read_reply(), "+OK\r\n");
    EXPECT_EQ(client.read_reply(), "$1\r\nb\r\n");
}

TEST_P(ServerIntegrationTest, BinaryValueRoundTrip) {
    Client client(harness_.port());
    const std::string value("\0\r\n\xff", 4);
    EXPECT_EQ(client.call({"SET", "bin", value}), "+OK\r\n");
    EXPECT_EQ(client.call({"GET", "bin"}), "$4\r\n" + value + "\r\n");
}

TEST_P(ServerIntegrationTest, HundredPipelinedCommandsGetRepliesInOrder) {
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

TEST_P(ServerIntegrationTest, RequestSplitAcrossWrites) {
    Client client(harness_.port());
    client.send("*3\r\n$3\r\nSE");
    std::this_thread::sleep_for(20ms);
    client.send("T\r\n$1\r\nk\r\n$5\r\nhel");
    std::this_thread::sleep_for(20ms);
    client.send("lo\r\n");
    EXPECT_EQ(client.read_reply(), "+OK\r\n");
    EXPECT_EQ(client.call({"GET", "k"}), "$5\r\nhello\r\n");
}

TEST_P(ServerIntegrationTest, TwoClientsAtOnce) {
    Client a(harness_.port());
    Client b(harness_.port());
    a.send_command({"SET", "a", "1"});
    b.send_command({"SET", "b", "2"});
    EXPECT_EQ(b.read_reply(), "+OK\r\n");
    EXPECT_EQ(a.read_reply(), "+OK\r\n");
    EXPECT_EQ(a.call({"GET", "b"}), "$1\r\n2\r\n");
    EXPECT_EQ(b.call({"GET", "a"}), "$1\r\n1\r\n");
}

TEST_P(ServerIntegrationTest, ProtocolErrorRepliesThenCloses) {
    Client client(harness_.port());
    client.send("*1\r\n$x\r\n");
    EXPECT_EQ(client.read_reply(), "-ERR Protocol error: invalid bulk length 'x'\r\n");
    EXPECT_TRUE(client.closed_by_server());
}

TEST_P(ServerIntegrationTest, RequestsBeforeAProtocolErrorAreStillAnswered) {
    Client client(harness_.port());
    client.send("PING\r\nECHO hi\r\n+bad\r\nPING\r\n");
    EXPECT_EQ(client.read_reply(), "+PONG\r\n");
    EXPECT_EQ(client.read_reply(), "$2\r\nhi\r\n");
    EXPECT_EQ(client.read_reply().rfind("-ERR Protocol error: unexpected type byte", 0), 0U);
    EXPECT_TRUE(client.closed_by_server());
}

TEST_P(ServerIntegrationTest, HalfClosedClientStillGetsReplies) {
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

TEST_P(ServerIntegrationTest, StopWithIdleClientConnectedMakesRunReturn) {
    Client idle(harness_.port());
    EXPECT_EQ(idle.call({"PING"}), "+PONG\r\n");  // The session definitely exists.
    EXPECT_TRUE(harness_.stop_and_wait()) << "io_context::run() did not return after stop()";
    EXPECT_TRUE(idle.closed_by_server());
}

TEST(ServerIntegrationBackpressureTest, PipelinedFloodIsAnsweredInOrderUnderTinyLimits) {
    KvStore store;
    LocalReplicator replicator(store);
    HarnessOptions options;
    options.limits.max_pending_replies = 4;
    options.limits.max_output_bytes = 64;
    options.limits.read_chunk_bytes = 16;
    Harness harness(replicator, store, options);

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

// Concurrency tests: a real Server whose io_context runs on several threads,
// driven by many blocking clients at once. Sizes are constants so the suite
// stays fast under ThreadSanitizer.

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <latch>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
#include <set>
#include <string>
#include <thread>
#include <unordered_set>
#include <utility>
#include <vector>

#include "protocol/resp.hpp"
#include "replication/local_replicator.hpp"
#include "server_test_support.hpp"
#include "storage/kv_store.hpp"

namespace distkv {
namespace {

using namespace std::chrono_literals;
using test::Client;
using test::Harness;
using test::HarnessOptions;
using test::kTimeout;

constexpr int kIoThreads = 4;

constexpr int kManyClients = 32;
constexpr int kCommandsPerClient = 1000;
constexpr int kKeysPerClient = 20;

constexpr int kSharedClients = 16;
constexpr int kSharedKeys = 10;
constexpr int kSharedOpsPerClient = 500;

constexpr int kChurnClients = 8;
constexpr int kChurnRounds = 40;
constexpr int kSteadyClients = 4;

constexpr int kStopClients = 16;

// Polls `done` until it holds or kTimeout passes.
template <typename Pred>
bool wait_until(Pred done) {
    const auto deadline = std::chrono::steady_clock::now() + kTimeout;
    while (!done()) {
        if (std::chrono::steady_clock::now() >= deadline) return false;
        std::this_thread::sleep_for(1ms);
    }
    return true;
}

// LocalReplicator that records which threads proposals arrive on. Sessions
// call propose() from their strand, so these are the io threads that ran
// session handlers.
class ThreadRecordingReplicator final : public Replicator {
public:
    explicit ThreadRecordingReplicator(StateMachine& state_machine) : inner_(state_machine) {}

    void propose(Command command, CommitCallback on_commit) override {
        {
            std::lock_guard lock(mutex_);
            threads_.insert(std::this_thread::get_id());
        }
        inner_.propose(std::move(command), std::move(on_commit));
    }
    [[nodiscard]] bool is_leader() const override { return inner_.is_leader(); }
    [[nodiscard]] std::uint64_t commit_index() const override { return inner_.commit_index(); }
    [[nodiscard]] std::uint64_t current_term() const override { return inner_.current_term(); }

    std::set<std::thread::id> threads() {
        std::lock_guard lock(mutex_);
        return threads_;
    }

private:
    LocalReplicator inner_;
    std::mutex mutex_;
    std::set<std::thread::id> threads_;  // Guarded by mutex_.
};

class ServerConcurrencyTest : public ::testing::Test {
protected:
    KvStore store_;
    ThreadRecordingReplicator replicator_{store_};
    Harness harness_{replicator_, store_, test::with_threads(kIoThreads)};
};

// Runs body(i) on `n` threads that start together, and joins them.
template <typename Body>
void run_clients(int n, Body body) {
    std::latch start(n);
    std::vector<std::thread> threads;
    threads.reserve(static_cast<std::size_t>(n));
    for (int i = 0; i < n; ++i) {
        threads.emplace_back([&, i] {
            start.arrive_and_wait();
            body(i);
        });
    }
    for (auto& thread : threads) thread.join();
}

TEST_F(ServerConcurrencyTest, ManyClientsPipeliningMixedCommandsGetTheirOwnRepliesInOrder) {
    std::atomic<int> clients_ok{0};
    run_clients(kManyClients, [&](int id) {
        // Each client works on its own keys, so its model predicts every
        // reply exactly regardless of the other clients.
        std::mt19937 rng(static_cast<std::uint32_t>(1234 + id));
        std::map<std::string, std::string> model;
        std::string batch;
        std::vector<std::string> expected;
        expected.reserve(kCommandsPerClient);
        for (int i = 0; i < kCommandsPerClient; ++i) {
            const std::string key = "c" + std::to_string(id) + ":k" +
                                    std::to_string(rng() % kKeysPerClient);
            switch (rng() % 3) {
                case 0: {
                    const std::string value = "v" + std::to_string(id) + "-" + std::to_string(i);
                    batch += resp::command({"SET", key, value});
                    model[key] = value;
                    expected.push_back(resp::simple_string("OK"));
                    break;
                }
                case 1: {
                    batch += resp::command({"GET", key});
                    const auto it = model.find(key);
                    expected.push_back(it == model.end() ? resp::null_bulk()
                                                         : resp::bulk_string(it->second));
                    break;
                }
                default:
                    batch += resp::command({"DEL", key});
                    expected.push_back(resp::integer(model.erase(key) > 0 ? 1 : 0));
                    break;
            }
        }

        Client client(harness_.port());
        client.send(batch);  // All 1,000 commands in one write.
        for (int i = 0; i < kCommandsPerClient; ++i) {
            const std::string reply = client.read_reply();
            if (reply != expected[static_cast<std::size_t>(i)]) {
                ADD_FAILURE() << "client " << id << " reply " << i << ": got " << reply
                              << " expected " << expected[static_cast<std::size_t>(i)];
                return;
            }
        }
        clients_ok.fetch_add(1);
    });
    EXPECT_EQ(clients_ok.load(), kManyClients);
}

// A value that identifies its writer and is long enough that a torn read
// would show.
std::string shared_value(int client, int op) {
    return "c" + std::to_string(client) + "-op" + std::to_string(op) + "|" +
           std::string(64 + static_cast<std::size_t>(op % 32),
                       static_cast<char>('a' + client % 26)) +
           "|end";
}

TEST_F(ServerConcurrencyTest, SharedKeysOnlyEverReadCompleteWrittenValues) {
    std::unordered_set<std::string> written;
    for (int c = 0; c < kSharedClients; ++c) {
        for (int op = 0; op < kSharedOpsPerClient; ++op) written.insert(shared_value(c, op));
    }

    std::atomic<long> gets{0};
    std::atomic<long> hits{0};
    run_clients(kSharedClients, [&](int id) {
        std::mt19937 rng(static_cast<std::uint32_t>(99 + id));
        std::string batch;
        std::vector<bool> is_get;
        for (int op = 0; op < kSharedOpsPerClient; ++op) {
            const std::string key = "shared:" + std::to_string(rng() % kSharedKeys);
            const bool get = op % 2 == 1;
            batch += get ? resp::command({"GET", key})
                         : resp::command({"SET", key, shared_value(id, op)});
            is_get.push_back(get);
        }

        Client client(harness_.port());
        client.send(batch);
        for (int op = 0; op < kSharedOpsPerClient; ++op) {
            const std::string reply = client.read_reply();
            if (!is_get[static_cast<std::size_t>(op)]) {
                EXPECT_EQ(reply, "+OK\r\n") << "client " << id << " op " << op;
                continue;
            }
            gets.fetch_add(1);
            if (reply == resp::null_bulk()) continue;
            // "$<len>\r\n<value>\r\n"
            const std::size_t header_end = reply.find("\r\n");
            ASSERT_EQ(reply[0], '$') << reply;
            const std::string value =
                reply.substr(header_end + 2, reply.size() - header_end - 4);
            EXPECT_EQ(reply, resp::bulk_string(value));
            EXPECT_TRUE(written.contains(value)) << "never written: " << value;
            hits.fetch_add(1);
        }
    });
    EXPECT_EQ(gets.load(), long{kSharedClients} * (kSharedOpsPerClient / 2));
    EXPECT_GT(hits.load(), 0);
}

TEST_F(ServerConcurrencyTest, ChurningClientsDoNotDisturbOthersOrLeakSessions) {
    std::atomic<bool> churn_done{false};
    std::atomic<long> steady_calls{0};

    std::vector<std::thread> steady;
    for (int id = 0; id < kSteadyClients; ++id) {
        steady.emplace_back([&, id] {
            Client client(harness_.port());
            const std::string key = "steady:" + std::to_string(id);
            for (int i = 0; !churn_done.load() || i < 10; ++i) {
                const std::string value = std::to_string(i);
                ASSERT_EQ(client.call({"SET", key, value}), "+OK\r\n");
                ASSERT_EQ(client.call({"GET", key}), resp::bulk_string(value));
                steady_calls.fetch_add(1);
            }
        });
    }

    run_clients(kChurnClients, [&](int id) {
        for (int round = 0; round < kChurnRounds; ++round) {
            Client client(harness_.port());
            switch ((id + round) % 4) {
                case 0:  // A partial frame.
                    client.send("*3\r\n$3\r\nSET\r\n$1\r\nk");
                    break;
                case 1:  // Half a pipeline: complete commands, then a partial one.
                    client.send(resp::command({"SET", "churn", "x"}) + resp::command({"PING"}) +
                                "*2\r\n$3\r\nGET");
                    break;
                case 2:  // Whole commands whose replies are never read.
                    for (int i = 0; i < 20; ++i) client.send(resp::command({"GET", "churn"}));
                    break;
                default:  // Connect and leave.
                    break;
            }
            if (round % 2 == 0) client.abort();  // RST; otherwise a normal close (FIN).
        }
    });
    churn_done.store(true);
    for (auto& thread : steady) thread.join();

    EXPECT_GT(steady_calls.load(), 0);
    EXPECT_TRUE(wait_until([&] { return harness_.server().connection_count() == 0; }))
        << harness_.server().connection_count() << " connections still counted";
    EXPECT_TRUE(wait_until([&] { return harness_.server().live_sessions() == 0; }))
        << harness_.server().live_sessions() << " sessions still alive";
}

TEST_F(ServerConcurrencyTest, StopWhileClientsAreSendingReturnsFromEveryIoThread) {
    std::atomic<long> replies{0};
    std::latch connected(kStopClients);
    std::vector<std::thread> clients;
    for (int id = 0; id < kStopClients; ++id) {
        clients.emplace_back([&, id] {
            Client client(harness_.port());
            connected.count_down();
            std::string batch;
            for (int i = 0; i < 10; ++i) {
                batch += resp::command({"SET", "load:" + std::to_string(id), std::to_string(i)});
            }
            // Until the server closes the connection.
            while (client.try_send(batch)) {
                for (int i = 0; i < 10; ++i) {
                    if (!client.try_read_reply()) return;
                    replies.fetch_add(1);
                }
            }
        });
    }
    connected.wait();
    EXPECT_TRUE(wait_until([&] { return replies.load() >= kStopClients * 10; }));

    EXPECT_TRUE(harness_.stop_and_wait()) << "an io thread did not return after stop()";
    for (auto& thread : clients) thread.join();
}

TEST_F(ServerConcurrencyTest, HandlersRunOnMoreThanOneThread) {
    run_clients(kSharedClients, [&](int id) {
        std::string batch;
        for (int i = 0; i < 200; ++i) {
            batch += resp::command({"SET", "t" + std::to_string(id), std::to_string(i)});
        }
        Client client(harness_.port());
        client.send(batch);
        for (int i = 0; i < 200; ++i) ASSERT_EQ(client.read_reply(), "+OK\r\n");
    });
    const auto threads = replicator_.threads();
    EXPECT_GT(threads.size(), 1U) << "every handler ran on one thread";
    EXPECT_LE(threads.size(), static_cast<std::size_t>(kIoThreads));
    EXPECT_FALSE(threads.contains(std::this_thread::get_id()));
}

TEST(ServerConnectionLimitTest, ExtraClientIsRejectedAndSlotsAreReleased) {
    KvStore store;
    LocalReplicator replicator(store);
    HarnessOptions options = test::with_threads(kIoThreads);
    options.max_connections = 2;
    Harness harness(replicator, store, options);
    Server& server = harness.server();

    auto first = std::make_unique<Client>(harness.port());
    Client second(harness.port());
    EXPECT_EQ(first->call({"PING"}), "+PONG\r\n");
    EXPECT_EQ(second.call({"PING"}), "+PONG\r\n");

    Client third(harness.port());
    EXPECT_EQ(third.try_read_reply(), "-ERR max number of clients reached\r\n");
    EXPECT_TRUE(third.closed_by_server());
    EXPECT_EQ(server.connection_count(), 2U);
    EXPECT_EQ(second.call({"PING"}), "+PONG\r\n");  // Existing clients are unaffected.

    first.reset();  // Leaves; its slot is released when the server sees EOF.
    EXPECT_TRUE(wait_until([&] { return server.connection_count() == 1; }));

    Client fourth(harness.port());
    EXPECT_EQ(fourth.call({"PING"}), "+PONG\r\n");
    EXPECT_EQ(server.connection_count(), 2U);
}

}  // namespace
}  // namespace distkv

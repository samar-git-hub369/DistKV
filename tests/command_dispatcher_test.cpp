#include "server/command_dispatcher.hpp"

#include <gtest/gtest.h>

#include <cstdint>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "replication/local_replicator.hpp"
#include "server/kv_service.hpp"
#include "storage/kv_store.hpp"

namespace distkv {
namespace {

using namespace std::string_literals;
using Args = std::vector<std::string>;

class CommandDispatcherTest : public ::testing::Test {
protected:
    // Dispatches and returns the reply, asserting the callback ran exactly once.
    std::string run(Args args) {
        int calls = 0;
        std::string reply;
        dispatcher_.dispatch(std::move(args), [&](std::string r) {
            ++calls;
            reply = std::move(r);
        });
        EXPECT_EQ(calls, 1);
        return reply;
    }

    KvStore store_;
    LocalReplicator replicator_{store_};
    KvService service_{replicator_, store_};
    CommandDispatcher dispatcher_{service_};
};

TEST_F(CommandDispatcherTest, Ping) {
    EXPECT_EQ(run({"PING"}), "+PONG\r\n");
    EXPECT_EQ(run({"PING", "hello"}), "$5\r\nhello\r\n");
    EXPECT_EQ(run({"PING", "a", "b"}), "-ERR wrong number of arguments for 'ping' command\r\n");
}

TEST_F(CommandDispatcherTest, Echo) {
    EXPECT_EQ(run({"ECHO", "hi there"}), "$8\r\nhi there\r\n");
    EXPECT_EQ(run({"ECHO"}), "-ERR wrong number of arguments for 'echo' command\r\n");
    EXPECT_EQ(run({"ECHO", "a", "b"}), "-ERR wrong number of arguments for 'echo' command\r\n");
}

TEST_F(CommandDispatcherTest, SetGet) {
    EXPECT_EQ(run({"SET", "k", "v"}), "+OK\r\n");
    EXPECT_EQ(run({"GET", "k"}), "$1\r\nv\r\n");
    EXPECT_EQ(run({"SET", "k", "v2"}), "+OK\r\n");
    EXPECT_EQ(run({"GET", "k"}), "$2\r\nv2\r\n");
    EXPECT_EQ(run({"GET", "missing"}), "$-1\r\n");
}

TEST_F(CommandDispatcherTest, SetEmptyValue) {
    EXPECT_EQ(run({"SET", "k", ""}), "+OK\r\n");
    EXPECT_EQ(run({"GET", "k"}), "$0\r\n\r\n");
}

TEST_F(CommandDispatcherTest, BinaryKeysAndValues) {
    const std::string key = "k\0\r\n"s;
    const std::string value = "\0\xff\r\nv"s;
    EXPECT_EQ(run({"SET", key, value}), "+OK\r\n");
    EXPECT_EQ(run({"GET", key}), "$5\r\n" + value + "\r\n");
}

TEST_F(CommandDispatcherTest, SetArity) {
    const std::string error = "-ERR wrong number of arguments for 'set' command\r\n";
    EXPECT_EQ(run({"SET"}), error);
    EXPECT_EQ(run({"SET", "k"}), error);
    EXPECT_EQ(run({"SET", "k", "v", "EX", "10"}), error);  // No options yet.
    EXPECT_EQ(store_.size(), 0U);
}

TEST_F(CommandDispatcherTest, GetArity) {
    const std::string error = "-ERR wrong number of arguments for 'get' command\r\n";
    EXPECT_EQ(run({"GET"}), error);
    EXPECT_EQ(run({"GET", "a", "b"}), error);
}

TEST_F(CommandDispatcherTest, DelCountsExistingKeys) {
    run({"SET", "a", "1"});
    run({"SET", "b", "2"});
    EXPECT_EQ(run({"DEL", "a"}), ":1\r\n");
    EXPECT_EQ(run({"GET", "a"}), "$-1\r\n");
    EXPECT_EQ(run({"DEL", "a"}), ":0\r\n");
    EXPECT_EQ(run({"SET", "a", "1"}), "+OK\r\n");
    EXPECT_EQ(run({"DEL", "a", "b", "missing"}), ":2\r\n");
    EXPECT_EQ(store_.size(), 0U);
}

TEST_F(CommandDispatcherTest, DelCountsDuplicateKeyOnce) {
    run({"SET", "a", "1"});
    EXPECT_EQ(run({"DEL", "a", "a"}), ":1\r\n");
}

TEST_F(CommandDispatcherTest, DelProposesOneDeletePerKey) {
    run({"SET", "a", "1"});  // Index 1.
    run({"DEL", "a", "b", "c"});
    EXPECT_EQ(replicator_.commit_index(), 4U);
}

TEST_F(CommandDispatcherTest, DelArity) {
    EXPECT_EQ(run({"DEL"}), "-ERR wrong number of arguments for 'del' command\r\n");
}

TEST_F(CommandDispatcherTest, CommandRepliesWithEmptyArray) {
    EXPECT_EQ(run({"COMMAND"}), "*0\r\n");
    EXPECT_EQ(run({"COMMAND", "DOCS"}), "*0\r\n");
}

TEST_F(CommandDispatcherTest, CommandNamesAreCaseInsensitive) {
    EXPECT_EQ(run({"ping"}), "+PONG\r\n");
    EXPECT_EQ(run({"PiNg"}), "+PONG\r\n");
    EXPECT_EQ(run({"set", "k", "v"}), "+OK\r\n");
    EXPECT_EQ(run({"gEt", "k"}), "$1\r\nv\r\n");
    EXPECT_EQ(run({"Del", "k"}), ":1\r\n");
    EXPECT_EQ(run({"echo", "x"}), "$1\r\nx\r\n");
    EXPECT_EQ(run({"command"}), "*0\r\n");
    // Arity errors use the lower-case name, like Redis.
    EXPECT_EQ(run({"GeT"}), "-ERR wrong number of arguments for 'get' command\r\n");
}

TEST_F(CommandDispatcherTest, UnknownCommand) {
    EXPECT_EQ(run({"FLUSHALL"}), "-ERR unknown command 'FLUSHALL'\r\n");
    EXPECT_EQ(run({"nope", "a"}), "-ERR unknown command 'nope'\r\n");
    // Non-printable bytes in the name cannot corrupt the reply framing.
    EXPECT_EQ(run({"x\r\ny"}), "-ERR unknown command 'x??y'\r\n");
}

TEST_F(CommandDispatcherTest, EmptyCommandIsAnError) {
    EXPECT_EQ(run({}), "-ERR empty command\r\n");
}

TEST_F(CommandDispatcherTest, EmptyKeyMapsInvalidArgumentToErr) {
    EXPECT_EQ(run({"SET", "", "v"}), "-ERR key must not be empty\r\n");
    EXPECT_EQ(run({"GET", ""}), "-ERR key must not be empty\r\n");
    EXPECT_EQ(run({"DEL", ""}), "-ERR key must not be empty\r\n");
}

TEST(CommandDispatcherErrorTest, EncodesEveryStatusCode) {
    EXPECT_EQ(CommandDispatcher::encode_error(Status::invalid_argument("bad")), "-ERR bad\r\n");
    EXPECT_EQ(CommandDispatcher::encode_error(Status::not_leader("node 2")),
              "-NOTLEADER node 2\r\n");
    EXPECT_EQ(CommandDispatcher::encode_error(Status::unavailable("no quorum")),
              "-TRYAGAIN no quorum\r\n");
    EXPECT_EQ(CommandDispatcher::encode_error(Status::internal("oops")),
              "-ERR internal: oops\r\n");
}

// Never the leader: reads and writes are refused.
class FollowerReplicator final : public Replicator {
public:
    void propose(Command /*command*/, CommitCallback on_commit) override {
        ++proposals;
        if (on_commit) on_commit(Status::not_leader("leader unknown"), 0);
    }
    [[nodiscard]] bool is_leader() const override { return false; }
    [[nodiscard]] std::uint64_t commit_index() const override { return 0; }
    [[nodiscard]] std::uint64_t current_term() const override { return 1; }

    int proposals{0};
};

TEST(CommandDispatcherFollowerTest, RepliesNotLeader) {
    KvStore store;
    FollowerReplicator replicator;
    KvService service(replicator, store);
    CommandDispatcher dispatcher(service);

    const auto run = [&](Args args) {
        std::string reply;
        dispatcher.dispatch(std::move(args), [&](std::string r) { reply = std::move(r); });
        return reply;
    };
    EXPECT_EQ(run({"GET", "k"}), "-NOTLEADER reads are served by the leader only\r\n");
    EXPECT_EQ(run({"SET", "k", "v"}), "-NOTLEADER leader unknown\r\n");
    // DEL checks first and proposes nothing on a follower.
    EXPECT_EQ(run({"DEL", "a", "b"}), "-NOTLEADER reads are served by the leader only\r\n");
    EXPECT_EQ(replicator.proposals, 1);  // Only the SET.
    // Commands that do not touch the store still work.
    EXPECT_EQ(run({"PING"}), "+PONG\r\n");
}

}  // namespace
}  // namespace distkv

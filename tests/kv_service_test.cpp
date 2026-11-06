#include "server/kv_service.hpp"

#include <gtest/gtest.h>

#include <cstdint>
#include <optional>
#include <string>

#include "replication/local_replicator.hpp"
#include "storage/kv_store.hpp"

namespace distkv {
namespace {

struct Result {
    int calls{0};
    Status status;
    std::uint64_t index{0};
};

// Returns a callback that records into `result`.
CommitCallback record(Result& result) {
    return [&result](const Status& status, std::uint64_t index) {
        ++result.calls;
        result.status = status;
        result.index = index;
    };
}

class KvServiceTest : public ::testing::Test {
protected:
    KvStore store_;
    LocalReplicator replicator_{store_};
    KvService service_{replicator_, store_};
};

TEST_F(KvServiceTest, SetThenGetRoundTrip) {
    Result result;
    service_.set("k", "v", record(result));
    EXPECT_EQ(result.calls, 1);
    ASSERT_TRUE(result.status.is_ok()) << result.status.to_string();
    EXPECT_EQ(result.index, 1U);

    const auto got = service_.get("k");
    ASSERT_TRUE(got.is_ok()) << got.status().to_string();
    EXPECT_EQ(*got, "v");
}

TEST_F(KvServiceTest, GetMissingKeyReturnsEmptyOptional) {
    const auto got = service_.get("missing");
    ASSERT_TRUE(got.is_ok());
    EXPECT_EQ(*got, std::nullopt);
}

TEST_F(KvServiceTest, DelThenGetReturnsNothing) {
    Result set_result;
    service_.set("k", "v", record(set_result));
    ASSERT_TRUE(set_result.status.is_ok());

    Result del_result;
    service_.del("k", record(del_result));
    EXPECT_EQ(del_result.calls, 1);
    ASSERT_TRUE(del_result.status.is_ok());
    EXPECT_EQ(del_result.index, 2U);

    const auto got = service_.get("k");
    ASSERT_TRUE(got.is_ok());
    EXPECT_EQ(*got, std::nullopt);
}

TEST_F(KvServiceTest, EmptyKeyIsRejected) {
    Result set_result;
    service_.set("", "v", record(set_result));
    EXPECT_EQ(set_result.calls, 1);
    EXPECT_EQ(set_result.status.code(), StatusCode::kInvalidArgument);

    Result del_result;
    service_.del("", record(del_result));
    EXPECT_EQ(del_result.calls, 1);
    EXPECT_EQ(del_result.status.code(), StatusCode::kInvalidArgument);

    EXPECT_EQ(service_.get("").status().code(), StatusCode::kInvalidArgument);
    EXPECT_EQ(store_.size(), 0U);
    EXPECT_EQ(replicator_.commit_index(), 0U);
}

TEST_F(KvServiceTest, CallbackRunsExactlyOncePerWrite) {
    Result a;
    Result b;
    Result c;
    service_.set("a", "1", record(a));
    service_.set("a", "2", record(b));
    service_.del("a", record(c));
    EXPECT_EQ(a.calls, 1);
    EXPECT_EQ(b.calls, 1);
    EXPECT_EQ(c.calls, 1);
    EXPECT_EQ(c.index, 3U);
}

TEST_F(KvServiceTest, NullCallbackIsAllowed) {
    service_.set("k", "v", nullptr);
    EXPECT_EQ(*service_.get("k"), "v");
}

// A Replicator that is never the leader and rejects every write.
class FollowerReplicator final : public Replicator {
public:
    void propose(Command /*command*/, CommitCallback on_commit) override {
        if (on_commit) on_commit(Status::not_leader("not the leader"), 0);
    }
    [[nodiscard]] bool is_leader() const override { return false; }
    [[nodiscard]] std::uint64_t commit_index() const override { return 0; }
    [[nodiscard]] std::uint64_t current_term() const override { return 1; }
};

TEST(KvServiceFollowerTest, ReadsAndWritesReportNotLeader) {
    KvStore store;
    FollowerReplicator replicator;
    KvService service(replicator, store);

    EXPECT_EQ(service.get("k").status().code(), StatusCode::kNotLeader);

    Result result;
    service.set("k", "v", record(result));
    EXPECT_EQ(result.calls, 1);
    EXPECT_EQ(result.status.code(), StatusCode::kNotLeader);
}

}  // namespace
}  // namespace distkv

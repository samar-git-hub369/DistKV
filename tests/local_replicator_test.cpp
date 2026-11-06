#include "replication/local_replicator.hpp"

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "storage/state_machine.hpp"

namespace distkv {
namespace {

// Records every applied entry. The Replicator serialises apply() calls, but
// the mutex keeps reads from the test thread well-defined too.
class RecordingStateMachine final : public StateMachine {
public:
    void apply(const LogEntry& entry) override {
        std::lock_guard lock(mutex_);
        applied_.push_back(entry);
    }

    std::vector<LogEntry> applied() const {
        std::lock_guard lock(mutex_);
        return applied_;
    }

    std::size_t size() const {
        std::lock_guard lock(mutex_);
        return applied_.size();
    }

private:
    mutable std::mutex mutex_;
    std::vector<LogEntry> applied_;
};

struct Result {
    Status status;
    std::uint64_t index{0};
};

class LocalReplicatorTest : public ::testing::Test {
protected:
    // Proposes and captures the callback result, asserting it ran exactly once.
    Result propose_sync(Command command) {
        int calls = 0;
        Result result;
        replicator_.propose(std::move(command), [&](const Status& status, std::uint64_t index) {
            ++calls;
            result = Result{status, index};
        });
        EXPECT_EQ(calls, 1);
        return result;
    }

    RecordingStateMachine sm_;
    LocalReplicator replicator_{sm_};
};

TEST_F(LocalReplicatorTest, InitialState) {
    EXPECT_TRUE(replicator_.is_leader());
    EXPECT_EQ(replicator_.current_term(), 1U);
    EXPECT_EQ(replicator_.commit_index(), 0U);
}

TEST_F(LocalReplicatorTest, AssignsContiguousIndexesFromOne) {
    for (std::uint64_t i = 1; i <= 5; ++i) {
        const Result r = propose_sync(Command::set("k" + std::to_string(i), "v"));
        ASSERT_TRUE(r.status.is_ok()) << r.status.to_string();
        EXPECT_EQ(r.index, i);
        EXPECT_EQ(replicator_.commit_index(), i);
    }

    const std::vector<LogEntry> applied = sm_.applied();
    ASSERT_EQ(applied.size(), 5U);
    for (std::size_t i = 0; i < applied.size(); ++i) {
        EXPECT_EQ(applied[i].index, i + 1);
        EXPECT_EQ(applied[i].term, 1U);
        EXPECT_EQ(applied[i].command, Command::set("k" + std::to_string(i + 1), "v"));
    }
}

TEST_F(LocalReplicatorTest, AppliesBeforeInvokingCallback) {
    std::optional<std::size_t> applied_at_callback;
    replicator_.propose(Command::set("k", "v"), [&](const Status& status, std::uint64_t index) {
        ASSERT_TRUE(status.is_ok());
        EXPECT_EQ(index, 1U);
        EXPECT_EQ(replicator_.commit_index(), 1U);
        applied_at_callback = sm_.size();
    });
    ASSERT_TRUE(applied_at_callback.has_value());
    EXPECT_EQ(*applied_at_callback, 1U);
}

TEST_F(LocalReplicatorTest, RejectsEmptyKeyWithoutConsumingIndex) {
    const Result rejected = propose_sync(Command::set("", "v"));
    EXPECT_EQ(rejected.status.code(), StatusCode::kInvalidArgument);
    EXPECT_EQ(rejected.index, 0U);
    EXPECT_EQ(propose_sync(Command::del("")).status.code(), StatusCode::kInvalidArgument);
    EXPECT_EQ(sm_.size(), 0U);
    EXPECT_EQ(replicator_.commit_index(), 0U);

    // The next valid write still gets index 1: no hole was left.
    const Result accepted = propose_sync(Command::set("k", "v"));
    ASSERT_TRUE(accepted.status.is_ok());
    EXPECT_EQ(accepted.index, 1U);
}

TEST_F(LocalReplicatorTest, NullCallbackIsAllowed) {
    replicator_.propose(Command::set("k", "v"), nullptr);
    replicator_.propose(Command::set("", "v"), nullptr);  // Rejected, still no crash.
    EXPECT_EQ(sm_.size(), 1U);
    EXPECT_EQ(replicator_.commit_index(), 1U);
}

TEST_F(LocalReplicatorTest, CallbackCanProposeAgain) {
    // Would deadlock if the callback ran while the replicator's mutex was held.
    std::vector<std::uint64_t> indexes;
    replicator_.propose(Command::set("a", "1"), [&](const Status& first, std::uint64_t i1) {
        ASSERT_TRUE(first.is_ok());
        indexes.push_back(i1);
        replicator_.propose(Command::set("b", "2"), [&](const Status& second, std::uint64_t i2) {
            ASSERT_TRUE(second.is_ok());
            indexes.push_back(i2);
        });
    });
    EXPECT_EQ(indexes, (std::vector<std::uint64_t>{1, 2}));
    EXPECT_EQ(sm_.size(), 2U);
}

TEST_F(LocalReplicatorTest, ConcurrentProposalsGetUniqueGapFreeIndexesAppliedInOrder) {
    constexpr int kThreads = 8;
    constexpr int kWritesPerThread = 1000;
    constexpr std::uint64_t kTotal = std::uint64_t{kThreads} * kWritesPerThread;

    std::vector<std::vector<std::uint64_t>> per_thread(kThreads);
    std::vector<std::thread> threads;
    threads.reserve(kThreads);
    for (int t = 0; t < kThreads; ++t) {
        threads.emplace_back([this, t, &per_thread] {
            auto& mine = per_thread[static_cast<std::size_t>(t)];
            mine.reserve(kWritesPerThread);
            for (int i = 0; i < kWritesPerThread; ++i) {
                const std::string key = "t" + std::to_string(t) + "-" + std::to_string(i);
                replicator_.propose(Command::set(key, "v"),
                                    [&mine](const Status& status, std::uint64_t index) {
                                        EXPECT_TRUE(status.is_ok());
                                        mine.push_back(index);
                                    });
            }
        });
    }
    for (auto& thread : threads) {
        thread.join();
    }

    // Every callback ran, and together they hold each index in 1..kTotal once.
    std::vector<std::uint64_t> all;
    all.reserve(kTotal);
    for (const auto& mine : per_thread) {
        ASSERT_EQ(mine.size(), static_cast<std::size_t>(kWritesPerThread));
        // A single thread's proposals are sequential, so its indexes increase.
        EXPECT_TRUE(std::is_sorted(mine.begin(), mine.end()));
        all.insert(all.end(), mine.begin(), mine.end());
    }
    std::sort(all.begin(), all.end());
    for (std::uint64_t i = 0; i < kTotal; ++i) {
        ASSERT_EQ(all[i], i + 1) << "duplicate or missing index";
    }

    // The state machine saw the entries in exact index order.
    const std::vector<LogEntry> applied = sm_.applied();
    ASSERT_EQ(applied.size(), kTotal);
    for (std::uint64_t i = 0; i < kTotal; ++i) {
        ASSERT_EQ(applied[i].index, i + 1);
    }
    EXPECT_EQ(replicator_.commit_index(), kTotal);
}

}  // namespace
}  // namespace distkv

#include "raft/raft_log.h"

#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <random>
#include <string>
#include <vector>

#include "raft/hard_state.h"

namespace distkv::raft {
namespace {

namespace fs = std::filesystem;

LogEntry Put(uint64_t term, uint64_t index, std::string key, std::string value) {
    return LogEntry{term, index, EntryOp::kPut, std::move(key), std::move(value)};
}
LogEntry Del(uint64_t term, uint64_t index, std::string key) {
    return LogEntry{term, index, EntryOp::kDelete, std::move(key), ""};
}
LogEntry Noop(uint64_t term, uint64_t index) { return LogEntry{term, index, EntryOp::kNoop, "", ""}; }

class RaftLogTest : public ::testing::Test {
  protected:
    void SetUp() override {
        std::random_device rd;
        dir_ = fs::temp_directory_path() / ("distkv_raft_test_" + std::to_string(rd()));
        fs::create_directories(dir_);
        path_ = (dir_ / "raft.log").string();
    }
    void TearDown() override {
        log_.reset();
        std::error_code ec;
        fs::remove_all(dir_, ec);
    }

    Status Reopen(RaftLogRecovery* rec = nullptr) {
        log_.reset();
        return RaftLog::Open(path_, /*sync=*/false, &log_, rec);
    }

    uint64_t FileSize() const { return fs::file_size(path_); }

    void FlipByte(uint64_t offset) {
        std::fstream f(path_, std::ios::in | std::ios::out | std::ios::binary);
        f.seekg(static_cast<std::streamoff>(offset));
        char c = 0;
        f.read(&c, 1);
        c = static_cast<char>(~c);
        f.seekp(static_cast<std::streamoff>(offset));
        f.write(&c, 1);
    }

    void AppendRaw(const std::string& bytes) {
        std::ofstream f(path_, std::ios::binary | std::ios::app);
        f << bytes;
    }

    std::vector<LogEntry> All() const { return log_->Slice(1, SIZE_MAX, SIZE_MAX); }

    fs::path dir_;
    std::string path_;
    std::unique_ptr<RaftLog> log_;
};

TEST(RaftTypesTest, EntryRoundTripsAndRejectsBadOps) {
    for (const LogEntry& e : {Put(3, 7, "k", "v w"), Del(4, 8, "gone"), Noop(5, 9),
                              Put(1, 1, "empty-value", "")}) {
        std::string buf;
        EncodeLogEntry(&buf, e);
        EXPECT_EQ(buf.size(), EncodedSize(e));
        std::string_view in = buf;
        LogEntry back;
        ASSERT_TRUE(DecodeLogEntry(&in, &back));
        EXPECT_EQ(back, e);
        EXPECT_TRUE(in.empty());
    }

    auto rejects = [](const LogEntry& e) {
        std::string buf;
        EncodeLogEntry(&buf, e);
        std::string_view in = buf;
        LogEntry back;
        return !DecodeLogEntry(&in, &back);
    };
    EXPECT_TRUE(rejects(LogEntry{1, 1, EntryOp::kNoop, "k", ""}));
    EXPECT_TRUE(rejects(LogEntry{1, 1, EntryOp::kPut, "", "v"}));
    EXPECT_TRUE(rejects(LogEntry{1, 1, EntryOp::kDelete, "k", "v"}));
    EXPECT_TRUE(rejects(LogEntry{1, 1, static_cast<EntryOp>(9), "k", ""}));

    std::string buf;
    EncodeLogEntry(&buf, Put(1, 1, "key", "value"));
    for (size_t cut = 0; cut < buf.size(); ++cut) {
        std::string_view in(buf.data(), cut);
        LogEntry back;
        EXPECT_FALSE(DecodeLogEntry(&in, &back)) << cut;
    }
}

TEST(RaftTypesTest, ElectionRestrictionComparesLastTermThenLength) {
    EXPECT_TRUE(CandidateLogIsUpToDate(3, 1, 2, 100));   // higher last term wins
    EXPECT_FALSE(CandidateLogIsUpToDate(2, 100, 3, 1));  // however long the log
    EXPECT_TRUE(CandidateLogIsUpToDate(3, 5, 3, 5));     // equal is enough
    EXPECT_TRUE(CandidateLogIsUpToDate(3, 6, 3, 5));
    EXPECT_FALSE(CandidateLogIsUpToDate(3, 4, 3, 5));
    EXPECT_TRUE(CandidateLogIsUpToDate(0, 0, 0, 0));  // two empty logs
}

TEST_F(RaftLogTest, EmptyLogHasIndexAndTermZero) {
    ASSERT_TRUE(Reopen().ok());
    EXPECT_EQ(log_->last_index(), 0u);
    EXPECT_EQ(log_->last_term(), 0u);
    uint64_t term = 99;
    EXPECT_TRUE(log_->TermAt(0, &term));
    EXPECT_EQ(term, 0u);
    EXPECT_FALSE(log_->TermAt(1, &term));
    EXPECT_TRUE(log_->Slice(1, 10, 1000).empty());
}

TEST_F(RaftLogTest, AppendedEntriesSurviveReopen) {
    ASSERT_TRUE(Reopen().ok());
    const std::vector<LogEntry> entries = {Noop(1, 1), Put(1, 2, "a", "1"), Put(2, 3, "b", "2"),
                                           Del(2, 4, "a")};
    ASSERT_TRUE(log_->Append({entries[0], entries[1]}).ok());
    ASSERT_TRUE(log_->Append({entries[2], entries[3]}).ok());
    EXPECT_EQ(log_->last_index(), 4u);
    EXPECT_EQ(log_->last_term(), 2u);

    RaftLogRecovery rec;
    ASSERT_TRUE(Reopen(&rec).ok());
    EXPECT_EQ(rec.entries, 4u);
    EXPECT_FALSE(rec.torn_tail);
    EXPECT_EQ(All(), entries);
    uint64_t term = 0;
    ASSERT_TRUE(log_->TermAt(3, &term));
    EXPECT_EQ(term, 2u);
    EXPECT_EQ(log_->At(2).key, "a");
}

TEST_F(RaftLogTest, AppendRejectsGapsAndDecreasingTerms) {
    ASSERT_TRUE(Reopen().ok());
    ASSERT_TRUE(log_->Append({Put(2, 1, "a", "1")}).ok());

    EXPECT_EQ(log_->Append({Put(2, 3, "b", "2")}).code(), StatusCode::kInvalidArgument);  // gap
    EXPECT_EQ(log_->Append({Put(2, 1, "b", "2")}).code(), StatusCode::kInvalidArgument);  // reuse
    EXPECT_EQ(log_->Append({Put(1, 2, "b", "2")}).code(), StatusCode::kInvalidArgument);  // term down
    EXPECT_EQ(log_->Append({Put(0, 2, "b", "2")}).code(), StatusCode::kInvalidArgument);  // term 0
    EXPECT_EQ(log_->Append({Put(2, 2, "b", "2"), Put(3, 4, "c", "3")}).code(),
              StatusCode::kInvalidArgument);  // gap inside the batch
    EXPECT_EQ(log_->Append({Put(3, 2, "b", "2"), Put(2, 3, "c", "3")}).code(),
              StatusCode::kInvalidArgument);  // term down inside the batch

    // A rejected batch writes nothing.
    EXPECT_EQ(log_->last_index(), 1u);
    ASSERT_TRUE(Reopen().ok());
    EXPECT_EQ(log_->last_index(), 1u);
}

TEST_F(RaftLogTest, TruncateFromDropsSuffixDurably) {
    ASSERT_TRUE(Reopen().ok());
    ASSERT_TRUE(log_->Append({Put(1, 1, "a", "1"), Put(1, 2, "b", "2"), Put(2, 3, "c", "3"),
                              Put(2, 4, "d", "4")})
                    .ok());
    const uint64_t two_entries = EncodeLogFrame(Put(1, 1, "a", "1")).size() +
                                 EncodeLogFrame(Put(1, 2, "b", "2")).size();

    ASSERT_TRUE(log_->TruncateFrom(3).ok());
    EXPECT_EQ(log_->last_index(), 2u);
    EXPECT_EQ(log_->last_term(), 1u);
    EXPECT_EQ(FileSize(), two_entries);
    EXPECT_TRUE(log_->TruncateFrom(10).ok());  // past the end: nothing to do
    EXPECT_EQ(log_->TruncateFrom(0).code(), StatusCode::kInvalidArgument);

    // New entries, even from a lower term than the dropped ones, follow on.
    ASSERT_TRUE(log_->Append({Put(1, 3, "x", "9")}).ok());
    ASSERT_TRUE(Reopen().ok());
    EXPECT_EQ(All(), (std::vector<LogEntry>{Put(1, 1, "a", "1"), Put(1, 2, "b", "2"),
                                            Put(1, 3, "x", "9")}));

    ASSERT_TRUE(log_->TruncateFrom(1).ok());
    EXPECT_EQ(FileSize(), 0u);
    ASSERT_TRUE(Reopen().ok());
    EXPECT_EQ(log_->last_index(), 0u);
}

TEST_F(RaftLogTest, SliceRespectsEntryAndByteLimits) {
    ASSERT_TRUE(Reopen().ok());
    std::vector<LogEntry> entries;
    for (uint64_t i = 1; i <= 10; ++i) {
        entries.push_back(Put(1, i, "k" + std::to_string(i), std::string(100, 'v')));
    }
    ASSERT_TRUE(log_->Append(entries).ok());
    const size_t one = EncodedSize(entries[0]);

    EXPECT_EQ(log_->Slice(4, 3, SIZE_MAX).size(), 3u);
    EXPECT_EQ(log_->Slice(4, 3, SIZE_MAX).front().index, 4u);
    EXPECT_EQ(log_->Slice(9, 100, SIZE_MAX).size(), 2u);
    EXPECT_EQ(log_->Slice(11, 100, SIZE_MAX).size(), 0u);
    EXPECT_EQ(log_->Slice(1, 100, one * 3).size(), 3u);
    EXPECT_EQ(log_->Slice(1, 100, one * 3 - 1).size(), 2u);
    EXPECT_EQ(log_->Slice(1, 100, 1).size(), 1u);  // always at least one
}

TEST_F(RaftLogTest, AppendFromLeaderSkipsMatchingAndReplacesConflicts) {
    ASSERT_TRUE(Reopen().ok());
    ASSERT_TRUE(log_->Append({Put(1, 1, "a", "1"), Put(1, 2, "b", "2"), Put(2, 3, "stale", "x"),
                              Put(2, 4, "stale", "y")})
                    .ok());
    uint64_t last_new = 0;

    // Entirely already present: nothing changes, nothing is truncated.
    ASSERT_TRUE(log_->AppendFromLeader({Put(1, 2, "b", "2")}, &last_new).ok());
    EXPECT_EQ(last_new, 2u);
    EXPECT_EQ(log_->last_index(), 4u);

    // Entry 3 conflicts (term 3 vs 2): drop 3..4, then append the leader's.
    ASSERT_TRUE(
        log_->AppendFromLeader({Put(1, 2, "b", "2"), Put(3, 3, "c", "3")}, &last_new).ok());
    EXPECT_EQ(last_new, 3u);
    ASSERT_TRUE(Reopen().ok());
    EXPECT_EQ(All(), (std::vector<LogEntry>{Put(1, 1, "a", "1"), Put(1, 2, "b", "2"),
                                            Put(3, 3, "c", "3")}));

    // Plain extension.
    ASSERT_TRUE(log_->AppendFromLeader({Put(3, 4, "d", "4"), Put(3, 5, "e", "5")}, &last_new).ok());
    EXPECT_EQ(log_->last_index(), 5u);

    // A delayed, older request covering a prefix must not erase entry 5.
    ASSERT_TRUE(log_->AppendFromLeader({Put(3, 4, "d", "4")}, &last_new).ok());
    EXPECT_EQ(last_new, 4u);
    EXPECT_EQ(log_->last_index(), 5u);

    // Entries that would leave a gap are refused.
    EXPECT_EQ(log_->AppendFromLeader({Put(3, 7, "g", "7")}, &last_new).code(),
              StatusCode::kInvalidArgument);
    EXPECT_TRUE(log_->AppendFromLeader({}, &last_new).ok());
    EXPECT_EQ(last_new, 0u);
}

TEST_F(RaftLogTest, TornTailIsCutOffOnOpen) {
    ASSERT_TRUE(Reopen().ok());
    ASSERT_TRUE(log_->Append({Put(1, 1, "a", "1"), Put(1, 2, "b", "2")}).ok());
    const uint64_t good = FileSize();
    const std::string next = EncodeLogFrame(Put(1, 3, "c", "3"));
    log_.reset();

    for (size_t cut : {size_t{1}, size_t{5}, next.size() - 1}) {
        fs::resize_file(path_, good);
        AppendRaw(next.substr(0, cut));
        RaftLogRecovery rec;
        ASSERT_TRUE(Reopen(&rec).ok()) << cut;
        EXPECT_TRUE(rec.torn_tail) << cut;
        EXPECT_EQ(log_->last_index(), 2u);
        EXPECT_EQ(FileSize(), good);
        // Appending after recovery lands right after the last good entry.
        ASSERT_TRUE(log_->Append({Put(1, 3, "c", "3")}).ok());
        log_.reset();
        fs::resize_file(path_, good);
    }
}

TEST_F(RaftLogTest, DamageBeforeTheEndIsCorruption) {
    ASSERT_TRUE(Reopen().ok());
    ASSERT_TRUE(log_->Append({Put(1, 1, "a", "1"), Put(1, 2, "b", "2")}).ok());
    log_.reset();
    FlipByte(20);  // inside the first entry
    EXPECT_EQ(Reopen().code(), StatusCode::kCorruption);
}

TEST_F(RaftLogTest, OutOfOrderEntriesOnDiskAreCorruption) {
    AppendRaw(EncodeLogFrame(Put(2, 1, "a", "1")));
    AppendRaw(EncodeLogFrame(Put(1, 2, "b", "2")));  // term went down
    EXPECT_EQ(Reopen().code(), StatusCode::kCorruption);

    fs::remove(path_);
    AppendRaw(EncodeLogFrame(Put(1, 1, "a", "1")));
    AppendRaw(EncodeLogFrame(Put(1, 3, "b", "2")));  // index gap
    EXPECT_EQ(Reopen().code(), StatusCode::kCorruption);
}

TEST_F(RaftLogTest, HardStateRoundTripsAndDetectsDamage) {
    const std::string path = (dir_ / "raft_state").string();
    HardState s;
    ASSERT_TRUE(LoadHardState(path, &s).ok());  // missing: fresh node
    EXPECT_EQ(s, HardState{});

    ASSERT_TRUE(SaveHardState(path, HardState{7, 3}).ok());
    ASSERT_TRUE(SaveHardState(path, HardState{8, kNoNode}).ok());
    ASSERT_TRUE(LoadHardState(path, &s).ok());
    EXPECT_EQ(s, (HardState{8, kNoNode}));
    EXPECT_FALSE(fs::exists(path + ".tmp"));

    {
        std::fstream f(path, std::ios::in | std::ios::out | std::ios::binary);
        f.seekp(9);
        f.put('\x55');
    }
    EXPECT_EQ(LoadHardState(path, &s).code(), StatusCode::kCorruption);

    fs::resize_file(path, 10);
    EXPECT_EQ(LoadHardState(path, &s).code(), StatusCode::kCorruption);
}

}  // namespace
}  // namespace distkv::raft

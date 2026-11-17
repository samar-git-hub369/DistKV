#include "storage/flush.h"

#include <gtest/gtest.h>

#include <filesystem>
#include <random>
#include <string>
#include <vector>

namespace distkv {
namespace {

namespace fs = std::filesystem;

constexpr size_t kLarge = 1 << 30;

class FlushTest : public ::testing::Test {
  protected:
    void SetUp() override {
        std::random_device rd;
        dir_ = fs::temp_directory_path() / ("distkv_flush_test_" + std::to_string(rd()));
        fs::create_directories(dir_);
        sst_ = (dir_ / "000001.sst").string();
        wal_ = (dir_ / "wal.log").string();
    }
    void TearDown() override {
        std::error_code ec;
        fs::remove_all(dir_, ec);
    }

    fs::path dir_;
    std::string sst_;
    std::string wal_;
};

TEST_F(FlushTest, TableAnswersExactlyLikeTheMemTable) {
    MemTable mem(kLarge);
    uint64_t seq = 1;
    for (int i = 0; i < 1000; ++i) {
        mem.Put(seq++, "k" + std::to_string(i), "v" + std::to_string(i));
    }
    for (int i = 0; i < 1000; i += 10) {
        mem.Delete(seq++, "k" + std::to_string(i));
    }
    mem.Put(seq++, "k5", "overwritten");
    mem.Delete(seq++, "never-written");

    FlushStats stats;
    ASSERT_TRUE(FlushMemTable(mem, sst_, SSTableOptions{512}, &stats).ok());
    EXPECT_EQ(stats.entries, mem.EntryCount());
    EXPECT_EQ(stats.tombstones, 101u);
    EXPECT_EQ(stats.max_seq, seq - 1);
    EXPECT_EQ(stats.file_bytes, fs::file_size(sst_));

    std::unique_ptr<SSTableReader> table;
    ASSERT_TRUE(SSTableReader::Open(sst_, &table).ok());
    EXPECT_EQ(table->entry_count(), mem.EntryCount());
    EXPECT_EQ(table->max_seq(), mem.MaxSeq());

    // Every key the MemTable knows gets the same answer and seq from the table.
    mem.ForEach([&](std::string_view key, const MemTableEntry& e) {
        LookupResult res;
        std::string v;
        uint64_t got_seq = 0;
        ASSERT_TRUE(table->Get(key, &res, &v, &got_seq).ok());
        EXPECT_EQ(got_seq, e.seq) << key;
        if (e.tombstone) {
            EXPECT_EQ(res, LookupResult::kDeleted) << key;
        } else {
            ASSERT_EQ(res, LookupResult::kFound) << key;
            EXPECT_EQ(v, e.value);
        }
    });
}

TEST_F(FlushTest, ScanOrderMatchesMemTableOrder) {
    MemTable mem(kLarge);
    std::mt19937 rng(3);
    for (uint64_t seq = 1; seq <= 500; ++seq) {
        std::string key(1 + rng() % 8, '\0');
        for (auto& c : key) c = static_cast<char>(rng() % 256);
        mem.Put(seq, key, std::to_string(seq));
    }

    FlushStats stats;
    ASSERT_TRUE(FlushMemTable(mem, sst_, SSTableOptions{256}, &stats).ok());
    std::unique_ptr<SSTableReader> table;
    ASSERT_TRUE(SSTableReader::Open(sst_, &table).ok());

    std::vector<std::string> mem_keys;
    mem.ForEach([&](std::string_view k, const MemTableEntry&) { mem_keys.emplace_back(k); });

    std::vector<std::string> table_keys;
    SSTableIterator it(table.get());
    for (it.SeekToFirst(); it.Valid(); it.Next()) {
        table_keys.emplace_back(it.key());
    }
    ASSERT_TRUE(it.status().ok());
    EXPECT_EQ(table_keys, mem_keys);
}

TEST_F(FlushTest, EmptyMemTableGivesValidEmptyTable) {
    MemTable mem(kLarge);
    FlushStats stats;
    ASSERT_TRUE(FlushMemTable(mem, sst_, SSTableOptions{}, &stats).ok());
    EXPECT_EQ(stats.entries, 0u);

    std::unique_ptr<SSTableReader> table;
    ASSERT_TRUE(SSTableReader::Open(sst_, &table).ok());
    EXPECT_EQ(table->entry_count(), 0u);
}

TEST_F(FlushTest, FailedFlushLeavesNothingBehind) {
    MemTable mem(kLarge);
    mem.Put(1, "k", "v");
    const std::string bad = (dir_ / "no-such-dir" / "000001.sst").string();

    FlushStats stats;
    EXPECT_FALSE(FlushMemTable(mem, bad, SSTableOptions{}, &stats).ok());
    EXPECT_FALSE(fs::exists(bad));
    EXPECT_EQ(stats.entries, 0u);
}

TEST_F(FlushTest, WalRecoveryThenFlushPreservesEveryWrite) {
    // Simulates a restart: writes go to the WAL, the process dies, recovery
    // rebuilds the MemTable, and the MemTable is flushed to an SSTable.
    {
        std::unique_ptr<WalWriter> w;
        ASSERT_TRUE(WalWriter::Open(wal_, WalSyncMode::kNone, &w).ok());
        for (uint64_t seq = 1; seq <= 200; ++seq) {
            const std::string key = "user:" + std::to_string(seq % 50);
            if (seq % 9 == 0) {
                ASSERT_TRUE(w->Append({seq, WalOp::kDelete, key, ""}).ok());
            } else {
                ASSERT_TRUE(w->Append({seq, WalOp::kPut, key, std::to_string(seq)}).ok());
            }
        }
    }

    MemTable mem(kLarge);
    WalReplayStats replay;
    ASSERT_TRUE(RecoverMemTable(wal_, &mem, &replay).ok());
    ASSERT_EQ(replay.records, 200u);

    FlushStats stats;
    ASSERT_TRUE(FlushMemTable(mem, sst_, SSTableOptions{}, &stats).ok());
    EXPECT_EQ(stats.max_seq, 200u);
    EXPECT_EQ(stats.entries, 50u);

    std::unique_ptr<SSTableReader> table;
    ASSERT_TRUE(SSTableReader::Open(sst_, &table).ok());
    // The last write to user:K had seq = largest seq <= 200 with seq % 50 == K.
    for (uint64_t k = 0; k < 50; ++k) {
        uint64_t last = 200 - ((200 - k) % 50);
        LookupResult res;
        std::string v;
        uint64_t seq = 0;
        ASSERT_TRUE(table->Get("user:" + std::to_string(k), &res, &v, &seq).ok());
        EXPECT_EQ(seq, last) << k;
        if (last % 9 == 0) {
            EXPECT_EQ(res, LookupResult::kDeleted) << k;
        } else {
            ASSERT_EQ(res, LookupResult::kFound) << k;
            EXPECT_EQ(v, std::to_string(last));
        }
    }
}

}  // namespace
}  // namespace distkv

#include "storage/memtable.h"

#include <gtest/gtest.h>

#include <atomic>
#include <filesystem>
#include <random>
#include <string>
#include <thread>
#include <vector>

namespace distkv {
namespace {

namespace fs = std::filesystem;

constexpr size_t kLarge = 1 << 30;  // threshold that never triggers

TEST(MemTableTest, EmptyTableKnowsNothing) {
    MemTable t(kLarge);
    std::string v = "untouched";
    EXPECT_EQ(t.Get("missing", &v), LookupResult::kNotPresent);
    EXPECT_EQ(v, "untouched");
    EXPECT_EQ(t.EntryCount(), 0u);
    EXPECT_EQ(t.ApproximateBytes(), 0u);
    EXPECT_EQ(t.MaxSeq(), 0u);
}

TEST(MemTableTest, PutThenGet) {
    MemTable t(kLarge);
    t.Put(1, "city", "Jaipur");
    std::string v;
    ASSERT_EQ(t.Get("city", &v), LookupResult::kFound);
    EXPECT_EQ(v, "Jaipur");
    EXPECT_EQ(t.MaxSeq(), 1u);
}

TEST(MemTableTest, NewerWriteOverwrites) {
    MemTable t(kLarge);
    t.Put(1, "k", "old");
    t.Put(2, "k", "new");
    std::string v;
    ASSERT_EQ(t.Get("k", &v), LookupResult::kFound);
    EXPECT_EQ(v, "new");
    EXPECT_EQ(t.EntryCount(), 1u);
}

TEST(MemTableTest, OlderOrRepeatedWriteIsIgnored) {
    MemTable t(kLarge);
    t.Put(5, "k", "five");
    t.Put(3, "k", "three");
    t.Put(5, "k", "five-again");
    t.Delete(4, "k");
    std::string v;
    ASSERT_EQ(t.Get("k", &v), LookupResult::kFound);
    EXPECT_EQ(v, "five");
    EXPECT_EQ(t.MaxSeq(), 5u);
}

TEST(MemTableTest, DeleteLeavesTombstone) {
    MemTable t(kLarge);
    t.Put(1, "k", "v");
    t.Delete(2, "k");
    std::string v = "untouched";
    EXPECT_EQ(t.Get("k", &v), LookupResult::kDeleted);
    EXPECT_EQ(v, "untouched");
    EXPECT_EQ(t.EntryCount(), 1u);  // the tombstone is still an entry
}

TEST(MemTableTest, DeleteOfUnknownKeyStillRecordsTombstone) {
    // The key may live in an older SSTable; the tombstone must hide it.
    MemTable t(kLarge);
    t.Delete(1, "only-on-disk");
    std::string v;
    EXPECT_EQ(t.Get("only-on-disk", &v), LookupResult::kDeleted);
}

TEST(MemTableTest, PutAfterDeleteRevivesKey) {
    MemTable t(kLarge);
    t.Put(1, "k", "a");
    t.Delete(2, "k");
    t.Put(3, "k", "b");
    std::string v;
    ASSERT_EQ(t.Get("k", &v), LookupResult::kFound);
    EXPECT_EQ(v, "b");
}

TEST(MemTableTest, ByteAccountingFollowsOverwrites) {
    MemTable t(kLarge);
    t.Put(1, "key", "12345");
    EXPECT_EQ(t.ApproximateBytes(), 3 + 5 + MemTable::kEntryOverhead);

    t.Put(2, "key", "1");
    EXPECT_EQ(t.ApproximateBytes(), 3 + 1 + MemTable::kEntryOverhead);

    t.Delete(3, "key");
    EXPECT_EQ(t.ApproximateBytes(), 3 + MemTable::kEntryOverhead);

    t.Put(4, "other", "xy");
    EXPECT_EQ(t.ApproximateBytes(),
              (3 + MemTable::kEntryOverhead) + (5 + 2 + MemTable::kEntryOverhead));
}

TEST(MemTableTest, ShouldFlushAtThreshold) {
    const size_t threshold = 4 * (1 + 10 + MemTable::kEntryOverhead);
    MemTable t(threshold);
    for (uint64_t i = 0; i < 3; ++i) {
        t.Put(i + 1, std::string(1, static_cast<char>('a' + i)), std::string(10, 'x'));
        EXPECT_FALSE(t.ShouldFlush()) << "after " << i + 1 << " entries";
    }
    t.Put(4, "d", std::string(10, 'x'));
    EXPECT_TRUE(t.ShouldFlush());
}

TEST(MemTableTest, ForEachVisitsKeysInUnsignedByteOrder) {
    MemTable t(kLarge);
    t.Put(1, "b", "2");
    t.Put(2, "\xff", "high");  // must sort after ASCII, not before
    t.Put(3, "a", "1");
    t.Delete(4, "ab");
    t.Put(5, std::string("a\0", 2), "nul");

    std::vector<std::string> keys;
    std::vector<bool> tombstones;
    t.ForEach([&](std::string_view key, const MemTableEntry& e) {
        keys.emplace_back(key);
        tombstones.push_back(e.tombstone);
    });

    const std::vector<std::string> want = {"a", std::string("a\0", 2), "ab", "b", "\xff"};
    EXPECT_EQ(keys, want);
    EXPECT_EQ(tombstones, (std::vector<bool>{false, false, true, false, false}));
}

TEST(MemTableTest, ApplyMapsWalRecords) {
    MemTable t(kLarge);
    t.Apply(WalRecord{1, WalOp::kPut, "k", "v"});
    t.Apply(WalRecord{2, WalOp::kDelete, "k", ""});
    std::string v;
    EXPECT_EQ(t.Get("k", &v), LookupResult::kDeleted);
}

TEST(MemTableTest, ConcurrentReadersAndWritersSeeConsistentValues) {
    MemTable t(kLarge);
    constexpr int kWriters = 4;
    constexpr int kPerWriter = 2000;
    std::atomic<uint64_t> next_seq{1};
    std::atomic<bool> bad_read{false};
    std::atomic<bool> done{false};

    std::vector<std::thread> readers;
    for (int r = 0; r < 4; ++r) {
        readers.emplace_back([&] {
            std::string v;
            while (!done.load()) {
                for (int w = 0; w < kWriters; ++w) {
                    const std::string key = "w" + std::to_string(w);
                    // Each writer only ever stores its own key as the value prefix.
                    if (t.Get(key, &v) == LookupResult::kFound && v.rfind(key + ":", 0) != 0) {
                        bad_read = true;
                    }
                }
            }
        });
    }

    std::vector<std::thread> writers;
    for (int w = 0; w < kWriters; ++w) {
        writers.emplace_back([&, w] {
            const std::string key = "w" + std::to_string(w);
            for (int i = 0; i < kPerWriter; ++i) {
                t.Put(next_seq.fetch_add(1), key, key + ":" + std::to_string(i));
            }
        });
    }
    for (auto& th : writers) th.join();
    done = true;
    for (auto& th : readers) th.join();

    EXPECT_FALSE(bad_read.load());
    EXPECT_EQ(t.EntryCount(), static_cast<size_t>(kWriters));
    EXPECT_EQ(t.MaxSeq(), static_cast<uint64_t>(kWriters * kPerWriter));
    for (int w = 0; w < kWriters; ++w) {
        std::string v;
        const std::string key = "w" + std::to_string(w);
        ASSERT_EQ(t.Get(key, &v), LookupResult::kFound);
        EXPECT_EQ(v, key + ":" + std::to_string(kPerWriter - 1));
    }
}

class MemTableRecoveryTest : public ::testing::Test {
  protected:
    void SetUp() override {
        std::random_device rd;
        dir_ = fs::temp_directory_path() / ("distkv_memtable_test_" + std::to_string(rd()));
        fs::create_directories(dir_);
        wal_ = (dir_ / "wal.log").string();
    }
    void TearDown() override {
        std::error_code ec;
        fs::remove_all(dir_, ec);
    }

    fs::path dir_;
    std::string wal_;
};

TEST_F(MemTableRecoveryTest, RebuildsStateFromWal) {
    {
        std::unique_ptr<WalWriter> w;
        ASSERT_TRUE(WalWriter::Open(wal_, WalSyncMode::kNone, &w).ok());
        ASSERT_TRUE(w->Append({1, WalOp::kPut, "a", "1"}).ok());
        ASSERT_TRUE(w->Append({2, WalOp::kPut, "b", "2"}).ok());
        ASSERT_TRUE(w->Append({3, WalOp::kDelete, "a", ""}).ok());
        ASSERT_TRUE(w->Append({4, WalOp::kPut, "b", "22"}).ok());
    }

    MemTable t(kLarge);
    WalReplayStats stats;
    ASSERT_TRUE(RecoverMemTable(wal_, &t, &stats).ok());
    EXPECT_EQ(stats.records, 4u);

    std::string v;
    EXPECT_EQ(t.Get("a", &v), LookupResult::kDeleted);
    ASSERT_EQ(t.Get("b", &v), LookupResult::kFound);
    EXPECT_EQ(v, "22");
    EXPECT_EQ(t.MaxSeq(), 4u);
}

TEST_F(MemTableRecoveryTest, ReplayingTwiceIsHarmless) {
    {
        std::unique_ptr<WalWriter> w;
        ASSERT_TRUE(WalWriter::Open(wal_, WalSyncMode::kNone, &w).ok());
        ASSERT_TRUE(w->Append({1, WalOp::kPut, "k", "v1"}).ok());
        ASSERT_TRUE(w->Append({2, WalOp::kPut, "k", "v2"}).ok());
    }
    MemTable t(kLarge);
    WalReplayStats stats;
    ASSERT_TRUE(RecoverMemTable(wal_, &t, &stats).ok());
    const size_t bytes = t.ApproximateBytes();
    ASSERT_TRUE(RecoverMemTable(wal_, &t, &stats).ok());

    std::string v;
    ASSERT_EQ(t.Get("k", &v), LookupResult::kFound);
    EXPECT_EQ(v, "v2");
    EXPECT_EQ(t.ApproximateBytes(), bytes);
}

TEST_F(MemTableRecoveryTest, DropsTornTailAndKeepsEarlierWrites) {
    {
        std::unique_ptr<WalWriter> w;
        ASSERT_TRUE(WalWriter::Open(wal_, WalSyncMode::kNone, &w).ok());
        ASSERT_TRUE(w->Append({1, WalOp::kPut, "kept", "yes"}).ok());
        ASSERT_TRUE(w->Append({2, WalOp::kPut, "lost", "in-crash"}).ok());
    }
    fs::resize_file(wal_, fs::file_size(wal_) - 2);

    MemTable t(kLarge);
    WalReplayStats stats;
    ASSERT_TRUE(RecoverMemTable(wal_, &t, &stats).ok());
    EXPECT_TRUE(stats.torn_tail);
    EXPECT_EQ(fs::file_size(wal_), stats.valid_bytes);

    std::string v;
    EXPECT_EQ(t.Get("kept", &v), LookupResult::kFound);
    EXPECT_EQ(t.Get("lost", &v), LookupResult::kNotPresent);
}

TEST_F(MemTableRecoveryTest, MissingWalGivesEmptyTable) {
    MemTable t(kLarge);
    WalReplayStats stats;
    ASSERT_TRUE(RecoverMemTable(wal_, &t, &stats).ok());
    EXPECT_EQ(t.EntryCount(), 0u);
}

}  // namespace
}  // namespace distkv

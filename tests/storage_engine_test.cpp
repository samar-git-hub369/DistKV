#include "storage/storage_engine.h"

#include <gtest/gtest.h>

#include <atomic>
#include <filesystem>
#include <fstream>
#include <map>
#include <random>
#include <string>
#include <thread>
#include <vector>

namespace distkv {
namespace {

namespace fs = std::filesystem;

class StorageEngineTest : public ::testing::Test {
  protected:
    void SetUp() override {
        std::random_device rd;
        dir_ = (fs::temp_directory_path() / ("distkv_engine_test_" + std::to_string(rd()))).string();
        options_.wal_sync = WalSyncMode::kNone;  // keep the tests fast
    }
    void TearDown() override {
        engine_.reset();
        std::error_code ec;
        fs::remove_all(dir_, ec);
    }

    void OpenEngine() {
        engine_.reset();
        const Status s = StorageEngine::Open(dir_, options_, &engine_);
        ASSERT_TRUE(s.ok()) << s.ToString();
    }

    std::string GetOrMissing(std::string_view key) {
        std::string v;
        const Status s = engine_->Get(key, &v);
        if (s.code() == StatusCode::kNotFound) return "<missing>";
        EXPECT_TRUE(s.ok()) << s.ToString();
        return v;
    }

    size_t CountFiles(std::string_view suffix) {
        size_t n = 0;
        for (const auto& e : fs::directory_iterator(dir_)) {
            const std::string name = e.path().filename().string();
            if (name.size() >= suffix.size() &&
                name.compare(name.size() - suffix.size(), suffix.size(), suffix) == 0) {
                ++n;
            }
        }
        return n;
    }

    std::string dir_;
    EngineOptions options_;
    std::unique_ptr<StorageEngine> engine_;
};

TEST_F(StorageEngineTest, PutGetDeleteOverwrite) {
    OpenEngine();
    EXPECT_EQ(GetOrMissing("city"), "<missing>");
    ASSERT_TRUE(engine_->Put("city", "Jaipur").ok());
    EXPECT_EQ(GetOrMissing("city"), "Jaipur");
    ASSERT_TRUE(engine_->Put("city", "Pune").ok());
    EXPECT_EQ(GetOrMissing("city"), "Pune");
    ASSERT_TRUE(engine_->Delete("city").ok());
    EXPECT_EQ(GetOrMissing("city"), "<missing>");
    EXPECT_EQ(engine_->stats().last_seq, 3u);
}

TEST_F(StorageEngineTest, RejectsEmptyKey) {
    OpenEngine();
    std::string v;
    EXPECT_EQ(engine_->Put("", "v").code(), StatusCode::kInvalidArgument);
    EXPECT_EQ(engine_->Delete("").code(), StatusCode::kInvalidArgument);
    EXPECT_EQ(engine_->Get("", &v).code(), StatusCode::kInvalidArgument);
}

TEST_F(StorageEngineTest, WritesSurviveReopenThroughTheWal) {
    OpenEngine();
    ASSERT_TRUE(engine_->Put("a", "1").ok());
    ASSERT_TRUE(engine_->Put("b", "2").ok());
    ASSERT_TRUE(engine_->Delete("a").ok());
    EXPECT_EQ(engine_->stats().sstables, 0u);

    OpenEngine();  // closes the old engine without flushing
    EXPECT_EQ(GetOrMissing("a"), "<missing>");
    EXPECT_EQ(GetOrMissing("b"), "2");
    // Recovery flushed the replayed writes and started a fresh WAL.
    EXPECT_EQ(engine_->stats().sstables, 1u);
    EXPECT_EQ(CountFiles(".log"), 1u);
    EXPECT_EQ(engine_->stats().last_seq, 3u);

    // Sequence numbers continue where they left off.
    ASSERT_TRUE(engine_->Put("c", "3").ok());
    EXPECT_EQ(engine_->stats().last_seq, 4u);
}

TEST_F(StorageEngineTest, ReadsFallThroughLayersNewestFirst) {
    OpenEngine();
    ASSERT_TRUE(engine_->Put("k", "in-table-1").ok());
    ASSERT_TRUE(engine_->Put("only-old", "old").ok());
    ASSERT_TRUE(engine_->Flush().ok());
    ASSERT_TRUE(engine_->Put("k", "in-table-2").ok());
    ASSERT_TRUE(engine_->Flush().ok());
    EXPECT_EQ(engine_->stats().sstables, 2u);

    EXPECT_EQ(GetOrMissing("k"), "in-table-2");    // newer table wins
    EXPECT_EQ(GetOrMissing("only-old"), "old");    // found in the older table

    ASSERT_TRUE(engine_->Put("k", "in-memtable").ok());
    EXPECT_EQ(GetOrMissing("k"), "in-memtable");   // MemTable beats tables
}

TEST_F(StorageEngineTest, TombstoneHidesValueInOlderTable) {
    OpenEngine();
    ASSERT_TRUE(engine_->Put("k", "v").ok());
    ASSERT_TRUE(engine_->Flush().ok());

    ASSERT_TRUE(engine_->Delete("k").ok());
    EXPECT_EQ(GetOrMissing("k"), "<missing>");  // tombstone in MemTable

    ASSERT_TRUE(engine_->Flush().ok());
    EXPECT_EQ(GetOrMissing("k"), "<missing>");  // tombstone now in the newer table

    OpenEngine();
    EXPECT_EQ(GetOrMissing("k"), "<missing>");
}

TEST_F(StorageEngineTest, FlushOfEmptyMemTableIsANoOp) {
    OpenEngine();
    ASSERT_TRUE(engine_->Flush().ok());
    EXPECT_EQ(engine_->stats().sstables, 0u);
    EXPECT_EQ(engine_->stats().flushes, 0u);
}

TEST_F(StorageEngineTest, FullMemTableFlushesInTheBackground) {
    options_.memtable_bytes = 16 * 1024;
    OpenEngine();
    for (int i = 0; i < 2000; ++i) {
        ASSERT_TRUE(engine_->Put("key" + std::to_string(i), std::string(100, 'x')).ok());
    }
    ASSERT_TRUE(engine_->Flush().ok());
    const EngineStats st = engine_->stats();
    EXPECT_GT(st.sstables, 5u);
    EXPECT_FALSE(st.flush_in_progress);
    // Only the active MemTable's WAL remains; flushed WALs were deleted.
    EXPECT_EQ(CountFiles(".log"), 1u);
    EXPECT_EQ(CountFiles(".sst"), st.sstables);

    for (int i = 0; i < 2000; ++i) {
        ASSERT_EQ(GetOrMissing("key" + std::to_string(i)), std::string(100, 'x')) << i;
    }
    OpenEngine();
    for (int i = 0; i < 2000; i += 37) {
        ASSERT_EQ(GetOrMissing("key" + std::to_string(i)), std::string(100, 'x')) << i;
    }
}

TEST_F(StorageEngineTest, MatchesReferenceMapAcrossFlushesAndReopens) {
    options_.memtable_bytes = 8 * 1024;
    options_.sstable.block_size = 512;
    OpenEngine();

    std::mt19937 rng(11);
    std::map<std::string, std::string> ref;
    for (int round = 0; round < 3; ++round) {
        for (int i = 0; i < 3000; ++i) {
            const std::string key = "k" + std::to_string(rng() % 500);
            if (rng() % 4 == 0) {
                ASSERT_TRUE(engine_->Delete(key).ok());
                ref.erase(key);
            } else {
                const std::string value = std::to_string(rng());
                ASSERT_TRUE(engine_->Put(key, value).ok());
                ref[key] = value;
            }
        }
        OpenEngine();
        for (int k = 0; k < 500; ++k) {
            const std::string key = "k" + std::to_string(k);
            const auto it = ref.find(key);
            ASSERT_EQ(GetOrMissing(key), it == ref.end() ? "<missing>" : it->second)
                << key << " round " << round;
        }
    }
}

TEST_F(StorageEngineTest, StaleWalLeftAfterFlushIsIgnoredOnRecovery) {
    // Simulates a crash after a flush published its SSTable but before it
    // deleted the WAL, followed by a newer table overwriting the key.
    fs::create_directories(dir_);
    {
        std::unique_ptr<WalWriter> w;
        ASSERT_TRUE(WalWriter::Open(dir_ + "/wal-000001.log", WalSyncMode::kNone, &w).ok());
        ASSERT_TRUE(w->Append({1, WalOp::kPut, "k", "stale"}).ok());
    }
    {
        std::unique_ptr<SSTableWriter> w;
        ASSERT_TRUE(SSTableWriter::Create(dir_ + "/000002.sst", SSTableOptions{}, &w).ok());
        ASSERT_TRUE(w->Add("k", 1, EntryType::kValue, "stale").ok());
        ASSERT_TRUE(w->Finish().ok());
    }
    {
        std::unique_ptr<SSTableWriter> w;
        ASSERT_TRUE(SSTableWriter::Create(dir_ + "/000003.sst", SSTableOptions{}, &w).ok());
        ASSERT_TRUE(w->Add("k", 2, EntryType::kValue, "fresh").ok());
        ASSERT_TRUE(w->Finish().ok());
    }

    OpenEngine();
    EXPECT_EQ(GetOrMissing("k"), "fresh");
    EXPECT_FALSE(fs::exists(dir_ + "/wal-000001.log"));
    EXPECT_EQ(engine_->stats().sstables, 2u);  // nothing new needed flushing
    EXPECT_EQ(engine_->stats().last_seq, 2u);
}

TEST_F(StorageEngineTest, LeftoverTempFilesAreRemovedOnOpen) {
    fs::create_directories(dir_);
    std::ofstream(dir_ + "/000007.sst.tmp") << "half-written";
    OpenEngine();
    EXPECT_FALSE(fs::exists(dir_ + "/000007.sst.tmp"));
    // File numbering continues past the highest number seen.
    ASSERT_TRUE(engine_->Put("k", "v").ok());
    ASSERT_TRUE(engine_->Flush().ok());
    EXPECT_EQ(CountFiles(".sst"), 1u);
}

TEST_F(StorageEngineTest, TornWalTailLosesOnlyTheLastWrite) {
    OpenEngine();
    ASSERT_TRUE(engine_->Put("first", "1").ok());
    ASSERT_TRUE(engine_->Put("second", "2").ok());
    engine_.reset();

    for (const auto& e : fs::directory_iterator(dir_)) {
        if (e.path().extension() == ".log") {
            fs::resize_file(e.path(), fs::file_size(e.path()) - 1);
        }
    }
    OpenEngine();
    EXPECT_EQ(GetOrMissing("first"), "1");
    EXPECT_EQ(GetOrMissing("second"), "<missing>");
}

TEST_F(StorageEngineTest, CorruptWalRefusesToOpen) {
    OpenEngine();
    ASSERT_TRUE(engine_->Put("a", "1").ok());
    ASSERT_TRUE(engine_->Put("b", "2").ok());
    engine_.reset();

    for (const auto& e : fs::directory_iterator(dir_)) {
        if (e.path().extension() == ".log") {
            std::fstream f(e.path(), std::ios::in | std::ios::out | std::ios::binary);
            f.seekp(10);
            f.put('\x5a');
        }
    }
    std::unique_ptr<StorageEngine> e;
    EXPECT_EQ(StorageEngine::Open(dir_, options_, &e).code(), StatusCode::kCorruption);
}

TEST_F(StorageEngineTest, ConcurrentWritersAndReadersWithBackgroundFlushes) {
    options_.memtable_bytes = 32 * 1024;
    OpenEngine();

    constexpr int kWriters = 4;
    constexpr int kPerWriter = 3000;
    std::atomic<bool> done{false};
    std::atomic<int> bad{0};

    std::vector<std::thread> readers;
    for (int r = 0; r < 3; ++r) {
        readers.emplace_back([&] {
            std::string v;
            while (!done.load()) {
                for (int w = 0; w < kWriters; ++w) {
                    const std::string key = "w" + std::to_string(w) + ":0";
                    const Status s = engine_->Get(key, &v);
                    // Once written, key 0 of each writer is never deleted.
                    if (s.ok() && v != "0") ++bad;
                    if (!s.ok() && s.code() != StatusCode::kNotFound) ++bad;
                }
            }
        });
    }
    std::vector<std::thread> writers;
    for (int w = 0; w < kWriters; ++w) {
        writers.emplace_back([&, w] {
            for (int i = 0; i < kPerWriter; ++i) {
                const std::string key = "w" + std::to_string(w) + ":" + std::to_string(i);
                if (!engine_->Put(key, std::to_string(i)).ok()) ++bad;
            }
        });
    }
    for (auto& t : writers) t.join();
    done = true;
    for (auto& t : readers) t.join();

    EXPECT_EQ(bad.load(), 0);
    EXPECT_GT(engine_->stats().flushes, 2u);
    EXPECT_EQ(engine_->stats().last_seq, static_cast<uint64_t>(kWriters * kPerWriter));
    for (int w = 0; w < kWriters; ++w) {
        for (int i = 0; i < kPerWriter; i += 101) {
            ASSERT_EQ(GetOrMissing("w" + std::to_string(w) + ":" + std::to_string(i)),
                      std::to_string(i));
        }
    }
}

}  // namespace
}  // namespace distkv

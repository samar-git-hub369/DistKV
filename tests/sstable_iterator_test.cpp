#include <gtest/gtest.h>

#include <cstdio>
#include <filesystem>
#include <fstream>
#include <map>
#include <random>
#include <string>
#include <vector>

#include "storage/sstable.h"

namespace distkv {
namespace {

namespace fs = std::filesystem;

class SSTableIteratorTest : public ::testing::Test {
  protected:
    void SetUp() override {
        std::random_device rd;
        dir_ = fs::temp_directory_path() / ("distkv_sst_iter_test_" + std::to_string(rd()));
        fs::create_directories(dir_);
        path_ = (dir_ / "000001.sst").string();
    }
    void TearDown() override {
        std::error_code ec;
        fs::remove_all(dir_, ec);
    }

    // Builds a table of keys key000000, key000002, ... (even numbers only) so
    // odd numbers fall between stored keys.
    void BuildEven(int n, size_t block_size) {
        std::unique_ptr<SSTableWriter> w;
        ASSERT_TRUE(SSTableWriter::Create(path_, SSTableOptions{block_size}, &w).ok());
        for (int i = 0; i < n; ++i) {
            const EntryType type = (i % 7 == 3) ? EntryType::kTombstone : EntryType::kValue;
            const std::string value = type == EntryType::kValue ? "v" + std::to_string(i) : "";
            ASSERT_TRUE(w->Add(Key(2 * i), static_cast<uint64_t>(i + 1), type, value).ok());
        }
        ASSERT_TRUE(w->Finish().ok());
        ASSERT_TRUE(SSTableReader::Open(path_, &table_).ok());
    }

    static std::string Key(int i) {
        char buf[16];
        std::snprintf(buf, sizeof(buf), "key%06d", i);
        return buf;
    }

    fs::path dir_;
    std::string path_;
    std::unique_ptr<SSTableReader> table_;
};

TEST_F(SSTableIteratorTest, UnpositionedIteratorIsInvalid) {
    BuildEven(10, 4096);
    SSTableIterator it(table_.get());
    EXPECT_FALSE(it.Valid());
    it.Next();  // harmless
    EXPECT_FALSE(it.Valid());
    EXPECT_TRUE(it.status().ok());
}

TEST_F(SSTableIteratorTest, FullScanVisitsEveryEntryInOrderAcrossBlocks) {
    BuildEven(3000, /*block_size=*/200);
    ASSERT_GT(table_->block_count(), 50u);

    SSTableIterator it(table_.get());
    int i = 0;
    for (it.SeekToFirst(); it.Valid(); it.Next(), ++i) {
        ASSERT_EQ(it.key(), Key(2 * i));
        ASSERT_EQ(it.seq(), static_cast<uint64_t>(i + 1));
        if (i % 7 == 3) {
            ASSERT_EQ(it.type(), EntryType::kTombstone);
            ASSERT_TRUE(it.value().empty());
        } else {
            ASSERT_EQ(it.type(), EntryType::kValue);
            ASSERT_EQ(it.value(), "v" + std::to_string(i));
        }
    }
    EXPECT_EQ(i, 3000);
    EXPECT_TRUE(it.status().ok());
}

TEST_F(SSTableIteratorTest, SeekLandsOnFirstKeyAtOrAfterTarget) {
    BuildEven(1000, /*block_size=*/128);
    SSTableIterator it(table_.get());

    // The largest stored key is 1998, so every target up to it has an answer.
    for (int target = 0; target <= 1998; ++target) {
        it.Seek(Key(target));
        ASSERT_TRUE(it.Valid()) << target;
        // Exact hit for even targets, next even key for odd ones.
        ASSERT_EQ(it.key(), Key(target % 2 == 0 ? target : target + 1)) << target;
    }
}

TEST_F(SSTableIteratorTest, SeekBeforeFirstAndAfterLast) {
    BuildEven(100, 256);
    SSTableIterator it(table_.get());

    it.Seek("");
    ASSERT_TRUE(it.Valid());
    EXPECT_EQ(it.key(), Key(0));

    it.Seek("a");  // sorts before "key..."
    ASSERT_TRUE(it.Valid());
    EXPECT_EQ(it.key(), Key(0));

    it.Seek(Key(198));  // last key
    ASSERT_TRUE(it.Valid());
    it.Next();
    EXPECT_FALSE(it.Valid());

    it.Seek(Key(199));
    EXPECT_FALSE(it.Valid());
    it.Seek("zzz");
    EXPECT_FALSE(it.Valid());
    EXPECT_TRUE(it.status().ok());
}

TEST_F(SSTableIteratorTest, SeekThenScanToEnd) {
    BuildEven(500, 128);
    SSTableIterator it(table_.get());
    int count = 0;
    for (it.Seek(Key(401)); it.Valid(); it.Next()) {
        ++count;
    }
    // Keys 402, 404, ..., 998.
    EXPECT_EQ(count, 299);
}

TEST_F(SSTableIteratorTest, IteratorCanBeRepositioned) {
    BuildEven(100, 128);
    SSTableIterator it(table_.get());
    it.Seek(Key(150));
    ASSERT_TRUE(it.Valid());
    it.SeekToFirst();
    ASSERT_TRUE(it.Valid());
    EXPECT_EQ(it.key(), Key(0));
    it.Seek(Key(20));
    ASSERT_TRUE(it.Valid());
    EXPECT_EQ(it.key(), Key(20));
}

TEST_F(SSTableIteratorTest, EmptyTable) {
    std::unique_ptr<SSTableWriter> w;
    ASSERT_TRUE(SSTableWriter::Create(path_, SSTableOptions{}, &w).ok());
    ASSERT_TRUE(w->Finish().ok());
    ASSERT_TRUE(SSTableReader::Open(path_, &table_).ok());

    SSTableIterator it(table_.get());
    it.SeekToFirst();
    EXPECT_FALSE(it.Valid());
    it.Seek("x");
    EXPECT_FALSE(it.Valid());
    EXPECT_TRUE(it.status().ok());
}

TEST_F(SSTableIteratorTest, DamagedBlockStopsScanWithCorruption) {
    BuildEven(200, 128);
    ASSERT_GT(table_->block_count(), 4u);

    // Damage one byte in the middle of the file, which lands in a data block
    // well after the first one.
    {
        std::fstream f(path_, std::ios::in | std::ios::out | std::ios::binary);
        const auto offset = static_cast<std::streamoff>(fs::file_size(path_) / 2);
        f.seekg(offset);
        char c = 0;
        f.read(&c, 1);
        c = static_cast<char>(~c);
        f.seekp(offset);
        f.write(&c, 1);
    }
    std::unique_ptr<SSTableReader> damaged;
    ASSERT_TRUE(SSTableReader::Open(path_, &damaged).ok());

    SSTableIterator it(damaged.get());
    int seen = 0;
    for (it.SeekToFirst(); it.Valid(); it.Next()) {
        ++seen;
    }
    EXPECT_EQ(it.status().code(), StatusCode::kCorruption);
    EXPECT_GT(seen, 0);    // entries before the damaged block came through
    EXPECT_LT(seen, 200);  // and the scan stopped at it
}

TEST_F(SSTableIteratorTest, MatchesReferenceMapOnRandomBinaryKeys) {
    std::mt19937 rng(7);
    std::map<std::string, std::string> ref;
    while (ref.size() < 2000) {
        std::string key(1 + rng() % 10, '\0');
        for (auto& c : key) c = static_cast<char>(rng() % 256);
        ref[key] = std::string(rng() % 50, 'x');
    }
    {
        std::unique_ptr<SSTableWriter> w;
        ASSERT_TRUE(SSTableWriter::Create(path_, SSTableOptions{512}, &w).ok());
        uint64_t seq = 1;
        for (const auto& [k, v] : ref) {
            ASSERT_TRUE(w->Add(k, seq++, EntryType::kValue, v).ok());
        }
        ASSERT_TRUE(w->Finish().ok());
    }
    ASSERT_TRUE(SSTableReader::Open(path_, &table_).ok());

    SSTableIterator it(table_.get());
    auto want = ref.begin();
    for (it.SeekToFirst(); it.Valid(); it.Next(), ++want) {
        ASSERT_NE(want, ref.end());
        ASSERT_EQ(it.key(), want->first);
        ASSERT_EQ(it.value(), want->second);
    }
    EXPECT_EQ(want, ref.end());

    // Seek to random probes and compare with std::map::lower_bound.
    for (int i = 0; i < 1000; ++i) {
        std::string probe(1 + rng() % 10, '\0');
        for (auto& c : probe) c = static_cast<char>(rng() % 256);
        it.Seek(probe);
        const auto expected = ref.lower_bound(probe);
        if (expected == ref.end()) {
            ASSERT_FALSE(it.Valid());
        } else {
            ASSERT_TRUE(it.Valid());
            ASSERT_EQ(it.key(), expected->first);
        }
    }
}

}  // namespace
}  // namespace distkv

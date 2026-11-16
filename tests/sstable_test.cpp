#include "storage/sstable.h"

#include <gtest/gtest.h>

#include <atomic>
#include <cstdio>
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

struct Row {
    std::string key;
    uint64_t seq;
    EntryType type;
    std::string value;
};

class SSTableTest : public ::testing::Test {
  protected:
    void SetUp() override {
        std::random_device rd;
        dir_ = fs::temp_directory_path() / ("distkv_sstable_test_" + std::to_string(rd()));
        fs::create_directories(dir_);
        path_ = (dir_ / "000001.sst").string();
    }
    void TearDown() override {
        std::error_code ec;
        fs::remove_all(dir_, ec);
    }

    void Build(const std::vector<Row>& rows, size_t block_size = 4096) {
        std::unique_ptr<SSTableWriter> w;
        ASSERT_TRUE(SSTableWriter::Create(path_, SSTableOptions{block_size}, &w).ok());
        for (const auto& r : rows) {
            ASSERT_TRUE(w->Add(r.key, r.seq, r.type, r.value).ok()) << r.key;
        }
        ASSERT_TRUE(w->Finish().ok());
    }

    std::unique_ptr<SSTableReader> OpenOk() {
        std::unique_ptr<SSTableReader> r;
        const Status s = SSTableReader::Open(path_, &r);
        EXPECT_TRUE(s.ok()) << s.ToString();
        return r;
    }

    void FlipByte(uint64_t offset) {
        std::fstream f(path_, std::ios::in | std::ios::out | std::ios::binary);
        f.seekg(static_cast<std::streamoff>(offset));
        char c = 0;
        f.read(&c, 1);
        c = static_cast<char>(~c);
        f.seekp(static_cast<std::streamoff>(offset));
        f.write(&c, 1);
    }

    static std::string Key(int i) {
        char buf[16];
        std::snprintf(buf, sizeof(buf), "key%06d", i);
        return buf;
    }

    fs::path dir_;
    std::string path_;
};

TEST_F(SSTableTest, RoundTripsValuesAndTombstones) {
    Build({
        {"apple", 3, EntryType::kValue, "red"},
        {"banana", 7, EntryType::kTombstone, ""},
        {"cherry", 5, EntryType::kValue, ""},
    });
    auto r = OpenOk();
    ASSERT_NE(r, nullptr);
    EXPECT_EQ(r->entry_count(), 3u);
    EXPECT_EQ(r->max_seq(), 7u);
    EXPECT_EQ(r->smallest_key(), "apple");
    EXPECT_EQ(r->largest_key(), "cherry");

    LookupResult res;
    std::string v;
    uint64_t seq = 0;
    ASSERT_TRUE(r->Get("apple", &res, &v, &seq).ok());
    EXPECT_EQ(res, LookupResult::kFound);
    EXPECT_EQ(v, "red");
    EXPECT_EQ(seq, 3u);

    ASSERT_TRUE(r->Get("banana", &res, &v, &seq).ok());
    EXPECT_EQ(res, LookupResult::kDeleted);
    EXPECT_EQ(seq, 7u);

    ASSERT_TRUE(r->Get("cherry", &res, &v).ok());
    EXPECT_EQ(res, LookupResult::kFound);
    EXPECT_EQ(v, "");
}

TEST_F(SSTableTest, MissingKeysBeforeBetweenAndAfter) {
    Build({{"b", 1, EntryType::kValue, "1"}, {"d", 2, EntryType::kValue, "2"}});
    auto r = OpenOk();
    LookupResult res;
    std::string v;
    for (const char* k : {"a", "c", "e", "", "bb"}) {
        ASSERT_TRUE(r->Get(k, &res, &v).ok());
        EXPECT_EQ(res, LookupResult::kNotPresent) << k;
    }
}

TEST_F(SSTableTest, ManyBlocksEveryKeyFound) {
    std::vector<Row> rows;
    for (int i = 0; i < 5000; ++i) {
        rows.push_back({Key(i * 2), static_cast<uint64_t>(i + 1), EntryType::kValue,
                        "value-" + std::to_string(i)});
    }
    Build(rows, /*block_size=*/256);
    auto r = OpenOk();
    EXPECT_GT(r->block_count(), 100u);  // the index really is sparse over many blocks
    EXPECT_EQ(r->entry_count(), 5000u);

    LookupResult res;
    std::string v;
    for (int i = 0; i < 5000; ++i) {
        ASSERT_TRUE(r->Get(Key(i * 2), &res, &v).ok());
        ASSERT_EQ(res, LookupResult::kFound) << i;
        ASSERT_EQ(v, "value-" + std::to_string(i));
        // The odd keys fall between stored keys, often on block boundaries.
        ASSERT_TRUE(r->Get(Key(i * 2 + 1), &res, &v).ok());
        ASSERT_EQ(res, LookupResult::kNotPresent) << i;
    }
}

TEST_F(SSTableTest, BinaryKeysUseUnsignedOrder) {
    const std::string nul_key("a\0b", 3);
    Build({
        {"a", 1, EntryType::kValue, "plain"},
        {nul_key, 2, EntryType::kValue, std::string("\0\1", 2)},
        {"\x7f", 3, EntryType::kValue, "del"},
        {"\xff\xff", 4, EntryType::kValue, "high"},
    });
    auto r = OpenOk();
    LookupResult res;
    std::string v;
    ASSERT_TRUE(r->Get(nul_key, &res, &v).ok());
    EXPECT_EQ(res, LookupResult::kFound);
    EXPECT_EQ(v, std::string("\0\1", 2));
    ASSERT_TRUE(r->Get("\xff\xff", &res, &v).ok());
    EXPECT_EQ(res, LookupResult::kFound);
    EXPECT_EQ(r->largest_key(), "\xff\xff");
}

TEST_F(SSTableTest, LargeValueSpansItsOwnBlock) {
    const std::string big(100000, 'z');
    Build({{"a", 1, EntryType::kValue, "small"},
           {"b", 2, EntryType::kValue, big},
           {"c", 3, EntryType::kValue, "small"}},
          /*block_size=*/64);
    auto r = OpenOk();
    LookupResult res;
    std::string v;
    ASSERT_TRUE(r->Get("b", &res, &v).ok());
    EXPECT_EQ(res, LookupResult::kFound);
    EXPECT_EQ(v, big);
}

TEST_F(SSTableTest, EmptyTableIsValid) {
    Build({});
    auto r = OpenOk();
    EXPECT_EQ(r->entry_count(), 0u);
    EXPECT_EQ(r->block_count(), 0u);
    LookupResult res;
    std::string v;
    ASSERT_TRUE(r->Get("anything", &res, &v).ok());
    EXPECT_EQ(res, LookupResult::kNotPresent);
}

TEST_F(SSTableTest, RejectsOutOfOrderAndDuplicateKeys) {
    std::unique_ptr<SSTableWriter> w;
    ASSERT_TRUE(SSTableWriter::Create(path_, SSTableOptions{}, &w).ok());
    ASSERT_TRUE(w->Add("m", 1, EntryType::kValue, "x").ok());
    EXPECT_EQ(w->Add("m", 2, EntryType::kValue, "y").code(), StatusCode::kInvalidArgument);
    EXPECT_EQ(w->Add("a", 3, EntryType::kValue, "y").code(), StatusCode::kInvalidArgument);
    EXPECT_EQ(w->Add("z", 4, EntryType::kTombstone, "has-value").code(),
              StatusCode::kInvalidArgument);
    EXPECT_TRUE(w->Add("z", 5, EntryType::kValue, "ok").ok());
}

TEST_F(SSTableTest, UnfinishedWriterLeavesNoFiles) {
    {
        std::unique_ptr<SSTableWriter> w;
        ASSERT_TRUE(SSTableWriter::Create(path_, SSTableOptions{}, &w).ok());
        ASSERT_TRUE(w->Add("k", 1, EntryType::kValue, "v").ok());
        EXPECT_TRUE(fs::exists(path_ + ".tmp"));
        EXPECT_FALSE(fs::exists(path_));
    }
    EXPECT_FALSE(fs::exists(path_ + ".tmp"));
    EXPECT_FALSE(fs::exists(path_));
}

TEST_F(SSTableTest, FinishPublishesUnderFinalName) {
    Build({{"k", 1, EntryType::kValue, "v"}});
    EXPECT_TRUE(fs::exists(path_));
    EXPECT_FALSE(fs::exists(path_ + ".tmp"));
}

TEST_F(SSTableTest, DetectsTruncatedFile) {
    Build({{"k", 1, EntryType::kValue, "v"}});
    fs::resize_file(path_, fs::file_size(path_) - 1);
    std::unique_ptr<SSTableReader> r;
    EXPECT_EQ(SSTableReader::Open(path_, &r).code(), StatusCode::kCorruption);

    fs::resize_file(path_, 10);
    EXPECT_EQ(SSTableReader::Open(path_, &r).code(), StatusCode::kCorruption);
}

TEST_F(SSTableTest, DetectsDamagedFooterAndIndex) {
    Build({{"a", 1, EntryType::kValue, "1"}, {"b", 2, EntryType::kValue, "2"}});
    const uint64_t size = fs::file_size(path_);
    std::unique_ptr<SSTableReader> r;

    FlipByte(size - kSSTableFooterSize + 12);  // entry_count inside the footer
    EXPECT_EQ(SSTableReader::Open(path_, &r).code(), StatusCode::kCorruption);
    FlipByte(size - kSSTableFooterSize + 12);  // restore
    ASSERT_TRUE(SSTableReader::Open(path_, &r).ok());

    FlipByte(size - kSSTableFooterSize - 6);  // inside the index block
    EXPECT_EQ(SSTableReader::Open(path_, &r).code(), StatusCode::kCorruption);
}

TEST_F(SSTableTest, DetectsDamagedDataBlockOnRead) {
    Build({{"a", 1, EntryType::kValue, "hello"}});
    FlipByte(6);  // inside the first entry's key/seq bytes

    // Open only checks footer and index, so it succeeds...
    auto r = OpenOk();
    ASSERT_NE(r, nullptr);
    // ...and the damage surfaces on the read that touches the block.
    LookupResult res;
    std::string v;
    EXPECT_EQ(r->Get("a", &res, &v).code(), StatusCode::kCorruption);
}

TEST_F(SSTableTest, ConcurrentReadersShareOneTable) {
    std::vector<Row> rows;
    for (int i = 0; i < 2000; ++i) {
        rows.push_back({Key(i), static_cast<uint64_t>(i + 1), EntryType::kValue, Key(i) + "!"});
    }
    Build(rows, /*block_size=*/512);
    auto r = OpenOk();

    std::atomic<int> failures{0};
    std::vector<std::thread> threads;
    for (int t = 0; t < 8; ++t) {
        threads.emplace_back([&, t] {
            LookupResult res;
            std::string v;
            for (int i = t; i < 2000; i += 8) {
                if (!r->Get(Key(i), &res, &v).ok() || res != LookupResult::kFound ||
                    v != Key(i) + "!") {
                    ++failures;
                }
            }
        });
    }
    for (auto& th : threads) th.join();
    EXPECT_EQ(failures.load(), 0);
}

TEST_F(SSTableTest, MatchesReferenceMapOnRandomData) {
    std::mt19937 rng(42);
    std::map<std::string, Row> ref;
    for (int i = 0; i < 3000; ++i) {
        std::string key(1 + rng() % 12, '\0');
        for (auto& c : key) c = static_cast<char>(rng() % 256);
        const bool tomb = rng() % 5 == 0;
        ref[key] = {key, static_cast<uint64_t>(i + 1),
                    tomb ? EntryType::kTombstone : EntryType::kValue,
                    tomb ? "" : std::string(rng() % 200, static_cast<char>('a' + i % 26))};
    }
    std::vector<Row> rows;
    for (const auto& [k, row] : ref) rows.push_back(row);
    Build(rows, /*block_size=*/1024);
    auto r = OpenOk();

    LookupResult res;
    std::string v;
    for (const auto& [k, row] : ref) {
        ASSERT_TRUE(r->Get(k, &res, &v).ok());
        if (row.type == EntryType::kTombstone) {
            ASSERT_EQ(res, LookupResult::kDeleted);
        } else {
            ASSERT_EQ(res, LookupResult::kFound);
            ASSERT_EQ(v, row.value);
        }
    }
    // Random probes that are not in the table.
    for (int i = 0; i < 2000; ++i) {
        std::string key(13 + rng() % 4, static_cast<char>(rng() % 256));
        ASSERT_TRUE(r->Get(key, &res, &v).ok());
        EXPECT_EQ(res, LookupResult::kNotPresent);
    }
}

}  // namespace
}  // namespace distkv

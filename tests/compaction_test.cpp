#include "storage/compaction.h"

#include <gtest/gtest.h>

#include <filesystem>
#include <map>
#include <random>
#include <string>
#include <tuple>
#include <vector>

#include "storage/merging_iterator.h"

namespace distkv {
namespace {

namespace fs = std::filesystem;

struct Row {
    std::string key;
    uint64_t seq;
    EntryType type;
    std::string value;
};

Row Put(std::string k, uint64_t seq, std::string v) {
    return {std::move(k), seq, EntryType::kValue, std::move(v)};
}
Row Del(std::string k, uint64_t seq) { return {std::move(k), seq, EntryType::kTombstone, ""}; }

class CompactionTest : public ::testing::Test {
  protected:
    void SetUp() override {
        std::random_device rd;
        dir_ = fs::temp_directory_path() / ("distkv_compaction_test_" + std::to_string(rd()));
        fs::create_directories(dir_);
    }
    void TearDown() override {
        std::error_code ec;
        fs::remove_all(dir_, ec);
    }

    // Rows must be in key order.
    const SSTableReader* Table(const std::vector<Row>& rows) {
        const std::string path = (dir_ / (std::to_string(tables_.size()) + ".sst")).string();
        std::unique_ptr<SSTableWriter> w;
        EXPECT_TRUE(SSTableWriter::Create(path, SSTableOptions{256}, &w).ok());
        for (const auto& r : rows) {
            EXPECT_TRUE(w->Add(r.key, r.seq, r.type, r.value).ok());
        }
        EXPECT_TRUE(w->Finish().ok());
        std::unique_ptr<SSTableReader> reader;
        EXPECT_TRUE(SSTableReader::Open(path, &reader).ok());
        tables_.push_back(std::move(reader));
        return tables_.back().get();
    }

    std::vector<Row> ReadAll(const std::string& path) {
        std::unique_ptr<SSTableReader> r;
        EXPECT_TRUE(SSTableReader::Open(path, &r).ok());
        std::vector<Row> rows;
        SSTableIterator it(r.get());
        for (it.SeekToFirst(); it.Valid(); it.Next()) {
            rows.push_back({std::string(it.key()), it.seq(), it.type(), std::string(it.value())});
        }
        EXPECT_TRUE(it.status().ok());
        return rows;
    }

    std::string Out() const { return (dir_ / "out.sst").string(); }

    fs::path dir_;
    std::vector<std::unique_ptr<SSTableReader>> tables_;
};

TEST_F(CompactionTest, MergingIteratorInterleavesAndOrdersVersionsNewestFirst) {
    const auto* a = Table({Put("a", 1, "a1"), Put("c", 2, "c2"), Put("e", 3, "e3")});
    const auto* b = Table({Put("b", 4, "b4"), Put("c", 5, "c5"), Put("f", 6, "f6")});
    const auto* c = Table({Del("c", 7)});

    MergingIterator it({a, b, c});
    std::vector<std::tuple<std::string, uint64_t>> got;
    for (it.SeekToFirst(); it.Valid(); it.Next()) {
        got.emplace_back(std::string(it.key()), it.seq());
    }
    ASSERT_TRUE(it.status().ok());
    const std::vector<std::tuple<std::string, uint64_t>> want = {
        {"a", 1}, {"b", 4}, {"c", 7}, {"c", 5}, {"c", 2}, {"e", 3}, {"f", 6}};
    EXPECT_EQ(got, want);
}

TEST_F(CompactionTest, MergingIteratorHandlesEmptyInputs) {
    MergingIterator none({});
    none.SeekToFirst();
    EXPECT_FALSE(none.Valid());

    const auto* empty = Table({});
    const auto* one = Table({Put("k", 1, "v")});
    MergingIterator it({empty, one, empty});
    it.SeekToFirst();
    ASSERT_TRUE(it.Valid());
    EXPECT_EQ(it.key(), "k");
    it.Next();
    EXPECT_FALSE(it.Valid());
}

TEST_F(CompactionTest, KeepsOnlyNewestVersionAndDropsTombstones) {
    const auto* older = Table({Put("a", 1, "old"), Put("b", 2, "b"), Put("d", 3, "doomed")});
    const auto* newer = Table({Put("a", 4, "new"), Del("d", 5), Del("z", 6)});

    CompactionStats st;
    ASSERT_TRUE(CompactTables({newer, older}, Out(), SSTableOptions{}, true, &st).ok());

    const auto rows = ReadAll(Out());
    ASSERT_EQ(rows.size(), 2u);
    EXPECT_EQ(rows[0].key, "a");
    EXPECT_EQ(rows[0].value, "new");
    EXPECT_EQ(rows[0].seq, 4u);
    EXPECT_EQ(rows[1].key, "b");

    EXPECT_EQ(st.input_tables, 2u);
    EXPECT_EQ(st.input_entries, 6u);
    EXPECT_EQ(st.output_entries, 2u);
    EXPECT_EQ(st.dropped_overwritten, 2u);  // a@1 and d@3
    EXPECT_EQ(st.dropped_tombstones, 2u);   // d@5 and z@6
    EXPECT_GT(st.input_bytes, st.output_bytes);
}

TEST_F(CompactionTest, KeepsTombstonesWhenOlderDataMayExist) {
    const auto* t = Table({Put("a", 1, "x"), Del("b", 2)});
    CompactionStats st;
    ASSERT_TRUE(CompactTables({t}, Out(), SSTableOptions{}, false, &st).ok());

    const auto rows = ReadAll(Out());
    ASSERT_EQ(rows.size(), 2u);
    EXPECT_EQ(rows[1].key, "b");
    EXPECT_EQ(rows[1].type, EntryType::kTombstone);
    EXPECT_EQ(st.dropped_tombstones, 0u);
}

TEST_F(CompactionTest, OutputKeepsInputsMaxSeqEvenWhenEverythingIsDropped) {
    const auto* older = Table({Put("k", 1, "v")});
    const auto* newer = Table({Del("k", 9)});
    CompactionStats st;
    ASSERT_TRUE(CompactTables({older, newer}, Out(), SSTableOptions{}, true, &st).ok());

    std::unique_ptr<SSTableReader> out;
    ASSERT_TRUE(SSTableReader::Open(Out(), &out).ok());
    EXPECT_EQ(out->entry_count(), 0u);
    EXPECT_EQ(out->max_seq(), 9u);
}

TEST_F(CompactionTest, InputOrderDoesNotMatter) {
    const auto* a = Table({Put("k", 1, "first")});
    const auto* b = Table({Put("k", 2, "second")});
    const auto* c = Table({Put("k", 3, "third")});
    CompactionStats st;
    ASSERT_TRUE(CompactTables({b, c, a}, Out(), SSTableOptions{}, true, &st).ok());
    const auto rows = ReadAll(Out());
    ASSERT_EQ(rows.size(), 1u);
    EXPECT_EQ(rows[0].value, "third");
}

TEST_F(CompactionTest, MatchesReplayOfAllWritesOnRandomData) {
    std::mt19937 rng(5);
    std::map<std::string, Row> latest;  // what replaying every write in seq order leaves
    uint64_t seq = 1;
    std::vector<const SSTableReader*> inputs;
    for (int t = 0; t < 5; ++t) {
        std::map<std::string, Row> table;
        for (int i = 0; i < 400; ++i) {
            const std::string key = "k" + std::to_string(rng() % 300);
            Row row = rng() % 4 == 0 ? Del(key, seq) : Put(key, seq, std::to_string(seq));
            ++seq;
            table[key] = row;  // within one table only the newest write survives
            latest[key] = row;
        }
        std::vector<Row> rows;
        for (auto& [k, r] : table) rows.push_back(r);
        inputs.push_back(Table(rows));
    }

    CompactionStats st;
    ASSERT_TRUE(CompactTables(inputs, Out(), SSTableOptions{512}, true, &st).ok());

    std::vector<Row> want;
    for (const auto& [k, r] : latest) {
        if (r.type == EntryType::kValue) want.push_back(r);
    }
    const auto got = ReadAll(Out());
    ASSERT_EQ(got.size(), want.size());
    for (size_t i = 0; i < got.size(); ++i) {
        EXPECT_EQ(got[i].key, want[i].key);
        EXPECT_EQ(got[i].seq, want[i].seq);
        EXPECT_EQ(got[i].value, want[i].value);
    }
    EXPECT_EQ(st.output_entries + st.dropped_overwritten + st.dropped_tombstones,
              st.input_entries);
}

TEST_F(CompactionTest, FailedCompactionLeavesNoOutput) {
    const auto* t = Table({Put("k", 1, "v")});
    const std::string bad = (dir_ / "missing-dir" / "out.sst").string();
    CompactionStats st;
    EXPECT_FALSE(CompactTables({t}, bad, SSTableOptions{}, true, &st).ok());
    EXPECT_FALSE(fs::exists(bad));
}

}  // namespace
}  // namespace distkv

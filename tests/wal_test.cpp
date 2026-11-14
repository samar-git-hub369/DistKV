#include "storage/wal.h"

#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <random>
#include <string>
#include <vector>

namespace distkv {
namespace {

namespace fs = std::filesystem;

class WalTest : public ::testing::Test {
  protected:
    void SetUp() override {
        std::random_device rd;
        dir_ = fs::temp_directory_path() / ("distkv_wal_test_" + std::to_string(rd()));
        fs::create_directories(dir_);
        path_ = (dir_ / "wal.log").string();
    }

    void TearDown() override {
        std::error_code ec;
        fs::remove_all(dir_, ec);
    }

    void WriteAll(const std::vector<WalRecord>& records,
                  WalSyncMode mode = WalSyncMode::kNone) {
        std::unique_ptr<WalWriter> writer;
        ASSERT_TRUE(WalWriter::Open(path_, mode, &writer).ok());
        for (const auto& r : records) {
            ASSERT_TRUE(writer->Append(r).ok());
        }
    }

    Status Replay(std::vector<WalRecord>* out, WalReplayStats* stats) {
        out->clear();
        return ReplayWal(path_, [out](const WalRecord& r) { out->push_back(r); }, stats);
    }

    // Overwrites one byte at `offset` with its bitwise complement.
    void FlipByte(uint64_t offset) {
        std::fstream f(path_, std::ios::in | std::ios::out | std::ios::binary);
        f.seekg(static_cast<std::streamoff>(offset));
        char c = 0;
        f.read(&c, 1);
        c = static_cast<char>(~c);
        f.seekp(static_cast<std::streamoff>(offset));
        f.write(&c, 1);
    }

    static WalRecord Put(uint64_t seq, std::string key, std::string value) {
        return WalRecord{seq, WalOp::kPut, std::move(key), std::move(value)};
    }
    static WalRecord Del(uint64_t seq, std::string key) {
        return WalRecord{seq, WalOp::kDelete, std::move(key), ""};
    }

    fs::path dir_;
    std::string path_;
};

TEST_F(WalTest, MissingFileReplaysAsEmpty) {
    std::vector<WalRecord> got;
    WalReplayStats stats;
    ASSERT_TRUE(Replay(&got, &stats).ok());
    EXPECT_TRUE(got.empty());
    EXPECT_EQ(stats.records, 0u);
    EXPECT_FALSE(stats.torn_tail);
}

TEST_F(WalTest, EmptyFileReplaysAsEmpty) {
    WriteAll({});
    ASSERT_TRUE(fs::exists(path_));
    std::vector<WalRecord> got;
    WalReplayStats stats;
    ASSERT_TRUE(Replay(&got, &stats).ok());
    EXPECT_TRUE(got.empty());
    EXPECT_FALSE(stats.torn_tail);
}

TEST_F(WalTest, ReplaysRecordsInOrder) {
    const std::vector<WalRecord> records = {
        Put(1, "user:1", "alice"),
        Put(2, "user:2", "bob"),
        Del(3, "user:1"),
        Put(4, "user:2", "bobby"),
    };
    WriteAll(records, WalSyncMode::kEveryAppend);

    std::vector<WalRecord> got;
    WalReplayStats stats;
    ASSERT_TRUE(Replay(&got, &stats).ok());
    EXPECT_EQ(got, records);
    EXPECT_EQ(stats.records, 4u);
    EXPECT_EQ(stats.valid_bytes, fs::file_size(path_));
    EXPECT_FALSE(stats.torn_tail);
}

TEST_F(WalTest, PreservesBinaryKeysValuesAndEmptyValues) {
    const std::vector<WalRecord> records = {
        Put(1, std::string("k\0ey", 4), std::string("\0\xff\x01", 3)),
        Put(2, "", ""),
        Put(3, "big", std::string(1 << 20, 'x')),
    };
    WriteAll(records);

    std::vector<WalRecord> got;
    WalReplayStats stats;
    ASSERT_TRUE(Replay(&got, &stats).ok());
    EXPECT_EQ(got, records);
}

TEST_F(WalTest, ReopeningAppendsAfterExistingRecords) {
    WriteAll({Put(1, "a", "1")});
    WriteAll({Put(2, "b", "2")});

    std::vector<WalRecord> got;
    WalReplayStats stats;
    ASSERT_TRUE(Replay(&got, &stats).ok());
    ASSERT_EQ(got.size(), 2u);
    EXPECT_EQ(got[0].key, "a");
    EXPECT_EQ(got[1].key, "b");
}

TEST_F(WalTest, RejectsDeleteWithValue) {
    std::unique_ptr<WalWriter> writer;
    ASSERT_TRUE(WalWriter::Open(path_, WalSyncMode::kNone, &writer).ok());
    WalRecord bad = Del(1, "k");
    bad.value = "oops";
    EXPECT_EQ(writer->Append(bad).code(), StatusCode::kInvalidArgument);
    EXPECT_EQ(writer->bytes_written(), 0u);
}

TEST_F(WalTest, TruncatedLastFrameIsATornTail) {
    WriteAll({Put(1, "a", "1"), Put(2, "b", "2")});
    const uint64_t first_frame = EncodeWalFrame(Put(1, "a", "1")).size();
    const uint64_t full = fs::file_size(path_);

    // Cut the file at every point inside the second frame. Walk downwards so
    // each cut only removes bytes; growing the file would pad it with zeros.
    for (uint64_t cut = full - 1; cut > first_frame; --cut) {
        fs::resize_file(path_, cut);
        std::vector<WalRecord> got;
        WalReplayStats stats;
        const Status s = Replay(&got, &stats);
        ASSERT_TRUE(s.ok()) << "cut at " << cut << ": " << s.ToString();
        ASSERT_EQ(got.size(), 1u) << "cut at " << cut;
        EXPECT_TRUE(stats.torn_tail);
        EXPECT_EQ(stats.valid_bytes, first_frame);
    }
}

TEST_F(WalTest, DamagedLastFrameIsATornTail) {
    WriteAll({Put(1, "a", "1"), Put(2, "b", "2")});
    FlipByte(fs::file_size(path_) - 1);

    std::vector<WalRecord> got;
    WalReplayStats stats;
    ASSERT_TRUE(Replay(&got, &stats).ok());
    EXPECT_EQ(got.size(), 1u);
    EXPECT_TRUE(stats.torn_tail);
}

TEST_F(WalTest, DamagedMiddleFrameIsCorruption) {
    WriteAll({Put(1, "a", "1"), Put(2, "b", "2"), Put(3, "c", "3")});
    const uint64_t first_frame = EncodeWalFrame(Put(1, "a", "1")).size();
    FlipByte(first_frame + 10);  // inside the second frame's payload

    std::vector<WalRecord> got;
    WalReplayStats stats;
    const Status s = Replay(&got, &stats);
    EXPECT_EQ(s.code(), StatusCode::kCorruption);
    EXPECT_EQ(got.size(), 1u);  // records before the damage were still delivered
}

TEST_F(WalTest, DamagedLengthFieldIsDetected) {
    WriteAll({Put(1, "a", "1"), Put(2, "b", "2"), Put(3, "c", "3")});
    FlipByte(4);  // low byte of the first frame's length

    std::vector<WalRecord> got;
    WalReplayStats stats;
    const Status s = Replay(&got, &stats);
    // The flipped length runs past the end of the file, so it reads as a torn
    // tail; either way no record may be delivered from damaged bytes.
    EXPECT_TRUE(got.empty());
    EXPECT_TRUE(!s.ok() || stats.torn_tail);
}

TEST_F(WalTest, RecoverCutsTornTailSoNewAppendsReplay) {
    WriteAll({Put(1, "a", "1"), Put(2, "b", "2")});
    fs::resize_file(path_, fs::file_size(path_) - 3);

    std::vector<WalRecord> recovered;
    WalReplayStats stats;
    ASSERT_TRUE(RecoverWal(path_, [&](const WalRecord& r) { recovered.push_back(r); }, &stats)
                    .ok());
    EXPECT_EQ(recovered.size(), 1u);
    EXPECT_TRUE(stats.torn_tail);
    EXPECT_EQ(fs::file_size(path_), stats.valid_bytes);

    WriteAll({Put(2, "b", "again")});

    std::vector<WalRecord> got;
    ASSERT_TRUE(Replay(&got, &stats).ok());
    EXPECT_FALSE(stats.torn_tail);
    ASSERT_EQ(got.size(), 2u);
    EXPECT_EQ(got[1].value, "again");
}

TEST_F(WalTest, RecoverLeavesACleanLogUntouched) {
    WriteAll({Put(1, "a", "1")});
    const auto size_before = fs::file_size(path_);

    WalReplayStats stats;
    ASSERT_TRUE(RecoverWal(path_, [](const WalRecord&) {}, &stats).ok());
    EXPECT_EQ(fs::file_size(path_), size_before);
    EXPECT_FALSE(stats.torn_tail);
}

}  // namespace
}  // namespace distkv

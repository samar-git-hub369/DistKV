#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/status.h"
#include "storage/lookup.h"

// Sorted String Table: an immutable, sorted file of key/value entries.
//
// When a MemTable fills up it is written out as one SSTable and never changed
// again. Layout:
//
//   data block 0 | data block 1 | ... | index block | footer
//
//   data block  := entry* crc32:u32
//   entry       := key_len:u32 key seq:u64 type:u8 value_len:u32 value
//   index block := smallest_key(len-prefixed) handle* crc32:u32
//   handle      := last_key(len-prefixed) offset:u64 size:u32
//   footer      := index_offset:u64 index_size:u32 entry_count:u64
//                  max_seq:u64 crc32:u32 magic:u64           (40 bytes)
//
// All integers are little-endian; every CRC covers the bytes before it in its
// block. `size` in a handle excludes the trailing CRC.
//
// The index is sparse: one handle per data block (~4 KiB), not one per key. A
// reader keeps the index in memory, binary-searches it to find the single
// block that could hold a key, and reads only that block from disk.
namespace distkv {

enum class EntryType : uint8_t {
    kValue = 1,
    kTombstone = 2,
};

struct SSTableOptions {
    // A data block is closed once its encoded entries reach this many bytes.
    size_t block_size = 4096;
};

// Builds an SSTable. Entries must be added in strictly increasing key order.
//
// Data goes to `<path>.tmp`; Finish() syncs it and renames it to `path`, so a
// crash mid-write never leaves a half-written table under the real name.
class SSTableWriter {
  public:
    static Status Create(const std::string& path, const SSTableOptions& options,
                         std::unique_ptr<SSTableWriter>* out);

    // Deletes the temporary file if Finish() was not reached.
    ~SSTableWriter();
    SSTableWriter(const SSTableWriter&) = delete;
    SSTableWriter& operator=(const SSTableWriter&) = delete;

    Status Add(std::string_view key, uint64_t seq, EntryType type, std::string_view value);
    Status Finish();

    uint64_t entry_count() const { return entry_count_; }

  private:
    SSTableWriter(int fd, std::string path, std::string tmp_path, SSTableOptions options);
    Status FlushBlock();
    Status Write(const std::string& bytes);

    int fd_;
    const std::string path_;
    const std::string tmp_path_;
    const SSTableOptions options_;

    std::string block_;     // encoded entries of the open data block
    std::string index_;     // encoded handles of finished blocks
    std::string last_key_;  // last key added, for the ordering check
    std::string smallest_key_;
    uint64_t offset_ = 0;
    uint64_t entry_count_ = 0;
    uint64_t max_seq_ = 0;
    bool finished_ = false;
    bool failed_ = false;
};

// Read-only view of a finished SSTable. Safe to share between threads.
class SSTableReader {
  public:
    // Reads and verifies the footer and index; data blocks are read on demand.
    static Status Open(const std::string& path, std::unique_ptr<SSTableReader>* out);

    ~SSTableReader();
    SSTableReader(const SSTableReader&) = delete;
    SSTableReader& operator=(const SSTableReader&) = delete;

    // On kFound fills `*value`; on kFound or kDeleted fills `*seq` if non-null.
    // A damaged block yields Corruption.
    Status Get(std::string_view key, LookupResult* result, std::string* value,
               uint64_t* seq = nullptr) const;

    const std::string& path() const { return path_; }
    const std::string& smallest_key() const { return smallest_key_; }
    const std::string& largest_key() const;
    uint64_t entry_count() const { return entry_count_; }
    uint64_t max_seq() const { return max_seq_; }
    size_t block_count() const { return blocks_.size(); }

  private:
    struct BlockHandle {
        std::string last_key;
        uint64_t offset;
        uint32_t size;
    };

    SSTableReader(int fd, std::string path) : fd_(fd), path_(std::move(path)) {}
    Status ReadBlock(const BlockHandle& handle, std::string* contents) const;

    int fd_;
    const std::string path_;
    std::string smallest_key_;
    std::vector<BlockHandle> blocks_;
    uint64_t entry_count_ = 0;
    uint64_t max_seq_ = 0;
};

// Marks the end of a valid table; a file missing it was never finished.
inline constexpr uint64_t kSSTableMagic = 0x4453544b56535354ull;
inline constexpr size_t kSSTableFooterSize = 40;

}  // namespace distkv

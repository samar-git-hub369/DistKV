#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/status.h"
#include "storage/bloom_filter.h"
#include "storage/lookup.h"

// Sorted String Table: an immutable, sorted file of key/value entries.
//
// When a MemTable fills up it is written out as one SSTable and never changed
// again. Layout:
//
//   data block 0 | data block 1 | ... | filter block | index block | footer
//
//   data block   := entry* crc32:u32
//   entry        := key_len:u32 key seq:u64 type:u8 value_len:u32 value
//   filter block := bloom_filter crc32:u32       (see bloom_filter.h)
//   index block  := smallest_key(len-prefixed) handle* crc32:u32
//   handle       := last_key(len-prefixed) offset:u64 size:u32
//   footer       := index_offset:u64 index_size:u32 filter_offset:u64
//                   filter_size:u32 entry_count:u64 max_seq:u64
//                   crc32:u32 magic:u64                         (52 bytes)
//
// All integers are little-endian; every CRC covers the bytes before it in its
// block. `size` in a handle excludes the trailing CRC.
//
// The index is sparse: one handle per data block (~4 KiB), not one per key. A
// reader keeps the index and the Bloom filter in memory. A lookup first asks
// the filter, which rules out most missing keys with no I/O, then binary-
// searches the index and reads the single block that could hold the key.
namespace distkv {

enum class EntryType : uint8_t {
    kValue = 1,
    kTombstone = 2,
};

struct SSTableOptions {
    // A data block is closed once its encoded entries reach this many bytes.
    size_t block_size = 4096;
    // Bloom filter size; 10 bits per key gives ~1% false positives. 0 disables it.
    int bloom_bits_per_key = 10;
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
    BloomFilterBuilder bloom_;
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

    // False only if `key` is definitely not in this table.
    bool MayContain(std::string_view key) const { return BloomMayContain(filter_, key); }
    // Lookups answered by the Bloom filter alone, without reading a block.
    uint64_t bloom_skips() const { return bloom_skips_.load(std::memory_order_relaxed); }

  private:
    friend class SSTableIterator;

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
    std::string filter_;
    uint64_t entry_count_ = 0;
    uint64_t max_seq_ = 0;
    mutable std::atomic<uint64_t> bloom_skips_{0};
};

// Walks the entries of one SSTable in key order, tombstones included.
//
// Holds one data block in memory at a time. key() and value() point into that
// block and stay valid only until the next call that moves the iterator. The
// table must outlive the iterator. Each iterator is single-threaded, but many
// iterators can read the same table at once.
//
//   SSTableIterator it(reader.get());
//   for (it.Seek("user:"); it.Valid(); it.Next()) { ... }
//   if (!it.status().ok()) { ... }   // stopped early on damage
class SSTableIterator {
  public:
    explicit SSTableIterator(const SSTableReader* table) : table_(table) {}

    void SeekToFirst();
    // Positions at the first entry whose key is >= target.
    void Seek(std::string_view target);
    void Next();

    // False past the last entry, before any Seek, or after an error.
    bool Valid() const { return valid_; }
    // Ok unless iteration stopped on a damaged block.
    const Status& status() const { return status_; }

    std::string_view key() const { return key_; }
    uint64_t seq() const { return seq_; }
    EntryType type() const { return type_; }
    std::string_view value() const { return value_; }

  private:
    // Loads block `index` and positions before its first entry. Returns false
    // past the last block or on error.
    bool LoadBlock(size_t index);
    // Decodes the next entry, crossing into later blocks as needed.
    void Advance();

    const SSTableReader* table_;
    size_t block_index_ = 0;
    std::string block_;
    std::string_view rest_;  // undecoded tail of block_

    bool valid_ = false;
    Status status_;
    std::string_view key_;
    uint64_t seq_ = 0;
    EntryType type_ = EntryType::kValue;
    std::string_view value_;
};

// Marks the end of a valid table; a file missing it was never finished.
inline constexpr uint64_t kSSTableMagic = 0x4453544b56535354ull;
inline constexpr size_t kSSTableFooterSize = 52;

}  // namespace distkv

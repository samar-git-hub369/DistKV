#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <shared_mutex>
#include <string>
#include <string_view>

#include "storage/lookup.h"
#include "storage/wal.h"

// In-memory write buffer of the LSM tree.
//
// Writes land here after the WAL and stay until the table is flushed to an
// immutable SSTable. Keys are kept sorted (unsigned byte order) so a flush is
// a single sequential pass.
//
// A delete is stored as a tombstone rather than removing the key: an older
// SSTable may still hold a value for it, and the tombstone is what hides that
// value from reads until compaction drops both.
namespace distkv {

struct MemTableEntry {
    uint64_t seq = 0;
    bool tombstone = false;
    std::string value;
};

class MemTable {
  public:
    // ShouldFlush() turns true once ApproximateBytes() reaches this size.
    explicit MemTable(size_t flush_threshold_bytes);

    MemTable(const MemTable&) = delete;
    MemTable& operator=(const MemTable&) = delete;

    // Writes carry the sequence number assigned when they were logged. A write
    // older than what the table already holds for that key is ignored, so
    // replaying the same WAL twice leaves the table unchanged.
    void Put(uint64_t seq, std::string_view key, std::string_view value);
    void Delete(uint64_t seq, std::string_view key);

    // Applies a record read back from the WAL.
    void Apply(const WalRecord& record);

    // On kFound, `*value` receives the value. It is left untouched otherwise.
    LookupResult Get(std::string_view key, std::string* value) const;

    // Visits every entry, tombstones included, in ascending key order. Holds a
    // shared lock for the duration, so `fn` must not write to this table.
    void ForEach(const std::function<void(std::string_view key, const MemTableEntry&)>& fn) const;

    size_t ApproximateBytes() const;
    size_t EntryCount() const;
    uint64_t MaxSeq() const;  // 0 when empty
    bool ShouldFlush() const;

    // Rough per-entry bookkeeping cost on top of key and value bytes, used so
    // millions of tiny keys still trigger a flush.
    static constexpr size_t kEntryOverhead = 64;

  private:
    void Upsert(uint64_t seq, std::string_view key, bool tombstone, std::string_view value);
    static size_t Charge(std::string_view key, const MemTableEntry& entry) {
        return key.size() + entry.value.size() + kEntryOverhead;
    }

    const size_t flush_threshold_;

    mutable std::shared_mutex mu_;
    std::map<std::string, MemTableEntry, std::less<>> entries_;
    size_t bytes_ = 0;
    uint64_t max_seq_ = 0;
};

// Rebuilds `table` from the WAL at `path`: replays every record, then cuts off
// a torn tail so the log is ready for new appends. A missing log is fine.
Status RecoverMemTable(const std::string& wal_path, MemTable* table, WalReplayStats* stats);

}  // namespace distkv

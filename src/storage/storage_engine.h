#pragma once

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "common/status.h"
#include "storage/memtable.h"
#include "storage/sstable.h"
#include "storage/wal.h"

// The LSM-tree storage engine for one node.
//
// Write path:  WAL append  ->  active MemTable
// Read path:   active MemTable -> frozen MemTable -> SSTables, newest first
//
// When the active MemTable fills up it is frozen, a fresh MemTable and WAL
// file take over new writes, and a background thread flushes the frozen table
// to a new SSTable. Once that SSTable is durable its WAL file is deleted.
//
// Directory layout:
//   NNNNNN.sst       SSTables; a higher number is newer data
//   wal-NNNNNN.log   the WAL behind the MemTable of the same number
//   *.tmp            unfinished SSTables, removed on open
//
// Recovery on Open: every WAL left on disk is replayed in order into one
// MemTable, which is flushed to a new SSTable before the WALs are deleted, so
// the engine always starts with an empty MemTable and a fresh WAL. A WAL can
// outlive its flush if the process dies between publishing the SSTable and
// deleting the WAL. Its records all carry sequence numbers no higher than the
// newest SSTable's, while unflushed writes are always higher, so replay skips
// anything at or below that mark.
namespace distkv {

struct EngineOptions {
    size_t memtable_bytes = 4u << 20;  // freeze and flush the MemTable at this size
    WalSyncMode wal_sync = WalSyncMode::kEveryAppend;
    SSTableOptions sstable;
};

struct EngineStats {
    uint64_t last_seq = 0;
    size_t sstables = 0;
    size_t memtable_bytes = 0;
    bool flush_in_progress = false;
    uint64_t flushes = 0;
};

class StorageEngine {
  public:
    // Opens or creates the engine in `dir` and recovers any unflushed writes.
    static Status Open(const std::string& dir, const EngineOptions& options,
                       std::unique_ptr<StorageEngine>* out);

    // Waits for an in-progress flush to finish. Writes still in the active
    // MemTable stay in its WAL and are recovered by the next Open.
    ~StorageEngine();
    StorageEngine(const StorageEngine&) = delete;
    StorageEngine& operator=(const StorageEngine&) = delete;

    // Durable (per wal_sync) once they return ok. Keys must be non-empty.
    Status Put(std::string_view key, std::string_view value);
    Status Delete(std::string_view key);

    // NotFound if the key was never written or its latest write is a delete.
    Status Get(std::string_view key, std::string* value) const;

    // Freezes the active MemTable (if non-empty) and waits until every frozen
    // MemTable is in an SSTable.
    Status Flush();

    EngineStats stats() const;
    const std::string& dir() const { return dir_; }

  private:
    StorageEngine(std::string dir, EngineOptions options);

    Status Recover();
    Status Write(WalOp op, std::string_view key, std::string_view value);
    // Freezes the active MemTable if it is full (or if `force`), waiting for a
    // previous flush to finish first. Caller holds write_mu_.
    Status MakeRoomForWrite(bool force);
    void FlushLoop();

    std::string SSTablePath(uint64_t number) const;
    std::string WalPath(uint64_t number) const;

    const std::string dir_;
    const EngineOptions options_;

    // Serialises writers so WAL order matches sequence order. Held across
    // the WAL append, never by readers.
    std::mutex write_mu_;

    // Guards the fields below. Held only briefly; readers copy the pointers
    // they need and search without it.
    mutable std::mutex mu_;
    std::condition_variable flush_cv_;  // imm_ set, imm_ cleared, or shutdown
    std::shared_ptr<MemTable> mem_;
    std::shared_ptr<MemTable> imm_;  // frozen, being flushed; null if none
    uint64_t mem_wal_number_ = 0;
    uint64_t imm_wal_number_ = 0;
    std::unique_ptr<WalWriter> wal_;
    // Newest first. Replaced, never mutated, so a reader's copy stays valid.
    std::shared_ptr<const std::vector<std::shared_ptr<SSTableReader>>> tables_;
    uint64_t next_file_number_ = 1;
    Status bg_error_;
    bool shutting_down_ = false;
    uint64_t flushes_ = 0;

    std::atomic<uint64_t> last_seq_{0};
    std::thread flush_thread_;
};

}  // namespace distkv

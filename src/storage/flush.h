#pragma once

#include <cstdint>
#include <string>

#include "common/status.h"
#include "storage/memtable.h"
#include "storage/sstable.h"

namespace distkv {

struct FlushStats {
    uint64_t entries = 0;     // values and tombstones written
    uint64_t tombstones = 0;
    uint64_t file_bytes = 0;  // size of the finished SSTable
    uint64_t max_seq = 0;     // highest sequence number now durable in the table
};

// Writes every entry of `mem`, tombstones included, to a new SSTable at `path`.
//
// The table is published atomically (see SSTableWriter), so on success all of
// `mem` is durable on disk and on failure nothing appears under `path`. Once
// this returns ok, WAL records up to `stats->max_seq` are no longer needed.
//
// Holds a shared lock on `mem` for the whole write. The caller is expected to
// have frozen `mem` (swapped in a fresh MemTable for new writes) first, so the
// lock is never contended.
Status FlushMemTable(const MemTable& mem, const std::string& path, const SSTableOptions& options,
                     FlushStats* stats);

}  // namespace distkv

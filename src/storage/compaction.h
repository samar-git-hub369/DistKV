#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "common/status.h"
#include "storage/sstable.h"

// Compaction merges several SSTables into one.
//
// Every write creates a new version rather than updating in place, so over
// time the same key appears in many tables and deleted keys linger as
// tombstones. Reads get slower (more tables to check) and disk use grows.
// Compaction keeps only the newest version of each key and, when it is safe,
// drops tombstones together with the values they were hiding.
namespace distkv {

struct CompactionStats {
    uint64_t input_tables = 0;
    uint64_t input_entries = 0;
    uint64_t output_entries = 0;
    uint64_t dropped_overwritten = 0;  // older versions of a key
    uint64_t dropped_tombstones = 0;
    uint64_t input_bytes = 0;
    uint64_t output_bytes = 0;
};

// Merges `inputs` into a new SSTable at `out_path`.
//
// `drop_tombstones` may only be true when no table older than the inputs
// exists: a tombstone is the only thing hiding an older value, so dropping it
// while such a value survives elsewhere would bring a deleted key back.
//
// The output's max_seq is the inputs' highest, even if the entry carrying it
// was dropped, so recovery and sequence numbering never move backwards.
Status CompactTables(const std::vector<const SSTableReader*>& inputs, const std::string& out_path,
                     const SSTableOptions& options, bool drop_tombstones, CompactionStats* stats);

}  // namespace distkv

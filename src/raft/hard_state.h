#pragma once

#include <cstdint>
#include <string>

#include "common/status.h"
#include "raft/raft_types.h"

// The part of a Raft node's state that must survive a restart besides the
// log: the latest term it has seen and whom it voted for in that term.
// Forgetting either could let a node vote twice in one term and elect two
// leaders.
//
// On disk it is a fixed 24-byte file:
//   magic:u64  term:u64  voted_for:u32  crc32:u32   (crc over the first 20 bytes)
// replaced atomically (write temp file, fsync, rename, fsync directory), so a
// crash leaves either the old state or the new one, never a mix.
namespace distkv::raft {

struct HardState {
    uint64_t term = 0;
    NodeId voted_for = kNoNode;

    bool operator==(const HardState& o) const {
        return term == o.term && voted_for == o.voted_for;
    }
};

// Loads the state saved at `path`. A missing file is a fresh node: term 0,
// no vote. A damaged file is Corruption, never silently reset, since that
// would forget a vote.
Status LoadHardState(const std::string& path, HardState* out);

// Durably replaces the state at `path`.
Status SaveHardState(const std::string& path, const HardState& state);

}  // namespace distkv::raft

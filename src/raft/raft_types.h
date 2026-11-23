#pragma once

#include <cstdint>
#include <string>
#include <string_view>

// Types shared by the Raft log, its persistent state and the RPC messages.
namespace distkv::raft {

// Nodes are numbered from 1; 0 means "nobody" (e.g. no vote cast this term).
using NodeId = uint32_t;
inline constexpr NodeId kNoNode = 0;

// What a committed entry does to the key-value store.
enum class EntryOp : uint8_t {
    kNoop = 1,    // appended by a new leader to commit entries from earlier terms
    kPut = 2,
    kDelete = 3,
};

// One slot of the replicated log. Indexes start at 1; index 0 is the empty
// log, whose term is 0.
struct LogEntry {
    uint64_t term = 0;
    uint64_t index = 0;
    EntryOp op = EntryOp::kNoop;
    std::string key;    // empty for kNoop
    std::string value;  // empty for kNoop and kDelete

    bool operator==(const LogEntry& o) const {
        return term == o.term && index == o.index && op == o.op && key == o.key &&
               value == o.value;
    }
    bool operator!=(const LogEntry& o) const { return !(*this == o); }
};

// Bytes EncodeLogEntry appends for `entry`.
size_t EncodedSize(const LogEntry& entry);

// Binary form used both in the log file and inside AppendEntries:
//   term:u64  index:u64  op:u8  key_len:u32  key  value_len:u32  value
void EncodeLogEntry(std::string* dst, const LogEntry& entry);

// Reads one entry from the front of `*in` and advances it. Fails on short
// input, an unknown op, or fields an op does not allow (a no-op with a key,
// a delete with a value).
bool DecodeLogEntry(std::string_view* in, LogEntry* entry);

// Raft's election restriction (§5.4.1): a voter only grants its vote to a
// candidate whose log is at least as up to date as its own. Logs are compared
// by the term of their last entry, then by length.
inline bool CandidateLogIsUpToDate(uint64_t candidate_last_term, uint64_t candidate_last_index,
                                   uint64_t own_last_term, uint64_t own_last_index) {
    if (candidate_last_term != own_last_term) {
        return candidate_last_term > own_last_term;
    }
    return candidate_last_index >= own_last_index;
}

}  // namespace distkv::raft

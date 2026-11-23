#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/status.h"
#include "raft/raft_types.h"

// The replicated log of one Raft node, kept in memory and in an append-only
// file. Each entry is one checksummed frame:
//
//   frame := crc32:u32  length:u32  entry[length]     (crc over length + entry)
//
// with the entry encoded by EncodeLogEntry. Entries are contiguous from index
// 1 and their terms never decrease; Open checks both.
//
// Unlike the storage WAL, entries can be removed from the end: a follower
// whose log disagrees with the leader's drops its conflicting suffix
// (TruncateFrom). The file offset of every entry is remembered so that is a
// single ftruncate.
//
// Not thread-safe: the Raft node serialises access under its own lock.
namespace distkv::raft {

// Entries larger than this are rejected on write and treated as damage on read.
inline constexpr uint32_t kMaxLogEntryBytes = 64u << 20;

struct RaftLogRecovery {
    uint64_t entries = 0;
    bool torn_tail = false;  // a frame cut short by a crash was dropped
};

class RaftLog {
  public:
    // Opens (creating if needed) the log at `path` and loads every entry. A
    // torn final frame is cut off; damage before the end is Corruption.
    // With `sync`, every Append and TruncateFrom reaches stable storage
    // before returning, which Raft needs before a node answers an RPC.
    static Status Open(const std::string& path, bool sync, std::unique_ptr<RaftLog>* out,
                       RaftLogRecovery* recovery = nullptr);

    ~RaftLog();
    RaftLog(const RaftLog&) = delete;
    RaftLog& operator=(const RaftLog&) = delete;

    uint64_t last_index() const { return entries_.size(); }
    uint64_t last_term() const { return entries_.empty() ? 0 : entries_.back().term; }

    // Term of the entry at `index`, with index 0 having term 0. Returns false
    // if the log has no such entry.
    bool TermAt(uint64_t index, uint64_t* term) const;

    // The entry at `index`, which must be in [1, last_index()].
    const LogEntry& At(uint64_t index) const { return entries_[index - 1]; }

    // Entries from `from` onwards, at most `max_entries` of them and stopping
    // once `max_bytes` of encoded entries is reached. Always includes the
    // first entry if there is one, however large, so replication progresses.
    std::vector<LogEntry> Slice(uint64_t from, size_t max_entries, size_t max_bytes) const;

    // Leader path: appends entries that continue the log, i.e. the first has
    // index last_index() + 1, indexes are consecutive, terms are positive and
    // never decrease.
    Status Append(const std::vector<LogEntry>& entries);

    // Removes every entry with index >= `index`. A no-op past the end.
    Status TruncateFrom(uint64_t index);

    // Follower path for AppendEntries (§5.3). `entries` continue some prefix
    // of this log (the first index is at most last_index() + 1). Entries
    // already present with the same term are skipped; at the first entry
    // whose term differs, this log's suffix is dropped and the rest appended.
    // Never truncates when nothing conflicts, so a stale, reordered
    // AppendEntries cannot erase newer entries. Sets `*last_new_index` to the
    // index of the last entry in `entries` (or 0 if empty).
    Status AppendFromLeader(const std::vector<LogEntry>& entries, uint64_t* last_new_index);

    uint64_t file_bytes() const { return file_size_; }

  private:
    RaftLog(int fd, bool sync, std::string path) : fd_(fd), sync_(sync), path_(std::move(path)) {}

    Status Validate(const std::vector<LogEntry>& entries) const;
    Status Sync();

    int fd_;
    bool sync_;
    std::string path_;
    std::vector<LogEntry> entries_;
    std::vector<uint64_t> offsets_;  // file offset of each entry's frame
    uint64_t file_size_ = 0;
};

// Exposed for tests: the bytes stored for `entry`.
std::string EncodeLogFrame(const LogEntry& entry);

}  // namespace distkv::raft

#pragma once

#include <cstdint>
#include <functional>

#include "common/log_entry.hpp"
#include "common/status.hpp"

namespace distkv {

// Invoked when a proposed command is resolved. On success `index` is the log
// index the command was committed and applied at; on failure it is 0.
using CommitCallback = std::function<void(const Status& status, std::uint64_t index)>;

// The seam between the request-handling layer and the replication protocol.
// The server only ever talks to this interface, so the protocol underneath can
// be swapped without touching request handling:
//   - LocalReplicator:  single node, no networking (Day 1).
//   - LeaderReplicator: static leader streams its log to followers (Day 8).
//   - RaftReplicator:   leader election + log replication (Phase 2).
//
// Contract:
//   - propose() is asynchronous: it may return before the command is
//     committed. It is safe to call from any thread.
//   - The callback is invoked exactly once per propose() call, from an
//     unspecified thread (possibly the calling thread, before propose()
//     returns). It is never invoked while internal locks are held, so it may
//     call propose() again.
//   - On success (status.is_ok()), the command has been committed and applied
//     to the StateMachine before the callback runs, so a read issued from the
//     callback observes the write.
//   - On failure, the command was not committed by this call (for example
//     kInvalidArgument, or kNotLeader on a follower). Note that a
//     kUnavailable from a networked replicator can mean "outcome unknown".
//   - A null callback is allowed (fire-and-forget).
//   - Committed indexes are 1-based, strictly increasing and gap-free.
class Replicator {
public:
    Replicator() = default;
    virtual ~Replicator() = default;

    Replicator(const Replicator&) = delete;
    Replicator& operator=(const Replicator&) = delete;
    Replicator(Replicator&&) = delete;
    Replicator& operator=(Replicator&&) = delete;

    virtual void propose(Command command, CommitCallback on_commit) = 0;

    // Whether this node currently believes it may accept writes.
    [[nodiscard]] virtual bool is_leader() const = 0;

    // Highest log index known to be committed (0 if none).
    [[nodiscard]] virtual std::uint64_t commit_index() const = 0;

    // Current leadership epoch.
    [[nodiscard]] virtual std::uint64_t current_term() const = 0;
};

}  // namespace distkv

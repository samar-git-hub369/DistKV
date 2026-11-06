#pragma once

#include <atomic>
#include <cstdint>
#include <mutex>

#include "replication/replicator.hpp"
#include "storage/state_machine.hpp"

namespace distkv {

// Single-node Replicator: no peers, so every proposal commits immediately.
// It is always the leader and stays in term 1. Useful as the Day 1 backend
// and as a reference implementation of the Replicator contract in tests.
//
// Thread safety: propose() may be called from any number of threads.
// last_index_ and the apply() call are guarded by mutex_; commit_index_ is an
// atomic; state_machine_ is a reference fixed at construction. Lock order:
// mutex_ is held while apply() takes KvStore::mutex_, never the reverse. The
// commit callback runs after mutex_ is released, on the proposing thread.
class LocalReplicator final : public Replicator {
public:
    static constexpr std::uint64_t kTerm = 1;

    // `state_machine` must outlive this object.
    explicit LocalReplicator(StateMachine& state_machine);

    void propose(Command command, CommitCallback on_commit) override;

    [[nodiscard]] bool is_leader() const override { return true; }
    [[nodiscard]] std::uint64_t commit_index() const override;
    [[nodiscard]] std::uint64_t current_term() const override { return kTerm; }

private:
    StateMachine& state_machine_;

    // Serialises index assignment and apply so that entries reach the state
    // machine one at a time and in index order.
    std::mutex mutex_;
    std::uint64_t last_index_{0};  // Guarded by mutex_.

    // Written under mutex_, readable without it.
    std::atomic<std::uint64_t> commit_index_{0};
};

}  // namespace distkv

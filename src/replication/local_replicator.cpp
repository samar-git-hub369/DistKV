#include "replication/local_replicator.hpp"

#include <utility>

namespace distkv {

LocalReplicator::LocalReplicator(StateMachine& state_machine) : state_machine_(state_machine) {}

void LocalReplicator::propose(Command command, CommitCallback on_commit) {
    // Validate before taking an index, so rejected commands leave no hole.
    if (command.key.empty()) {
        if (on_commit) {
            on_commit(Status::invalid_argument("key must not be empty"), 0);
        }
        return;
    }

    std::uint64_t index = 0;
    {
        std::lock_guard lock(mutex_);
        index = ++last_index_;
        const LogEntry entry{index, kTerm, std::move(command)};
        state_machine_.apply(entry);
        commit_index_.store(index, std::memory_order_release);
    }

    // Outside the lock: the callback may call propose() again.
    if (on_commit) {
        on_commit(Status::ok(), index);
    }
}

std::uint64_t LocalReplicator::commit_index() const {
    return commit_index_.load(std::memory_order_acquire);
}

}  // namespace distkv

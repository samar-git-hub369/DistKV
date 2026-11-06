#include "storage/kv_store.hpp"

#include <cstdlib>
#include <iostream>
#include <mutex>

namespace distkv {

void KvStore::apply(const LogEntry& entry) {
    std::unique_lock lock(mutex_);

    // Idempotent replay: an entry at or below last_applied_ has already been
    // applied, so it is skipped silently. On restart (Day 5) the write-ahead
    // log is replayed from the last snapshot or from the beginning, and the
    // snapshot/WAL boundary may overlap. Skipping by index makes replaying the
    // same prefix any number of times harmless, without needing to know
    // exactly where the previous apply stopped.
    if (entry.index <= last_applied_) {
        return;
    }

    // A gap means an entry was lost or reordered upstream: a bug in the
    // Replicator or the log. Continuing would silently diverge from other
    // replicas, and the StateMachine contract forbids throwing, so fail fast.
    if (entry.index != last_applied_ + 1) {
        std::cerr << "distkv: KvStore::apply: log gap: expected index " << last_applied_ + 1
                  << ", got " << entry.index << std::endl;
        std::abort();
    }

    const Command& command = entry.command;
    switch (command.op) {
        case OpType::kSet:
            map_.insert_or_assign(command.key, command.value);
            break;
        case OpType::kDelete:
            // Deleting a missing key is not an error.
            if (const auto it = map_.find(command.key); it != map_.end()) {
                map_.erase(it);
            }
            break;
    }
    last_applied_ = entry.index;
}

std::optional<std::string> KvStore::get(std::string_view key) const {
    std::shared_lock lock(mutex_);
    if (const auto it = map_.find(key); it != map_.end()) {
        return it->second;
    }
    return std::nullopt;
}

std::size_t KvStore::size() const {
    std::shared_lock lock(mutex_);
    return map_.size();
}

std::uint64_t KvStore::last_applied_index() const {
    std::shared_lock lock(mutex_);
    return last_applied_;
}

}  // namespace distkv

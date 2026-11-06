#pragma once

#include "common/log_entry.hpp"

namespace distkv {

// The deterministic component that committed log entries are applied to (the
// key-value map, later). It is the "state machine" of state machine
// replication: every replica applies the same log, so every replica must end
// up in the same state.
//
// Contract for implementations:
//   - Deterministic: the result of apply() depends only on the current state
//     and the entry. No clocks, randomness, thread ids or I/O-dependent
//     branching, otherwise replicas diverge.
//   - Entries are applied in strictly increasing index order, with no gaps,
//     each exactly once.
//   - apply() is called one entry at a time, never concurrently. The caller
//     (a Replicator) serialises calls, so implementations need no locking
//     for writes. Concurrent reads from other threads are the
//     implementation's own concern.
//   - apply() must not throw and must not call back into the Replicator.
class StateMachine {
public:
    StateMachine() = default;
    virtual ~StateMachine() = default;

    StateMachine(const StateMachine&) = delete;
    StateMachine& operator=(const StateMachine&) = delete;
    StateMachine(StateMachine&&) = delete;
    StateMachine& operator=(StateMachine&&) = delete;

    virtual void apply(const LogEntry& entry) = 0;
};

}  // namespace distkv

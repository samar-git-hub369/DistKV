#pragma once

#include <cstdint>
#include <string>
#include <utility>

namespace distkv {

// The kind of mutation a command performs. Values are explicit because they
// will be written to the wire and to disk; never renumber them.
enum class OpType : std::uint8_t {
    kSet = 1,
    kDelete = 2,
};

// A client-requested mutation of the key-value state. Reads are not commands:
// they never enter the log.
struct Command {
    OpType op{OpType::kSet};
    std::string key;
    std::string value;  // Empty for kDelete.

    static Command set(std::string key, std::string value) {
        return Command{OpType::kSet, std::move(key), std::move(value)};
    }

    static Command del(std::string key) { return Command{OpType::kDelete, std::move(key), {}}; }

    friend bool operator==(const Command&, const Command&) = default;
};

// One slot in the replicated log.
//
// `index` and `term` exist from day one even though the single-node replicator
// only ever uses term 1. They are the coordinates Raft (Phase 2) needs:
//   - index: 1-based, gap-free position in the log. It gives every write a
//     total order and lets replicas say "I have applied up to N".
//   - term: the leadership epoch in which the entry was created. Raft uses
//     (index, term) pairs to detect divergent logs and to decide which
//     entries are safe to commit.
// Carrying them through every interface now means the storage, the state
// machine and the wire format do not have to change shape later.
struct LogEntry {
    std::uint64_t index{0};
    std::uint64_t term{0};
    Command command;

    friend bool operator==(const LogEntry&, const LogEntry&) = default;
};

}  // namespace distkv

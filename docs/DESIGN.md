# DistKV design

DistKV is a fault-tolerant, replicated key-value store written in C++20. This
note records the shape of the system and the decisions behind it. It is kept
to one page on purpose and updated as the design changes.

## Goals

- **Linearizable writes** through a single leader: a successful write is
  durable on a majority (Phase 2) and visible to every later read on the leader.
- **Survive node failures** without losing acknowledged writes, and elect a new
  leader automatically (Raft, Phase 2).
- **Pluggable replication** behind one interface, so the system can grow from a
  single node to leader-follower to Raft without rewriting request handling.
- **Testable and observable**: deterministic state machine, sanitizer-clean
  builds (ASan/UBSan/TSan) and CI on GCC and Clang.

## Non-goals

- Byzantine fault tolerance, multi-key transactions, sharding or cross-shard
  rebalancing.
- Secondary indexes, range scans, TTLs.
- Beating production stores (etcd, Redis) on throughput. Clarity first.

## Architecture

```
                +------------------------ distkv-server -------------------------+
                |                                                                |
 client --TCP-->| Network (Asio) --> Request handler ---propose()---> Replicator  |<--> peers
                |                     |        ^                        |        |   (Day 8+)
                |                     |        +---commit callback------+        |
                |               reads |                                 | apply(entry),
                |                     v                                 | in index order
                |              StateMachine (KV) <----------------------+        |
                +----------------------------------------------------------------+
```

## Life of a write

1. A client sends `SET k v` to the leader.
2. The request handler builds a `Command` and calls `Replicator::propose()`.
3. The replicator assigns the next log index under the current term, producing
   a `LogEntry{index, term, command}`.
4. The entry is replicated (Day 1: nothing to do; Day 8: followers; Phase 2: a
   majority acknowledges it) and becomes committed.
5. Committed entries are applied to the `StateMachine` strictly in index order.
6. The commit callback fires with `Status::ok()` and the index; the handler
   replies to the client. On failure (`kNotLeader`, `kUnavailable`, ...) the
   client is told why and where to retry.

## Key abstractions

| Type | Role |
| --- | --- |
| `Command` | A mutation (`kSet`, `kDelete`) on one key. Reads are not commands. |
| `LogEntry` | `{index, term, command}`: a slot in the replicated log. |
| `Status` | Error code + message returned by value; no exceptions across layers. |
| `StateMachine` | Deterministic target of committed entries, applied one at a time, in order. |
| `Replicator` | `propose(Command, CommitCallback)`, `is_leader()`, `commit_index()`, `current_term()`. Implemented by `LocalReplicator` (Day 1), `LeaderReplicator` (Day 8) and `RaftReplicator` (Phase 2). |

## Decisions

- **Index and term from day one.** They cost nothing now and keep the log,
  state machine and (future) wire format stable when Raft arrives.
- **Asynchronous propose with a callback**, called exactly once and after
  apply on success. Networked replicators cannot answer synchronously, so the
  interface does not pretend they can. Callbacks run outside internal locks so
  they may re-enter `propose()`.
- **Validate before assigning an index**, so rejected commands never leave
  holes in the log.
- **Status values, not exceptions**, because "not leader" and "unavailable" are
  normal outcomes in a distributed system.
- **Standalone Asio** for networking: mature, header-only, and no Boost.
- **Dependencies pinned** via FetchContent tags for reproducible builds.

## Consistency model

- **Day 1 (`LocalReplicator`)**: one node, so trivially linearizable; no fault
  tolerance.
- **Day 8 (`LeaderReplicator`)**: a statically configured leader streams its
  log to followers. Reads from the leader are consistent, reads from followers
  may be stale. **Known limitation: split brain.** There is no election and no
  term-based fencing, so if an operator promotes a follower while the old
  leader is partitioned but alive, both accept writes and their logs diverge.
  Acknowledged writes can be lost when the partition heals. This is the
  motivating problem for Phase 2.
- **Phase 2 (`RaftReplicator`)**: writes commit only on a majority, terms fence
  stale leaders, and the (index, term) log matching property guarantees that
  committed entries are never lost or reordered. Linearizable reads go through
  the leader with a read-index or lease check.

## Roadmap

- **Phase 1 (Days 1-12)**: build system and core types; in-memory KV state
  machine; text protocol and client; write-ahead log and recovery;
  leader-follower replication; failure tests.
- **Phase 2 (Days 13-25)**: Raft leader election, log replication, persistence
  of term/vote, snapshots and log compaction, membership changes, fault
  injection and Jepsen-style linearizability checks, benchmarks.

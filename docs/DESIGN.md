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
 client --RESP/TCP--> Server / Session     src/network: framing, reply ordering, backpressure
                            | args
                            v
                     CommandDispatcher     src/server: Redis commands -> KvService calls
                            |
                            v
                        KvService ---propose()---> Replicator <----> peers (Day 8+)
                         |      ^                    |
                   reads |      +--commit callback---+ apply(entry), in index order
                         v                           |
                      KvStore (StateMachine) <-------+
```

## Life of a write

1. A client sends `SET k v` (a RESP array) to the leader.
2. Its `Session` parses the frame and reserves a reply slot; the
   `CommandDispatcher` calls `KvService::set()`, which builds a `Command` and
   calls `Replicator::propose()`.
3. The replicator assigns the next log index under the current term, producing
   a `LogEntry{index, term, command}`.
4. The entry is replicated (Day 1: nothing to do; Day 8: followers; Phase 2: a
   majority acknowledges it) and becomes committed.
5. Committed entries are applied to the `StateMachine` strictly in index order.
6. The commit callback fires with `Status::ok()` and the index; the encoded
   `+OK` is posted back to the session's strand, fills its slot and is written
   once every earlier reply has been. On failure (`kNotLeader`,
   `kUnavailable`, ...) the client gets a RESP error saying why.

## Key abstractions

| Type | Role |
| --- | --- |
| `Command` | A mutation (`kSet`, `kDelete`) on one key. Reads are not commands. |
| `LogEntry` | `{index, term, command}`: a slot in the replicated log. |
| `Status` | Error code + message returned by value; no exceptions across layers. |
| `StatusOr<T>` | A value or a non-OK `Status`, for operations that return a result. |
| `StateMachine` | Deterministic target of committed entries, applied one at a time, in order. |
| `KvStore` | The `StateMachine`: an in-memory, binary-safe `string -> string` map. |
| `KvService` | Protocol-independent facade: `set`/`del` go through `propose()`, `get` reads the store (leader only). |
| `CommandDispatcher` | Redis commands (`GET`, `SET`, `DEL`, ...) on top of `KvService`; no sockets. |
| `Session` / `Server` | One RESP connection with ordered, batched replies and backpressure / the acceptor that owns sessions. |
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
- **`KvStore` uses a `std::shared_mutex`.** Reads dominate a KV workload and
  can run in parallel under a shared lock. Writes take an exclusive lock, but
  they are already serialised by the Replicator (one `apply()` at a time), so
  finer-grained locking or sharding would buy nothing yet. Lookups use
  transparent hashing so a `string_view` key is never copied into a `string`.
- **Idempotent replay.** `KvStore::apply()` silently skips any entry whose
  index is at or below `last_applied_index()`. Recovery (Day 5) can then
  replay the write-ahead log from a snapshot or from the start, even if the
  replayed range overlaps what is already applied, without double-applying.
- **Abort on a gap.** An entry whose index is not `last_applied_index() + 1`
  means an entry was lost or reordered upstream, which is a bug. Applying it
  would silently diverge from other replicas, and `apply()` must not throw, so
  the store logs the expected and actual index and calls `std::abort()`.
- **Status values, not exceptions**, because "not leader" and "unavailable" are
  normal outcomes in a distributed system.
- **Standalone Asio** for networking: mature, header-only, and no Boost.
- **Dependencies pinned** via FetchContent tags for reproducible builds.

## Protocol

DistKV speaks RESP2, the Redis protocol, so existing tools work unchanged.

- **Framing.** Requests are arrays of bulk strings
  (`*2\r\n$3\r\nGET\r\n$1\r\nk\r\n`), as every Redis client sends, or inline
  commands (`GET k\r\n`) for nc/telnet. The parser is incremental and
  stateless: given a buffer it reports a complete frame plus bytes consumed,
  "incomplete", or a protocol error, and never reads past the buffer. Bulk
  payloads are binary-safe because the length prefix is trusted, so values may
  contain `\r\n` and `\0`.
- **Limits.** 1 MiB per bulk string, 1024 arguments per request, 64 KiB per
  inline line and 8 MiB per request in total. Exceeding one, a bad type byte, a
  negative or non-numeric length, or a missing CRLF after a bulk is a protocol
  error: the server replies `-ERR Protocol error: <msg>` after any earlier
  replies and closes the connection. The limits also bound how much a
  half-sent request can make the server buffer.
- **Reply ordering.** Clients may pipeline. Each request gets a slot in a
  per-session FIFO when it is dispatched; a reply fills its slot and filled
  slots are written strictly from the front. A later request whose commit
  completes first therefore waits for the earlier ones. Commit callbacks may
  arrive on any thread (a networked replicator completes on its own threads),
  so they only post the reply back to the session's strand; all session state
  is touched on that strand. At most one write is in flight per session, and
  replies that complete meanwhile are batched into the next one.
- **Errors.** `Status` codes map to RESP errors: `kInvalidArgument` ->
  `-ERR <msg>`, `kNotLeader` -> `-NOTLEADER <msg>`, `kUnavailable` ->
  `-TRYAGAIN <msg>`, `kInternal` -> `-ERR internal: <msg>`. Wrong arity and
  unknown commands use Redis's own wording.
- **Backpressure.** A session stops parsing and reading while 1024 replies are
  pending or 4 MiB of output is queued, and resumes as the backlog drains. A
  client that pipelines without reading cannot grow server memory without
  bound; TCP flow control pushes back on it instead.
- **Connection limit.** At most `--max-connections` (default 10000) clients
  are connected at once. Like Redis, a client accepted beyond that gets
  `-ERR max number of clients reached` and is closed; existing clients are
  unaffected. A slot is taken when a session is created and released when its
  connection closes, through a counter shared with the sessions (an atomic,
  because slots are released on session strands while the acceptor takes them
  on its own). The OS file-descriptor limit (`ulimit -n`) may be the tighter
  bound; if `accept()` runs out of descriptors the acceptor backs off for
  100 ms instead of spinning.
- **Known limitation: DEL's count.** `DEL k1 k2 ...` proposes one delete per key
  and replies with how many of the distinct keys existed when checked just
  before proposing. Under concurrent writers the count can be off (another
  client may create or delete a key between the check and the apply). It will
  be exact once the state machine reports apply results back through the
  commit callback.

## Threading model

`distkv-server --threads N` runs `io_context::run()` on N threads (the main
thread is one of them; the default is the number of hardware threads). Nothing
is pinned to a thread; what keeps state consistent is the executor or lock each
piece of code runs under:

| Code | Runs on | Protects |
| --- | --- | --- |
| `Server` accept loop, `stop()`'s body | the server's strand | the acceptor and its retry timer |
| `Session` handlers (read, parse, dispatch, reply, write, close) | that session's strand, one per connection | all session state |
| `CommandDispatcher`, `KvService` | the calling session's strand | nothing to protect: immutable after construction |
| `LocalReplicator::propose()` | the caller's thread | `last_index_` and `apply()` under `LocalReplicator::mutex_` |
| `KvStore` | the caller's thread | `map_`, `last_applied_` under `KvStore::mutex_` (shared for reads) |
| Commit callbacks | the replicator's completing thread | they only post to a session strand (or lock a per-request mutex, for DEL) |

Any number of sessions run in parallel on different threads; one session never
runs on two threads at once. Other shared objects are protected as follows:
`Server::sessions_` and `stopped_` by `Server::sessions_mutex_`; the open
connection count by an atomic; `LocalReplicator::commit_index_` by an atomic;
a DEL's per-key completion state by a mutex in a heap object shared by its
callbacks. Each class's header states the same.

**Lock hierarchy.** `LocalReplicator::mutex_` -> `KvStore::mutex_`: the
replicator applies entries while holding its mutex, so the store's lock is
always taken second and is the innermost lock; nothing is called while holding
it. `Server::sessions_mutex_` is a leaf: the server copies what it needs under
it and releases it before calling into a `Session` or the dispatcher.

**No calls under locks.** Commit callbacks, reply callbacks and dispatcher
calls are never made while holding a lock. A callback may therefore re-enter
`propose()` or post to a strand without risk of deadlock, and a slow callback
never blocks other threads.

**Shutdown.** `Server::stop()` may be called from any thread and more than
once: it posts to the server's strand, which closes the acceptor and posts a
close to every live session. Once their handlers finish (and the signal set is
cancelled or has fired) the io_context runs out of work, every thread's
`run()` returns and `main` joins them. An exception that escapes a handler
unwinds out of `run()` on that thread only; `main` logs it with the thread's
index, starts the same shutdown, re-enters `run()` to help drain it, and exits
non-zero after joining every thread rather than calling `std::terminate`.

## Consistency model

- **Day 1 (`LocalReplicator`)**: one node, so trivially linearizable; no fault
  tolerance.
- **Day 8 (`LeaderReplicator`)**: a statically configured leader streams its
  log to followers. Reads are served by the leader only (followers answer
  `kNotLeader`), so a client never reads from a lagging follower. **Known limitation: split brain.** There is no election and no
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

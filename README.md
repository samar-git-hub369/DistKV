# DistKV

A fault-tolerant distributed key-value store in C++20, built step by step from
a single node to Raft consensus.

[![CI](https://github.com/samar-git-hub369/DistKV/actions/workflows/ci.yml/badge.svg)](https://github.com/samar-git-hub369/DistKV/actions/workflows/ci.yml)

## Status

Day 3 of 25: a single-node server that speaks RESP2, so `redis-cli` and Redis
client libraries can talk to it. It supports `PING`, `ECHO`, `GET`, `SET`,
`DEL` and `COMMAND`, with pipelining and in-order replies. Writes go through the
`Replicator` interface (today a single-node `LocalReplicator`) into a
thread-safe in-memory `KvStore`. Data is not yet persisted (Day 5). See
[docs/DESIGN.md](docs/DESIGN.md) for the architecture and the reasoning behind it.

## Roadmap

- [x] Build system, CI with sanitizers, core types, replication interface
- [x] Thread-safe in-memory KV state machine (`KvStore`) and `KvService`
- [x] RESP2 network layer (`PING` / `ECHO` / `GET` / `SET` / `DEL`), redis-cli compatible
- [x] Command-line client (`distkv-cli`)
- [ ] Multi-threaded I/O
- [ ] Write-ahead log and crash recovery
- [ ] Leader-follower replication (`LeaderReplicator`)
- [ ] Failure and partition tests
- [ ] Raft leader election
- [ ] Raft log replication and persistence (`RaftReplicator`)
- [ ] Snapshots and log compaction
- [ ] Cluster membership changes
- [ ] Linearizability checking and benchmarks

## Building

Requirements: CMake 3.20+, a C++20 compiler (GCC 12+, Clang 15+ or Apple Clang),
and network access on first configure (Asio and GoogleTest are fetched at
pinned tags).

```bash
cmake --preset debug
cmake --build --preset debug -j
ctest --preset debug
```

Presets: `debug`, `release`, `asan` (AddressSanitizer + UBSan), `tsan`
(ThreadSanitizer). Add `-DDISTKV_WERROR=ON` to the configure step to fail on
warnings, as CI does. Binaries land in `build/<preset>/bin/`.

## Running

```bash
./build/debug/bin/distkv-server --id 1 --port 7000
```

Talk to it with `redis-cli`:

```bash
redis-cli -p 7000 set k v
```

```bash
redis-cli -p 7000 get k
```

Or with the bundled `distkv-cli`, which prints replies the same way and runs a
REPL when given no command:

```bash
./build/debug/bin/distkv-cli -p 7000 set k v
```

```bash
./build/debug/bin/distkv-cli -p 7000
```

```
127.0.0.1:7000> get k
"v"
127.0.0.1:7000> del k
(integer) 1
127.0.0.1:7000> get k
(nil)
```

Inline commands work too, for quick checks with `nc` (pipelined commands get
replies in order):

```bash
printf 'PING\r\nSET a b\r\nGET a\r\n' | nc 127.0.0.1 7000
```

Stop the server with Ctrl+C (or `SIGTERM`): it stops accepting, closes client
connections and exits cleanly. Run either binary with `--help` for all options.

On macOS, the AirPlay Receiver listens on port 7000. If the server reports
that the port is already in use, pass another port (`--port 7001`, and
`-p 7001` for the clients) or turn off AirPlay Receiver in System Settings.

## Layout

```
cmake/            Warnings, sanitizers and dependency setup
docs/DESIGN.md    Design note
src/common/       Command, LogEntry, Status, StatusOr
src/storage/      StateMachine interface, KvStore
src/replication/  Replicator interface, LocalReplicator
src/protocol/     RESP2 request parser and reply encoder
src/network/      Session (one per connection) and Server (acceptor)
src/server/       KvService, CommandDispatcher and the distkv-server entry point
tools/            distkv-cli
tests/            GoogleTest suites, including end-to-end TCP tests
```

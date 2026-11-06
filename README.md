# DistKV

A fault-tolerant distributed key-value store in C++20, built step by step from
a single node to Raft consensus.

[![CI](https://github.com/samar-git-hub369/DistKV/actions/workflows/ci.yml/badge.svg)](https://github.com/samar-git-hub369/DistKV/actions/workflows/ci.yml)

## Status

Day 2 of 25: the build system, core types (`Command`, `LogEntry`, `Status`,
`StatusOr`), the `Replicator` interface with a single-node `LocalReplicator`,
a thread-safe in-memory `KvStore` and the `KvService` facade over them, and a
server skeleton that accepts TCP connections. The server does not speak a
client protocol yet (Day 3). See
[docs/DESIGN.md](docs/DESIGN.md) for the architecture and the reasoning behind it.

## Roadmap

- [x] Build system, CI with sanitizers, core types, replication interface
- [x] Thread-safe in-memory KV state machine (`KvStore`) and `KvService`
- [ ] Text protocol (`SET` / `GET` / `DEL`)
- [ ] Command-line client
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

```bash
nc 127.0.0.1 7000
```

The server replies with a banner and closes the connection. Stop it with
Ctrl+C (or `SIGTERM`); it stops accepting and exits cleanly. Run with `--help`
for all options.

## Layout

```
cmake/            Warnings, sanitizers and dependency setup
docs/DESIGN.md    Design note
src/common/       Command, LogEntry, Status, StatusOr
src/storage/      StateMachine interface, KvStore
src/replication/  Replicator interface, LocalReplicator
src/server/       KvService and the distkv-server entry point
tests/            GoogleTest suites
```

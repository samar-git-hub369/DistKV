# DistKV

A distributed key-value store written from scratch in C++17: an LSM-tree storage engine, Raft consensus across a three-node cluster, an ML-driven cache eviction policy served in-process with ONNX Runtime, a FastAPI layer with natural-language queries, and a React dashboard for live metrics.

> Work in progress. The roadmap below tracks what exists today.

## Planned architecture

```
 clients ── REST / WebSocket / natural language
    │
 FastAPI layer (Python) ─── React + Recharts dashboard
    │  TCP text protocol
 Raft consensus (3 nodes, Boost.Asio)
    │  in-process
 Storage engine per node
    WAL → MemTable → SSTables (+ Bloom filters) → compaction
    ML cache (XGBoost model, ONNX Runtime)
```

## Roadmap

- [x] Project scaffold, CMake build, GoogleTest
- [x] Write-ahead log with checksums and crash recovery
- [x] MemTable with tombstones, flush threshold and WAL recovery
- [ ] SSTable format
- [ ] Bloom filters and the storage engine read/write path
- [ ] Background compaction
- [ ] TCP server and text protocol
- [ ] Raft: leader election, log replication, failover
- [ ] Benchmarks
- [ ] ML eviction model and in-process inference
- [ ] FastAPI, natural-language queries, dashboard
- [ ] Docker Compose cluster

## Building

Requires CMake 3.20+ and a C++17 compiler (GCC or Clang). GoogleTest is fetched automatically.

```bash
cmake -S . -B build
cmake --build build -j
ctest --test-dir build --output-on-failure
```

## Write-ahead log

Every mutation is appended to the WAL before it is applied in memory. Each frame is

```
crc32:u32 | length:u32 | op:u8 | seq:u64 | key_len:u32 | key | value_len:u32 | value
```

with little-endian integers. The checksum covers the length field as well as the payload.

On startup `RecoverWal` replays the log. A damaged or incomplete frame at the very end of the file is a write cut short by a crash; it is dropped and the file is truncated so new appends follow the last good frame. A damaged frame with valid data after it is reported as corruption rather than silently skipped.

`WalSyncMode::kEveryAppend` syncs after each append so an acknowledged write survives power loss. On macOS it uses `F_FULLFSYNC`, because plain `fsync` there does not flush the drive's cache.

## MemTable

The MemTable is the in-memory write buffer: a sorted map from key to `{seq, tombstone, value}`, guarded by a `std::shared_mutex` so reads run in parallel and writes are exclusive. Keys sort in unsigned byte order, so a flush to an SSTable is one sequential pass.

- **Tombstones.** A delete is stored as a marker, not removed. `Get` returns one of `kFound`, `kDeleted` or `kNotPresent`. `kDeleted` tells the read path to stop instead of falling through to an older SSTable that may still hold the value.
- **Sequence numbers.** Every write carries the sequence number it was logged with. A write that is not newer than what the table already holds is ignored, so replaying a WAL twice leaves the table unchanged.
- **Flush threshold.** The table tracks key bytes, value bytes and a fixed per-entry overhead. `ShouldFlush()` turns true once that reaches the configured size.
- **Recovery.** `RecoverMemTable` replays the WAL into a fresh table and trims a torn tail, restoring exactly the writes that were durably logged.

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
- [x] SSTable format: writer, sparse index, checksummed point lookups
- [ ] SSTable iteration and MemTable flush
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

## SSTables

A full MemTable is written out as an SSTable: an immutable file of entries sorted by key.

```
data block 0 | data block 1 | ... | index block | footer (40 bytes)

data block  := entry* crc32
entry       := key_len:u32 key seq:u64 type:u8 value_len:u32 value
index block := smallest_key handle* crc32
handle      := last_key offset:u64 size:u32
footer      := index_offset:u64 index_size:u32 entry_count:u64 max_seq:u64 crc32 magic:u64
```

- **Sparse index.** Entries are packed into blocks of about 4 KiB, and the index holds one handle per block (its last key and position), not one per key. The reader keeps the index in memory, binary-searches it for the one block that could contain a key, and reads only that block with a single `pread`.
- **Checksums everywhere.** The footer, index and every data block end in a CRC-32. Opening a table verifies the footer and index; a damaged data block is reported as `Corruption` when a read touches it.
- **Atomic publish.** The writer streams to `<name>.tmp`, syncs it, renames it into place and syncs the directory. A crash mid-flush leaves either no table or a complete one, never a partial file under the real name.
- **Tombstones are kept.** A delete is written as a tombstone entry, so it keeps hiding older values in older tables until compaction removes both.
- **Concurrency.** A reader is immutable after `Open`, and `pread` does not share a file offset, so any number of threads can look up keys in the same table at once.

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
- [x] SSTable iteration and MemTable flush
- [x] Bloom filters and the storage engine read/write path
- [x] Background compaction
- [x] TCP server and text protocol (Boost.Asio)
- [ ] Raft: leader election, log replication, failover
- [ ] Benchmarks
- [ ] ML eviction model and in-process inference
- [ ] FastAPI, natural-language queries, dashboard
- [ ] Docker Compose cluster

## Building

Requires CMake 3.20+, a C++17 compiler (GCC or Clang) and Boost 1.74+ (headers only, for Asio). GoogleTest is fetched automatically.

```bash
brew install boost                # macOS
sudo apt-get install libboost-dev # Debian / Ubuntu
```

```bash
cmake -S . -B build
cmake --build build -j
ctest --test-dir build --output-on-failure
```

## Running a node

```bash
./build/src/kvstore_server --port=7001 --data-dir=./data --threads=4
```

| Flag | Default | Meaning |
|---|---|---|
| `--host` | `0.0.0.0` | listen address |
| `--port` | `7001` | client port |
| `--data-dir` | `./data` | where WAL and SSTable files live |
| `--threads` | `4` | io threads serving connections |
| `--max-connections` | `1024` | extra clients get `ERR too many connections` |
| `--memtable-mb` | `4` | MemTable flush threshold |
| `--sync` | `always` | `always` fsyncs every write; `none` leaves it to the OS |

SIGINT or SIGTERM stops it cleanly: connections are closed after their in-flight request, and the engine waits for any running flush or compaction.

```console
$ nc localhost 7001
PUT city New Delhi
OK
GET city
VALUE New Delhi
DELETE city
OK
GET city
NOT_FOUND
STATUS
STATUS role=standalone last_seq=2 sstables=0 memtable_bytes=68 flushes=0 compactions=0 requests=5
```

## Text protocol

One request per line (`\n` or `\r\n`), one response line per request, in order.

| Request | Response |
|---|---|
| `PUT <key> <value>` (alias `SET`) | `OK` — the value is the rest of the line, spaces included |
| `GET <key>` | `VALUE <value>` or `NOT_FOUND` |
| `DELETE <key>` (alias `DEL`) | `OK` |
| `STATUS` | `STATUS key=value ...` |
| `PING` | `PONG` |
| anything malformed | `ERR <reason>` |

Commands are case-insensitive; keys are case-sensitive and cannot contain spaces. Lines longer than 1 MiB get `ERR request line too long` and the connection is closed, since the rest of the stream can no longer be framed.

## Networking

`TcpServer` runs one Boost.Asio `io_context` on a pool of threads. Each connection gets its own strand, so its handlers never run concurrently while different connections are served in parallel.

- **Read, handle, write, repeat.** A connection reads one line, runs it, writes the response, and only then reads the next line. Responses therefore come back in request order, and a client that stops reading stops being read from: natural backpressure with no unbounded queues. Pipelined requests wait in the connection's buffer.
- **Bounded memory.** The read buffer is capped at 1 MiB per connection, and the connection limit caps how many buffers exist.
- **Shutdown.** `Stop()` closes the acceptor and posts a close onto every connection's strand, then lets the worker threads drain and exit. Sessions are tracked through weak pointers, so shutdown never keeps a closing connection alive or touches one being destroyed.
- **Backend interface.** Requests go through a `KvBackend` interface. Today `StandaloneBackend` calls the local `StorageEngine` directly; with Raft, a replicated backend will take its place without the network layer changing.

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
- **Iteration.** `SSTableIterator` walks a table in key order, tombstones included, holding one block in memory at a time. `Seek(target)` binary-searches the index for the first block that can hold a key `>= target`, then scans within it. Point lookups (`Get`) are a `Seek` plus a key comparison, and keys outside the table's smallest/largest range return without touching disk. Range scans and compaction (which merges several tables) build on the same iterator.

## Flushing

`FlushMemTable` writes a MemTable, tombstones included, to a new SSTable in one sequential pass and reports the highest sequence number it made durable. Because the SSTable is published atomically, a flush either fully succeeds or leaves nothing behind. Once it succeeds, WAL records up to that sequence number are no longer needed for recovery.

## Bloom filters

Every SSTable carries a Bloom filter over its keys (10 bits per key by default, about 1% false positives), stored in a checksummed block between the data and the index. The reader keeps it in memory and checks it before touching the index, so a lookup for a key the table does not hold almost never reads from disk. That matters because a missing key would otherwise cost one block read in every SSTable.

The filter uses double hashing: one 64-bit hash (FNV-1a with a splitmix64 finalizer) is split into two 32-bit halves, and probe *i* tests bit `(h1 + i·h2) mod m`. The number of probes is `bits_per_key · ln 2`, which minimises false positives. A missing or damaged filter answers "maybe" for every key, so it can cost an extra read but can never hide data.

## Storage engine

`StorageEngine` ties the pieces together for one node:

```
Put/Delete ─► WAL append ─► active MemTable
                                │ full
                                ▼
                      frozen MemTable ─► background flush ─► NNNNNN.sst
Get ─► active MemTable ─► frozen MemTable ─► SSTables, newest first (Bloom filter first)
```

- **Writes** are serialised by one mutex so WAL order equals sequence order. A write becomes visible to readers only after its WAL append returns.
- **Freezing.** When the active MemTable reaches `memtable_bytes`, it is frozen, a new MemTable and WAL file take over, and a background thread flushes the frozen one. If the previous flush is still running, the writer waits; that is the engine's backpressure.
- **Publishing.** The flush thread deletes the frozen MemTable's WAL once the SSTable is durable, then swaps in a new table list and drops the frozen MemTable under one lock, so a reader always finds the data in one place or the other.
- **Reads** take the lock only to copy three pointers (active MemTable, frozen MemTable, table list); the search itself runs unlocked. The table list is copy-on-write, so a reader's snapshot stays valid while flushes publish new tables.
- **Recovery.** `Open` removes leftover `.tmp` files, opens the SSTables, replays every WAL into one MemTable, flushes it, and deletes the WALs. If the process died after a flush published its SSTable but before deleting the WAL, that WAL's records have sequence numbers no higher than the newest SSTable's, so replay skips them; genuinely unflushed writes are always higher.
- **Errors.** If a background flush fails, its MemTable stays readable and later writes fail fast with the error; the data is still in the WAL for the next `Open`.

## Compaction

Every write adds a new version instead of updating in place, so the same key piles up across SSTables and deleted keys linger as tombstones. Reads slow down (more tables to check) and disk use grows. Compaction merges tables back into one.

- **Trigger.** Once `compaction_trigger` SSTables exist (4 by default), a second background thread merges all of them. Flushes keep running meanwhile; tables they produce are newer than every input and are left alone. `CompactAll()` runs one on demand.
- **Merge.** A `MergingIterator` walks every input in key order, returning versions of the same key newest first. The compactor keeps the first version of each key and skips the rest.
- **Dropping tombstones.** A tombstone only matters while an older value for its key might exist. Because every older table is an input, the compactor drops tombstones along with the values they hid.
- **Sequence numbers.** The output records the inputs' highest sequence number even if the entry carrying it was dropped. Otherwise compacting everything away could make recovery restart numbering from an older value.
- **Ordering by sequence number.** The output takes a file number after any table flushed during the compaction, yet holds older data, so the engine orders tables by max sequence number rather than file number.
- **Crash safety.** The output is published atomically, then the inputs are deleted. If the process dies in between, the inputs and output overlap; every version in the output is also in an input, so reads stay correct and the next compaction cleans up. Readers holding the old table list keep their open file descriptors, so deleting inputs never disrupts an in-flight read.

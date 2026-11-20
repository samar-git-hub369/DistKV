#include "storage/storage_engine.h"

#include <algorithm>
#include <cinttypes>
#include <cstdio>
#include <filesystem>

#include "common/file_util.h"
#include "storage/compaction.h"
#include "storage/flush.h"

namespace distkv {
namespace {

namespace fs = std::filesystem;

using TableList = std::vector<std::shared_ptr<SSTableReader>>;

constexpr std::string_view kSSTableSuffix = ".sst";
constexpr std::string_view kWalPrefix = "wal-";
constexpr std::string_view kWalSuffix = ".log";
constexpr std::string_view kTmpSuffix = ".tmp";

bool EndsWith(std::string_view s, std::string_view suffix) {
    return s.size() >= suffix.size() && s.substr(s.size() - suffix.size()) == suffix;
}

// Parses an all-digit file number. Rejects empty strings and overflow.
bool ParseNumber(std::string_view digits, uint64_t* out) {
    if (digits.empty() || digits.size() > 19) {
        return false;
    }
    uint64_t n = 0;
    for (const char c : digits) {
        if (c < '0' || c > '9') {
            return false;
        }
        n = n * 10 + static_cast<uint64_t>(c - '0');
    }
    *out = n;
    return true;
}

}  // namespace

StorageEngine::StorageEngine(std::string dir, EngineOptions options)
    : dir_(std::move(dir)),
      options_(options),
      tables_(std::make_shared<const TableList>()) {}

std::string StorageEngine::SSTablePath(uint64_t number) const {
    char name[32];
    std::snprintf(name, sizeof(name), "%06" PRIu64 ".sst", number);
    return (fs::path(dir_) / name).string();
}

std::string StorageEngine::WalPath(uint64_t number) const {
    char name[32];
    std::snprintf(name, sizeof(name), "wal-%06" PRIu64 ".log", number);
    return (fs::path(dir_) / name).string();
}

Status StorageEngine::Open(const std::string& dir, const EngineOptions& options,
                           std::unique_ptr<StorageEngine>* out) {
    if (options.memtable_bytes == 0) {
        return Status::InvalidArgument("memtable_bytes must be positive");
    }
    std::error_code ec;
    fs::create_directories(dir, ec);
    if (ec) {
        return Status::IOError("create directory " + dir + ": " + ec.message());
    }

    std::unique_ptr<StorageEngine> engine(new StorageEngine(dir, options));
    Status s = engine->Recover();
    if (!s.ok()) {
        return s;
    }
    engine->flush_thread_ = std::thread(&StorageEngine::FlushLoop, engine.get());
    engine->compaction_thread_ = std::thread(&StorageEngine::CompactionLoop, engine.get());
    *out = std::move(engine);
    return Status::Ok();
}

Status StorageEngine::Recover() {
    std::vector<uint64_t> sst_numbers;
    std::vector<uint64_t> wal_numbers;
    uint64_t max_number = 0;

    std::error_code ec;
    for (const auto& entry : fs::directory_iterator(dir_, ec)) {
        const std::string name = entry.path().filename().string();
        uint64_t n = 0;
        if (EndsWith(name, kTmpSuffix)) {
            std::error_code ignored;
            fs::remove(entry.path(), ignored);  // an SSTable whose flush never finished
            continue;
        }
        const std::string_view v(name);
        if (EndsWith(v, kSSTableSuffix) &&
            ParseNumber(v.substr(0, v.size() - kSSTableSuffix.size()), &n)) {
            sst_numbers.push_back(n);
        } else if (v.substr(0, kWalPrefix.size()) == kWalPrefix && EndsWith(v, kWalSuffix) &&
                   ParseNumber(v.substr(kWalPrefix.size(),
                                        v.size() - kWalPrefix.size() - kWalSuffix.size()),
                               &n)) {
            wal_numbers.push_back(n);
        } else {
            continue;
        }
        max_number = std::max(max_number, n);
    }
    if (ec) {
        return Status::IOError("list directory " + dir_ + ": " + ec.message());
    }
    next_file_number_ = max_number + 1;

    // Open SSTables, then order them newest first by max sequence number (see
    // the header for why not by file number). Opening in descending file
    // order first makes the stable sort break ties toward the higher number.
    std::sort(sst_numbers.rbegin(), sst_numbers.rend());
    auto tables = std::make_shared<TableList>();
    uint64_t table_max_seq = 0;
    for (const uint64_t n : sst_numbers) {
        std::unique_ptr<SSTableReader> reader;
        Status s = SSTableReader::Open(SSTablePath(n), &reader);
        if (!s.ok()) {
            return s;
        }
        table_max_seq = std::max(table_max_seq, reader->max_seq());
        tables->push_back(std::move(reader));
    }
    std::stable_sort(tables->begin(), tables->end(), [](const auto& a, const auto& b) {
        return a->max_seq() > b->max_seq();
    });

    // Replay WALs oldest first, skipping writes some SSTable already holds.
    std::sort(wal_numbers.begin(), wal_numbers.end());
    MemTable recovered(SIZE_MAX);
    for (const uint64_t n : wal_numbers) {
        WalReplayStats replay;
        Status s = RecoverWal(
            WalPath(n),
            [&](const WalRecord& r) {
                if (r.seq > table_max_seq) {
                    recovered.Apply(r);
                }
            },
            &replay);
        if (!s.ok()) {
            return s;
        }
    }

    uint64_t last_seq = std::max(table_max_seq, recovered.MaxSeq());
    if (recovered.EntryCount() > 0) {
        const uint64_t n = next_file_number_++;
        FlushStats flushed;
        Status s = FlushMemTable(recovered, SSTablePath(n), options_.sstable, &flushed);
        if (!s.ok()) {
            return s;
        }
        std::unique_ptr<SSTableReader> reader;
        s = SSTableReader::Open(SSTablePath(n), &reader);
        if (!s.ok()) {
            return s;
        }
        tables->insert(tables->begin(), std::move(reader));
    }
    // Everything the WALs held is now in an SSTable.
    for (const uint64_t n : wal_numbers) {
        std::error_code ignored;
        fs::remove(WalPath(n), ignored);
    }

    mem_wal_number_ = next_file_number_++;
    Status s = WalWriter::Open(WalPath(mem_wal_number_), options_.wal_sync, &wal_);
    if (!s.ok()) {
        return s;
    }
    mem_ = std::make_shared<MemTable>(options_.memtable_bytes);
    tables_ = std::move(tables);
    last_seq_.store(last_seq);
    return Status::Ok();
}

StorageEngine::~StorageEngine() {
    {
        std::lock_guard<std::mutex> lock(mu_);
        shutting_down_ = true;
    }
    flush_cv_.notify_all();
    compact_cv_.notify_all();
    if (flush_thread_.joinable()) {
        flush_thread_.join();
    }
    if (compaction_thread_.joinable()) {
        compaction_thread_.join();
    }
}

Status StorageEngine::Put(std::string_view key, std::string_view value) {
    return Write(WalOp::kPut, key, value);
}

Status StorageEngine::Delete(std::string_view key) { return Write(WalOp::kDelete, key, {}); }

Status StorageEngine::Write(WalOp op, std::string_view key, std::string_view value) {
    if (key.empty()) {
        return Status::InvalidArgument("key must not be empty");
    }
    std::lock_guard<std::mutex> write_lock(write_mu_);
    Status s = MakeRoomForWrite(/*force=*/false);
    if (!s.ok()) {
        return s;
    }

    // mem_ and wal_ are only replaced while write_mu_ is held, so this thread
    // can use them without mu_.
    const uint64_t seq = last_seq_.load(std::memory_order_relaxed) + 1;
    const WalRecord record{seq, op, std::string(key), std::string(value)};
    s = wal_->Append(record);
    if (!s.ok()) {
        return s;
    }
    mem_->Apply(record);  // visible to readers only once it is logged
    last_seq_.store(seq, std::memory_order_release);
    return Status::Ok();
}

Status StorageEngine::MakeRoomForWrite(bool force) {
    std::unique_lock<std::mutex> lock(mu_);
    while (true) {
        if (!bg_error_.ok()) {
            return bg_error_;
        }
        if (force ? mem_->EntryCount() == 0 : !mem_->ShouldFlush()) {
            return Status::Ok();
        }
        if (imm_ != nullptr) {
            // The previous MemTable is still flushing; this is the engine's
            // backpressure when writes outpace the disk.
            flush_cv_.wait(lock);
            continue;
        }

        const uint64_t wal_number = next_file_number_++;
        lock.unlock();  // don't block readers on file creation
        std::unique_ptr<WalWriter> wal;
        Status s = WalWriter::Open(WalPath(wal_number), options_.wal_sync, &wal);
        lock.lock();
        if (!s.ok()) {
            return s;
        }

        imm_ = std::move(mem_);
        imm_wal_number_ = mem_wal_number_;
        mem_ = std::make_shared<MemTable>(options_.memtable_bytes);
        mem_wal_number_ = wal_number;
        wal_ = std::move(wal);
        flush_cv_.notify_all();
        return Status::Ok();
    }
}

void StorageEngine::FlushLoop() {
    std::unique_lock<std::mutex> lock(mu_);
    while (true) {
        flush_cv_.wait(lock, [this] { return imm_ != nullptr || shutting_down_; });
        if (imm_ == nullptr) {
            return;  // shutting down with nothing left to flush
        }
        const std::shared_ptr<MemTable> imm = imm_;
        const uint64_t wal_number = imm_wal_number_;
        const uint64_t sst_number = next_file_number_++;
        lock.unlock();

        const std::string path = SSTablePath(sst_number);
        FlushStats flushed;
        Status s = FlushMemTable(*imm, path, options_.sstable, &flushed);
        std::unique_ptr<SSTableReader> reader;
        if (s.ok()) {
            s = SSTableReader::Open(path, &reader);
        }
        if (s.ok()) {
            // The table is durable, so the WAL is redundant. Readers still see
            // imm_ until it is published below. If removing fails, recovery
            // skips the WAL's records because the table covers their seqs.
            std::error_code ec;
            fs::remove(WalPath(wal_number), ec);
        }

        lock.lock();
        if (!s.ok()) {
            // Keep imm_ so its data stays readable; it is still in its WAL and
            // will be recovered on the next Open. Writers now fail fast.
            bg_error_ = s;
            flush_cv_.notify_all();
            return;
        }
        // Publish the table and drop the frozen MemTable in one step so a
        // reader always finds the data in one place or the other.
        auto tables = std::make_shared<TableList>();
        tables->reserve(tables_->size() + 1);
        tables->push_back(std::move(reader));
        tables->insert(tables->end(), tables_->begin(), tables_->end());
        tables_ = std::move(tables);
        imm_.reset();
        ++flushes_;
        flush_cv_.notify_all();
        compact_cv_.notify_all();
    }
}

Status StorageEngine::Get(std::string_view key, std::string* value) const {
    if (key.empty()) {
        return Status::InvalidArgument("key must not be empty");
    }
    std::shared_ptr<MemTable> mem;
    std::shared_ptr<MemTable> imm;
    std::shared_ptr<const TableList> tables;
    {
        std::lock_guard<std::mutex> lock(mu_);
        mem = mem_;
        imm = imm_;
        tables = tables_;
    }

    for (const MemTable* m : {mem.get(), imm.get()}) {
        if (m == nullptr) {
            continue;
        }
        switch (m->Get(key, value)) {
            case LookupResult::kFound:
                return Status::Ok();
            case LookupResult::kDeleted:
                return Status::NotFound("key deleted");
            case LookupResult::kNotPresent:
                break;
        }
    }

    for (const auto& table : *tables) {
        LookupResult result = LookupResult::kNotPresent;
        Status s = table->Get(key, &result, value);
        if (!s.ok()) {
            return s;
        }
        if (result == LookupResult::kFound) {
            return Status::Ok();
        }
        if (result == LookupResult::kDeleted) {
            return Status::NotFound("key deleted");
        }
    }
    return Status::NotFound("key not found");
}

Status StorageEngine::Flush() {
    std::lock_guard<std::mutex> write_lock(write_mu_);
    Status s = MakeRoomForWrite(/*force=*/true);
    if (!s.ok()) {
        return s;
    }
    std::unique_lock<std::mutex> lock(mu_);
    flush_cv_.wait(lock, [this] { return imm_ == nullptr || !bg_error_.ok(); });
    return bg_error_;
}

void StorageEngine::CompactionLoop() {
    if (options_.compaction_trigger == 0) {
        return;
    }
    std::unique_lock<std::mutex> lock(mu_);
    while (true) {
        compact_cv_.wait(lock, [this] {
            return shutting_down_ ||
                   (bg_error_.ok() && tables_->size() >= options_.compaction_trigger);
        });
        if (shutting_down_) {
            return;
        }
        lock.unlock();
        Status s = RunCompaction();
        lock.lock();
        if (!s.ok()) {
            // Compaction never loses data, but a failure here (disk full, a
            // damaged table) will hit writes too; surface it the same way.
            if (bg_error_.ok()) {
                bg_error_ = s;
            }
            flush_cv_.notify_all();
            return;
        }
    }
}

Status StorageEngine::RunCompaction() {
    std::lock_guard<std::mutex> compact_lock(compact_mu_);

    std::shared_ptr<const TableList> inputs;
    uint64_t out_number = 0;
    {
        std::lock_guard<std::mutex> lock(mu_);
        if (!bg_error_.ok()) {
            return bg_error_;
        }
        inputs = tables_;
        if (inputs->empty()) {
            return Status::Ok();
        }
        out_number = next_file_number_++;
        compacting_ = true;
    }

    // Every table that exists is an input; tables flushed from now on are
    // newer. With nothing older left, tombstones have nothing to hide.
    std::vector<const SSTableReader*> raw;
    raw.reserve(inputs->size());
    for (const auto& t : *inputs) {
        raw.push_back(t.get());
    }
    const std::string out_path = SSTablePath(out_number);
    CompactionStats compacted;
    Status s = CompactTables(raw, out_path, options_.sstable, /*drop_tombstones=*/true,
                             &compacted);
    std::unique_ptr<SSTableReader> output;
    if (s.ok()) {
        s = SSTableReader::Open(out_path, &output);
    }

    {
        std::lock_guard<std::mutex> lock(mu_);
        compacting_ = false;
        if (!s.ok()) {
            return s;
        }
        // Keep the tables flushed during the compaction (they sit in front of
        // the inputs), then the output in place of all the inputs.
        auto tables = std::make_shared<TableList>();
        for (const auto& t : *tables_) {
            if (std::find(inputs->begin(), inputs->end(), t) == inputs->end()) {
                tables->push_back(t);
            }
        }
        tables->push_back(std::move(output));
        tables_ = std::move(tables);
        ++compactions_;
    }

    // Readers that copied the old list keep their open file descriptors, so
    // unlinking the inputs is safe while they finish.
    for (const auto& t : *inputs) {
        std::error_code ec;
        fs::remove(t->path(), ec);
    }
    return Status::Ok();
}

Status StorageEngine::CompactAll() { return RunCompaction(); }

EngineStats StorageEngine::stats() const {
    std::lock_guard<std::mutex> lock(mu_);
    EngineStats st;
    st.last_seq = last_seq_.load(std::memory_order_acquire);
    st.sstables = tables_->size();
    st.memtable_bytes = mem_->ApproximateBytes();
    st.flush_in_progress = imm_ != nullptr;
    st.flushes = flushes_;
    st.compactions = compactions_;
    st.compaction_in_progress = compacting_;
    return st;
}

}  // namespace distkv

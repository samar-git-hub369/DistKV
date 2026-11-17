#include "storage/flush.h"

#include <filesystem>
#include <memory>

namespace distkv {

Status FlushMemTable(const MemTable& mem, const std::string& path, const SSTableOptions& options,
                     FlushStats* stats) {
    *stats = FlushStats{};

    std::unique_ptr<SSTableWriter> writer;
    Status s = SSTableWriter::Create(path, options, &writer);
    if (!s.ok()) {
        return s;
    }

    // ForEach cannot stop early, so after the first failure the rest of the
    // entries are skipped and the error is returned at the end.
    Status add_status;
    FlushStats counted;
    mem.ForEach([&](std::string_view key, const MemTableEntry& e) {
        if (!add_status.ok()) {
            return;
        }
        const EntryType type = e.tombstone ? EntryType::kTombstone : EntryType::kValue;
        add_status = writer->Add(key, e.seq, type, e.value);
        ++counted.entries;
        if (e.tombstone) {
            ++counted.tombstones;
        }
    });
    if (!add_status.ok()) {
        return add_status;  // the writer's destructor removes the temp file
    }

    s = writer->Finish();
    if (!s.ok()) {
        return s;
    }

    std::error_code ec;
    counted.file_bytes = std::filesystem::file_size(path, ec);
    if (ec) {
        return Status::IOError("stat sstable " + path + ": " + ec.message());
    }
    counted.max_seq = mem.MaxSeq();
    *stats = counted;
    return Status::Ok();
}

}  // namespace distkv

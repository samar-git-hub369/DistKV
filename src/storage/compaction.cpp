#include "storage/compaction.h"

#include <algorithm>
#include <filesystem>
#include <memory>

#include "storage/merging_iterator.h"

namespace distkv {

Status CompactTables(const std::vector<const SSTableReader*>& inputs, const std::string& out_path,
                     const SSTableOptions& options, bool drop_tombstones, CompactionStats* stats) {
    *stats = CompactionStats{};
    CompactionStats counted;
    counted.input_tables = inputs.size();

    std::error_code ec;
    uint64_t max_seq = 0;
    for (const SSTableReader* t : inputs) {
        max_seq = std::max(max_seq, t->max_seq());
        counted.input_entries += t->entry_count();
        counted.input_bytes += std::filesystem::file_size(t->path(), ec);
    }

    std::unique_ptr<SSTableWriter> writer;
    Status s = SSTableWriter::Create(out_path, options, &writer);
    if (!s.ok()) {
        return s;
    }

    MergingIterator it(inputs);
    std::string last_key;
    bool have_last = false;
    for (it.SeekToFirst(); it.Valid(); it.Next()) {
        // Versions of one key arrive newest first; only the first one counts.
        if (have_last && it.key() == last_key) {
            ++counted.dropped_overwritten;
            continue;
        }
        last_key.assign(it.key().data(), it.key().size());
        have_last = true;

        if (it.type() == EntryType::kTombstone && drop_tombstones) {
            ++counted.dropped_tombstones;
            continue;
        }
        s = writer->Add(it.key(), it.seq(), it.type(), it.value());
        if (!s.ok()) {
            return s;
        }
        ++counted.output_entries;
    }
    if (!it.status().ok()) {
        return it.status();  // the writer's destructor removes the temp file
    }

    writer->RaiseMaxSeq(max_seq);
    s = writer->Finish();
    if (!s.ok()) {
        return s;
    }
    counted.output_bytes = std::filesystem::file_size(out_path, ec);
    *stats = counted;
    return Status::Ok();
}

}  // namespace distkv

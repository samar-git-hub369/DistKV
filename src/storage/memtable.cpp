#include "storage/memtable.h"

#include <algorithm>
#include <mutex>

namespace distkv {

MemTable::MemTable(size_t flush_threshold_bytes) : flush_threshold_(flush_threshold_bytes) {}

void MemTable::Put(uint64_t seq, std::string_view key, std::string_view value) {
    Upsert(seq, key, /*tombstone=*/false, value);
}

void MemTable::Delete(uint64_t seq, std::string_view key) {
    Upsert(seq, key, /*tombstone=*/true, {});
}

void MemTable::Apply(const WalRecord& record) {
    switch (record.op) {
        case WalOp::kPut:
            Put(record.seq, record.key, record.value);
            break;
        case WalOp::kDelete:
            Delete(record.seq, record.key);
            break;
    }
}

void MemTable::Upsert(uint64_t seq, std::string_view key, bool tombstone, std::string_view value) {
    std::unique_lock lock(mu_);

    auto it = entries_.find(key);
    if (it == entries_.end()) {
        it = entries_.emplace(std::string(key), MemTableEntry{}).first;
    } else {
        if (seq <= it->second.seq) {
            return;  // already have this write or a newer one
        }
        bytes_ -= Charge(key, it->second);
    }

    MemTableEntry& entry = it->second;
    entry.seq = seq;
    entry.tombstone = tombstone;
    entry.value.assign(value.data(), value.size());

    bytes_ += Charge(key, entry);
    max_seq_ = std::max(max_seq_, seq);
}

LookupResult MemTable::Get(std::string_view key, std::string* value) const {
    std::shared_lock lock(mu_);
    const auto it = entries_.find(key);
    if (it == entries_.end()) {
        return LookupResult::kNotPresent;
    }
    if (it->second.tombstone) {
        return LookupResult::kDeleted;
    }
    *value = it->second.value;
    return LookupResult::kFound;
}

void MemTable::ForEach(
    const std::function<void(std::string_view key, const MemTableEntry&)>& fn) const {
    std::shared_lock lock(mu_);
    for (const auto& [key, entry] : entries_) {
        fn(key, entry);
    }
}

size_t MemTable::ApproximateBytes() const {
    std::shared_lock lock(mu_);
    return bytes_;
}

size_t MemTable::EntryCount() const {
    std::shared_lock lock(mu_);
    return entries_.size();
}

uint64_t MemTable::MaxSeq() const {
    std::shared_lock lock(mu_);
    return max_seq_;
}

bool MemTable::ShouldFlush() const {
    return ApproximateBytes() >= flush_threshold_;
}

Status RecoverMemTable(const std::string& wal_path, MemTable* table, WalReplayStats* stats) {
    return RecoverWal(wal_path, [table](const WalRecord& r) { table->Apply(r); }, stats);
}

}  // namespace distkv

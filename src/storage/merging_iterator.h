#pragma once

#include <memory>
#include <string_view>
#include <vector>

#include "common/status.h"
#include "storage/sstable.h"

namespace distkv {

// Merges several SSTables into one stream sorted by key. When the same key
// appears in more than one table, every version is returned, newest (highest
// sequence number) first, so a consumer that keeps the first entry per key
// sees each key's latest state.
//
// Each step scans the current entry of every input to find the next one. That
// is O(inputs) per entry, which beats a heap for the handful of tables a
// compaction merges.
class MergingIterator {
  public:
    explicit MergingIterator(const std::vector<const SSTableReader*>& tables);

    void SeekToFirst();
    void Next();

    bool Valid() const { return current_ != nullptr; }
    // First error from any input; iteration stops when one occurs.
    const Status& status() const { return status_; }

    std::string_view key() const { return current_->key(); }
    uint64_t seq() const { return current_->seq(); }
    EntryType type() const { return current_->type(); }
    std::string_view value() const { return current_->value(); }

  private:
    void FindSmallest();

    std::vector<std::unique_ptr<SSTableIterator>> children_;
    SSTableIterator* current_ = nullptr;
    Status status_;
};

}  // namespace distkv

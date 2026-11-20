#include "storage/merging_iterator.h"

namespace distkv {

MergingIterator::MergingIterator(const std::vector<const SSTableReader*>& tables) {
    children_.reserve(tables.size());
    for (const SSTableReader* t : tables) {
        children_.push_back(std::make_unique<SSTableIterator>(t));
    }
}

void MergingIterator::SeekToFirst() {
    status_ = Status::Ok();
    for (auto& child : children_) {
        child->SeekToFirst();
    }
    FindSmallest();
}

void MergingIterator::Next() {
    if (current_ != nullptr) {
        current_->Next();
        FindSmallest();
    }
}

void MergingIterator::FindSmallest() {
    current_ = nullptr;
    for (auto& child : children_) {
        if (!child->status().ok()) {
            status_ = child->status();
            current_ = nullptr;
            return;
        }
        if (!child->Valid()) {
            continue;
        }
        if (current_ == nullptr || child->key() < current_->key() ||
            (child->key() == current_->key() && child->seq() > current_->seq())) {
            current_ = child.get();
        }
    }
}

}  // namespace distkv

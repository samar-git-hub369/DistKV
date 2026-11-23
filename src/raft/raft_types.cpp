#include "raft/raft_types.h"

#include "common/coding.h"

namespace distkv::raft {

size_t EncodedSize(const LogEntry& entry) {
    return 8 + 8 + 1 + 4 + entry.key.size() + 4 + entry.value.size();
}

void EncodeLogEntry(std::string* dst, const LogEntry& entry) {
    dst->reserve(dst->size() + EncodedSize(entry));
    PutFixed64(dst, entry.term);
    PutFixed64(dst, entry.index);
    dst->push_back(static_cast<char>(entry.op));
    PutLengthPrefixed(dst, entry.key);
    PutLengthPrefixed(dst, entry.value);
}

bool DecodeLogEntry(std::string_view* in, LogEntry* entry) {
    if (in->size() < 8 + 8 + 1) {
        return false;
    }
    std::string_view rest = *in;
    const uint64_t term = DecodeFixed64(rest.data());
    const uint64_t index = DecodeFixed64(rest.data() + 8);
    const auto op = static_cast<uint8_t>(rest[16]);
    rest.remove_prefix(17);

    std::string_view key;
    std::string_view value;
    if (!GetLengthPrefixed(&rest, &key) || !GetLengthPrefixed(&rest, &value)) {
        return false;
    }
    switch (static_cast<EntryOp>(op)) {
        case EntryOp::kNoop:
            if (!key.empty() || !value.empty()) return false;
            break;
        case EntryOp::kPut:
            if (key.empty()) return false;
            break;
        case EntryOp::kDelete:
            if (key.empty() || !value.empty()) return false;
            break;
        default:
            return false;
    }

    entry->term = term;
    entry->index = index;
    entry->op = static_cast<EntryOp>(op);
    entry->key.assign(key.data(), key.size());
    entry->value.assign(value.data(), value.size());
    *in = rest;
    return true;
}

}  // namespace distkv::raft

#include "raft/raft_rpc.h"

#include <utility>

#include "common/coding.h"
#include "common/crc32.h"

namespace distkv::raft {
namespace {

// Small cursor over a message body; every getter fails once input runs out.
class Reader {
  public:
    explicit Reader(std::string_view in) : in_(in) {}

    bool U64(uint64_t* v) {
        if (in_.size() < 8) return false;
        *v = DecodeFixed64(in_.data());
        in_.remove_prefix(8);
        return true;
    }
    bool U32(uint32_t* v) {
        if (in_.size() < 4) return false;
        *v = DecodeFixed32(in_.data());
        in_.remove_prefix(4);
        return true;
    }
    bool Bool(bool* v) {
        if (in_.empty()) return false;
        const auto b = static_cast<uint8_t>(in_[0]);
        if (b > 1) return false;
        *v = b == 1;
        in_.remove_prefix(1);
        return true;
    }
    bool Entry(LogEntry* e) { return DecodeLogEntry(&in_, e); }
    bool done() const { return in_.empty(); }
    size_t remaining() const { return in_.size(); }

  private:
    std::string_view in_;
};

void PutBool(std::string* dst, bool v) { dst->push_back(v ? '\1' : '\0'); }

void EncodeBody(std::string* dst, const RequestVoteRequest& m) {
    PutFixed64(dst, m.term);
    PutFixed32(dst, m.candidate_id);
    PutFixed64(dst, m.last_log_index);
    PutFixed64(dst, m.last_log_term);
}

void EncodeBody(std::string* dst, const RequestVoteResponse& m) {
    PutFixed64(dst, m.term);
    PutFixed32(dst, m.voter_id);
    PutBool(dst, m.vote_granted);
}

void EncodeBody(std::string* dst, const AppendEntriesRequest& m) {
    PutFixed64(dst, m.term);
    PutFixed32(dst, m.leader_id);
    PutFixed64(dst, m.prev_log_index);
    PutFixed64(dst, m.prev_log_term);
    PutFixed64(dst, m.leader_commit);
    PutFixed32(dst, static_cast<uint32_t>(m.entries.size()));
    for (const LogEntry& e : m.entries) {
        EncodeLogEntry(dst, e);
    }
}

void EncodeBody(std::string* dst, const AppendEntriesResponse& m) {
    PutFixed64(dst, m.term);
    PutFixed32(dst, m.follower_id);
    PutBool(dst, m.success);
    PutFixed64(dst, m.match_index);
    PutFixed64(dst, m.conflict_index);
}

bool DecodeBody(Reader* r, RequestVoteRequest* m) {
    return r->U64(&m->term) && r->U32(&m->candidate_id) && r->U64(&m->last_log_index) &&
           r->U64(&m->last_log_term);
}

bool DecodeBody(Reader* r, RequestVoteResponse* m) {
    return r->U64(&m->term) && r->U32(&m->voter_id) && r->Bool(&m->vote_granted);
}

bool DecodeBody(Reader* r, AppendEntriesRequest* m) {
    uint32_t count = 0;
    if (!r->U64(&m->term) || !r->U32(&m->leader_id) || !r->U64(&m->prev_log_index) ||
        !r->U64(&m->prev_log_term) || !r->U64(&m->leader_commit) || !r->U32(&count)) {
        return false;
    }
    // Every entry takes at least 25 bytes, so a count the body cannot hold is
    // rejected before reserving memory for it.
    constexpr size_t kMinEntryBytes = 8 + 8 + 1 + 4 + 4;
    if (count > r->remaining() / kMinEntryBytes) {
        return false;
    }
    m->entries.clear();
    m->entries.reserve(count);
    for (uint32_t i = 0; i < count; ++i) {
        LogEntry e;
        if (!r->Entry(&e)) return false;
        // Entries in one request must be consecutive and follow prev_log_index.
        if (e.index != m->prev_log_index + 1 + i) return false;
        m->entries.push_back(std::move(e));
    }
    return true;
}

bool DecodeBody(Reader* r, AppendEntriesResponse* m) {
    return r->U64(&m->term) && r->U32(&m->follower_id) && r->Bool(&m->success) &&
           r->U64(&m->match_index) && r->U64(&m->conflict_index);
}

template <typename T>
bool DecodeAs(Reader* r, RpcMessage* out) {
    T m;
    if (!DecodeBody(r, &m) || !r->done()) {
        return false;
    }
    *out = std::move(m);
    return true;
}

}  // namespace

RpcType TypeOf(const RpcMessage& message) {
    return static_cast<RpcType>(message.index() + 1);
}

const char* RpcTypeName(RpcType type) {
    switch (type) {
        case RpcType::kRequestVote:
            return "RequestVote";
        case RpcType::kRequestVoteResponse:
            return "RequestVoteResponse";
        case RpcType::kAppendEntries:
            return "AppendEntries";
        case RpcType::kAppendEntriesResponse:
            return "AppendEntriesResponse";
    }
    return "Unknown";
}

std::string EncodeRpcFrame(const RpcMessage& message) {
    std::string payload;
    payload.push_back(static_cast<char>(TypeOf(message)));
    std::visit([&payload](const auto& m) { EncodeBody(&payload, m); }, message);

    std::string frame;
    frame.reserve(kRpcFrameHeader + payload.size());
    PutFixed32(&frame, static_cast<uint32_t>(payload.size()));
    PutFixed32(&frame, Crc32(payload, Crc32(frame.data(), 4)));
    frame += payload;
    return frame;
}

FrameResult DecodeRpcFrame(std::string_view buffer, RpcMessage* out, size_t* consumed,
                           std::string* error) {
    auto corrupt = [error](std::string why) {
        if (error != nullptr) *error = std::move(why);
        return FrameResult::kCorrupt;
    };

    *consumed = 0;
    if (buffer.size() < kRpcFrameHeader) {
        return FrameResult::kIncomplete;
    }
    const uint32_t length = DecodeFixed32(buffer.data());
    if (length == 0 || length > kMaxRpcFrameBytes) {
        return corrupt("bad rpc frame length " + std::to_string(length));
    }
    if (buffer.size() - kRpcFrameHeader < length) {
        return FrameResult::kIncomplete;
    }
    const std::string_view payload = buffer.substr(kRpcFrameHeader, length);
    if (Crc32(payload, Crc32(buffer.data(), 4)) != DecodeFixed32(buffer.data() + 4)) {
        return corrupt("rpc frame checksum mismatch");
    }

    Reader r(payload.substr(1));
    bool ok = false;
    switch (static_cast<RpcType>(payload[0])) {
        case RpcType::kRequestVote:
            ok = DecodeAs<RequestVoteRequest>(&r, out);
            break;
        case RpcType::kRequestVoteResponse:
            ok = DecodeAs<RequestVoteResponse>(&r, out);
            break;
        case RpcType::kAppendEntries:
            ok = DecodeAs<AppendEntriesRequest>(&r, out);
            break;
        case RpcType::kAppendEntriesResponse:
            ok = DecodeAs<AppendEntriesResponse>(&r, out);
            break;
        default:
            return corrupt("unknown rpc type " +
                           std::to_string(static_cast<unsigned>(static_cast<uint8_t>(payload[0]))));
    }
    if (!ok) {
        return corrupt(std::string("malformed ") + RpcTypeName(static_cast<RpcType>(payload[0])));
    }
    *consumed = kRpcFrameHeader + length;
    return FrameResult::kOk;
}

}  // namespace distkv::raft

#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

#include "raft/raft_types.h"

// Raft RPC messages and their wire format.
//
// Nodes exchange length-prefixed, checksummed binary frames:
//
//   frame   := length:u32  crc32:u32  payload[length]   (crc over length + payload)
//   payload := type:u8  body
//
// A frame decoder works on a growing byte buffer, so a transport can feed it
// whatever the socket returned and learn whether a whole frame is there yet.
namespace distkv::raft {

// Sent by a candidate to ask for a vote (§5.2).
struct RequestVoteRequest {
    uint64_t term = 0;
    NodeId candidate_id = kNoNode;
    uint64_t last_log_index = 0;
    uint64_t last_log_term = 0;
};

struct RequestVoteResponse {
    uint64_t term = 0;  // voter's current term, so a stale candidate steps down
    NodeId voter_id = kNoNode;
    bool vote_granted = false;
};

// Sent by the leader to replicate entries; with no entries it is a heartbeat.
struct AppendEntriesRequest {
    uint64_t term = 0;
    NodeId leader_id = kNoNode;
    uint64_t prev_log_index = 0;  // entry just before `entries`
    uint64_t prev_log_term = 0;
    uint64_t leader_commit = 0;
    std::vector<LogEntry> entries;
};

struct AppendEntriesResponse {
    uint64_t term = 0;
    NodeId follower_id = kNoNode;
    bool success = false;
    // On success: the follower's log matches the leader's up to here.
    uint64_t match_index = 0;
    // On failure: where the leader should try next, so it can skip back a
    // whole conflicting term instead of one entry per round trip.
    uint64_t conflict_index = 0;
};

using RpcMessage = std::variant<RequestVoteRequest, RequestVoteResponse, AppendEntriesRequest,
                                AppendEntriesResponse>;

enum class RpcType : uint8_t {
    kRequestVote = 1,
    kRequestVoteResponse = 2,
    kAppendEntries = 3,
    kAppendEntriesResponse = 4,
};

RpcType TypeOf(const RpcMessage& message);
const char* RpcTypeName(RpcType type);

// Frames larger than this are refused, so a damaged or hostile length prefix
// cannot make a node allocate unbounded memory.
inline constexpr uint32_t kMaxRpcFrameBytes = 64u << 20;
inline constexpr size_t kRpcFrameHeader = 8;

// The bytes to send for `message`.
std::string EncodeRpcFrame(const RpcMessage& message);

enum class FrameResult {
    kOk,          // decoded one message; `*consumed` bytes can be dropped
    kIncomplete,  // need more bytes
    kCorrupt,     // bad length, checksum or body; the connection should be dropped
};

// Tries to decode one frame from the front of `buffer`. On kCorrupt,
// `*error` (if given) says why.
FrameResult DecodeRpcFrame(std::string_view buffer, RpcMessage* out, size_t* consumed,
                           std::string* error = nullptr);

}  // namespace distkv::raft

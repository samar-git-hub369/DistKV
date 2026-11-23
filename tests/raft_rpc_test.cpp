#include "raft/raft_rpc.h"

#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "common/coding.h"
#include "common/crc32.h"

namespace distkv::raft {
namespace {

RpcMessage RoundTrip(const RpcMessage& in) {
    const std::string frame = EncodeRpcFrame(in);
    RpcMessage out;
    size_t consumed = 0;
    std::string error;
    EXPECT_EQ(DecodeRpcFrame(frame, &out, &consumed, &error), FrameResult::kOk) << error;
    EXPECT_EQ(consumed, frame.size());
    return out;
}

AppendEntriesRequest SampleAppend() {
    AppendEntriesRequest m;
    m.term = 5;
    m.leader_id = 2;
    m.prev_log_index = 10;
    m.prev_log_term = 4;
    m.leader_commit = 9;
    m.entries = {LogEntry{5, 11, EntryOp::kNoop, "", ""},
                 LogEntry{5, 12, EntryOp::kPut, "user:1", "alice smith"},
                 LogEntry{5, 13, EntryOp::kDelete, "user:0", ""}};
    return m;
}

TEST(RaftRpcTest, EveryMessageRoundTrips) {
    {
        const auto out = std::get<RequestVoteRequest>(RoundTrip(RequestVoteRequest{7, 3, 42, 6}));
        EXPECT_EQ(out.term, 7u);
        EXPECT_EQ(out.candidate_id, 3u);
        EXPECT_EQ(out.last_log_index, 42u);
        EXPECT_EQ(out.last_log_term, 6u);
    }
    {
        const auto out =
            std::get<RequestVoteResponse>(RoundTrip(RequestVoteResponse{7, 1, true}));
        EXPECT_EQ(out.term, 7u);
        EXPECT_EQ(out.voter_id, 1u);
        EXPECT_TRUE(out.vote_granted);
    }
    {
        const AppendEntriesRequest in = SampleAppend();
        const auto out = std::get<AppendEntriesRequest>(RoundTrip(in));
        EXPECT_EQ(out.term, in.term);
        EXPECT_EQ(out.leader_id, in.leader_id);
        EXPECT_EQ(out.prev_log_index, in.prev_log_index);
        EXPECT_EQ(out.prev_log_term, in.prev_log_term);
        EXPECT_EQ(out.leader_commit, in.leader_commit);
        EXPECT_EQ(out.entries, in.entries);
    }
    {
        AppendEntriesRequest heartbeat;
        heartbeat.term = 1;
        heartbeat.leader_id = 1;
        EXPECT_TRUE(std::get<AppendEntriesRequest>(RoundTrip(heartbeat)).entries.empty());
    }
    {
        const auto out =
            std::get<AppendEntriesResponse>(RoundTrip(AppendEntriesResponse{5, 3, false, 0, 8}));
        EXPECT_EQ(out.term, 5u);
        EXPECT_EQ(out.follower_id, 3u);
        EXPECT_FALSE(out.success);
        EXPECT_EQ(out.match_index, 0u);
        EXPECT_EQ(out.conflict_index, 8u);
    }
}

TEST(RaftRpcTest, TypeNames) {
    EXPECT_EQ(TypeOf(RequestVoteRequest{}), RpcType::kRequestVote);
    EXPECT_EQ(TypeOf(AppendEntriesResponse{}), RpcType::kAppendEntriesResponse);
    EXPECT_STREQ(RpcTypeName(RpcType::kAppendEntries), "AppendEntries");
}

TEST(RaftRpcTest, PartialFramesAreIncompleteAtEveryCut) {
    const std::string frame = EncodeRpcFrame(SampleAppend());
    for (size_t cut = 0; cut < frame.size(); ++cut) {
        RpcMessage out;
        size_t consumed = 99;
        EXPECT_EQ(DecodeRpcFrame(std::string_view(frame.data(), cut), &out, &consumed),
                  FrameResult::kIncomplete)
            << cut;
        EXPECT_EQ(consumed, 0u);
    }
}

TEST(RaftRpcTest, BackToBackFramesDecodeOneAtATime) {
    std::string stream = EncodeRpcFrame(RequestVoteRequest{1, 1, 0, 0}) +
                         EncodeRpcFrame(SampleAppend()) +
                         EncodeRpcFrame(RequestVoteResponse{1, 2, false});
    std::vector<RpcType> types;
    std::string_view rest = stream;
    while (!rest.empty()) {
        RpcMessage m;
        size_t consumed = 0;
        ASSERT_EQ(DecodeRpcFrame(rest, &m, &consumed), FrameResult::kOk);
        types.push_back(TypeOf(m));
        rest.remove_prefix(consumed);
    }
    EXPECT_EQ(types, (std::vector<RpcType>{RpcType::kRequestVote, RpcType::kAppendEntries,
                                           RpcType::kRequestVoteResponse}));
}

TEST(RaftRpcTest, EveryFlippedByteIsDetected) {
    const std::string frame = EncodeRpcFrame(SampleAppend());
    for (size_t i = 0; i < frame.size(); ++i) {
        std::string bad = frame;
        bad[i] = static_cast<char>(~bad[i]);
        RpcMessage out;
        size_t consumed = 0;
        // A damaged length may look like a longer frame still arriving; any
        // other damage must be caught by the checksum.
        const FrameResult r = DecodeRpcFrame(bad, &out, &consumed);
        EXPECT_NE(r, FrameResult::kOk) << i;
        if (i >= 4) EXPECT_EQ(r, FrameResult::kCorrupt) << i;
    }
}

// Builds a frame with a valid checksum around an arbitrary payload, to check
// that decoding validates the body itself, not just the checksum.
std::string FrameAround(const std::string& payload) {
    std::string frame;
    PutFixed32(&frame, static_cast<uint32_t>(payload.size()));
    PutFixed32(&frame, Crc32(payload, Crc32(frame.data(), 4)));
    return frame + payload;
}

TEST(RaftRpcTest, RejectsMalformedBodiesEvenWithValidChecksums) {
    auto decode = [](const std::string& frame) {
        RpcMessage out;
        size_t consumed = 0;
        std::string error;
        const FrameResult r = DecodeRpcFrame(frame, &out, &consumed, &error);
        return std::make_pair(r, error);
    };

    EXPECT_EQ(decode(FrameAround("\x09")).first, FrameResult::kCorrupt);  // unknown type
    EXPECT_EQ(decode(FrameAround("\x09")).second, "unknown rpc type 9");

    // Truncated body.
    std::string payload = EncodeRpcFrame(RequestVoteRequest{1, 1, 0, 0}).substr(kRpcFrameHeader);
    EXPECT_EQ(decode(FrameAround(payload.substr(0, payload.size() - 1))).second,
              "malformed RequestVote");
    // Trailing garbage.
    EXPECT_EQ(decode(FrameAround(payload + "x")).first, FrameResult::kCorrupt);

    // A bool that is neither 0 nor 1.
    payload = EncodeRpcFrame(RequestVoteResponse{1, 1, true}).substr(kRpcFrameHeader);
    payload.back() = '\x02';
    EXPECT_EQ(decode(FrameAround(payload)).first, FrameResult::kCorrupt);

    // Entries that do not follow prev_log_index.
    AppendEntriesRequest gap = SampleAppend();
    gap.entries[1].index = 20;
    EXPECT_EQ(decode(EncodeRpcFrame(gap)).second, "malformed AppendEntries");

    // An entry count far larger than the body could hold.
    AppendEntriesRequest empty;
    payload = EncodeRpcFrame(empty).substr(kRpcFrameHeader);
    payload.replace(payload.size() - 4, 4, std::string("\xff\xff\xff\x7f", 4));
    EXPECT_EQ(decode(FrameAround(payload)).first, FrameResult::kCorrupt);
}

TEST(RaftRpcTest, RejectsZeroAndOversizedLengths) {
    std::string header;
    PutFixed32(&header, 0);
    PutFixed32(&header, 0);
    RpcMessage out;
    size_t consumed = 0;
    EXPECT_EQ(DecodeRpcFrame(header, &out, &consumed), FrameResult::kCorrupt);

    header.clear();
    PutFixed32(&header, kMaxRpcFrameBytes + 1);
    PutFixed32(&header, 0);
    EXPECT_EQ(DecodeRpcFrame(header, &out, &consumed), FrameResult::kCorrupt);
}

}  // namespace
}  // namespace distkv::raft

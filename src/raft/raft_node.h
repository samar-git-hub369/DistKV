#pragma once

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <random>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "common/status.h"
#include "raft/hard_state.h"
#include "raft/raft_log.h"
#include "raft/raft_rpc.h"

// One member of a Raft group: leader election, heartbeats and the follower
// side of AppendEntries.
//
// Every node starts as a follower. If it hears nothing from a leader for a
// randomised election timeout it becomes a candidate: it bumps the term,
// votes for itself and asks the others for votes. A majority makes it leader,
// and the leader keeps everyone else a follower with periodic heartbeats
// (empty AppendEntries). Randomising the timeout makes split votes rare.
//
// The term and vote are saved to disk before any message that depends on
// them is sent, so a node that crashes and restarts can never vote twice in
// a term.
//
// Threading: a single mutex guards all state. Messages arrive through Step()
// on the transport's threads; a ticker thread fires election timeouts and
// heartbeats. Outgoing messages are handed to the transport under the lock,
// so RaftTransport::Send must only queue.
namespace distkv::raft {

enum class Role { kFollower, kCandidate, kLeader };
const char* RoleName(Role role);

// How nodes reach each other. Delivery is best effort: messages may be lost,
// delayed, duplicated or reordered, all of which Raft tolerates.
class RaftTransport {
  public:
    virtual ~RaftTransport() = default;
    // Queues `message` for node `to`. Must not block and must not call back
    // into the sending node before returning.
    virtual void Send(NodeId to, const RpcMessage& message) = 0;
};

struct RaftStatus {
    NodeId id = kNoNode;
    Role role = Role::kFollower;
    uint64_t term = 0;
    NodeId leader_id = kNoNode;  // as far as this node knows
    NodeId voted_for = kNoNode;
    uint64_t last_log_index = 0;
    uint64_t last_log_term = 0;
    uint64_t commit_index = 0;
    uint64_t elections_started = 0;
};

struct RaftOptions {
    NodeId id = kNoNode;
    std::vector<NodeId> peers;  // the other members of the group
    std::string data_dir;       // holds raft_state and raft.log
    std::chrono::milliseconds election_timeout_min{300};
    std::chrono::milliseconds election_timeout_max{600};
    std::chrono::milliseconds heartbeat_interval{75};
    bool sync = true;  // fsync the log and state; off only in tests
    // Called with the lock held whenever role, term or known leader changes.
    // Must not call back into the node.
    std::function<void(const RaftStatus&)> on_state_change;
};

class RaftNode {
  public:
    // Loads the saved term, vote and log. Does not start the timers.
    static Status Open(RaftOptions options, RaftTransport* transport,
                       std::unique_ptr<RaftNode>* out);

    ~RaftNode();  // calls Stop()
    RaftNode(const RaftNode&) = delete;
    RaftNode& operator=(const RaftNode&) = delete;

    // Starts the election timer and heartbeats.
    void Start();
    // Stops the timers. Messages that arrive afterwards are ignored.
    void Stop();

    // Handles one message from a peer.
    void Step(const RpcMessage& message);

    // Starts an election now instead of waiting for the timeout.
    void Campaign();

    RaftStatus status() const;
    // The first persistence error hit, if any (the node keeps running but
    // refuses the action that needed it).
    Status last_error() const;

  private:
    using Clock = std::chrono::steady_clock;

    RaftNode(RaftOptions options, RaftTransport* transport);

    void Run();
    void StartElection();
    void BecomeLeader();
    void BroadcastHeartbeat();
    bool Persist(uint64_t term, NodeId voted_for);
    bool StepDown(uint64_t term);
    void ResetElectionDeadline();
    size_t Majority() const { return (options_.peers.size() + 1) / 2 + 1; }
    bool IsPeer(NodeId id) const;
    RaftStatus StatusLocked() const;
    void NotifyIfChanged();

    void Handle(const RequestVoteRequest& m);
    void Handle(const RequestVoteResponse& m);
    void Handle(const AppendEntriesRequest& m);
    void Handle(const AppendEntriesResponse& m);

    const RaftOptions options_;
    RaftTransport* transport_;
    std::string state_path_;

    mutable std::mutex mu_;
    std::condition_variable cv_;
    std::thread ticker_;
    bool running_ = false;
    bool stopping_ = false;

    HardState hard_;
    std::unique_ptr<RaftLog> log_;
    Role role_ = Role::kFollower;
    NodeId leader_id_ = kNoNode;
    uint64_t commit_index_ = 0;
    std::set<NodeId> votes_;
    uint64_t elections_started_ = 0;
    Status error_;

    Clock::time_point election_deadline_;
    Clock::time_point next_heartbeat_;
    std::mt19937_64 rng_;

    // Last values passed to on_state_change.
    Role reported_role_ = Role::kFollower;
    uint64_t reported_term_ = 0;
    NodeId reported_leader_ = kNoNode;
    bool reported_once_ = false;
};

}  // namespace distkv::raft

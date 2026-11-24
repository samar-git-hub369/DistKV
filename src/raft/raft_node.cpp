#include "raft/raft_node.h"

#include <algorithm>
#include <filesystem>
#include <type_traits>
#include <variant>

namespace distkv::raft {

const char* RoleName(Role role) {
    switch (role) {
        case Role::kFollower:
            return "follower";
        case Role::kCandidate:
            return "candidate";
        case Role::kLeader:
            return "leader";
    }
    return "unknown";
}

RaftNode::RaftNode(RaftOptions options, RaftTransport* transport)
    : options_(std::move(options)),
      transport_(transport),
      state_path_((std::filesystem::path(options_.data_dir) / "raft_state").string()),
      rng_(std::random_device{}() ^ (uint64_t{options_.id} << 32)) {}

Status RaftNode::Open(RaftOptions options, RaftTransport* transport,
                      std::unique_ptr<RaftNode>* out) {
    if (options.id == kNoNode) {
        return Status::InvalidArgument("raft node id must be positive");
    }
    std::set<NodeId> seen;
    for (const NodeId p : options.peers) {
        if (p == kNoNode || p == options.id || !seen.insert(p).second) {
            return Status::InvalidArgument("bad raft peer id " + std::to_string(p));
        }
    }
    if (options.election_timeout_min.count() <= 0 ||
        options.election_timeout_max < options.election_timeout_min ||
        options.heartbeat_interval.count() <= 0) {
        return Status::InvalidArgument("bad raft timeouts");
    }
    if (transport == nullptr && !options.peers.empty()) {
        return Status::InvalidArgument("a raft group with peers needs a transport");
    }

    std::error_code ec;
    std::filesystem::create_directories(options.data_dir, ec);
    if (ec) {
        return Status::IOError("create raft dir " + options.data_dir + ": " + ec.message());
    }

    std::unique_ptr<RaftNode> node(new RaftNode(std::move(options), transport));
    Status s = LoadHardState(node->state_path_, &node->hard_);
    if (!s.ok()) return s;
    s = RaftLog::Open((std::filesystem::path(node->options_.data_dir) / "raft.log").string(),
                      node->options_.sync, &node->log_);
    if (!s.ok()) return s;

    node->ResetElectionDeadline();
    *out = std::move(node);
    return Status::Ok();
}

RaftNode::~RaftNode() { Stop(); }

void RaftNode::Start() {
    std::lock_guard<std::mutex> lock(mu_);
    if (running_ || stopping_) return;
    running_ = true;
    ResetElectionDeadline();
    NotifyIfChanged();
    ticker_ = std::thread([this] { Run(); });
}

void RaftNode::Stop() {
    {
        std::lock_guard<std::mutex> lock(mu_);
        if (stopping_) return;
        stopping_ = true;
    }
    cv_.notify_all();
    if (ticker_.joinable()) ticker_.join();
}

void RaftNode::Run() {
    std::unique_lock<std::mutex> lock(mu_);
    while (!stopping_) {
        const auto now = Clock::now();
        if (role_ == Role::kLeader) {
            if (now >= next_heartbeat_) {
                BroadcastHeartbeat();
            }
            cv_.wait_until(lock, next_heartbeat_);
        } else {
            if (now >= election_deadline_) {
                StartElection();
                NotifyIfChanged();
            }
            if (role_ != Role::kLeader) {
                cv_.wait_until(lock, election_deadline_);
            }
        }
    }
}

void RaftNode::Campaign() {
    std::lock_guard<std::mutex> lock(mu_);
    if (stopping_ || role_ == Role::kLeader) return;
    StartElection();
    NotifyIfChanged();
}

void RaftNode::Step(const RpcMessage& message) {
    std::lock_guard<std::mutex> lock(mu_);
    if (stopping_) return;
    std::visit([this](const auto& m) { Handle(m); }, message);
    NotifyIfChanged();
}

RaftStatus RaftNode::status() const {
    std::lock_guard<std::mutex> lock(mu_);
    return StatusLocked();
}

Status RaftNode::last_error() const {
    std::lock_guard<std::mutex> lock(mu_);
    return error_;
}

RaftStatus RaftNode::StatusLocked() const {
    RaftStatus s;
    s.id = options_.id;
    s.role = role_;
    s.term = hard_.term;
    s.leader_id = leader_id_;
    s.voted_for = hard_.voted_for;
    s.last_log_index = log_->last_index();
    s.last_log_term = log_->last_term();
    s.commit_index = commit_index_;
    s.elections_started = elections_started_;
    return s;
}

void RaftNode::NotifyIfChanged() {
    if (!options_.on_state_change) return;
    if (reported_once_ && reported_role_ == role_ && reported_term_ == hard_.term &&
        reported_leader_ == leader_id_) {
        return;
    }
    reported_once_ = true;
    reported_role_ = role_;
    reported_term_ = hard_.term;
    reported_leader_ = leader_id_;
    options_.on_state_change(StatusLocked());
}

bool RaftNode::IsPeer(NodeId id) const {
    return std::find(options_.peers.begin(), options_.peers.end(), id) != options_.peers.end();
}

void RaftNode::ResetElectionDeadline() {
    std::uniform_int_distribution<int64_t> dist(options_.election_timeout_min.count(),
                                                options_.election_timeout_max.count());
    election_deadline_ = Clock::now() + std::chrono::milliseconds(dist(rng_));
}

bool RaftNode::Persist(uint64_t term, NodeId voted_for) {
    const HardState next{term, voted_for};
    if (next == hard_) return true;
    Status s = SaveHardState(state_path_, next);
    if (!s.ok()) {
        if (error_.ok()) error_ = s;
        return false;
    }
    hard_ = next;
    return true;
}

// Moves to follower, adopting `term` if it is newer. Returns false if the
// newer term could not be saved, in which case nothing changes.
bool RaftNode::StepDown(uint64_t term) {
    if (term > hard_.term) {
        if (!Persist(term, kNoNode)) return false;
        leader_id_ = kNoNode;
    }
    const bool was_leader = role_ == Role::kLeader;
    role_ = Role::kFollower;
    votes_.clear();
    if (was_leader) {
        ResetElectionDeadline();
        cv_.notify_all();  // the ticker was waiting for a heartbeat time
    }
    return true;
}

void RaftNode::StartElection() {
    if (!Persist(hard_.term + 1, options_.id)) {
        ResetElectionDeadline();  // try again later
        return;
    }
    role_ = Role::kCandidate;
    leader_id_ = kNoNode;
    votes_ = {options_.id};
    ++elections_started_;
    ResetElectionDeadline();

    if (votes_.size() >= Majority()) {
        BecomeLeader();  // single-node group
        return;
    }
    const RequestVoteRequest req{hard_.term, options_.id, log_->last_index(), log_->last_term()};
    for (const NodeId peer : options_.peers) {
        transport_->Send(peer, req);
    }
}

void RaftNode::BecomeLeader() {
    role_ = Role::kLeader;
    leader_id_ = options_.id;
    votes_.clear();
    BroadcastHeartbeat();
    cv_.notify_all();  // switch the ticker to heartbeat timing
}

void RaftNode::BroadcastHeartbeat() {
    next_heartbeat_ = Clock::now() + options_.heartbeat_interval;
    AppendEntriesRequest hb;
    hb.term = hard_.term;
    hb.leader_id = options_.id;
    hb.prev_log_index = log_->last_index();
    hb.prev_log_term = log_->last_term();
    hb.leader_commit = commit_index_;
    for (const NodeId peer : options_.peers) {
        transport_->Send(peer, hb);
    }
}

void RaftNode::Handle(const RequestVoteRequest& m) {
    if (!IsPeer(m.candidate_id)) return;
    if (m.term > hard_.term && !StepDown(m.term)) return;

    bool granted = false;
    if (m.term == hard_.term &&
        (hard_.voted_for == kNoNode || hard_.voted_for == m.candidate_id) &&
        CandidateLogIsUpToDate(m.last_log_term, m.last_log_index, log_->last_term(),
                               log_->last_index())) {
        // The vote must be on disk before the candidate can count it.
        granted = Persist(hard_.term, m.candidate_id);
        if (granted) {
            // Granting a vote counts as hearing from a plausible leader; it
            // stops this node from starting a competing election right away.
            ResetElectionDeadline();
        }
    }
    transport_->Send(m.candidate_id, RequestVoteResponse{hard_.term, options_.id, granted});
}

void RaftNode::Handle(const RequestVoteResponse& m) {
    if (!IsPeer(m.voter_id)) return;
    if (m.term > hard_.term) {
        StepDown(m.term);
        return;
    }
    if (role_ != Role::kCandidate || m.term != hard_.term || !m.vote_granted) return;
    votes_.insert(m.voter_id);
    if (votes_.size() >= Majority()) {
        BecomeLeader();
    }
}

void RaftNode::Handle(const AppendEntriesRequest& m) {
    if (!IsPeer(m.leader_id)) return;
    AppendEntriesResponse resp{hard_.term, options_.id, false, 0, 0};
    if (m.term < hard_.term) {
        transport_->Send(m.leader_id, resp);  // stale leader learns the newer term
        return;
    }
    if (m.term == hard_.term && role_ == Role::kLeader) {
        return;  // two leaders in one term cannot happen; ignore rather than corrupt
    }
    // A candidate that hears from a leader of its own term lost the election.
    if (!StepDown(m.term)) return;
    leader_id_ = m.leader_id;
    ResetElectionDeadline();
    resp.term = hard_.term;

    // Consistency check: our log must contain prev_log_index with the
    // leader's term there, or the leader has to back up.
    uint64_t prev_term = 0;
    if (!log_->TermAt(m.prev_log_index, &prev_term)) {
        resp.conflict_index = log_->last_index() + 1;
        transport_->Send(m.leader_id, resp);
        return;
    }
    if (prev_term != m.prev_log_term) {
        // Skip back over the whole conflicting term in one go.
        uint64_t i = m.prev_log_index;
        while (i > 1 && log_->At(i - 1).term == prev_term) --i;
        resp.conflict_index = i;
        transport_->Send(m.leader_id, resp);
        return;
    }

    uint64_t last_new = 0;
    Status s = log_->AppendFromLeader(m.entries, &last_new);
    if (!s.ok()) {
        if (error_.ok()) error_ = s;
        resp.conflict_index = log_->last_index() + 1;
        transport_->Send(m.leader_id, resp);
        return;
    }
    const uint64_t match = m.entries.empty() ? m.prev_log_index : last_new;
    if (m.leader_commit > commit_index_) {
        // Only entries known to match the leader's log may be committed.
        commit_index_ = std::max(commit_index_, std::min(m.leader_commit, match));
    }
    resp.success = true;
    resp.match_index = match;
    transport_->Send(m.leader_id, resp);
}

void RaftNode::Handle(const AppendEntriesResponse& m) {
    if (!IsPeer(m.follower_id)) return;
    if (m.term > hard_.term) {
        StepDown(m.term);
    }
    // Tracking follower progress belongs to log replication.
}

}  // namespace distkv::raft

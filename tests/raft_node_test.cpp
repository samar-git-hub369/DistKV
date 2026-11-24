#include "raft/raft_node.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <filesystem>
#include <map>
#include <mutex>
#include <random>
#include <set>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace distkv::raft {
namespace {

namespace fs = std::filesystem;
using namespace std::chrono_literals;

// Records what a node sends, for driving one node by hand.
class RecordingTransport : public RaftTransport {
  public:
    void Send(NodeId to, const RpcMessage& message) override {
        std::lock_guard<std::mutex> lock(mu_);
        sent_.emplace_back(to, message);
    }

    std::vector<std::pair<NodeId, RpcMessage>> Take() {
        std::lock_guard<std::mutex> lock(mu_);
        return std::exchange(sent_, {});
    }

  private:
    std::mutex mu_;
    std::vector<std::pair<NodeId, RpcMessage>> sent_;
};

// In-process network for whole clusters: messages are delivered on a separate
// thread (so Send never re-enters a node), and links can be cut.
class LocalNetwork {
  public:
    class Endpoint : public RaftTransport {
      public:
        Endpoint(LocalNetwork* net, NodeId self) : net_(net), self_(self) {}
        void Send(NodeId to, const RpcMessage& message) override {
            net_->Enqueue(self_, to, message);
        }

      private:
        LocalNetwork* net_;
        NodeId self_;
    };

    LocalNetwork() : thread_([this] { Deliver(); }) {}
    ~LocalNetwork() { Shutdown(); }

    void Shutdown() {
        {
            std::lock_guard<std::mutex> lock(mu_);
            if (stop_) return;
            stop_ = true;
        }
        cv_.notify_all();
        thread_.join();
    }

    Endpoint* AddEndpoint(NodeId id) {
        endpoints_.push_back(std::make_unique<Endpoint>(this, id));
        return endpoints_.back().get();
    }

    void Attach(NodeId id, RaftNode* node) {
        std::lock_guard<std::mutex> lock(mu_);
        nodes_[id] = node;
    }

    // Drops all traffic to and from `id` until Heal().
    void Isolate(NodeId id) {
        std::lock_guard<std::mutex> lock(mu_);
        isolated_.insert(id);
    }
    void Heal() {
        std::lock_guard<std::mutex> lock(mu_);
        isolated_.clear();
    }

  private:
    struct Message {
        NodeId from;
        NodeId to;
        RpcMessage body;
    };

    void Enqueue(NodeId from, NodeId to, const RpcMessage& body) {
        {
            std::lock_guard<std::mutex> lock(mu_);
            queue_.push_back(Message{from, to, body});
        }
        cv_.notify_one();
    }

    void Deliver() {
        std::unique_lock<std::mutex> lock(mu_);
        while (true) {
            cv_.wait(lock, [this] { return stop_ || !queue_.empty(); });
            if (stop_) return;
            Message m = std::move(queue_.front());
            queue_.pop_front();
            if (isolated_.count(m.from) || isolated_.count(m.to)) continue;
            auto it = nodes_.find(m.to);
            if (it == nodes_.end()) continue;
            RaftNode* node = it->second;
            lock.unlock();
            node->Step(m.body);
            lock.lock();
        }
    }

    std::mutex mu_;
    std::condition_variable cv_;
    std::deque<Message> queue_;
    std::map<NodeId, RaftNode*> nodes_;
    std::set<NodeId> isolated_;
    std::vector<std::unique_ptr<Endpoint>> endpoints_;
    bool stop_ = false;
    std::thread thread_;
};

fs::path TempDir(const std::string& name) {
    std::random_device rd;
    return fs::temp_directory_path() / ("distkv_" + name + "_" + std::to_string(rd()));
}

template <typename Pred>
bool Eventually(Pred pred, std::chrono::milliseconds limit = 10s) {
    const auto deadline = std::chrono::steady_clock::now() + limit;
    while (!pred()) {
        if (std::chrono::steady_clock::now() > deadline) return false;
        std::this_thread::sleep_for(5ms);
    }
    return true;
}

// ---------------------------------------------------------------------------
// One node driven by hand. Timeouts are long, so only the test acts.

class RaftNodeTest : public ::testing::Test {
  protected:
    void SetUp() override { dir_ = TempDir("raft_node"); }
    void TearDown() override {
        node_.reset();
        std::error_code ec;
        fs::remove_all(dir_, ec);
    }

    RaftOptions Options(NodeId id, std::vector<NodeId> peers) {
        RaftOptions o;
        o.id = id;
        o.peers = std::move(peers);
        o.data_dir = (dir_ / ("n" + std::to_string(id))).string();
        o.election_timeout_min = 1h;
        o.election_timeout_max = 1h;
        o.sync = false;
        return o;
    }

    void OpenNode(NodeId id = 1, std::vector<NodeId> peers = {2, 3}) {
        node_.reset();
        ASSERT_TRUE(RaftNode::Open(Options(id, std::move(peers)), &transport_, &node_).ok());
    }

    // Seeds node 1's log before it is opened.
    void SeedLog(const std::vector<LogEntry>& entries) {
        const fs::path d = dir_ / "n1";
        fs::create_directories(d);
        std::unique_ptr<RaftLog> log;
        ASSERT_TRUE(RaftLog::Open((d / "raft.log").string(), false, &log).ok());
        ASSERT_TRUE(log->Append(entries).ok());
    }

    // The single message sent, which must be of type T to `to`.
    template <typename T>
    T SentOne(NodeId to) {
        auto sent = transport_.Take();
        EXPECT_EQ(sent.size(), 1u);
        if (sent.size() != 1) return T{};
        EXPECT_EQ(sent[0].first, to);
        EXPECT_TRUE(std::holds_alternative<T>(sent[0].second));
        if (!std::holds_alternative<T>(sent[0].second)) return T{};
        return std::get<T>(sent[0].second);
    }

    fs::path dir_;
    RecordingTransport transport_;
    std::unique_ptr<RaftNode> node_;
};

TEST_F(RaftNodeTest, RejectsBadOptions) {
    std::unique_ptr<RaftNode> n;
    EXPECT_EQ(RaftNode::Open(Options(0, {2}), &transport_, &n).code(),
              StatusCode::kInvalidArgument);
    EXPECT_EQ(RaftNode::Open(Options(1, {1, 2}), &transport_, &n).code(),
              StatusCode::kInvalidArgument);
    EXPECT_EQ(RaftNode::Open(Options(1, {2, 2}), &transport_, &n).code(),
              StatusCode::kInvalidArgument);
    EXPECT_EQ(RaftNode::Open(Options(1, {2}), nullptr, &n).code(),
              StatusCode::kInvalidArgument);
    RaftOptions o = Options(1, {2});
    o.election_timeout_max = 1ms;
    o.election_timeout_min = 10ms;
    EXPECT_EQ(RaftNode::Open(o, &transport_, &n).code(), StatusCode::kInvalidArgument);
}

TEST_F(RaftNodeTest, GrantsOneVotePerTerm) {
    OpenNode();
    node_->Step(RequestVoteRequest{1, 2, 0, 0});
    auto r = SentOne<RequestVoteResponse>(2);
    EXPECT_TRUE(r.vote_granted);
    EXPECT_EQ(r.term, 1u);
    EXPECT_EQ(r.voter_id, 1u);

    // Another candidate in the same term is refused...
    node_->Step(RequestVoteRequest{1, 3, 0, 0});
    EXPECT_FALSE(SentOne<RequestVoteResponse>(3).vote_granted);
    // ...but a repeated request from the same candidate is granted again.
    node_->Step(RequestVoteRequest{1, 2, 0, 0});
    EXPECT_TRUE(SentOne<RequestVoteResponse>(2).vote_granted);

    // A new term frees the vote.
    node_->Step(RequestVoteRequest{2, 3, 0, 0});
    EXPECT_TRUE(SentOne<RequestVoteResponse>(3).vote_granted);
    EXPECT_EQ(node_->status().term, 2u);
    EXPECT_EQ(node_->status().voted_for, 3u);
}

TEST_F(RaftNodeTest, VoteSurvivesRestart) {
    OpenNode();
    node_->Step(RequestVoteRequest{5, 2, 0, 0});
    EXPECT_TRUE(SentOne<RequestVoteResponse>(2).vote_granted);

    OpenNode();  // crash and restart
    EXPECT_EQ(node_->status().term, 5u);
    EXPECT_EQ(node_->status().voted_for, 2u);
    node_->Step(RequestVoteRequest{5, 3, 0, 0});
    EXPECT_FALSE(SentOne<RequestVoteResponse>(3).vote_granted);
}

TEST_F(RaftNodeTest, RefusesStaleTermsAndOutdatedLogs) {
    SeedLog({LogEntry{1, 1, EntryOp::kPut, "a", "1"}, LogEntry{2, 2, EntryOp::kPut, "b", "2"}});
    OpenNode();
    node_->Step(RequestVoteRequest{3, 2, 0, 0});  // adopt term 3 via an outdated candidate
    auto r = SentOne<RequestVoteResponse>(2);
    EXPECT_FALSE(r.vote_granted);
    EXPECT_EQ(r.term, 3u);

    node_->Step(RequestVoteRequest{2, 3, 9, 2});  // stale term
    r = SentOne<RequestVoteResponse>(3);
    EXPECT_FALSE(r.vote_granted);
    EXPECT_EQ(r.term, 3u);

    node_->Step(RequestVoteRequest{3, 3, 9, 1});  // longer log, older last term
    EXPECT_FALSE(SentOne<RequestVoteResponse>(3).vote_granted);
    node_->Step(RequestVoteRequest{3, 3, 1, 2});  // same last term, shorter
    EXPECT_FALSE(SentOne<RequestVoteResponse>(3).vote_granted);
    node_->Step(RequestVoteRequest{3, 3, 2, 2});  // as up to date
    EXPECT_TRUE(SentOne<RequestVoteResponse>(3).vote_granted);
}

TEST_F(RaftNodeTest, CampaignNeedsAMajority) {
    OpenNode(1, {2, 3, 4, 5});
    node_->Campaign();
    auto sent = transport_.Take();
    ASSERT_EQ(sent.size(), 4u);
    for (const auto& [to, msg] : sent) {
        const auto& req = std::get<RequestVoteRequest>(msg);
        EXPECT_EQ(req.term, 1u);
        EXPECT_EQ(req.candidate_id, 1u);
    }
    EXPECT_EQ(node_->status().role, Role::kCandidate);
    EXPECT_EQ(node_->status().voted_for, 1u);

    node_->Step(RequestVoteResponse{1, 2, true});
    node_->Step(RequestVoteResponse{1, 2, true});  // duplicate counts once
    node_->Step(RequestVoteResponse{1, 4, false});
    node_->Step(RequestVoteResponse{0, 5, true});  // from an older term
    node_->Step(RequestVoteResponse{1, 9, true});  // not a member
    EXPECT_EQ(node_->status().role, Role::kCandidate);

    node_->Step(RequestVoteResponse{1, 3, true});  // 3 of 5
    EXPECT_EQ(node_->status().role, Role::kLeader);
    EXPECT_EQ(node_->status().leader_id, 1u);
    sent = transport_.Take();
    ASSERT_EQ(sent.size(), 4u);  // immediate heartbeats
    for (const auto& [to, msg] : sent) {
        EXPECT_EQ(std::get<AppendEntriesRequest>(msg).term, 1u);
        EXPECT_TRUE(std::get<AppendEntriesRequest>(msg).entries.empty());
    }
}

TEST_F(RaftNodeTest, HigherTermMakesLeaderStepDown) {
    OpenNode();
    node_->Campaign();
    node_->Step(RequestVoteResponse{1, 2, true});
    ASSERT_EQ(node_->status().role, Role::kLeader);
    transport_.Take();

    node_->Step(AppendEntriesResponse{4, 3, false, 0, 0});
    const RaftStatus s = node_->status();
    EXPECT_EQ(s.role, Role::kFollower);
    EXPECT_EQ(s.term, 4u);
    EXPECT_EQ(s.voted_for, kNoNode);
    EXPECT_EQ(s.leader_id, kNoNode);
}

TEST_F(RaftNodeTest, SingleNodeElectsItself) {
    OpenNode(1, {});
    node_->Campaign();
    EXPECT_EQ(node_->status().role, Role::kLeader);
    EXPECT_EQ(node_->status().term, 1u);
    EXPECT_TRUE(transport_.Take().empty());
}

TEST_F(RaftNodeTest, AppendEntriesFollowsLeaderAndChecksConsistency) {
    OpenNode();
    node_->Campaign();
    transport_.Take();
    ASSERT_EQ(node_->status().role, Role::kCandidate);

    // Stale leader: refused with our term.
    AppendEntriesRequest stale;
    stale.term = 0;
    stale.leader_id = 2;
    node_->Step(stale);
    auto r = SentOne<AppendEntriesResponse>(2);
    EXPECT_FALSE(r.success);
    EXPECT_EQ(r.term, 1u);

    // A leader of our own term wins: the candidate steps down.
    AppendEntriesRequest req;
    req.term = 1;
    req.leader_id = 2;
    req.entries = {LogEntry{1, 1, EntryOp::kPut, "a", "1"}, LogEntry{1, 2, EntryOp::kPut, "b", "2"},
                   LogEntry{1, 3, EntryOp::kPut, "c", "3"}};
    req.leader_commit = 2;
    node_->Step(req);
    r = SentOne<AppendEntriesResponse>(2);
    EXPECT_TRUE(r.success);
    EXPECT_EQ(r.match_index, 3u);
    RaftStatus s = node_->status();
    EXPECT_EQ(s.role, Role::kFollower);
    EXPECT_EQ(s.leader_id, 2u);
    EXPECT_EQ(s.last_log_index, 3u);
    EXPECT_EQ(s.commit_index, 2u);

    // Missing entries: tell the leader where our log ends.
    AppendEntriesRequest gap;
    gap.term = 1;
    gap.leader_id = 2;
    gap.prev_log_index = 7;
    gap.prev_log_term = 1;
    node_->Step(gap);
    r = SentOne<AppendEntriesResponse>(2);
    EXPECT_FALSE(r.success);
    EXPECT_EQ(r.conflict_index, 4u);

    // Term mismatch at prev: skip back to the start of our conflicting term.
    AppendEntriesRequest mismatch;
    mismatch.term = 2;
    mismatch.leader_id = 3;
    mismatch.prev_log_index = 3;
    mismatch.prev_log_term = 2;
    node_->Step(mismatch);
    r = SentOne<AppendEntriesResponse>(3);
    EXPECT_FALSE(r.success);
    EXPECT_EQ(r.conflict_index, 1u);
    EXPECT_EQ(node_->status().leader_id, 3u);

    // A heartbeat that matches but knows a later commit only commits what
    // matches.
    AppendEntriesRequest hb;
    hb.term = 2;
    hb.leader_id = 3;
    hb.prev_log_index = 3;
    hb.prev_log_term = 1;
    hb.leader_commit = 10;
    node_->Step(hb);
    r = SentOne<AppendEntriesResponse>(3);
    EXPECT_TRUE(r.success);
    EXPECT_EQ(r.match_index, 3u);
    EXPECT_EQ(node_->status().commit_index, 3u);
}

TEST_F(RaftNodeTest, IgnoresUnknownSendersAndMessagesAfterStop) {
    OpenNode();
    node_->Step(RequestVoteRequest{1, 7, 0, 0});
    AppendEntriesRequest hb;
    hb.term = 1;
    hb.leader_id = 7;
    node_->Step(hb);
    EXPECT_TRUE(transport_.Take().empty());
    EXPECT_EQ(node_->status().term, 0u);

    node_->Stop();
    node_->Step(RequestVoteRequest{1, 2, 0, 0});
    EXPECT_TRUE(transport_.Take().empty());
}

TEST_F(RaftNodeTest, ReportsStateChanges) {
    std::vector<std::string> events;
    RaftOptions o = Options(1, {2});
    o.on_state_change = [&events](const RaftStatus& s) {
        events.push_back(std::string(RoleName(s.role)) + "@" + std::to_string(s.term) + "/" +
                         std::to_string(s.leader_id));
    };
    ASSERT_TRUE(RaftNode::Open(o, &transport_, &node_).ok());
    node_->Start();
    node_->Campaign();
    node_->Step(RequestVoteResponse{1, 2, true});
    node_->Step(RequestVoteResponse{1, 2, true});  // no change, no event
    node_->Step(AppendEntriesResponse{3, 2, false, 0, 0});
    node_->Stop();
    EXPECT_EQ(events, (std::vector<std::string>{"follower@0/0", "candidate@1/0", "leader@1/1",
                                                "follower@3/0"}));
}

// ---------------------------------------------------------------------------
// Whole clusters on the in-process network, with real timers.

class RaftClusterTest : public ::testing::Test {
  protected:
    void TearDown() override {
        net_.Shutdown();
        nodes_.clear();
        std::error_code ec;
        fs::remove_all(dir_, ec);
    }

    void StartCluster(size_t n) {
        dir_ = TempDir("raft_cluster");
        for (NodeId id = 1; id <= n; ++id) {
            RaftOptions o;
            o.id = id;
            for (NodeId p = 1; p <= n; ++p) {
                if (p != id) o.peers.push_back(p);
            }
            o.data_dir = (dir_ / ("n" + std::to_string(id))).string();
            o.election_timeout_min = 100ms;
            o.election_timeout_max = 200ms;
            o.heartbeat_interval = 20ms;
            o.sync = false;
            std::unique_ptr<RaftNode> node;
            ASSERT_TRUE(RaftNode::Open(o, net_.AddEndpoint(id), &node).ok());
            net_.Attach(id, node.get());
            nodes_[id] = std::move(node);
        }
        for (auto& [id, node] : nodes_) node->Start();
    }

    // The leader agreed on by every node in `among`, or kNoNode.
    NodeId StableLeader(const std::set<NodeId>& among) {
        NodeId leader = kNoNode;
        uint64_t term = 0;
        size_t leaders = 0;
        for (const NodeId id : among) {
            const RaftStatus s = nodes_[id]->status();
            if (s.role == Role::kLeader) ++leaders;
            if (id == *among.begin()) {
                leader = s.leader_id;
                term = s.term;
            } else if (s.leader_id != leader || s.term != term) {
                return kNoNode;
            }
        }
        return leaders == 1 && among.count(leader) ? leader : kNoNode;
    }

    std::set<NodeId> All() const {
        std::set<NodeId> ids;
        for (const auto& [id, node] : nodes_) ids.insert(id);
        return ids;
    }

    fs::path dir_;
    LocalNetwork net_;
    std::map<NodeId, std::unique_ptr<RaftNode>> nodes_;
};

TEST_F(RaftClusterTest, ElectsExactlyOneLeaderAndKeepsIt) {
    StartCluster(3);
    NodeId leader = kNoNode;
    ASSERT_TRUE(Eventually([&] { return (leader = StableLeader(All())) != kNoNode; }));
    const uint64_t term = nodes_[leader]->status().term;

    // Heartbeats keep the followers from starting elections.
    std::this_thread::sleep_for(600ms);
    EXPECT_EQ(StableLeader(All()), leader);
    EXPECT_EQ(nodes_[leader]->status().term, term);
}

TEST_F(RaftClusterTest, IsolatedLeaderIsReplacedAndStepsDownOnReturn) {
    StartCluster(3);
    NodeId old_leader = kNoNode;
    ASSERT_TRUE(Eventually([&] { return (old_leader = StableLeader(All())) != kNoNode; }));
    const uint64_t old_term = nodes_[old_leader]->status().term;

    net_.Isolate(old_leader);
    std::set<NodeId> rest = All();
    rest.erase(old_leader);
    NodeId new_leader = kNoNode;
    ASSERT_TRUE(Eventually([&] { return (new_leader = StableLeader(rest)) != kNoNode; }));
    EXPECT_NE(new_leader, old_leader);
    EXPECT_GT(nodes_[new_leader]->status().term, old_term);
    // Cut off, the old leader still believes it leads; it cannot do harm
    // because it cannot reach a majority.
    EXPECT_EQ(nodes_[old_leader]->status().role, Role::kLeader);

    net_.Heal();
    ASSERT_TRUE(Eventually([&] { return StableLeader(All()) != kNoNode; }));
    EXPECT_NE(nodes_[old_leader]->status().role, Role::kLeader);
}

TEST_F(RaftClusterTest, MinorityCannotElectALeader) {
    StartCluster(5);
    NodeId leader = kNoNode;
    ASSERT_TRUE(Eventually([&] { return (leader = StableLeader(All())) != kNoNode; }));

    const NodeId lonely = leader == 1 ? 2 : 1;
    net_.Isolate(lonely);
    std::this_thread::sleep_for(800ms);
    const RaftStatus s = nodes_[lonely]->status();
    EXPECT_NE(s.role, Role::kLeader);
    EXPECT_GT(s.elections_started, 0u);  // it keeps trying, and keeps failing
    std::set<NodeId> rest = All();
    rest.erase(lonely);
    EXPECT_EQ(StableLeader(rest), leader);  // the majority side is undisturbed
}

}  // namespace
}  // namespace distkv::raft

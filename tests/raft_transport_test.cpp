#include "raft/asio_transport.h"

#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <map>
#include <mutex>
#include <random>
#include <thread>
#include <vector>

namespace distkv::raft {
namespace {

namespace asio = boost::asio;
namespace fs = std::filesystem;
using asio::ip::tcp;
using namespace std::chrono_literals;

template <typename Pred>
bool Eventually(Pred pred, std::chrono::milliseconds limit = 10s) {
    const auto deadline = std::chrono::steady_clock::now() + limit;
    while (!pred()) {
        if (std::chrono::steady_clock::now() > deadline) return false;
        std::this_thread::sleep_for(5ms);
    }
    return true;
}

// Collects received messages.
struct Inbox {
    std::mutex mu;
    std::vector<RpcMessage> messages;

    AsioTransport::Handler Handler() {
        return [this](const RpcMessage& m) {
            std::lock_guard<std::mutex> lock(mu);
            messages.push_back(m);
        };
    }
    size_t size() {
        std::lock_guard<std::mutex> lock(mu);
        return messages.size();
    }
};

TransportOptions Options(std::vector<PeerAddress> peers, uint16_t port = 0) {
    TransportOptions o;
    o.listen_host = "127.0.0.1";
    o.listen_port = port;
    o.peers = std::move(peers);
    o.reconnect_backoff = 20ms;
    return o;
}

// A port nothing is listening on right now.
uint16_t FreePort() {
    asio::io_context io;
    tcp::acceptor a(io, tcp::endpoint(asio::ip::make_address("127.0.0.1"), 0));
    return a.local_endpoint().port();
}

TEST(PeerListTest, ParsesAndRejects) {
    std::vector<PeerAddress> peers;
    std::string error;
    ASSERT_TRUE(ParsePeerList("2@10.0.0.2:7101,3@node3:7102", &peers, &error)) << error;
    ASSERT_EQ(peers.size(), 2u);
    EXPECT_EQ(peers[0].id, 2u);
    EXPECT_EQ(peers[0].host, "10.0.0.2");
    EXPECT_EQ(peers[0].port, 7101);
    EXPECT_EQ(peers[1].host, "node3");

    ASSERT_TRUE(ParsePeerList("4@::1:9000", &peers, &error));  // last colon splits the port
    EXPECT_EQ(peers[0].host, "::1");

    for (const char* bad : {"", "2", "2@host", "@h:1", "2@:1", "2@h:", "0@h:1", "x@h:1",
                            "2@h:70000", "2@h:0", "2@h:1,2@g:2", "2@h:1,"}) {
        EXPECT_FALSE(ParsePeerList(bad, &peers, &error)) << bad;
    }
}

TEST(AsioTransportTest, DeliversBothWays) {
    Inbox inbox_a;
    Inbox inbox_b;
    const uint16_t port_a = FreePort();
    const uint16_t port_b = FreePort();
    AsioTransport a(Options({{2, "127.0.0.1", port_b}}, port_a), inbox_a.Handler());
    AsioTransport b(Options({{1, "localhost", port_a}}, port_b), inbox_b.Handler());
    ASSERT_TRUE(a.Start().ok());
    ASSERT_TRUE(b.Start().ok());

    for (uint64_t i = 1; i <= 100; ++i) {
        a.Send(2, RequestVoteRequest{i, 1, 0, 0});
    }
    ASSERT_TRUE(Eventually([&] { return inbox_b.size() == 100; }));
    {
        std::lock_guard<std::mutex> lock(inbox_b.mu);
        for (uint64_t i = 1; i <= 100; ++i) {  // one connection keeps order
            EXPECT_EQ(std::get<RequestVoteRequest>(inbox_b.messages[i - 1]).term, i);
        }
    }

    b.Send(1, RequestVoteResponse{9, 2, true});
    ASSERT_TRUE(Eventually([&] { return inbox_a.size() == 1; }));
    EXPECT_TRUE(std::get<RequestVoteResponse>(inbox_a.messages[0]).vote_granted);

    a.Send(42, RequestVoteRequest{});  // unknown peer
    EXPECT_EQ(a.stats().frames_dropped, 1u);
    EXPECT_EQ(a.stats().frames_sent, 100u);
    EXPECT_EQ(b.stats().frames_received, 100u);
}

TEST(AsioTransportTest, ReconnectsWhenPeerComesBack) {
    Inbox inbox;
    const uint16_t port_b = FreePort();
    AsioTransport a(Options({{2, "127.0.0.1", port_b}}), [](const RpcMessage&) {});
    ASSERT_TRUE(a.Start().ok());

    // Nobody listening yet: sends are dropped, not queued forever.
    a.Send(2, RequestVoteRequest{1, 1, 0, 0});
    ASSERT_TRUE(Eventually([&] { return a.stats().connect_failures >= 1; }));

    auto b = std::make_unique<AsioTransport>(Options({}, port_b), inbox.Handler());
    ASSERT_TRUE(b->Start().ok());
    ASSERT_TRUE(Eventually([&] {
        a.Send(2, RequestVoteRequest{2, 1, 0, 0});  // keep trying, as Raft does
        std::this_thread::sleep_for(10ms);
        return inbox.size() > 0;
    }));

    // Peer restarts on the same port: the dead connection is noticed and
    // replaced.
    b.reset();
    Inbox inbox2;
    b = std::make_unique<AsioTransport>(Options({}, port_b), inbox2.Handler());
    ASSERT_TRUE(b->Start().ok());
    ASSERT_TRUE(Eventually([&] {
        a.Send(2, RequestVoteRequest{3, 1, 0, 0});
        std::this_thread::sleep_for(10ms);
        return inbox2.size() > 0;
    }));
}

TEST(AsioTransportTest, GarbageClosesOnlyThatConnection) {
    Inbox inbox;
    AsioTransport b(Options({}), inbox.Handler());
    ASSERT_TRUE(b.Start().ok());

    asio::io_context io;
    tcp::socket raw(io);
    raw.connect(tcp::endpoint(asio::ip::make_address("127.0.0.1"), b.port()));
    const std::string garbage(16, '\x7f');
    asio::write(raw, asio::buffer(garbage));
    char c = 0;
    boost::system::error_code ec;
    raw.read_some(asio::buffer(&c, 1), ec);  // server hangs up
    EXPECT_TRUE(ec);
    EXPECT_EQ(b.stats().corrupt_connections, 1u);

    // Well-formed frames on a fresh connection still get through.
    tcp::socket good(io);
    good.connect(tcp::endpoint(asio::ip::make_address("127.0.0.1"), b.port()));
    const std::string frame = EncodeRpcFrame(RequestVoteRequest{1, 1, 0, 0});
    // Byte by byte, to exercise reassembly.
    for (const char byte : frame) asio::write(good, asio::buffer(&byte, 1));
    ASSERT_TRUE(Eventually([&] { return inbox.size() == 1; }));
}

TEST(AsioTransportTest, StopIsIdempotentAndRejectsRestart) {
    AsioTransport t(Options({}), [](const RpcMessage&) {});
    ASSERT_TRUE(t.Start().ok());
    EXPECT_FALSE(t.Start().ok());
    t.Stop();
    t.Stop();
    t.Send(1, RequestVoteRequest{});  // after Stop: harmless
}

// Three real nodes, each with its own TCP transport.
TEST(AsioTransportTest, ThreeNodesElectALeaderOverTcp) {
    std::random_device rd;
    const fs::path dir = fs::temp_directory_path() / ("distkv_raft_tcp_" + std::to_string(rd()));
    std::map<NodeId, uint16_t> ports = {{1, FreePort()}, {2, FreePort()}, {3, FreePort()}};

    // Transports start delivering before every node exists, so handlers look
    // their node up here and drop messages until it is filled in.
    std::array<std::atomic<RaftNode*>, 4> slots{};
    std::map<NodeId, std::unique_ptr<RaftNode>> nodes;
    std::map<NodeId, std::unique_ptr<AsioTransport>> transports;
    for (const auto& [id, port] : ports) {
        TransportOptions to = Options({}, port);
        RaftOptions ro;
        ro.id = id;
        for (const auto& [peer, peer_port] : ports) {
            if (peer == id) continue;
            to.peers.push_back({peer, "127.0.0.1", peer_port});
            ro.peers.push_back(peer);
        }
        ro.data_dir = (dir / ("n" + std::to_string(id))).string();
        ro.election_timeout_min = 150ms;
        ro.election_timeout_max = 300ms;
        ro.heartbeat_interval = 30ms;
        ro.sync = false;
        transports[id] = std::make_unique<AsioTransport>(to, [&slots, self = id](const RpcMessage& m) {
            if (RaftNode* node = slots[self].load()) node->Step(m);
        });
        ASSERT_TRUE(RaftNode::Open(ro, transports[id].get(), &nodes[id]).ok());
        slots[id] = nodes[id].get();
        ASSERT_TRUE(transports[id]->Start().ok());
    }
    for (auto& [id, node] : nodes) node->Start();

    auto leaders = [&] {
        size_t n = 0;
        for (auto& [id, node] : nodes) n += node->status().role == Role::kLeader ? 1 : 0;
        return n;
    };
    ASSERT_TRUE(Eventually([&] {
        if (leaders() != 1) return false;
        const RaftStatus s1 = nodes[1]->status();
        for (auto& [id, node] : nodes) {
            const RaftStatus s = node->status();
            if (s.term != s1.term || s.leader_id != s1.leader_id || s.leader_id == kNoNode) {
                return false;
            }
        }
        return true;
    }));

    for (auto& [id, t] : transports) t->Stop();
    for (auto& [id, n] : nodes) n->Stop();
    transports.clear();
    nodes.clear();
    std::error_code ec;
    fs::remove_all(dir, ec);
}

}  // namespace
}  // namespace distkv::raft

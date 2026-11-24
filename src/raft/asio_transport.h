#pragma once

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <vector>

#include <boost/asio.hpp>

#include "common/status.h"
#include "raft/raft_node.h"

// Carries Raft messages between nodes over TCP.
//
// Each node listens on a peer port and opens one outgoing connection to every
// other node. Outgoing connections only write and incoming ones only read,
// so a message and its reply travel on different sockets; messages name their
// sender, so nothing else has to be negotiated.
//
// Delivery is best effort, which is all Raft asks for. A peer that is down is
// retried at most once per backoff period, and messages for it are dropped
// meanwhile; the next heartbeat or election retries anyway. Each connection's
// queue is bounded so a slow peer cannot exhaust memory.
//
// All socket work runs on one io thread: Raft traffic is small, and a single
// thread needs no strands.
namespace distkv::raft {

struct PeerAddress {
    NodeId id = kNoNode;
    std::string host;
    uint16_t port = 0;
};

// Parses "2@10.0.0.2:7101,3@node3:7101". Returns false with `*error` set on
// malformed input, ids of 0 or duplicate ids.
bool ParsePeerList(std::string_view text, std::vector<PeerAddress>* out, std::string* error);

struct TransportOptions {
    std::string listen_host = "0.0.0.0";
    uint16_t listen_port = 0;  // 0 picks a free port; see port()
    std::vector<PeerAddress> peers;
    size_t max_queued_frames = 1024;  // per peer
    std::chrono::milliseconds reconnect_backoff{100};
};

struct TransportStats {
    uint64_t frames_sent = 0;
    uint64_t frames_received = 0;
    uint64_t frames_dropped = 0;  // unknown peer, peer unreachable or queue full
    uint64_t connect_failures = 0;
    uint64_t corrupt_connections = 0;  // incoming connections closed for bad frames
};

class AsioTransport : public RaftTransport {
  public:
    // `handler` receives every decoded message, on the io thread.
    using Handler = std::function<void(const RpcMessage&)>;

    AsioTransport(TransportOptions options, Handler handler);
    ~AsioTransport() override;  // calls Stop()

    AsioTransport(const AsioTransport&) = delete;
    AsioTransport& operator=(const AsioTransport&) = delete;

    // Binds the peer port and starts the io thread.
    Status Start();
    // Stops the io thread and closes every connection. After Stop the handler
    // is never called again. Idempotent.
    void Stop();

    void Send(NodeId to, const RpcMessage& message) override;

    uint16_t port() const { return bound_port_; }
    TransportStats stats() const;

  private:
    class Peer;
    class Inbound;

    void DoAccept();

    const TransportOptions options_;
    Handler handler_;

    boost::asio::io_context io_;
    boost::asio::executor_work_guard<boost::asio::io_context::executor_type> work_;
    boost::asio::ip::tcp::acceptor acceptor_;
    std::unordered_map<NodeId, std::shared_ptr<Peer>> peers_;  // fixed after construction
    std::vector<std::weak_ptr<Inbound>> inbound_;              // io thread only
    std::thread thread_;
    uint16_t bound_port_ = 0;

    std::mutex lifecycle_mu_;
    bool started_ = false;
    bool stopped_ = false;

    std::atomic<uint64_t> sent_{0};
    std::atomic<uint64_t> received_{0};
    std::atomic<uint64_t> dropped_{0};
    std::atomic<uint64_t> connect_failures_{0};
    std::atomic<uint64_t> corrupt_{0};
};

}  // namespace distkv::raft

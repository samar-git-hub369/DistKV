#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <boost/asio.hpp>

#include "common/status.h"
#include "server/command_handler.h"

// TCP front end for the text protocol, built on Boost.Asio.
//
// Threading: one io_context run by `threads` worker threads. Every connection
// lives on its own strand, so its handlers never run concurrently even though
// different connections are served in parallel.
//
// Per connection the server reads one line, handles it, writes the response,
// and only then reads the next line. That keeps responses in request order
// and gives natural backpressure: a client that stops reading responses stops
// being read from. Pipelined requests (several lines in one packet) are fine;
// the unread lines wait in the connection's buffer.
namespace distkv {

struct ServerOptions {
    std::string host = "0.0.0.0";
    uint16_t port = 7001;  // 0 picks a free port; see TcpServer::port()
    size_t threads = 4;
    size_t max_connections = 1024;
};

class TcpServer {
  public:
    TcpServer(ServerOptions options, CommandHandler* handler);
    ~TcpServer();  // calls Stop()

    TcpServer(const TcpServer&) = delete;
    TcpServer& operator=(const TcpServer&) = delete;

    // Binds, listens and starts the worker threads. Returns once the port is
    // accepting connections.
    Status Start();

    // Stops accepting, closes every connection and joins the workers.
    // Idempotent and safe to call from any thread except a worker.
    void Stop();

    // The bound port (useful after binding port 0).
    uint16_t port() const { return bound_port_; }
    size_t active_connections() const { return active_.load(); }
    uint64_t accepted_connections() const { return accepted_.load(); }
    uint64_t rejected_connections() const { return rejected_.load(); }

  private:
    class Session;

    void DoAccept();
    void Register(const std::shared_ptr<Session>& session);
    void Unregister(Session* session);

    const ServerOptions options_;
    CommandHandler* handler_;

    // Declared before io_ so they outlive it: a session unregisters itself in
    // its destructor, which can run while io_ is torn down.
    std::mutex sessions_mu_;
    // Live sessions, held weakly so Stop() can reach them without keeping
    // them alive.
    std::unordered_map<Session*, std::weak_ptr<Session>> sessions_;

    boost::asio::io_context io_;
    boost::asio::ip::tcp::acceptor acceptor_;
    std::vector<std::thread> workers_;
    uint16_t bound_port_ = 0;
    bool started_ = false;
    bool stopped_ = false;

    std::atomic<size_t> active_{0};
    std::atomic<uint64_t> accepted_{0};
    std::atomic<uint64_t> rejected_{0};
};

}  // namespace distkv

#include "network/server.hpp"

#include <algorithm>
#include <chrono>
#include <iostream>
#include <string>
#include <string_view>
#include <utility>

#include "protocol/resp.hpp"

namespace distkv {

namespace {

constexpr auto kAcceptRetryDelay = std::chrono::milliseconds(100);

bool is_resource_exhaustion(const asio::error_code& ec) {
    return ec == asio::error::no_descriptors || ec == std::errc::too_many_files_open_in_system ||
           ec == asio::error::no_buffer_space || ec == asio::error::no_memory;
}

}  // namespace

Server::Server(asio::io_context& io, CommandDispatcher& dispatcher, Session::Limits limits,
               std::size_t max_connections)
    : io_(io),
      strand_(asio::make_strand(io)),
      acceptor_(strand_),
      retry_timer_(strand_),
      dispatcher_(dispatcher),
      limits_(limits),
      max_connections_(max_connections) {}

std::error_code Server::listen(const asio::ip::address& address, std::uint16_t port) {
    const asio::ip::tcp::endpoint endpoint(address, port);
    asio::error_code ec;
    acceptor_.open(endpoint.protocol(), ec);
    if (!ec) acceptor_.set_option(asio::socket_base::reuse_address(true), ec);
    if (!ec) acceptor_.bind(endpoint, ec);
    if (!ec) acceptor_.listen(asio::socket_base::max_listen_connections, ec);
    if (!ec) port_ = acceptor_.local_endpoint(ec).port();
    if (ec) {
        asio::error_code ignored;
        acceptor_.close(ignored);
    }
    return ec;
}

void Server::start() {
    asio::dispatch(strand_, [this] { accept_next(); });
}

void Server::stop() {
    asio::post(strand_, [this] {
        asio::error_code ignored;
        acceptor_.close(ignored);
        retry_timer_.cancel();

        std::vector<std::weak_ptr<Session>> sessions;
        {
            std::lock_guard lock(sessions_mutex_);
            stopped_ = true;
            sessions.swap(sessions_);
        }
        for (const auto& weak : sessions) {
            if (const auto session = weak.lock()) session->close();
        }
    });
}

std::size_t Server::connection_count() const { return connections_->load(); }

std::size_t Server::live_sessions() {
    std::lock_guard lock(sessions_mutex_);
    return static_cast<std::size_t>(
        std::count_if(sessions_.begin(), sessions_.end(),
                      [](const auto& weak) { return !weak.expired(); }));
}

void Server::accept_next() {
    // Each connection gets its own strand, so sessions run safely on a
    // multi-threaded io_context without locking. The handler itself runs on
    // strand_, the acceptor's executor.
    acceptor_.async_accept(asio::make_strand(io_), [this](const asio::error_code& ec,
                                                          asio::ip::tcp::socket socket) {
        if (ec == asio::error::operation_aborted || !acceptor_.is_open()) {
            return;  // Stopped.
        }
        if (ec && is_resource_exhaustion(ec)) {
            // Out of file descriptors or memory: retrying at once would spin
            // until something is released, so back off briefly.
            std::cerr << "distkv: accept failed: " << ec.message() << '\n';
            retry_timer_.expires_after(kAcceptRetryDelay);
            retry_timer_.async_wait([this](const asio::error_code& wait_ec) {
                if (!wait_ec && acceptor_.is_open()) accept_next();
            });
            return;
        }
        // Any other error concerns that one connection (e.g. the client reset
        // it before it was accepted): skip it.
        if (!ec) on_accept(std::move(socket));
        accept_next();
    });
}

void Server::on_accept(asio::ip::tcp::socket socket) {
    // Take a slot first; give it back if that went over the limit. Slots are
    // released on session strands, hence the atomic.
    if (connections_->fetch_add(1) >= max_connections_) {
        connections_->fetch_sub(1);
        reject(std::move(socket));
        return;
    }

    auto session = std::make_shared<Session>(std::move(socket), dispatcher_, limits_,
                                             [connections = connections_] {
                                                 connections->fetch_sub(1);
                                             });
    bool stopped = false;
    {
        std::lock_guard lock(sessions_mutex_);
        stopped = stopped_;
        if (!stopped) {
            std::erase_if(sessions_, [](const auto& weak) { return weak.expired(); });
            sessions_.push_back(session);
        }
    }
    // Outside the lock: never call into a Session while holding it.
    if (stopped) {
        session->close();
    } else {
        session->start();
    }
}

// Like Redis: tell the client why, then close. The reply is tiny, so the write
// completes straight into the kernel's socket buffer.
void Server::reject(asio::ip::tcp::socket socket) {
    static const std::string kReply = resp::error("ERR max number of clients reached");
    auto peer = std::make_shared<asio::ip::tcp::socket>(std::move(socket));
    asio::async_write(*peer, asio::buffer(kReply), [peer](const asio::error_code&, std::size_t) {
        asio::error_code ignored;
        peer->shutdown(asio::ip::tcp::socket::shutdown_both, ignored);
        peer->close(ignored);
    });
}

}  // namespace distkv

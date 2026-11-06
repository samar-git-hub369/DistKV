#include "network/server.hpp"

#include <algorithm>
#include <chrono>
#include <iostream>
#include <utility>

namespace distkv {

namespace {

constexpr auto kAcceptRetryDelay = std::chrono::milliseconds(100);

bool is_resource_exhaustion(const asio::error_code& ec) {
    return ec == asio::error::no_descriptors || ec == std::errc::too_many_files_open_in_system ||
           ec == asio::error::no_buffer_space || ec == asio::error::no_memory;
}

}  // namespace

Server::Server(asio::io_context& io, CommandDispatcher& dispatcher, Session::Limits limits)
    : io_(io),
      strand_(asio::make_strand(io)),
      acceptor_(strand_),
      retry_timer_(strand_),
      dispatcher_(dispatcher),
      limits_(limits) {}

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
        retry_timer_.cancel();
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
        if (!ec) {
            auto session = std::make_shared<Session>(std::move(socket), dispatcher_, limits_);
            bool stopped = false;
            {
                std::lock_guard lock(sessions_mutex_);
                stopped = stopped_;
                if (!stopped) {
                    std::erase_if(sessions_, [](const auto& weak) { return weak.expired(); });
                    sessions_.push_back(session);
                }
            }
            if (stopped) {
                session->close();
            } else {
                session->start();
            }
        }
        accept_next();
    });
}

}  // namespace distkv

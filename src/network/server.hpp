#pragma once

#include <asio.hpp>
#include <cstdint>
#include <memory>
#include <mutex>
#include <system_error>
#include <vector>

#include "network/session.hpp"
#include "server/command_dispatcher.hpp"

namespace distkv {

// Accepts TCP connections and runs one Session per connection.
class Server {
public:
    // `io` and `dispatcher` must outlive the server, and the server must
    // outlive io.run(). `limits` applies to every session.
    Server(asio::io_context& io, CommandDispatcher& dispatcher, Session::Limits limits);
    Server(asio::io_context& io, CommandDispatcher& dispatcher)
        : Server(io, dispatcher, Session::Limits{}) {}

    Server(const Server&) = delete;
    Server& operator=(const Server&) = delete;

    // Opens the listening socket (with SO_REUSEADDR). Port 0 picks a free
    // port; see port(). Returns an error instead of throwing so callers can
    // report it precisely (e.g. address_in_use).
    std::error_code listen(const asio::ip::address& address, std::uint16_t port);
    std::error_code listen(std::uint16_t port) {
        return listen(asio::ip::address_v4::any(), port);
    }

    // The bound port. Valid after a successful listen().
    [[nodiscard]] std::uint16_t port() const { return port_; }

    // Starts accepting connections.
    void start();

    // Closes the acceptor and every open session, so io_context::run()
    // returns once their pending handlers finish, even with idle clients
    // connected. Thread-safe and idempotent.
    void stop();

private:
    void accept_next();

    asio::io_context& io_;
    asio::strand<asio::io_context::executor_type> strand_;  // Serialises acceptor use.
    asio::ip::tcp::acceptor acceptor_;
    CommandDispatcher& dispatcher_;
    const Session::Limits limits_;
    std::uint16_t port_{0};

    std::mutex sessions_mutex_;
    std::vector<std::weak_ptr<Session>> sessions_;  // Guarded by sessions_mutex_.
    bool stopped_{false};                           // Guarded by sessions_mutex_.
};

}  // namespace distkv

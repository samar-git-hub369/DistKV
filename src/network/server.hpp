#pragma once

#include <asio.hpp>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <system_error>
#include <vector>

#include "network/session.hpp"
#include "server/command_dispatcher.hpp"

namespace distkv {

// Accepts TCP connections and runs one Session per connection.
//
// Connection limit: at most `max_connections` sessions are open at once. A
// client accepted beyond that gets "-ERR max number of clients reached" and
// is closed, as Redis does; existing clients are unaffected. A slot is taken
// when a session is created and released when its connection closes.
//
// Thread safety (io_context::run() may be on any number of threads):
//   - acceptor_ and retry_timer_ are touched only on strand_, apart from
//     listen(), which must be called before start().
//   - sessions_ and stopped_ are guarded by sessions_mutex_. The mutex is
//     never held while calling into a Session or the dispatcher.
//   - The open-connection count is an atomic shared with every session (it
//     must outlive the Server if the io_context is destroyed later).
//   - io_, dispatcher_, limits_ and max_connections_ are immutable after
//     construction; port_ is written only by listen().
//   - stop(), connection_count() and live_sessions() may be called from any
//     thread.
class Server {
public:
    static constexpr std::size_t kDefaultMaxConnections = 10000;

    // `io` and `dispatcher` must outlive the server, and the server must
    // outlive io.run(). `limits` applies to every session.
    Server(asio::io_context& io, CommandDispatcher& dispatcher, Session::Limits limits,
           std::size_t max_connections);
    Server(asio::io_context& io, CommandDispatcher& dispatcher, Session::Limits limits)
        : Server(io, dispatcher, limits, kDefaultMaxConnections) {}
    Server(asio::io_context& io, CommandDispatcher& dispatcher)
        : Server(io, dispatcher, Session::Limits{}, kDefaultMaxConnections) {}

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

    // Connections currently holding a slot (open sessions).
    [[nodiscard]] std::size_t connection_count() const;

    // Session objects still alive, open or not. Drops to 0 once every
    // session's handlers have finished; a leak keeps it above 0.
    [[nodiscard]] std::size_t live_sessions();

private:
    void accept_next();
    void on_accept(asio::ip::tcp::socket socket);
    void reject(asio::ip::tcp::socket socket);

    asio::io_context& io_;
    asio::strand<asio::io_context::executor_type> strand_;  // Serialises acceptor use.
    asio::ip::tcp::acceptor acceptor_;
    asio::steady_timer retry_timer_;  // Backs off after accept errors (e.g. EMFILE).
    CommandDispatcher& dispatcher_;
    const Session::Limits limits_;
    const std::size_t max_connections_;
    std::uint16_t port_{0};

    const std::shared_ptr<std::atomic<std::size_t>> connections_ =
        std::make_shared<std::atomic<std::size_t>>(0);

    std::mutex sessions_mutex_;
    std::vector<std::weak_ptr<Session>> sessions_;  // Guarded by sessions_mutex_.
    bool stopped_{false};                           // Guarded by sessions_mutex_.
};

}  // namespace distkv

// Test support for end-to-end Server tests: a blocking RESP client and a
// harness running a real Server on an ephemeral loopback port, with its
// io_context on one or more threads.

#pragma once

#include <gtest/gtest.h>
#include <sys/socket.h>
#include <sys/time.h>

#include <asio.hpp>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "network/server.hpp"
#include "protocol/resp.hpp"
#include "replication/replicator.hpp"
#include "server/command_dispatcher.hpp"
#include "server/kv_service.hpp"
#include "storage/kv_store.hpp"

namespace distkv::test {

using Args = std::vector<std::string>;

constexpr auto kTimeout = std::chrono::seconds(5);

// Length of the complete RESP reply at the start of `buf`, if there is one.
inline std::optional<std::size_t> reply_length(std::string_view buf, std::size_t pos = 0) {
    if (pos >= buf.size()) return std::nullopt;
    const std::size_t eol = buf.find("\r\n", pos);
    if (eol == std::string_view::npos) return std::nullopt;
    const std::string_view header = buf.substr(pos + 1, eol - pos - 1);
    const std::size_t after_header = eol + 2;
    switch (buf[pos]) {
        case '+':
        case '-':
        case ':':
            return after_header - pos;
        case '$': {
            const long len = std::stol(std::string(header));
            if (len < 0) return after_header - pos;
            const std::size_t end = after_header + static_cast<std::size_t>(len) + 2;
            if (end > buf.size()) return std::nullopt;
            return end - pos;
        }
        case '*': {
            const long count = std::stol(std::string(header));
            std::size_t cursor = after_header;
            for (long i = 0; i < count; ++i) {
                const auto element = reply_length(buf, cursor);
                if (!element) return std::nullopt;
                cursor += *element;
            }
            return cursor - pos;
        }
        default:
            ADD_FAILURE() << "bad reply type byte: " << buf[pos];
            return std::nullopt;
    }
}

// Blocking test client. Reads time out so a server bug fails instead of hanging.
class Client {
public:
    explicit Client(std::uint16_t port) : socket_(io_) {
        socket_.connect({asio::ip::address_v4::loopback(), port});
        timeval tv{};
        tv.tv_sec = static_cast<decltype(tv.tv_sec)>(
            std::chrono::duration_cast<std::chrono::seconds>(kTimeout).count());
        ::setsockopt(socket_.native_handle(), SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    }

    void send(std::string_view bytes) { asio::write(socket_, asio::buffer(bytes)); }

    // Like send(), but reports failure (e.g. the server closed) instead of
    // throwing.
    bool try_send(std::string_view bytes) {
        asio::error_code ec;
        asio::write(socket_, asio::buffer(bytes), ec);
        return !ec;
    }
    void send_command(const Args& args) { send(resp::command(args)); }

    // Returns the next complete reply, or "" on timeout/EOF (with a failure).
    std::string read_reply() {
        while (true) {
            if (const auto len = reply_length(buffer_)) {
                std::string reply = buffer_.substr(0, *len);
                buffer_.erase(0, *len);
                return reply;
            }
            if (!read_more()) {
                ADD_FAILURE() << "connection ended while waiting for a reply";
                return {};
            }
        }
    }

    // Like read_reply(), but a closed or timed-out connection is not a test
    // failure.
    std::optional<std::string> try_read_reply() {
        while (true) {
            if (const auto len = reply_length(buffer_)) {
                std::string reply = buffer_.substr(0, *len);
                buffer_.erase(0, *len);
                return reply;
            }
            if (!read_more()) return std::nullopt;
        }
    }

    // Closes with a TCP reset (SO_LINGER 0), like a crashed client.
    void abort() {
        asio::error_code ignored;
        socket_.set_option(asio::socket_base::linger(true, 0), ignored);
        socket_.close(ignored);
    }

    std::string call(const Args& args) {
        send_command(args);
        return read_reply();
    }

    // True if the server closed the connection (EOF) before any more data.
    bool closed_by_server() {
        return buffer_.empty() && !read_more() && last_error_ == asio::error::eof;
    }

private:
    bool read_more() {
        char chunk[4096];
        const std::size_t n = socket_.read_some(asio::buffer(chunk), last_error_);
        if (last_error_) return false;
        buffer_.append(chunk, n);
        return true;
    }

    asio::io_context io_;
    asio::ip::tcp::socket socket_;
    std::string buffer_;
    asio::error_code last_error_;
};

struct HarnessOptions {
    int threads = 1;  // io threads running the io_context.
    Session::Limits limits;
    std::size_t max_connections = Server::kDefaultMaxConnections;
};

// Options with only the io thread count changed. (Designated initializers
// that skip members trip GCC's -Wmissing-field-initializers.)
inline HarnessOptions with_threads(int threads) {
    HarnessOptions options;
    options.threads = threads;
    return options;
}

// A Server and the io threads running its io_context.
class Harness {
public:
    Harness(Replicator& replicator, KvStore& store, HarnessOptions options = {})
        : service_(replicator, store),
          dispatcher_(service_),
          server_(io_, dispatcher_, options.limits, options.max_connections) {
        const std::error_code ec = server_.listen(asio::ip::address_v4::loopback(), 0);
        EXPECT_FALSE(ec) << ec.message();
        EXPECT_NE(server_.port(), 0);
        server_.start();
        for (int i = 0; i < options.threads; ++i) {
            threads_.emplace_back([this] {
                io_.run();
                std::lock_guard lock(mutex_);
                ++returned_;
                returned_cv_.notify_all();
            });
        }
    }

    ~Harness() {
        if (!joined_) stop_and_wait();
    }

    std::uint16_t port() const { return server_.port(); }
    Server& server() { return server_; }

    // Stops the server; returns whether every io thread's run() returned in
    // time.
    bool stop_and_wait() {
        server_.stop();
        bool all_returned = false;
        {
            std::unique_lock lock(mutex_);
            all_returned = returned_cv_.wait_for(
                lock, kTimeout, [this] { return returned_ == threads_.size(); });
        }
        if (!all_returned) io_.stop();  // Unblock the threads so the test can fail cleanly.
        for (auto& thread : threads_) thread.join();
        joined_ = true;
        return all_returned;
    }

private:
    KvService service_;
    CommandDispatcher dispatcher_;
    asio::io_context io_;
    Server server_;
    std::vector<std::thread> threads_;
    std::mutex mutex_;
    std::condition_variable returned_cv_;
    std::size_t returned_{0};  // Guarded by mutex_.
    bool joined_{false};
};

}  // namespace distkv::test

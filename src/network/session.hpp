#pragma once

#include <asio.hpp>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "server/command_dispatcher.hpp"

namespace distkv {

// One client connection speaking RESP2.
//
// Reads into a buffer, parses every complete frame, dispatches each one and
// writes replies back in request order. All session state is touched only on
// the socket's executor, which is a strand: reply callbacks (which may fire on
// a Replicator thread) are posted back to it rather than touching state.
//
// Ordering: every request gets a response slot in a FIFO when it is
// dispatched. A completed reply fills its slot; filled slots are flushed
// strictly from the front, so a later request that completes first waits for
// the earlier ones.
//
// Writes: at most one async_write is in flight. Replies that complete while it
// runs accumulate and go out together in the next write.
//
// Backpressure: when too many replies are pending or too many output bytes are
// queued, the session stops parsing and reading until the backlog drains, so a
// client that pipelines without reading cannot grow server memory unbounded.
//
// Owned by shared_ptr: every outstanding async operation and reply callback
// holds a reference, so the session lives until the last one finishes.
//
// Thread safety: every member below is touched only on executor_, a strand,
// so the io_context may run on any number of threads. start() and close()
// only post to that strand and may be called from any thread. executor_,
// dispatcher_ and limits_ are immutable after construction, which is what
// lets reply callbacks on other threads read executor_ to post back.
class Session : public std::enable_shared_from_this<Session> {
public:
    struct Limits {
        std::size_t max_pending_replies = 1024;          // Dispatched but not yet written.
        std::size_t max_output_bytes = 4 * 1024 * 1024;  // Queued plus in-flight reply bytes.
        std::size_t read_chunk_bytes = 16 * 1024;
    };

    // `socket` should use a strand executor (e.g. accepted with
    // asio::make_strand). `dispatcher` must outlive the session.
    Session(asio::ip::tcp::socket socket, CommandDispatcher& dispatcher, Limits limits);
    Session(asio::ip::tcp::socket socket, CommandDispatcher& dispatcher)
        : Session(std::move(socket), dispatcher, Limits{}) {}

    Session(const Session&) = delete;
    Session& operator=(const Session&) = delete;

    // Starts reading. Thread-safe.
    void start();

    // Closes the connection immediately, dropping unsent replies. Thread-safe
    // and idempotent.
    void close();

private:
    void do_read();
    void on_read(const asio::error_code& ec, std::size_t bytes);
    void process_input();
    void on_reply(std::uint64_t seq, std::string reply);
    void flush();
    void on_write(const asio::error_code& ec);
    void resume_if_possible();
    void do_close();

    [[nodiscard]] bool over_limits() const;
    [[nodiscard]] bool finished() const;

    // Copied from the socket at construction and never changed, so it is
    // safe to read from any thread (unlike calling socket_.get_executor()).
    const asio::any_io_executor executor_;
    asio::ip::tcp::socket socket_;
    CommandDispatcher& dispatcher_;
    const Limits limits_;

    std::vector<char> read_buffer_;
    std::string input_;  // Received bytes not yet parsed.

    // Response slots, front = oldest outstanding request.
    std::deque<std::optional<std::string>> slots_;
    std::uint64_t front_seq_{0};  // Sequence number of slots_.front().
    std::uint64_t next_seq_{0};   // Sequence number for the next request.

    std::string output_;   // Flushed replies waiting for the next write.
    std::string writing_;  // Bytes owned by the in-flight write.

    bool reading_{false};  // An async_read_some is in flight.
    bool write_in_flight_{false};
    bool input_blocked_{false};  // Backpressure paused parsing with bytes left in input_.
    bool eof_{false};            // Peer finished sending; drain, reply, then close.
    bool stop_parsing_{false};   // Protocol error: reply with the error, then close.
    bool closed_{false};
};

}  // namespace distkv

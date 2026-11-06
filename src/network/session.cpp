#include "network/session.hpp"

#include <string_view>
#include <utility>
#include <variant>

#include "protocol/resp.hpp"

namespace distkv {

Session::Session(asio::ip::tcp::socket socket, CommandDispatcher& dispatcher, Limits limits)
    : executor_(socket.get_executor()),
      socket_(std::move(socket)),
      dispatcher_(dispatcher),
      limits_(limits),
      read_buffer_(limits.read_chunk_bytes) {}

void Session::start() {
    asio::dispatch(executor_, [self = shared_from_this()] { self->do_read(); });
}

void Session::close() {
    asio::post(executor_, [self = shared_from_this()] { self->do_close(); });
}

bool Session::over_limits() const {
    return slots_.size() >= limits_.max_pending_replies ||
           output_.size() + writing_.size() >= limits_.max_output_bytes;
}

// Nothing more will be read, parsed or written.
bool Session::finished() const {
    const bool no_more_requests = stop_parsing_ || (eof_ && !input_blocked_);
    return no_more_requests && slots_.empty() && output_.empty() && !write_in_flight_;
}

void Session::do_read() {
    if (closed_ || reading_ || eof_ || stop_parsing_ || input_blocked_ || over_limits()) return;
    reading_ = true;
    socket_.async_read_some(asio::buffer(read_buffer_),
                            [self = shared_from_this()](const asio::error_code& ec,
                                                        std::size_t bytes) {
                                self->on_read(ec, bytes);
                            });
}

void Session::on_read(const asio::error_code& ec, std::size_t bytes) {
    reading_ = false;
    if (closed_) return;
    if (ec == asio::error::eof) {
        // The peer may half-close after sending (e.g. `printf ... | nc`):
        // still answer everything it sent, then close.
        eof_ = true;
        process_input();
        if (finished()) do_close();
        return;
    }
    if (ec) {
        do_close();
        return;
    }
    input_.append(read_buffer_.data(), bytes);
    process_input();
    do_read();
}

void Session::process_input() {
    std::size_t offset = 0;
    input_blocked_ = false;
    while (!stop_parsing_) {
        if (over_limits()) {
            // Leave the rest buffered; resume_if_possible() picks it up.
            input_blocked_ = offset < input_.size();
            break;
        }
        resp::ParseResult result = resp::parse_request(std::string_view(input_).substr(offset));
        if (std::holds_alternative<resp::Incomplete>(result)) break;

        if (auto* error = std::get_if<resp::ProtocolError>(&result)) {
            // Answer in order after earlier requests, then close.
            stop_parsing_ = true;
            slots_.emplace_back(resp::error("ERR Protocol error: " + error->message));
            ++next_seq_;
            offset = input_.size();
            break;
        }

        auto& frame = std::get<resp::Complete>(result);
        offset += frame.consumed;
        if (frame.args.empty()) continue;  // Blank line or "*0": nothing to answer.

        const std::uint64_t seq = next_seq_++;
        slots_.emplace_back(std::nullopt);
        // The callback may run on another thread and after this function
        // returns; it only posts back to the session's strand.
        dispatcher_.dispatch(std::move(frame.args),
                             [self = shared_from_this(), seq](std::string reply) {
                                 asio::post(self->executor_,
                                            [self, seq, reply = std::move(reply)]() mutable {
                                                self->on_reply(seq, std::move(reply));
                                            });
                             });
    }
    input_.erase(0, offset);
    flush();
}

void Session::on_reply(std::uint64_t seq, std::string reply) {
    if (closed_) return;
    slots_[static_cast<std::size_t>(seq - front_seq_)] = std::move(reply);
    flush();
    resume_if_possible();
}

void Session::flush() {
    if (closed_) return;
    while (!slots_.empty() && slots_.front().has_value()) {
        output_ += *slots_.front();
        slots_.pop_front();
        ++front_seq_;
    }
    if (!write_in_flight_ && !output_.empty()) {
        writing_.swap(output_);
        write_in_flight_ = true;
        asio::async_write(socket_, asio::buffer(writing_),
                          [self = shared_from_this()](const asio::error_code& ec, std::size_t) {
                              self->on_write(ec);
                          });
        return;
    }
    if (finished()) do_close();
}

void Session::on_write(const asio::error_code& ec) {
    write_in_flight_ = false;
    writing_.clear();
    if (closed_) return;
    if (ec) {
        do_close();
        return;
    }
    flush();
    resume_if_possible();
}

// Called when the backlog may have shrunk: continue parsing buffered frames
// and reading from the socket if the limits now allow it.
void Session::resume_if_possible() {
    if (closed_ || over_limits()) return;
    if (input_blocked_) {
        process_input();
        if (closed_) return;
    }
    do_read();
}

void Session::do_close() {
    if (closed_) return;
    closed_ = true;
    asio::error_code ignored;
    socket_.shutdown(asio::ip::tcp::socket::shutdown_both, ignored);
    socket_.close(ignored);
    slots_.clear();
    output_.clear();
    input_.clear();
}

}  // namespace distkv

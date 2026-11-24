#include "raft/asio_transport.h"

#include <algorithm>
#include <array>
#include <deque>
#include <set>

namespace distkv::raft {

namespace asio = boost::asio;
using asio::ip::tcp;
using boost::system::error_code;

bool ParsePeerList(std::string_view text, std::vector<PeerAddress>* out, std::string* error) {
    out->clear();
    std::set<NodeId> seen;
    bool more = true;  // every comma promises another entry, so "a," is an error
    while (more) {
        const size_t comma = text.find(',');
        const std::string_view item = text.substr(0, comma);
        more = comma != std::string_view::npos;
        text = more ? text.substr(comma + 1) : std::string_view();

        const size_t at = item.find('@');
        const size_t colon = item.rfind(':');
        if (at == std::string_view::npos || colon == std::string_view::npos || colon < at ||
            at == 0 || colon == at + 1 || colon + 1 == item.size()) {
            *error = "expected id@host:port, got '" + std::string(item) + "'";
            return false;
        }
        auto parse_number = [](std::string_view digits, uint64_t max, uint64_t* value) {
            if (digits.empty() || digits.size() > 10) return false;
            uint64_t n = 0;
            for (const char c : digits) {
                if (c < '0' || c > '9') return false;
                n = n * 10 + static_cast<uint64_t>(c - '0');
            }
            if (n > max) return false;
            *value = n;
            return true;
        };
        uint64_t id = 0;
        uint64_t port = 0;
        if (!parse_number(item.substr(0, at), UINT32_MAX, &id) || id == 0) {
            *error = "bad node id in '" + std::string(item) + "'";
            return false;
        }
        if (!parse_number(item.substr(colon + 1), 65535, &port) || port == 0) {
            *error = "bad port in '" + std::string(item) + "'";
            return false;
        }
        if (!seen.insert(static_cast<NodeId>(id)).second) {
            *error = "node id " + std::to_string(id) + " listed twice";
            return false;
        }
        out->push_back(PeerAddress{static_cast<NodeId>(id),
                                   std::string(item.substr(at + 1, colon - at - 1)),
                                   static_cast<uint16_t>(port)});
    }
    return true;
}

// Outgoing connection to one peer. Everything runs on the io thread.
class AsioTransport::Peer : public std::enable_shared_from_this<Peer> {
  public:
    Peer(AsioTransport* transport, PeerAddress address)
        : t_(transport),
          address_(std::move(address)),
          socket_(transport->io_),
          resolver_(transport->io_) {}

    void Enqueue(std::string frame) {
        if (state_ == State::kDisconnected) {
            if (std::chrono::steady_clock::now() < retry_after_) {
                ++t_->dropped_;
                return;
            }
            Connect();
        }
        if (queue_.size() >= t_->options_.max_queued_frames) {
            ++t_->dropped_;
            return;
        }
        queue_.push_back(std::move(frame));
        if (state_ == State::kConnected && !writing_) {
            WriteNext();
        }
    }

    void Close() {
        error_code ignored;
        resolver_.cancel();
        socket_.close(ignored);
    }

  private:
    enum class State { kDisconnected, kConnecting, kConnected };

    void Connect() {
        state_ = State::kConnecting;
        const uint64_t gen = ++generation_;
        resolver_.async_resolve(
            address_.host, std::to_string(address_.port),
            [self = shared_from_this(), gen](error_code ec, tcp::resolver::results_type results) {
                if (gen != self->generation_) return;
                if (ec) {
                    self->Fail(gen);
                    return;
                }
                asio::async_connect(self->socket_, results,
                                    [self, gen](error_code cec, const tcp::endpoint&) {
                                        self->OnConnect(cec, gen);
                                    });
            });
    }

    void OnConnect(error_code ec, uint64_t gen) {
        if (gen != generation_) return;
        if (ec) {
            Fail(gen);
            return;
        }
        state_ = State::kConnected;
        error_code ignored;
        socket_.set_option(tcp::no_delay(true), ignored);
        WatchForClose(gen);
        if (!writing_) WriteNext();
    }

    // The peer never writes on this connection, so any read completing means
    // it closed or reset: notice that now rather than on the next write.
    void WatchForClose(uint64_t gen) {
        socket_.async_read_some(asio::buffer(probe_),
                                [self = shared_from_this(), gen](error_code, size_t) {
                                    self->Fail(gen);
                                });
    }

    void WriteNext() {
        if (queue_.empty()) {
            writing_ = false;
            return;
        }
        writing_ = true;
        const uint64_t gen = generation_;
        asio::async_write(socket_, asio::buffer(queue_.front()),
                          [self = shared_from_this(), gen](error_code ec, size_t) {
                              if (gen != self->generation_) return;
                              if (ec) {
                                  self->Fail(gen);
                                  return;
                              }
                              ++self->t_->sent_;
                              self->queue_.pop_front();
                              self->WriteNext();
                          });
    }

    // Drops the connection and everything queued on it. Stale handlers from
    // an earlier connection (older generation) are ignored.
    void Fail(uint64_t gen) {
        if (gen != generation_ || state_ == State::kDisconnected) return;
        ++generation_;
        if (state_ == State::kConnecting) ++t_->connect_failures_;
        t_->dropped_ += queue_.size();
        queue_.clear();
        writing_ = false;
        error_code ignored;
        socket_.close(ignored);
        state_ = State::kDisconnected;
        retry_after_ = std::chrono::steady_clock::now() + t_->options_.reconnect_backoff;
    }

    AsioTransport* t_;
    const PeerAddress address_;
    tcp::socket socket_;
    tcp::resolver resolver_;
    State state_ = State::kDisconnected;
    uint64_t generation_ = 0;
    std::deque<std::string> queue_;
    bool writing_ = false;
    std::chrono::steady_clock::time_point retry_after_{};
    std::array<char, 1> probe_{};
};

// Incoming connection: decodes frames and hands them to the handler.
class AsioTransport::Inbound : public std::enable_shared_from_this<Inbound> {
  public:
    Inbound(AsioTransport* transport, tcp::socket socket)
        : t_(transport), socket_(std::move(socket)) {}

    void Start() { Read(); }

    void Close() {
        error_code ignored;
        socket_.close(ignored);
    }

  private:
    void Read() {
        socket_.async_read_some(asio::buffer(chunk_),
                                [self = shared_from_this()](error_code ec, size_t n) {
                                    self->OnRead(ec, n);
                                });
    }

    void OnRead(error_code ec, size_t n) {
        if (ec) {
            Close();
            return;
        }
        buffer_.append(chunk_.data(), n);
        size_t offset = 0;
        while (true) {
            RpcMessage message;
            size_t consumed = 0;
            const FrameResult r = DecodeRpcFrame(std::string_view(buffer_).substr(offset),
                                                 &message, &consumed);
            if (r == FrameResult::kIncomplete) break;
            if (r == FrameResult::kCorrupt) {
                // The stream can no longer be framed; the sender reconnects.
                ++t_->corrupt_;
                Close();
                return;
            }
            offset += consumed;
            ++t_->received_;
            t_->handler_(message);
        }
        buffer_.erase(0, offset);
        Read();
    }

    AsioTransport* t_;
    tcp::socket socket_;
    std::array<char, 64 * 1024> chunk_{};
    std::string buffer_;
};

AsioTransport::AsioTransport(TransportOptions options, Handler handler)
    : options_(std::move(options)),
      handler_(std::move(handler)),
      work_(asio::make_work_guard(io_)),
      acceptor_(io_) {
    for (const PeerAddress& p : options_.peers) {
        peers_.emplace(p.id, std::make_shared<Peer>(this, p));
    }
}

AsioTransport::~AsioTransport() { Stop(); }

Status AsioTransport::Start() {
    std::lock_guard<std::mutex> lock(lifecycle_mu_);
    if (started_ || stopped_) {
        return Status::InvalidArgument("transport already started");
    }
    error_code ec;
    const auto address = asio::ip::make_address(options_.listen_host, ec);
    if (ec) {
        return Status::InvalidArgument("bad raft listen address '" + options_.listen_host + "'");
    }
    const tcp::endpoint endpoint(address, options_.listen_port);
    acceptor_.open(endpoint.protocol(), ec);
    if (!ec) acceptor_.set_option(tcp::acceptor::reuse_address(true), ec);
    if (!ec) acceptor_.bind(endpoint, ec);
    if (!ec) acceptor_.listen(asio::socket_base::max_listen_connections, ec);
    if (ec) {
        error_code ignored;
        acceptor_.close(ignored);
        return Status::IOError("raft listen on " + options_.listen_host + ":" +
                               std::to_string(options_.listen_port) + ": " + ec.message());
    }
    bound_port_ = acceptor_.local_endpoint().port();
    DoAccept();
    started_ = true;
    thread_ = std::thread([this] { io_.run(); });
    return Status::Ok();
}

void AsioTransport::Stop() {
    std::lock_guard<std::mutex> lock(lifecycle_mu_);
    if (stopped_) return;
    stopped_ = true;
    work_.reset();
    io_.stop();
    if (thread_.joinable()) thread_.join();

    // The io thread is gone, so the sockets can be closed from here. Peers
    // see EOF straight away instead of when this object is destroyed.
    error_code ignored;
    acceptor_.close(ignored);
    for (auto& [id, peer] : peers_) peer->Close();
    for (auto& weak : inbound_) {
        if (auto in = weak.lock()) in->Close();
    }
}

void AsioTransport::Send(NodeId to, const RpcMessage& message) {
    const auto it = peers_.find(to);
    if (it == peers_.end()) {
        ++dropped_;
        return;
    }
    asio::post(io_, [peer = it->second, frame = EncodeRpcFrame(message)]() mutable {
        peer->Enqueue(std::move(frame));
    });
}

void AsioTransport::DoAccept() {
    acceptor_.async_accept([this](error_code ec, tcp::socket socket) {
        if (ec) {
            if (ec == asio::error::operation_aborted) return;
            DoAccept();
            return;
        }
        auto in = std::make_shared<Inbound>(this, std::move(socket));
        // Forget connections that have already finished.
        inbound_.erase(std::remove_if(inbound_.begin(), inbound_.end(),
                                      [](const auto& w) { return w.expired(); }),
                       inbound_.end());
        inbound_.push_back(in);
        in->Start();
        DoAccept();
    });
}

TransportStats AsioTransport::stats() const {
    TransportStats s;
    s.frames_sent = sent_.load();
    s.frames_received = received_.load();
    s.frames_dropped = dropped_.load();
    s.connect_failures = connect_failures_.load();
    s.corrupt_connections = corrupt_.load();
    return s;
}

}  // namespace distkv::raft

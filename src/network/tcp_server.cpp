#include "network/tcp_server.h"

namespace distkv {

namespace asio = boost::asio;
using asio::ip::tcp;
using boost::system::error_code;

// One client connection. Every handler runs on the socket's strand.
class TcpServer::Session : public std::enable_shared_from_this<Session> {
  public:
    Session(TcpServer* server, tcp::socket socket, bool counted)
        : server_(server), socket_(std::move(socket)), buffer_(kMaxLineBytes), counted_(counted) {}

    ~Session() {
        if (counted_) {
            server_->Unregister(this);
        }
    }

    void Start() { DoRead(); }

    // Sends one error line and closes, for connections over the limit.
    void Reject(std::string_view reason) { WriteThenClose(ErrorResponse(reason)); }

    // Thread-safe: hops onto the strand before touching the socket.
    void Close() {
        asio::post(socket_.get_executor(), [self = shared_from_this()] { self->CloseNow(); });
    }

  private:
    void DoRead() {
        asio::async_read_until(socket_, buffer_, '\n',
                               [self = shared_from_this()](error_code ec, size_t n) {
                                   self->OnRead(ec, n);
                               });
    }

    void OnRead(error_code ec, size_t n) {
        if (ec == asio::error::not_found) {
            // The buffer filled up without a newline: the line is too long and
            // the stream can no longer be framed, so answer and hang up.
            WriteThenClose(ErrorResponse("request line too long"));
            return;
        }
        if (ec) {
            CloseNow();  // EOF, reset, or closed by Stop()
            return;
        }

        const auto begin = asio::buffers_begin(buffer_.data());
        std::string line(begin, begin + static_cast<std::ptrdiff_t>(n - 1));  // drop '\n'
        buffer_.consume(n);

        response_ = server_->handler_->HandleLine(line);
        asio::async_write(socket_, asio::buffer(response_),
                          [self = shared_from_this()](error_code write_ec, size_t) {
                              if (write_ec) {
                                  self->CloseNow();
                              } else {
                                  self->DoRead();
                              }
                          });
    }

    void WriteThenClose(std::string message) {
        response_ = std::move(message);
        asio::async_write(socket_, asio::buffer(response_),
                          [self = shared_from_this()](error_code, size_t) { self->CloseNow(); });
    }

    void CloseNow() {
        error_code ignored;
        socket_.shutdown(tcp::socket::shutdown_both, ignored);
        socket_.close(ignored);
    }

    TcpServer* server_;
    tcp::socket socket_;
    asio::streambuf buffer_;  // capped at kMaxLineBytes
    std::string response_;    // must outlive the async_write using it
    const bool counted_;
};

TcpServer::TcpServer(ServerOptions options, CommandHandler* handler)
    : options_(std::move(options)), handler_(handler), acceptor_(asio::make_strand(io_)) {}

TcpServer::~TcpServer() { Stop(); }

Status TcpServer::Start() {
    if (started_) {
        return Status::InvalidArgument("server already started");
    }
    if (options_.threads == 0) {
        return Status::InvalidArgument("threads must be positive");
    }

    error_code ec;
    const auto address = asio::ip::make_address(options_.host, ec);
    if (ec) {
        return Status::InvalidArgument("bad host '" + options_.host + "': " + ec.message());
    }
    const tcp::endpoint endpoint(address, options_.port);
    acceptor_.open(endpoint.protocol(), ec);
    if (!ec) acceptor_.set_option(tcp::acceptor::reuse_address(true), ec);
    if (!ec) acceptor_.bind(endpoint, ec);
    if (!ec) acceptor_.listen(asio::socket_base::max_listen_connections, ec);
    if (ec) {
        return Status::IOError("listen on " + options_.host + ":" +
                               std::to_string(options_.port) + ": " + ec.message());
    }
    bound_port_ = acceptor_.local_endpoint().port();
    started_ = true;

    DoAccept();
    workers_.reserve(options_.threads);
    for (size_t i = 0; i < options_.threads; ++i) {
        // run() returns once Stop() has closed the acceptor and every session
        // and no handlers are left.
        workers_.emplace_back([this] { io_.run(); });
    }
    return Status::Ok();
}

void TcpServer::DoAccept() {
    // Each connection gets its own strand.
    acceptor_.async_accept(asio::make_strand(io_), [this](error_code ec, tcp::socket socket) {
        if (ec == asio::error::operation_aborted || !acceptor_.is_open()) {
            return;  // Stop() closed the acceptor
        }
        if (!ec) {
            const bool over_limit = active_.load() >= options_.max_connections;
            auto session = std::make_shared<Session>(this, std::move(socket), !over_limit);
            if (over_limit) {
                rejected_.fetch_add(1);
                session->Reject("too many connections");
            } else {
                Register(session);
            }
        }
        // Accept errors such as running out of file descriptors are transient;
        // keep listening.
        DoAccept();
    });
}

void TcpServer::Register(const std::shared_ptr<Session>& session) {
    {
        std::lock_guard<std::mutex> lock(sessions_mu_);
        if (stopped_) {
            // Stop() already took its snapshot; this session would be missed.
            session->Close();
            return;
        }
        sessions_[session.get()] = session;
    }
    active_.fetch_add(1);
    accepted_.fetch_add(1);
    session->Start();
}

void TcpServer::Unregister(Session* session) {
    std::lock_guard<std::mutex> lock(sessions_mu_);
    if (sessions_.erase(session) > 0) {
        active_.fetch_sub(1);
    }
}

void TcpServer::Stop() {
    std::vector<std::shared_ptr<Session>> to_close;
    {
        std::lock_guard<std::mutex> lock(sessions_mu_);
        if (stopped_ || !started_) {
            stopped_ = true;
            return;
        }
        stopped_ = true;
        for (auto& [ptr, weak] : sessions_) {
            if (auto s = weak.lock()) {  // null if already being destroyed
                to_close.push_back(std::move(s));
            }
        }
    }

    asio::post(acceptor_.get_executor(), [this] {
        error_code ignored;
        acceptor_.close(ignored);
    });
    for (auto& s : to_close) {
        s->Close();
    }
    to_close.clear();

    for (auto& t : workers_) {
        if (t.joinable()) {
            t.join();
        }
    }
    workers_.clear();
}

}  // namespace distkv

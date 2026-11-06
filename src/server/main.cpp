// distkv-server: Day 1 skeleton. Parses flags, listens on a TCP port, greets
// each connection with a banner and closes it, and shuts down gracefully on
// SIGINT/SIGTERM. The request protocol arrives in later days.

#include <asio.hpp>
#include <charconv>
#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <exception>
#include <iostream>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <variant>

#include "replication/local_replicator.hpp"
#include "storage/state_machine.hpp"

namespace {

constexpr std::string_view kProgram = "distkv-server";
constexpr std::string_view kVersion = DISTKV_VERSION;

struct Options {
    std::uint32_t id = 1;
    std::uint16_t port = 7000;
};

void print_usage(std::ostream& out) {
    out << "Usage: " << kProgram << " [--id N] [--port P]\n"
        << "\n"
        << "Options:\n"
        << "  --id N       Node id (positive integer, default 1)\n"
        << "  --port P     TCP port to listen on (1-65535, default 7000)\n"
        << "  --version    Print version and exit\n"
        << "  --help       Print this help and exit\n";
}

// Parses the whole of `text` as an unsigned integer in [min, max].
template <typename T>
std::optional<T> parse_uint(std::string_view text, T min, T max) {
    unsigned long long value = 0;
    const char* first = text.data();
    const char* last = text.data() + text.size();
    const auto [ptr, ec] = std::from_chars(first, last, value);
    if (ec != std::errc{} || ptr != last || value < min || value > max) {
        return std::nullopt;
    }
    return static_cast<T>(value);
}

// Returns the parsed options, or an exit code if the process should stop.
std::variant<Options, int> parse_args(int argc, char** argv) {
    Options options;
    for (int i = 1; i < argc; ++i) {
        const std::string_view arg = argv[i];
        if (arg == "--help" || arg == "-h") {
            print_usage(std::cout);
            return EXIT_SUCCESS;
        }
        if (arg == "--version") {
            std::cout << kProgram << ' ' << kVersion << '\n';
            return EXIT_SUCCESS;
        }
        if (arg == "--id" || arg == "--port") {
            if (i + 1 >= argc) {
                std::cerr << kProgram << ": " << arg << " requires a value\n";
                return EXIT_FAILURE;
            }
            const std::string_view value = argv[++i];
            if (arg == "--id") {
                const auto id = parse_uint<std::uint32_t>(value, 1, UINT32_MAX);
                if (!id) {
                    std::cerr << kProgram << ": invalid --id '" << value
                              << "' (expected a positive integer)\n";
                    return EXIT_FAILURE;
                }
                options.id = *id;
            } else {
                const auto port = parse_uint<std::uint16_t>(value, 1, UINT16_MAX);
                if (!port) {
                    std::cerr << kProgram << ": invalid --port '" << value
                              << "' (expected an integer in 1-65535)\n";
                    return EXIT_FAILURE;
                }
                options.port = *port;
            }
            continue;
        }
        std::cerr << kProgram << ": unknown argument '" << arg << "'\n";
        print_usage(std::cerr);
        return EXIT_FAILURE;
    }
    return options;
}

// Placeholder until the key-value store lands; the replicator needs a target.
class NullStateMachine final : public distkv::StateMachine {
public:
    void apply(const distkv::LogEntry& /*entry*/) override {}
};

class Server {
public:
    Server(asio::io_context& io, const Options& options)
        : acceptor_(io),
          banner_(std::make_shared<const std::string>(std::string(kProgram) + ' ' +
                                                      std::string(kVersion) + " node " +
                                                      std::to_string(options.id) + "\r\n")) {}

    // Opens the listening socket. Returns an error instead of throwing so
    // main() can print a precise message.
    std::error_code listen(std::uint16_t port) {
        const asio::ip::tcp::endpoint endpoint(asio::ip::tcp::v4(), port);
        asio::error_code ec;
        acceptor_.open(endpoint.protocol(), ec);
        if (!ec) acceptor_.set_option(asio::socket_base::reuse_address(true), ec);
        if (!ec) acceptor_.bind(endpoint, ec);
        if (!ec) acceptor_.listen(asio::socket_base::max_listen_connections, ec);
        if (ec) {
            asio::error_code ignored;
            acceptor_.close(ignored);
        }
        return ec;
    }

    void start() { accept_next(); }

    // Stops accepting. Connections already in flight finish their banner,
    // after which io_context::run() returns because no work is left.
    void stop() {
        asio::error_code ignored;
        acceptor_.close(ignored);
    }

private:
    void accept_next() {
        acceptor_.async_accept([this](const asio::error_code& ec, asio::ip::tcp::socket socket) {
            if (ec == asio::error::operation_aborted) {
                return;  // Acceptor closed: shutting down.
            }
            if (ec) {
                std::cerr << kProgram << ": accept failed: " << ec.message() << '\n';
            } else {
                greet(std::move(socket));
            }
            if (acceptor_.is_open()) {
                accept_next();
            }
        });
    }

    void greet(asio::ip::tcp::socket socket) {
        auto conn = std::make_shared<asio::ip::tcp::socket>(std::move(socket));
        auto banner = banner_;
        asio::async_write(*conn, asio::buffer(*banner),
                          [conn, banner](const asio::error_code& /*ec*/, std::size_t /*n*/) {
                              asio::error_code ignored;
                              conn->shutdown(asio::ip::tcp::socket::shutdown_both, ignored);
                              conn->close(ignored);
                          });
    }

    asio::ip::tcp::acceptor acceptor_;
    std::shared_ptr<const std::string> banner_;
};

}  // namespace

int main(int argc, char** argv) {
    const auto parsed = parse_args(argc, argv);
    if (const int* exit_code = std::get_if<int>(&parsed)) {
        return *exit_code;
    }
    const Options options = std::get<Options>(parsed);

    NullStateMachine state_machine;
    distkv::LocalReplicator replicator(state_machine);

    try {
        asio::io_context io;
        Server server(io, options);

        if (const std::error_code ec = server.listen(options.port)) {
            if (ec == asio::error::address_in_use) {
                std::cerr << kProgram << ": port " << options.port
                          << " is already in use (is another distkv-server running?)\n";
            } else {
                std::cerr << kProgram << ": cannot listen on port " << options.port << ": "
                          << ec.message() << '\n';
            }
            return EXIT_FAILURE;
        }

        asio::signal_set signals(io, SIGINT, SIGTERM);
        signals.async_wait([&server](const asio::error_code& ec, int signo) {
            if (ec) return;
            std::cout << kProgram << ": received signal " << signo << ", shutting down\n"
                      << std::flush;
            server.stop();
        });

        server.start();
        std::cout << kProgram << ' ' << kVersion << ": node " << options.id
                  << " listening on port " << options.port << " (term "
                  << replicator.current_term() << ", leader="
                  << (replicator.is_leader() ? "yes" : "no") << ")\n"
                  << std::flush;

        io.run();
    } catch (const std::exception& e) {
        std::cerr << kProgram << ": fatal: " << e.what() << '\n';
        return EXIT_FAILURE;
    }

    std::cout << kProgram << ": stopped\n";
    return EXIT_SUCCESS;
}

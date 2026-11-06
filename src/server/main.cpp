// distkv-server: parses flags, builds the single-node stack
//   KvStore -> LocalReplicator -> KvService -> CommandDispatcher -> Server
// and serves RESP2 on a TCP port until SIGINT/SIGTERM, then shuts down
// gracefully: stop accepting, close sessions, let io_context drain.

#include <asio.hpp>
#include <charconv>
#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <exception>
#include <iostream>
#include <optional>
#include <string_view>
#include <system_error>
#include <variant>

#include "network/server.hpp"
#include "replication/local_replicator.hpp"
#include "server/command_dispatcher.hpp"
#include "server/kv_service.hpp"
#include "storage/kv_store.hpp"

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

}  // namespace

int main(int argc, char** argv) {
    const auto parsed = parse_args(argc, argv);
    if (const int* exit_code = std::get_if<int>(&parsed)) {
        return *exit_code;
    }
    const Options options = std::get<Options>(parsed);

    distkv::KvStore store;
    distkv::LocalReplicator replicator(store);
    distkv::KvService service(replicator, store);
    distkv::CommandDispatcher dispatcher(service);

    try {
        asio::io_context io;
        distkv::Server server(io, dispatcher);

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

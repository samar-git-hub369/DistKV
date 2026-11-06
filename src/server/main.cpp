// distkv-server: parses flags, builds the single-node stack
//   KvStore -> LocalReplicator -> KvService -> CommandDispatcher -> Server
// and serves RESP2 on a TCP port from a pool of io threads until
// SIGINT/SIGTERM, then shuts down gracefully: stop accepting, close sessions,
// let io_context drain, join every thread.

#include <asio.hpp>
#include <charconv>
#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <atomic>
#include <exception>
#include <iostream>
#include <optional>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <variant>
#include <vector>

#include "network/server.hpp"
#include "replication/local_replicator.hpp"
#include "server/command_dispatcher.hpp"
#include "server/kv_service.hpp"
#include "storage/kv_store.hpp"

namespace {

constexpr std::string_view kProgram = "distkv-server";
constexpr std::string_view kVersion = DISTKV_VERSION;

constexpr unsigned kMaxThreads = 256;

// One io thread per hardware thread, within [1, kMaxThreads].
unsigned default_threads() {
    const unsigned hardware = std::thread::hardware_concurrency();
    if (hardware == 0) return 1;  // Unknown.
    return hardware < kMaxThreads ? hardware : kMaxThreads;
}

struct Options {
    std::uint32_t id = 1;
    std::uint16_t port = 7000;
    unsigned threads = default_threads();
    std::uint32_t max_connections = distkv::Server::kDefaultMaxConnections;
};

void print_usage(std::ostream& out) {
    out << "Usage: " << kProgram
        << " [--id N] [--port P] [--threads N] [--max-connections N]\n"
        << "\n"
        << "Options:\n"
        << "  --id N                Node id (positive integer, default 1)\n"
        << "  --port P              TCP port to listen on (1-65535, default 7000)\n"
        << "  --threads N           io threads (1-" << kMaxThreads
        << ", default: hardware threads, here " << default_threads() << ")\n"
        << "  --max-connections N   Maximum open client connections (default "
        << distkv::Server::kDefaultMaxConnections << ")\n"
        << "  --version             Print version and exit\n"
        << "  --help                Print this help and exit\n";
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
        if (arg == "--id" || arg == "--port" || arg == "--threads" ||
            arg == "--max-connections") {
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
            } else if (arg == "--threads") {
                const auto threads = parse_uint<unsigned>(value, 1, kMaxThreads);
                if (!threads) {
                    std::cerr << kProgram << ": invalid --threads '" << value
                              << "' (expected an integer in 1-" << kMaxThreads << ")\n";
                    return EXIT_FAILURE;
                }
                options.threads = *threads;
            } else if (arg == "--max-connections") {
                const auto max = parse_uint<std::uint32_t>(value, 1, UINT32_MAX);
                if (!max) {
                    std::cerr << kProgram << ": invalid --max-connections '" << value
                              << "' (expected a positive integer)\n";
                    return EXIT_FAILURE;
                }
                options.max_connections = *max;
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

// Writes one line to stderr in a single call, so lines from different
// threads do not interleave.
void log_error(const std::string& line) { std::cerr << line + '\n' << std::flush; }

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

    bool failed = false;
    try {
        asio::io_context io;
        distkv::Server server(io, dispatcher, distkv::Session::Limits{},
                              options.max_connections);

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

        // On a strand so shutdown() can cancel it from any thread.
        const auto signal_strand = asio::make_strand(io);
        asio::signal_set signals(signal_strand, SIGINT, SIGTERM);
        signals.async_wait([&server](const asio::error_code& ec, int signo) {
            if (ec) return;
            std::cout << kProgram << ": received signal " << signo << ", shutting down\n"
                      << std::flush;
            server.stop();
        });

        // Removes every source of work so each io thread's run() returns.
        // Safe from any thread and more than once.
        const auto shutdown = [&] {
            server.stop();
            asio::post(signal_strand, [&signals] { signals.cancel(); });
        };

        // An exception escaping a handler unwinds out of run() on that thread
        // only. Log it, start a graceful shutdown, and go back into run() to
        // help drain it rather than letting std::terminate kill the process.
        std::atomic<bool> handler_failed{false};
        const auto run_io = [&](unsigned index) {
            while (true) {
                try {
                    io.run();
                    return;
                } catch (const std::exception& e) {
                    log_error(std::string(kProgram) + ": fatal: exception in io thread " +
                              std::to_string(index) + ": " + e.what());
                } catch (...) {
                    log_error(std::string(kProgram) + ": fatal: unknown exception in io thread " +
                              std::to_string(index));
                }
                handler_failed.store(true);
                shutdown();
            }
        };

        server.start();
        std::cout << kProgram << ' ' << kVersion << ": node " << options.id
                  << " listening on port " << options.port << " (term "
                  << replicator.current_term() << ", leader="
                  << (replicator.is_leader() ? "yes" : "no") << ", threads=" << options.threads
                  << ", max-connections=" << options.max_connections << ")\n"
                  << std::flush;

        // The main thread is io thread 0.
        std::vector<std::thread> pool;
        pool.reserve(options.threads - 1);
        for (unsigned i = 1; i < options.threads; ++i) {
            try {
                pool.emplace_back(run_io, i);
            } catch (const std::system_error& e) {
                // Threads already running must still be joined, so shut down
                // instead of unwinding past them.
                log_error(std::string(kProgram) + ": fatal: cannot start io thread " +
                          std::to_string(i) + ": " + e.what());
                handler_failed.store(true);
                shutdown();
                break;
            }
        }
        run_io(0);
        for (auto& thread : pool) thread.join();
        failed = handler_failed.load();
    } catch (const std::exception& e) {
        std::cerr << kProgram << ": fatal: " << e.what() << '\n';
        return EXIT_FAILURE;
    }

    if (failed) {
        std::cerr << kProgram << ": stopped after a fatal error\n";
        return EXIT_FAILURE;
    }
    std::cout << kProgram << ": stopped\n";
    return EXIT_SUCCESS;
}

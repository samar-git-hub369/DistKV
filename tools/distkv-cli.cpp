// distkv-cli: a small synchronous RESP client, in the spirit of redis-cli.
//
//   distkv-cli [-h host] [-p port] CMD [args...]   send one command, print the reply
//   distkv-cli [-h host] [-p port]                 read commands from stdin (REPL)

#include <unistd.h>

#include <asio.hpp>
#include <charconv>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <exception>
#include <iostream>
#include <optional>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <variant>
#include <vector>

#include "protocol/resp.hpp"

namespace {

constexpr std::string_view kProgram = "distkv-cli";

struct Options {
    std::string host = "127.0.0.1";
    std::uint16_t port = 7000;
    std::vector<std::string> command;  // Empty: REPL.
};

void print_usage(std::ostream& out) {
    out << "Usage: " << kProgram << " [-h host] [-p port] [CMD [args...]]\n"
        << "\n"
        << "  -h host      Server host (default 127.0.0.1)\n"
        << "  -p port      Server port (default 7000)\n"
        << "  --help       Print this help and exit\n"
        << "  --version    Print version and exit\n"
        << "\n"
        << "With a command, sends it and prints the reply. Without one, reads\n"
        << "commands from stdin, one per line (\"quit\" or EOF to exit).\n";
}

std::optional<std::uint16_t> parse_port(std::string_view text) {
    unsigned value = 0;
    const auto [ptr, ec] = std::from_chars(text.data(), text.data() + text.size(), value);
    if (ec != std::errc{} || ptr != text.data() + text.size() || value < 1 || value > 65535) {
        return std::nullopt;
    }
    return static_cast<std::uint16_t>(value);
}

// Returns options, or an exit code if the process should stop.
std::variant<Options, int> parse_args(int argc, char** argv) {
    Options options;
    int i = 1;
    for (; i < argc; ++i) {
        const std::string_view arg = argv[i];
        if (arg == "--help") {
            print_usage(std::cout);
            return EXIT_SUCCESS;
        }
        if (arg == "--version") {
            std::cout << kProgram << ' ' << DISTKV_VERSION << '\n';
            return EXIT_SUCCESS;
        }
        if (arg == "-h" || arg == "-p") {
            if (i + 1 >= argc) {
                std::cerr << kProgram << ": " << arg << " requires a value\n";
                return EXIT_FAILURE;
            }
            const std::string_view value = argv[++i];
            if (arg == "-h") {
                options.host = value;
            } else if (const auto port = parse_port(value)) {
                options.port = *port;
            } else {
                std::cerr << kProgram << ": invalid port '" << value << "'\n";
                return EXIT_FAILURE;
            }
            continue;
        }
        if (arg == "--") {
            ++i;
            break;
        }
        if (arg.size() > 1 && arg.front() == '-') {
            std::cerr << kProgram << ": unknown option '" << arg << "'\n";
            print_usage(std::cerr);
            return EXIT_FAILURE;
        }
        break;  // First command word.
    }
    for (; i < argc; ++i) options.command.emplace_back(argv[i]);
    return options;
}

// --- Replies ------------------------------------------------------------------

struct Reply {
    char type{'+'};  // '+', '-', ':', '$', '*'
    bool null{false};
    std::string text;  // Simple string, error, bulk payload, or integer digits.
    std::vector<Reply> elements;
};

struct MalformedReply {};

// Parses one reply starting at `pos`. Returns the position after it, or
// nullopt if `buf` does not hold all of it yet. Throws MalformedReply.
std::optional<std::size_t> parse_reply(std::string_view buf, std::size_t pos, Reply& out) {
    if (pos >= buf.size()) return std::nullopt;
    const std::size_t eol = buf.find("\r\n", pos);
    if (eol == std::string_view::npos) return std::nullopt;
    out.type = buf[pos];
    const std::string_view header = buf.substr(pos + 1, eol - pos - 1);
    const std::size_t next = eol + 2;

    const auto parse_count = [&]() -> long long {
        long long n = 0;
        const auto [ptr, ec] = std::from_chars(header.data(), header.data() + header.size(), n);
        if (ec != std::errc{} || ptr != header.data() + header.size()) throw MalformedReply{};
        return n;
    };

    switch (out.type) {
        case '+':
        case '-':
        case ':':
            out.text = header;
            return next;
        case '$': {
            const long long len = parse_count();
            if (len < 0) {
                out.null = true;
                return next;
            }
            const auto size = static_cast<std::size_t>(len);
            if (buf.size() - next < size + 2) return std::nullopt;
            if (buf.substr(next + size, 2) != "\r\n") throw MalformedReply{};
            out.text = buf.substr(next, size);
            return next + size + 2;
        }
        case '*': {
            const long long count = parse_count();
            if (count < 0) {
                out.null = true;
                return next;
            }
            std::size_t cursor = next;
            for (long long i = 0; i < count; ++i) {
                Reply element;
                const auto end = parse_reply(buf, cursor, element);
                if (!end) return std::nullopt;
                out.elements.push_back(std::move(element));
                cursor = *end;
            }
            return cursor;
        }
        default:
            throw MalformedReply{};
    }
}

// Quotes a bulk string the way redis-cli does.
std::string quote(std::string_view s) {
    std::string out = "\"";
    for (const char c : s) {
        switch (c) {
            case '\\': out += "\\\\"; break;
            case '"': out += "\\\""; break;
            case '\n': out += "\\n"; break;
            case '\r': out += "\\r"; break;
            case '\t': out += "\\t"; break;
            case '\a': out += "\\a"; break;
            case '\b': out += "\\b"; break;
            default: {
                const auto byte = static_cast<unsigned char>(c);
                if (byte >= 0x20 && byte < 0x7f) {
                    out += c;
                } else {
                    char hex[5];
                    std::snprintf(hex, sizeof(hex), "\\x%02x", byte);
                    out += hex;
                }
            }
        }
    }
    out += '"';
    return out;
}

// Renders a reply like redis-cli. `indent` is the column nested array lines
// start at.
std::string render(const Reply& reply, std::size_t indent = 0) {
    switch (reply.type) {
        case '+':
            return reply.text;
        case '-':
            return "(error) " + reply.text;
        case ':':
            return "(integer) " + reply.text;
        case '$':
            return reply.null ? "(nil)" : quote(reply.text);
        case '*': {
            if (reply.null) return "(nil)";
            if (reply.elements.empty()) return "(empty array)";
            const std::size_t width = std::to_string(reply.elements.size()).size();
            std::string out;
            for (std::size_t i = 0; i < reply.elements.size(); ++i) {
                std::string label = std::to_string(i + 1);
                label.insert(0, width - label.size(), ' ');
                label += ") ";
                if (i > 0) out += '\n' + std::string(indent, ' ');
                out += label + render(reply.elements[i], indent + label.size());
            }
            return out;
        }
        default:
            return "(unknown reply)";
    }
}

// --- Connection ----------------------------------------------------------------

class Connection {
public:
    Connection(const std::string& host, std::uint16_t port) : socket_(io_) {
        asio::ip::tcp::resolver resolver(io_);
        asio::connect(socket_, resolver.resolve(host, std::to_string(port)));
    }

    // Sends one command and returns its reply. Throws asio::system_error if
    // the connection fails and MalformedReply on garbage.
    Reply call(const std::vector<std::string>& args) {
        asio::write(socket_, asio::buffer(distkv::resp::command(args)));
        while (true) {
            Reply reply;
            if (const auto end = parse_reply(buffer_, 0, reply)) {
                buffer_.erase(0, *end);
                return reply;
            }
            char chunk[16 * 1024];
            const std::size_t n = socket_.read_some(asio::buffer(chunk));
            buffer_.append(chunk, n);
        }
    }

private:
    asio::io_context io_;
    asio::ip::tcp::socket socket_;
    std::string buffer_;
};

// Splits a REPL line into arguments. Supports "double quotes" with \", \\,
// \n, \r, \t and \xHH escapes, and 'single quotes' taken literally.
std::optional<std::vector<std::string>> split_line(std::string_view line) {
    std::vector<std::string> args;
    std::size_t i = 0;
    const auto hex = [](char c) -> int {
        if (c >= '0' && c <= '9') return c - '0';
        if (c >= 'a' && c <= 'f') return c - 'a' + 10;
        if (c >= 'A' && c <= 'F') return c - 'A' + 10;
        return -1;
    };
    while (true) {
        while (i < line.size() && (line[i] == ' ' || line[i] == '\t')) ++i;
        if (i == line.size()) return args;
        std::string arg;
        if (line[i] == '"') {
            ++i;
            while (true) {
                if (i == line.size()) return std::nullopt;  // Unterminated.
                const char c = line[i++];
                if (c == '"') break;
                if (c != '\\' || i == line.size()) {
                    arg += c;
                    continue;
                }
                const char e = line[i++];
                if (e == 'n') {
                    arg += '\n';
                } else if (e == 'r') {
                    arg += '\r';
                } else if (e == 't') {
                    arg += '\t';
                } else if (e == 'x' && i + 1 < line.size() && hex(line[i]) >= 0 &&
                           hex(line[i + 1]) >= 0) {
                    arg += static_cast<char>(hex(line[i]) * 16 + hex(line[i + 1]));
                    i += 2;
                } else {
                    arg += e;
                }
            }
        } else if (line[i] == '\'') {
            const std::size_t end = line.find('\'', i + 1);
            if (end == std::string_view::npos) return std::nullopt;
            arg = line.substr(i + 1, end - i - 1);
            i = end + 1;
        } else {
            while (i < line.size() && line[i] != ' ' && line[i] != '\t') arg += line[i++];
            args.push_back(std::move(arg));
            continue;
        }
        if (i < line.size() && line[i] != ' ' && line[i] != '\t') return std::nullopt;
        args.push_back(std::move(arg));
    }
}

int run_repl(Connection& connection, const Options& options) {
    const bool interactive = ::isatty(STDIN_FILENO) != 0;
    const std::string prompt = options.host + ":" + std::to_string(options.port) + "> ";
    std::string line;
    while (true) {
        if (interactive) std::cout << prompt << std::flush;
        if (!std::getline(std::cin, line)) break;
        const auto args = split_line(line);
        if (!args) {
            std::cout << "Invalid argument(s)\n";
            continue;
        }
        if (args->empty()) continue;
        if (args->size() == 1 && ((*args)[0] == "quit" || (*args)[0] == "exit")) break;
        std::cout << render(connection.call(*args)) << '\n';
    }
    return EXIT_SUCCESS;
}

}  // namespace

int main(int argc, char** argv) {
    const auto parsed = parse_args(argc, argv);
    if (const int* exit_code = std::get_if<int>(&parsed)) return *exit_code;
    const Options& options = std::get<Options>(parsed);

    std::optional<Connection> connection;
    try {
        connection.emplace(options.host, options.port);
    } catch (const std::exception& e) {
        std::cerr << "Could not connect to DistKV at " << options.host << ':' << options.port
                  << ": " << e.what() << '\n';
        return EXIT_FAILURE;
    }

    try {
        if (options.command.empty()) return run_repl(*connection, options);
        std::cout << render(connection->call(options.command)) << '\n';
        return EXIT_SUCCESS;
    } catch (const MalformedReply&) {
        std::cerr << kProgram << ": malformed reply from server\n";
    } catch (const std::exception& e) {
        std::cerr << kProgram << ": connection error: " << e.what() << '\n';
    }
    return EXIT_FAILURE;
}

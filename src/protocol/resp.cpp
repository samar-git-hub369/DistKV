#include "protocol/resp.hpp"

#include <charconv>
#include <optional>
#include <system_error>
#include <utility>

namespace distkv::resp {
namespace {

constexpr std::string_view kCrlf = "\r\n";

ProtocolError make_error(std::string message) { return ProtocolError{std::move(message)}; }

// Result of reading one CRLF-terminated line starting at `pos`.
struct Line {
    std::string_view text;  // Without the terminator.
    std::size_t next{0};    // Position just past the terminator.
};
using LineResult = std::variant<Line, Incomplete, ProtocolError>;

// Reads a CRLF-terminated line of at most `max_len` bytes. `too_long` is the
// error reported when no terminator appears within that many bytes.
LineResult read_crlf_line(std::string_view buf, std::size_t pos, std::size_t max_len,
                          std::string_view too_long) {
    const std::string_view rest = buf.substr(pos);
    const std::size_t end = rest.find(kCrlf);
    if (end == std::string_view::npos) {
        // Allow for a trailing '\r' whose '\n' has not arrived yet.
        if (rest.size() > max_len + 1) return make_error(std::string(too_long));
        return Incomplete{};
    }
    if (end > max_len) return make_error(std::string(too_long));
    return Line{rest.substr(0, end), pos + end + kCrlf.size()};
}

// Parses a non-negative decimal length. nullopt for empty, signed,
// non-numeric or overflowing input.
std::optional<std::size_t> parse_length(std::string_view text) {
    if (text.empty() || text.front() < '0' || text.front() > '9') return std::nullopt;
    std::size_t value = 0;
    const auto [ptr, ec] = std::from_chars(text.data(), text.data() + text.size(), value);
    if (ec != std::errc{} || ptr != text.data() + text.size()) return std::nullopt;
    return value;
}

// "<prefix>: '<text>'" with the offending text truncated, for error messages.
std::string describe(std::string_view prefix, std::string_view text) {
    constexpr std::size_t kMaxShown = 32;
    std::string out(prefix);
    out += " '";
    out += text.substr(0, kMaxShown);
    out += '\'';
    return out;
}

ParseResult parse_inline(std::string_view buf) {
    const std::size_t nl = buf.find('\n');
    if (nl == std::string_view::npos) {
        if (buf.size() > kMaxInlineLength + 1) return make_error("too big inline request");
        return Incomplete{};
    }
    std::string_view line = buf.substr(0, nl);
    if (!line.empty() && line.back() == '\r') line.remove_suffix(1);
    if (line.size() > kMaxInlineLength) return make_error("too big inline request");

    Complete result;
    result.consumed = nl + 1;
    std::size_t i = 0;
    while (i < line.size()) {
        while (i < line.size() && (line[i] == ' ' || line[i] == '\t')) ++i;
        const std::size_t start = i;
        while (i < line.size() && line[i] != ' ' && line[i] != '\t') ++i;
        if (i > start) {
            if (result.args.size() == kMaxArrayElements) {
                return make_error("too many arguments in inline request");
            }
            result.args.emplace_back(line.substr(start, i - start));
        }
    }
    return result;
}

ParseResult parse_array(std::string_view buf) {
    // buf[0] == '*'.
    LineResult header = read_crlf_line(buf, 1, kMaxLengthDigits, "invalid multibulk length");
    if (auto* r = std::get_if<Incomplete>(&header)) return *r;
    if (auto* r = std::get_if<ProtocolError>(&header)) return std::move(*r);
    const Line count_line = std::get<Line>(header);

    const std::optional<std::size_t> count = parse_length(count_line.text);
    if (!count) return make_error(describe("invalid multibulk length", count_line.text));
    if (*count > kMaxArrayElements) return make_error("too many elements in multibulk request");

    // First pass: locate every element without copying, so an incomplete
    // frame costs no allocations however often it is re-parsed.
    std::vector<std::string_view> elements;
    elements.reserve(*count);
    std::size_t pos = count_line.next;
    for (std::size_t i = 0; i < *count; ++i) {
        if (pos >= buf.size()) return Incomplete{};
        if (buf[pos] != '$') {
            return make_error(describe("expected '$', got", buf.substr(pos, 1)));
        }
        LineResult len_line = read_crlf_line(buf, pos + 1, kMaxLengthDigits, "invalid bulk length");
        if (auto* r = std::get_if<Incomplete>(&len_line)) return *r;
        if (auto* r = std::get_if<ProtocolError>(&len_line)) return std::move(*r);
        const Line len = std::get<Line>(len_line);

        const std::optional<std::size_t> size = parse_length(len.text);
        if (!size) return make_error(describe("invalid bulk length", len.text));
        if (*size > kMaxBulkLength) return make_error("bulk length exceeds 1 MiB limit");
        if (len.next + *size + kCrlf.size() > kMaxRequestBytes) {
            return make_error("request exceeds 8 MiB limit");
        }

        // Payload plus CRLF. Compare without forming an out-of-range position.
        if (buf.size() - len.next < *size + kCrlf.size()) return Incomplete{};
        if (buf.substr(len.next + *size, kCrlf.size()) != kCrlf) {
            return make_error("expected CRLF after bulk payload");
        }
        elements.push_back(buf.substr(len.next, *size));
        pos = len.next + *size + kCrlf.size();
    }

    Complete result;
    result.consumed = pos;
    result.args.reserve(elements.size());
    for (const std::string_view element : elements) result.args.emplace_back(element);
    return result;
}

std::string sanitize_line(std::string_view s) {
    std::string out(s);
    for (char& c : out) {
        if (c == '\r' || c == '\n') c = ' ';
    }
    return out;
}

}  // namespace

ParseResult parse_request(std::string_view buffer) {
    if (buffer.empty()) return Incomplete{};
    switch (buffer.front()) {
        case '*':
            return parse_array(buffer);
        // Reply-type bytes are not valid at the start of a request.
        case '$':
        case '+':
        case '-':
        case ':':
            return make_error(describe("unexpected type byte, expected '*' or inline, got",
                                       buffer.substr(0, 1)));
        default:
            return parse_inline(buffer);
    }
}

std::string simple_string(std::string_view s) { return "+" + sanitize_line(s) + "\r\n"; }

std::string error(std::string_view message) { return "-" + sanitize_line(message) + "\r\n"; }

std::string integer(std::int64_t n) { return ":" + std::to_string(n) + "\r\n"; }

std::string bulk_string(std::string_view bytes) {
    std::string out = "$" + std::to_string(bytes.size()) + "\r\n";
    out.reserve(out.size() + bytes.size() + kCrlf.size());
    out += bytes;
    out += kCrlf;
    return out;
}

std::string null_bulk() { return "$-1\r\n"; }

std::string array(const std::vector<std::string>& encoded_elements) {
    std::string out = "*" + std::to_string(encoded_elements.size()) + "\r\n";
    for (const std::string& element : encoded_elements) out += element;
    return out;
}

std::string command(const std::vector<std::string>& args) {
    std::string out = "*" + std::to_string(args.size()) + "\r\n";
    for (const std::string& arg : args) out += bulk_string(arg);
    return out;
}

}  // namespace distkv::resp

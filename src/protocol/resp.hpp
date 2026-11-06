#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

namespace distkv::resp {

// RESP2 (REdis Serialization Protocol) request parsing and reply encoding.
// Pure byte manipulation: no sockets, no state between calls.
//
// Requests come in two forms:
//   - Arrays of bulk strings, as sent by redis-cli and client libraries:
//       *2\r\n$3\r\nGET\r\n$1\r\nk\r\n
//     Bulk payloads are binary-safe: the length prefix is trusted, so the
//     payload may contain \r, \n and \0.
//   - Inline commands, for humans with nc/telnet: "SET a b\r\n", split on
//     spaces and tabs. A bare "\n" terminator is also accepted.

// Limits. Exceeding one is a protocol error, which also bounds how much an
// incomplete frame can make the connection buffer.
inline constexpr std::size_t kMaxBulkLength = 1024 * 1024;  // 1 MiB per argument.
inline constexpr std::size_t kMaxArrayElements = 1024;      // Arguments per request.
inline constexpr std::size_t kMaxInlineLength = 64 * 1024;  // One inline line, excluding CRLF.
// Total encoded size of one array request. Without it, 1024 elements of
// 1 MiB each would let a single incomplete request buffer 1 GiB.
inline constexpr std::size_t kMaxRequestBytes = 8 * 1024 * 1024;
// Cap on the "<n>" in "*<n>\r\n" / "$<n>\r\n", excluding the type byte and CRLF.
inline constexpr std::size_t kMaxLengthDigits = 20;

// A full request was parsed. `args` may be empty: blank inline lines and
// "*0\r\n" are valid no-op frames that the caller should skip (as Redis does).
struct Complete {
    std::vector<std::string> args;
    std::size_t consumed{0};  // Bytes of the buffer this frame occupied.
};

// The buffer holds only a prefix of a frame. Nothing was consumed; call again
// with the same bytes plus more.
struct Incomplete {};

// The bytes can never become a valid frame. The connection should be closed.
struct ProtocolError {
    std::string message;
};

using ParseResult = std::variant<Complete, Incomplete, ProtocolError>;

// Parses the first request frame in `buffer`, which may hold a partial frame,
// one frame, or several pipelined frames. Never reads past the end of
// `buffer`. Call repeatedly, dropping `consumed` bytes each time, to drain
// pipelined frames.
[[nodiscard]] ParseResult parse_request(std::string_view buffer);

// --- Reply encoding ---------------------------------------------------------
// Each function returns the complete encoded reply.

// "+<s>\r\n". CR and LF in `s` are replaced with spaces to keep the framing.
[[nodiscard]] std::string simple_string(std::string_view s);
// "-<message>\r\n", e.g. error("ERR unknown command"). CR/LF become spaces.
[[nodiscard]] std::string error(std::string_view message);
// ":<n>\r\n".
[[nodiscard]] std::string integer(std::int64_t n);
// "$<len>\r\n<bytes>\r\n". Binary-safe.
[[nodiscard]] std::string bulk_string(std::string_view bytes);
// "$-1\r\n".
[[nodiscard]] std::string null_bulk();
// "*<n>\r\n" followed by the already-encoded elements.
[[nodiscard]] std::string array(const std::vector<std::string>& encoded_elements);
// A request as clients send it: an array of bulk strings.
[[nodiscard]] std::string command(const std::vector<std::string>& args);

}  // namespace distkv::resp

#pragma once

#include <cstddef>
#include <string>
#include <string_view>

// Line-based text protocol spoken on the client port.
//
// Each request is one line ending in "\n" ("\r\n" is accepted too):
//
//   PUT <key> <value>     value is the rest of the line and may contain spaces;
//                         SET is accepted as an alias
//   GET <key>
//   DELETE <key>          DEL is accepted as an alias
//   STATUS
//   PING
//
// Commands are case-insensitive; keys are case-sensitive and may not contain
// spaces. Each request gets exactly one response line, in request order:
//
//   OK                    PUT / DELETE succeeded
//   VALUE <value>         GET hit
//   NOT_FOUND             GET miss
//   STATUS k=v k=v ...    node status
//   PONG
//   ERR <message>         bad request or server-side failure
//
// Being line-based, the protocol cannot carry values containing "\n"; it is
// meant for humans with nc/telnet and the Python API layer.
namespace distkv {

enum class CommandType { kPut, kGet, kDelete, kStatus, kPing };

struct Request {
    CommandType type = CommandType::kPing;
    std::string key;
    std::string value;
};

// Longest request line accepted, including the newline. Longer lines are
// answered with an error and the connection is closed, since the rest of the
// line can no longer be framed.
inline constexpr size_t kMaxLineBytes = 1u << 20;

// Parses one line with its trailing "\n" / "\r\n" already removed. On failure
// returns false and puts a human-readable reason in `*error`.
bool ParseRequest(std::string_view line, Request* out, std::string* error);

// Response builders. Each returns a complete line including "\n".
std::string OkResponse();
std::string ValueResponse(std::string_view value);
std::string NotFoundResponse();
std::string ErrorResponse(std::string_view message);
std::string PongResponse();

}  // namespace distkv

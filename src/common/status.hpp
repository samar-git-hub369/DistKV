#pragma once

#include <string>
#include <string_view>
#include <utility>

namespace distkv {

enum class StatusCode {
    kOk,
    kInvalidArgument,  // The request is malformed (e.g. empty key).
    kNotLeader,        // This node cannot accept writes; retry on the leader.
    kUnavailable,      // Transient failure (no quorum, shutting down); retry later.
    kInternal,         // A bug or invariant violation.
};

constexpr std::string_view to_string(StatusCode code) {
    switch (code) {
        case StatusCode::kOk:
            return "OK";
        case StatusCode::kInvalidArgument:
            return "INVALID_ARGUMENT";
        case StatusCode::kNotLeader:
            return "NOT_LEADER";
        case StatusCode::kUnavailable:
            return "UNAVAILABLE";
        case StatusCode::kInternal:
            return "INTERNAL";
    }
    return "UNKNOWN";
}

// The outcome of an operation, returned by value. Errors are expected,
// recoverable events in a distributed system (lost leadership, timeouts), so
// they are modelled as values rather than exceptions.
class [[nodiscard]] Status {
public:
    Status() = default;  // OK.

    static Status ok() { return Status{}; }
    static Status invalid_argument(std::string message) {
        return Status{StatusCode::kInvalidArgument, std::move(message)};
    }
    static Status not_leader(std::string message) {
        return Status{StatusCode::kNotLeader, std::move(message)};
    }
    static Status unavailable(std::string message) {
        return Status{StatusCode::kUnavailable, std::move(message)};
    }
    static Status internal(std::string message) {
        return Status{StatusCode::kInternal, std::move(message)};
    }

    [[nodiscard]] bool is_ok() const { return code_ == StatusCode::kOk; }
    [[nodiscard]] StatusCode code() const { return code_; }
    [[nodiscard]] const std::string& message() const { return message_; }

    // "OK", or "<CODE>: <message>".
    [[nodiscard]] std::string to_string() const {
        std::string out{distkv::to_string(code_)};
        if (!message_.empty()) {
            out += ": ";
            out += message_;
        }
        return out;
    }

    friend bool operator==(const Status&, const Status&) = default;

private:
    Status(StatusCode code, std::string message) : code_(code), message_(std::move(message)) {}

    StatusCode code_{StatusCode::kOk};
    std::string message_;
};

}  // namespace distkv

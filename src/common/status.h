#pragma once

#include <string>
#include <utility>

namespace distkv {

enum class StatusCode {
    kOk,
    kNotFound,
    kCorruption,
    kIOError,
    kInvalidArgument,
};

// Result of an operation that can fail. Cheap to return by value when ok.
class Status {
  public:
    Status() = default;

    static Status Ok() { return Status(); }
    static Status NotFound(std::string msg) { return {StatusCode::kNotFound, std::move(msg)}; }
    static Status Corruption(std::string msg) { return {StatusCode::kCorruption, std::move(msg)}; }
    static Status IOError(std::string msg) { return {StatusCode::kIOError, std::move(msg)}; }
    static Status InvalidArgument(std::string msg) {
        return {StatusCode::kInvalidArgument, std::move(msg)};
    }

    bool ok() const { return code_ == StatusCode::kOk; }
    StatusCode code() const { return code_; }
    const std::string& message() const { return message_; }

    std::string ToString() const {
        switch (code_) {
            case StatusCode::kOk:
                return "OK";
            case StatusCode::kNotFound:
                return "NotFound: " + message_;
            case StatusCode::kCorruption:
                return "Corruption: " + message_;
            case StatusCode::kIOError:
                return "IOError: " + message_;
            case StatusCode::kInvalidArgument:
                return "InvalidArgument: " + message_;
        }
        return "Unknown: " + message_;
    }

  private:
    Status(StatusCode code, std::string msg) : code_(code), message_(std::move(msg)) {}

    StatusCode code_ = StatusCode::kOk;
    std::string message_;
};

}  // namespace distkv

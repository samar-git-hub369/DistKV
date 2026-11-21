#include "protocol/text_protocol.h"

#include <algorithm>
#include <cctype>

namespace distkv {
namespace {

bool EqualsIgnoreCase(std::string_view a, std::string_view b) {
    return a.size() == b.size() &&
           std::equal(a.begin(), a.end(), b.begin(), [](char x, char y) {
               return std::toupper(static_cast<unsigned char>(x)) ==
                      std::toupper(static_cast<unsigned char>(y));
           });
}

// Splits off the first space-separated token of `*rest`.
std::string_view NextToken(std::string_view* rest) {
    const size_t space = rest->find(' ');
    const std::string_view token = rest->substr(0, space);
    rest->remove_prefix(space == std::string_view::npos ? rest->size() : space + 1);
    return token;
}

}  // namespace

bool ParseRequest(std::string_view line, Request* out, std::string* error) {
    if (!line.empty() && line.back() == '\r') {
        line.remove_suffix(1);
    }
    std::string_view rest = line;
    const std::string_view command = NextToken(&rest);
    if (command.empty()) {
        *error = "empty request";
        return false;
    }

    *out = Request{};
    if (EqualsIgnoreCase(command, "PING") || EqualsIgnoreCase(command, "STATUS")) {
        if (!rest.empty()) {
            *error = std::string(command) + " takes no arguments";
            return false;
        }
        out->type = EqualsIgnoreCase(command, "PING") ? CommandType::kPing : CommandType::kStatus;
        return true;
    }

    CommandType type;
    if (EqualsIgnoreCase(command, "PUT") || EqualsIgnoreCase(command, "SET")) {
        type = CommandType::kPut;
    } else if (EqualsIgnoreCase(command, "GET")) {
        type = CommandType::kGet;
    } else if (EqualsIgnoreCase(command, "DELETE") || EqualsIgnoreCase(command, "DEL")) {
        type = CommandType::kDelete;
    } else {
        *error = "unknown command '" + std::string(command) + "'";
        return false;
    }

    const std::string_view key = NextToken(&rest);
    if (key.empty()) {
        *error = "missing key";
        return false;
    }
    if (type == CommandType::kPut) {
        // The value is everything after the key, spaces included. An empty
        // value is allowed when the separating space is present ("PUT k ").
        out->value.assign(rest.data(), rest.size());
        if (rest.empty() && line.size() == command.size() + 1 + key.size()) {
            *error = "missing value";
            return false;
        }
    } else if (!rest.empty()) {
        *error = "too many arguments";
        return false;
    }
    out->type = type;
    out->key.assign(key.data(), key.size());
    return true;
}

std::string OkResponse() { return "OK\n"; }

std::string ValueResponse(std::string_view value) {
    std::string r;
    r.reserve(6 + value.size() + 1);
    r += "VALUE ";
    r += value;
    r += '\n';
    return r;
}

std::string NotFoundResponse() { return "NOT_FOUND\n"; }

std::string ErrorResponse(std::string_view message) {
    std::string r = "ERR ";
    // A message must never break the one-line framing.
    for (const char c : message) {
        r += (c == '\n' || c == '\r') ? ' ' : c;
    }
    r += '\n';
    return r;
}

std::string PongResponse() { return "PONG\n"; }

}  // namespace distkv

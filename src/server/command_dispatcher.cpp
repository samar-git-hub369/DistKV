#include "server/command_dispatcher.hpp"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <string_view>
#include <unordered_set>
#include <utility>

#include "protocol/resp.hpp"

namespace distkv {
namespace {

std::string to_lower(std::string_view s) {
    std::string out(s);
    for (char& c : out) {
        if (c >= 'A' && c <= 'Z') c = static_cast<char>(c - 'A' + 'a');
    }
    return out;
}

// Command names are echoed back in errors: keep them printable and short.
std::string printable(std::string_view s) {
    constexpr std::size_t kMaxShown = 64;
    std::string out;
    for (const char c : s.substr(0, kMaxShown)) {
        const auto byte = static_cast<unsigned char>(c);
        out += (byte >= 0x20 && byte < 0x7f) ? c : '?';
    }
    return out;
}

std::string wrong_arity(std::string_view lower_name) {
    return resp::error("ERR wrong number of arguments for '" + printable(lower_name) +
                       "' command");
}

}  // namespace

CommandDispatcher::CommandDispatcher(KvService& service) : service_(service) {}

std::string CommandDispatcher::encode_error(const Status& status) {
    switch (status.code()) {
        case StatusCode::kInvalidArgument:
            return resp::error("ERR " + status.message());
        case StatusCode::kNotLeader:
            return resp::error("NOTLEADER " + status.message());
        case StatusCode::kUnavailable:
            return resp::error("TRYAGAIN " + status.message());
        case StatusCode::kInternal:
            return resp::error("ERR internal: " + status.message());
        case StatusCode::kOk:
            break;
    }
    return resp::error("ERR internal: unexpected status " + status.to_string());
}

void CommandDispatcher::dispatch(std::vector<std::string> args, ReplyCallback reply) {
    if (args.empty()) {
        reply(resp::error("ERR empty command"));
        return;
    }
    const std::string name = to_lower(args[0]);
    if (name == "get") {
        get(args, reply);
    } else if (name == "set") {
        set(std::move(args), std::move(reply));
    } else if (name == "del") {
        del(std::move(args), std::move(reply));
    } else if (name == "ping") {
        ping(args, reply);
    } else if (name == "echo") {
        echo(args, reply);
    } else if (name == "command") {
        reply(resp::array({}));
    } else {
        reply(resp::error("ERR unknown command '" + printable(args[0]) + "'"));
    }
}

void CommandDispatcher::ping(const std::vector<std::string>& args, const ReplyCallback& reply) {
    if (args.size() == 1) {
        reply(resp::simple_string("PONG"));
    } else if (args.size() == 2) {
        reply(resp::bulk_string(args[1]));
    } else {
        reply(wrong_arity("ping"));
    }
}

void CommandDispatcher::echo(const std::vector<std::string>& args, const ReplyCallback& reply) {
    if (args.size() != 2) {
        reply(wrong_arity("echo"));
        return;
    }
    reply(resp::bulk_string(args[1]));
}

void CommandDispatcher::get(const std::vector<std::string>& args, const ReplyCallback& reply) {
    if (args.size() != 2) {
        reply(wrong_arity("get"));
        return;
    }
    const StatusOr<std::optional<std::string>> result = service_.get(args[1]);
    if (!result.is_ok()) {
        reply(encode_error(result.status()));
    } else if (!result->has_value()) {
        reply(resp::null_bulk());
    } else {
        reply(resp::bulk_string(**result));
    }
}

void CommandDispatcher::set(std::vector<std::string> args, ReplyCallback reply) {
    if (args.size() != 3) {
        reply(wrong_arity("set"));
        return;
    }
    service_.set(std::move(args[1]), std::move(args[2]),
                 [reply = std::move(reply)](const Status& status, std::uint64_t /*index*/) {
                     reply(status.is_ok() ? resp::simple_string("OK") : encode_error(status));
                 });
}

// DEL proposes one delete per key and replies with how many of the (distinct)
// keys existed when checked just before proposing. The count is approximate
// under concurrent writers: another client may create or delete a key between
// the check and the apply. It becomes exact once apply results flow back
// through the commit callback.
void CommandDispatcher::del(std::vector<std::string> args, ReplyCallback reply) {
    if (args.size() < 2) {
        reply(wrong_arity("del"));
        return;
    }

    // Read first, so a node that cannot serve the command (e.g. NOTLEADER)
    // rejects it before anything is proposed.
    std::int64_t existed = 0;
    std::unordered_set<std::string_view> seen;
    for (std::size_t i = 1; i < args.size(); ++i) {
        const StatusOr<std::optional<std::string>> current = service_.get(args[i]);
        if (!current.is_ok()) {
            reply(encode_error(current.status()));
            return;
        }
        if (current->has_value() && seen.insert(args[i]).second) ++existed;
    }
    seen.clear();  // Holds views into args, which are moved from below.

    // Shared by the per-key callbacks, which may run on other threads.
    // remaining and first_error are guarded by mutex; existed and reply are
    // set before any callback can run and only read afterwards (reply by the
    // last callback, once no other callback can touch this object).
    struct Pending {
        std::mutex mutex;
        std::size_t remaining;               // Guarded by mutex.
        std::optional<Status> first_error;   // Guarded by mutex.
        std::int64_t existed;                // Immutable once shared.
        ReplyCallback reply;                 // Immutable once shared.
    };
    auto pending = std::make_shared<Pending>();
    pending->remaining = args.size() - 1;
    pending->existed = existed;
    pending->reply = std::move(reply);

    for (std::size_t i = 1; i < args.size(); ++i) {
        service_.del(std::move(args[i]), [pending](const Status& status, std::uint64_t) {
            bool last = false;
            {
                std::lock_guard lock(pending->mutex);
                if (!status.is_ok() && !pending->first_error) pending->first_error = status;
                last = --pending->remaining == 0;
            }
            if (last) {
                // All callbacks have run, so no other thread touches `pending`.
                pending->reply(pending->first_error ? encode_error(*pending->first_error)
                                                    : resp::integer(pending->existed));
            }
        });
    }
}

}  // namespace distkv

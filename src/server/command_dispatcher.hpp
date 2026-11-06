#pragma once

#include <functional>
#include <string>
#include <vector>

#include "common/status.hpp"
#include "server/kv_service.hpp"

namespace distkv {

// Receives one encoded RESP reply.
using ReplyCallback = std::function<void(std::string reply)>;

// Executes protocol-level commands (Redis command names and reply shapes) on
// top of KvService. Knows nothing about sockets, so it is unit-testable.
//
// Commands (names are case-insensitive):
//   PING [message]        +PONG, or the message as a bulk string
//   ECHO message          the message as a bulk string
//   GET key               bulk string, or null bulk if absent
//   SET key value         +OK once committed (no options yet)
//   DEL key [key ...]     integer: number of keys removed (see below)
//   COMMAND [...]         empty array (keeps redis-cli's startup probe quiet)
//
// Thread safety: dispatch() may be called concurrently. The reply callback is
// invoked exactly once per dispatch(), either before dispatch() returns or
// later from whatever thread the Replicator completes on, so the caller must
// not assume which thread it runs on.
class CommandDispatcher {
public:
    // `service` must outlive this object.
    explicit CommandDispatcher(KvService& service);

    void dispatch(std::vector<std::string> args, ReplyCallback reply);

    // Encodes a non-OK Status as a RESP error:
    //   kInvalidArgument -> -ERR <msg>
    //   kNotLeader       -> -NOTLEADER <msg>
    //   kUnavailable     -> -TRYAGAIN <msg>
    //   kInternal        -> -ERR internal: <msg>
    [[nodiscard]] static std::string encode_error(const Status& status);

private:
    void ping(const std::vector<std::string>& args, const ReplyCallback& reply);
    void echo(const std::vector<std::string>& args, const ReplyCallback& reply);
    void get(const std::vector<std::string>& args, const ReplyCallback& reply);
    void set(std::vector<std::string> args, ReplyCallback reply);
    void del(std::vector<std::string> args, ReplyCallback reply);

    KvService& service_;
};

}  // namespace distkv

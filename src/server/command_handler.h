#pragma once

#include <atomic>
#include <cstdint>
#include <string>
#include <string_view>

#include "protocol/text_protocol.h"
#include "server/kv_backend.h"

namespace distkv {

// Turns one request line into one response line. Stateless apart from
// counters, so every connection and io thread can share one handler.
class CommandHandler {
  public:
    explicit CommandHandler(KvBackend* backend) : backend_(backend) {}

    // `line` excludes the trailing newline. The result includes it.
    std::string HandleLine(std::string_view line);
    std::string Handle(const Request& request);

    uint64_t requests() const { return requests_.load(std::memory_order_relaxed); }
    uint64_t errors() const { return errors_.load(std::memory_order_relaxed); }

  private:
    KvBackend* backend_;
    std::atomic<uint64_t> requests_{0};
    std::atomic<uint64_t> errors_{0};
};

}  // namespace distkv

#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <optional>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <unordered_map>

#include "common/log_entry.hpp"
#include "storage/state_machine.hpp"

namespace distkv {

// In-memory key-value map: the StateMachine that committed log entries are
// applied to. Keys and values are arbitrary byte strings (embedded '\0' and
// non-UTF-8 bytes are fine).
//
// Thread safety: all methods are safe to call concurrently. map_ and
// last_applied_ are guarded by mutex_ (exclusive for apply(), shared for the
// readers). apply() is only ever called by the Replicator, one entry at a time
// and with LocalReplicator::mutex_ held; get(), size() and
// last_applied_index() may be called from any number of threads at once.
// mutex_ is the innermost lock: nothing is called while holding it.
class KvStore final : public StateMachine {
public:
    KvStore() = default;

    // Applies a committed entry. Entries at or below last_applied_index() are
    // ignored (see kv_store.cpp); an entry that would leave a gap aborts.
    void apply(const LogEntry& entry) override;

    // Returns the value for `key`, or std::nullopt if it is not present.
    // Looking up by string_view does not allocate.
    [[nodiscard]] std::optional<std::string> get(std::string_view key) const;

    [[nodiscard]] std::size_t size() const;

    // Index of the last entry applied (0 if none).
    [[nodiscard]] std::uint64_t last_applied_index() const;

private:
    // Transparent hash so find() accepts std::string_view without building a
    // std::string. Paired with std::equal_to<> this enables C++20
    // heterogeneous lookup in unordered_map (libstdc++ 11+, libc++ 12+).
    struct StringHash {
        using is_transparent = void;
        std::size_t operator()(std::string_view key) const noexcept {
            return std::hash<std::string_view>{}(key);
        }
    };

    using Map = std::unordered_map<std::string, std::string, StringHash, std::equal_to<>>;

    // shared_mutex rather than mutex: reads vastly outnumber writes in a KV
    // workload and many readers can proceed in parallel under shared_lock.
    // Writes gain nothing from finer-grained locking because the Replicator
    // already serialises apply(), so a single exclusive lock per write costs
    // nothing extra.
    mutable std::shared_mutex mutex_;
    Map map_;                        // Guarded by mutex_.
    std::uint64_t last_applied_{0};  // Guarded by mutex_.
};

}  // namespace distkv

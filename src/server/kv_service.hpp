#pragma once

#include <optional>
#include <string>
#include <string_view>

#include "common/status_or.hpp"
#include "replication/replicator.hpp"
#include "storage/kv_store.hpp"

namespace distkv {

// The operations the network layer (Day 3) exposes to clients, independent of
// any wire protocol. Writes go through the Replicator so they are ordered and
// replicated; reads go straight to the local KvStore.
//
// Thread safety: as thread-safe as the Replicator and KvStore it wraps (both
// are safe for concurrent use), so one instance may serve all connections.
class KvService {
public:
    // Both references must outlive this object. The store is held const: all
    // mutations must go through the replicator, never directly.
    KvService(Replicator& replicator, const KvStore& store);

    // Proposes a write. `on_commit` follows the Replicator contract: called
    // exactly once, after the write is applied on success.
    void set(std::string key, std::string value, CommitCallback on_commit);
    void del(std::string key, CommitCallback on_commit);

    // Returns the current value (std::nullopt if the key is absent), or:
    //   kNotLeader       if this node is not the leader. In Phase 1 reads are
    //                    served by the leader only, so they never return
    //                    stale data from a lagging follower.
    //   kInvalidArgument if `key` is empty.
    [[nodiscard]] StatusOr<std::optional<std::string>> get(std::string_view key) const;

private:
    Replicator& replicator_;
    const KvStore& store_;
};

}  // namespace distkv

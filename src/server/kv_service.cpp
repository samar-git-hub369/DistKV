#include "server/kv_service.hpp"

#include <utility>

namespace distkv {

KvService::KvService(Replicator& replicator, const KvStore& store)
    : replicator_(replicator), store_(store) {}

void KvService::set(std::string key, std::string value, CommitCallback on_commit) {
    replicator_.propose(Command::set(std::move(key), std::move(value)), std::move(on_commit));
}

void KvService::del(std::string key, CommitCallback on_commit) {
    replicator_.propose(Command::del(std::move(key)), std::move(on_commit));
}

StatusOr<std::optional<std::string>> KvService::get(std::string_view key) const {
    if (!replicator_.is_leader()) {
        return Status::not_leader("reads are served by the leader only");
    }
    if (key.empty()) {
        return Status::invalid_argument("key must not be empty");
    }
    return store_.get(key);
}

}  // namespace distkv

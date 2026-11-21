#pragma once

#include <string>
#include <string_view>

#include "common/status.h"

namespace distkv {

// What the request handler needs from whatever stores the data. Today that is
// a single StorageEngine; with Raft it becomes a node that replicates writes
// before applying them, without the network layer having to change.
class KvBackend {
  public:
    virtual ~KvBackend() = default;

    virtual Status Put(std::string_view key, std::string_view value) = 0;
    virtual Status Delete(std::string_view key) = 0;
    // NotFound when the key is absent or deleted.
    virtual Status Get(std::string_view key, std::string* value) = 0;
    // Space-separated key=value pairs describing the node.
    virtual std::string StatusFields() = 0;
};

class StorageEngine;

// Serves requests straight from one local StorageEngine.
class StandaloneBackend : public KvBackend {
  public:
    explicit StandaloneBackend(StorageEngine* engine) : engine_(engine) {}

    Status Put(std::string_view key, std::string_view value) override;
    Status Delete(std::string_view key) override;
    Status Get(std::string_view key, std::string* value) override;
    std::string StatusFields() override;

  private:
    StorageEngine* engine_;
};

}  // namespace distkv

#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <string>

#include "common/status.h"

// Write-ahead log.
//
// Every mutation is appended here before it is applied in memory, so a crash
// loses nothing that was acknowledged. On disk the log is a sequence of frames:
//
//   frame   := crc32:u32  length:u32  payload[length]
//   payload := op:u8  seq:u64  key_len:u32  key  value_len:u32  value
//
// All integers are little-endian. The CRC covers the length field and the
// payload, so a damaged length is caught as well as damaged data.
namespace distkv {

enum class WalOp : uint8_t {
    kPut = 1,
    kDelete = 2,
};

struct WalRecord {
    uint64_t seq = 0;
    WalOp op = WalOp::kPut;
    std::string key;
    std::string value;  // empty for kDelete

    bool operator==(const WalRecord& o) const {
        return seq == o.seq && op == o.op && key == o.key && value == o.value;
    }
};

// Frames larger than this are rejected on write and treated as damage on read,
// so a corrupted length can never make replay allocate gigabytes.
inline constexpr uint32_t kMaxWalPayload = 64u << 20;

enum class WalSyncMode {
    kNone,         // leave flushing to the OS; fastest, loses recent writes on power loss
    kEveryAppend,  // fsync after each Append; an acknowledged write survives power loss
};

class WalWriter {
  public:
    // Opens `path` for appending, creating it if needed.
    static Status Open(const std::string& path, WalSyncMode mode, std::unique_ptr<WalWriter>* out);

    ~WalWriter();
    WalWriter(const WalWriter&) = delete;
    WalWriter& operator=(const WalWriter&) = delete;

    Status Append(const WalRecord& record);
    Status Sync();

    uint64_t bytes_written() const { return bytes_written_; }

  private:
    WalWriter(int fd, WalSyncMode mode) : fd_(fd), mode_(mode) {}

    int fd_;
    WalSyncMode mode_;
    uint64_t bytes_written_ = 0;
};

struct WalReplayStats {
    uint64_t records = 0;
    uint64_t valid_bytes = 0;    // offset just past the last good frame
    bool torn_tail = false;      // the file ended in an incomplete or damaged final frame
};

// Calls `fn` for each record in order. A missing file replays as empty.
//
// A damaged frame that runs to the end of the file is a write cut short by a
// crash: replay stops before it and sets `torn_tail`. A damaged frame with
// more data after it means the log itself is corrupt, and replay fails.
Status ReplayWal(const std::string& path, const std::function<void(const WalRecord&)>& fn,
                 WalReplayStats* stats);

// ReplayWal, then cut off any torn tail so new appends follow the last good
// frame. Call this on startup before opening a WalWriter on the same file.
Status RecoverWal(const std::string& path, const std::function<void(const WalRecord&)>& fn,
                  WalReplayStats* stats);

// Exposed for tests: the bytes Append writes for `record`.
std::string EncodeWalFrame(const WalRecord& record);

}  // namespace distkv

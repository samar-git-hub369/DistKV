#pragma once

#include <cstddef>
#include <cstdint>
#include <string>

#include "common/status.h"

// Thin POSIX file helpers shared by the WAL and SSTables.
namespace distkv {

// IOError carrying `what path: strerror(errno)`.
Status ErrnoStatus(const std::string& what, const std::string& path);

// Flushes `fd` all the way to stable storage. Plain fsync is not enough on
// macOS: it hands data to the drive without flushing the drive's own cache,
// so this uses F_FULLFSYNC there. Returns 0 or -1 with errno set.
int DurableSync(int fd);

// Makes a newly created or renamed directory entry for `path` durable.
Status SyncParentDir(const std::string& path);

// Writes all `n` bytes, retrying short writes and EINTR.
Status WriteAll(int fd, const char* data, size_t n, const std::string& path);

// Reads exactly `n` bytes at `offset` into `*out`. Hitting end of file first is
// reported as Corruption, since callers only read ranges the file promised.
Status ReadAt(int fd, uint64_t offset, size_t n, std::string* out, const std::string& path);

}  // namespace distkv

#include "storage/wal.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <string_view>

#include "common/coding.h"
#include "common/crc32.h"

namespace distkv {
namespace {

constexpr size_t kHeaderSize = 8;  // crc32 + length

Status ErrnoStatus(const std::string& what, const std::string& path) {
    return Status::IOError(what + " " + path + ": " + std::strerror(errno));
}

// fsync is not enough on macOS: it hands data to the drive but does not ask
// the drive to flush its own cache. F_FULLFSYNC does.
int DurableSync(int fd) {
#ifdef __APPLE__
    if (::fcntl(fd, F_FULLFSYNC) == 0) {
        return 0;
    }
    // Some filesystems (e.g. network mounts) do not support F_FULLFSYNC.
#endif
    return ::fsync(fd);
}

// A new file's directory entry is only durable once the directory is synced.
Status SyncParentDir(const std::string& path) {
    std::filesystem::path parent = std::filesystem::path(path).parent_path();
    if (parent.empty()) {
        parent = ".";
    }
    const int dir_fd = ::open(parent.c_str(), O_RDONLY | O_CLOEXEC);
    if (dir_fd < 0) {
        return ErrnoStatus("open directory", parent.string());
    }
    const int rc = DurableSync(dir_fd);
    ::close(dir_fd);
    if (rc != 0) {
        return ErrnoStatus("sync directory", parent.string());
    }
    return Status::Ok();
}

Status WriteAll(int fd, const char* data, size_t n) {
    while (n > 0) {
        const ssize_t written = ::write(fd, data, n);
        if (written < 0) {
            if (errno == EINTR) {
                continue;
            }
            return Status::IOError(std::string("write wal: ") + std::strerror(errno));
        }
        data += written;
        n -= static_cast<size_t>(written);
    }
    return Status::Ok();
}

bool DecodePayload(std::string_view in, WalRecord* out) {
    if (in.size() < 1 + 8) {
        return false;
    }
    const auto op = static_cast<uint8_t>(in[0]);
    if (op != static_cast<uint8_t>(WalOp::kPut) && op != static_cast<uint8_t>(WalOp::kDelete)) {
        return false;
    }
    out->op = static_cast<WalOp>(op);
    out->seq = DecodeFixed64(in.data() + 1);
    in.remove_prefix(1 + 8);

    std::string_view key;
    std::string_view value;
    if (!GetLengthPrefixed(&in, &key) || !GetLengthPrefixed(&in, &value) || !in.empty()) {
        return false;
    }
    out->key.assign(key.data(), key.size());
    out->value.assign(value.data(), value.size());
    return true;
}

}  // namespace

std::string EncodeWalFrame(const WalRecord& record) {
    std::string payload;
    payload.reserve(1 + 8 + 4 + record.key.size() + 4 + record.value.size());
    payload.push_back(static_cast<char>(record.op));
    PutFixed64(&payload, record.seq);
    PutLengthPrefixed(&payload, record.key);
    PutLengthPrefixed(&payload, record.value);

    std::string len_bytes;
    PutFixed32(&len_bytes, static_cast<uint32_t>(payload.size()));
    const uint32_t crc = Crc32(payload, Crc32(len_bytes));

    std::string frame;
    frame.reserve(kHeaderSize + payload.size());
    PutFixed32(&frame, crc);
    frame += len_bytes;
    frame += payload;
    return frame;
}

Status WalWriter::Open(const std::string& path, WalSyncMode mode,
                       std::unique_ptr<WalWriter>* out) {
    struct stat st {};
    const bool existed = ::stat(path.c_str(), &st) == 0;

    const int fd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_APPEND | O_CLOEXEC, 0644);
    if (fd < 0) {
        return ErrnoStatus("open wal", path);
    }
    if (!existed) {
        Status s = SyncParentDir(path);
        if (!s.ok()) {
            ::close(fd);
            return s;
        }
    }
    out->reset(new WalWriter(fd, mode));
    return Status::Ok();
}

WalWriter::~WalWriter() {
    if (fd_ >= 0) {
        ::close(fd_);
    }
}

Status WalWriter::Append(const WalRecord& record) {
    if (record.op == WalOp::kDelete && !record.value.empty()) {
        return Status::InvalidArgument("delete record must not carry a value");
    }
    const uint64_t payload_size = 1 + 8 + 4 + record.key.size() + 4 + record.value.size();
    if (payload_size > kMaxWalPayload) {
        return Status::InvalidArgument("wal record too large");
    }

    const std::string frame = EncodeWalFrame(record);
    Status s = WriteAll(fd_, frame.data(), frame.size());
    if (!s.ok()) {
        return s;
    }
    bytes_written_ += frame.size();

    if (mode_ == WalSyncMode::kEveryAppend) {
        return Sync();
    }
    return Status::Ok();
}

Status WalWriter::Sync() {
    if (DurableSync(fd_) != 0) {
        return Status::IOError(std::string("sync wal: ") + std::strerror(errno));
    }
    return Status::Ok();
}

Status ReplayWal(const std::string& path, const std::function<void(const WalRecord&)>& fn,
                 WalReplayStats* stats) {
    *stats = WalReplayStats{};

    std::error_code ec;
    if (!std::filesystem::exists(path, ec)) {
        return ec ? Status::IOError("stat wal " + path + ": " + ec.message()) : Status::Ok();
    }
    const uint64_t file_size = std::filesystem::file_size(path, ec);
    if (ec) {
        return Status::IOError("stat wal " + path + ": " + ec.message());
    }

    std::ifstream in(path, std::ios::binary);
    if (!in) {
        return ErrnoStatus("open wal", path);
    }

    uint64_t offset = 0;
    char header[kHeaderSize];
    std::string payload;
    WalRecord record;

    while (offset < file_size) {
        const uint64_t remaining = file_size - offset;
        if (remaining < kHeaderSize) {
            stats->torn_tail = true;
            break;
        }
        if (!in.read(header, kHeaderSize)) {
            return Status::IOError("read wal " + path);
        }
        const uint32_t expected_crc = DecodeFixed32(header);
        const uint32_t length = DecodeFixed32(header + 4);

        // A frame that claims to run past the end of the file was cut short.
        if (length > kMaxWalPayload || length > remaining - kHeaderSize) {
            stats->torn_tail = true;
            break;
        }

        payload.resize(length);
        if (length > 0 && !in.read(payload.data(), length)) {
            return Status::IOError("read wal " + path);
        }

        const uint64_t frame_end = offset + kHeaderSize + length;
        const uint32_t actual_crc = Crc32(payload, Crc32(header + 4, 4));
        if (actual_crc != expected_crc) {
            if (frame_end == file_size) {
                stats->torn_tail = true;
                break;
            }
            return Status::Corruption("wal checksum mismatch at offset " + std::to_string(offset));
        }
        if (!DecodePayload(payload, &record)) {
            return Status::Corruption("malformed wal record at offset " + std::to_string(offset));
        }

        fn(record);
        ++stats->records;
        offset = frame_end;
        stats->valid_bytes = offset;
    }
    return Status::Ok();
}

Status RecoverWal(const std::string& path, const std::function<void(const WalRecord&)>& fn,
                  WalReplayStats* stats) {
    Status s = ReplayWal(path, fn, stats);
    if (!s.ok() || !stats->torn_tail) {
        return s;
    }

    const int fd = ::open(path.c_str(), O_WRONLY | O_CLOEXEC);
    if (fd < 0) {
        return ErrnoStatus("open wal", path);
    }
    int rc = ::ftruncate(fd, static_cast<off_t>(stats->valid_bytes));
    if (rc == 0) {
        rc = DurableSync(fd);
    }
    const int saved_errno = errno;
    ::close(fd);
    if (rc != 0) {
        errno = saved_errno;
        return ErrnoStatus("truncate wal", path);
    }
    return Status::Ok();
}

}  // namespace distkv

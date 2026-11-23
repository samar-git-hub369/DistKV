#include "raft/hard_state.h"

#include <fcntl.h>
#include <unistd.h>

#include <cerrno>
#include <cstdio>
#include <filesystem>

#include "common/coding.h"
#include "common/crc32.h"
#include "common/file_util.h"

namespace distkv::raft {
namespace {

constexpr uint64_t kHardStateMagic = 0x4154534454464152;  // "RAFTDSTA" little-endian
constexpr size_t kHardStateSize = 24;

}  // namespace

Status LoadHardState(const std::string& path, HardState* out) {
    *out = HardState{};
    const int fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
    if (fd < 0) {
        if (errno == ENOENT) {
            return Status::Ok();
        }
        return ErrnoStatus("open raft state", path);
    }

    std::error_code ec;
    const uint64_t size = std::filesystem::file_size(path, ec);
    if (ec) {
        ::close(fd);
        return Status::IOError("stat raft state " + path + ": " + ec.message());
    }
    if (size != kHardStateSize) {
        ::close(fd);
        return Status::Corruption("raft state " + path + " has size " + std::to_string(size));
    }

    std::string buf;
    Status s = ReadAt(fd, 0, kHardStateSize, &buf, path);
    ::close(fd);
    if (!s.ok()) {
        return s;
    }
    if (DecodeFixed64(buf.data()) != kHardStateMagic) {
        return Status::Corruption("bad magic in raft state " + path);
    }
    if (Crc32(buf.data(), 20) != DecodeFixed32(buf.data() + 20)) {
        return Status::Corruption("checksum mismatch in raft state " + path);
    }
    out->term = DecodeFixed64(buf.data() + 8);
    out->voted_for = DecodeFixed32(buf.data() + 16);
    return Status::Ok();
}

Status SaveHardState(const std::string& path, const HardState& state) {
    std::string buf;
    PutFixed64(&buf, kHardStateMagic);
    PutFixed64(&buf, state.term);
    PutFixed32(&buf, state.voted_for);
    PutFixed32(&buf, Crc32(buf));

    const std::string tmp = path + ".tmp";
    const int fd = ::open(tmp.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
    if (fd < 0) {
        return ErrnoStatus("create raft state", tmp);
    }
    Status s = WriteAll(fd, buf.data(), buf.size(), tmp);
    if (s.ok() && DurableSync(fd) != 0) {
        s = ErrnoStatus("sync raft state", tmp);
    }
    ::close(fd);
    if (s.ok() && std::rename(tmp.c_str(), path.c_str()) != 0) {
        s = ErrnoStatus("rename raft state", tmp);
    }
    if (!s.ok()) {
        std::remove(tmp.c_str());
        return s;
    }
    return SyncParentDir(path);
}

}  // namespace distkv::raft

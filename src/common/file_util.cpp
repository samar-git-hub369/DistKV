#include "common/file_util.h"

#include <fcntl.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <filesystem>

namespace distkv {

Status ErrnoStatus(const std::string& what, const std::string& path) {
    return Status::IOError(what + " " + path + ": " + std::strerror(errno));
}

int DurableSync(int fd) {
#ifdef __APPLE__
    if (::fcntl(fd, F_FULLFSYNC) == 0) {
        return 0;
    }
    // Some filesystems (e.g. network mounts) do not support F_FULLFSYNC.
#endif
    return ::fsync(fd);
}

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
    const int saved_errno = errno;
    ::close(dir_fd);
    if (rc != 0) {
        errno = saved_errno;
        return ErrnoStatus("sync directory", parent.string());
    }
    return Status::Ok();
}

Status WriteAll(int fd, const char* data, size_t n, const std::string& path) {
    while (n > 0) {
        const ssize_t written = ::write(fd, data, n);
        if (written < 0) {
            if (errno == EINTR) {
                continue;
            }
            return ErrnoStatus("write", path);
        }
        data += written;
        n -= static_cast<size_t>(written);
    }
    return Status::Ok();
}

Status ReadAt(int fd, uint64_t offset, size_t n, std::string* out, const std::string& path) {
    out->resize(n);
    size_t done = 0;
    while (done < n) {
        const ssize_t got =
            ::pread(fd, out->data() + done, n - done, static_cast<off_t>(offset + done));
        if (got < 0) {
            if (errno == EINTR) {
                continue;
            }
            return ErrnoStatus("read", path);
        }
        if (got == 0) {
            return Status::Corruption("unexpected end of file in " + path);
        }
        done += static_cast<size_t>(got);
    }
    return Status::Ok();
}

}  // namespace distkv

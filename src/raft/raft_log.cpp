#include "raft/raft_log.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#include <string_view>

#include "common/coding.h"
#include "common/crc32.h"
#include "common/file_util.h"

namespace distkv::raft {
namespace {

constexpr size_t kFrameHeader = 8;  // crc32 + length

}  // namespace

std::string EncodeLogFrame(const LogEntry& entry) {
    std::string payload;
    EncodeLogEntry(&payload, entry);

    std::string len_bytes;
    PutFixed32(&len_bytes, static_cast<uint32_t>(payload.size()));

    std::string frame;
    frame.reserve(kFrameHeader + payload.size());
    PutFixed32(&frame, Crc32(payload, Crc32(len_bytes)));
    frame += len_bytes;
    frame += payload;
    return frame;
}

Status RaftLog::Open(const std::string& path, bool sync, std::unique_ptr<RaftLog>* out,
                     RaftLogRecovery* recovery) {
    struct stat st {};
    const bool existed = ::stat(path.c_str(), &st) == 0;

    const int fd = ::open(path.c_str(), O_RDWR | O_CREAT | O_APPEND | O_CLOEXEC, 0644);
    if (fd < 0) {
        return ErrnoStatus("open raft log", path);
    }
    std::unique_ptr<RaftLog> log(new RaftLog(fd, sync, path));
    if (!existed) {
        Status s = SyncParentDir(path);
        if (!s.ok()) return s;
    }

    const uint64_t size = existed ? static_cast<uint64_t>(st.st_size) : 0;
    std::string data;
    if (size > 0) {
        Status s = ReadAt(fd, 0, static_cast<size_t>(size), &data, path);
        if (!s.ok()) return s;
    }

    RaftLogRecovery rec;
    uint64_t offset = 0;
    while (offset < size) {
        const uint64_t remaining = size - offset;
        const char* frame = data.data() + offset;
        if (remaining < kFrameHeader) {
            rec.torn_tail = true;
            break;
        }
        const uint32_t expected_crc = DecodeFixed32(frame);
        const uint32_t length = DecodeFixed32(frame + 4);
        if (length > kMaxLogEntryBytes || length > remaining - kFrameHeader) {
            rec.torn_tail = true;  // claims to run past the end: cut short
            break;
        }
        const uint64_t frame_end = offset + kFrameHeader + length;
        if (Crc32(frame + kFrameHeader, length, Crc32(frame + 4, 4)) != expected_crc) {
            if (frame_end == size) {
                rec.torn_tail = true;
                break;
            }
            return Status::Corruption("raft log checksum mismatch at offset " +
                                      std::to_string(offset) + " in " + path);
        }

        std::string_view body(frame + kFrameHeader, length);
        LogEntry entry;
        if (!DecodeLogEntry(&body, &entry) || !body.empty()) {
            return Status::Corruption("malformed raft log entry at offset " +
                                      std::to_string(offset) + " in " + path);
        }
        if (entry.index != log->last_index() + 1 || entry.term < log->last_term() ||
            entry.term == 0) {
            return Status::Corruption("raft log entry " + std::to_string(entry.index) +
                                      " (term " + std::to_string(entry.term) +
                                      ") does not continue the log in " + path);
        }
        log->offsets_.push_back(offset);
        log->entries_.push_back(std::move(entry));
        offset = frame_end;
    }

    if (rec.torn_tail) {
        if (::ftruncate(fd, static_cast<off_t>(offset)) != 0 || DurableSync(fd) != 0) {
            return ErrnoStatus("truncate raft log", path);
        }
    }
    log->file_size_ = offset;
    rec.entries = log->last_index();
    if (recovery != nullptr) {
        *recovery = rec;
    }
    *out = std::move(log);
    return Status::Ok();
}

RaftLog::~RaftLog() {
    if (fd_ >= 0) {
        ::close(fd_);
    }
}

bool RaftLog::TermAt(uint64_t index, uint64_t* term) const {
    if (index == 0) {
        *term = 0;
        return true;
    }
    if (index > last_index()) {
        return false;
    }
    *term = At(index).term;
    return true;
}

std::vector<LogEntry> RaftLog::Slice(uint64_t from, size_t max_entries, size_t max_bytes) const {
    std::vector<LogEntry> out;
    if (from == 0) from = 1;
    size_t bytes = 0;
    for (uint64_t i = from; i <= last_index() && out.size() < max_entries; ++i) {
        const LogEntry& e = At(i);
        const size_t size = EncodedSize(e);
        if (!out.empty() && bytes + size > max_bytes) {
            break;
        }
        bytes += size;
        out.push_back(e);
    }
    return out;
}

Status RaftLog::Validate(const std::vector<LogEntry>& entries) const {
    uint64_t expect_index = last_index() + 1;
    uint64_t min_term = last_term();
    for (const LogEntry& e : entries) {
        if (e.index != expect_index) {
            return Status::InvalidArgument("raft log append expected index " +
                                           std::to_string(expect_index) + ", got " +
                                           std::to_string(e.index));
        }
        if (e.term == 0 || e.term < min_term) {
            return Status::InvalidArgument("raft log entry " + std::to_string(e.index) +
                                           " has term " + std::to_string(e.term) +
                                           " below the log's " + std::to_string(min_term));
        }
        if (EncodedSize(e) > kMaxLogEntryBytes) {
            return Status::InvalidArgument("raft log entry too large");
        }
        ++expect_index;
        min_term = e.term;
    }
    return Status::Ok();
}

Status RaftLog::Sync() {
    if (sync_ && DurableSync(fd_) != 0) {
        return ErrnoStatus("sync raft log", path_);
    }
    return Status::Ok();
}

Status RaftLog::Append(const std::vector<LogEntry>& entries) {
    if (entries.empty()) {
        return Status::Ok();
    }
    Status s = Validate(entries);
    if (!s.ok()) {
        return s;
    }

    std::string buf;
    std::vector<uint64_t> offsets;
    offsets.reserve(entries.size());
    for (const LogEntry& e : entries) {
        offsets.push_back(file_size_ + buf.size());
        buf += EncodeLogFrame(e);
    }

    s = WriteAll(fd_, buf.data(), buf.size(), path_);
    if (s.ok()) {
        s = Sync();
    }
    if (!s.ok()) {
        // Drop whatever part of the batch reached the file so the file keeps
        // matching memory; if that fails too, the next Open cuts it off.
        (void)::ftruncate(fd_, static_cast<off_t>(file_size_));
        return s;
    }

    file_size_ += buf.size();
    offsets_.insert(offsets_.end(), offsets.begin(), offsets.end());
    entries_.insert(entries_.end(), entries.begin(), entries.end());
    return Status::Ok();
}

Status RaftLog::TruncateFrom(uint64_t index) {
    if (index == 0) {
        return Status::InvalidArgument("raft log indexes start at 1");
    }
    if (index > last_index()) {
        return Status::Ok();
    }
    const uint64_t new_size = offsets_[index - 1];
    if (::ftruncate(fd_, static_cast<off_t>(new_size)) != 0) {
        return ErrnoStatus("truncate raft log", path_);
    }
    // Memory follows the file even if the sync below fails: the file is
    // already shorter.
    file_size_ = new_size;
    entries_.resize(index - 1);
    offsets_.resize(index - 1);
    return Sync();
}

Status RaftLog::AppendFromLeader(const std::vector<LogEntry>& entries, uint64_t* last_new_index) {
    *last_new_index = entries.empty() ? 0 : entries.back().index;
    if (entries.empty()) {
        return Status::Ok();
    }
    if (entries.front().index == 0 || entries.front().index > last_index() + 1) {
        return Status::InvalidArgument("leader entries start at " +
                                       std::to_string(entries.front().index) +
                                       ", beyond the end of the log at " +
                                       std::to_string(last_index()));
    }

    size_t first_new = 0;
    for (; first_new < entries.size(); ++first_new) {
        const LogEntry& e = entries[first_new];
        if (e.index > last_index()) {
            break;
        }
        if (At(e.index).term != e.term) {
            Status s = TruncateFrom(e.index);
            if (!s.ok()) return s;
            break;
        }
    }
    if (first_new == entries.size()) {
        return Status::Ok();
    }
    return Append(std::vector<LogEntry>(entries.begin() + static_cast<std::ptrdiff_t>(first_new),
                                        entries.end()));
}

}  // namespace distkv::raft

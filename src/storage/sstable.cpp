#include "storage/sstable.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <cstdio>

#include "common/coding.h"
#include "common/crc32.h"
#include "common/file_util.h"
#include "storage/bloom_filter.h"

namespace distkv {
namespace {

constexpr size_t kCrcSize = 4;

void AppendCrc(std::string* block) { PutFixed32(block, Crc32(*block)); }

// Splits `raw` into contents and checks the trailing CRC.
bool CheckCrc(std::string_view raw, std::string_view* contents) {
    if (raw.size() < kCrcSize) {
        return false;
    }
    *contents = raw.substr(0, raw.size() - kCrcSize);
    return Crc32(*contents) == DecodeFixed32(raw.data() + contents->size());
}

struct ParsedEntry {
    std::string_view key;
    uint64_t seq;
    EntryType type;
    std::string_view value;
};

// Reads one entry from the front of `*in`. Returns false on malformed input.
bool ParseEntry(std::string_view* in, ParsedEntry* out) {
    if (!GetLengthPrefixed(in, &out->key) || in->size() < 8 + 1) {
        return false;
    }
    out->seq = DecodeFixed64(in->data());
    const auto type = static_cast<uint8_t>((*in)[8]);
    if (type != static_cast<uint8_t>(EntryType::kValue) &&
        type != static_cast<uint8_t>(EntryType::kTombstone)) {
        return false;
    }
    out->type = static_cast<EntryType>(type);
    in->remove_prefix(8 + 1);
    return GetLengthPrefixed(in, &out->value);
}

}  // namespace

// ---------------------------------------------------------------------------
// Writer

SSTableWriter::SSTableWriter(int fd, std::string path, std::string tmp_path,
                             SSTableOptions options)
    : fd_(fd),
      path_(std::move(path)),
      tmp_path_(std::move(tmp_path)),
      options_(options),
      bloom_(options.bloom_bits_per_key) {}

Status SSTableWriter::Create(const std::string& path, const SSTableOptions& options,
                             std::unique_ptr<SSTableWriter>* out) {
    if (options.block_size == 0) {
        return Status::InvalidArgument("block_size must be positive");
    }
    std::string tmp_path = path + ".tmp";
    const int fd = ::open(tmp_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
    if (fd < 0) {
        return ErrnoStatus("create sstable", tmp_path);
    }
    out->reset(new SSTableWriter(fd, path, std::move(tmp_path), options));
    return Status::Ok();
}

SSTableWriter::~SSTableWriter() {
    if (fd_ >= 0) {
        ::close(fd_);
    }
    if (!finished_) {
        std::remove(tmp_path_.c_str());
    }
}

Status SSTableWriter::Write(const std::string& bytes) {
    Status s = WriteAll(fd_, bytes.data(), bytes.size(), tmp_path_);
    if (!s.ok()) {
        failed_ = true;
        return s;
    }
    offset_ += bytes.size();
    return Status::Ok();
}

Status SSTableWriter::Add(std::string_view key, uint64_t seq, EntryType type,
                          std::string_view value) {
    if (finished_ || failed_) {
        return Status::InvalidArgument("sstable writer is no longer accepting entries");
    }
    if (entry_count_ > 0 && key <= std::string_view(last_key_)) {
        return Status::InvalidArgument("sstable keys must be added in strictly increasing order");
    }
    if (type == EntryType::kTombstone && !value.empty()) {
        return Status::InvalidArgument("tombstone must not carry a value");
    }
    if (key.size() > UINT32_MAX || value.size() > UINT32_MAX) {
        return Status::InvalidArgument("key or value too large");
    }

    if (entry_count_ == 0) {
        smallest_key_.assign(key.data(), key.size());
    }
    bloom_.AddKey(key);
    PutLengthPrefixed(&block_, key);
    PutFixed64(&block_, seq);
    block_.push_back(static_cast<char>(type));
    PutLengthPrefixed(&block_, value);

    last_key_.assign(key.data(), key.size());
    ++entry_count_;
    max_seq_ = std::max(max_seq_, seq);

    if (block_.size() >= options_.block_size) {
        return FlushBlock();
    }
    return Status::Ok();
}

Status SSTableWriter::FlushBlock() {
    if (block_.empty()) {
        return Status::Ok();
    }
    const uint64_t block_offset = offset_;
    const auto contents_size = static_cast<uint32_t>(block_.size());

    AppendCrc(&block_);
    Status s = Write(block_);
    if (!s.ok()) {
        return s;
    }
    block_.clear();

    // last_key_ is the last key of the block just written.
    PutLengthPrefixed(&index_, last_key_);
    PutFixed64(&index_, block_offset);
    PutFixed32(&index_, contents_size);
    return Status::Ok();
}

Status SSTableWriter::Finish() {
    if (finished_ || failed_) {
        return Status::InvalidArgument("sstable writer already finished or failed");
    }
    Status s = FlushBlock();
    if (!s.ok()) {
        return s;
    }

    std::string filter = bloom_.Finish();
    const uint64_t filter_offset = offset_;
    const auto filter_size = static_cast<uint32_t>(filter.size());
    AppendCrc(&filter);
    s = Write(filter);
    if (!s.ok()) {
        return s;
    }

    std::string index;
    PutLengthPrefixed(&index, smallest_key_);
    index += index_;
    const uint64_t index_offset = offset_;
    const auto index_size = static_cast<uint32_t>(index.size());
    AppendCrc(&index);
    s = Write(index);
    if (!s.ok()) {
        return s;
    }

    std::string footer;
    PutFixed64(&footer, index_offset);
    PutFixed32(&footer, index_size);
    PutFixed64(&footer, filter_offset);
    PutFixed32(&footer, filter_size);
    PutFixed64(&footer, entry_count_);
    PutFixed64(&footer, max_seq_);
    AppendCrc(&footer);
    PutFixed64(&footer, kSSTableMagic);
    s = Write(footer);
    if (!s.ok()) {
        return s;
    }

    // Make the contents durable before the rename publishes them, then make
    // the rename itself durable.
    if (DurableSync(fd_) != 0) {
        failed_ = true;
        return ErrnoStatus("sync sstable", tmp_path_);
    }
    ::close(fd_);
    fd_ = -1;
    if (std::rename(tmp_path_.c_str(), path_.c_str()) != 0) {
        failed_ = true;
        return ErrnoStatus("rename sstable to", path_);
    }
    finished_ = true;
    return SyncParentDir(path_);
}

// ---------------------------------------------------------------------------
// Reader

SSTableReader::~SSTableReader() {
    if (fd_ >= 0) {
        ::close(fd_);
    }
}

Status SSTableReader::Open(const std::string& path, std::unique_ptr<SSTableReader>* out) {
    const int fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
    if (fd < 0) {
        return ErrnoStatus("open sstable", path);
    }
    // Owns fd from here on, so every early return closes it.
    std::unique_ptr<SSTableReader> reader(new SSTableReader(fd, path));

    struct stat st {};
    if (::fstat(fd, &st) != 0) {
        return ErrnoStatus("stat sstable", path);
    }
    const auto file_size = static_cast<uint64_t>(st.st_size);
    if (file_size < kSSTableFooterSize) {
        return Status::Corruption("sstable too small: " + path);
    }

    std::string footer;
    Status s = ReadAt(fd, file_size - kSSTableFooterSize, kSSTableFooterSize, &footer, path);
    if (!s.ok()) {
        return s;
    }
    constexpr size_t kChecked = kSSTableFooterSize - 8;  // everything but the magic
    if (DecodeFixed64(footer.data() + kChecked) != kSSTableMagic) {
        return Status::Corruption("bad sstable magic: " + path);
    }
    std::string_view footer_contents;
    if (!CheckCrc(std::string_view(footer).substr(0, kChecked), &footer_contents)) {
        return Status::Corruption("sstable footer checksum mismatch: " + path);
    }
    const uint64_t index_offset = DecodeFixed64(footer.data());
    const uint32_t index_size = DecodeFixed32(footer.data() + 8);
    const uint64_t filter_offset = DecodeFixed64(footer.data() + 12);
    const uint32_t filter_size = DecodeFixed32(footer.data() + 20);
    reader->entry_count_ = DecodeFixed64(footer.data() + 24);
    reader->max_seq_ = DecodeFixed64(footer.data() + 32);

    // Layout is data | filter | index | footer; check each region fits.
    if (index_offset > file_size - kSSTableFooterSize ||
        index_size + kCrcSize > file_size - kSSTableFooterSize - index_offset) {
        return Status::Corruption("sstable index out of bounds: " + path);
    }
    if (filter_offset > index_offset || filter_size + kCrcSize > index_offset - filter_offset) {
        return Status::Corruption("sstable filter out of bounds: " + path);
    }

    std::string raw_filter;
    s = ReadAt(fd, filter_offset, filter_size + kCrcSize, &raw_filter, path);
    if (!s.ok()) {
        return s;
    }
    std::string_view filter;
    if (!CheckCrc(raw_filter, &filter)) {
        return Status::Corruption("sstable filter checksum mismatch: " + path);
    }
    reader->filter_.assign(filter.data(), filter.size());

    std::string raw_index;
    s = ReadAt(fd, index_offset, index_size + kCrcSize, &raw_index, path);
    if (!s.ok()) {
        return s;
    }
    std::string_view index;
    if (!CheckCrc(raw_index, &index)) {
        return Status::Corruption("sstable index checksum mismatch: " + path);
    }

    std::string_view smallest;
    if (!GetLengthPrefixed(&index, &smallest)) {
        return Status::Corruption("malformed sstable index: " + path);
    }
    reader->smallest_key_.assign(smallest.data(), smallest.size());

    while (!index.empty()) {
        std::string_view last_key;
        if (!GetLengthPrefixed(&index, &last_key) || index.size() < 8 + 4) {
            return Status::Corruption("malformed sstable index: " + path);
        }
        BlockHandle h{std::string(last_key), DecodeFixed64(index.data()),
                      DecodeFixed32(index.data() + 8)};
        index.remove_prefix(8 + 4);
        if (h.offset > filter_offset || h.size + kCrcSize > filter_offset - h.offset) {
            return Status::Corruption("sstable block out of bounds: " + path);
        }
        reader->blocks_.push_back(std::move(h));
    }

    *out = std::move(reader);
    return Status::Ok();
}

const std::string& SSTableReader::largest_key() const {
    static const std::string kEmpty;
    return blocks_.empty() ? kEmpty : blocks_.back().last_key;
}

Status SSTableReader::ReadBlock(const BlockHandle& handle, std::string* contents) const {
    Status s = ReadAt(fd_, handle.offset, handle.size + kCrcSize, contents, path_);
    if (!s.ok()) {
        return s;
    }
    std::string_view checked;
    if (!CheckCrc(*contents, &checked)) {
        return Status::Corruption("sstable block checksum mismatch at offset " +
                                  std::to_string(handle.offset) + " in " + path_);
    }
    contents->resize(checked.size());
    return Status::Ok();
}

Status SSTableReader::Get(std::string_view key, LookupResult* result, std::string* value,
                          uint64_t* seq) const {
    *result = LookupResult::kNotPresent;
    if (blocks_.empty() || key < std::string_view(smallest_key_) ||
        key > std::string_view(blocks_.back().last_key)) {
        return Status::Ok();  // outside the table's key range: no disk read
    }
    if (!BloomMayContain(filter_, key)) {
        bloom_skips_.fetch_add(1, std::memory_order_relaxed);
        return Status::Ok();  // definitely absent: no disk read
    }

    SSTableIterator it(this);
    it.Seek(key);
    if (!it.status().ok()) {
        return it.status();
    }
    if (!it.Valid() || it.key() != key) {
        return Status::Ok();
    }
    if (seq != nullptr) {
        *seq = it.seq();
    }
    if (it.type() == EntryType::kTombstone) {
        *result = LookupResult::kDeleted;
    } else {
        value->assign(it.value().data(), it.value().size());
        *result = LookupResult::kFound;
    }
    return Status::Ok();
}

// ---------------------------------------------------------------------------
// Iterator

bool SSTableIterator::LoadBlock(size_t index) {
    block_index_ = index;
    rest_ = {};
    if (index >= table_->blocks_.size()) {
        valid_ = false;
        return false;
    }
    Status s = table_->ReadBlock(table_->blocks_[index], &block_);
    if (!s.ok()) {
        status_ = std::move(s);
        valid_ = false;
        return false;
    }
    rest_ = block_;
    return true;
}

void SSTableIterator::Advance() {
    while (rest_.empty()) {
        if (!LoadBlock(block_index_ + 1)) {
            return;
        }
    }
    ParsedEntry e{};
    if (!ParseEntry(&rest_, &e)) {
        status_ = Status::Corruption("malformed sstable entry in " + table_->path_);
        valid_ = false;
        return;
    }
    key_ = e.key;
    seq_ = e.seq;
    type_ = e.type;
    value_ = e.value;
    valid_ = true;
}

void SSTableIterator::SeekToFirst() {
    status_ = Status::Ok();
    valid_ = false;
    if (LoadBlock(0)) {
        Advance();
    }
}

void SSTableIterator::Seek(std::string_view target) {
    status_ = Status::Ok();
    valid_ = false;

    // The first block whose last key is >= target is the first that can hold
    // an entry >= target; every earlier block ends below it.
    const auto& blocks = table_->blocks_;
    const auto it = std::lower_bound(blocks.begin(), blocks.end(), target,
                                     [](const SSTableReader::BlockHandle& h, std::string_view k) {
                                         return std::string_view(h.last_key) < k;
                                     });
    if (!LoadBlock(static_cast<size_t>(it - blocks.begin()))) {
        return;
    }
    Advance();
    while (valid_ && key_ < target) {
        Advance();
    }
}

void SSTableIterator::Next() {
    if (valid_) {
        Advance();
    }
}

}  // namespace distkv

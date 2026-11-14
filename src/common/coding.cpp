#include "common/coding.h"

namespace distkv {

void PutFixed32(std::string* dst, uint32_t value) {
    char buf[4];
    for (int i = 0; i < 4; ++i) {
        buf[i] = static_cast<char>((value >> (8 * i)) & 0xFFu);
    }
    dst->append(buf, sizeof(buf));
}

void PutFixed64(std::string* dst, uint64_t value) {
    char buf[8];
    for (int i = 0; i < 8; ++i) {
        buf[i] = static_cast<char>((value >> (8 * i)) & 0xFFu);
    }
    dst->append(buf, sizeof(buf));
}

uint32_t DecodeFixed32(const char* src) {
    uint32_t value = 0;
    for (int i = 0; i < 4; ++i) {
        value |= static_cast<uint32_t>(static_cast<unsigned char>(src[i])) << (8 * i);
    }
    return value;
}

uint64_t DecodeFixed64(const char* src) {
    uint64_t value = 0;
    for (int i = 0; i < 8; ++i) {
        value |= static_cast<uint64_t>(static_cast<unsigned char>(src[i])) << (8 * i);
    }
    return value;
}

void PutLengthPrefixed(std::string* dst, std::string_view value) {
    PutFixed32(dst, static_cast<uint32_t>(value.size()));
    dst->append(value.data(), value.size());
}

bool GetLengthPrefixed(std::string_view* input, std::string_view* value) {
    if (input->size() < 4) {
        return false;
    }
    const uint32_t len = DecodeFixed32(input->data());
    if (input->size() - 4 < len) {
        return false;
    }
    *value = input->substr(4, len);
    input->remove_prefix(4 + static_cast<size_t>(len));
    return true;
}

}  // namespace distkv

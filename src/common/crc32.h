#pragma once

#include <cstddef>
#include <cstdint>
#include <string_view>

namespace distkv {

// CRC-32 (IEEE 802.3, reflected polynomial 0xEDB88320), the same checksum
// zlib and PNG use. Pass a previous result as `crc` to continue over more data.
uint32_t Crc32(const char* data, size_t n, uint32_t crc = 0);

inline uint32_t Crc32(std::string_view data, uint32_t crc = 0) {
    return Crc32(data.data(), data.size(), crc);
}

}  // namespace distkv

#include "common/crc32.h"

#include <array>

namespace distkv {
namespace {

constexpr std::array<uint32_t, 256> MakeTable() {
    std::array<uint32_t, 256> table{};
    for (uint32_t i = 0; i < 256; ++i) {
        uint32_t c = i;
        for (int bit = 0; bit < 8; ++bit) {
            c = (c & 1u) ? (0xEDB88320u ^ (c >> 1)) : (c >> 1);
        }
        table[i] = c;
    }
    return table;
}

constexpr std::array<uint32_t, 256> kTable = MakeTable();

}  // namespace

uint32_t Crc32(const char* data, size_t n, uint32_t crc) {
    crc = ~crc;
    for (size_t i = 0; i < n; ++i) {
        const auto byte = static_cast<unsigned char>(data[i]);
        crc = kTable[(crc ^ byte) & 0xFFu] ^ (crc >> 8);
    }
    return ~crc;
}

}  // namespace distkv

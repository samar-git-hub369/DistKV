#include "storage/bloom_filter.h"

#include <algorithm>
#include <cmath>

#include "common/hash.h"

namespace distkv {
namespace {

constexpr uint32_t kMaxProbes = 30;
constexpr size_t kMinBits = 64;

}  // namespace

BloomFilterBuilder::BloomFilterBuilder(int bits_per_key) : bits_per_key_(bits_per_key) {}

void BloomFilterBuilder::AddKey(std::string_view key) { hashes_.push_back(Hash64(key)); }

std::string BloomFilterBuilder::Finish() const {
    if (bits_per_key_ <= 0 || hashes_.empty()) {
        return {};
    }

    // k = bits_per_key * ln 2 minimises the false-positive rate.
    const auto k = static_cast<uint32_t>(std::clamp(
        std::lround(bits_per_key_ * 0.69314718056), 1L, static_cast<long>(kMaxProbes)));

    size_t bits = std::max(kMinBits, hashes_.size() * static_cast<size_t>(bits_per_key_));
    const size_t bytes = (bits + 7) / 8;
    bits = bytes * 8;

    std::string filter(bytes, '\0');
    for (const uint64_t h : hashes_) {
        const auto h1 = static_cast<uint32_t>(h);
        const auto h2 = static_cast<uint32_t>(h >> 32) | 1u;  // odd, so probes never repeat early
        for (uint32_t i = 0; i < k; ++i) {
            const uint64_t bit = (h1 + static_cast<uint64_t>(i) * h2) % bits;
            filter[bit / 8] = static_cast<char>(static_cast<unsigned char>(filter[bit / 8]) |
                                                (1u << (bit % 8)));
        }
    }
    filter.push_back(static_cast<char>(k));
    return filter;
}

bool BloomMayContain(std::string_view filter, std::string_view key) {
    if (filter.size() < 2) {
        return true;
    }
    const auto k = static_cast<unsigned char>(filter.back());
    if (k == 0 || k > kMaxProbes) {
        return true;  // unknown encoding: never claim absence
    }
    const uint64_t bits = (filter.size() - 1) * 8;

    const uint64_t h = Hash64(key);
    const auto h1 = static_cast<uint32_t>(h);
    const auto h2 = static_cast<uint32_t>(h >> 32) | 1u;
    for (uint32_t i = 0; i < k; ++i) {
        const uint64_t bit = (h1 + static_cast<uint64_t>(i) * h2) % bits;
        if ((static_cast<unsigned char>(filter[bit / 8]) & (1u << (bit % 8))) == 0) {
            return false;
        }
    }
    return true;
}

}  // namespace distkv

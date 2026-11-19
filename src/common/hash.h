#pragma once

#include <cstdint>
#include <string_view>

namespace distkv {

// 64-bit hash of `data`: FNV-1a over the bytes, then a splitmix64 finalizer.
// FNV-1a alone mixes its high bits poorly for short keys; the finalizer spreads
// every input bit across the whole result, which the Bloom filter relies on
// when it splits the hash into two halves. Stable across platforms and runs,
// so it can be persisted.
inline uint64_t Hash64(std::string_view data) {
    uint64_t h = 0xcbf29ce484222325ull;
    for (const char c : data) {
        h ^= static_cast<unsigned char>(c);
        h *= 0x100000001b3ull;
    }
    h ^= h >> 30;
    h *= 0xbf58476d1ce4e5b9ull;
    h ^= h >> 27;
    h *= 0x94d049bb133111ebull;
    h ^= h >> 31;
    return h;
}

}  // namespace distkv

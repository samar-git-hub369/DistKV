#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

// Bloom filter: a compact bit array that answers "is this key possibly in the
// set?" A "no" is always right; a "yes" is wrong with a small probability
// (about 1% at 10 bits per key). Each SSTable stores one over its keys, so a
// lookup for a missing key usually skips the table without reading a block.
//
// Encoding: bit array (little-endian bit order within each byte) followed by
// one byte holding the number of probes k.
//
// Probes use double hashing: the 64-bit key hash is split into h1 and h2 and
// probe i tests bit (h1 + i * h2) mod m. Two hashes stand in for k independent
// ones with no measurable loss in accuracy.
namespace distkv {

class BloomFilterBuilder {
  public:
    // bits_per_key trades space for accuracy; 10 gives ~1% false positives.
    explicit BloomFilterBuilder(int bits_per_key);

    void AddKey(std::string_view key);
    size_t key_count() const { return hashes_.size(); }

    // Encodes the filter over every key added. With no keys or bits_per_key
    // <= 0 this returns an empty string, which BloomMayContain treats as
    // "anything may be present".
    std::string Finish() const;

  private:
    int bits_per_key_;
    std::vector<uint64_t> hashes_;
};

// False only if `key` was definitely not added to the filter that produced
// `filter`. An empty or malformed filter answers true, so a damaged filter can
// cost an extra read but never hide a key.
bool BloomMayContain(std::string_view filter, std::string_view key);

}  // namespace distkv

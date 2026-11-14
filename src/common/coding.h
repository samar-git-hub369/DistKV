#pragma once

#include <cstdint>
#include <string>
#include <string_view>

// Fixed-width little-endian encoding, independent of the host byte order, so
// files written on one machine can be read on another.
namespace distkv {

void PutFixed32(std::string* dst, uint32_t value);
void PutFixed64(std::string* dst, uint64_t value);

// Caller guarantees at least 4 / 8 readable bytes at `src`.
uint32_t DecodeFixed32(const char* src);
uint64_t DecodeFixed64(const char* src);

// Appends a 32-bit length followed by the bytes of `value`.
void PutLengthPrefixed(std::string* dst, std::string_view value);

// Reads a value written by PutLengthPrefixed from the front of `*input` and
// advances it. Returns false if `*input` is too short.
bool GetLengthPrefixed(std::string_view* input, std::string_view* value);

}  // namespace distkv

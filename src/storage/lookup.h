#pragma once

namespace distkv {

// Outcome of looking a key up in one layer of the LSM tree (the MemTable or
// one SSTable). Reads go newest layer first and stop at the first layer that
// returns anything other than kNotPresent.
enum class LookupResult {
    kFound,       // live value returned
    kDeleted,     // tombstone: the key is deleted, do not search older layers
    kNotPresent,  // this layer knows nothing about the key
};

}  // namespace distkv

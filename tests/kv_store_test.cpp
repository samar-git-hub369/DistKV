#include "storage/kv_store.hpp"

#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <cstdint>
#include <latch>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "common/log_entry.hpp"
#include "replication/local_replicator.hpp"

namespace distkv {
namespace {

// Applies commands directly with consecutive indexes, bypassing a Replicator.
class KvStoreTest : public ::testing::Test {
protected:
    void apply(Command command) { store_.apply(LogEntry{next_index_++, 1, std::move(command)}); }

    KvStore store_;
    std::uint64_t next_index_{1};
};

TEST_F(KvStoreTest, StartsEmpty) {
    EXPECT_EQ(store_.size(), 0U);
    EXPECT_EQ(store_.last_applied_index(), 0U);
    EXPECT_EQ(store_.get("missing"), std::nullopt);
}

TEST_F(KvStoreTest, SetThenGet) {
    apply(Command::set("k", "v"));
    EXPECT_EQ(store_.get("k"), "v");
    EXPECT_EQ(store_.size(), 1U);
}

TEST_F(KvStoreTest, SetOverwrites) {
    apply(Command::set("k", "v1"));
    apply(Command::set("k", "v2"));
    EXPECT_EQ(store_.get("k"), "v2");
    EXPECT_EQ(store_.size(), 1U);
}

TEST_F(KvStoreTest, DeleteRemovesKey) {
    apply(Command::set("k", "v"));
    apply(Command::del("k"));
    EXPECT_EQ(store_.get("k"), std::nullopt);
    EXPECT_EQ(store_.size(), 0U);
}

TEST_F(KvStoreTest, DeleteMissingKeyIsNotAnError) {
    apply(Command::set("other", "v"));
    apply(Command::del("missing"));
    EXPECT_EQ(store_.size(), 1U);
    EXPECT_EQ(store_.last_applied_index(), 2U);  // Still consumes its index.
}

TEST_F(KvStoreTest, GetMissingKeyReturnsNullopt) {
    apply(Command::set("a", "1"));
    EXPECT_EQ(store_.get("b"), std::nullopt);
    EXPECT_EQ(store_.get(""), std::nullopt);
}

TEST_F(KvStoreTest, EmptyValueIsStoredAndDistinctFromMissing) {
    apply(Command::set("k", ""));
    const std::optional<std::string> value = store_.get("k");
    ASSERT_TRUE(value.has_value());
    EXPECT_TRUE(value->empty());
    EXPECT_EQ(store_.size(), 1U);
}

TEST_F(KvStoreTest, LastAppliedIndexTracksEachEntry) {
    for (std::uint64_t i = 1; i <= 3; ++i) {
        apply(Command::set("k", std::to_string(i)));
        EXPECT_EQ(store_.last_applied_index(), i);
    }
}

TEST_F(KvStoreTest, DuplicateAndOldIndexesAreSkipped) {
    store_.apply(LogEntry{1, 1, Command::set("k", "a")});
    store_.apply(LogEntry{2, 1, Command::set("k", "b")});

    store_.apply(LogEntry{2, 1, Command::del("k")});          // Duplicate of the latest.
    store_.apply(LogEntry{1, 1, Command::set("k", "old")});   // Older entry.
    store_.apply(LogEntry{0, 1, Command::set("z", "zero")});  // Index 0 is never valid.

    EXPECT_EQ(store_.get("k"), "b");
    EXPECT_EQ(store_.get("z"), std::nullopt);
    EXPECT_EQ(store_.last_applied_index(), 2U);

    // Replay of the whole prefix followed by the next entry still works.
    store_.apply(LogEntry{1, 1, Command::set("k", "a")});
    store_.apply(LogEntry{2, 1, Command::set("k", "b")});
    store_.apply(LogEntry{3, 1, Command::set("k", "c")});
    EXPECT_EQ(store_.get("k"), "c");
    EXPECT_EQ(store_.last_applied_index(), 3U);
}

TEST_F(KvStoreTest, BinarySafeKeysAndValues) {
    const std::string key("k\0ey", 4);
    const std::string value("\0\x01\xff\xfe\x80 \0end", 11);
    ASSERT_EQ(value.size(), 11U);
    apply(Command::set(key, value));

    const std::optional<std::string> got = store_.get(key);
    ASSERT_TRUE(got.has_value());
    EXPECT_EQ(got->size(), value.size());
    EXPECT_EQ(*got, value);
    // The embedded '\0' is part of the key, not a terminator.
    EXPECT_EQ(store_.get("k"), std::nullopt);
}

TEST_F(KvStoreTest, GetWithNonNullTerminatedStringView) {
    apply(Command::set("abc", "v"));
    const std::array<char, 4> buffer{'a', 'b', 'c', 'X'};  // No terminator.
    EXPECT_EQ(store_.get(std::string_view(buffer.data(), 3)), "v");
    EXPECT_EQ(store_.get(std::string_view(buffer.data(), 2)), std::nullopt);
    EXPECT_EQ(store_.get(std::string_view(buffer.data(), 4)), std::nullopt);
}

// "threadsafe" re-executes the test binary for the death test instead of
// forking a possibly multi-threaded process, which is reliable under TSan.
TEST(KvStoreDeathTest, GapInIndexesAborts) {
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_DEATH(
        {
            KvStore store;
            store.apply(LogEntry{1, 1, Command::set("k", "v")});
            store.apply(LogEntry{3, 1, Command::set("k", "v")});
        },
        "log gap: expected index 2, got 3");
}

TEST(KvStoreDeathTest, FirstEntryMustBeIndexOne) {
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_DEATH(
        {
            KvStore store;
            store.apply(LogEntry{2, 1, Command::set("k", "v")});
        },
        "log gap: expected index 1, got 2");
}

// Writers propose through a real LocalReplicator while readers hammer get().
TEST(KvStoreConcurrencyTest, ConcurrentWritersAndReaders) {
    constexpr int kWriters = 4;
    constexpr int kReaders = 4;
    constexpr int kKeysPerWriter = 100;
    constexpr int kRounds = 20;

    KvStore store;
    LocalReplicator replicator(store);

    const auto key_for = [](int writer, int k) {
        return "w" + std::to_string(writer) + "-k" + std::to_string(k);
    };
    const auto value_for = [](int writer, int k, int round) {
        return "w" + std::to_string(writer) + "-k" + std::to_string(k) + "-r" +
               std::to_string(round);
    };

    std::atomic<bool> writers_done{false};
    std::atomic<int> failed_proposals{0};
    std::atomic<long> reads{0};

    // Writers start only after every reader has done one read, so reads > 0
    // holds even if the writers would otherwise finish before any reader is
    // scheduled.
    std::latch readers_started(kReaders);

    std::vector<std::thread> readers;
    readers.reserve(kReaders);
    for (int r = 0; r < kReaders; ++r) {
        readers.emplace_back([&, r] {
            long local_reads = 0;
            int k = r;
            const auto read_once = [&] {
                const int writer = k % kWriters;
                const int key = k % kKeysPerWriter;
                const std::string expected_prefix = key_for(writer, key) + "-r";
                if (const auto value = store.get(key_for(writer, key))) {
                    // Any value seen must be a complete write for this key.
                    EXPECT_EQ(value->rfind(expected_prefix, 0), 0U) << *value;
                }
                (void)store.size();
                (void)store.last_applied_index();
                ++local_reads;
                ++k;
            };
            read_once();
            readers_started.count_down();
            while (!writers_done.load(std::memory_order_acquire)) read_once();
            reads.fetch_add(local_reads);
        });
    }
    readers_started.wait();

    std::vector<std::thread> writers;
    writers.reserve(kWriters);
    for (int w = 0; w < kWriters; ++w) {
        writers.emplace_back([&, w] {
            const auto check = [&failed_proposals](const Status& status, std::uint64_t) {
                if (!status.is_ok()) failed_proposals.fetch_add(1);
            };
            for (int round = 0; round < kRounds; ++round) {
                for (int k = 0; k < kKeysPerWriter; ++k) {
                    replicator.propose(Command::set(key_for(w, k), value_for(w, k, round)), check);
                }
            }
            // Finally delete every odd key so size() has something to check.
            for (int k = 1; k < kKeysPerWriter; k += 2) {
                replicator.propose(Command::del(key_for(w, k)), check);
            }
        });
    }

    for (auto& t : writers) t.join();
    writers_done.store(true, std::memory_order_release);
    for (auto& t : readers) t.join();

    EXPECT_EQ(failed_proposals.load(), 0);
    EXPECT_GT(reads.load(), 0);

    constexpr std::uint64_t kExpectedEntries =
        std::uint64_t{kWriters} * (kKeysPerWriter * kRounds + kKeysPerWriter / 2);
    EXPECT_EQ(store.last_applied_index(), kExpectedEntries);
    EXPECT_EQ(replicator.commit_index(), kExpectedEntries);
    EXPECT_EQ(store.size(), static_cast<std::size_t>(kWriters * (kKeysPerWriter / 2)));

    for (int w = 0; w < kWriters; ++w) {
        for (int k = 0; k < kKeysPerWriter; ++k) {
            if (k % 2 == 1) {
                EXPECT_EQ(store.get(key_for(w, k)), std::nullopt) << key_for(w, k);
            } else {
                EXPECT_EQ(store.get(key_for(w, k)), value_for(w, k, kRounds - 1));
            }
        }
    }
}

}  // namespace
}  // namespace distkv

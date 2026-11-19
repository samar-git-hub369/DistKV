#include "storage/bloom_filter.h"

#include <gtest/gtest.h>

#include <string>

#include "common/hash.h"

namespace distkv {
namespace {

std::string Key(int i) { return "key:" + std::to_string(i); }

TEST(HashTest, IsStableAndSpreadsInput) {
    // Persisted filters depend on this exact function never changing.
    EXPECT_EQ(Hash64("distkv"), Hash64("distkv"));
    EXPECT_NE(Hash64("a"), Hash64("b"));
    EXPECT_NE(Hash64(""), Hash64(std::string(1, '\0')));
    // Keys differing in one low byte must still differ in the high half.
    EXPECT_NE(Hash64("key:1") >> 32, Hash64("key:2") >> 32);
}

TEST(BloomFilterTest, NeverReportsAnAddedKeyAsAbsent) {
    BloomFilterBuilder b(10);
    for (int i = 0; i < 10000; ++i) b.AddKey(Key(i));
    const std::string f = b.Finish();
    for (int i = 0; i < 10000; ++i) {
        ASSERT_TRUE(BloomMayContain(f, Key(i))) << i;
    }
}

TEST(BloomFilterTest, FalsePositiveRateNearOnePercentAtTenBitsPerKey) {
    BloomFilterBuilder b(10);
    for (int i = 0; i < 10000; ++i) b.AddKey(Key(i));
    const std::string f = b.Finish();

    int false_positives = 0;
    constexpr int kProbes = 100000;
    for (int i = 0; i < kProbes; ++i) {
        if (BloomMayContain(f, "absent:" + std::to_string(i))) ++false_positives;
    }
    const double rate = static_cast<double>(false_positives) / kProbes;
    EXPECT_LT(rate, 0.02) << "rate " << rate;  // theory says ~0.8%
}

TEST(BloomFilterTest, MoreBitsPerKeyMeansFewerFalsePositives) {
    auto rate_for = [](int bits_per_key) {
        BloomFilterBuilder b(bits_per_key);
        for (int i = 0; i < 5000; ++i) b.AddKey(Key(i));
        const std::string f = b.Finish();
        int fp = 0;
        for (int i = 0; i < 50000; ++i) {
            if (BloomMayContain(f, "x" + std::to_string(i))) ++fp;
        }
        return fp;
    };
    EXPECT_GT(rate_for(4), rate_for(10));
    EXPECT_GT(rate_for(10), rate_for(16));
}

TEST(BloomFilterTest, SizeIsAboutBitsPerKeyTimesKeys) {
    BloomFilterBuilder b(10);
    for (int i = 0; i < 8000; ++i) b.AddKey(Key(i));
    EXPECT_EQ(b.Finish().size(), 8000u * 10 / 8 + 1);  // bit array + k byte
}

TEST(BloomFilterTest, TinySetsStillGetUsefulFilter) {
    BloomFilterBuilder b(10);
    b.AddKey("only");
    const std::string f = b.Finish();
    EXPECT_EQ(f.size(), 64u / 8 + 1);  // minimum 64 bits
    EXPECT_TRUE(BloomMayContain(f, "only"));
    int fp = 0;
    for (int i = 0; i < 1000; ++i) fp += BloomMayContain(f, Key(i)) ? 1 : 0;
    EXPECT_LT(fp, 100);
}

TEST(BloomFilterTest, EmptyOrDisabledFilterAdmitsEverything) {
    EXPECT_TRUE(BloomFilterBuilder(10).Finish().empty());

    BloomFilterBuilder disabled(0);
    disabled.AddKey("k");
    const std::string f = disabled.Finish();
    EXPECT_TRUE(f.empty());
    EXPECT_TRUE(BloomMayContain(f, "anything"));
}

TEST(BloomFilterTest, MalformedFilterAdmitsEverything) {
    EXPECT_TRUE(BloomMayContain(std::string("\x00\x00\x00", 3), "k"));    // k = 0
    EXPECT_TRUE(BloomMayContain(std::string(8, '\0') + "\x7f", "k"));     // k too large
    EXPECT_TRUE(BloomMayContain("x", "k"));                               // too short
}

TEST(BloomFilterTest, BinaryKeys) {
    BloomFilterBuilder b(10);
    const std::string a("\0\0", 2);
    const std::string c("\xff\x00\xff", 3);
    b.AddKey(a);
    b.AddKey(c);
    b.AddKey("");
    const std::string f = b.Finish();
    EXPECT_TRUE(BloomMayContain(f, a));
    EXPECT_TRUE(BloomMayContain(f, c));
    EXPECT_TRUE(BloomMayContain(f, ""));
}

}  // namespace
}  // namespace distkv

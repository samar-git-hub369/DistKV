#include <gtest/gtest.h>

#include <string>
#include <string_view>

#include "common/coding.h"
#include "common/crc32.h"
#include "common/status.h"

namespace distkv {
namespace {

TEST(CodingTest, Fixed32IsLittleEndian) {
    std::string buf;
    PutFixed32(&buf, 0x04030201u);
    ASSERT_EQ(buf.size(), 4u);
    EXPECT_EQ(buf, std::string("\x01\x02\x03\x04", 4));
    EXPECT_EQ(DecodeFixed32(buf.data()), 0x04030201u);
}

TEST(CodingTest, Fixed64RoundTripsExtremes) {
    for (uint64_t v : {uint64_t{0}, uint64_t{1}, uint64_t{0xFFFFFFFFFFFFFFFF},
                       uint64_t{0x8000000000000000}, uint64_t{0x0123456789ABCDEF}}) {
        std::string buf;
        PutFixed64(&buf, v);
        ASSERT_EQ(buf.size(), 8u);
        EXPECT_EQ(DecodeFixed64(buf.data()), v);
    }
}

TEST(CodingTest, LengthPrefixedRoundTripsBinaryData) {
    const std::string binary("a\0b\xff", 4);
    std::string buf;
    PutLengthPrefixed(&buf, binary);
    PutLengthPrefixed(&buf, "");

    std::string_view in(buf);
    std::string_view first;
    std::string_view second;
    ASSERT_TRUE(GetLengthPrefixed(&in, &first));
    ASSERT_TRUE(GetLengthPrefixed(&in, &second));
    EXPECT_EQ(first, binary);
    EXPECT_TRUE(second.empty());
    EXPECT_TRUE(in.empty());
}

TEST(CodingTest, LengthPrefixedRejectsShortInput) {
    std::string buf;
    PutLengthPrefixed(&buf, "hello");
    std::string_view value;

    std::string_view truncated(buf.data(), buf.size() - 1);
    EXPECT_FALSE(GetLengthPrefixed(&truncated, &value));

    std::string_view no_length(buf.data(), 3);
    EXPECT_FALSE(GetLengthPrefixed(&no_length, &value));
}

TEST(Crc32Test, MatchesStandardCheckValue) {
    // The published check value for CRC-32/ISO-HDLC.
    EXPECT_EQ(Crc32("123456789"), 0xCBF43926u);
    EXPECT_EQ(Crc32(""), 0u);
}

TEST(Crc32Test, CanBeComputedIncrementally) {
    const std::string data = "the quick brown fox";
    const uint32_t whole = Crc32(data);
    const uint32_t split = Crc32(std::string_view(data).substr(7), Crc32(data.substr(0, 7)));
    EXPECT_EQ(whole, split);
}

TEST(StatusTest, CarriesCodeAndMessage) {
    EXPECT_TRUE(Status::Ok().ok());
    const Status s = Status::Corruption("bad frame");
    EXPECT_FALSE(s.ok());
    EXPECT_EQ(s.code(), StatusCode::kCorruption);
    EXPECT_EQ(s.message(), "bad frame");
    EXPECT_EQ(s.ToString(), "Corruption: bad frame");
}

}  // namespace
}  // namespace distkv

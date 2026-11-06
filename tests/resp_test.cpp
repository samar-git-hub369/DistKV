#include "protocol/resp.hpp"

#include <gtest/gtest.h>

#include <algorithm>
#include <cstddef>
#include <random>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

namespace distkv::resp {
namespace {

using namespace std::string_literals;
using Args = std::vector<std::string>;

// Expects `buffer` to start with a complete frame and returns it.
Complete expect_complete(std::string_view buffer) {
    ParseResult result = parse_request(buffer);
    if (auto* error = std::get_if<ProtocolError>(&result)) {
        ADD_FAILURE() << "protocol error: " << error->message;
        return {};
    }
    if (std::holds_alternative<Incomplete>(result)) {
        ADD_FAILURE() << "incomplete";
        return {};
    }
    return std::get<Complete>(std::move(result));
}

bool is_incomplete(std::string_view buffer) {
    return std::holds_alternative<Incomplete>(parse_request(buffer));
}

// Returns the error message, or "" (with a test failure) if there is none.
std::string expect_error(std::string_view buffer) {
    const ParseResult result = parse_request(buffer);
    if (const auto* error = std::get_if<ProtocolError>(&result)) return error->message;
    ADD_FAILURE() << "expected a protocol error for: " << buffer.substr(0, 64);
    return {};
}

// Feeds `stream` in chunks produced by `next_chunk`, draining every complete
// frame after each chunk, the way a connection does.
template <typename NextChunk>
std::vector<Args> parse_stream(std::string_view stream, NextChunk next_chunk) {
    std::vector<Args> commands;
    std::string buffer;
    std::size_t fed = 0;
    while (fed < stream.size()) {
        const std::size_t n = std::min(next_chunk(), stream.size() - fed);
        buffer.append(stream.substr(fed, n));
        fed += n;
        while (true) {
            ParseResult result = parse_request(buffer);
            if (std::holds_alternative<Incomplete>(result)) break;
            if (auto* error = std::get_if<ProtocolError>(&result)) {
                ADD_FAILURE() << error->message;
                return commands;
            }
            Complete& frame = std::get<Complete>(result);
            commands.push_back(std::move(frame.args));
            buffer.erase(0, frame.consumed);
        }
    }
    EXPECT_TRUE(buffer.empty()) << "leftover bytes: " << buffer.size();
    return commands;
}

// --- Arrays of bulk strings --------------------------------------------------

TEST(RespParserTest, ParsesArrayOfBulkStrings) {
    const std::string frame = "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$5\r\nvalue\r\n";
    const Complete c = expect_complete(frame);
    EXPECT_EQ(c.args, (Args{"SET", "k", "value"}));
    EXPECT_EQ(c.consumed, frame.size());
}

TEST(RespParserTest, ParsesRedisCliCommandProbe) {
    const Complete c = expect_complete("*2\r\n$7\r\nCOMMAND\r\n$4\r\nDOCS\r\n");
    EXPECT_EQ(c.args, (Args{"COMMAND", "DOCS"}));
}

TEST(RespParserTest, EmptyArrayIsANoOpFrame) {
    const Complete c = expect_complete("*0\r\n");
    EXPECT_TRUE(c.args.empty());
    EXPECT_EQ(c.consumed, 4U);
}

TEST(RespParserTest, EmptyBulkString) {
    const Complete c = expect_complete("*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$0\r\n\r\n");
    EXPECT_EQ(c.args, (Args{"SET", "k", ""}));
}

TEST(RespParserTest, BulkPayloadIsBinarySafe) {
    const std::string payload("a\r\nb\0c\r\n\xff", 9);
    const std::string frame = "*1\r\n$9\r\n" + payload + "\r\n";
    const Complete c = expect_complete(frame);
    ASSERT_EQ(c.args.size(), 1U);
    EXPECT_EQ(c.args[0], payload);
    EXPECT_EQ(c.consumed, frame.size());
}

TEST(RespParserTest, PipelinedFramesAreParsedOneAtATime) {
    const std::string first = "*1\r\n$4\r\nPING\r\n";
    const std::string second = "*2\r\n$3\r\nGET\r\n$1\r\nk\r\n";
    const std::string third = "PING\r\n";
    const std::string buffer = first + second + third;

    const Complete a = expect_complete(buffer);
    EXPECT_EQ(a.args, (Args{"PING"}));
    EXPECT_EQ(a.consumed, first.size());

    const Complete b = expect_complete(std::string_view(buffer).substr(a.consumed));
    EXPECT_EQ(b.args, (Args{"GET", "k"}));

    const Complete c = expect_complete(std::string_view(buffer).substr(a.consumed + b.consumed));
    EXPECT_EQ(c.args, (Args{"PING"}));
    EXPECT_EQ(a.consumed + b.consumed + c.consumed, buffer.size());
}

TEST(RespParserTest, EveryStrictPrefixIsIncomplete) {
    const std::string frame = "*2\r\n$3\r\nGET\r\n$10\r\nab\r\ncd\0efg\r\n"s;
    for (std::size_t n = 0; n < frame.size(); ++n) {
        EXPECT_TRUE(is_incomplete(std::string_view(frame).substr(0, n))) << "prefix " << n;
    }
    EXPECT_EQ(expect_complete(frame).consumed, frame.size());
}

TEST(RespParserTest, ByteAtATimeAndRandomChunksYieldIdenticalCommands) {
    std::string stream;
    std::vector<Args> expected;
    const auto add = [&](const Args& args) {
        stream += command(args);
        expected.push_back(args);
    };
    add({"PING"});
    add({"SET", "key", std::string("bin\r\n\0ary", 9)});
    add({"GET", "key"});
    add({"SET", "empty", ""});
    add({"DEL", "a", "b", "c"});
    add({"SET", "big", std::string(5000, 'x')});
    stream += "SET inline value\r\n";
    expected.push_back({"SET", "inline", "value"});
    add({"ECHO", "done"});

    const std::vector<Args> whole = parse_stream(stream, [&] { return stream.size(); });
    EXPECT_EQ(whole, expected);

    const std::vector<Args> bytewise = parse_stream(stream, [] { return std::size_t{1}; });
    EXPECT_EQ(bytewise, expected);

    std::mt19937 rng(12345);  // Fixed seed: deterministic chunking.
    std::uniform_int_distribution<std::size_t> chunk(1, 64);
    for (int round = 0; round < 20; ++round) {
        const std::vector<Args> random = parse_stream(stream, [&] { return chunk(rng); });
        EXPECT_EQ(random, expected) << "round " << round;
    }
}

// --- Inline commands ---------------------------------------------------------

TEST(RespParserTest, InlineCommand) {
    const Complete c = expect_complete("PING\r\n");
    EXPECT_EQ(c.args, (Args{"PING"}));
    EXPECT_EQ(c.consumed, 6U);
}

TEST(RespParserTest, InlineCommandSplitsOnWhitespace) {
    const Complete c = expect_complete("  SET \t a   b  \r\nrest");
    EXPECT_EQ(c.args, (Args{"SET", "a", "b"}));
    EXPECT_EQ(c.consumed, 17U);
}

TEST(RespParserTest, InlineCommandAcceptsBareNewline) {
    const Complete c = expect_complete("GET k\n");
    EXPECT_EQ(c.args, (Args{"GET", "k"}));
    EXPECT_EQ(c.consumed, 6U);
}

TEST(RespParserTest, BlankInlineLineIsANoOpFrame) {
    const Complete c = expect_complete("\r\nPING\r\n");
    EXPECT_TRUE(c.args.empty());
    EXPECT_EQ(c.consumed, 2U);
}

TEST(RespParserTest, InlineWithoutNewlineIsIncomplete) {
    EXPECT_TRUE(is_incomplete("SET a b"));
    EXPECT_TRUE(is_incomplete("SET a b\r"));
}

// --- Errors ------------------------------------------------------------------

TEST(RespParserTest, RejectsReplyTypeBytesAtFrameStart) {
    for (const char* frame : {"$3\r\nGET\r\n", "+OK\r\n", "-ERR x\r\n", ":1\r\n"}) {
        EXPECT_NE(expect_error(frame).find("unexpected type byte"), std::string::npos) << frame;
    }
}

TEST(RespParserTest, RejectsNonBulkArrayElement) {
    EXPECT_NE(expect_error("*1\r\n:5\r\n").find("expected '$'"), std::string::npos);
    EXPECT_NE(expect_error("*2\r\n$1\r\na\r\n+b\r\n").find("expected '$'"), std::string::npos);
}

TEST(RespParserTest, RejectsBadArrayLength) {
    EXPECT_NE(expect_error("*-1\r\n").find("invalid multibulk length"), std::string::npos);
    EXPECT_NE(expect_error("*abc\r\n").find("invalid multibulk length"), std::string::npos);
    EXPECT_NE(expect_error("*\r\n").find("invalid multibulk length"), std::string::npos);
    EXPECT_NE(expect_error("*+1\r\n").find("invalid multibulk length"), std::string::npos);
    EXPECT_NE(expect_error("*1x\r\n").find("invalid multibulk length"), std::string::npos);
    EXPECT_NE(expect_error("*99999999999999999999999\r\n").find("invalid multibulk length"),
              std::string::npos);
}

TEST(RespParserTest, RejectsBadBulkLength) {
    EXPECT_NE(expect_error("*1\r\n$-1\r\n").find("invalid bulk length"), std::string::npos);
    EXPECT_NE(expect_error("*1\r\n$x\r\n").find("invalid bulk length"), std::string::npos);
    EXPECT_NE(expect_error("*1\r\n$\r\n").find("invalid bulk length"), std::string::npos);
    EXPECT_NE(expect_error("*1\r\n$3 \r\nabc\r\n").find("invalid bulk length"),
              std::string::npos);
}

TEST(RespParserTest, RejectsMissingCrlfAfterBulk) {
    EXPECT_EQ(expect_error("*1\r\n$3\r\nabcX\r\n"), "expected CRLF after bulk payload");
    EXPECT_EQ(expect_error("*1\r\n$3\r\nabc\rX"), "expected CRLF after bulk payload");
    EXPECT_EQ(expect_error("*1\r\n$3\r\nabc\n\r"), "expected CRLF after bulk payload");
}

TEST(RespParserTest, LengthLineWithoutTerminatorIsBounded) {
    // A short unterminated length is just incomplete...
    EXPECT_TRUE(is_incomplete("*12"));
    EXPECT_TRUE(is_incomplete("*1\r\n$12"));
    // ...but one longer than any valid length is an error, not a wait.
    EXPECT_NE(expect_error("*" + std::string(40, '1')).find("invalid multibulk length"),
              std::string::npos);
    EXPECT_NE(expect_error("*1\r\n$" + std::string(40, '1')).find("invalid bulk length"),
              std::string::npos);
}

// --- Limits ------------------------------------------------------------------

TEST(RespParserTest, EnforcesMaxArrayElements) {
    const std::string at_limit = "*" + std::to_string(kMaxArrayElements) + "\r\n";
    EXPECT_TRUE(is_incomplete(at_limit));
    const std::string over = "*" + std::to_string(kMaxArrayElements + 1) + "\r\n";
    EXPECT_EQ(expect_error(over), "too many elements in multibulk request");
}

TEST(RespParserTest, EnforcesMaxBulkLength) {
    const std::string at_limit = "*1\r\n$" + std::to_string(kMaxBulkLength) + "\r\n";
    EXPECT_TRUE(is_incomplete(at_limit));
    const Complete c =
        expect_complete(at_limit + std::string(kMaxBulkLength, 'x') + "\r\n");
    ASSERT_EQ(c.args.size(), 1U);
    EXPECT_EQ(c.args[0].size(), kMaxBulkLength);

    const std::string over = "*1\r\n$" + std::to_string(kMaxBulkLength + 1) + "\r\n";
    EXPECT_EQ(expect_error(over), "bulk length exceeds 1 MiB limit");
}

TEST(RespParserTest, EnforcesMaxRequestBytes) {
    // Nine maximal bulks exceed the 8 MiB total even though each is legal.
    std::string frame = "*9\r\n";
    const std::string header = "$" + std::to_string(kMaxBulkLength) + "\r\n";
    const std::string bulk = header + std::string(kMaxBulkLength, 'x') + "\r\n";
    for (int i = 0; i < 8; ++i) frame += bulk;
    frame += header;
    EXPECT_EQ(expect_error(frame), "request exceeds 8 MiB limit");
}

TEST(RespParserTest, EnforcesMaxInlineLength) {
    const std::string at_limit(kMaxInlineLength, 'a');
    EXPECT_TRUE(is_incomplete(at_limit));
    EXPECT_EQ(expect_complete(at_limit + "\r\n").args.size(), 1U);

    EXPECT_EQ(expect_error(std::string(kMaxInlineLength + 2, 'a')), "too big inline request");
    EXPECT_EQ(expect_error(std::string(kMaxInlineLength + 1, 'a') + "\r\n"),
              "too big inline request");
}

// --- Encoder -----------------------------------------------------------------

TEST(RespEncoderTest, ExactBytes) {
    EXPECT_EQ(simple_string("OK"), "+OK\r\n");
    EXPECT_EQ(error("ERR bad"), "-ERR bad\r\n");
    EXPECT_EQ(integer(0), ":0\r\n");
    EXPECT_EQ(integer(42), ":42\r\n");
    EXPECT_EQ(integer(-7), ":-7\r\n");
    EXPECT_EQ(bulk_string("hello"), "$5\r\nhello\r\n");
    EXPECT_EQ(bulk_string(""), "$0\r\n\r\n");
    EXPECT_EQ(bulk_string(std::string_view("a\0\r\n", 4)), std::string("$4\r\na\0\r\n\r\n", 10));
    EXPECT_EQ(null_bulk(), "$-1\r\n");
    EXPECT_EQ(array({}), "*0\r\n");
    EXPECT_EQ(array({integer(1), bulk_string("x"), null_bulk()}), "*3\r\n:1\r\n$1\r\nx\r\n$-1\r\n");
    EXPECT_EQ(command({"GET", "k"}), "*2\r\n$3\r\nGET\r\n$1\r\nk\r\n");
}

TEST(RespEncoderTest, SimpleStringsCannotBreakFraming) {
    EXPECT_EQ(simple_string("a\r\nb"), "+a  b\r\n");
    EXPECT_EQ(error("ERR x\ny"), "-ERR x y\r\n");
}

TEST(RespEncoderTest, CommandRoundTripsThroughParser) {
    const Args args{"SET", std::string("k\0", 2), std::string("\r\n\xfe", 3)};
    const std::string encoded = command(args);
    const Complete c = expect_complete(encoded);
    EXPECT_EQ(c.args, args);
    EXPECT_EQ(c.consumed, encoded.size());
}

}  // namespace
}  // namespace distkv::resp

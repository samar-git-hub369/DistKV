#include "protocol/text_protocol.h"

#include <gtest/gtest.h>

#include <string>

namespace distkv {
namespace {

Request ParseOk(std::string_view line) {
    Request r;
    std::string err;
    EXPECT_TRUE(ParseRequest(line, &r, &err)) << line << ": " << err;
    return r;
}

std::string ParseErr(std::string_view line) {
    Request r;
    std::string err;
    EXPECT_FALSE(ParseRequest(line, &r, &err)) << line;
    return err;
}

TEST(TextProtocolTest, ParsesEveryCommand) {
    Request r = ParseOk("PUT user:1 alice");
    EXPECT_EQ(r.type, CommandType::kPut);
    EXPECT_EQ(r.key, "user:1");
    EXPECT_EQ(r.value, "alice");

    r = ParseOk("GET user:1");
    EXPECT_EQ(r.type, CommandType::kGet);
    EXPECT_EQ(r.key, "user:1");

    EXPECT_EQ(ParseOk("DELETE k").type, CommandType::kDelete);
    EXPECT_EQ(ParseOk("DEL k").type, CommandType::kDelete);
    EXPECT_EQ(ParseOk("SET k v").type, CommandType::kPut);
    EXPECT_EQ(ParseOk("STATUS").type, CommandType::kStatus);
    EXPECT_EQ(ParseOk("PING").type, CommandType::kPing);
}

TEST(TextProtocolTest, CommandsAreCaseInsensitiveKeysAreNot) {
    Request r = ParseOk("pUt Key Value");
    EXPECT_EQ(r.type, CommandType::kPut);
    EXPECT_EQ(r.key, "Key");
    EXPECT_EQ(r.value, "Value");
    EXPECT_EQ(ParseOk("get K").key, "K");
}

TEST(TextProtocolTest, ValueKeepsSpacesAndAllowsEmpty) {
    EXPECT_EQ(ParseOk("PUT city New Delhi, India").value, "New Delhi, India");
    EXPECT_EQ(ParseOk("PUT k  two leading spaces").value, " two leading spaces");
    EXPECT_EQ(ParseOk("PUT k ").value, "");
}

TEST(TextProtocolTest, StripsCarriageReturn) {
    EXPECT_EQ(ParseOk("GET k\r").key, "k");
    EXPECT_EQ(ParseOk("PUT k v\r").value, "v");
    EXPECT_EQ(ParseOk("PING\r").type, CommandType::kPing);
}

TEST(TextProtocolTest, RejectsMalformedRequests) {
    EXPECT_EQ(ParseErr(""), "empty request");
    EXPECT_EQ(ParseErr("\r"), "empty request");
    EXPECT_EQ(ParseErr("FLY away"), "unknown command 'FLY'");
    EXPECT_EQ(ParseErr("GET"), "missing key");
    EXPECT_EQ(ParseErr("PUT"), "missing key");
    EXPECT_EQ(ParseErr("PUT k"), "missing value");
    EXPECT_EQ(ParseErr("GET a b"), "too many arguments");
    EXPECT_EQ(ParseErr("DELETE a b"), "too many arguments");
    EXPECT_EQ(ParseErr("PING now"), "PING takes no arguments");
    EXPECT_EQ(ParseErr(" GET k"), "empty request");  // leading space = empty command
}

TEST(TextProtocolTest, ResponsesAreSingleLines) {
    EXPECT_EQ(OkResponse(), "OK\n");
    EXPECT_EQ(ValueResponse("a b"), "VALUE a b\n");
    EXPECT_EQ(ValueResponse(""), "VALUE \n");
    EXPECT_EQ(NotFoundResponse(), "NOT_FOUND\n");
    EXPECT_EQ(PongResponse(), "PONG\n");
    EXPECT_EQ(ErrorResponse("bad\nthing\r"), "ERR bad thing \n");
}

}  // namespace
}  // namespace distkv

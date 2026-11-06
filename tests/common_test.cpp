#include <gtest/gtest.h>

#include "common/log_entry.hpp"
#include "common/status.hpp"

namespace distkv {
namespace {

TEST(CommandTest, SetFactory) {
    const Command cmd = Command::set("k", "v");
    EXPECT_EQ(cmd.op, OpType::kSet);
    EXPECT_EQ(cmd.key, "k");
    EXPECT_EQ(cmd.value, "v");
}

TEST(CommandTest, DelFactoryHasEmptyValue) {
    const Command cmd = Command::del("k");
    EXPECT_EQ(cmd.op, OpType::kDelete);
    EXPECT_EQ(cmd.key, "k");
    EXPECT_TRUE(cmd.value.empty());
}

TEST(CommandTest, OpTypeValuesAreStable) {
    EXPECT_EQ(static_cast<int>(OpType::kSet), 1);
    EXPECT_EQ(static_cast<int>(OpType::kDelete), 2);
}

TEST(CommandTest, Equality) {
    EXPECT_EQ(Command::set("a", "1"), Command::set("a", "1"));
    EXPECT_NE(Command::set("a", "1"), Command::set("a", "2"));
    EXPECT_NE(Command::set("a", ""), Command::del("a"));
}

TEST(LogEntryTest, DefaultIsZero) {
    const LogEntry entry;
    EXPECT_EQ(entry.index, 0U);
    EXPECT_EQ(entry.term, 0U);
}

TEST(LogEntryTest, Equality) {
    const LogEntry a{1, 1, Command::set("k", "v")};
    EXPECT_EQ(a, (LogEntry{1, 1, Command::set("k", "v")}));
    EXPECT_NE(a, (LogEntry{2, 1, Command::set("k", "v")}));
    EXPECT_NE(a, (LogEntry{1, 2, Command::set("k", "v")}));
    EXPECT_NE(a, (LogEntry{1, 1, Command::del("k")}));
}

TEST(StatusTest, DefaultIsOk) {
    const Status s;
    EXPECT_TRUE(s.is_ok());
    EXPECT_EQ(s.code(), StatusCode::kOk);
    EXPECT_EQ(s.to_string(), "OK");
    EXPECT_EQ(s, Status::ok());
}

TEST(StatusTest, FactoriesSetCodeAndMessage) {
    EXPECT_EQ(Status::invalid_argument("x").code(), StatusCode::kInvalidArgument);
    EXPECT_EQ(Status::not_leader("x").code(), StatusCode::kNotLeader);
    EXPECT_EQ(Status::unavailable("x").code(), StatusCode::kUnavailable);
    EXPECT_EQ(Status::internal("x").code(), StatusCode::kInternal);

    const Status s = Status::invalid_argument("key must not be empty");
    EXPECT_FALSE(s.is_ok());
    EXPECT_EQ(s.message(), "key must not be empty");
}

TEST(StatusTest, ToString) {
    EXPECT_EQ(Status::not_leader("leader is node 2").to_string(), "NOT_LEADER: leader is node 2");
    EXPECT_EQ(Status::unavailable("").to_string(), "UNAVAILABLE");
    EXPECT_EQ(to_string(StatusCode::kInternal), "INTERNAL");
}

}  // namespace
}  // namespace distkv

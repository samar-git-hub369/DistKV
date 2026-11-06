#include <gtest/gtest.h>

#include <optional>
#include <string>
#include <utility>

#include "common/log_entry.hpp"
#include "common/status.hpp"
#include "common/status_or.hpp"

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

TEST(StatusOrTest, HoldsValue) {
    StatusOr<std::string> r = std::string("hello");
    ASSERT_TRUE(r.is_ok());
    EXPECT_TRUE(r.status().is_ok());
    EXPECT_EQ(r.value(), "hello");
    EXPECT_EQ(*r, "hello");
    EXPECT_EQ(r->size(), 5U);
    const std::string moved = std::move(r).value();
    EXPECT_EQ(moved, "hello");
}

TEST(StatusOrTest, HoldsError) {
    const StatusOr<int> r = Status::not_leader("leader is node 2");
    EXPECT_FALSE(r.is_ok());
    EXPECT_EQ(r.status().code(), StatusCode::kNotLeader);
    EXPECT_EQ(r.status().message(), "leader is node 2");
}

TEST(StatusOrTest, OkStatusWithoutValueBecomesInternal) {
    const StatusOr<int> r = Status::ok();
    EXPECT_FALSE(r.is_ok());
    EXPECT_EQ(r.status().code(), StatusCode::kInternal);
}

TEST(StatusOrTest, OptionalValueDistinguishesAbsentFromError) {
    const StatusOr<std::optional<std::string>> absent{std::nullopt};
    ASSERT_TRUE(absent.is_ok());
    EXPECT_FALSE(absent->has_value());

    const StatusOr<std::optional<std::string>> present = std::optional<std::string>("v");
    ASSERT_TRUE(present.is_ok());
    EXPECT_EQ(*present, "v");
}

TEST(StatusOrDeathTest, ValueOnErrorAborts) {
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    const StatusOr<int> r = Status::unavailable("no quorum");
    EXPECT_DEATH((void)r.value(), "StatusOr::value\\(\\) on error: UNAVAILABLE: no quorum");
}

}  // namespace
}  // namespace distkv

#pragma once

#include <cstdlib>
#include <iostream>
#include <type_traits>
#include <utility>
#include <variant>

#include "common/status.hpp"

namespace distkv {

// Either a value of type T or a non-OK Status, returned by value. The
// companion to Status for operations that produce a result.
//
//   StatusOr<int> r = compute();
//   if (!r.is_ok()) return r.status();
//   use(*r);
template <typename T>
class [[nodiscard]] StatusOr {
    static_assert(!std::is_same_v<std::remove_cv_t<T>, Status>,
                  "StatusOr<Status> is ambiguous; use Status");

public:
    // Implicit so functions can `return value;` or `return Status::...;`.
    StatusOr(T value) : rep_(std::in_place_index<1>, std::move(value)) {}  // NOLINT

    // `status` must not be OK: an OK StatusOr must hold a value. Passing an OK
    // status is a bug and is turned into kInternal rather than hidden.
    StatusOr(Status status)  // NOLINT
        : rep_(std::in_place_index<0>,
               status.is_ok() ? Status::internal("StatusOr constructed from an OK Status")
                              : std::move(status)) {}

    [[nodiscard]] bool is_ok() const { return rep_.index() == 1; }

    // Status::ok() when a value is held.
    [[nodiscard]] Status status() const { return is_ok() ? Status::ok() : std::get<0>(rep_); }

    // Precondition: is_ok(). Accessing the value of an error aborts.
    [[nodiscard]] const T& value() const& {
        check_ok();
        return std::get<1>(rep_);
    }
    [[nodiscard]] T& value() & {
        check_ok();
        return std::get<1>(rep_);
    }
    [[nodiscard]] T&& value() && {
        check_ok();
        return std::get<1>(std::move(rep_));
    }

    const T& operator*() const& { return value(); }
    T& operator*() & { return value(); }
    T&& operator*() && { return std::move(*this).value(); }
    const T* operator->() const { return &value(); }
    T* operator->() { return &value(); }

private:
    void check_ok() const {
        if (!is_ok()) {
            std::cerr << "distkv: StatusOr::value() on error: " << std::get<0>(rep_).to_string()
                      << std::endl;
            std::abort();
        }
    }

    std::variant<Status, T> rep_;
};

}  // namespace distkv

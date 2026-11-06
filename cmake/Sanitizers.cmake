# Global sanitizer configuration, controlled by DISTKV_SANITIZER:
#   address -> AddressSanitizer + UndefinedBehaviorSanitizer
#   thread  -> ThreadSanitizer
# Include this before any add_library/add_executable/FetchContent call.

if(DISTKV_SANITIZER STREQUAL "")
    return()
endif()

if(NOT CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    message(FATAL_ERROR "DISTKV_SANITIZER requires GCC or Clang (got ${CMAKE_CXX_COMPILER_ID})")
endif()

if(DISTKV_SANITIZER STREQUAL "address")
    set(_distkv_san_flags -fsanitize=address,undefined -fno-sanitize-recover=all)
elseif(DISTKV_SANITIZER STREQUAL "thread")
    set(_distkv_san_flags -fsanitize=thread)
else()
    message(FATAL_ERROR "Unknown DISTKV_SANITIZER '${DISTKV_SANITIZER}' (expected address or thread)")
endif()

message(STATUS "DistKV: sanitizer enabled: ${DISTKV_SANITIZER}")

add_compile_options(${_distkv_san_flags} -fno-omit-frame-pointer -g)
add_link_options(${_distkv_san_flags})

# GCC warns that TSan does not instrument std::atomic_thread_fence, which Asio
# uses internally. The warning is not actionable for us.
if(DISTKV_SANITIZER STREQUAL "thread" AND CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    add_compile_options(-Wno-tsan)
endif()

# On macOS, Asio's kqueue reactor hands per-socket state between threads
# through kevent(), which TSan does not model as synchronisation, so any
# io_context run on several threads reports false races inside Asio. Its
# select-based reactor synchronises with mutexes TSan can see. Linux (CI) uses
# epoll, whose hand-off TSan does model, so it keeps the default reactor.
if(DISTKV_SANITIZER STREQUAL "thread" AND APPLE)
    add_compile_definitions(ASIO_DISABLE_KQUEUE)
endif()

unset(_distkv_san_flags)

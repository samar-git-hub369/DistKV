cmake_minimum_required(VERSION 3.20)

project(DistKV
    VERSION 0.1.0
    DESCRIPTION "Fault-tolerant distributed key-value store"
    LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "${PROJECT_BINARY_DIR}/bin")

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
    set(CMAKE_BUILD_TYPE Debug CACHE STRING "Build type" FORCE)
endif()

option(DISTKV_BUILD_TESTS "Build the DistKV test suite" ON)
option(DISTKV_WERROR "Treat compiler warnings as errors" OFF)
set(DISTKV_SANITIZER "" CACHE STRING "Sanitizer to enable: '', 'address' (ASan+UBSan) or 'thread' (TSan)")
set_property(CACHE DISTKV_SANITIZER PROPERTY STRINGS "" address thread)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

# Sanitizer flags are global and must be set before any target (ours or
# third-party) is created so that everything is instrumented consistently.
include(Sanitizers)
include(Warnings)
include(Dependencies)

add_subdirectory(src)

if(DISTKV_BUILD_TESTS)
    enable_testing()
    add_subdirectory(tests)
endif()

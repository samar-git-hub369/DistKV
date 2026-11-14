cmake_minimum_required(VERSION 3.20)
project(DistKV VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
    set(CMAKE_BUILD_TYPE Debug CACHE STRING "Build type" FORCE)
endif()

option(DISTKV_BUILD_TESTS "Build the unit tests" ON)
option(DISTKV_WERROR "Treat compiler warnings as errors" OFF)

add_library(distkv_warnings INTERFACE)
target_compile_options(distkv_warnings INTERFACE
    -Wall -Wextra -Wpedantic -Wshadow -Wconversion -Wsign-conversion
    $<$<BOOL:${DISTKV_WERROR}>:-Werror>)

add_subdirectory(src)

if(DISTKV_BUILD_TESTS)
    include(cmake/Dependencies.cmake)
    enable_testing()
    add_subdirectory(tests)
endif()

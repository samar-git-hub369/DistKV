# Third-party dependencies, fetched at configure time and pinned to exact tags.

include(FetchContent)

find_package(Threads REQUIRED)

# --- Standalone Asio (header-only) ------------------------------------------
FetchContent_Declare(asio
    GIT_REPOSITORY https://github.com/chriskohlhoff/asio.git
    GIT_TAG        asio-1-30-2
    GIT_SHALLOW    TRUE)
# Asio ships no top-level CMakeLists.txt, so this only downloads the sources.
FetchContent_MakeAvailable(asio)

add_library(distkv_asio INTERFACE)
add_library(asio::asio ALIAS distkv_asio)
target_include_directories(distkv_asio SYSTEM INTERFACE "${asio_SOURCE_DIR}/asio/include")
target_compile_definitions(distkv_asio INTERFACE ASIO_STANDALONE ASIO_NO_DEPRECATED)
target_link_libraries(distkv_asio INTERFACE Threads::Threads)

# --- GoogleTest ------------------------------------------------------------
if(DISTKV_BUILD_TESTS)
    FetchContent_Declare(googletest
        GIT_REPOSITORY https://github.com/google/googletest.git
        GIT_TAG        v1.15.2
        GIT_SHALLOW    TRUE)
    set(INSTALL_GTEST OFF CACHE BOOL "" FORCE)
    set(BUILD_GMOCK OFF CACHE BOOL "" FORCE)
    set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
    FetchContent_MakeAvailable(googletest)

    foreach(_gtest_target gtest gtest_main)
        # Treat GoogleTest headers as system headers so our strict warnings do
        # not fire inside them, and silence warnings in its own sources.
        get_target_property(_dirs ${_gtest_target} INTERFACE_INCLUDE_DIRECTORIES)
        set_target_properties(${_gtest_target} PROPERTIES
            INTERFACE_SYSTEM_INCLUDE_DIRECTORIES "${_dirs}")
        if(NOT MSVC)
            target_compile_options(${_gtest_target} PRIVATE -w)
        endif()
    endforeach()
endif()

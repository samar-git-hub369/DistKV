include(GoogleTest)

add_executable(distkv_tests
    coding_test.cpp
    wal_test.cpp
)

target_link_libraries(distkv_tests PRIVATE distkv_storage distkv_warnings GTest::gtest_main)

gtest_discover_tests(distkv_tests)

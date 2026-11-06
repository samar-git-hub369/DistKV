include(GoogleTest)

# distkv_add_test(<name> <sources>...)
function(distkv_add_test name)
    add_executable(${name} ${ARGN})
    target_link_libraries(${name} PRIVATE distkv_core GTest::gtest_main)
    distkv_set_warnings(${name})
    gtest_discover_tests(${name} DISCOVERY_TIMEOUT 30 PROPERTIES TIMEOUT 60)
endfunction()

distkv_add_test(common_test common_test.cpp)

distkv_add_test(asio_smoke_test asio_smoke_test.cpp)
target_link_libraries(asio_smoke_test PRIVATE asio::asio)

distkv_add_test(local_replicator_test local_replicator_test.cpp)

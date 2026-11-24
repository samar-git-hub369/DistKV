include(GoogleTest)

add_executable(distkv_tests
    bloom_filter_test.cpp
    coding_test.cpp
    compaction_test.cpp
    flush_test.cpp
    memtable_test.cpp
    raft_log_test.cpp
    raft_node_test.cpp
    raft_rpc_test.cpp
    raft_transport_test.cpp
    sstable_iterator_test.cpp
    sstable_test.cpp
    storage_engine_test.cpp
    tcp_server_test.cpp
    text_protocol_test.cpp
    wal_test.cpp
)

target_link_libraries(distkv_tests PRIVATE distkv_server distkv_raft distkv_storage distkv_warnings GTest::gtest_main)

gtest_discover_tests(distkv_tests)

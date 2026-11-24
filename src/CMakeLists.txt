add_library(distkv_storage STATIC
    common/coding.cpp
    common/crc32.cpp
    common/file_util.cpp
    storage/bloom_filter.cpp
    storage/compaction.cpp
    storage/flush.cpp
    storage/memtable.cpp
    storage/merging_iterator.cpp
    storage/sstable.cpp
    storage/storage_engine.cpp
    storage/wal.cpp
)

target_include_directories(distkv_storage PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
find_package(Threads REQUIRED)
target_link_libraries(distkv_storage PUBLIC Threads::Threads PRIVATE distkv_warnings)

# Networking and request handling on top of the storage engine.
find_package(Boost 1.74 CONFIG REQUIRED)

add_library(distkv_server STATIC
    network/tcp_server.cpp
    protocol/text_protocol.cpp
    server/command_handler.cpp
)
target_include_directories(distkv_server PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(distkv_server PUBLIC distkv_storage Boost::headers PRIVATE distkv_warnings)

add_executable(kvstore_server server/main.cpp)
target_link_libraries(kvstore_server PRIVATE distkv_server distkv_raft distkv_warnings)

# Raft consensus: log, term/vote state, RPC messages, election and peer transport.
add_library(distkv_raft STATIC
    raft/asio_transport.cpp
    raft/hard_state.cpp
    raft/raft_log.cpp
    raft/raft_node.cpp
    raft/raft_rpc.cpp
    raft/raft_types.cpp
)
target_include_directories(distkv_raft PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(distkv_raft PUBLIC distkv_storage Boost::headers PRIVATE distkv_warnings)

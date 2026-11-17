add_library(distkv_storage STATIC
    common/coding.cpp
    common/crc32.cpp
    common/file_util.cpp
    storage/flush.cpp
    storage/memtable.cpp
    storage/sstable.cpp
    storage/wal.cpp
)

target_include_directories(distkv_storage PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(distkv_storage PRIVATE distkv_warnings)

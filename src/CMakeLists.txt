# distkv_core: everything except the server entry point, so tests and tools
# can link the real code.
add_library(distkv_core STATIC
    replication/local_replicator.cpp
    server/kv_service.cpp
    storage/kv_store.cpp)
target_include_directories(distkv_core PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}")
target_compile_features(distkv_core PUBLIC cxx_std_20)
distkv_set_warnings(distkv_core)

add_executable(distkv-server server/main.cpp)
target_link_libraries(distkv-server PRIVATE distkv_core asio::asio)
target_compile_definitions(distkv-server PRIVATE DISTKV_VERSION="${PROJECT_VERSION}")
distkv_set_warnings(distkv-server)

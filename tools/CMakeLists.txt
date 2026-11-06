add_executable(distkv-cli distkv-cli.cpp)
target_link_libraries(distkv-cli PRIVATE distkv_core asio::asio)
target_compile_definitions(distkv-cli PRIVATE DISTKV_VERSION="${PROJECT_VERSION}")
distkv_set_warnings(distkv-cli)

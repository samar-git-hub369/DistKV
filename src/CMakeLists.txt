# distkv_core: everything except the server entry point, so tests and tools
# can link the real code.
add_library(distkv_core INTERFACE)
target_include_directories(distkv_core INTERFACE "${CMAKE_CURRENT_SOURCE_DIR}")
target_compile_features(distkv_core INTERFACE cxx_std_20)

add_executable(distkv-server server/main.cpp)
target_link_libraries(distkv-server PRIVATE distkv_core)
target_compile_definitions(distkv-server PRIVATE DISTKV_VERSION="${PROJECT_VERSION}")
distkv_set_warnings(distkv-server)

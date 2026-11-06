add_executable(distkv-server server/main.cpp)
target_compile_features(distkv-server PRIVATE cxx_std_20)
target_compile_definitions(distkv-server PRIVATE DISTKV_VERSION="${PROJECT_VERSION}")
distkv_set_warnings(distkv-server)

cmake_minimum_required(VERSION 3.15)

project(TinyKV)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(CMAKE_BUILD_TYPE Debug)

# 收集 src 中的源文件。
aux_source_directory(
    "${CMAKE_CURRENT_SOURCE_DIR}/src"
    CORE_SOURCES
)

# 服务端入口单独使用，不加入公共库。
list(REMOVE_ITEM CORE_SOURCES
    "${CMAKE_CURRENT_SOURCE_DIR}/src/server_main.cpp"
)

add_library(tinykv_core STATIC ${CORE_SOURCES})

target_include_directories(tinykv_core PUBLIC
    "${CMAKE_CURRENT_SOURCE_DIR}/include"
)

# 服务端。
add_executable(tinykv_server
    src/server_main.cpp
)
target_link_libraries(tinykv_server PRIVATE tinykv_core)

# 客户端。
aux_source_directory(
    "${CMAKE_CURRENT_SOURCE_DIR}/client"
    CLIENT_SOURCES
)

add_executable(tinykv_client ${CLIENT_SOURCES})
target_link_libraries(tinykv_client PRIVATE tinykv_core)

# 两个可执行程序都生成到项目根目录下的 output。
set_target_properties(tinykv_server tinykv_client PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/output"
)
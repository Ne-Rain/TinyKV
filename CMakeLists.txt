cmake_minimum_required(VERSION 3.15)

project(TinyKV)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(CMAKE_BUILD_TYPE Debug)

# 按职责目录收集公共库实现；应用入口单独加入服务端目标。
aux_source_directory("${CMAKE_CURRENT_SOURCE_DIR}/src/core" CORE_SOURCES)
aux_source_directory("${CMAKE_CURRENT_SOURCE_DIR}/src/net" CORE_SOURCES)
aux_source_directory("${CMAKE_CURRENT_SOURCE_DIR}/src/server" CORE_SOURCES)
aux_source_directory("${CMAKE_CURRENT_SOURCE_DIR}/src/async" CORE_SOURCES)
aux_source_directory("${CMAKE_CURRENT_SOURCE_DIR}/src/threading" CORE_SOURCES)

add_library(tinykv_core STATIC ${CORE_SOURCES})

target_include_directories(tinykv_core PUBLIC
    "${CMAKE_CURRENT_SOURCE_DIR}/include"
)
find_package(Threads REQUIRED)
target_link_libraries(tinykv_core PUBLIC Threads::Threads)

# 服务端。
add_executable(tinykv_server
    src/server_main.cpp
    src/app/server_options.cpp
    src/app/blocking_server.cpp
    src/app/reactor_server.cpp
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
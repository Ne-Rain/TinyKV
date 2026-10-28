#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <string>

struct ServerOptions {
    std::string host = "127.0.0.1";
    std::uint16_t port = 8888;
    std::string mode = "blocking";
    std::size_t threads{4};
    std::chrono::milliseconds idle_timeout{30000}; // 空闲超时
    std::chrono::milliseconds frame_timeout{10000}; // 帧接收超时（一帧必须在该时间内接收完成）
    std::chrono::milliseconds output_stall_timeout{10000}; // 输出停滞超时（防止响应一直不发）
    std::chrono::milliseconds shutdown_grace{5000}; // 程序使用信号中断后，允许的排空超时
};

bool parse_server_options(int argc, char* argv[], ServerOptions& options_out, std::string& error_out);

#pragma once
#include <arpa/inet.h>
#include <string>
#include <vector>

enum class ParseStatus { NeedMore, FrameReady, ProtocolError };
enum class CommandType { Ping, Set, Get, Del };

struct Command {
    CommandType type = CommandType::Ping;
    std::string key;
    std::string value;
};

// 消息帧封装
std::string encode_frame(const std::string& payload);

// 消息帧解码
ParseStatus try_decode_frame(std::string& buffer, std::string& payload_out);

// 格式检查
bool valid_command_text(const std::string& payload);

// 命令分割
std::vector<std::string> split_command_text(const std::string& payload);

// 命令识别
bool parse_command(const std::string& payload, Command& command_out, std::string& error_out);
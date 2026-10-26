#include "protocol.hpp"
#include <arpa/inet.h>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <netinet/in.h>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

std::string encode_frame(const std::string& payload) {
    if (payload.empty() || payload.size() > 8192) {
        throw std::invalid_argument("payload size out of range");
    }
    std::uint32_t length = static_cast<std::uint32_t>(payload.size());
    std::uint32_t network_length = htonl(length);

    std::string frame;
    const char* header = reinterpret_cast<const char*>(&network_length); // 用字符串存储（只考虑实际储存字节）
    frame.append(header, sizeof(network_length));
    frame.append(payload);
    return frame;
}

ParseStatus peek_frame(const std::string& buffer, std::size_t offset, std::size_t& frame_size_out) {
    frame_size_out = 0;
    if (offset > buffer.size()) {
        return ParseStatus::ProtocolError;
    }
    std::size_t available = buffer.size() - offset; // 计算余下字节是否有完整帧头（4字节）
    if (available < sizeof(std::uint32_t)) {
        return ParseStatus::NeedMore;
    }
    std::uint32_t network_length = 0;
    std::memcpy(&network_length, buffer.data() + offset, sizeof(network_length)); // 读取实际数据长度
    std::uint32_t length = ntohl(network_length);
    if (length == 0 || length > 8192) {
        return ParseStatus::ProtocolError;
    }
    frame_size_out = sizeof(network_length) + static_cast<std::size_t>(length);
    return available < frame_size_out ? ParseStatus::NeedMore : ParseStatus::FrameReady;
}

ParseStatus try_decode_frame(std::string& buffer, std::string& payload_out) {
    std::size_t frame_size = 0;
    ParseStatus status = peek_frame(buffer, 0, frame_size);
    if (status != ParseStatus::FrameReady) {
        return status;
    }
    payload_out.assign(buffer, sizeof(std::uint32_t), frame_size - sizeof(std::uint32_t));
    buffer.erase(0, frame_size);
    return ParseStatus::FrameReady;
}

bool valid_command_text(const std::string& payload) {
    if (payload.empty()) {
        return false;
    }
    if (payload.front() == ' ' || payload.back() == ' ' || payload.find("  ") != std::string::npos) {
        return false;
    }
    for (char ch : payload) {
        unsigned char byte = static_cast<unsigned char>(ch);
        if (byte < 0x20 || byte > 0x7E) {
            return false;
        }
    }
    return true;
}

std::vector<std::string> split_command_text(const std::string& payload) {
    std::vector<std::string> tokens;
    std::size_t start = 0;
    while (true) {
        std::size_t pos = payload.find(' ', start);
        if (pos == std::string::npos) {
            tokens.push_back(payload.substr(start));
            break;
        }
        tokens.push_back(payload.substr(start, pos - start));
        start = pos + 1;
    }
    return tokens;
}

bool parse_command(const std::string& payload, Command& command_out, std::string& error_out) {
    if (!valid_command_text(payload)) {
        error_out = "ERR INVALID_ARGUMENT";
        return false;
    }
    std::vector<std::string> tokens = split_command_text(payload);
    Command command;
    std::size_t expected_tokens = 0;
    if (tokens[0] == "PING") {
        command.type = CommandType::Ping;
        expected_tokens = 1;
    } else if (tokens[0] == "SET") {
        command.type = CommandType::Set;
        expected_tokens = 3;
    } else if (tokens[0] == "GET") {
        command.type = CommandType::Get;
        expected_tokens = 2;
    } else if (tokens[0] == "DEL") {
        command.type = CommandType::Del;
        expected_tokens = 2;
    } else {
        error_out = "ERR UNKNOWN_COMMAND";
        return false;
    }
    if (tokens.size() != expected_tokens) {
        error_out = "ERR INVALID_ARGUMENT";
        return false;
    }
    if (tokens[0] != "PING") {
        if (tokens[1].size() > 128) {
            error_out = "ERR KEY_TOO_LONG";
            return false;
        }
        command.key = tokens[1];
        if (tokens[0] == "SET") {
            if (tokens[2].size() > 4096) {
                error_out = "ERR VALUE_TOO_LONG";
                return false;
            }
            command.value = tokens[2];
        }
    }
    command_out = std::move(command);
    error_out.clear();
    return true;
}
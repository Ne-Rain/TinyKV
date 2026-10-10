#include "protocol.hpp"
#include <arpa/inet.h>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <netinet/in.h>
#include <stdexcept>
#include <string>
#include <vector>

std::string encode_frame(const std::string& payload) {
    if (payload.empty() || payload.size() > 8192) {
        throw std::invalid_argument("payload size out of range");
    }
    std::uint32_t length = static_cast<std::uint32_t>(payload.size());
    std::uint32_t network_length = htonl(length);

    std::string frame;
    const char* header = reinterpret_cast<const char*>(&network_length);
    frame.append(header, sizeof(network_length));
    frame.append(payload);
    return frame;
}

ParseStatus try_decode_frame(std::string& buffer, std::string& payload_out) {
    if (buffer.size() < 4) {
        return ParseStatus::NeedMore;
    }
    std::uint32_t network_length = 0;
    memcpy(&network_length, buffer.data(), sizeof(network_length));
    std::uint32_t length = ntohl(network_length);
    if (length == 0 || length > 8192) {
        return ParseStatus::ProtocolError;
    }
    if (buffer.size() < 4 + static_cast<std::size_t>(length)) {
        return ParseStatus::NeedMore;
    }
    payload_out.assign(buffer, 4, length);
    buffer.erase(0, 4 + static_cast<std::size_t>(length));
    return ParseStatus::FrameReady;
}

bool valid_command_text(const std::string& payload) {
    if (payload.empty()) {
        return false;
    }
    if (payload.front() == ' ' || payload.back() == ' ' ||
        payload.find("  ") != std::string::npos) {
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
            tokens.push_back(payload.substr(start, payload.size() - start));
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
        command.type = CommandType::Set;
        expected_tokens = 2;
    } else if (tokens[0] == "DEL") {
        command.type = CommandType::Set;
        expected_tokens = 2;
    } else {
        error_out = "ERR UNKNOWN_COMMAND";
        return false;
    }
    if(tokens.size() != expected_tokens){
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
#include "protocol.hpp"
#include <arpa/inet.h>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <netinet/in.h>
#include <stdexcept>

std::string encode_frame(const std::string& payload){
    if(payload.empty() || payload.size() > 8192){
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

ParseStatus try_decode_frame(std::string& buffer, std::string& payload_out){
    if(buffer.size() < 4){
        return ParseStatus::NeedMore;
    }
    std::uint32_t network_length = 0;
    memcpy(&network_length, buffer.data(),sizeof(network_length));
    std::uint32_t length = ntohl(network_length);
    if(length == 0 || length > 8192){
        return ParseStatus::ProtocolError;
    }
    if(buffer.size() < 4 + static_cast<std::size_t>(length)){
        return ParseStatus::NeedMore;
    }
    payload_out.assign(buffer, 4, length);
    buffer.erase(0, 4 + static_cast<std::size_t>(length));
    return ParseStatus::FrameReady;
}
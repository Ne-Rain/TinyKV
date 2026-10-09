#pragma once
#include <arpa/inet.h>
#include <string>

enum class ParseStatus { NeedMore, FrameReady, ProtocolError };

std::string encode_frame(const std::string& payload);

ParseStatus try_decode_frame(std::string& buffer, std::string& payload_out);
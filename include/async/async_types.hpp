#pragma once
#include <cstdint>
#include <string>

// 请求任务
struct RequestTask {
    std::uint64_t connection_id;    // 标识请求属于哪一次连接
    std::uint64_t sequence;         // 标识它是该连接上的第几个请求
    std::string payload;            // 请求帧的内容，不包含外层长度头
};

// 任务结果
struct Completion {
    std::uint64_t connection_id;    
    std::uint64_t sequence;     
    std::string response;           // 响应内容    
};


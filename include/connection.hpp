#pragma once
#include "kv_store.hpp"
#include <cstdint>
#include <string>

// 创建监听fd
int create_listen_socket(const std::string& host, std::uint16_t port);
// 发送完整数据
bool send_all(int fd, const std::string& data);
// 处理客户端数据
void handle_client(int client_fd, KVStore& store);
// 连接服务器
int connect_to_server(const std::string& ip, std::uint16_t port);
// 持续解码数据
bool receive_frame(int fd, std::string& buffer, std::string& payload_out);
// 设置fd阻塞属性
bool set_nonblocking(int fd);
#pragma once
#include "kv_store.hpp"
#include <cstdint>
#include <string>

int create_listen_socket(const std::string& host, std::uint16_t port);

bool send_all(int fd, const std::string& data);

void handle_client(int client_fd, KVStore& store);

int connect_to_server(const std::string& ip, std::uint16_t port);

bool receive_frame(int fd, std::string& buffer, std::string& payload_out);
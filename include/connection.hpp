#pragma once
#include "kv_store.hpp"
#include <cstdint>
#include <string>

int create_listen_socket(std::uint16_t port);

bool send_all(int fd, const std::string& data);

void handle_client(int client_fd, KVStore& store);
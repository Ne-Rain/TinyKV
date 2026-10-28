#pragma once
#include "core/kv_store.hpp"
#include "core/protocol.hpp"
#include <cstring>

std::string execute_command(KVStore& store, const Command& command);
// 统一业务入口
std::string handle_payload(KVStore& store, const std::string& payload);
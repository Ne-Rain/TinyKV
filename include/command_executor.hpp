#include "kv_store.hpp"
#include "protocol.hpp"
#include <cstring>

std::string execute_command(KVStore& store, const Command& command);

std::string handle_payload(KVStore& store, const std::string& payload);
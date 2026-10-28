#include "core/command_executor.hpp"
#include "core/kv_store.hpp"
#include "core/protocol.hpp"
#include <cstring>

std::string execute_command(KVStore& store, const Command& command) {
    switch (command.type) {
        case CommandType::Ping:
            return "PONG";
        case CommandType::Set: {
            if (store.set(command.key, command.value)) {
                return "OK";
            }
            return "ERR STORE_FULL";
        }
        case CommandType::Get: {
            std::string value;
            if (store.get(command.key, value)) {
                return "VALUE " + value;
            }
            return "NOT_FOUND";
        }
        case CommandType::Del: {
            if (store.del(command.key)) {
                return "DELETED 1";
            }
            return "DELETED 0";
        }
    }
    return "ERR INTERNAL";
}

std::string handle_payload(KVStore& store, const std::string& payload){
    Command command;
    std::string error;
    if(parse_command(payload, command, error)){
        return execute_command(store, command);
    }
    return error;
}
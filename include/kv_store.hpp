#pragma once
#include <cstddef>
#include <string>
#include <unordered_map>
class KVStore {
public:
    KVStore(std::size_t max_bytes) : max_bytes_(max_bytes){}

    std::size_t get_used_bytes();

    bool set(const std::string& key, const std::string& value);
    bool get(const std::string& key, std::string& value_out) const;
    std::size_t del(const std::string& key);

private:
    std::size_t max_bytes_;
    std::size_t used_bytes_{0};
    std::unordered_map<std::string, std::string> data_;
};

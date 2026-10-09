#include "kv_store.hpp"
#include <cstddef>

std::size_t KVStore::get_used_bytes() { return used_bytes_; }

bool KVStore::set(const std::string& key, const std::string& value) {
    // key存在
    auto it = data_.find(key);
    if (it != data_.end()) {
        std::size_t used_bytes = used_bytes_ - it->second.size() + value.size();
        if (used_bytes <= max_bytes_) {
            it->second = value;
            used_bytes_ = used_bytes;
            return true;
        } else {
            return false;
        }
    }
    // key不存在
    std::size_t add_used_bytes = key.size() + value.size();
    if (used_bytes_ + add_used_bytes <= max_bytes_) {
        data_.emplace(key, value);
        used_bytes_ += add_used_bytes;
        return true;
    }
    return false;
}

bool KVStore::get(const std::string& key, std::string& value_out) const {
    auto it = data_.find(key);
    if (it != data_.end()) {
        value_out = it->second;
        return true;
    }
    return false;
}

std::size_t KVStore::del(const std::string& key) {
    auto it = data_.find(key);
    if (it == data_.end()) {
        return 0;
    }
    std::size_t de_used_bytes = it->first.size() + it->second.size();
    data_.erase(key);
    used_bytes_ -= de_used_bytes;
    return 1;
}
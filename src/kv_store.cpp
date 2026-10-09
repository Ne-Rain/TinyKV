#include "kv_store.hpp"

bool KVStroe::set(const std::string& key, const std::string& value){
    data_[key] = value;
    return true;
}

bool KVStroe::get(const std::string& key, std::string& value_out) const{
    auto it = data_.find(key);
    if(it != data_.end()){
        value_out = it->first;
        return true;
    }
    return false;
}

std::size_t KVStroe::del(const std::string& key){
    return data_.erase(key);
}
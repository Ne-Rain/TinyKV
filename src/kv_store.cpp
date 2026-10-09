#include "kv_store.hpp"

bool KVStore::set(const std::string& key, const std::string& value){
    data_[key] = value;
    return true;
}

bool KVStore::get(const std::string& key, std::string& value_out) const{
    auto it = data_.find(key);
    if(it != data_.end()){
        value_out = it->second;
        return true;
    }
    return false;
}

std::size_t KVStore::del(const std::string& key){
    return data_.erase(key);
}
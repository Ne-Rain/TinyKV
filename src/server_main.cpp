#include "kv_store.hpp"
#include <cassert>
#include <string>


void set_get_test(){
    KVStore kvs(16);
    kvs.set("name", "NeRain");
    std::string name;
    int res = kvs.get("name", name);
    assert(res && name == "NeRain");

    res = kvs.get("sex", name);
    assert(!res && name == "NeRain");
}

void del_test(){
    KVStore kvs(16);
    kvs.set("name", "NeRain");
    assert(kvs.del("name"));
    assert(!kvs.del("name"));
    std::string name;
    int res = kvs.get("name", name);
    assert(!res);
}

void capacity_test(){
    KVStore kvs(16);
    assert(kvs.set("name", "Alice"));
    assert(kvs.set("lang", "C++"));
    assert(kvs.set("name", "Bob"));
    assert(kvs.get_used_bytes() == 14);
    assert(!kvs.set("lang", "CPP2026"));
    assert(kvs.del("name"));
    assert(kvs.get_used_bytes() == 7);
}

int main() {
    set_get_test();
    del_test();
    capacity_test();
    return 0;
}
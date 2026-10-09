#include "kv_store.hpp"
#include <cassert>
#include <string>


void set_get_test(){
    KVStore kvs;
    kvs.set("name", "NeRain");
    std::string name;
    int res = kvs.get("name", name);
    assert(res && name == "NeRain");

    res = kvs.get("sex", name);
    assert(!res && name == "NeRain");
}

void del_test(){
    KVStore kvs;
    kvs.set("name", "NeRain");
    assert(kvs.del("name"));
    assert(!kvs.del("name"));
    std::string name;
    int res = kvs.get("name", name);
    assert(!res);
}

int main() {
    set_get_test();
    del_test();
    return 0;
}
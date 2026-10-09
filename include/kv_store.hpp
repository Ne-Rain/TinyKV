#include <cstddef>
#include <string>
#include <unordered_map>


class KVStroe{
public:
    bool set(const std::string& key, const std::string& value);

    bool get(const std::string&key, std::string&value_out) const;

    std::size_t del(const std::string& key);

private:
    std::unordered_map<std::string, std::string> data_;
};



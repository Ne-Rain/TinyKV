#include "connection.hpp"
#include "protocol.hpp"
#include <cstdint>
#include <iostream>
#include <string>
#include <unistd.h>

int main() {
    const std::string ip = "127.0.0.1";
    std::uint16_t port = 8888;
    int cfd = connect_to_server(ip, port);
    if (cfd < 0)
        return 1;
    std::string buffer;
    std::string line;
    int exit_code = 0;
    while (true) {
        std::cout << "tinykv>" << std::flush;
        if (!std::getline(std::cin, line)) {
            break;
        }
        if (line.empty())
            continue;
        if (line.size() > 8192) {
            std::cout << "context size out of range";
        }
        std::string request = encode_frame(line);
        if (!send_all(cfd, request)) {
            std::cerr << "send failed" << std::endl;
            exit_code = 1;
            break;
        }
        std::string response;
        if (!receive_frame(cfd, buffer, response)) {
            std::cerr << "receive failed\n";
            exit_code = 1;
            break;
        }
        std::cout << response << '\n';
    }
    close(cfd);
    return 0;
}
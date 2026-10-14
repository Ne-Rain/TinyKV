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
    if (cfd <= 0)
        return 1;
    std::string request = encode_frame("PING");
    if (!send_all(cfd, request)) {
        std::cerr << "send failed\n";
        close(cfd);
        return 1;
    }
    std::string buffer;
    std::string response;

    if (!receive_frame(cfd, buffer, response)) {
        std::cerr << "接收响应失败\n";
        close(cfd);
        return 1;
    }

    std::cout << response << '\n';
    close(cfd);
    return 0;
}
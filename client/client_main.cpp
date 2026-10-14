#include "connection.hpp"
#include "protocol.hpp"
#include <cstdint>
#include <cstring>
#include <iostream>
#include <string>
#include <unistd.h>

struct ClientOptions {
    std::string host = "127.0.0.1";
    std::uint16_t port = 8888;
};

bool parse_port(const std::string& text, std::uint16_t& port_out) {
    if (text.empty())
        return false;
    std::uint32_t port = 0;
    for (int i = 0; i < text.size(); ++i) {
        if (text[i] < '0' || text[i] > '9') {
            return false;
        }
        port *= 10;
        port += (text[i] - '0');
        if (port > 65535) {
            return false;
        }
    }
    if (port < 1) {
        return false;
    }
    port_out = static_cast<std::uint16_t>(port);
    return true;
}

bool parse_client_options(int argc, char* argv[], ClientOptions& options_out,
                          std::string& error_out) {
    for (int i = 1; i < argc; ++i) {
        if (strcmp(argv[i], "--host") == 0) {
            if (i + 1 >= argc) {
                error_out = "Missing parameter value";
                return false;
            }
            ++i;
            options_out.host = argv[i];
        } else if (strcmp(argv[i], "--port") == 0) {
            if (i + 1 >= argc) {
                error_out = "Missing parameter value";
                return false;
            }
            ++i;
            std::uint16_t port = 0;
            if (!parse_port(argv[i], port)){
                error_out = "The port must be a decimal integer ranging from 1 to 65535";
                return false;
            } 
            options_out.port = port;
        } else {
            error_out = "Unkown option: " + std::string(argv[i]);
            return false;
        }
    }
    return true;
}

int main(int argc, char* argv[]) {
    ClientOptions options;
    std::string error_out;
    if (!parse_client_options(argc, argv, options, error_out)) {
        std::cerr << error_out;
        return -1;
    }
    int cfd = connect_to_server(options.host, options.port);
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
            std::cerr << "context size out of range";
            continue;
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
    return exit_code;
}
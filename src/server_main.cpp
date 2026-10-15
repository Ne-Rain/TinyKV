#include "command_executor.hpp"
#include "connection.hpp"
#include "kv_store.hpp"
#include "protocol.hpp"
#include <cassert>
#include <cerrno>
#include <cstddef>
#include <iostream>
#include <netinet/in.h>
#include <string>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#include <unordered_set>
#include <vector>

struct ServerOptions {
    std::string host = "127.0.0.1";
    std::uint16_t port = 8888;
    std::string mode = "blocking";
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

bool parse_server_options(int argc, char* argv[], ServerOptions& options_out,
                          std::string& error_out) {
    ServerOptions so{};
    for (int i = 1; i < argc; ++i) {
        if (strcmp(argv[i], "--host") == 0) {
            if (i + 1 >= argc) {
                error_out = "Missing parameter value";
                return false;
            }
            ++i;
            so.host = argv[i];
        } else if (strcmp(argv[i], "--port") == 0) {
            if (i + 1 >= argc) {
                error_out = "Missing parameter value";
                return false;
            }
            ++i;
            std::uint16_t port = 0;
            if (!parse_port(argv[i], port)) {
                error_out = "The port must be a decimal integer ranging from 1 to 65535";
                return false;
            }
            so.port = port;
        } else if (strcmp(argv[i], "--mode") == 0) {
            if (i + 1 >= argc) {
                error_out = "Missing parameter value";
                return false;
            }
            ++i;
            if (strcmp(argv[i], "blocking") != 0) {
                error_out = "parameter value error";
                return false;
            }
            so.mode = argv[i];
        } else {
            error_out = "Unkown option: " + std::string(argv[i]);
            return false;
        }
    }
    options_out = so;
    return true;
}

int run_blocking_server(const ServerOptions& options) {
    int listen_fd = create_listen_socket(options.host, options.port);
    if (listen_fd == -1) {
        return -1;
    }
    KVStore store(1024 * 1024); // 1 MiB 的逻辑存储额度
    while (true) {
        sockaddr_in caddr{};
        socklen_t len = sizeof(caddr);
        int client_fd = accept(listen_fd, reinterpret_cast<sockaddr*>(&caddr), &len);
        if (client_fd == -1) {
            if (errno == EINTR) {
                continue;
            }
            perror("accept");
            break;
        }
        char ip[32];
        std::cout << inet_ntop(AF_INET, &caddr.sin_addr, ip, sizeof(ip)) << ':'
                  << ntohs(caddr.sin_port) << std::endl;
        handle_client(client_fd, store);
        close(client_fd);
    }
    close(listen_fd);
    return 1;
}

int main(int argc, char* argv[]) {
    ServerOptions so;
    std::string error;
    if (!parse_server_options(argc, argv, so, error)) {
        std::cerr << error << std::endl;
        return 1;
    }
    int lfd = create_listen_socket(so.host, so.port);
    set_nonblocking(lfd);
    int epfd = epoll_create1(EPOLL_CLOEXEC);
    epoll_event event{};
    event.events = EPOLLIN;
    event.data.fd = lfd;
    int res = epoll_ctl(epfd, EPOLL_CTL_ADD, lfd, &event);

    epoll_event events[16];
    int flag = 0;
    int cont = 0;
    int exit_code = 0;
    std::unordered_set<int> clients;
    while (true) {
        int n = epoll_wait(epfd, events, sizeof(events) / sizeof(events[0]), -1);
        if (n == -1) {
            if (errno == EINTR) {
                continue; // 这里继续外层 while，重新等待。
            }
            perror("epoll_wait");
            exit_code = 1;
            break;
        }
        for (int i = 0; i < n; ++i) {
            int fd = events[i].data.fd;
            if (fd == lfd && (events[i].events & EPOLLIN)) {
                for (int attempt = 0; attempt < 16; ++attempt) {
                    int cfd = accept(fd, nullptr, nullptr);
                    if (cfd >= 0) {
                        if (!set_nonblocking(cfd)) {
                            perror("set_nonblocking client");
                            close(cfd);
                            exit_code = 1;
                            flag = 1;
                            break;
                        }
                        ++cont;
                        std::cout << "accepted #" << cont << ", client fd=" << cfd << std::endl;
                        epoll_event client_event{};
                        client_event.events = EPOLLIN | EPOLLRDHUP;
                        client_event.data.fd = cfd;
                        epoll_ctl(epfd, EPOLL_CTL_ADD, cfd, &client_event);
                        clients.insert(cfd);
                        break; // 每次监听事件接入一个客户端。
                    } else {
                        if (errno == EINTR) {
                            continue;
                        } else if (errno == EAGAIN || errno == EWOULDBLOCK) {
                            break;
                        } else {
                            perror("accept");
                            exit_code = 1;
                            flag = 1;
                            break;
                            break;
                        }
                    }
                }
            }
            char buffer[1024];
            ssize_t received = recv(fd, buffer, sizeof(buffer), 0);
            if (received > 0) {
                write(fd, buffer, received);
                continue;
            } else {
                if (received == 0) {
                    std::cout << "client disconnected fd = " << fd << std::endl;
                } else {
                    if (errno == EAGAIN) {
                        break;
                    }
                    perror("recv");
                }
                epoll_ctl(epfd, EPOLL_CTL_DEL, fd, nullptr);
                close(fd);
                clients.erase(fd);
                break;
            }
            if (flag)
                break;
        }
        if (flag)
            break;
    }
    close(epfd);
    close(lfd);
    return exit_code;
}
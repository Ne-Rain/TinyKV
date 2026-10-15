#include "connection.hpp"
#include "command_executor.hpp"
#include "protocol.hpp"
#include <arpa/inet.h>
#include <cerrno>
#include <cstddef>
#include <cstdio>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

int create_listen_socket(const std::string& host, std::uint16_t port) {
    int lfd = socket(AF_INET, SOCK_STREAM, 0);
    if (lfd == -1) {
        perror("socket");
        return -1;
    }
    sockaddr_in saddr{};
    saddr.sin_family = AF_INET;
    saddr.sin_port = htons(port);
    int res = inet_pton(AF_INET, host.c_str(), &saddr.sin_addr);
    if (res != 1) {
        std::fprintf(stderr, "Illegal IPv4: %s\n", host.c_str());
        close(lfd);
        return -1;
    }
    int ret = bind(lfd, reinterpret_cast<sockaddr*>(&saddr), sizeof(saddr));
    if (ret == -1) {
        perror("bind");
        close(lfd);
        return -1;
    }
    ret = listen(lfd, 128);
    if (ret == -1) {
        perror("listen");
        close(lfd);
        return -1;
    }
    return lfd;
}

bool send_all(int fd, const std::string& data) {
    std::size_t sent = 0;
    while (sent < data.size()) {
        ssize_t n = send(fd, data.data() + sent, data.size() - sent, MSG_NOSIGNAL);
        if (n > 0) {
            sent += n;
        } else if (n == -1 && errno == EINTR) {
            continue;
        } else {
            return false;
        }
    }
    return true;
}

void handle_client(int client_fd, KVStore& store) {
    std::string buffer;
    char chunk[4096];
    bool flag = true;

    while (true) {
        ssize_t received = recv(client_fd, chunk, sizeof(chunk), 0);
        if (received > 0) {
            buffer.append(chunk, static_cast<std::size_t>(received));
            while (true) {
                std::string payload;
                ParseStatus status = try_decode_frame(buffer, payload);
                if (status == ParseStatus::FrameReady) {
                    std::string res = handle_payload(store, payload);
                    std::string frame = encode_frame(res);
                    if (!send_all(client_fd, frame)) {
                        flag = false;
                        break;
                    }
                } else if (status == ParseStatus::ProtocolError) {
                    flag = false;
                    break;
                } else {
                    break;
                }
            }
        } else if (received == 0) {
            break; // 对端关闭发送方向
        } else {
            if (errno == EINTR) {
                continue;
            }
            perror("recv");
            break;
        }
        if (!flag)
            break;
    }
}

int connect_to_server(const std::string& ip, std::uint16_t port) {
    int cfd = socket(AF_INET, SOCK_STREAM, 0);
    if (cfd == -1) {
        perror("socket");
        return -1;
    }
    sockaddr_in saddr{};
    saddr.sin_family = AF_INET;
    saddr.sin_port = htons(port);
    int res = inet_pton(AF_INET, ip.c_str(), &saddr.sin_addr);
    if (res != 1) {
        close(cfd);
        return -1;
    }
    res = connect(cfd, reinterpret_cast<sockaddr*>(&saddr), sizeof(saddr));
    if (res == -1) {
        perror("connect");
        close(cfd);
        return -1;
    }
    return cfd;
}

bool receive_frame(int fd, std::string& buffer, std::string& payload_out) {
    while (true) {
        ParseStatus status = try_decode_frame(buffer, payload_out);
        if (status == ParseStatus::FrameReady)
            return true;
        else if (status == ParseStatus::ProtocolError)
            return false;
        else {
            char buf[1024];
            ssize_t len = recv(fd, buf, sizeof(buf), 0);
            if (len > 0) {
                buffer.append(buf, len);
            } else if (len == 0) {
                return false;
            } else {
                if (errno == EINTR)
                    continue;
                return false;
            }
        }
    }
}

bool set_nonblocking(int fd) {
    int flag = fcntl(fd, F_GETFL);
    if(flag == -1)
        return false;
    flag |= O_NONBLOCK;
    int res = fcntl(fd, F_SETFL, flag);
    if(res == 0)
        return true;
    return false;
}
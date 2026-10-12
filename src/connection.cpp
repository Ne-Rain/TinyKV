#include "connection.hpp"
#include "command_executor.hpp"
#include "protocol.hpp"
#include <arpa/inet.h>
#include <cerrno>
#include <cstddef>
#include <cstdio>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

int create_listen_socket(std::uint16_t port) {
    int lfd = socket(AF_INET, SOCK_STREAM, 0);
    if (lfd == -1) {
        perror("socket");
        return -1;
    }
    sockaddr_in saddr{};
    saddr.sin_family = AF_INET;
    saddr.sin_port = htons(port);
    saddr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
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
        } else {
            break;
        }
        if (!flag)
            break;
    }
}
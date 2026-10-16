#include "command_executor.hpp"
#include "connection.hpp"
#include "kv_store.hpp"
#include "protocol.hpp"
#include <algorithm>
#include <cassert>
#include <cerrno>
#include <cstddef>
#include <deque>
#include <iostream>
#include <netinet/in.h>
#include <string>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

enum class ProcessResult { NeedMore, BudgetExhausted, ProtocolError };
struct ServerOptions {
    std::string host = "127.0.0.1";
    std::uint16_t port = 8888;
    std::string mode = "blocking";
};

struct Connection {
    std::uint64_t id;
    int fd;

    std::string input;
    std::string output;
    std::size_t sent = 0;
    bool read_closed = false;
    bool queued = false;

    Connection(std::uint64_t connection_id, int socket_fd) : id(connection_id), fd(socket_fd) {}
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

bool parse_server_options(int argc, char* argv[], ServerOptions& options_out, std::string& error_out) {
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
        std::cout << inet_ntop(AF_INET, &caddr.sin_addr, ip, sizeof(ip)) << ':' << ntohs(caddr.sin_port)
                  << std::endl;
        handle_client(client_fd, store);
        close(client_fd);
    }
    close(listen_fd);
    return 1;
}

bool flush_output(Connection& conn) {
    while (conn.sent < conn.output.size()) {
        ssize_t sent =
            send(conn.fd, conn.output.data() + conn.sent, conn.output.size() - conn.sent, MSG_NOSIGNAL);
        if (sent > 0) {
            conn.sent += static_cast<std::size_t>(sent);
            continue;
        } else if (sent == -1) {
            if (errno == EINTR) {
                continue;
            } else if (errno == EAGAIN || errno == EWOULDBLOCK) {
                return true;
            } else {
                return false;
            }
        } else {
            return false;
        }
    }
    conn.output.clear();
    conn.sent = 0;
    return true;
}

ProcessResult process_input(Connection& conn, KVStore& store, std::size_t max_frames) {
    while (max_frames) {
        std::string payload;
        ParseStatus status = try_decode_frame(conn.input, payload);
        if (status == ParseStatus::FrameReady) {
            std::string response = handle_payload(store, payload);
            std::string frame = encode_frame(response);
            conn.output.append(frame);
            std::cout << "id=" << conn.id << " pending=" << conn.output.size() - conn.sent << std::endl;
            max_frames--;
            continue;
        } else if (status == ParseStatus::NeedMore) {
            return ProcessResult::NeedMore;
        } else {
            std::cout << "ProtocolError" << std::endl;
            return ProcessResult::ProtocolError;
        }
    }
    return ProcessResult::BudgetExhausted;
}

bool update_interest(int epfd, const Connection& conn) {
    epoll_event event{};
    event.data.u64 = conn.id;
    if (!conn.read_closed) {
        event.events |= EPOLLIN | EPOLLRDHUP;
    }
    if (conn.sent < conn.output.size()) {
        event.events |= EPOLLOUT;
    }
    return epoll_ctl(epfd, EPOLL_CTL_MOD, conn.fd, &event) == 0;
}

int main(int argc, char* argv[]) {
    ServerOptions so{};
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
    event.data.u64 = 0;
    int res = epoll_ctl(epfd, EPOLL_CTL_ADD, lfd, &event);

    epoll_event events[16];
    int flag = 0;
    int cont = 0;
    int exit_code = 0;
    std::uint64_t next_id = 1;
    std::unordered_map<std::uint64_t, Connection> connections;
    std::deque<std::uint64_t> runnable;
    std::size_t max_frames = 16;
    KVStore store(1024 * 1024);
    while (true) {
        int timeout = runnable.empty() ? -1 : 0;
        int n = epoll_wait(epfd, events, sizeof(events) / sizeof(events[0]), timeout);
        if (n == -1) {
            if (errno == EINTR) {
                continue; // 这里继续外层 while，重新等待
            }
            perror("epoll_wait");
            exit_code = 1;
            break;
        }
        for (int i = 0; i < n; ++i) {
            std::uint64_t id = events[i].data.u64;
            if (id == 0 && (events[i].events & EPOLLIN)) {
                for (int attempt = 0; attempt < 16; ++attempt) {
                    int cfd = accept(lfd, nullptr, nullptr);
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
                        std::uint64_t id = next_id++;
                        client_event.data.u64 = id;
                        int res = epoll_ctl(epfd, EPOLL_CTL_ADD, cfd, &client_event);
                        if (res == -1) {
                            perror("epoll_ctl ADD");
                            close(cfd);
                            continue;
                        }
                        connections.emplace(id, Connection(id, cfd));
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
                continue;
            }
            auto it = connections.find(id);
            if (it == connections.end()) {
                continue; // 这条连接已经移除
            }
            Connection& conn = it->second;
            int fd = conn.fd;

            std::uint32_t ready = events[i].events;
            bool close_now = false;

            // 1. 处理读取。
            if (!conn.read_closed && (ready & (EPOLLIN | EPOLLRDHUP | EPOLLHUP))) {
                char buffer[1024];
                ssize_t received;

                do {
                    received = recv(fd, buffer, sizeof(buffer), 0);
                } while (received == -1 && errno == EINTR);

                if (received > 0) {
                    conn.input.append(buffer, static_cast<std::size_t>(received));
                    if (!conn.queued) {
                        runnable.push_back(conn.id);
                        conn.queued = true;
                    }
                } else if (received == 0) {
                    conn.read_closed = true;
                    if (!conn.queued) {
                        runnable.push_back(conn.id);
                        conn.queued = true;
                    }
                } else if (errno != EAGAIN && errno != EWOULDBLOCK) {
                    perror("recv");
                    close_now = true;
                }
            }

            // 2. 处理发送。
            if (!close_now && (ready & EPOLLOUT)) {
                if (!flush_output(conn)) {
                    close_now = true;
                }
            }

            // 3. 已读到 EOF，并且响应全部发送完成。
            if (!close_now && conn.read_closed && conn.sent == conn.output.size() && !conn.queued &&
                conn.input.empty()) {
                close_now = true;
            }

            // 4. 保留连接时，更新关注。
            // 新生成的响应会让 update_interest 加入 EPOLLOUT。
            if (!close_now && !update_interest(epfd, conn)) {
                perror("epoll_ctl MOD");
                close_now = true;
            }

            // 5. 统一关闭；此后不再使用 conn。
            if (close_now) {
                epoll_ctl(epfd, EPOLL_CTL_DEL, fd, nullptr);
                close(fd);
                connections.erase(id);
                continue; // 处理下一个 events[i]。
            }
            if (flag)
                break;
        }
        if (flag)
            break;
        std::size_t turns = std::min<std::size_t>(runnable.size(), 32);
        for (std::size_t turn = 0; turn < turns; ++turn) {
            std::uint64_t task_id = runnable.front();
            runnable.pop_front();
            auto it = connections.find(task_id);
            if (it == connections.end()) {
                continue;
            }
            Connection& conn = it->second;
            conn.queued = false;
            bool close_now = false;
            auto result = process_input(conn, store, max_frames);
            if (result == ProcessResult::BudgetExhausted) {
                if (!conn.queued) {
                    runnable.push_back(conn.id);
                    conn.queued = true;
                }
            } else if (result == ProcessResult::NeedMore) {
                // 所有完整帧已经处理完。
                if (conn.read_closed && !conn.input.empty()) {
                    // 已经 EOF，残留半帧不可能再补齐。
                    std::cout << "discard incomplete frame, id=" << conn.id << std::endl;
                    conn.input.clear();
                }
            } else {
                close_now = true;
            }

            if (!close_now && !update_interest(epfd, conn)) {
                perror("epoll_ctl MOD");
                close_now = true;
            }

            if (close_now) {
                int fd = conn.fd;
                if (epoll_ctl(epfd, EPOLL_CTL_DEL, fd, nullptr) == -1) {
                    perror("epoll_ctl DEL");
                }
                close(fd);
                connections.erase(it);
                // conn 已失效，不再访问。
                continue;
            }
        }
    }
    for (auto& conn : connections) {
        close(conn.second.fd);
    }
    connections.clear();
    close(epfd);
    close(lfd);
    return exit_code;
}
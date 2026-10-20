#include "command_executor.hpp"
#include "connection.hpp"
#include "kv_store.hpp"
#include "protocol.hpp"
#include <algorithm>
#include <cassert>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstring>
#include <deque>
#include <iostream>
#include <limits>
#include <netinet/in.h>
#include <stdexcept>
#include <string>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#include <vector>

using Clock = std::chrono::steady_clock;

enum class ProcessResult { NeedMore, BudgetExhausted, ProtocolError, OutputLimit, FrameTimeout };
struct ServerOptions {
    std::string host = "127.0.0.1";
    std::uint16_t port = 8888;
    std::string mode = "blocking";
    std::chrono::milliseconds idle_timeout{30000};
    std::chrono::milliseconds frame_timeout{10000};
    std::chrono::milliseconds output_stall_timeout{10000};
};

struct InputChunk {
    std::size_t bytes;             // 这一批还有多少字节留在缓冲
    Clock::time_point received_at; // 这一批字节的接收时间
};

struct Connection {
    std::uint64_t id;
    int fd;
    std::chrono::steady_clock::time_point last_activity;
    Clock::time_point output_progress_at;
    std::string input;
    std::deque<InputChunk> input_chunks;
    std::string output;
    std::size_t sent = 0;
    bool read_closed = false;
    bool queued = false;

    Connection(std::uint64_t connection_id, int socket_fd,
               std::chrono::steady_clock::time_point last_activity_time)
        : id(connection_id), fd(socket_fd), last_activity(last_activity_time) {}
};

struct Budget_Bytes {
    const std::size_t limit_bytes = 64 * 1024 * 1024; // 全局总上限
    std::size_t used_bytes = 0;                       // 当前已经占用的字节
    std::size_t peak_bytes = 0;                       // 已占用字节历史峰值
};

// 游标按递增偏移查询接收时间，使多帧扫描只遍历每个批次一次。
class InputTimeCursor {
public:
    explicit InputTimeCursor(const Connection& conn)
        : next_(conn.input_chunks.begin()), end_(conn.input_chunks.end()) {}

    Clock::time_point at(std::size_t offset) {
        if (offset < begin_) {
            throw std::logic_error("input timestamp offsets must be nondecreasing");
        }
        while (next_ != end_ && offset - begin_ >= next_->bytes) {
            begin_ += next_->bytes;
            ++next_;
        }
        if (next_ == end_) {
            throw std::logic_error("input timestamp metadata is incomplete");
        }
        return next_->received_at;
    }

private:
    std::deque<InputChunk>::const_iterator next_; // 当前正在查看的接收批次
    std::deque<InputChunk>::const_iterator end_;  // 时间记录队列的结束位置
    std::size_t begin_ = 0;                       // 当前批次在input中的起始偏移
};

Clock::time_point input_received_at(const Connection& conn, std::size_t offset) {
    InputTimeCursor times(conn);
    return times.at(offset);
}

void consume_input_times(Connection& conn, std::size_t bytes) {
    while (bytes > 0) {
        if (conn.input_chunks.empty()) {
            throw std::logic_error("cannot consume missing input timestamps");
        }
        InputChunk& chunk = conn.input_chunks.front();
        std::size_t consumed = std::min(bytes, chunk.bytes);
        chunk.bytes -= consumed;
        bytes -= consumed;
        if (chunk.bytes == 0) {
            conn.input_chunks.pop_front();
        }
    }
}

void append_received_input(Connection& conn, const char* data, std::size_t bytes,
                           Clock::time_point received_at, Budget_Bytes& budget) {
    if (bytes == 0) {
        return;
    }
    conn.input_chunks.push_back(InputChunk{bytes, received_at});
    try {
        conn.input.append(data, bytes);
    } catch (...) {
        conn.input_chunks.pop_back();
        throw;
    }
    budget.used_bytes += bytes;
    budget.peak_bytes = std::max(budget.peak_bytes, budget.used_bytes);
}

void discard_input(Connection& conn, Budget_Bytes& budget) {
    assert(budget.used_bytes >= conn.input.size());
    budget.used_bytes -= conn.input.size();
    conn.input.clear();
    conn.input_chunks.clear();
}

// 完整帧看接收耗时，半帧看当前年龄；不把排队等待时间当作接收时间。
bool frame_deadline_expired(const Connection& conn, Clock::time_point now,
                            std::chrono::milliseconds frame_timeout) {
    std::size_t offset = 0;
    InputTimeCursor times(conn);
    while (offset < conn.input.size()) {
        std::size_t frame_size = 0;
        ParseStatus status = peek_frame(conn.input, offset, frame_size);
        if (status == ParseStatus::ProtocolError) {
            return false; // 协议错误由现有请求处理路径处理。
        }
        Clock::time_point first = times.at(offset);
        if (status == ParseStatus::NeedMore) {
            return now - first >= frame_timeout;
        }
        Clock::time_point last = times.at(offset + frame_size - 1);
        if (last - first >= frame_timeout) {
            return true;
        }
        offset += frame_size;
    }
    return false;
}

bool parse_timeout_ms(const std::string& text, std::chrono::milliseconds& timeout_out) {
    if (text.empty()) {
        return false;
    }
    const std::uint32_t limit = static_cast<std::uint32_t>(std::numeric_limits<int>::max());
    std::uint32_t value = 0;
    for (char c : text) {
        if (c < '0' || c > '9') {
            return false;
        }
        std::uint32_t digit = static_cast<std::uint32_t>(c - '0');
        if (value > (limit - digit) / 10) {
            return false;
        }
        value = value * 10 + digit;
    }
    if (value == 0) {
        return false;
    }
    timeout_out = std::chrono::milliseconds(value);
    return true;
}

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
        } else if (strcmp(argv[i], "--idle-timeout-ms") == 0 || strcmp(argv[i], "--frame-timeout-ms") == 0) {
            std::string name = argv[i];
            if (i + 1 >= argc) {
                error_out = "Missing parameter value: " + name;
                return false;
            }
            std::chrono::milliseconds timeout;
            if (!parse_timeout_ms(argv[++i], timeout)) {
                error_out = "Timeout must be an integer from 1 to 2147483647 ms: " + name;
                return false;
            }
            if (name == "--idle-timeout-ms") {
                so.idle_timeout = timeout;
            } else {
                so.frame_timeout = timeout;
            }
        } else if (strcmp(argv[i], "--mode") == 0) {
            if (i + 1 >= argc) {
                error_out = "Missing parameter value";
                return false;
            }
            ++i;
            if (strcmp(argv[i], "blocking") != 0 && strcmp(argv[i], "reactor") != 0) {
                error_out = "Unsupported mode: " + std::string(argv[i]);
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

bool flush_output(Connection& conn, std::size_t max_bytes, Budget_Bytes& budb) {
    std::size_t remaining_budget = max_bytes;
    while (conn.sent < conn.output.size() && remaining_budget > 0) {
        std::size_t cout = std::min(conn.output.size() - conn.sent, remaining_budget);
        ssize_t sent = send(conn.fd, conn.output.data() + conn.sent, cout, MSG_NOSIGNAL);
        if (sent > 0) {
            conn.sent += static_cast<std::size_t>(sent);
            remaining_budget -= static_cast<std::size_t>(sent);
            budb.used_bytes -= sent;
            const auto now = Clock::now();
            conn.last_activity = now;
            conn.output_progress_at = now;
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
    if (conn.sent == conn.output.size()) {
        conn.output.clear();
        conn.sent = 0;
    }
    return true;
}

ProcessResult process_input(Connection& conn, KVStore& store, std::size_t max_frames, Budget_Bytes& budb,
                            std::chrono::milliseconds frame_timeout = std::chrono::seconds(10)) {
    std::size_t max_send_bytes = 64 * 1024;
    while (max_frames > 0) {
        std::string payload;
        std::size_t frame_size = 0;
        ParseStatus status = peek_frame(conn.input, 0, frame_size);
        if (status == ParseStatus::FrameReady) {
            InputTimeCursor times(conn);
            Clock::time_point first = times.at(0);
            Clock::time_point last = times.at(frame_size - 1);
            if (last - first >= frame_timeout) {
                std::cerr << "frame receive timeout, id=" << conn.id << '\n';
                return ProcessResult::FrameTimeout;
            }
            try_decode_frame(conn.input, payload);
            consume_input_times(conn, frame_size);
            assert(budb.used_bytes >= frame_size);
            budb.used_bytes -= frame_size;
            std::string response = handle_payload(store, payload);
            std::string frame = encode_frame(response);
            if (budb.used_bytes > budb.limit_bytes || frame.size() > budb.limit_bytes - budb.used_bytes) {
                std::cerr << "litmit_bytes exceeded, id=" << conn.id << "\n";
                return ProcessResult::OutputLimit;
            }
            if (conn.sent > 0) {
                conn.output.erase(0, conn.sent);
                conn.sent = 0;
            } else {
                conn.output_progress_at = Clock::now(); // 出现积压开始计时
            }
            std::size_t pending = conn.output.size();
            if (pending > max_send_bytes || frame.size() > max_send_bytes - pending) {
                std::cerr << "output limit exceeded, id=" << conn.id << ", pending=" << pending
                          << ", frame=" << frame.size() << ", limit=" << max_send_bytes << '\n';
                return ProcessResult::OutputLimit;
            }
            conn.output.append(frame);
            budb.used_bytes += frame.size();
            budb.peak_bytes = std::max(budb.used_bytes, budb.peak_bytes);
            std::cout << "id=" << conn.id << " pending=" << conn.output.size() - conn.sent << std::endl;
            --max_frames;
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

void close_connection(int epfd, std::unordered_map<std::uint64_t, Connection>& connections, std::uint64_t id,
                      Budget_Bytes& budb) {

    auto it = connections.find(id);
    if (it == connections.end()) {
        return;
    }
    int fd = it->second.fd;
    if (epoll_ctl(epfd, EPOLL_CTL_DEL, fd, nullptr) == -1) {
        perror("epoll_ctl DEL");
    }
    budb.used_bytes -= it->second.input.size();
    budb.used_bytes -= (it->second.output.size() - it->second.sent);
    close(fd);
    connections.erase(it);
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

int run_reactor_server(const ServerOptions& so) {
    const std::size_t max_connections = 128;
    Budget_Bytes budb;
    int lfd = create_listen_socket(so.host, so.port);
    if (lfd == -1) {
        return -1;
    }
    if (set_nonblocking(lfd) == false) {
        perror("set_nonblocking listen");
        close(lfd);
        return 1;
    }
    int epfd = epoll_create1(EPOLL_CLOEXEC);
    if (epfd == -1) {
        perror("epoll_create1");
        close(lfd);
        return 1;
    }
    epoll_event event{};
    event.events = EPOLLIN;
    event.data.u64 = 0;
    int res = epoll_ctl(epfd, EPOLL_CTL_ADD, lfd, &event);
    if (res == -1) {
        perror("epoll_ctl ADD listen");
        close(epfd);
        close(lfd);
        return 1;
    }

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
        int timeout = runnable.empty() ? 200 : 0;
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
                        if (connections.size() >= max_connections) {
                            close(cfd);
                            continue;
                        }
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
                        connections.emplace(id, Connection(id, cfd, std::chrono::steady_clock::now()));
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
                std::size_t max_received_bytes = 64 * 1024; // 单次最大接收

                do {
                    received = recv(fd, buffer, sizeof(buffer), 0);
                } while (received == -1 && errno == EINTR);

                if (received > 0) {
                    Clock::time_point received_at = Clock::now();
                    std::size_t received_bytes = static_cast<std::size_t>(received);
                    if (conn.input.size() > max_received_bytes || budb.used_bytes > budb.limit_bytes ||
                        received_bytes > max_received_bytes - conn.input.size() ||
                        received_bytes > budb.limit_bytes - budb.used_bytes) {
                        std::cerr << "input budget exceeded, id=" << conn.id << '\n';
                        close_now = true;
                    } else {
                        conn.last_activity = received_at;
                        append_received_input(conn, buffer, received_bytes, received_at, budb);
                        if (!conn.queued) {
                            runnable.push_back(conn.id);
                            conn.queued = true;
                        }
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
                if (!flush_output(conn, 64 * 1024, budb)) {
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
                close_connection(epfd, connections, conn.id, budb);
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
            auto result = process_input(conn, store, max_frames, budb, so.frame_timeout);
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
                    discard_input(conn, budb);
                }
            } else {
                close_now = true;
            }

            if (!close_now && conn.read_closed && !conn.queued && conn.input.empty() &&
                conn.sent == conn.output.size()) {
                close_now = true;
            }

            if (!close_now && !update_interest(epfd, conn)) {
                perror("epoll_ctl MOD");
                close_now = true;
            }

            if (close_now) {
                close_connection(epfd, connections, task_id, budb);
                // conn 已失效，不再访问。
                continue;
            }
        }
        Clock::time_point time_now = Clock::now();
        std::vector<std::uint64_t> timeout_id;
        for (const auto& entry : connections) {
            const Connection& conn = entry.second;
            if (frame_deadline_expired(conn, time_now, so.frame_timeout)) {
                std::cerr << "frame receive timeout, id=" << conn.id << '\n';
                timeout_id.push_back(entry.first);
            } else if (time_now - conn.last_activity >= so.idle_timeout) {
                std::cerr << "idle timeout, id=" << conn.id << '\n';
                timeout_id.push_back(entry.first);
            } else if (conn.sent < conn.output.size() &&
                       time_now - conn.output_progress_at >= so.output_stall_timeout) {
                std::cerr << "output stall timeout, id=" << conn.id << '\n';
                timeout_id.push_back(entry.first);
            }
        }
        for (std::uint64_t id : timeout_id) {
            close_connection(epfd, connections, id, budb);
        }
    }
    while (!connections.empty()) {
        auto id = connections.begin()->first;
        close_connection(epfd, connections, id, budb);
    }
    connections.clear();
    close(epfd);
    close(lfd);
    return exit_code;
}

int main(int argc, char* argv[]) {
    ServerOptions so{};
    std::string error;

    if (!parse_server_options(argc, argv, so, error)) {
        std::cerr << error << std::endl;
        return 1;
    }

    if (so.mode == "blocking") {
        return run_blocking_server(so);
    }

    if (so.mode == "reactor") {
        return run_reactor_server(so);
    }

    std::cerr << "Unsupported mode: " << so.mode << std::endl;
    return 1;
}

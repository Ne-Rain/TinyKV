#include "command_executor.hpp"
#include "connection.hpp"
#include "kv_store.hpp"
#include "protocol.hpp"
#include <cassert>
#include <cstddef>
#include <iostream>
#include <netinet/in.h>
#include <string>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
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

void set_get_test() {
    KVStore kvs(16);
    kvs.set("name", "NeRain");
    std::string name;
    int res = kvs.get("name", name);
    assert(res && name == "NeRain");

    res = kvs.get("sex", name);
    assert(!res && name == "NeRain");
}

void del_test() {
    KVStore kvs(16);
    kvs.set("name", "NeRain");
    assert(kvs.del("name"));
    assert(!kvs.del("name"));
    std::string name;
    int res = kvs.get("name", name);
    assert(!res);
}

void capacity_test() {
    KVStore kvs(16);
    assert(kvs.set("name", "Alice"));
    assert(kvs.set("lang", "C++"));
    assert(kvs.set("name", "Bob"));
    assert(kvs.get_used_bytes() == 14);
    assert(!kvs.set("lang", "CPP2026"));
    assert(kvs.del("name"));
    assert(kvs.get_used_bytes() == 7);
}

void encode_frame_test() {
    std::string network_frame = encode_frame("PING");
    std::string frame;
    ParseStatus status = try_decode_frame(network_frame, frame);
    assert(status == ParseStatus::FrameReady);
    assert(frame == "PING");
    assert(network_frame.empty());
}

// 同一帧分三次输入：2 字节、4 字节、2 字节。
void fragmented_frame_test() {
    const std::string frame = encode_frame("PING");
    assert(frame.size() == 8);

    std::string buffer;
    std::string payload_out = "old";

    // 第一次：只有头部前两个字节。
    buffer.append(frame, 0, 2);
    std::string before = buffer;

    ParseStatus status = try_decode_frame(buffer, payload_out);
    assert(status == ParseStatus::NeedMore);
    assert(buffer == before);
    assert(payload_out == "old");

    // 第二次：头部完整，但正文只有 "PI"。
    buffer.append(frame, 2, 4);
    before = buffer;

    status = try_decode_frame(buffer, payload_out);
    assert(status == ParseStatus::NeedMore);
    assert(buffer == before);
    assert(payload_out == "old");

    // 第三次：补齐正文 "NG"。
    buffer.append(frame, 6, 2);

    status = try_decode_frame(buffer, payload_out);
    assert(status == ParseStatus::FrameReady);
    assert(payload_out == "PING");
    assert(buffer.empty());
}

// 两个完整帧一次输入，每次调用只解析一个帧。
void consecutive_frames_test() {
    const std::string first = encode_frame("PING");
    const std::string second = encode_frame("GET k");

    std::string buffer = first + second;
    std::string payload_out;

    // 第一次：取出 PING，保留完整的第二帧。
    ParseStatus status = try_decode_frame(buffer, payload_out);
    assert(status == ParseStatus::FrameReady);
    assert(payload_out == "PING");
    assert(buffer == second);

    // 第二次：取出 GET k，缓冲区清空。
    status = try_decode_frame(buffer, payload_out);
    assert(status == ParseStatus::FrameReady);
    assert(payload_out == "GET k");
    assert(buffer.empty());

    // 第三次：没有新数据，输出保留上次的内容。
    status = try_decode_frame(buffer, payload_out);
    assert(status == ParseStatus::NeedMore);
    assert(buffer.empty());
    assert(payload_out == "GET k");
}

void split_command_text_test() {
    std::vector<std::string> vec = split_command_text("SET name Alice");
    assert(vec[0] == "SET");
    assert(vec[1] == "name");
    assert(vec[2] == "Alice");
}

void parse_command_test() {
    Command command{};
    std::string error_out;
    assert(parse_command("PING", command, error_out));
    assert(parse_command("SET name Alice", command, error_out));
    assert(command.key == "name");
    assert(command.value == "Alice");
    assert(parse_command("GET name", command, error_out));
    assert(command.key == "name");
    assert(!parse_command("GET name Alice", command, error_out));
    assert(error_out == "ERR INVALID_ARGUMENT");
    assert(!parse_command("HELLO name", command, error_out));
    assert(error_out == "ERR UNKNOWN_COMMAND");
}

void command_executor_test() {
    KVStore store(16);
    assert(handle_payload(store, "PING") == "PONG");
    assert(handle_payload(store, "SET name Alice") == "OK");
    assert(handle_payload(store, "GET name") == "VALUE Alice");
    assert(handle_payload(store, "GET name extra") == "ERR INVALID_ARGUMENT");
    assert(handle_payload(store, "GET name") == "VALUE Alice");
    assert(handle_payload(store, "DEL name") == "DELETED 1");
    assert(handle_payload(store, "DEL name") == "DELETED 0");
    assert(handle_payload(store, "GET name") == "NOT_FOUND");
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
        // 终端执行 nc 127.0.0.1 8888
        // assert(std::string(inet_ntop(AF_INET, &caddr.sin_addr, ip, sizeof(ip))) ==
        // "127.0.0.1");
        std::cout << inet_ntop(AF_INET, &caddr.sin_addr, ip, sizeof(ip)) << ':'
                  << ntohs(caddr.sin_port) << std::endl;
        handle_client(client_fd, store);
        close(client_fd);
    }
    close(listen_fd);
    return 1;
}

int main(int argc, char* argv[]) {
    // set_get_test();
    // del_test();
    // capacity_test();
    // encode_frame_test();
    // fragmented_frame_test();
    // consecutive_frames_test();
    // split_command_text_test();
    // parse_command_test();
    // command_executor_test();
    ServerOptions so;
    std::string error;
    if (!parse_server_options(argc, argv, so, error)) {
        std::cerr << error << std::endl;
        return 1;
    }
    run_blocking_server(so);
    return 0;
}
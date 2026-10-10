#include "kv_store.hpp"
#include "protocol.hpp"
#include <cassert>
#include <iostream>
#include <string>
#include <vector>

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

void split_command_text_test(){
    std::vector<std::string> vec = split_command_text("SET name Alice");
    assert(vec[0] == "SET");
    assert(vec[1] == "name");
    assert(vec[2] == "Alice");
}

void parse_command_test(){
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

int main() {
    // set_get_test();
    // del_test();
    // capacity_test();
    // encode_frame_test();
    // fragmented_frame_test();
    // consecutive_frames_test();
    // split_command_text_test();
    parse_command_test();
    return 0;
}
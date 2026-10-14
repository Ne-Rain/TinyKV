import socket
import struct

ADDRESS = ("127.0.0.1", 8888)


def connect():
    # 超时可以让程序出现空转或不响应时，测试明确失败。
    return socket.create_connection(ADDRESS, timeout=3)


def encode_frame(text):
    body = text.encode("ascii")
    return struct.pack("!I", len(body)) + body


def recv_exact(sock, size):
    """循环接收，直到收齐指定字节数。"""
    data = bytearray()
    while len(data) < size:
        chunk = sock.recv(size - len(data))
        if not chunk:
            raise AssertionError(
                f"提前遇到 EOF：需要 {size} 字节，收到 {len(data)} 字节"
            )
        data.extend(chunk)
    return bytes(data)


def recv_frame(sock):
    header = recv_exact(sock, 4)
    length = struct.unpack("!I", header)[0]
    assert 1 <= length <= 8192, f"非法响应长度：{length}"
    return recv_exact(sock, length).decode("ascii")


def expect_response(sock, expected):
    actual = recv_frame(sock)
    assert actual == expected, (
        f"预期响应 {expected!r}，实际响应 {actual!r}"
    )


def expect_eof(sock):
    data = sock.recv(1)
    assert data == b"", f"预期连接结束，却收到额外数据：{data!r}"


# 1. 两个顺序连接共享存储。
def test_shared_store():
    with connect() as client_a:
        client_a.sendall(encode_frame("SET verify_shared Alice"))
        expect_response(client_a, "OK")

    with connect() as client_b:
        client_b.sendall(encode_frame("GET verify_shared"))
        expect_response(client_b, "VALUE Alice")


# 2. 同一连接连续发送两个请求，响应必须按序返回。
def test_pipeline():
    with connect() as sock:
        requests = (
            encode_frame("SET verify_pipeline Bob")
            + encode_frame("GET verify_pipeline")
        )
        sock.sendall(requests)
        expect_response(sock, "OK")
        expect_response(sock, "VALUE Bob")


# 3. 完整请求后关闭发送方向，仍然能够接收响应。
def test_half_close():
    with connect() as sock:
        sock.sendall(encode_frame("PING"))
        sock.shutdown(socket.SHUT_WR)

        expect_response(sock, "PONG")
        expect_eof(sock)


# 4. 不完整请求遇到 EOF，不应执行或产生响应。
def test_incomplete_frame():
    with connect() as sock:
        # 清除旧记录，使测试可以重复运行。
        sock.sendall(encode_frame("DEL verify_half"))
        response = recv_frame(sock)
        assert response in ("DELETED 0", "DELETED 1")

        frame = encode_frame("SET verify_half Alice")
        sock.sendall(frame[:-2])  # 正文故意少两个字节。
        sock.shutdown(socket.SHUT_WR)

        expect_eof(sock)

    # 新连接确认：刚才的半帧没有修改存储。
    with connect() as sock:
        sock.sendall(encode_frame("GET verify_half"))
        expect_response(sock, "NOT_FOUND")


tests = [
    ("顺序连接共享存储", test_shared_store),
    ("连续请求与响应顺序", test_pipeline),
    ("完整请求后半关闭", test_half_close),
    ("半帧 EOF 不执行", test_incomplete_frame),
]

for name, test in tests:
    print(f"正在验收：{name}", flush=True)
    try:
        test()
    except Exception as error:
        print(f"FAIL：{name}：{error}")
        raise SystemExit(1)
    print(f"PASS：{name}")

print("S1.3 本组网络验收全部通过")
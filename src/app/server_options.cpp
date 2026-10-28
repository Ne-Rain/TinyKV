#include "server_options.hpp"
#include <cstring>
#include <limits>

bool parse_thread_count(const std::string& text, std::size_t& count_out) {
    if (text.empty()) {
        return false;
    }
    std::size_t cout = 0;
    const std::size_t limit = static_cast<std::size_t>(std::numeric_limits<std::size_t>::max());
    for (char c : text) {
        if (c < '0' || c > '9') {
            return false;
        }
        std::size_t digit = static_cast<std::size_t>(c - '0');
        if (cout > (limit - digit) / 10) {
            return false;
        }
        cout = cout * 10 + digit;
    }
    if (cout == 0 || cout > 64) {
        return false;
    }
    count_out = cout;
    return true;
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
        } else if (strcmp(argv[i], "--idle-timeout-ms") == 0 || strcmp(argv[i], "--frame-timeout-ms") == 0 ||
                   strcmp(argv[i], "--output-stall-timeout-ms") == 0 ||
                   strcmp(argv[i], "--shutdown-grace-ms") == 0) {
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
            } else if (name == "--frame-timeout-ms") {
                so.frame_timeout = timeout;
            } else if (name == "--output-stall-timeout-ms") {
                so.output_stall_timeout = timeout;
            } else {
                so.shutdown_grace = timeout;
            }
        } else if (strcmp(argv[i], "--mode") == 0) {
            if (i + 1 >= argc) {
                error_out = "Missing parameter value";
                return false;
            }
            ++i;
            if (strcmp(argv[i], "blocking") != 0 && strcmp(argv[i], "reactor") != 0 &&
                strcmp(argv[i], "pool") != 0) {
                error_out = "Unsupported mode: " + std::string(argv[i]);
                return false;
            }
            so.mode = argv[i];
        } else if (strcmp(argv[i], "--threads") == 0) {
            if (i + 1 >= argc) {
                error_out = "Missing parameter value";
                return false;
            }
            std::size_t thread_count;
            if (!parse_thread_count(argv[++i], thread_count)) {
                error_out = "The threads must be a decimal integer ranging from 1 to 64";
                return false;
            }
            so.threads = thread_count;
        } else {
            error_out = "Unkown option: " + std::string(argv[i]);
            return false;
        }
    }
    options_out = so;
    return true;
}


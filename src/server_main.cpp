#include "app/server_entrypoints.hpp"
#include <iostream>
#include <string>

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

    if (so.mode == "reactor" || so.mode == "pool") {
        return run_reactor_server(so);
    }

    std::cerr << "Unsupported mode: " << so.mode << std::endl;
    return 1;
}

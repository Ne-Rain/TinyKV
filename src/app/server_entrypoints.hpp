#pragma once

#include "server_options.hpp"

int run_blocking_server(const ServerOptions& options);
int run_reactor_server(const ServerOptions& so);

#include <csignal>
#include <cstdlib>
#include <iostream>
#include <thread>

#include "flexql/tcp_server.hpp"

namespace {
volatile std::sig_atomic_t g_stop = 0;

void on_signal(int) {
  g_stop = 1;
}
}  // namespace

int main(int argc, char** argv) {
  int port = 9090;
  if (argc >= 2) {
    port = std::atoi(argv[1]);
  }

  flexql::FlexQLServer server(port);
  if (!server.start()) {
    std::cerr << "failed to start FlexQL server on port " << port << "\n";
    return 1;
  }

  std::signal(SIGINT, on_signal);
  std::signal(SIGTERM, on_signal);

  std::cout << "FlexQL server listening on port " << port << "\n";
  while (!g_stop) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  server.stop();
  return 0;
}

// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#include "moxygen/moqtest/MoQTestServer.h"

namespace moxygen {

} // namespace moxygen

DEFINE_int32(port, 9999, "Port to listen on");

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, false);
  folly::Init init(&argc, &argv);

  // Initialize Server with correct port
  auto server = std::make_shared<moxygen::MoQTestServer>(FLAGS_port);

  std::cout << "\nEnter anything to exit." << std::endl;
  std::string input;
  std::getline(std::cin, input);
  std::cout << "\nExiting." << std::endl;

  return 0;
}

/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <atomic>
#include <cstdint>
#include <thread>

namespace moxygen::media_server {

// Loopback-only HTTP control surface for the disposable file_pr fixture.
class FilePrControlServer {
 public:
  explicit FilePrControlServer(uint16_t port);
  ~FilePrControlServer();

  FilePrControlServer(const FilePrControlServer&) = delete;
  FilePrControlServer& operator=(const FilePrControlServer&) = delete;

  bool start();
  void stop();

 private:
  void run();
  void handleClient(int fd);

  uint16_t port_;
  std::atomic<bool> running_{false};
  std::atomic<int> listenFd_{-1};
  std::thread thread_;
};

} // namespace moxygen::media_server

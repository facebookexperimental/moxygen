/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <folly/init/Init.h>
#include <gflags/gflags.h>

#include "moxygen/samples/echo_server/MoQAudioEchoServer.h"

DEFINE_string(cert, "", "Cert path");
DEFINE_string(key, "", "Key path");
DEFINE_string(endpoint, "/moq-echo", "End point");
DEFINE_int32(port, 9669, "Echo Server Port");

int main(int argc, char* argv[]) {
  folly::Init init(&argc, &argv, true);

  // Start server
  moxygen::MoQAudioEchoServer server(
      FLAGS_port, FLAGS_cert, FLAGS_key, FLAGS_endpoint);

  // Run until killed
  folly::EventBase evb;
  evb.loopForever();
  return 0;
}

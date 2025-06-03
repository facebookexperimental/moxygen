// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#include <folly/Expected.h>
#include <folly/coro/BlockingWait.h>
#include "folly/init/Init.h"
#include "folly/io/async/ScopedEventBaseThread.h"
#include "moxygen/moqtest/MoQTestClient.h"
#include "moxygen/moqtest/Utils.h"

namespace moxygen {
DEFINE_string(url, "http://localhost:9999", "URL to connect to");

const uint64_t kLastObjectInTrack = 10;
const uint64_t kLastGroupInTrack = 10;

} // namespace moxygen

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, false);
  folly::Init init(&argc, &argv);

  folly::ScopedEventBaseThread evb;

  // Initialize Client with url
  moxygen::MoQTestParameters defaultMoqParams;
  defaultMoqParams.lastObjectInTrack = moxygen::kLastObjectInTrack;
  defaultMoqParams.lastGroupInTrack = moxygen::kLastGroupInTrack;

  auto url = proxygen::URL(moxygen::FLAGS_url);
  std::shared_ptr<moxygen::MoQTestClient> client =
      std::make_shared<moxygen::MoQTestClient>(evb.getEventBase(), url);
  client->initialize();

  // Connect Client to Server
  XLOG(INFO) << "Connecting to " << url.getHostAndPort();
  folly::coro::blockingWait(
      client->connect(evb.getEventBase()).scheduleOn(evb.getEventBase()));

  XLOG(INFO) << "Subscribing to " << url.getHostAndPort();
  // Test a Subscribe Call
  folly::coro::blockingWait(
      client->subscribe(defaultMoqParams).scheduleOn(evb.getEventBase()));

  sleep(100);

  return 0;
}

// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#include <folly/coro/BlockingWait.h>
#include <folly/coro/Sleep.h>
#include <folly/init/Init.h>
#include <folly/io/async/ScopedEventBaseThread.h>
#include <folly/portability/GFlags.h>
#include <glog/logging.h>
#include <moxygen/tools/moqperf/MoQPerfClient.h>
#include <moxygen/tools/moqperf/MoQPerfServer.h>

// server (addr, port)
DEFINE_string(server_addr, "", "Server's address");
DEFINE_int32(server_port, 16001, "Server's port");

DEFINE_string(mode, "server", "Must be one of \"server\" or \"client\"");

using namespace moxygen;

static const uint32_t kDefaultConnectTimeoutMs = 60000;
static const uint32_t kDefaultTransactionTimeoutMs = 60000;

folly::coro::Task<void> connect(std::shared_ptr<MoQPerfClient> moqPerfClient) {
  co_await moqPerfClient->connect();
  co_return;
}

folly::coro::Task<moxygen::MoQPerfServer::SubscribeResult> subscribe(
    std::shared_ptr<MoQPerfClient> moqPerfClient,
    std::shared_ptr<MoQPerfClientTrackConsumer> trackConsumer) {
  auto subscribeResult = co_await moqPerfClient->subscribe(trackConsumer);
  co_return subscribeResult;
}

void runClient(folly::EventBase* evb) {
  auto moqPerfClient = std::make_shared<MoQPerfClient>(
      folly::SocketAddress(FLAGS_server_addr, FLAGS_server_port),
      evb,
      std::chrono::milliseconds(kDefaultConnectTimeoutMs),
      std::chrono::milliseconds(kDefaultTransactionTimeoutMs));

  folly::coro::blockingWait(connect(moqPerfClient).scheduleOn(evb));

  auto trackConsumer = std::make_shared<MoQPerfClientTrackConsumer>();
  auto subscribeResult = folly::coro::blockingWait(
      subscribe(moqPerfClient, trackConsumer).scheduleOn(evb));
  CHECK(subscribeResult.hasValue()) << "Subscription failed";

  // Wait for 10 seconds before unsubscribing
  sleep(10);

  evb->runInEventBaseThreadAndWait([&]() {
    auto subscribeHandle = subscribeResult.value();
    subscribeHandle->unsubscribe();
    moqPerfClient->drain();
  });

  LOG(INFO) << "Sent " << trackConsumer->getDataSent()
            << " bytes in 10 seconds.";
}

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, false);
  folly::Init init(&argc, &argv);

  CHECK(FLAGS_mode == "client" || FLAGS_mode == "server")
      << "Mode must be one of \"server\" or \"client\"";

  if (FLAGS_mode == "client") {
    folly::ScopedEventBaseThread evb;
    runClient(evb.getEventBase());
  } else if (FLAGS_mode == "server") {
    auto moqPerfServer =
        std::make_shared<MoQPerfServer>(FLAGS_server_port, "", "");
    std::cout << "\nEnter anything to exit." << std::endl;
    std::string input;
    std::getline(std::cin, input);
    std::cout << "\nExiting." << std::endl;
  }

  return 0;
}

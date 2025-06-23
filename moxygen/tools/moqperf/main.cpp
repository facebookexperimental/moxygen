/*
 *  Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 *  This source code is licensed under the MIT license found in the LICENSE
 *  file in the root directory of this source tree.
 *
 */

#include <folly/coro/BlockingWait.h>
#include <folly/coro/Sleep.h>
#include <folly/init/Init.h>
#include <folly/io/async/ScopedEventBaseThread.h>
#include <folly/portability/GFlags.h>
#include <glog/logging.h>
#include <moxygen/tools/moqperf/MoQPerfClient.h>
#include <moxygen/tools/moqperf/MoQPerfParams.h>
#include <moxygen/tools/moqperf/MoQPerfServer.h>
#include <moxygen/tools/moqperf/MoQPerfUtils.h>

#include <folly/Conv.h>
#include <folly/container/F14Map.h>
#include <folly/container/F14Set.h>

// server (addr, port)
DEFINE_string(server_addr, "", "Server's address");
DEFINE_int32(server_port, 16001, "Server's port");

DEFINE_string(mode, "server", "Must be one of \"server\" or \"client\"");

// Client Flags
DEFINE_uint64(objects_per_subgroup, 10, "Number of objects per subgroup");
DEFINE_uint64(subgroups_per_group, 10, "Number of subgroups per group");
DEFINE_uint64(object_size, 512, "Size of each object in bytes");
DEFINE_bool(send_end_of_flag_markers, false, "Send end of flag markers");
DEFINE_string(
    request_type,
    "subscribe",
    "Request type: Must be one of \"subscribe\" or \"fetch\"");

using namespace moxygen;

folly::coro::Task<void> connect(std::shared_ptr<MoQPerfClient> moqPerfClient) {
  co_await moqPerfClient->connect();
  co_return;
}

folly::coro::Task<moxygen::MoQPerfServer::SubscribeResult> subscribe(
    std::shared_ptr<MoQPerfClient> moqPerfClient,
    std::shared_ptr<MoQPerfClientTrackConsumer> trackConsumer,
    moxygen::MoQPerfParams params) {
  auto subscribeResult =
      co_await moqPerfClient->subscribe(trackConsumer, params);
  co_return subscribeResult;
}

void runSubscribe(
    folly::EventBase* evb,
    std::shared_ptr<moxygen::MoQPerfClient> client,
    moxygen::MoQPerfParams params) {
  auto trackConsumer = std::make_shared<MoQPerfClientTrackConsumer>();
  auto subscribeResult = folly::coro::blockingWait(
      subscribe(client, trackConsumer, params).scheduleOn(evb));
  CHECK(subscribeResult.hasValue()) << "Subscription failed";

  sleep(10);

  evb->runInEventBaseThreadAndWait([&]() {
    auto subscribeHandle = subscribeResult.value();
    subscribeHandle->unsubscribe();
    client->drain();
  });

  LOG(INFO) << "Sent " << trackConsumer->getDataSent()
            << " bytes in 10 seconds.";
}

void runFetch(
    folly::EventBase* evb,
    std::shared_ptr<moxygen::MoQPerfClient> client,
    moxygen::MoQPerfParams params) {
  auto fetchConsumer = std::make_shared<MoQPerfClientFetchConsumer>();
  auto fetchResult = folly::coro::blockingWait(
      client->fetch(fetchConsumer, params).scheduleOn(evb));
  CHECK(fetchResult.hasValue()) << "Fetch failed";

  sleep(10);

  evb->runInEventBaseThreadAndWait([&]() {
    auto fetchHandle = fetchResult.value();
    fetchHandle->fetchCancel();
    client->drain();
  });

  LOG(INFO) << "Received " << fetchConsumer->getFetchDataSent()
            << " bytes in 10 seconds.";
}

void runClient(folly::EventBase* evb) {
  // Create MoQPerfParams From Flags
  RequestType req = (FLAGS_request_type == "subscribe") ? RequestType::SUBSCRIBE
                                                        : RequestType::FETCH;
  moxygen::MoQPerfParams params{
      FLAGS_objects_per_subgroup,
      FLAGS_subgroups_per_group,
      FLAGS_object_size,
      FLAGS_send_end_of_flag_markers,
      req};

  auto moqPerfClient = std::make_shared<MoQPerfClient>(
      folly::SocketAddress(FLAGS_server_addr, FLAGS_server_port),
      evb,
      std::chrono::milliseconds(60000),
      std::chrono::milliseconds(60000));

  folly::coro::blockingWait(connect(moqPerfClient).scheduleOn(evb));

  if (params.request == RequestType::SUBSCRIBE) {
    runSubscribe(evb, moqPerfClient, params);
  } else if (params.request == RequestType::FETCH) {
    runFetch(evb, moqPerfClient, params);
  }
}

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, false);
  folly::Init init(&argc, &argv);

  CHECK(FLAGS_mode == "client" || FLAGS_mode == "server")
      << "Mode must be one of \"server\" or \"client\"";
  CHECK(FLAGS_request_type == "subscribe" || FLAGS_request_type == "fetch")
      << "Request type must be one of \"subscribe\" or \"fetch\"";

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

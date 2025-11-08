/*
 *  Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 *  This source code is licensed under the MIT license found in the LICENSE
 *  file in the root directory of this source tree.
 *
 */

#include <folly/coro/BlockingWait.h>
#include "folly/init/Init.h"
#include "folly/io/async/ScopedEventBaseThread.h"
#include "moxygen/moqtest/MoQTestClient.h"

namespace moxygen {

} // namespace moxygen

DEFINE_string(url, "http://localhost:9999", "URL to connect to");
DEFINE_int64(forwarding_preference, 0, "Forwarding preference");
DEFINE_uint64(start_group, moxygen::kDefaultStart, "Start group for MoQParams");
DEFINE_uint64(
    start_object,
    moxygen::kDefaultStart,
    "Start object for MoQParams");
DEFINE_uint64(
    last_group,
    moxygen::kDefaultLastGroupInTrack,
    "Last group for MoQParams");
DEFINE_uint64(
    objects_per_group,
    moxygen::kDefaultObjectsPerGroup,
    "Objects per group");
DEFINE_uint64(
    size_of_object_zero,
    moxygen::kDefaultSizeOfObjectZero,
    "Size of object zero");
DEFINE_uint64(
    size_of_object_greater_than_zero,
    moxygen::kDefaultSizeOfObjectGreaterThanZero,
    "Size of object nonzero");
DEFINE_uint64(
    object_frequency,
    moxygen::kDefaultObjectFrequency,
    "Object frequency");
DEFINE_uint64(group_increment, moxygen::kDefaultIncrement, "Group increment");
DEFINE_uint64(object_increment, moxygen::kDefaultIncrement, "Object increment");
DEFINE_bool(send_end_of_group_markers, false, "Send end of group markers");
DEFINE_int64(test_integer_extension, -1, "Test integer extension");
DEFINE_int64(test_variable_extension, -1, "Test variable extension");
DEFINE_uint64(
    publisher_delivery_timeout,
    moxygen::kDefaultPublisherDeliveryTimeout,
    "Publisher delivery timeout");
DEFINE_uint64(
    last_object_in_track,
    moxygen::kLocationMax.object,
    "Last object in track");
DEFINE_uint64(
    delivery_timeout,
    0,
    "Delivery timeout in milliseconds (0 = disabled)");
DEFINE_string(
    request,
    "subscribe",
    "Request Type: must be one of \"subscribe\" or \"fetch\"");
DEFINE_bool(
    log,
    false,
    "Log to mlog file.  Default is false.  If true, will log to mlog file");
DEFINE_string(mlog_path, moxygen::kDefaultClientFilePath, "Path to mlog file.");

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, false);
  folly::Init init(&argc, &argv);

  folly::EventBase evb;
  XLOG(INFO) << "Starting MoQTestClient";

  // Initialize Client with url and moq params
  moxygen::MoQTestParameters defaultMoqParams;
  defaultMoqParams.forwardingPreference =
      moxygen::ForwardingPreference(FLAGS_forwarding_preference);
  defaultMoqParams.startGroup = FLAGS_start_group;
  defaultMoqParams.startObject = FLAGS_start_object;
  defaultMoqParams.lastGroupInTrack = FLAGS_last_group;
  defaultMoqParams.objectsPerGroup = FLAGS_objects_per_group;
  defaultMoqParams.sizeOfObjectZero = FLAGS_size_of_object_zero;
  defaultMoqParams.sizeOfObjectGreaterThanZero =
      FLAGS_size_of_object_greater_than_zero;
  defaultMoqParams.objectFrequency = FLAGS_object_frequency;
  defaultMoqParams.groupIncrement = FLAGS_group_increment;
  defaultMoqParams.objectIncrement = FLAGS_object_increment;
  defaultMoqParams.sendEndOfGroupMarkers = FLAGS_send_end_of_group_markers;
  defaultMoqParams.testIntegerExtension = FLAGS_test_integer_extension;
  defaultMoqParams.testVariableExtension = FLAGS_test_variable_extension;
  defaultMoqParams.publisherDeliveryTimeout = FLAGS_publisher_delivery_timeout;
  defaultMoqParams.deliveryTimeout = FLAGS_delivery_timeout;
  defaultMoqParams.lastObjectInTrack =
      FLAGS_last_object_in_track == moxygen::kLocationMax.object
      ? FLAGS_object_increment *
          (FLAGS_objects_per_group + (int)FLAGS_send_end_of_group_markers)
      : FLAGS_last_object_in_track;

  auto url = proxygen::URL(FLAGS_url);
  std::shared_ptr<moxygen::MoQTestClient> client =
      std::make_shared<moxygen::MoQTestClient>(&evb, url);

  std::shared_ptr<moxygen::MLogger> logger;
  if (FLAGS_log) {
    logger = std::make_shared<moxygen::MLogger>(moxygen::VantagePoint::CLIENT);
    logger->setPath(FLAGS_mlog_path);
    client->setLogger(logger);
  }

  try {
    // Connect Client to Server
    XLOG(INFO) << "Connecting to " << url.getHostAndPort();
    folly::coro::blockingWait(
        folly::coro::co_withExecutor(&evb, client->connect(&evb)), &evb);

    if (FLAGS_request == "subscribe") {
      XLOG(INFO) << "Subscribing to " << url.getHostAndPort();
      // Test a Subscribe Call
      folly::coro::co_withExecutor(&evb, client->subscribe(defaultMoqParams))
          .start();
    } else if (FLAGS_request == "fetch") {
      XLOG(INFO) << "Fetching from " << url.getHostAndPort();
      // Test a Fetch Call
      folly::coro::co_withExecutor(&evb, client->fetch(defaultMoqParams))
          .start();
    } else {
      XLOG(ERR) << "Invalid Request Type: " << FLAGS_request;
    }
    // Run the event loop to process events and coroutines
    evb.loop();
  } catch (const std::exception& ex) {
    XLOG(ERR) << "Exception: " << ex.what();
    evb.loop();
    return 1;
  }
  return 0;
}

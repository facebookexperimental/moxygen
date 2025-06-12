// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#include <folly/Expected.h>
#include <folly/coro/BlockingWait.h>
#include "folly/init/Init.h"
#include "folly/io/async/ScopedEventBaseThread.h"
#include "moxygen/moqtest/MoQTestClient.h"
#include "moxygen/moqtest/Utils.h"

namespace moxygen {
DEFINE_string(url, "http://localhost:9999", "URL to connect to");
DEFINE_int64(forwarding_preference, 0, "Forwarding preference");
DEFINE_uint64(start_group, kDefaultStart, "Start group for MoQParams");
DEFINE_uint64(start_object, kDefaultStart, "Start object for MoQParams");
DEFINE_uint64(last_group, kDefaultLastGroupInTrack, "Last group for MoQParams");
DEFINE_uint64(objects_per_group, kDefaultObjectsPerGroup, "Objects per group");
DEFINE_uint64(
    size_of_object_zero,
    kDefaultSizeOfObjectZero,
    "Size of object zero");
DEFINE_uint64(
    size_of_object_greater_than_zero,
    kDefaultSizeOfObjectGreaterThanZero,
    "Size of object nonzero");
DEFINE_uint64(object_frequency, kDefaultObjectFrequency, "Object frequency");
DEFINE_uint64(group_increment, kDefaultIncrement, "Group increment");
DEFINE_uint64(object_increment, kDefaultIncrement, "Object increment");
DEFINE_bool(send_end_of_group_markers, false, "Send end of group markers");
DEFINE_int64(test_integer_extension, -1, "Test integer extension");
DEFINE_int64(test_variable_extension, -1, "Test variable extension");
DEFINE_uint64(
    publisher_delivery_timeout,
    kDefaultPublisherDeliveryTimeout,
    "Publisher delivery timeout");
DEFINE_uint64(
    last_object_in_track,
    FLAGS_objects_per_group + (int)FLAGS_send_end_of_group_markers,
    "Last object in track");
DEFINE_string(
    request,
    "subscribe",
    "Request Type: must be one of \"subscribe\" or \"fetch\"");

} // namespace moxygen

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, false);
  folly::Init init(&argc, &argv);

  folly::ScopedEventBaseThread evb;
  XLOG(INFO) << "Starting MoQTestClient";

  // Initialize Client with url and moq params
  moxygen::MoQTestParameters defaultMoqParams;
  defaultMoqParams.forwardingPreference =
      moxygen::ForwardingPreference(moxygen::FLAGS_forwarding_preference);
  defaultMoqParams.startGroup = moxygen::FLAGS_start_group;
  defaultMoqParams.startObject = moxygen::FLAGS_start_object;
  defaultMoqParams.lastGroupInTrack = moxygen::FLAGS_last_group;
  defaultMoqParams.objectsPerGroup = moxygen::FLAGS_objects_per_group;
  defaultMoqParams.sizeOfObjectZero = moxygen::FLAGS_size_of_object_zero;
  defaultMoqParams.sizeOfObjectGreaterThanZero =
      moxygen::FLAGS_size_of_object_greater_than_zero;
  defaultMoqParams.objectFrequency = moxygen::FLAGS_object_frequency;
  defaultMoqParams.groupIncrement = moxygen::FLAGS_group_increment;
  defaultMoqParams.objectIncrement = moxygen::FLAGS_object_increment;
  defaultMoqParams.sendEndOfGroupMarkers =
      moxygen::FLAGS_send_end_of_group_markers;
  defaultMoqParams.testIntegerExtension = moxygen::FLAGS_test_integer_extension;
  defaultMoqParams.testVariableExtension =
      moxygen::FLAGS_test_variable_extension;
  defaultMoqParams.publisherDeliveryTimeout =
      moxygen::FLAGS_publisher_delivery_timeout;
  defaultMoqParams.lastObjectInTrack = moxygen::FLAGS_last_object_in_track;

  auto url = proxygen::URL(moxygen::FLAGS_url);
  std::shared_ptr<moxygen::MoQTestClient> client =
      std::make_shared<moxygen::MoQTestClient>(evb.getEventBase(), url);
  client->initialize();

  // Connect Client to Server
  XLOG(INFO) << "Connecting to " << url.getHostAndPort();
  folly::coro::blockingWait(
      client->connect(evb.getEventBase()).scheduleOn(evb.getEventBase()));

  if (moxygen::FLAGS_request == "subscribe") {
    XLOG(INFO) << "Subscribing to " << url.getHostAndPort();
    // Test a Subscribe Call
    folly::coro::blockingWait(
        client->subscribe(defaultMoqParams).scheduleOn(evb.getEventBase()));
  } else if (moxygen::FLAGS_request == "fetch") {
    XLOG(INFO) << "Fetching from " << url.getHostAndPort();
    // Test a Fetch Call
    folly::coro::blockingWait(
        client->fetch(defaultMoqParams).scheduleOn(evb.getEventBase()));
  } else {
    XLOG(ERR) << "Invalid Request Type: " << moxygen::FLAGS_request;
  }
}

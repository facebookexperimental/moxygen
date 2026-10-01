/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <folly/coro/BlockingWait.h>
#include <folly/io/async/AsyncSignalHandler.h>
#include <csignal>
#include <limits>
#include "folly/init/Init.h"
#include "folly/io/async/ScopedEventBaseThread.h"
#include "moxygen/mlog/FileMLogger.h"
#include "moxygen/moqtest/MoQTestClient.h"
#include "moxygen/samples/util/Utils.h"

namespace {

using moxygen::AbsoluteLocation;

// Fills `out` from a "group,object" flag, leaving it unset for an empty flag.
bool parseLocationFlag(
    const std::string& flag,
    const std::string& value,
    std::optional<AbsoluteLocation>& out) {
  if (value.empty()) {
    return true;
  }
  auto parsed = moxygen::parseLocation(value);
  if (parsed.hasError()) {
    XLOG(ERR) << "Invalid --" << flag << "=" << value << ": "
              << parsed.error().what();
    return false;
  }
  out = parsed.value();
  return true;
}

// Fills `out` from a signed integer flag, leaving it unset for an empty flag.
bool parseInt64Flag(
    const std::string& flag,
    const std::string& value,
    std::optional<int64_t>& out) {
  if (value.empty()) {
    return true;
  }
  auto parsed = folly::tryTo<int64_t>(value);
  if (parsed.hasError()) {
    XLOG(ERR) << "Invalid --" << flag << "=" << value
              << ": expected an integer";
    return false;
  }
  out = parsed.value();
  return true;
}

} // namespace

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
    start_location,
    "",
    "With --request=fetch, the start location as \"group,object\". "
    "Empty means the start of the track.");
DEFINE_string(
    end_location,
    "",
    "With --request=fetch, the end location as \"group,object\".  The object "
    "is exclusive, and an object of 0 selects all of the end group.  Empty "
    "means the end of the track.");
DEFINE_string(
    request,
    "subscribe",
    "Request Type: must be one of \"subscribe\", \"fetch\" or \"publish\". "
    "\"publish\" asks the relay for the track via SUBSCRIBE_TRACKS and "
    "PUBLISHes it on a second session to the same endpoint. It requires a "
    "relay, and only works when the whole namespace is specified.");
DEFINE_string(
    join_start,
    "",
    "With --request=subscribe, also send a joining FETCH that backfills what "
    "ran before the subscription. A non-negative value is the absolute group "
    "to fetch from; a negative value counts that many groups back from where "
    "the subscription begins. Empty means a plain SUBSCRIBE.");
DEFINE_string(
    publish_order,
    "subscribe_first",
    "With --request=publish, whether to send SUBSCRIBE_TRACKS before the "
    "PUBLISH (\"subscribe_first\", the default) or after (\"publish_first\").");
DEFINE_bool(
    log,
    false,
    "Log to mlog file.  Default is false.  If true, will log to mlog file");
DEFINE_string(mlog_path, moxygen::kDefaultClientFilePath, "Path to mlog file.");
DEFINE_string(
    transport,
    "h3wt",
    "Client transport: 'quic' (raw QUIC), 'h3wt' (HTTP/3 + WebTransport, "
    "default), 'qmux' (QMUX-on-TCP, TLS via Fizz mandatory).");
DEFINE_bool(
    quic_transport,
    false,
    "DEPRECATED: use --transport=quic (or --transport=h3wt) instead. "
    "Selects raw QUIC vs WebTransport.");
DEFINE_string(
    versions,
    "",
    "Comma-separated MoQ draft versions (e.g. '14,16'). Empty = all supported.");
DEFINE_uint64(
    datagram_drops_allowed_percentage,
    moxygen::kDefaultDatagramDropPercentage,
    "Allowed datagram drop percentage for DATAGRAM forwarding (default 1%)");

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, false);
  folly::Init init(&argc, &argv);
  auto transportType =
      moxygen::samples::selectClientTransport("transport", "quic_transport");

  if (FLAGS_publish_order != "subscribe_first" &&
      FLAGS_publish_order != "publish_first") {
    XLOG(ERR) << "Invalid --publish_order: " << FLAGS_publish_order
              << " (expected \"subscribe_first\" or \"publish_first\")";
    return 1;
  }
  if (FLAGS_publish_order != "subscribe_first" && FLAGS_request != "publish") {
    XLOG(ERR) << "--publish_order only applies with --request=publish";
    return 1;
  }
  auto publishOrder = FLAGS_publish_order == "publish_first"
      ? moxygen::PublishOrder::PublishFirst
      : moxygen::PublishOrder::SubscribeFirst;

  std::optional<moxygen::AbsoluteLocation> startLocation;
  std::optional<moxygen::AbsoluteLocation> endLocation;
  if (!parseLocationFlag(
          "start_location", FLAGS_start_location, startLocation) ||
      !parseLocationFlag("end_location", FLAGS_end_location, endLocation)) {
    return 1;
  }
  if ((startLocation || endLocation) && FLAGS_request != "fetch") {
    XLOG(ERR) << "--start_location/--end_location only apply with "
                 "--request=fetch";
    return 1;
  }

  std::optional<int64_t> joinStart;
  if (!parseInt64Flag("join_start", FLAGS_join_start, joinStart)) {
    return 1;
  }
  if (joinStart && FLAGS_request != "subscribe") {
    XLOG(ERR) << "--join_start only applies with --request=subscribe";
    return 1;
  }
  // A negative value is negated to get the number of groups to count back,
  // which INT64_MIN has no positive counterpart for.
  if (joinStart == std::numeric_limits<int64_t>::min()) {
    XLOG(ERR) << "--join_start=" << FLAGS_join_start << " is out of range";
    return 1;
  }

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
  defaultMoqParams.datagramDropPercentage =
      FLAGS_datagram_drops_allowed_percentage;
  defaultMoqParams.lastObjectInTrack =
      FLAGS_last_object_in_track == moxygen::kLocationMax.object
      ? FLAGS_object_increment *
          (FLAGS_objects_per_group + (int)FLAGS_send_end_of_group_markers)
      : FLAGS_last_object_in_track;

  std::optional<moxygen::StandaloneFetch> fetchRange;
  if (startLocation || endLocation) {
    // Whichever end the flags left out stays at the track's own boundary.
    auto range = moxygen::wholeTrackFetch(defaultMoqParams);
    if (startLocation) {
      range.start = *startLocation;
    }
    if (endLocation) {
      range.end = *endLocation;
    }
    fetchRange = range;
  }

  auto url = proxygen::URL(FLAGS_url);
  std::shared_ptr<moxygen::MoQTestClient> client =
      moxygen::MoQTestClient::create(&evb, url, transportType);

  std::shared_ptr<moxygen::MLogger> logger;
  if (FLAGS_log) {
    logger = std::make_shared<moxygen::FileMLogger>(
        moxygen::VantagePoint::CLIENT, FLAGS_mlog_path);
    client->setLogger(logger);
  }

  // Drain on SIGINT/SIGTERM. We don't terminate the loop here: draining closes
  // the session, which flushes CONNECTION_CLOSE and lets evb.loop() return.
  class SigHandler : public folly::AsyncSignalHandler {
   public:
    SigHandler(folly::EventBase* evb, std::shared_ptr<moxygen::MoQTestClient> c)
        : folly::AsyncSignalHandler(evb), client_(std::move(c)) {
      registerSignalHandler(SIGINT);
      registerSignalHandler(SIGTERM);
    }
    void signalReceived(int) noexcept override {
      client_->shutdown();
      unreg();
    }

    void unreg() {
      unregisterSignalHandler(SIGINT);
      unregisterSignalHandler(SIGTERM);
    }

   private:
    std::shared_ptr<moxygen::MoQTestClient> client_;
  };
  SigHandler sigHandler(&evb, client);

  try {
    // Connect Client to Server
    XLOG(INFO) << "Connecting to " << url.getHostAndPort();
    folly::coro::blockingWait(
        folly::coro::co_withExecutor(
            &evb, client->connect(&evb, FLAGS_versions)),
        &evb);

    // Close the session and unregister the signal handler when the request
    // completes, so evb.loop() can return; between them they are what keeps
    // the loop alive.
    auto onComplete = [&sigHandler, &client](auto&&) {
      client->shutdown();
      sigHandler.unreg();
    };
    if (FLAGS_request == "subscribe" && joinStart) {
      XLOG(INFO) << "Joining from group " << *joinStart << " at "
                 << url.getHostAndPort();
      folly::coro::co_withExecutor(
          &evb, client->join(defaultMoqParams, *joinStart))
          .start()
          .via(&evb)
          .thenTry(onComplete);
    } else if (FLAGS_request == "subscribe") {
      XLOG(INFO) << "Subscribing to " << url.getHostAndPort();
      folly::coro::co_withExecutor(&evb, client->subscribe(defaultMoqParams))
          .start()
          .via(&evb)
          .thenTry(onComplete);
    } else if (FLAGS_request == "fetch") {
      XLOG(INFO) << "Fetching from " << url.getHostAndPort();
      folly::coro::co_withExecutor(
          &evb, client->fetch(defaultMoqParams, fetchRange))
          .start()
          .via(&evb)
          .thenTry(onComplete);
    } else if (FLAGS_request == "publish") {
      XLOG(INFO) << "Requesting PUBLISH from " << url.getHostAndPort();
      folly::coro::co_withExecutor(
          &evb,
          client->publishTrack(defaultMoqParams, FLAGS_versions, publishOrder))
          .start()
          .via(&evb)
          .thenTry(onComplete);
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

/*
 *  Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 *  This source code is licensed under the MIT license found in the LICENSE
 *  file in the root directory of this source tree.
 *
 */

#include "moxygen/moqtest/MoQPerfTestClient.h"

#include <folly/experimental/coro/Sleep.h>
#include <folly/logging/xlog.h>
#include "moxygen/moqtest/Utils.h"
#include "moxygen/util/InsecureVerifierDangerousDoNotUseInProduction.h"

namespace moxygen {

DEFINE_int32(perf_connect_timeout, 1000, "connect timeout in ms for perf test");
DEFINE_int32(
    perf_transaction_timeout,
    1000,
    "transaction timeout in ms for perf test");

// Constants for moq-test scheme parameters
constexpr uint64_t kStartGroup = 0;

constexpr uint64_t kObjectInterval = 33; // ms
constexpr int kRequestId = 0;
constexpr const char* kTrackName = "test";

// ============================================================================
// SubscriberState implementation
// ============================================================================

SubscriberState::SubscriberState(
    MoQPerfTestClient& client,
    size_t id,
    MoQExecutor::KeepAlive executor,
    const proxygen::URL& url,
    bool useQuicTransport)
    : testClient_(client),
      id_(id),
      moqExecutor_(std::move(executor)),
      receiver_(
          std::make_shared<ObjectReceiver>(
              ObjectReceiver::SUBSCRIBE,
              std::shared_ptr<ObjectReceiverCallback>(
                  std::shared_ptr<void>(),
                  &callback_))) {
  if (useQuicTransport) {
    moqClient_ = std::make_unique<MoQClient>(
        moqExecutor_,
        url,
        std::make_shared<
            test::InsecureVerifierDangerousDoNotUseInProduction>());
  } else {
    moqClient_ = std::make_unique<MoQWebTransportClient>(
        moqExecutor_,
        url,
        std::make_shared<
            test::InsecureVerifierDangerousDoNotUseInProduction>());
  }
}

SubscriberState::~SubscriberState() {
  try {
    // Unsubscribe if we have a valid subHandle_
    if (subHandle_) {
      // Unsubscribe is a synchronous call; ignore errors
      subHandle_->unsubscribe();
      subHandle_.reset();
    }
    // Always drain the connection if one was established, regardless of
    // whether we successfully created a subscription. This ensures QUIC
    // connections are properly closed even if subscribe() failed.
    drain();
  } catch (const std::exception& ex) {
    XLOG(ERR) << "Subscriber " << id_ << " cleanup failed: " << ex.what();
  }
}

folly::coro::Task<void> SubscriberState::connect() {
  try {
    co_await moqClient_->setupMoQSession(
        std::chrono::milliseconds(FLAGS_perf_connect_timeout),
        std::chrono::seconds(FLAGS_perf_transaction_timeout),
        nullptr,
        nullptr,
        [] {
          quic::TransportSettings ts;
          ts.orderedReadCallbacks = true;
          return ts;
        }());
    XLOG(DBG2) << "Subscriber " << id_ << " connected successfully";
  } catch (const std::exception& ex) {
    XLOG(ERR) << "Subscriber " << id_ << " failed to connect: " << ex.what();
    hasError_ = true;
    throw;
  }
}

folly::coro::Task<void> SubscriberState::subscribe(
    const MoQTestParameters& params,
    uint32_t deliveryTimeoutMs) {
  auto trackNamespace = convertMoqTestParamToTrackNamespace(params);
  if (trackNamespace.hasError()) {
    XLOG(ERR) << "Subscriber " << id_
              << " failed to convert params to TrackNamespace: "
              << trackNamespace.error().what();
    hasError_ = true;
    co_return;
  }

  SubscribeRequest sub;
  sub.requestID = kRequestId;

  FullTrackName ftn;
  ftn.trackNamespace = trackNamespace.value();
  ftn.trackName = kTrackName;

  sub.fullTrackName = ftn;
  sub.groupOrder = GroupOrder::OldestFirst;
  sub.locType = LocationType::NextGroupStart;
  sub.endGroup = std::numeric_limits<uint32_t>::max();

  // Add delivery timeout parameter
  if (deliveryTimeoutMs > 0) {
    sub.params.insertParam(
        {folly::to_underlying(TrackRequestParamKey::DELIVERY_TIMEOUT),
         static_cast<uint64_t>(deliveryTimeoutMs)});
  }

  auto res = co_await moqClient_->moqSession_->subscribe(sub, receiver_);

  if (!res.hasError()) {
    subHandle_ = res.value();
    XLOG(DBG2) << "Subscriber " << id_ << " subscribed successfully";

    // Check if SubscribeOk.largest indicates a track restart
    // Only consider it a restart if more than 2 groups behind (to avoid false
    // positives from timing)
    const auto& subscribeOk = subHandle_->subscribeOk();
    auto largestSeen = testClient_.getLargestObjectSeen();
    if (largestSeen.has_value()) {
      auto subOkLargest =
          subscribeOk.largest.value_or(AbsoluteLocation{kStartGroup, 0});

      // Only check if subscribeOk is behind largestSeen
      if (subOkLargest.group < largestSeen->group) {
        auto groupsBack = largestSeen->group - subOkLargest.group;
        if (groupsBack > 2) {
          XLOG(INFO) << "Subscriber " << id_ << " detected track restart - "
                     << "SubscribeOk.largest (" << subOkLargest.group << ":"
                     << subOkLargest.object << ") is " << groupsBack
                     << " groups behind largestSeen (" << largestSeen->group
                     << ":" << largestSeen->object
                     << ") - unsubscribing immediately";

          // Signal that track restarted - stop adding new subscribers
          testClient_.recordTrackRestart();

          // Unsubscribe immediately - this subscriber joined a restarted track
          subHandle_->unsubscribe();
          subHandle_.reset();
          hasError_ = true;
          drain();
          co_return;
        }
      }
    }
  } else {
    XLOG(ERR) << "Subscriber " << id_ << " failed to subscribe. Error code: "
              << static_cast<uint64_t>(res.error().errorCode)
              << ", Reason: " << res.error().reasonPhrase;
    hasError_ = true;
  }
  drain();
}

void SubscriberState::drain() {
  if (moqClient_ && moqClient_->moqSession_) {
    moqClient_->moqSession_->drain();
  }
}

// SubscriberState::Callback implementation

ObjectReceiverCallback::FlowControlState SubscriberState::Callback::onObject(
    std::optional<TrackAlias> /* trackAlias */,
    const ObjectHeader& objHeader,
    Payload payload) {
  state_.objectsReceived_++;
  if (payload) {
    state_.bytesReceived_ += payload->computeChainDataLength();
  }

  // Update largest object seen for track restart detection
  AbsoluteLocation location(objHeader.group, objHeader.id);
  state_.testClient_.updateLargestObjectSeen(location);

  return FlowControlState::UNBLOCKED;
}

void SubscriberState::Callback::onObjectStatus(
    std::optional<TrackAlias> /* trackAlias */,
    const ObjectHeader& /* objHeader */) {
  // No-op for performance test
}

void SubscriberState::Callback::onEndOfStream() {
  // No-op for performance test
}

void SubscriberState::Callback::onError(ResetStreamErrorCode code) {
  XLOG(ERR) << "Subscriber " << state_.id_
            << " received stream reset: " << static_cast<uint64_t>(code);
  // Don't unsubscribe immediately - let removeSubscriber handle it
  state_.testClient_.recordReset();
}

void SubscriberState::Callback::onPublishDone(PublishDone done) {
  XLOG(DBG1) << "Subscriber " << state_.id_
             << " received PublishDone - status: "
             << static_cast<uint32_t>(done.statusCode)
             << ", reason: " << done.reasonPhrase;

  // Library has already cleaned up subscription state; just release our handle
  state_.subHandle_.reset();

  // Only signal completion if track ended naturally
  if (done.statusCode == PublishDoneStatusCode::TRACK_ENDED) {
    XLOG(DBG1) << "Subscriber " << state_.id_ << " - track ended naturally";
    state_.testClient_.completed();
  } else {
    // Other status codes (errors, going away, etc.) don't end the test
    XLOG(DBG1) << "Subscriber " << state_.id_
               << " - PublishDone with non-TRACK_ENDED status, not ending test";
  }
}

void SubscriberState::Callback::onAllDataReceived() {
  XLOG(DBG1) << "Subscriber " << state_.id_
             << " - all data received, removing subscriber";
  // Remove this subscriber from the client's map now that all streams are done
  state_.testClient_.removeSubscriber(state_.id_);
}

// ============================================================================
// MoQPerfTestClient implementation
// ============================================================================

MoQPerfTestClient::MoQPerfTestClient(
    folly::EventBase* evb,
    proxygen::URL url,
    bool useQuicTransport,
    uint32_t durationSeconds,
    uint32_t maxSubscribersPerSecond,
    uint32_t maxSubscribers,
    uint32_t firstObjectSize,
    uint32_t otherObjectSize,
    uint32_t deliveryTimeoutMs,
    uint32_t objectsPerGroup)
    : evb_(evb),
      url_(std::move(url)),
      useQuicTransport_(useQuicTransport),
      durationSeconds_(durationSeconds),
      maxSubscribersPerSecond_(maxSubscribersPerSecond),
      maxSubscribers_(maxSubscribers),
      deliveryTimeoutMs_(deliveryTimeoutMs),
      sharedExecutor_(std::make_unique<MoQFollyExecutorImpl>(evb)) {
  // Initialize MoQ test parameters for moq-test scheme
  params_.forwardingPreference = ForwardingPreference::ONE_SUBGROUP_PER_GROUP;
  params_.objectsPerGroup = objectsPerGroup;
  params_.sizeOfObjectZero = firstObjectSize;
  params_.sizeOfObjectGreaterThanZero = otherObjectSize;
  params_.objectFrequency = kObjectInterval;
  params_.sendEndOfGroupMarkers = false;
  params_.testIntegerExtension = -1;  // no extensions
  params_.testVariableExtension = -1; // no extensions
  params_.deliveryTimeout = 0;        // don't set the server's delivery timeout
  params_.lastGroupInTrack =
      durationSeconds; // Triggers track end from publisher
  params_.startGroup = kStartGroup;
  params_.startObject = 0;
  params_.groupIncrement = 1;
  params_.objectIncrement = 1;
  params_.lastObjectInTrack = objectsPerGroup;
}

folly::coro::Task<void> MoQPerfTestClient::run() {
  startTime_ = std::chrono::steady_clock::now();
  auto hardDeadline = startTime_ + std::chrono::seconds(durationSeconds_) +
      std::chrono::seconds(5);

  auto currentIncrement = maxSubscribersPerSecond_;
  auto token = co_await folly::coro::co_current_cancellation_token;
  auto shouldContinue = [&]() {
    return !token.isCancellationRequested() && numCompleted_ == 0 &&
        !trackRestarted_ && std::chrono::steady_clock::now() < hardDeadline;
  };

  // Adaptive loop: continuously adjust subscriber count based on resets
  while (shouldContinue()) {
    // Clear interval counters before adding subscribers
    resetsInCurrentInterval_ = 0;
    failuresInCurrentInterval_ = 0;

    // === Add subscribers gradually (spread over 1 second)
    uint32_t subscribersToAdd =
        std::min(currentIncrement, maxSubscribersPerSecond_);
    // Cap the number of subscribers to add by remaining capacity
    if (subscribers_.size() >= maxSubscribers_) {
      XLOG(INFO) << "Max subscribers reached (" << maxSubscribers_
                 << ") - not adding more this interval";
      subscribersToAdd = 0;
    } else {
      auto remainingCapacity = maxSubscribers_ - subscribers_.size();
      subscribersToAdd =
          std::min(subscribersToAdd, static_cast<uint32_t>(remainingCapacity));
    }

    uint32_t sleepIntervalMs =
        subscribersToAdd > 0 ? 1000 / subscribersToAdd : 0;

    uint32_t subscribersAddedInBatch = 0;
    for (uint32_t i = 0; shouldContinue() && i < subscribersToAdd; i++) {
      folly::coro::co_withExecutor(evb_, addSubscriber()).start();
      subscribersAddedInBatch++;
      if (sleepIntervalMs > 0) {
        co_await folly::coro::sleepReturnEarlyOnCancel(
            std::chrono::milliseconds(sleepIntervalMs));
      }
    }
    XLOG(INFO) << "Added " << subscribersAddedInBatch
               << " subscribers (increment: " << currentIncrement << ")";

    // === Wait 2 seconds to observe if resets or failures occur
    co_await folly::coro::sleepReturnEarlyOnCancel(std::chrono::seconds(2));

    // === Process stats and adjust subscriber increment
    auto totalErrors = resetsInCurrentInterval_ + failuresInCurrentInterval_;
    if (totalErrors > 0) {
      // Errors detected - back down by half of current increment
      currentIncrement = std::max(1u, currentIncrement / 2);
      XLOG(INFO) << "Errors detected (" << resetsInCurrentInterval_
                 << " resets, " << failuresInCurrentInterval_
                 << " failures) during interval - reducing increment to: "
                 << currentIncrement;
    } else if (currentIncrement < maxSubscribersPerSecond_) {
      // No errors and we're below target - increase increment
      currentIncrement =
          std::min(maxSubscribersPerSecond_, currentIncrement * 2);
      XLOG(INFO) << "No errors detected - increasing increment to: "
                 << currentIncrement;
    }
  }

  // Log why we exited
  if (trackRestarted_) {
    XLOG(INFO) << "Track restart detected - stopping subscriber additions, "
               << subscribers_.size() << " subscribers remaining";
  } else if (numCompleted_ > 0) {
    XLOG(INFO) << "Track ended (PublishDone received) - test complete, "
               << subscribers_.size() << " subscribers remaining";
  } else if (std::chrono::steady_clock::now() >= hardDeadline) {
    XLOG(INFO) << "Hard deadline reached (duration + 5s) - forcing exit, "
               << subscribers_.size() << " subscribers remaining";
  } else {
    XLOG(INFO) << "Test cancelled";
  }

  // Clear all subscribers to trigger cleanup
  subscribers_.clear();
}

folly::coro::Task<void> MoQPerfTestClient::addSubscriber() {
  auto token = co_await folly::coro::co_current_cancellation_token;
  if (token.isCancellationRequested()) {
    co_return;
  }

  auto id = subscribersAdded_++;

  try {
    auto subscriber = std::make_unique<SubscriberState>(
        *this, id, sharedExecutor_->keepAlive(), url_, useQuicTransport_);

    // Connect the subscriber
    co_await subscriber->connect();

    // Subscribe to the track
    co_await subscriber->subscribe(params_, deliveryTimeoutMs_);

    // Check if subscriber had an error during setup
    if (subscriber->hasError_) {
      XLOG(DBG1) << "Subscriber " << id << " had error during setup";
      recordFailure();
      co_return;
    }
    // Add to subscribers map
    XLOG(DBG1) << "Added subscriber " << id;
    subscribers_[id] = std::move(subscriber);

    // Update peak subscribers
    auto currentSize = subscribers_.size();
    auto currentPeak = peakSubscribers_.load();
    while (currentSize > currentPeak &&
           !peakSubscribers_.compare_exchange_weak(currentPeak, currentSize)) {
      // Loop until we successfully update the peak
    }

    XLOG(DBG1) << "Added subscriber " << id
               << " (total: " << subscribers_.size() << ")";

  } catch (const std::exception& ex) {
    XLOG(ERR) << "Failed to add subscriber " << id << ": " << ex.what();
    recordFailure();
  }
}

void MoQPerfTestClient::removeSubscriber(size_t id) {
  auto it = subscribers_.find(id);
  if (it == subscribers_.end()) {
    return;
  }

  XLOG(DBG1) << "Removing subscriber " << id
             << " (current total: " << subscribers_.size() << ")";

  // Add subscriber's stats to cumulative totals before removing
  cumulativeObjects_ += it->second->objectsReceived_;
  cumulativeBytes_ += it->second->bytesReceived_;

  // Destructor will handle unsubscribe
  subscribers_.erase(it);
}

void MoQPerfTestClient::completed() {
  numCompleted_++;
  XLOG(DBG1) << "Subscriber completed (total: " << numCompleted_ << ")";
}

void MoQPerfTestClient::recordReset() {
  resetsInCurrentInterval_++;
  totalResets_++;
}

void MoQPerfTestClient::recordFailure() {
  failuresInCurrentInterval_++;
  totalFailures_++;
}

void MoQPerfTestClient::recordTrackRestart() {
  trackRestarted_ = true;
}

void MoQPerfTestClient::updateLargestObjectSeen(
    const AbsoluteLocation& location) {
  if (!largestObjectSeen_.has_value() ||
      location > largestObjectSeen_.value()) {
    largestObjectSeen_ = location;
  }
}

std::optional<AbsoluteLocation> MoQPerfTestClient::getLargestObjectSeen()
    const {
  return largestObjectSeen_;
}

MoQPerfTestClient::TestResults MoQPerfTestClient::getResults() const {
  TestResults results;
  results.subscribersReached = peakSubscribers_.load();
  results.currentSubscribers = subscribers_.size();
  results.totalResets = totalResets_.load();
  results.totalFailures = totalFailures_.load();
  results.trackEnded = (numCompleted_ > 0);

  // Combine cumulative stats with current active subscribers
  results.totalObjects = cumulativeObjects_.load();
  results.totalBytes = cumulativeBytes_.load();

  for (const auto& [id, sub] : subscribers_) {
    results.totalObjects += sub->objectsReceived_;
    results.totalBytes += sub->bytesReceived_;
  }

  auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
                     std::chrono::steady_clock::now() - startTime_)
                     .count();
  results.durationSeconds = elapsed;

  return results;
}

} // namespace moxygen

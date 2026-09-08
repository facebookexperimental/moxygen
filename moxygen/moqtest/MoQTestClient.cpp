/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/moqtest/MoQTestClient.h"

#include <utility>
#include "moxygen/moqtest/Utils.h"
#include "moxygen/samples/util/Utils.h"
#include "moxygen/util/InsecureVerifierDangerousDoNotUseInProduction.h"

namespace moxygen {

DEFINE_int32(connect_timeout, 1000, "connect timeout in ms");
DEFINE_int32(transaction_timeout, 1000, "transaction timeout in ms");
const int kDefaultRequestId = 0;
const std::string kDefaultTrackName = "test";
const GroupOrder kDefaultGroupOrder = GroupOrder::OldestFirst;
const LocationType kDefaultLocationType = LocationType::NextGroupStart;
const uint64_t kDefaultEndGroup = 10;

MoQTestClient::MoQTestClient(
    PrivateTag,
    folly::EventBase* evb,
    proxygen::URL url,
    samples::TransportType transportType)
    : url_(std::move(url)),
      transportType_(transportType),
      moqExecutor_(std::make_shared<MoQFollyExecutorImpl>(evb)),
      subReceiver_(
          std::make_shared<VerifyingObjectReceiver>(
              ObjectReceiver::SUBSCRIBE,
              std::shared_ptr<ObjectReceiverCallback>(
                  std::shared_ptr<void>(),
                  &subCallback_),
              *this,
              subState_)),
      fetchReceiver_(
          std::make_shared<VerifyingObjectReceiver>(
              ObjectReceiver::FETCH,
              std::shared_ptr<ObjectReceiverCallback>(
                  std::shared_ptr<void>(),
                  &fetchCallback_),
              *this,
              fetchState_)) {}

void MoQTestClient::setLogger(const std::shared_ptr<MLogger>& logger) {
  logger_ = logger;
  if (moqClient_) {
    moqClient_->setLogger(logger);
  }
}

void MoQTestClient::shutdown() {
  tearingDown_ = true;
  // Cancel the active request first: drain() only closes once there are no
  // active subscriptions, otherwise it waits for the whole track.
  if (subHandle_) {
    subHandle_->unsubscribe();
    subHandle_.reset();
  }
  if (fetchHandle_) {
    fetchHandle_->fetchCancel();
    fetchHandle_.reset();
  }
  if (subscribeTracksHandle_) {
    subscribeTracksHandle_->unsubscribeTracks();
    subscribeTracksHandle_.reset();
  }
  if (publisher_) {
    publisher_->cancelAll();
  }
  if (moqClient_ && moqClient_->moqSession_) {
    moqClient_->moqSession_->drain();
  }
  if (pubClient_ && pubClient_->moqSession_) {
    pubClient_->moqSession_->drain();
  }
  doneBaton_.post();
}

folly::coro::Task<void> MoQTestClient::doSubscribeUpdate(
    std::shared_ptr<Publisher::SubscriptionHandle> handle,
    RequestUpdate update) {
  auto result = co_await handle->requestUpdate(std::move(update));
  if (result.hasError()) {
    XLOG(ERR) << "requestUpdate failed: error code="
              << static_cast<uint64_t>(result.error().errorCode)
              << ", reason=" << result.error().reasonPhrase;
  } else {
    XLOG(INFO) << "requestUpdate succeeded: requestID="
               << result.value().requestID.value;
  }
}

void MoQTestClient::subscribeUpdate(SubscribeUpdate update) {
  XLOG(DBG1) << "MoQTest DEBUGGING: calling subscribeUpdate";
  if (subState_.active && subHandle_) {
    folly::coro::co_withExecutor(
        moqExecutor_->getBackingEventBase(),
        doSubscribeUpdate(subHandle_, std::move(update)))
        .start();
  }
}

folly::coro::Task<std::unique_ptr<MoQClientBase>> MoQTestClient::connectSession(
    const std::string& versions,
    std::shared_ptr<Publisher> publishHandler,
    std::shared_ptr<Subscriber> subscribeHandler) {
  // The relay-session factory is required for SUBSCRIBE_TRACKS; a plain
  // MoQSession does not implement it.
  auto client = samples::makeRelayClientTransport(
      moqExecutor_,
      url_,
      MoQRelaySession::createRelaySessionFactory(),
      std::make_shared<test::InsecureVerifierDangerousDoNotUseInProduction>(),
      transportType_);

  co_await client->setupMoQSession(
      std::chrono::milliseconds(FLAGS_connect_timeout),
      std::chrono::seconds(FLAGS_transaction_timeout),
      std::move(publishHandler),
      std::move(subscribeHandler),
      [] {
        quic::TransportSettings ts;
        ts.orderedReadCallbacks = true;
        return ts;
      }(),
      getMoqtProtocols(versions, true));

  co_return client;
}

folly::coro::Task<void> MoQTestClient::connect(
    folly::EventBase* /*evb*/,
    const std::string& versions) {
  // Register as the subscribe handler so a relay-forwarded PUBLISH lands in
  // publish(); harmless in subscribe/fetch mode, where none arrives.
  moqClient_ = co_await connectSession(
      versions, /*publishHandler=*/nullptr, shared_from_this());
  if (logger_) {
    moqClient_->setLogger(logger_);
  }
  co_return;
}

folly::coro::Task<moxygen::TrackNamespace> MoQTestClient::subscribe(
    MoQTestParameters params) {
  auto trackNamespace = convertMoqTestParamToTrackNamespace(params);
  if (trackNamespace.hasError()) {
    XLOG(ERR)
        << "MoQTest verification result: "
        << "FAILURE! Reason: Error Converting Parameters to TrackNamespace: "
        << trackNamespace.error().what();
    moqClient_->moqSession_->drain();
    co_yield folly::coro::co_error(trackNamespace.error());
  }

  // Create a SubRequest with the created TrackNamespace as its fullTrackName
  SubscribeRequest sub;
  sub.requestID = kDefaultRequestId;
  requestID_ = kDefaultRequestId;

  FullTrackName ftn;
  ftn.trackNamespace = trackNamespace.value();
  ftn.trackName = kDefaultTrackName;

  sub.fullTrackName = ftn;
  sub.groupOrder = kDefaultGroupOrder;
  sub.locType = kDefaultLocationType;
  sub.endGroup = kDefaultEndGroup;

  // Add delivery timeout parameter if configured
  if (params.deliveryTimeout > 0) {
    sub.params.insertParam(
        {folly::to_underlying(TrackRequestParamKey::DELIVERY_TIMEOUT),
         params.deliveryTimeout});
  }

  // Set Current Request
  initializeExpecteds(params, resolveFetchWindow(params));
  startReceiving(subState_, ReceivingType::SUBSCRIBE);

  // Subscribe to the receiver
  auto res = co_await moqClient_->moqSession_->subscribe(sub, subReceiver_);
  armObjectDeadlines();

  if (!res.hasError()) {
    subHandle_ = res.value();
  } else {
    XLOG(ERR)
        << "MoQTest verification result: FAILURE! Reason: Error Subscribing to receiver. "
        << "Error code: " << static_cast<uint64_t>(res.error().errorCode)
        << ", Reason: " << res.error().reasonPhrase;
    co_yield folly::coro::co_error(
        std::runtime_error(
            folly::to<std::string>(
                "Error code: ",
                static_cast<uint64_t>(res.error().errorCode),
                ", Reason: ",
                res.error().reasonPhrase)));
  }

  co_await doneBaton_;
  co_return trackNamespace.value();
}

Subscriber::PublishResult MoQTestClient::publish(
    PublishRequest pub,
    std::shared_ptr<SubscriptionHandle> handle) {
  XLOG(INFO) << "MoQTest: received PUBLISH for ns="
             << pub.fullTrackName.trackNamespace;
  // Keep the handle so a validation failure can unsubscribe.
  if (handle) {
    subHandle_ = std::move(handle);
  }

  PublishOk ok;
  ok.requestID = pub.requestID;
  ok.forward = true;
  ok.subscriberPriority = kDefaultPriority;
  ok.groupOrder = GroupOrder::Default;
  ok.locType = LocationType::AbsoluteStart;
  ok.start = AbsoluteLocation(0, 0);

  return Subscriber::PublishConsumerAndReplyTask{
      subReceiver_,
      folly::coro::makeTask(
          folly::Expected<PublishOk, PublishError>(std::move(ok)))};
}

folly::coro::Task<void> MoQTestClient::subscribeTracks(
    const TrackNamespace& trackNamespace) {
  auto relaySession =
      std::dynamic_pointer_cast<MoQRelaySession>(moqClient_->moqSession_);
  if (!relaySession) {
    co_yield folly::coro::co_error(
        std::runtime_error("Session does not support SUBSCRIBE_TRACKS"));
  }

  SubscribeTracks subTracks;
  subTracks.requestID = kDefaultRequestId;
  subTracks.trackNamespacePrefix = trackNamespace;
  subTracks.forward = true;

  auto res = co_await relaySession->subscribeTracks(subTracks);
  if (res.hasError()) {
    co_yield folly::coro::co_error(
        std::runtime_error(
            folly::to<std::string>(
                "SUBSCRIBE_TRACKS failed. Error code: ",
                static_cast<uint64_t>(res.error().errorCode),
                ", Reason: ",
                res.error().reasonPhrase)));
  }
  subscribeTracksHandle_ = res.value();
}

folly::coro::Task<moxygen::TrackNamespace> MoQTestClient::publishTrack(
    MoQTestParameters params,
    const std::string& versions,
    PublishOrder order) {
  auto trackNamespace = convertMoqTestParamToTrackNamespace(params);
  if (trackNamespace.hasError()) {
    XLOG(ERR)
        << "MoQTest verification result: "
        << "FAILURE! Reason: Error Converting Parameters to TrackNamespace: "
        << trackNamespace.error().what();
    moqClient_->moqSession_->drain();
    co_yield folly::coro::co_error(trackNamespace.error());
  }

  requestID_ = kDefaultRequestId;
  initializeExpecteds(params, resolveFetchWindow(params));
  startReceiving(subState_, ReceivingType::SUBSCRIBE);

  auto onFailure = [this](const std::exception& ex) {
    XLOG(ERR) << "MoQTest verification result: FAILURE! Reason: " << ex.what();
    shutdown();
  };

  if (order == PublishOrder::SubscribeFirst) {
    // The relay has a SUBSCRIBE_TRACKS subscriber registered when our PUBLISH
    // arrives, so it answers with forward=1 and we stream immediately.
    auto subRes = co_await folly::coro::co_awaitTry(
        subscribeTracks(trackNamespace.value()));
    if (subRes.hasException()) {
      onFailure(std::runtime_error(subRes.exception().what().toStdString()));
      co_return trackNamespace.value();
    }
  }

  publisher_ = std::make_shared<MoQTestPublisher>();
  pubClient_ = co_await connectSession(
      versions, /*publishHandler=*/nullptr, /*subscribeHandler=*/nullptr);

  FullTrackName ftn;
  ftn.trackNamespace = trackNamespace.value();
  ftn.trackName = kDefaultTrackName;
  // The PUBLISH and the SUBSCRIBE_TRACKS travel on different sessions, so
  // waiting for PUBLISH_OK is the only thing that puts them in a known order
  // at the relay.
  auto streamTask =
      co_await folly::coro::co_awaitTry(publisher_->startPublishTrack(
          pubClient_->moqSession_,
          std::move(ftn),
          params,
          RequestID(kDefaultRequestId)));
  if (streamTask.hasException()) {
    onFailure(std::runtime_error(streamTask.exception().what().toStdString()));
    co_return trackNamespace.value();
  }

  if (order == PublishOrder::PublishFirst) {
    // Nothing was subscribed, so the relay answered PUBLISH_OK with forward=0
    // and the track stays paused until SUBSCRIBE_TRACKS makes the relay send a
    // REQUEST_UPDATE turning forwarding on.
    auto subRes = co_await folly::coro::co_awaitTry(
        subscribeTracks(trackNamespace.value()));
    if (subRes.hasException()) {
      onFailure(std::runtime_error(subRes.exception().what().toStdString()));
      co_return trackNamespace.value();
    }
  }

  auto pubRes =
      co_await folly::coro::co_awaitTry(std::move(streamTask.value()));
  if (pubRes.hasException()) {
    onFailure(std::runtime_error(pubRes.exception().what().toStdString()));
    co_return trackNamespace.value();
  }
  armObjectDeadlines();

  co_await doneBaton_;
  // Drained here rather than up front: draining the subscriber session before
  // the PUBLISH arrives would reject it.
  shutdown();
  co_return trackNamespace.value();
}

folly::coro::Task<moxygen::TrackNamespace> MoQTestClient::fetch(
    MoQTestParameters params,
    std::optional<StandaloneFetch> range) {
  auto trackNamespace = convertMoqTestParamToTrackNamespace(params);
  if (trackNamespace.hasError()) {
    XLOG(ERR)
        << "MoQTest verification result: "
        << "FAILURE! Reason: Error Converting Parameters to TrackNamespace: "
        << trackNamespace.error().what();
    moqClient_->moqSession_->drain();
    co_yield folly::coro::co_error(trackNamespace.error());
  }

  // Create a Fetch with the created TrackNamespace as its fullTrackName
  Fetch fetch;
  fetch.requestID = kDefaultRequestId;
  requestID_ = kDefaultRequestId;

  FullTrackName ftn;
  ftn.trackNamespace = trackNamespace.value();
  ftn.trackName = kDefaultTrackName;
  fetch.fullTrackName = ftn;
  fetch.groupOrder = kDefaultGroupOrder;
  const auto fetchRange = range.value_or(wholeTrackFetch(params));
  fetch.args = fetchRange;

  // Set Current Request
  initializeExpecteds(params, resolveFetchWindow(params, fetchRange));
  startReceiving(fetchState_, ReceivingType::FETCH);

  // Fetch to the receiver
  auto res = co_await moqClient_->moqSession_->fetch(fetch, fetchReceiver_);
  armObjectDeadlines();
  if (!res.hasError()) {
    fetchHandle_ = res.value();
    validateFetchOk(fetchHandle_->fetchOk(), params, fetchRange);
  } else {
    XLOG(ERR)
        << "MoQTest verification result: FAILURE! Reason: Error Fetching to receiver. "
        << "Error code: " << static_cast<uint64_t>(res.error().errorCode)
        << ", Reason: " << res.error().reasonPhrase;
    co_yield folly::coro::co_error(
        std::runtime_error(
            folly::to<std::string>(
                "Error code: ",
                static_cast<uint64_t>(res.error().errorCode),
                ", Reason: ",
                res.error().reasonPhrase)));
  }

  co_await doneBaton_;
  co_return trackNamespace.value();
}

folly::coro::Task<moxygen::TrackNamespace> MoQTestClient::join(
    MoQTestParameters params,
    int64_t joinStart) {
  auto trackNamespace = convertMoqTestParamToTrackNamespace(params);
  if (trackNamespace.hasError()) {
    XLOG(ERR)
        << "MoQTest verification result: FAILURE! Reason: Error Converting Parameters to TrackNamespace: "
        << trackNamespace.error().what();
    moqClient_->moqSession_->drain();
    co_yield folly::coro::co_error(trackNamespace.error());
  }

  SubscribeRequest sub;
  sub.requestID = kDefaultRequestId;
  requestID_ = kDefaultRequestId;
  sub.fullTrackName = {trackNamespace.value(), kDefaultTrackName};
  sub.groupOrder = kDefaultGroupOrder;
  // The backfill ends on the object the subscription starts after, so the two
  // halves only meet with no gap if the subscription starts at Largest.
  sub.locType = LocationType::LargestObject;

  if (params.deliveryTimeout > 0) {
    sub.params.insertParam(
        {folly::to_underlying(TrackRequestParamKey::DELIVERY_TIMEOUT),
         params.deliveryTimeout});
  }

  const bool relative = joinStart < 0;
  const uint64_t joiningStart =
      relative ? static_cast<uint64_t>(-joinStart) : joinStart;

  // Neither half starts where the client would guess: the publisher resolves
  // the backfill against its own Largest, and the subscription picks up from
  // there.
  initializeExpecteds(params, resolveFetchWindow(params));
  startReceiving(subState_, ReceivingType::SUBSCRIBE, /*seeded=*/false);
  startReceiving(fetchState_, ReceivingType::FETCH, /*seeded=*/false);

  auto res = co_await moqClient_->moqSession_->join(
      sub,
      subReceiver_,
      joiningStart,
      kDefaultPriority,
      kDefaultGroupOrder,
      {},
      fetchReceiver_,
      relative ? FetchType::RELATIVE_JOINING : FetchType::ABSOLUTE_JOINING);
  armObjectDeadlines();

  if (res.subscribeResult.hasError()) {
    XLOG(ERR)
        << "MoQTest verification result: FAILURE! Reason: Error Subscribing to receiver. "
        << "Error code: "
        << static_cast<uint64_t>(res.subscribeResult.error().errorCode)
        << ", Reason: " << res.subscribeResult.error().reasonPhrase;
    co_yield folly::coro::co_error(
        std::runtime_error(res.subscribeResult.error().reasonPhrase));
  }
  subHandle_ = res.subscribeResult.value();

  if (res.fetchResult.hasError()) {
    // A subscription with no Largest gives the join nothing to anchor to, and
    // covers the track from its start anyway.  The publisher reports that with
    // more than one error code, so key off the anchor.
    if (subHandle_->subscribeOk().largest) {
      XLOG(ERR)
          << "MoQTest verification result: FAILURE! Reason: Error Fetching to receiver. "
          << "Error code: "
          << static_cast<uint64_t>(res.fetchResult.error().errorCode)
          << ", Reason: " << res.fetchResult.error().reasonPhrase;
      co_yield folly::coro::co_error(
          std::runtime_error(res.fetchResult.error().reasonPhrase));
    }
    XLOG(INFO) << "MoQTest: joining FETCH backfills nothing, subscription "
               << "covers the whole track";
    fetchState_ = ReceiveState{};
    subState_.seeded = true;
  } else {
    fetchHandle_ = res.fetchResult.value();
    const auto& largest = subHandle_->subscribeOk().largest;
    if (!largest) {
      recordSemanticsFailure(
          "Joining FETCH accepted for a subscription with no Largest");
    } else {
      const uint64_t startGroup = joiningStartGroup(joinStart, *largest);
      validateJoiningFetchOk(
          fetchHandle_->fetchOk(), params, *largest, startGroup);
      trimExpectedBefore(startGroup);
      // Where the two halves met, so a caller can tell the join landed
      // mid-track.
      XLOG(INFO) << "MoQTest: joining FETCH backfills groups " << startGroup
                 << ".." << largest->group;
    }
  }

  co_await doneBaton_;
  co_return trackNamespace.value();
}

ObjectReceiverCallback::FlowControlState MoQTestClient::onObject(
    ReceiveState& state,
    const std::optional<TrackAlias>& /* trackAlias */,
    const ObjectHeader& objHeader,
    Payload payload) {
  XLOG(DBG1) << "MoQTest DEBUGGING: Calling onObject";

  // A zero-length object arrives with no payload buffer at all.
  auto payloadStr = payload ? payload->toString() : std::string();

  // Validate the received data
  if (!validateSubscribedData(state, objHeader, payloadStr)) {
    XLOG(ERR)
        << "MoQTest verification result: FAILURE! reason: Data Validation Failed";
    cancelRequest();
    moqClient_->moqSession_->close(SessionCloseErrorCode::PROTOCOL_VIOLATION);
    doneBaton_.post();
    return ObjectReceiverCallback::FlowControlState::UNBLOCKED;
  }

  // Adjust the expected data (If Still receiving data, leave unblocked)
  adjustExpected(state, params_, objHeader);
  return ObjectReceiverCallback::FlowControlState::UNBLOCKED;
}

void MoQTestClient::onObjectStatus(
    ReceiveState& state,
    const std::optional<TrackAlias>& /* trackAlias */,
    const ObjectHeader& objHeader) {
  XLOG(DBG1) << "MoQTest DEBUGGING: calling onObjectStatus";

  ObjectHeader header = objHeader;
  // Validate the received data
  if (header.status != ObjectStatus::END_OF_GROUP) {
    XLOG(ERR)
        << "MoQTest verification result: FAILURE! reason: Unknown object status received: "
        << header.status;
    return;
  }

  if (!state.expectEndOfGroup) {
    XLOG(ERR)
        << "MoQTest verification result: FAILURE! reason: End of Group Marker Received When Not Expected";
    return;
  }

  if (header.id != lastObjectInGroup(params_)) {
    XLOG(ERR)
        << "MoQTest verification result: FAILURE! reason: Object Id Mismatch For End of Group Marker: Actual="
        << header.id << "  Expected=" << lastObjectInGroup(params_);
    return;
  }

  // Remove the end-of-group marker from the scoreboard.  End-of-group markers
  // don't go through validateSubscribedData(), so nothing else erases them and
  // they would show up as "objects still expected".
  auto marker = expectedObjects_.find(std::make_pair(header.group, header.id));
  if (marker != expectedObjects_.end()) {
    if (marker->second->expired()) {
      ++datagramDrops_;
    }
    expectedObjects_.erase(marker);
  }

  // Adjust the expected data
  if (adjustExpected(state, params_, objHeader) ==
      AdjustedExpectedResult::RECEIVED_ALL_DATA) {
    XLOG(DBG1)
        << "MoQTest DEBUGGING: onObjectStatus: No more data to be expected";
  }
}

void MoQTestClient::onEndOfStream() {
  XLOG(DBG1) << "MoQTest DEBUGGING: calling onEndOfStream";
}

void MoQTestClient::onError(ReceiveState& state, ResetStreamErrorCode error) {
  XLOG(DBG1) << "MoQTest DEBUGGING: calling onError";
  if (tearingDown_) {
    return;
  }
  // Otherwise the datagram drop budget would swallow the reset.
  if (!state.done) {
    recordSemanticsFailure(
        folly::to<std::string>(
            "Stream reset with error code ", folly::to_underlying(error)));
  }
  // An unfinished half gates the other half's verdict forever.
  onAllDataReceived(state);
}
void MoQTestClient::onAllDataReceived(ReceiveState& state) {
  XLOG(DBG1) << "MoQTest DEBUGGING: onAllDataReceived";
  // A natural completion and a reset both land here; the verdict runs once.
  if (state.done) {
    return;
  }
  state.done = true;
  // The peer retired this half's request, so a later failure must not cancel
  // it.
  if (state.type == ReceivingType::FETCH) {
    fetchHandle_.reset();
  } else {
    subHandle_.reset();
  }
  // A request with more than one half only reaches a verdict once every half
  // has finished, since they tick off one shared scoreboard.
  if ((subState_.active && !subState_.done) ||
      (fetchState_.active && !fetchState_.done)) {
    return;
  }

  // Whatever verdict the checks below reach, the request is over.
  cancelDeadlines();
  auto doneGuard = folly::makeGuard([this] { doneBaton_.post(); });

  if (semanticsFailed_) {
    // The individual mismatches were already logged as they were detected
    XLOG(ERR)
        << "MoQTest verification result: FAILURE! reason: Delivery Semantics Not Preserved";
    return;
  }

  // Only a subscription delivers datagrams; a FETCH of the same track arrives
  // reliably.  Drops are tolerable exactly when a subscription is in play.
  const bool datagramsInFlight = subState_.active &&
      params_.forwardingPreference == ForwardingPreference::DATAGRAM;
  if (datagramsInFlight) {
    // For datagrams, some drops are allowed based on datagramDropPercentage
    uint64_t totalExpected = expectedObjectsIn(params_, window_).size();
    // Allow configured percentage of drops, with minimum of 1
    uint64_t dropsAllowed = std::max(
        uint64_t{1}, totalExpected * params_.datagramDropPercentage / 100);
    if (datagramObjects_ == 0) {
      XLOG(ERR)
          << "MoQTest verification result: FAILURE! reason: Datagram Failed - 0 Objects Received";
      cancelRequest();
      return;
    } else if (expectedObjects_.size() + datagramDrops_ > dropsAllowed) {
      XLOG(ERR)
          << "MoQTest verification result: FAILURE! reason: Datagram had too many drops: "
          << expectedObjects_.size() << " missing, " << datagramDrops_
          << " late, allowed " << dropsAllowed;
      cancelRequest();
      return;
    } else {
      XLOG(INFO) << "MoQTest verification result: SUCCESS! Datagram Received "
                 << datagramObjects_ << " objects";
      return;
    }
  }

  // For non-datagram: success == scoreboard.empty()
  if (!expectedObjects_.empty()) {
    XLOG(ERR)
        << "MoQTest verification result: FAILURE! reason: PublishDone received while "
        << expectedObjects_.size() << " objects are still expected";
    for (const auto& [key, cb] : expectedObjects_) {
      XLOG(ERR) << "  Missing object: group=" << key.first
                << " id=" << key.second;
    }
    cancelRequest();
    return;
  }

  XLOG(INFO) << "MoQTest verification result: SUCCESS! All Data Received";
}

uint64_t MoQTestClient::draftMajorVersion() const {
  auto version = moqClient_->moqSession_->getNegotiatedVersion();
  return version ? getDraftMajorVersion(*version) : 0;
}

void MoQTestClient::recordSemanticsFailure(const std::string& reason) {
  semanticsFailed_ = true;
  XLOG(ERR) << "MoQTest verification result: FAILURE! reason: " << reason;
}

namespace {
// A liveness backstop, not a latency bound: added to the interval the track
// itself implies, so it only has to cover scheduling jitter.
constexpr std::chrono::milliseconds kDeadlineSlack{1000};
// Allowed per object, because a publisher that runs a few percent slow drifts
// further behind with every object and a fixed slack is eventually spent.
constexpr std::chrono::milliseconds kObjectDrift{10};
} // namespace

void MoQTestClient::armObjectDeadlines() {
  for (auto& [unused, cb] : expectedObjects_) {
    moqExecutor_->scheduleTimeout(cb.get(), cb->timeout());
  }
}

std::chrono::milliseconds MoQTestClient::objectInterval() const {
  return std::chrono::milliseconds(params_.objectFrequency) + kObjectDrift;
}

void MoQTestClient::cancelDeadlines() {
  for (auto& [unused, cb] : expectedObjects_) {
    cb->cancelTimerCallback();
  }
  requestDeadline_.cancelTimerCallback();
}

void MoQTestClient::objectDeadlineExpired(uint64_t group, uint64_t id) {
  if (tearingDown_) {
    return;
  }
  // A late datagram spends drop budget rather than failing outright, so the
  // run continues and the verdict weighs it against the threshold.
  if (params_.forwardingPreference != ForwardingPreference::DATAGRAM) {
    recordSemanticsFailure(folly::to<std::string>(
        "Object not delivered before deadline: group=", group, " id=", id));
    finishRequest();
  }
}

// Reaching a verdict is what cancels this, so it also catches a track that
// delivered everything and never closed its streams.
void MoQTestClient::requestDeadlineExpired() {
  if (tearingDown_) {
    return;
  }
  recordSemanticsFailure(folly::to<std::string>(
      "Request did not complete by the track's expected end; ",
      expectedObjects_.size(),
      " objects outstanding, PUBLISH_DONE ",
      publishDoneReceived_ ? "received" : "not received"));
  finishRequest();
}

void MoQTestClient::finishRequest() {
  // Each half returns early until the last one lands, which runs the verdict.
  if (subState_.active && !subState_.done) {
    onAllDataReceived(subState_);
  }
  if (fetchState_.active && !fetchState_.done) {
    onAllDataReceived(fetchState_);
  }
}

void MoQTestClient::onPublishDone(PublishDone /* done */) {
  publishDoneReceived_ = true;
}

void MoQTestClient::validateSubgroupHeader(
    const ReceiveState& state,
    uint64_t groupID,
    uint64_t subgroupID,
    Priority priority,
    const TrackConsumer::BeginSubgroupOptions& options) {
  if (state.type != ReceivingType::SUBSCRIBE) {
    // FETCH responses arrive on a fetch stream, which has no subgroup header
    return;
  }

  // A joining subscription opens its first subgroup partway through a group,
  // so neither the end-of-group signal nor the first-object signal describes a
  // whole subgroup until the cursor has a position.
  if (!state.seeded) {
    return;
  }

  auto expectEndOfGroup = subgroupCarriesLastObject(params_, subgroupID);
  if (options.containsLastInGroup != expectEndOfGroup) {
    recordSemanticsFailure(
        folly::to<std::string>(
            "End of Group Signal Mismatch for group=",
            groupID,
            " subgroup=",
            subgroupID,
            ": Actual=",
            options.containsLastInGroup,
            "  Expected=",
            expectEndOfGroup));
  }

  // Every subgroup the test server opens starts at its own first object, but
  // the draft only carries that signal from 18 onwards.
  if (draftMajorVersion() >= 18 && !options.beginsWithFirstObject) {
    recordSemanticsFailure(
        folly::to<std::string>(
            "Missing Begins With First Object Signal for group=",
            groupID,
            " subgroup=",
            subgroupID));
  }

  // The publisher may elide the priority from the wire, but the value the
  // subscriber ends up with must still be the one the publisher chose.
  auto expectedPriority = publisherPriorityForGroup(groupID);
  if (priority != expectedPriority) {
    recordSemanticsFailure(
        folly::to<std::string>(
            "Subgroup Priority Mismatch for group=",
            groupID,
            " subgroup=",
            subgroupID,
            ": Actual=",
            static_cast<uint64_t>(priority),
            "  Expected=",
            static_cast<uint64_t>(expectedPriority)));
  }
}

void MoQTestClient::validateDatagramHeader(
    const ReceiveState& state,
    const ObjectHeader& header,
    bool endOfGroup) {
  // The type byte carries the end-of-group bit or a status datagram, never
  // both, so when the track sends markers the bit stays clear and the
  // END_OF_GROUP status datagram is the signal instead.
  auto expectEndOfGroup =
      !state.expectEndOfGroup && header.id == lastObjectInGroup(params_);
  if (endOfGroup != expectEndOfGroup) {
    recordSemanticsFailure(
        folly::to<std::string>(
            "Datagram End of Group Signal Mismatch for group=",
            header.group,
            " id=",
            header.id,
            ": Actual=",
            endOfGroup,
            "  Expected=",
            expectEndOfGroup));
  }

  auto expectedPriority = publisherPriorityForGroup(header.group);
  if (header.priority != expectedPriority) {
    recordSemanticsFailure(
        folly::to<std::string>(
            "Datagram Priority Mismatch for group=",
            header.group,
            " id=",
            header.id,
            ": Actual=",
            header.priority ? std::to_string(*header.priority) : "none",
            "  Expected=",
            static_cast<uint64_t>(expectedPriority)));
  }
}

ForwardingPreference MoQTestClient::deliveredForwardingPreference(
    const ReceiveState& state) const {
  return state.type == ReceivingType::FETCH
      ? fetchForwardingPreference(params_.forwardingPreference)
      : params_.forwardingPreference;
}

void MoQTestClient::cancelRequest() {
  tearingDown_ = true;
  if (fetchHandle_) {
    fetchHandle_->fetchCancel();
  }
  if (subHandle_) {
    subHandle_->unsubscribe();
  }
}

bool MoQTestClient::validateSubscribedData(
    ReceiveState& state,
    const ObjectHeader& header,
    const std::string& payload) {
  const auto preference = deliveredForwardingPreference(state);
  if (!state.seeded) {
    seedCursor(state, header);
  }
  // Validate Group, Object Id, SubGroup (and End of Group Markers if
  // applicable)
  XLOG(DBG1) << "MoQTest DEBUGGING: Expected Group=" << state.expectedGroup
             << " Expected ObjectId="
             << state.subgroupToExpectedObjId[header.subgroup];
  XLOG(DBG1) << "MoQTest DEBUGGING: Object Group=" << header.group
             << " end of group markers=" << params_.sendEndOfGroupMarkers
             << " expected end of group markers=" << state.expectEndOfGroup;
  if (preference != ForwardingPreference::DATAGRAM) {
    if (preference == ForwardingPreference::ONE_SUBGROUP_PER_OBJECT) {
      // Allow out-of-order groups, just validate range
      if (header.group < params_.startGroup ||
          header.group > params_.lastGroupInTrack) {
        XLOG(ERR)
            << "MoQTest verification result: FAILURE! reason: Group out of range: "
            << header.group << " not in [" << params_.startGroup << ", "
            << params_.lastGroupInTrack << "]";
        return false;
      }
      // Validate group increment
      if ((header.group - params_.startGroup) % params_.groupIncrement != 0) {
        XLOG(ERR)
            << "MoQTest verification result: FAILURE! reason: Group not on increment boundary: "
            << header.group << " with startGroup=" << params_.startGroup
            << " and groupIncrement=" << params_.groupIncrement;
        return false;
      }
    } else if (header.group != state.expectedGroup) {
      // Can spuriously fail; groups are separate streams and may reorder.  The
      // server publishes even and odd groups one priority apart, so every even
      // group outranks every odd one and the halves can interleave.
      XLOG(ERR)
          << "MoQTest verification result: FAILURE! reason: Group Mismatch: Actual="
          << header.group << "  Expected=" << state.expectedGroup;
      return false;
    }
  }

  if (preference == ForwardingPreference::ONE_SUBGROUP_PER_GROUP &&
      header.subgroup != state.expectedSubgroup) {
    XLOG(ERR)
        << "MoQTest verification result: FAILURE! reason: SubGroup Mismatch: Actual="
        << header.subgroup << "  Expected=" << state.expectedSubgroup;
    return false;
  }

  // Validate function for Datagram Objects
  if (preference == ForwardingPreference::DATAGRAM) {
    if (!validateDatagramObjects(header)) {
      return false;
    }
  }

  // Validate subgroup ID according to forwarding preference
  if ((preference == ForwardingPreference::ONE_SUBGROUP_PER_GROUP &&
       header.subgroup != 0) ||
      (preference == ForwardingPreference::TWO_SUBGROUPS_PER_GROUP &&
       header.subgroup > 1)) {
    XLOG(ERR)
        << "MoQTest verification result: FAILURE! reason: SubGroup Mismatch: Actual="
        << header.subgroup << "  Expected="
        << (preference == ForwardingPreference::ONE_SUBGROUP_PER_GROUP
                ? "0"
                : (preference == ForwardingPreference::TWO_SUBGROUPS_PER_GROUP
                       ? "0 or 1"
                       : "N/A"));
    return false;
  }

  // Validate ONE_SUBGROUP_PER_OBJECT constraints
  if (preference == ForwardingPreference::ONE_SUBGROUP_PER_OBJECT) {
    // Subgroup must equal object ID
    if (header.subgroup != header.id) {
      XLOG(ERR)
          << "MoQTest verification result: FAILURE! reason: SubGroup must equal "
          << "Object ID for ONE_SUBGROUP_PER_OBJECT: subgroup="
          << header.subgroup << " id=" << header.id;
      return false;
    }
    // Object ID must be in valid range
    if (header.id < params_.startObject ||
        header.id > params_.lastObjectInTrack) {
      XLOG(ERR)
          << "MoQTest verification result: FAILURE! reason: Object ID out of range: "
          << header.id << " not in [" << params_.startObject << ", "
          << params_.lastObjectInTrack << "]";
      return false;
    }
  }

  // Scoreboard-based duplicate detection for non-DATAGRAM forwarding
  // preferences
  if (preference != ForwardingPreference::DATAGRAM) {
    auto key = std::make_pair(header.group, header.id);
    auto it = expectedObjects_.find(key);
    if (it == expectedObjects_.end()) {
      XLOG(ERR)
          << "MoQTest verification result: FAILURE! reason: Duplicate or unexpected object: "
          << "group=" << header.group << " id=" << header.id;
      return false;
    }
    // Erase from scoreboard - object received successfully
    expectedObjects_.erase(it);
  }

  if (preference != ForwardingPreference::DATAGRAM &&
      preference != ForwardingPreference::ONE_SUBGROUP_PER_OBJECT &&
      header.id != state.subgroupToExpectedObjId[header.subgroup]) {
    XLOG(ERR)
        << "MoQTest verification result: FAILURE! reason: Object Id Mismatch: Actual="
        << header.id
        << "  Expected=" << state.subgroupToExpectedObjId[header.subgroup]
        << " (Subgroup=" << header.subgroup << ")";
    return false;
  }

  // Validate End of Group
  if (header.id == lastObjectInGroup(params_) && state.expectEndOfGroup) {
    if (header.status != ObjectStatus::END_OF_GROUP) {
      XLOG(ERR)
          << "MoQTest verification result: FAILURE! reason: End of Group Mismatch: Actual="
          << header.status << "  Expected=" << ObjectStatus::END_OF_GROUP;
      return false;
    }
  }

  // Validate Extensions have been made
  auto result =
      validateExtensions(header.extensions.getMutableExtensions(), &params_);
  if (result.hasError()) {
    XLOG(ERR)
        << "MoQTest verification result: FAILURE! reason: Extension Error="
        << std::to_string(result.error().code)
        << " Reason=" << result.error().reason;
    return false;
  }

  // Validate Payload
  int objectSize = moxygen::getObjectSize(header.id, &params_);
  if (!validatePayload(objectSize, payload)) {
    XLOG(ERR)
        << "MoQTest verification result: FAILURE! reason: Payload Mismatch: Actual="
        << payload << "  Expected=" << std::string(objectSize, 't');
    return false;
  }

  return true;
}

AdjustedExpectedResult MoQTestClient::adjustExpectedForOneSubgroupPerGroup(
    ReceiveState& state,
    MoQTestParameters& params) {
  // Adjust Expected Group and ObjectId
  const uint64_t lastObject = window_.lastObjectIn(state.expectedGroup);
  if (state.expectedGroup < window_.last.group &&
      state.subgroupToExpectedObjId[0] >= lastObject) {
    state.expectedGroup += params.groupIncrement;
    state.subgroupToExpectedObjId[0] =
        window_.firstObjectIn(state.expectedGroup);
  } else if (state.subgroupToExpectedObjId[0] < lastObject) {
    state.subgroupToExpectedObjId[0] += params.objectIncrement;
  } else {
    return AdjustedExpectedResult::RECEIVED_ALL_DATA;
  }
  return AdjustedExpectedResult::STILL_RECEIVING_DATA;
}

AdjustedExpectedResult MoQTestClient::adjustExpectedForOneSubgroupPerObject() {
  // With scoreboard approach, we check if all expected objects have been
  // received
  if (expectedObjects_.empty()) {
    return AdjustedExpectedResult::RECEIVED_ALL_DATA;
  }
  return AdjustedExpectedResult::STILL_RECEIVING_DATA;
}

AdjustedExpectedResult MoQTestClient::adjustExpectedForTwoSubgroupsPerGroup(
    ReceiveState& state,
    const ObjectHeader& header,
    MoQTestParameters& params) {
  const uint64_t lastObject = window_.lastObjectIn(state.expectedGroup);
  auto subgroup = header.subgroup;
  // Adjust Expected Group, ObjectId and Subgroup
  if (state.expectedGroup < window_.last.group &&
      state.subgroupToExpectedObjId[subgroup] >= lastObject) {
    // Increment Group, Reset ObjectId and Subgroup
    state.expectedGroup += params.groupIncrement;
    const uint64_t firstObject = window_.firstObjectIn(state.expectedGroup);
    state.subgroupToExpectedObjId[firstObject % 2] = firstObject;
    state.subgroupToExpectedObjId[1 - (firstObject % 2)] =
        firstObject + params.objectIncrement;
  } else if (state.subgroupToExpectedObjId[subgroup] < lastObject) {
    // Increment ObjectId for this subgroup.  If increment is odd, increment
    // twice
    state.subgroupToExpectedObjId[subgroup] += params.objectIncrement;
    if (params.objectIncrement % 2 == 1) {
      state.subgroupToExpectedObjId[subgroup] += params.objectIncrement;
    }
  } else {
    return AdjustedExpectedResult::RECEIVED_ALL_DATA;
  }
  return AdjustedExpectedResult::STILL_RECEIVING_DATA;
}

AdjustedExpectedResult MoQTestClient::adjustExpectedForDatagram(
    const ReceiveState& state,
    MoQTestParameters& params) {
  // Adjust Object Count
  datagramObjects_++;
  if (state.expectedGroup == params.lastGroupInTrack &&
      state.subgroupToExpectedObjId[0] == params.lastObjectInTrack) {
    return AdjustedExpectedResult::RECEIVED_ALL_DATA;
  }
  return AdjustedExpectedResult::STILL_RECEIVING_DATA;
}

folly::Expected<folly::Unit, ExtensionError> MoQTestClient::validateExtensions(
    const std::vector<Extension>& extensions,
    MoQTestParameters* params) {
  // validate extension size
  if (!validateExtensionSize(extensions, params)) {
    int expectedAmount = (int)(params->testIntegerExtension >= 0) +
        (int)(params->testVariableExtension >= 0);
    ExtensionError error{
        ExtensionErrorCode::INVALID_EXTENSION_AMOUNT,
        folly::to<std::string>(
            "Invalid Extensions Amount-> Expected size: ",
            expectedAmount,
            " Actual size: ",
            extensions.size())};
    return folly::makeUnexpected(error);
  }

  // Get Extensions
  Extension intExt;
  Extension varExt;
  for (const Extension& ext : extensions) {
    if (ext.type % 2 == 0) {
      intExt = ext;
    } else {
      varExt = ext;
    }
  }

  // validate integer extensions
  if (params->testIntegerExtension >= 0 &&
      !validateIntExtensions(intExt, params)) {
    ExtensionError error{
        ExtensionErrorCode::INVALID_INT_EXTENSION,
        folly::to<std::string>(
            "Invalid Integer Extension-> Expected id: ",
            (2 * params->testIntegerExtension),
            " Actual id: ",
            intExt.type)};
    return folly::makeUnexpected(error);
  }

  // validate variable extensions
  if (params->testVariableExtension >= 0 &&
      (!validateVarExtensions(varExt, params))) {
    ExtensionError error{
        ExtensionErrorCode::INVALID_VAR_EXTENSION,
        folly::to<std::string>(
            "Invalid Variable Extension-> Expected id: ",
            2 * params->testVariableExtension + 1,
            " Actual id: ",
            varExt.type)};
    return folly::makeUnexpected(error);
  }

  // Return Validated
  return folly::Unit({});
}

void MoQTestClient::validateFetchOk(
    const FetchOk& ok,
    const MoQTestParameters& params,
    const StandaloneFetch& range) {
  auto expectedEnd = fetchEndLocation(params, range);
  if (ok.endLocation != expectedEnd) {
    XLOG(ERR)
        << "MoQTest verification result: FAILURE! reason: FETCH_OK End Location Mismatch: Actual="
        << ok.endLocation << "  Expected=" << expectedEnd;
    semanticsFailed_ = true;
  }
  uint8_t expectedEndOfTrack = window_.endOfTrack ? 1 : 0;
  if (ok.endOfTrack != expectedEndOfTrack) {
    XLOG(ERR)
        << "MoQTest verification result: FAILURE! reason: FETCH_OK End Of Track Mismatch: Actual="
        << static_cast<int>(ok.endOfTrack)
        << "  Expected=" << static_cast<int>(expectedEndOfTrack);
    semanticsFailed_ = true;
  }
}

uint64_t MoQTestClient::joiningStartGroup(
    int64_t joinStart,
    const AbsoluteLocation& largest) {
  if (joinStart >= 0) {
    return static_cast<uint64_t>(joinStart);
  }
  const auto back = static_cast<uint64_t>(-joinStart);
  return largest.group >= back ? largest.group - back : 0;
}

void MoQTestClient::trimExpectedBefore(uint64_t group) {
  expectedObjects_.erase(
      expectedObjects_.begin(), expectedObjects_.lower_bound({group, 0}));
}

void MoQTestClient::validateJoiningFetchOk(
    const FetchOk& ok,
    const MoQTestParameters& params,
    const AbsoluteLocation& largest,
    uint64_t startGroup) {
  const StandaloneFetch range(
      AbsoluteLocation{startGroup, 0},
      AbsoluteLocation{largest.group, largest.object + 1});
  auto expectedEnd = fetchEndLocation(params, range);
  if (ok.endLocation != expectedEnd) {
    recordSemanticsFailure(
        folly::to<std::string>(
            "Joining FETCH_OK End Location Mismatch: Actual=",
            folly::to<std::string>(
                ok.endLocation.group, ":", ok.endLocation.object),
            "  Expected=",
            folly::to<std::string>(
                expectedEnd.group, ":", expectedEnd.object)));
  }
}

void MoQTestClient::initializeExpecteds(
    MoQTestParameters& params,
    MoQTestFetchWindow window) {
  params_ = params;
  window_ = window;
  semanticsFailed_ = false;
  subState_ = ReceiveState{};
  fetchState_ = ReceiveState{};

  expectedObjects_.clear();
  int64_t n = 0;
  for (const auto& key : expectedObjectsIn(params, window)) {
    expectedObjects_.emplace(
        key,
        std::make_unique<ObjectDeadline>(
            *this,
            key.first,
            key.second,
            n++ * objectInterval() + kDeadlineSlack));
  }

  // Only relevant for Datagram Forwarding Preference
  datagramObjects_ = 0;
  datagramDrops_ = 0;

  publishDoneReceived_ = false;
  moqExecutor_->scheduleTimeout(
      &requestDeadline_,
      int64_t(expectedObjects_.size()) * objectInterval() + kDeadlineSlack);
}

void MoQTestClient::seedCursor(
    ReceiveState& state,
    const ObjectHeader& header) {
  state.seeded = true;
  state.expectedGroup = header.group;
  if (params_.forwardingPreference ==
      ForwardingPreference::TWO_SUBGROUPS_PER_GROUP) {
    state.subgroupToExpectedObjId[header.id % 2] = header.id;
    state.subgroupToExpectedObjId[1 - (header.id % 2)] =
        header.id + params_.objectIncrement;
  } else {
    state.subgroupToExpectedObjId[0] = header.id;
  }
}

void MoQTestClient::startReceiving(
    ReceiveState& state,
    ReceivingType type,
    bool seeded) {
  // An empty window carries the kLocationMax sentinel, and seeding the cursor
  // from it is what makes a stray object on such a fetch fail to match.
  const uint64_t firstObject = window_.first.object;
  state = ReceiveState{};
  state.type = type;
  state.active = true;
  state.seeded = seeded;
  state.expectedGroup = window_.first.group;
  if (params_.forwardingPreference ==
      ForwardingPreference::TWO_SUBGROUPS_PER_GROUP) {
    state.subgroupToExpectedObjId[firstObject % 2] = firstObject;
    state.subgroupToExpectedObjId[1 - (firstObject % 2)] =
        firstObject + params_.objectIncrement;
  } else {
    state.subgroupToExpectedObjId[0] = firstObject;
  }
  // A fetched datagram track is the one combination the server cannot mark:
  // FetchConsumer::endOfGroup() has no way to flag its status object as a
  // datagram.  Every other combination honors the parameter.
  state.expectEndOfGroup = params_.sendEndOfGroupMarkers &&
      !(type == ReceivingType::FETCH &&
        params_.forwardingPreference == ForwardingPreference::DATAGRAM);
}

AdjustedExpectedResult MoQTestClient::adjustExpected(
    ReceiveState& state,
    MoQTestParameters& params,
    const ObjectHeader& header) {
  switch (deliveredForwardingPreference(state)) {
    case (ForwardingPreference::ONE_SUBGROUP_PER_GROUP): {
      return adjustExpectedForOneSubgroupPerGroup(state, params);
    }
    case (ForwardingPreference::ONE_SUBGROUP_PER_OBJECT): {
      return adjustExpectedForOneSubgroupPerObject();
    }
    case (ForwardingPreference::TWO_SUBGROUPS_PER_GROUP): {
      return adjustExpectedForTwoSubgroupsPerGroup(state, header, params);
    }
    case (ForwardingPreference::DATAGRAM): {
      return adjustExpectedForDatagram(state, params);
    }
    default: {
      break;
    }
  }

  return AdjustedExpectedResult::ERROR_RECEIVING_DATA;
}

bool MoQTestClient::validateDatagramObjects(const ObjectHeader& header) {
  // Validate Datagram Group and ObjectId

  // Group Must be Properly incremented
  if (header.group % params_.groupIncrement != 0) {
    XLOG(ERR)
        << "MoQTest verification result: FAILURE! reason: Datagram Group Mismatch: Actual="
        << header.group << "Expected Increment of " << params_.groupIncrement;
    return false;
  }

  // Group Must be before last group in track
  if (header.group > params_.lastGroupInTrack) {
    XLOG(ERR)
        << "MoQTest verification result: FAILURE! reason: Datagram Group Mismatch: Actual="
        << header.group << "Can't be greater than last group "
        << params_.lastGroupInTrack;
    return false;
  }

  // Object Id Must be Properly incremented
  if (((header.id - params_.startObject) % params_.objectIncrement) != 0) {
    XLOG(ERR)
        << "MoQTest verification result: FAILURE! reason: Datagram Object Id Mismatch: Actual="
        << header.id << "Expected Increment of " << params_.objectIncrement;
    return false;
  }

  // Object Id Must be before last object in track
  if (header.id > params_.lastObjectInTrack) {
    XLOG(ERR)
        << "MoQTest verification result: FAILURE! reason: Datagram Object Id Mismatch: Actual="
        << header.id << "Can't be greater than last object "
        << params_.lastObjectInTrack;
    return false;
  }

  // Check for duplicates - if not in scoreboard, we already received this
  // object
  auto key = std::make_pair(header.group, header.id);
  auto it = expectedObjects_.find(key);
  if (it == expectedObjects_.end()) {
    XLOG(ERR)
        << "MoQTest verification result: FAILURE! reason: Duplicate datagram object: "
        << "group=" << header.group << " id=" << header.id;
    return false;
  }
  if (it->second->expired()) {
    ++datagramDrops_;
  }
  expectedObjects_.erase(it);

  return true;
}

folly::coro::Task<void> MoQTestClient::trackStatus(TrackStatus req) {
  co_await moqClient_->moqSession_->trackStatus(req);
}

} // namespace moxygen

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
                  &objectReceiverCallback_),
              *this)),
      fetchReceiver_(
          std::make_shared<VerifyingObjectReceiver>(
              ObjectReceiver::FETCH,
              std::shared_ptr<ObjectReceiverCallback>(
                  std::shared_ptr<void>(),
                  &objectReceiverCallback_),
              *this)) {}

void MoQTestClient::setLogger(const std::shared_ptr<MLogger>& logger) {
  logger_ = logger;
  if (moqClient_) {
    moqClient_->setLogger(logger);
  }
}

void MoQTestClient::shutdown() {
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
  if (receivingType_ == ReceivingType::SUBSCRIBE && subHandle_) {
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
  receivingType_ = ReceivingType::SUBSCRIBE;
  initializeExpecteds(params, resolveFetchWindow(params));

  // Subscribe to the receiver
  auto res = co_await moqClient_->moqSession_->subscribe(sub, subReceiver_);
  moqClient_->moqSession_->drain();

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

  receivingType_ = ReceivingType::SUBSCRIBE;
  requestID_ = kDefaultRequestId;
  initializeExpecteds(params, resolveFetchWindow(params));

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
  receivingType_ = ReceivingType::FETCH;
  initializeExpecteds(params, resolveFetchWindow(params, fetchRange));

  // Fetch to the receiver
  auto res = co_await moqClient_->moqSession_->fetch(fetch, fetchReceiver_);
  moqClient_->moqSession_->drain();
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

ObjectReceiverCallback::FlowControlState MoQTestClient::onObject(
    const std::optional<TrackAlias>& /* trackAlias */,
    const ObjectHeader& objHeader,
    Payload payload) {
  XLOG(DBG1) << "MoQTest DEBUGGING: Calling onObject";

  // Validate the received data
  if (!validateSubscribedData(objHeader, payload->toString())) {
    XLOG(ERR)
        << "MoQTest verification result: FAILURE! reason: Data Validation Failed";
    cancelRequest();
    moqClient_->moqSession_->close(SessionCloseErrorCode::PROTOCOL_VIOLATION);
    doneBaton_.post();
    return ObjectReceiverCallback::FlowControlState::UNBLOCKED;
  }

  // Adjust the expected data (If Still receiving data, leave unblocked)
  adjustExpected(params_, objHeader);
  return ObjectReceiverCallback::FlowControlState::UNBLOCKED;
}

void MoQTestClient::onObjectStatus(
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

  if (!expectEndOfGroup_) {
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
  expectedObjects_.erase(std::make_pair(header.group, header.id));

  // Adjust the expected data
  if (adjustExpected(params_, objHeader) ==
      AdjustedExpectedResult::RECEIVED_ALL_DATA) {
    XLOG(DBG1)
        << "MoQTest DEBUGGING: onObjectStatus: No more data to be expected";
  }
}

void MoQTestClient::onEndOfStream() {
  XLOG(DBG1) << "MoQTest DEBUGGING: calling onEndOfStream";
}

void MoQTestClient::onError(ResetStreamErrorCode) {
  XLOG(DBG1) << "MoQTest DEBUGGING: calling onError";
}
void MoQTestClient::onAllDataReceived() {
  XLOG(DBG1) << "MoQTest DEBUGGING: onAllDataReceived";
  // Ensure subHandle_ is reset at the end of this function, even if an early
  // return occurs
  auto subHandleResetGuard = folly::makeGuard([this] {
    subHandle_.reset();
    doneBaton_.post();
  });

  if (semanticsFailed_) {
    // The individual mismatches were already logged as they were detected
    XLOG(ERR)
        << "MoQTest verification result: FAILURE! reason: Delivery Semantics Not Preserved";
    return;
  }

  if (deliveredForwardingPreference() == ForwardingPreference::DATAGRAM) {
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
    } else if (expectedObjects_.size() > dropsAllowed) {
      XLOG(ERR)
          << "MoQTest verification result: FAILURE! reason: Datagram had too many drops: "
          << expectedObjects_.size() << " missing, allowed " << dropsAllowed;
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
    for (const auto& [group, objId] : expectedObjects_) {
      XLOG(ERR) << "  Missing object: group=" << group << " id=" << objId;
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

void MoQTestClient::validateSubgroupHeader(
    uint64_t groupID,
    uint64_t subgroupID,
    Priority priority,
    const TrackConsumer::BeginSubgroupOptions& options) {
  if (receivingType_ != ReceivingType::SUBSCRIBE) {
    // FETCH responses arrive on a fetch stream, which has no subgroup header
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
    const ObjectHeader& header,
    bool endOfGroup) {
  // The type byte carries the end-of-group bit or a status datagram, never
  // both, so when the track sends markers the bit stays clear and the
  // END_OF_GROUP status datagram is the signal instead.
  auto expectEndOfGroup =
      !expectEndOfGroup_ && header.id == lastObjectInGroup(params_);
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

ForwardingPreference MoQTestClient::deliveredForwardingPreference() const {
  return receivingType_ == ReceivingType::FETCH
      ? fetchForwardingPreference(params_.forwardingPreference)
      : params_.forwardingPreference;
}

void MoQTestClient::cancelRequest() {
  if (receivingType_ == ReceivingType::FETCH) {
    if (fetchHandle_) {
      fetchHandle_->fetchCancel();
    }
  } else if (subHandle_) {
    subHandle_->unsubscribe();
  }
}

bool MoQTestClient::validateSubscribedData(
    const ObjectHeader& header,
    const std::string& payload) {
  const auto preference = deliveredForwardingPreference();
  // Validate Group, Object Id, SubGroup (and End of Group Markers if
  // applicable)
  XLOG(DBG1) << "MoQTest DEBUGGING: Expected Group=" << expectedGroup_
             << " Expected ObjectId="
             << subgroupToExpectedObjId_[header.subgroup];
  XLOG(DBG1) << "MoQTest DEBUGGING: Object Group=" << header.group
             << " end of group markers=" << params_.sendEndOfGroupMarkers
             << " expected end of group markers=" << expectEndOfGroup_;
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
    } else if (header.group != expectedGroup_) {
      // Can spuriously fail; groups are separate streams and may reorder.  The
      // server publishes even and odd groups one priority apart, so every even
      // group outranks every odd one and the halves can interleave.
      XLOG(ERR)
          << "MoQTest verification result: FAILURE! reason: Group Mismatch: Actual="
          << header.group << "  Expected=" << expectedGroup_;
      return false;
    }
  }

  if (preference == ForwardingPreference::ONE_SUBGROUP_PER_GROUP &&
      header.subgroup != expectedSubgroup_) {
    XLOG(ERR)
        << "MoQTest verification result: FAILURE! reason: SubGroup Mismatch: Actual="
        << header.subgroup << "  Expected=" << expectedSubgroup_;
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
      header.id != subgroupToExpectedObjId_[header.subgroup]) {
    XLOG(ERR)
        << "MoQTest verification result: FAILURE! reason: Object Id Mismatch: Actual="
        << header.id
        << "  Expected=" << subgroupToExpectedObjId_[header.subgroup]
        << " (Subgroup=" << header.subgroup << ")";
    return false;
  }

  // Validate End of Group
  if (header.id == lastObjectInGroup(params_) && expectEndOfGroup_) {
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
    MoQTestParameters& params) {
  // Adjust Expected Group and ObjectId
  const uint64_t lastObject = window_.lastObjectIn(expectedGroup_);
  if (expectedGroup_ < window_.last.group &&
      subgroupToExpectedObjId_[0] >= lastObject) {
    expectedGroup_ += params.groupIncrement;
    subgroupToExpectedObjId_[0] = window_.firstObjectIn(expectedGroup_);
  } else if (subgroupToExpectedObjId_[0] < lastObject) {
    subgroupToExpectedObjId_[0] += params.objectIncrement;
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
    const ObjectHeader& header,
    MoQTestParameters& params) {
  const uint64_t lastObject = window_.lastObjectIn(expectedGroup_);
  auto subgroup = header.subgroup;
  // Adjust Expected Group, ObjectId and Subgroup
  if (expectedGroup_ < window_.last.group &&
      subgroupToExpectedObjId_[subgroup] >= lastObject) {
    // Increment Group, Reset ObjectId and Subgroup
    expectedGroup_ += params.groupIncrement;
    const uint64_t firstObject = window_.firstObjectIn(expectedGroup_);
    subgroupToExpectedObjId_[firstObject % 2] = firstObject;
    subgroupToExpectedObjId_[1 - (firstObject % 2)] =
        firstObject + params.objectIncrement;
  } else if (subgroupToExpectedObjId_[subgroup] < lastObject) {
    // Increment ObjectId for this subgroup.  If increment is odd, increment
    // twice
    subgroupToExpectedObjId_[subgroup] += params.objectIncrement;
    if (params.objectIncrement % 2 == 1) {
      subgroupToExpectedObjId_[subgroup] += params.objectIncrement;
    }
  } else {
    return AdjustedExpectedResult::RECEIVED_ALL_DATA;
  }
  return AdjustedExpectedResult::STILL_RECEIVING_DATA;
}

AdjustedExpectedResult MoQTestClient::adjustExpectedForDatagram(
    MoQTestParameters& params) {
  // Adjust Object Count
  datagramObjects_++;
  // Only Complete if expectedGroup_ and subgroupToExpectedObjId_ are at the end
  if (expectedGroup_ == params_.lastGroupInTrack &&
      subgroupToExpectedObjId_[0] == params_.lastObjectInTrack) {
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

void MoQTestClient::initializeExpecteds(
    MoQTestParameters& params,
    MoQTestFetchWindow window) {
  params_ = params;
  window_ = window;
  // An empty window carries the kLocationMax sentinel, and seeding the cursor
  // from it is what makes a stray object on such a fetch fail to match.
  const uint64_t firstObject = window.first.object;

  expectedGroup_ = window.first.group;
  if (params.forwardingPreference ==
      ForwardingPreference::TWO_SUBGROUPS_PER_GROUP) {
    subgroupToExpectedObjId_[firstObject % 2] = firstObject;
    subgroupToExpectedObjId_[1 - (firstObject % 2)] =
        firstObject + params.objectIncrement;
  } else {
    subgroupToExpectedObjId_[0] = firstObject;
  }
  expectedSubgroup_ = 0;
  // A fetched datagram track is the one combination the server cannot mark:
  // FetchConsumer::endOfGroup() has no way to flag its status object as a
  // datagram.  Every other combination honors the parameter.
  expectEndOfGroup_ = params.sendEndOfGroupMarkers &&
      !(receivingType_ == ReceivingType::FETCH &&
        params.forwardingPreference == ForwardingPreference::DATAGRAM);
  semanticsFailed_ = false;

  expectedObjects_ = expectedObjectsIn(params, window);

  // Only relevant for Datagram Forwarding Preference
  datagramObjects_ = 0;
}

AdjustedExpectedResult MoQTestClient::adjustExpected(
    MoQTestParameters& params,
    const ObjectHeader& header) {
  switch (deliveredForwardingPreference()) {
    case (ForwardingPreference::ONE_SUBGROUP_PER_GROUP): {
      return adjustExpectedForOneSubgroupPerGroup(params);
    }
    case (ForwardingPreference::ONE_SUBGROUP_PER_OBJECT): {
      return adjustExpectedForOneSubgroupPerObject();
    }
    case (ForwardingPreference::TWO_SUBGROUPS_PER_GROUP): {
      return adjustExpectedForTwoSubgroupsPerGroup(header, params);
    }
    case (ForwardingPreference::DATAGRAM): {
      return adjustExpectedForDatagram(params);
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
  expectedObjects_.erase(it);

  return true;
}

folly::coro::Task<void> MoQTestClient::trackStatus(TrackStatus req) {
  co_await moqClient_->moqSession_->trackStatus(req);
}

} // namespace moxygen

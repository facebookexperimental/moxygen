/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/coro/Baton.h>
#include <moxygen/events/MoQFollyExecutorImpl.h>
#include "moxygen/MoQClientBase.h"
#include "moxygen/MoQRelaySession.h"
#include "moxygen/ObjectReceiver.h"
#include "moxygen/Subscriber.h"
#include "moxygen/mlog/MLogger.h"
#include "moxygen/moqtest/MoQTestPublisher.h"
#include "moxygen/moqtest/Types.h"
#include "moxygen/moqtest/Utils.h"
#include "moxygen/samples/util/Utils.h"

namespace moxygen {

enum ReceivingType : int {
  SUBSCRIBE = 0,
  FETCH = 1,
  UNKNOWN_RECEIVING_TYPE = 2
};

// Ordering between the SUBSCRIBE_TRACKS and the PUBLISH in publish mode. Each
// exercises a different relay path: SubscribeFirst is answered by the relay's
// PUBLISH fan-out, PublishFirst by its SUBSCRIBE_TRACKS backfill.
enum class PublishOrder : int { SubscribeFirst = 0, PublishFirst = 1 };

enum ExtensionErrorCode : int {
  INVALID_INT_EXTENSION = 0,
  INVALID_VAR_EXTENSION = 1,
  INVALID_EXTENSION_AMOUNT = 2
};

struct ExtensionError {
  ExtensionErrorCode code;
  std::string reason;
};

enum AdjustedExpectedResult : int {
  RECEIVED_ALL_DATA = 0,
  STILL_RECEIVING_DATA = 1,
  ERROR_RECEIVING_DATA = 2
};

// MoQTestClient is also a Subscriber so it can receive the PUBLISH the relay
// forwards in publish mode. It must be heap-allocated as a shared_ptr -- use
// create() -- because it registers itself as a subscribe handler.
class MoQTestClient : public Subscriber,
                      public std::enable_shared_from_this<MoQTestClient> {
 private:
  struct PrivateTag {
    explicit PrivateTag() = default;
  };

 public:
  static std::shared_ptr<MoQTestClient> create(
      folly::EventBase* evb,
      proxygen::URL url,
      samples::TransportType transportType) {
    return std::make_shared<MoQTestClient>(
        PrivateTag{}, evb, std::move(url), transportType);
  }

  MoQTestClient(
      PrivateTag,
      folly::EventBase* evb,
      proxygen::URL url,
      samples::TransportType transportType);

  ~MoQTestClient() override {}

  MoQTestClient(const MoQTestClient&) = delete;
  MoQTestClient& operator=(const MoQTestClient&) = delete;
  MoQTestClient(MoQTestClient&&) = delete;
  MoQTestClient& operator=(MoQTestClient&&) = delete;

  folly::coro::Task<void> connect(
      folly::EventBase* evb,
      const std::string& versions = "");

  folly::coro::Task<moxygen::TrackNamespace> subscribe(
      MoQTestParameters params);

  // `range` defaults to the whole track the namespace describes.
  folly::coro::Task<moxygen::TrackNamespace> fetch(
      MoQTestParameters params,
      std::optional<StandaloneFetch> range = std::nullopt);

  // SUBSCRIBE plus a FETCH that backfills what ran before it, so a late
  // subscriber still sees the whole track.  A non-negative `joinStart` is an
  // absolute group to fetch from; a negative one counts that many groups back
  // from where the subscription begins.
  folly::coro::Task<moxygen::TrackNamespace> join(
      MoQTestParameters params,
      int64_t joinStart);

  // Asks the relay for the track via SUBSCRIBE_TRACKS, then opens a second
  // session to the same endpoint and PUBLISHes it. The relay matches the two
  // and forwards the objects back, which this client validates as it would a
  // SUBSCRIBE. Requires a relay: a bare moqtest server will reject the PUBLISH.
  folly::coro::Task<moxygen::TrackNamespace> publishTrack(
      MoQTestParameters params,
      const std::string& versions = "",
      PublishOrder order = PublishOrder::SubscribeFirst);

  void setLogger(const std::shared_ptr<MLogger>& logger);

  // Subscriber: accept the relay's PUBLISH by handing back the receiver that
  // validates the track.
  PublishResult publish(
      PublishRequest pub,
      std::shared_ptr<SubscriptionHandle> handle) override;

  // Drains the session so the peer sees a clean close; the event loop exits
  // once the session finishes closing.
  void shutdown();

  // Completes when the track finishes, validation fails, or shutdown() runs.
  // Request coroutines await this so their completion marks "all done".
  folly::coro::Baton doneBaton_;

  folly::coro::Task<void> trackStatus(TrackStatus req);
  void subscribeUpdate(SubscribeUpdate update);

 private:
  // Brings up one session to url_ and returns its client. Used for both the
  // subscriber session and, in publish mode, the publisher session.
  folly::coro::Task<std::unique_ptr<MoQClientBase>> connectSession(
      const std::string& versions,
      std::shared_ptr<Publisher> publishHandler,
      std::shared_ptr<Subscriber> subscribeHandler);

  // Sends SUBSCRIBE_TRACKS for the namespace encoding the test parameters.
  folly::coro::Task<void> subscribeTracks(const TrackNamespace& trackNamespace);

  folly::coro::Task<void> doSubscribeUpdate(
      std::shared_ptr<Publisher::SubscriptionHandle> handle,
      SubscribeUpdate update);
  // Where validation has got to in the track.  A join delivers its fetch and
  // subscribe halves concurrently on separate streams, so each carries its own
  // position; the scoreboard of expected objects stays shared.
  struct ReceiveState {
    ReceivingType type{ReceivingType::UNKNOWN_RECEIVING_TYPE};
    uint64_t expectedGroup{};
    uint64_t expectedSubgroup{};
    std::array<uint64_t, 2> subgroupToExpectedObjId{};
    bool expectEndOfGroup{};
    bool active{false};
    bool done{false};
    // A joining subscription starts wherever the publisher had reached, which
    // the client only learns from the first object it delivers.
    bool seeded{true};
  };

  // An ObjectReceiverCallback implementation that forwards calls to a
  // MoQTestClient, tagged with the half of the request it belongs to.
  class ObjectReceiverCallback : public moxygen::ObjectReceiverCallback {
   public:
    ObjectReceiverCallback(MoQTestClient& client, ReceiveState& state)
        : client_(client), state_(state) {}

    FlowControlState onObject(
        std::optional<TrackAlias> trackAlias,
        const ObjectHeader& objHeader,
        Payload payload) override {
      return client_.onObject(
          state_, std::move(trackAlias), objHeader, std::move(payload));
    }

    void onObjectStatus(
        std::optional<TrackAlias> trackAlias,
        const ObjectHeader& objHeader) override {
      client_.onObjectStatus(state_, std::move(trackAlias), objHeader);
    }

    void onEndOfStream() override {
      client_.onEndOfStream();
    }

    void onError(ResetStreamErrorCode code) override {
      client_.onError(state_, code);
    }

    void onPublishDone(PublishDone /* done */) override {}

    void onAllDataReceived() override {
      client_.onAllDataReceived(state_);
    }

   private:
    MoQTestClient& client_;
    ReceiveState& state_;
  };

  // Wraps ObjectReceiver to inspect the delivery semantics the publisher
  // signalled -- which subgroup ends a group, whether a subgroup starts at the
  // group's first object, and the datagram end-of-group marker -- none of
  // which reach ObjectReceiverCallback.
  class VerifyingObjectReceiver : public ObjectReceiver {
   public:
    VerifyingObjectReceiver(
        Type type,
        std::shared_ptr<moxygen::ObjectReceiverCallback> callback,
        MoQTestClient& client,
        ReceiveState& state)
        : ObjectReceiver(type, std::move(callback)),
          client_(client),
          state_(state) {}

    folly::Expected<std::shared_ptr<SubgroupConsumer>, MoQPublishError>
    beginSubgroup(
        uint64_t groupID,
        uint64_t subgroupID,
        Priority priority,
        BeginSubgroupOptions options) override {
      client_.validateSubgroupHeader(
          state_, groupID, subgroupID, priority, options);
      return ObjectReceiver::beginSubgroup(
          groupID, subgroupID, priority, options);
    }

    folly::Expected<folly::Unit, MoQPublishError> datagram(
        const ObjectHeader& header,
        Payload payload,
        bool endOfGroup) override {
      client_.validateDatagramHeader(state_, header, endOfGroup);
      return ObjectReceiver::datagram(header, std::move(payload), endOfGroup);
    }

   private:
    MoQTestClient& client_;
    ReceiveState& state_;
  };

  // Override Vritual Functions for now to return basic print statements
  ObjectReceiverCallback::FlowControlState onObject(
      ReceiveState& state,
      const std::optional<TrackAlias>& trackAlias,
      const ObjectHeader& objHeader,
      Payload payload);
  void onObjectStatus(
      ReceiveState& state,
      const std::optional<TrackAlias>& trackAlias,
      const ObjectHeader& objHeader);
  void onEndOfStream();
  void onError(ReceiveState& state, ResetStreamErrorCode);
  void onAllDataReceived(ReceiveState& state);

  ReceiveState subState_;
  ReceiveState fetchState_;
  ObjectReceiverCallback subCallback_{*this, subState_};
  ObjectReceiverCallback fetchCallback_{*this, fetchState_};

  proxygen::URL url_;
  samples::TransportType transportType_;
  // Sessions are created by connect(), which can run after setLogger().
  std::shared_ptr<MLogger> logger_;
  std::shared_ptr<MoQFollyExecutorImpl> moqExecutor_;
  std::unique_ptr<MoQClientBase> moqClient_;
  std::shared_ptr<ObjectReceiver> subReceiver_;
  std::shared_ptr<ObjectReceiver> fetchReceiver_;

  // Publish mode only: a second session to the same endpoint that acts as the
  // origin, plus the generator that feeds it. Null in subscribe/fetch mode.
  std::unique_ptr<MoQClientBase> pubClient_;
  std::shared_ptr<MoQTestPublisher> publisher_;

  // Holds Current Request Parameters
  MoQTestParameters params_;
  RequestID requestID_{};

  // The slice of the track the current request covers
  MoQTestFetchWindow window_;

  // Scoreboard of expected (group, objectId) pairs
  // When receiving: if present, erase; if absent, it's a duplicate
  // At end: success == scoreboard.empty() (or within drop limit for datagrams)
  std::set<std::pair<uint64_t, uint64_t>> expectedObjects_;

  // Set when a delivery-semantics check fails; suppresses the final SUCCESS
  bool semanticsFailed_{false};

  // Set once we cancel the request ourselves; the peer answers with a stream
  // reset that must not count against the track
  bool tearingDown_{false};

  // Holds Datagram Objects Recieved - (Only relevant for forwarding preference
  // 3)
  uint64_t datagramObjects_{};

  // Handles
  std::shared_ptr<Publisher::SubscriptionHandle> subHandle_;
  std::shared_ptr<Publisher::FetchHandle> fetchHandle_;
  std::shared_ptr<Publisher::SubscribeTracksHandle> subscribeTracksHandle_;

  // Delivery semantics validation.  A relay is free to re-encode a subgroup
  // header or datagram, so these check what the encoding means rather than
  // which stream/datagram type byte was used.
  void validateSubgroupHeader(
      const ReceiveState& state,
      uint64_t groupID,
      uint64_t subgroupID,
      Priority priority,
      const TrackConsumer::BeginSubgroupOptions& options);
  void validateDatagramHeader(
      const ReceiveState& state,
      const ObjectHeader& header,
      bool endOfGroup);
  void recordSemanticsFailure(const std::string& reason);
  uint64_t draftMajorVersion() const;

  // The shape the objects actually arrive in, which is not always the track's
  // forwarding preference -- see fetchForwardingPreference().
  ForwardingPreference deliveredForwardingPreference(
      const ReceiveState& state) const;

  // Cancels every request in flight.  Handles are assigned only after the peer
  // replies, and objects can be parsed before that, so this is a no-op if a
  // failure is detected that early.
  void cancelRequest();

  // Checks FETCH_OK against the range the client asked for.  Must run after
  // initializeExpecteds, which sets the window this compares against.
  void validateFetchOk(
      const FetchOk& ok,
      const MoQTestParameters& params,
      const StandaloneFetch& range);

  // The group a joining FETCH backfills from.  A relative join counts back
  // from where the subscription begins, so it needs the reported Largest.
  static uint64_t joiningStartGroup(
      int64_t joinStart,
      const AbsoluteLocation& largest);

  // The client does not choose a joining FETCH's range, but it can reconstruct
  // it: the backfill runs from `startGroup` up to the object the subscription
  // reported as Largest.
  void validateJoiningFetchOk(
      const FetchOk& ok,
      const MoQTestParameters& params,
      const AbsoluteLocation& largest,
      uint64_t startGroup);

  // Drops expectations below `group`.  A join that does not reach back to the
  // start of the track never delivers those, and neither half is at fault.
  void trimExpectedBefore(uint64_t group);

  // Subscription Data Validation functions.  Expectations follow `window`,
  // which is the whole track except for a ranged FETCH.
  void initializeExpecteds(
      MoQTestParameters& params,
      MoQTestFetchWindow window);
  // Arms one half of the request and puts its cursor at the window's start.
  // `seeded` is false for a joining subscription, whose start the publisher
  // chooses.
  void
  startReceiving(ReceiveState& state, ReceivingType type, bool seeded = true);

  // Puts an unseeded cursor at the first object its half delivers.
  void seedCursor(ReceiveState& state, const ObjectHeader& header);
  bool validateSubscribedData(
      ReceiveState& state,
      const ObjectHeader& header,
      const std::string& payload);
  folly::Expected<folly::Unit, ExtensionError> validateExtensions(
      const std::vector<Extension>& extensions,
      MoQTestParameters* params);

  AdjustedExpectedResult adjustExpected(
      ReceiveState& state,
      MoQTestParameters& params,
      const ObjectHeader& header);
  AdjustedExpectedResult adjustExpectedForOneSubgroupPerGroup(
      ReceiveState& state,
      MoQTestParameters& params);
  AdjustedExpectedResult adjustExpectedForOneSubgroupPerObject();
  AdjustedExpectedResult adjustExpectedForTwoSubgroupsPerGroup(
      ReceiveState& state,
      const ObjectHeader& header,
      MoQTestParameters& params);
  AdjustedExpectedResult adjustExpectedForDatagram(
      const ReceiveState& state,
      MoQTestParameters& params);
  bool validateDatagramObjects(const ObjectHeader& header);
};
} // namespace moxygen

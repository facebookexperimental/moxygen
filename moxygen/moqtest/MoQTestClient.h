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
#include "moxygen/moqtest/Types.h"
#include "moxygen/samples/util/Utils.h"

namespace moxygen {

enum ReceivingType : int {
  SUBSCRIBE = 0,
  FETCH = 1,
  UNKNOWN_RECEIVING_TYPE = 2
};

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

// MoQTestClient is also a Subscriber so it can receive server-initiated
// PUBLISH (the response to SUBSCRIBE_TRACKS).
class MoQTestClient : public Subscriber,
                      public std::enable_shared_from_this<MoQTestClient> {
 public:
  MoQTestClient(
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

  // Sends SUBSCRIBE_TRACKS; the server replies with a PUBLISH that this client
  // validates like a SUBSCRIBE. Only works when the whole namespace is
  // specified.
  folly::coro::Task<moxygen::TrackNamespace> subscribeTracks(
      MoQTestParameters params);

  folly::coro::Task<moxygen::TrackNamespace> fetch(MoQTestParameters params);

  void setLogger(const std::shared_ptr<MLogger>& logger);

  // Drains the session so the peer sees a clean close; the event loop exits
  // once the session finishes closing.
  void shutdown();

  // Completes when the track finishes, validation fails, or shutdown() runs.
  // Request coroutines await this so their completion marks "all done".
  folly::coro::Baton doneBaton_;

  folly::coro::Task<void> trackStatus(TrackStatus req);
  void subscribeUpdate(SubscribeUpdate update);

  // Subscriber: handle an incoming PUBLISH by returning the receiver that
  // validates the published track.
  PublishResult publish(
      PublishRequest pub,
      std::shared_ptr<SubscriptionHandle> handle) override;

 private:
  folly::coro::Task<void> doSubscribeUpdate(
      std::shared_ptr<Publisher::SubscriptionHandle> handle,
      SubscribeUpdate update);
  // An ObjectReceiverCallback implementation that forwards calls to a
  // MoQTestClient.
  class ObjectReceiverCallback : public moxygen::ObjectReceiverCallback {
   public:
    explicit ObjectReceiverCallback(MoQTestClient& client) : client_(client) {}

    FlowControlState onObject(
        std::optional<TrackAlias> trackAlias,
        const ObjectHeader& objHeader,
        Payload payload) override {
      return client_.onObject(
          std::move(trackAlias), objHeader, std::move(payload));
    }

    void onObjectStatus(
        std::optional<TrackAlias> trackAlias,
        const ObjectHeader& objHeader) override {
      client_.onObjectStatus(std::move(trackAlias), objHeader);
    }

    void onEndOfStream() override {
      client_.onEndOfStream();
    }

    void onError(ResetStreamErrorCode code) override {
      client_.onError(code);
    }

    void onPublishDone(PublishDone /* done */) override {}

    void onAllDataReceived() override {
      client_.onAllDataReceived();
    }

   private:
    MoQTestClient& client_;
  };

  // Override Vritual Functions for now to return basic print statements
  ObjectReceiverCallback::FlowControlState onObject(
      const std::optional<TrackAlias>& trackAlias,
      const ObjectHeader& objHeader,
      Payload payload);
  void onObjectStatus(
      const std::optional<TrackAlias>& trackAlias,
      const ObjectHeader& objHeader);
  void onEndOfStream();
  void onError(ResetStreamErrorCode);
  void onAllDataReceived();

  ObjectReceiverCallback objectReceiverCallback_{*this};

  std::shared_ptr<MoQFollyExecutorImpl> moqExecutor_;
  std::unique_ptr<MoQClientBase> moqClient_;
  std::shared_ptr<ObjectReceiver> subReceiver_;
  std::shared_ptr<ObjectReceiver> fetchReceiver_;

  // Holds Current Request Parameters
  ReceivingType receivingType_ = ReceivingType::UNKNOWN_RECEIVING_TYPE;
  MoQTestParameters params_;
  RequestID requestID_{};

  // True when the track is delivered via server-initiated PUBLISH (the response
  // to SUBSCRIBE_TRACKS). In that mode we can't drain the session up front (it
  // would reject the incoming PUBLISH), so we drain once the track completes.
  bool publishMode_{false};

  // Holds Current Request Group, SubGroup, and objectId (updated based on
  // expected data)
  uint64_t expectedGroup_{};
  uint64_t expectedSubgroup_{};
  std::array<uint64_t, 2> subgroupToExpectedObjId_{};

  // Scoreboard of expected (group, objectId) pairs
  // When receiving: if present, erase; if absent, it's a duplicate
  // At end: success == scoreboard.empty() (or within drop limit for datagrams)
  std::set<std::pair<uint64_t, uint64_t>> expectedObjects_;

  // Holds if current request expects end of group markers
  bool expectEndOfGroup_{};

  // Holds Datagram Objects Recieved - (Only relevant for forwarding preference
  // 3)
  uint64_t datagramObjects_{};

  // Handles
  std::shared_ptr<Publisher::SubscriptionHandle> subHandle_;
  std::shared_ptr<Publisher::FetchHandle> fetchHandle_;
  std::shared_ptr<Publisher::SubscribeTracksHandle> subscribeTracksHandle_;

  // Subscription Data Validation functions
  void initializeExpecteds(MoQTestParameters& params);
  bool validateSubscribedData(
      const ObjectHeader& header,
      const std::string& payload);
  folly::Expected<folly::Unit, ExtensionError> validateExtensions(
      const std::vector<Extension>& extensions,
      MoQTestParameters* params);

  AdjustedExpectedResult adjustExpected(
      MoQTestParameters& params,
      const ObjectHeader* header);
  AdjustedExpectedResult adjustExpectedForOneSubgroupPerGroup(
      MoQTestParameters& params);
  AdjustedExpectedResult adjustExpectedForOneSubgroupPerObject();
  AdjustedExpectedResult adjustExpectedForTwoSubgroupsPerGroup(
      const ObjectHeader* header,
      MoQTestParameters& params);
  AdjustedExpectedResult adjustExpectedForDatagram(MoQTestParameters& params);
  bool validateDatagramObjects(const ObjectHeader& header);
};
} // namespace moxygen

/*
 *  Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 *  This source code is licensed under the MIT license found in the LICENSE
 *  file in the root directory of this source tree.
 *
 */

#pragma once

#include <moxygen/events/MoQFollyExecutorImpl.h>
#include "moxygen/MoQClient.h"
#include "moxygen/ObjectReceiver.h"
#include "moxygen/Subscriber.h"
#include "moxygen/mlog/MLogger.h"
#include "moxygen/moqtest/Types.h"

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

class MoQTestClient {
 public:
  MoQTestClient(
      folly::EventBase* evb,
      proxygen::URL url,
      bool useQuicTransport);

  ~MoQTestClient() {}

  MoQTestClient(const MoQTestClient&) = delete;
  MoQTestClient& operator=(const MoQTestClient&) = delete;
  MoQTestClient(MoQTestClient&&) = default;
  MoQTestClient& operator=(MoQTestClient&&) = default;

  folly::coro::Task<void> connect(folly::EventBase* evb);

  folly::coro::Task<moxygen::TrackNamespace> subscribe(
      MoQTestParameters params);

  folly::coro::Task<moxygen::TrackNamespace> fetch(MoQTestParameters params);

  void setLogger(const std::shared_ptr<MLogger>& logger);

  folly::coro::Task<void> trackStatus(TrackStatus req);
  void subscribeUpdate(SubscribeUpdate update);

 private:
  // An ObjectReceiverCallback implementation that forwards calls to a
  // MoQTestClient.
  class ObjectReceiverCallback : public moxygen::ObjectReceiverCallback {
   public:
    explicit ObjectReceiverCallback(MoQTestClient& client) : client_(client) {}

    FlowControlState onObject(
        folly::Optional<TrackAlias> trackAlias,
        const ObjectHeader& objHeader,
        Payload payload) override {
      return client_.onObject(
          std::move(trackAlias), objHeader, std::move(payload));
    }

    void onObjectStatus(
        folly::Optional<TrackAlias> trackAlias,
        const ObjectHeader& objHeader) override {
      client_.onObjectStatus(std::move(trackAlias), objHeader);
    }

    void onEndOfStream() override {
      client_.onEndOfStream();
    }

    void onError(ResetStreamErrorCode code) override {
      client_.onError(code);
    }

    void onSubscribeDone(SubscribeDone done) override {
      client_.onSubscribeDone(std::move(done));
    }

   private:
    MoQTestClient& client_;
  };

  // Override Vritual Functions for now to return basic print statements
  ObjectReceiverCallback::FlowControlState onObject(
      const folly::Optional<TrackAlias>& trackAlias,
      const ObjectHeader& objHeader,
      Payload payload);
  void onObjectStatus(
      const folly::Optional<TrackAlias>& trackAlias,
      const ObjectHeader& objHeader);
  void onEndOfStream();
  void onError(ResetStreamErrorCode);
  void onSubscribeDone(const SubscribeDone& done);

  ObjectReceiverCallback objectReceiverCallback_{*this};

  std::shared_ptr<MoQFollyExecutorImpl> moqExecutor_;
  std::unique_ptr<MoQClient> moqClient_;
  std::shared_ptr<ObjectReceiver> subReceiver_;
  std::shared_ptr<ObjectReceiver> fetchReceiver_;

  // Holds Current Request Parameters
  ReceivingType receivingType_ = ReceivingType::UNKNOWN_RECEIVING_TYPE;
  MoQTestParameters params_;
  RequestID requestID_{};

  // Holds Current Request Group, SubGroup, and objectId (updated based on
  // expected data)
  uint64_t expectedGroup_{};
  uint64_t expectedSubgroup_{};
  std::array<uint64_t, 2> subgroupToExpectedObjId_{};

  // Holds if current request expects end of group markers
  bool expectEndOfGroup_{};

  // Holds Datagram Objects Recieved - (Only relevant for forwarding preference
  // 3)
  uint64_t datagramObjects_{};

  // Handles
  std::shared_ptr<Publisher::SubscriptionHandle> subHandle_;
  std::shared_ptr<Publisher::FetchHandle> fetchHandle_;

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
  AdjustedExpectedResult adjustExpectedForOneSubgroupPerObject(
      MoQTestParameters& params);
  AdjustedExpectedResult adjustExpectedForTwoSubgroupsPerGroup(
      const ObjectHeader* header,
      MoQTestParameters& params);
  AdjustedExpectedResult adjustExpectedForDatagram(MoQTestParameters& params);
  bool validateDatagramObjects(const ObjectHeader& header);
};
} // namespace moxygen

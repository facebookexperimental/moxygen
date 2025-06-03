// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#pragma once

#include "moxygen/MoQClient.h"
#include "moxygen/ObjectReceiver.h"
#include "moxygen/Subscriber.h"
#include "moxygen/moqtest/Types.h"

namespace moxygen {

enum ReceivingType : int {
  SUBSCRIBE = 0,
  FETCH = 1,
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

class MoQTestClient : public moxygen::Subscriber,
                      public std::enable_shared_from_this<MoQTestClient>,
                      public ObjectReceiverCallback {
 public:
  MoQTestClient(folly::EventBase* evb, proxygen::URL url);

  ~MoQTestClient() override {}

  folly::coro::Task<void> connect(folly::EventBase* evb);

  void initialize();

  folly::coro::Task<moxygen::TrackNamespace> subscribe(
      MoQTestParameters params);

  folly::coro::Task<moxygen::TrackNamespace> fetch(MoQTestParameters params);

  // Override Vritual Functions for now to return basic print statements
  virtual FlowControlState onObject(
      const ObjectHeader& objHeader,
      Payload payload) override;
  virtual void onObjectStatus(const ObjectHeader& objHeader) override;
  virtual void onEndOfStream() override;
  virtual void onError(ResetStreamErrorCode) override;
  virtual void onSubscribeDone(SubscribeDone done) override;

 private:
  std::unique_ptr<MoQClient> moqClient_;
  std::shared_ptr<ObjectReceiver> subReceiver_;
  std::shared_ptr<ObjectReceiver> fetchReceiver_;

  // Holds Current Request Parameters
  ReceivingType receivingType_;
  MoQTestParameters params_;

  // Holds Current Request Group, SubGroup, and objectId (updated based on
  // expected data)
  uint64_t expectedGroup_;
  uint64_t expectedSubgroup_;
  uint64_t expectedObjectId_;

  // Holds if current request expects end of group markers
  bool expectEndOfGroup_;

  // Holds Datagram Objects Recieved - (Only relevant for forwarding preference
  // 3)
  uint64_t datagramObjects_;

  // Handles
  std::shared_ptr<MoQSession::SubscriptionHandle> subHandle_;
  std::shared_ptr<MoQSession::FetchHandle> fetchHandle_;

  // Subscription Data Validation functions
  void initializeExpecteds(MoQTestParameters& params);
  bool validateSubscribedData(
      const ObjectHeader& header,
      const std::string& payload);
  folly::Expected<folly::Unit, ExtensionError> validateExtensions(
      std::vector<Extension> extensions,
      MoQTestParameters* params);

  AdjustedExpectedResult adjustExpected(MoQTestParameters& params);
  AdjustedExpectedResult adjustExpectedForOneSubgroupPerGroup(
      MoQTestParameters& params);
  AdjustedExpectedResult adjustExpectedForOneSubgroupPerObject(
      MoQTestParameters& params);
  AdjustedExpectedResult adjustExpectedForTwoSubgroupsPerGroup(
      MoQTestParameters& params);
  AdjustedExpectedResult adjustExpectedForDatagram(MoQTestParameters& params);
  bool validateDatagramObjects(const ObjectHeader& header);
};
} // namespace moxygen

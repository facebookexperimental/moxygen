// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#pragma once

#include "moxygen/MoQClient.h"
#include "moxygen/ObjectReceiver.h"
#include "moxygen/Subscriber.h"
#include "moxygen/moqtest/Types.h"

namespace moxygen {

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
};
} // namespace moxygen

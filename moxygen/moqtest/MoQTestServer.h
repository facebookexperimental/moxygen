// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#pragma once

#include "moxygen/MoQServer.h"
#include "moxygen/Publisher.h"
#include "moxygen/moqtest/Types.h"

namespace moxygen {

class MoQTestSubscriptionHandle : public Publisher::SubscriptionHandle {
 public:
  MoQTestSubscriptionHandle(SubscribeOk ok)
      : Publisher::SubscriptionHandle(ok){};

  virtual void unsubscribe() override;
  virtual void subscribeUpdate(SubscribeUpdate subUpdate) override;

 private:
  SubscribeOk subscribeOk_;
};

class MoQTestServer : public moxygen::Publisher,
                      public moxygen::MoQServer,
                      public std::enable_shared_from_this<MoQTestServer> {
 public:
  MoQTestServer(uint16_t port);
  // Override onNewSession to set publisher handler to be this object
  virtual void onNewSession(
      std::shared_ptr<MoQSession> clientSession) override {
    clientSession->setPublishHandler(shared_from_this());
  }

  virtual folly::coro::Task<SubscribeResult> subscribe(
      SubscribeRequest sub,
      std::shared_ptr<TrackConsumer> callback) override;

  folly::coro::Task<void> onSubscribe(
      SubscribeRequest sub,
      std::shared_ptr<TrackConsumer> callback);

  folly::coro::Task<void> sendObjectsForForwardPreferenceZero(
      MoQTestParameters params,
      std::shared_ptr<TrackConsumer> callback);

 private:
};

} // namespace moxygen

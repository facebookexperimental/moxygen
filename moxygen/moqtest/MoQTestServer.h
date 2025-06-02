// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#pragma once

#include "moxygen/MoQServer.h"
#include "moxygen/Publisher.h"
#include "moxygen/moqtest/Types.h"

namespace moxygen {

class MoQTestSubscriptionHandle : public Publisher::SubscriptionHandle {
 public:
  MoQTestSubscriptionHandle(
      SubscribeOk ok,
      folly::CancellationSource* cancellationSource)
      : Publisher::SubscriptionHandle(ok), cancelSource_(cancellationSource){};

  virtual void unsubscribe() override;
  virtual void subscribeUpdate(SubscribeUpdate subUpdate) override;

 private:
  SubscribeOk subscribeOk_;
  folly::CancellationSource* cancelSource_;
};

class MoQTestFetchHandle : public Publisher::FetchHandle {
 public:
  MoQTestFetchHandle(FetchOk ok, folly::CancellationSource* cancellationSource)
      : Publisher::FetchHandle(ok),
        fetchOk_(ok),
        cancelSource_(cancellationSource){};

  virtual void fetchCancel() override;

 private:
  FetchOk fetchOk_;
  folly::CancellationSource* cancelSource_;
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

  // Subscribing Methods
  virtual folly::coro::Task<SubscribeResult> subscribe(
      SubscribeRequest sub,
      std::shared_ptr<TrackConsumer> callback) override;

  folly::coro::Task<void> onSubscribe(
      SubscribeRequest sub,
      std::shared_ptr<TrackConsumer> callback);

  folly::coro::Task<void> sendOneSubgroupPerGroup(
      MoQTestParameters params,
      std::shared_ptr<TrackConsumer> callback);

  folly::coro::Task<void> sendOneSubgroupPerObject(
      MoQTestParameters params,
      std::shared_ptr<TrackConsumer> callback);

  folly::coro::Task<void> sendTwoSubgroupsPerGroup(
      MoQTestParameters params,
      std::shared_ptr<TrackConsumer> callback);

  folly::coro::Task<SubscribeResult> sendDatagram(
      SubscribeRequest sub,
      MoQTestParameters params,
      std::shared_ptr<TrackConsumer> callback);

  // Fetching Methods
  virtual folly::coro::Task<FetchResult> fetch(
      Fetch fetch,
      std::shared_ptr<FetchConsumer> fetchCallback) override;

  folly::coro::Task<void> onFetch(
      Fetch fetch,
      std::shared_ptr<FetchConsumer> callback);

  folly::coro::Task<void> fetchOneSubgroupPerGroup(
      MoQTestParameters params,
      std::shared_ptr<FetchConsumer> callback);

  folly::coro::Task<void> fetchOneSubgroupPerObject(
      MoQTestParameters params,
      std::shared_ptr<FetchConsumer> callback);

  folly::coro::Task<void> fetchTwoSubgroupsPerGroup(
      MoQTestParameters params,
      std::shared_ptr<FetchConsumer> callback);

 private:
  folly::CancellationSource cancelSource_;
};

} // namespace moxygen

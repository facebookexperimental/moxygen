/*
 *  Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 *  This source code is licensed under the MIT license found in the LICENSE
 *  file in the root directory of this source tree.
 *
 */

#pragma once

#include <utility>

#include "moxygen/MoQClient.h"
#include "moxygen/MoQRelaySession.h"
#include "moxygen/MoQServer.h"
#include "moxygen/MoQWebTransportClient.h"
#include "moxygen/Publisher.h"
#include "moxygen/events/MoQFollyExecutorImpl.h"
#include "moxygen/moqtest/Types.h"

namespace moxygen {
class MoQTestSubscriptionHandle : public Publisher::SubscriptionHandle {
 public:
  MoQTestSubscriptionHandle(
      SubscribeOk ok,
      folly::CancellationSource cancellationSource)
      : Publisher::SubscriptionHandle(std::move(ok)),
        cancelSource_(std::move(cancellationSource)) {}

  virtual void unsubscribe() override;
  virtual folly::coro::Task<
      folly::Expected<SubscribeUpdateOk, SubscribeUpdateError>>
  subscribeUpdate(SubscribeUpdate update) override;

 private:
  SubscribeOk subscribeOk_;
  folly::CancellationSource cancelSource_;
};

class MoQTestFetchHandle : public Publisher::FetchHandle {
 public:
  MoQTestFetchHandle(
      const FetchOk& ok,
      folly::CancellationSource cancellationSource)
      : Publisher::FetchHandle(ok),
        fetchOk_(ok),
        cancelSource_(std::move(cancellationSource)) {}

  virtual void fetchCancel() override;

 private:
  FetchOk fetchOk_;
  folly::CancellationSource cancelSource_;
};

class MoQTestServer : public moxygen::Publisher,
                      public moxygen::MoQServer,
                      public std::enable_shared_from_this<MoQTestServer> {
 public:
  MoQTestServer(const std::string& cert = "", const std::string& key = "");

  //  Override onNewSession to set publisher handler to be this object
  virtual void onNewSession(
      std::shared_ptr<MoQSession> clientSession) override {
    clientSession->setPublishHandler(shared_from_this());
    // Use server-level logger if set, otherwise try factory
    if (auto logger = createLogger()) {
      clientSession->setLogger(std::move(logger));
    }
  }

  // Relay client support
  bool startRelayClient(
      const std::string& relayUrl,
      int32_t connectTimeout,
      int32_t transactionTimeout,
      bool useQuicTransport);

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

  folly::coro::Task<void> sendDatagram(
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

  folly::coro::Task<void> fetchDatagram(
      MoQTestParameters params,
      std::shared_ptr<FetchConsumer> callback) {
    co_return co_await fetchOneSubgroupPerObject(params, std::move(callback));
  }

 private:
  folly::coro::Task<void> doRelaySetup(
      const std::string& relayUrl,
      int32_t connectTimeout,
      int32_t transactionTimeout);

  // Relay client connection (if using relay mode)
  std::unique_ptr<MoQClient> relayClient_;
  std::shared_ptr<MoQRelaySession> relaySession_;
  std::shared_ptr<Subscriber::AnnounceHandle> announceHandle_;
  std::shared_ptr<MoQFollyExecutorImpl> moqEvb_;
};

} // namespace moxygen

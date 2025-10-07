/*
 *  Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 *  This source code is licensed under the MIT license found in the LICENSE
 *  file in the root directory of this source tree.
 *
 */

#pragma once

#include <utility>

#include "moxygen/MoQServer.h"
#include "moxygen/Publisher.h"
#include "moxygen/moqtest/Types.h"

namespace moxygen {

class MoQTAnnounceCallback : public Subscriber::AnnounceCallback {
 public:
  MoQTAnnounceCallback() = default;

  virtual void announceCancel(
      AnnounceErrorCode errorCode,
      std::string reasonPhrase) override;
};

class MoQTestSubscriptionHandle : public Publisher::SubscriptionHandle {
 public:
  MoQTestSubscriptionHandle(
      SubscribeOk ok,
      folly::CancellationSource* cancellationSource)
      : Publisher::SubscriptionHandle(std::move(ok)),
        cancelSource_(cancellationSource){};

  virtual void unsubscribe() override;
  virtual void subscribeUpdate(SubscribeUpdate subUpdate) override;

 private:
  SubscribeOk subscribeOk_;
  folly::CancellationSource* cancelSource_;
};

class MoQTestSubscribeAnnouncesHandle
    : public Publisher::SubscribeAnnouncesHandle {
 public:
  MoQTestSubscribeAnnouncesHandle(SubscribeAnnouncesOk ok)
      : Publisher::SubscribeAnnouncesHandle(std::move(ok)) {}
  virtual void unsubscribeAnnounces() override;
};

class MoQTestFetchHandle : public Publisher::FetchHandle {
 public:
  MoQTestFetchHandle(
      const FetchOk& ok,
      folly::CancellationSource* cancellationSource)
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
  MoQTestServer();
  //  Override onNewSession to set publisher handler to be this object
  virtual void onNewSession(
      std::shared_ptr<MoQSession> clientSession) override {
    clientSession->setPublishHandler(shared_from_this());
    if (logger_) {
      clientSession->setLogger(logger_);
    }
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

  // Methods For Tests in MoQTrackServerTest
  bool isSubCancelled();
  bool isFetchCancelled();
  void initializeCancellationSources() {
    subCancelSource_ = std::make_shared<folly::CancellationSource>();
    fetchCancelSource_ = std::make_shared<folly::CancellationSource>();
  }

  virtual void goaway(Goaway goaway) override;
  virtual folly::coro::Task<SubscribeAnnouncesResult> subscribeAnnounces(
      SubscribeAnnounces subAnn) override;

 private:
  std::shared_ptr<folly::CancellationSource> subCancelSource_;
  std::shared_ptr<folly::CancellationSource> fetchCancelSource_;
  std::shared_ptr<MoQSession> subSession_;
  std::shared_ptr<MoQSession> fetchSession_;
};

} // namespace moxygen

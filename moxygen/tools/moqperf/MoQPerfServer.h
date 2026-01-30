/*
 *  Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 *  This source code is licensed under the MIT license found in the LICENSE
 *  file in the root directory of this source tree.
 *
 */

#pragma once

#include <moxygen/MoQServer.h>

namespace moxygen {

class PerfSubscriptionHandle : public Publisher::SubscriptionHandle {
 public:
  PerfSubscriptionHandle(
      SubscribeOk ok,
      folly::CancellationSource* cancellationSource)
      : Publisher::SubscriptionHandle(std::move(ok)),
        cancellationSource_(cancellationSource) {}
  ~PerfSubscriptionHandle() override = default;

  void unsubscribe() override {
    cancellationSource_->requestCancellation();
  }

  folly::coro::Task<folly::Expected<SubscribeUpdateOk, SubscribeUpdateError>>
  subscribeUpdate(SubscribeUpdate update) override {
    co_return folly::makeUnexpected(
        SubscribeUpdateError{
            update.requestID,
            SubscribeUpdateErrorCode::NOT_SUPPORTED,
            "Subscribe update not implemented"});
  }

 private:
  folly::CancellationSource* cancellationSource_;
};

class PerfFetchHandle : public Publisher::FetchHandle {
 public:
  PerfFetchHandle(FetchOk ok, folly::CancellationSource* cancellationSource)
      : Publisher::FetchHandle(ok), cancellationSource_(cancellationSource) {}
  ~PerfFetchHandle() override = default;

  virtual void fetchCancel() {
    cancellationSource_->requestCancellation();
  }

 private:
  folly::CancellationSource* cancellationSource_;
};

class MoQPerfServer : public moxygen::Publisher,
                      public moxygen::MoQServer,
                      public std::enable_shared_from_this<MoQPerfServer> {
 public:
  MoQPerfServer(std::string cert, std::string key);

  folly::coro::Task<SubscribeResult> subscribe(
      SubscribeRequest subscribeRequest,
      std::shared_ptr<TrackConsumer> callback) override;

  folly::coro::Task<FetchResult> fetch(
      Fetch fetch,
      std::shared_ptr<FetchConsumer> callback) override;

  void onNewSession(
      std::shared_ptr<moxygen::MoQSession> clientSession) override;

  void terminateClientSession(
      std::shared_ptr<moxygen::MoQSession> /* clientSession */) override {}

 private:
  folly::coro::Task<void> writeLoop(
      std::shared_ptr<TrackConsumer> trackConsumer,
      SubscribeRequest req);
  folly::coro::Task<void> writeLoopFetch(
      std::shared_ptr<FetchConsumer> fetchConsumer,
      Fetch req);

  // Used to cancel the write loop when the client sends us an UNSUBSCRIBE.
  // Note that we currently only support one subscription during the lifetime
  // of this server.
  folly::CancellationSource cancellationSource_;
  std::optional<RequestID> requestId_;
};

} // namespace moxygen

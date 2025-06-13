// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

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

  void subscribeUpdate(SubscribeUpdate /* subUpdate */) override {}

 private:
  folly::CancellationSource* cancellationSource_;
};

class MoQPerfServer : public moxygen::Publisher,
                      public moxygen::MoQServer,
                      public std::enable_shared_from_this<MoQPerfServer> {
 public:
  MoQPerfServer(uint16_t sourcePort, std::string cert, std::string key);

  folly::coro::Task<SubscribeResult> subscribe(
      SubscribeRequest subscribeRequest,
      std::shared_ptr<TrackConsumer> callback) override;

  void onNewSession(
      std::shared_ptr<moxygen::MoQSession> clientSession) override;

  void terminateClientSession(
      std::shared_ptr<moxygen::MoQSession> /* clientSession */) override {}

 private:
  folly::coro::Task<void> writeLoop(
      std::shared_ptr<TrackConsumer> trackConsumer);

  // Used to cancel the write loop when the client sends us an UNSUBSCRIBE.
  // Note that we currently only support one subscription during the lifetime
  // of this server.
  folly::CancellationSource cancellationSource_;
};

} // namespace moxygen

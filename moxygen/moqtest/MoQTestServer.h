/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <utility>

#include <folly/container/F14Map.h>
#include "moxygen/MoQClient.h"
#include "moxygen/MoQQmuxServer.h"
#include "moxygen/MoQRelaySession.h"
#include "moxygen/MoQServer.h"
#include "moxygen/MoQWebTransportClient.h"
#include "moxygen/Publisher.h"
#include "moxygen/events/MoQFollyExecutorImpl.h"
#include "moxygen/moqtest/Types.h"
#include "moxygen/relay/MoQForwarder.h"

namespace moxygen {

class MoQTestFetchHandle : public Publisher::FetchHandle {
 public:
  MoQTestFetchHandle(
      const FetchOk& ok,
      folly::CancellationSource cancellationSource)
      : Publisher::FetchHandle(ok),
        fetchOk_(ok),
        cancelSource_(std::move(cancellationSource)) {}

  virtual void fetchCancel() override;
  using RequestUpdateResult = folly::Expected<RequestOk, RequestError>;
  virtual folly::coro::Task<RequestUpdateResult> requestUpdate(
      RequestUpdate reqUpdate) override;

 private:
  FetchOk fetchOk_;
  folly::CancellationSource cancelSource_;
};

class MoQTestServer : public moxygen::Publisher, public moxygen::MoQServer {
 public:
  struct SubKey {
    MoQSession* session;
    uint64_t requestID;
    bool operator==(const SubKey& o) const {
      return session == o.session && requestID == o.requestID;
    }
    struct Hash {
      size_t operator()(const SubKey& k) const {
        return folly::hash::hash_combine(k.session, k.requestID);
      }
    };
  };

  MoQTestServer(
      const std::string& cert = "",
      const std::string& key = "",
      const std::string& versions = "");

  void setIncludeTimestampExtension(bool include) {
    includeTimestampExtension_ = include;
  }

  //  Override onNewSession to set publisher handler to be this object
  virtual void onNewSession(
      std::shared_ptr<MoQSession> clientSession) override {
    clientSession->setPublishHandler(
        std::static_pointer_cast<MoQTestServer>(shared_from_this()));
    // Use server-level logger if set, otherwise try factory
    if (auto logger = createLogger()) {
      clientSession->setLogger(std::move(logger));
    }
  }

  void removeSubscription(SubKey key);

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

  struct SubscriptionState {
    std::shared_ptr<MoQForwarder> forwarder;
    folly::CancellationSource cancelSource;
  };

  // Relay client connection (if using relay mode)
  std::string versions_;
  std::unique_ptr<MoQClient> relayClient_;
  std::shared_ptr<MoQRelaySession> relaySession_;
  std::shared_ptr<Subscriber::PublishNamespaceHandle> publishNamespaceHandle_;
  std::shared_ptr<MoQFollyExecutorImpl> moqEvb_;
  folly::F14FastMap<SubKey, SubscriptionState, SubKey::Hash>
      activeSubscriptions_;
  bool includeTimestampExtension_{false};
};

// QMUX-on-TCP variant of MoQTestServer. Holds a shared MoQTestServer purely
// for its Publisher implementation (subscribe/fetch). The wrapped instance
// does NOT need to also be started as a QUIC server — constructing it just
// to use as a publisher is fine.
class MoQTestQmuxServer : public MoQQmuxServer {
 public:
  MoQTestQmuxServer(
      std::shared_ptr<MoQTestServer> publisher,
      std::string endpoint,
      std::shared_ptr<const fizz::server::FizzServerContext> fizzContext,
      Config config = {})
      : MoQQmuxServer(
            std::move(endpoint),
            std::move(fizzContext),
            std::move(config)),
        publisher_(std::move(publisher)) {}

  void onNewSession(std::shared_ptr<MoQSession> clientSession) override {
    clientSession->setPublishHandler(publisher_);
    if (auto logger = createLogger()) {
      clientSession->setLogger(logger);
    }
  }

 private:
  std::shared_ptr<MoQTestServer> publisher_;
};

} // namespace moxygen

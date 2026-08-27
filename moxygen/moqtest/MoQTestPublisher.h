/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/container/F14Map.h>
#include <folly/futures/ThreadWheelTimekeeper.h>
#include "moxygen/Publisher.h"
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

// Generates moq-test track data for SUBSCRIBE and FETCH. Transport-agnostic:
// it holds no listener and no relay connection, so it can be used by the
// moqtest server, the QMUX server, the client's publisher session, and unit
// tests alike.
class MoQTestPublisher : public Publisher,
                         public std::enable_shared_from_this<MoQTestPublisher> {
 public:
  struct SubKey {
    MoQSession* session{nullptr};
    uint64_t requestID{0};
    bool operator==(const SubKey& o) const {
      return session == o.session && requestID == o.requestID;
    }
    struct Hash {
      size_t operator()(const SubKey& k) const {
        return folly::hash::hash_combine(k.session, k.requestID);
      }
    };
  };

  void setIncludeTimestampExtension(bool include) {
    includeTimestampExtension_ = include;
  }

  // Cancels in-flight send coroutines so they stop co_await'ing on the
  // timekeeper, which would otherwise crash if recreated during teardown.
  void cancelAll();

  void removeSubscription(SubKey key);

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
  // Inter-object delay using the publisher-owned timekeeper.
  folly::coro::Task<void> delay(uint64_t ms);

  struct SubscriptionState {
    std::shared_ptr<MoQForwarder> forwarder;
    folly::CancellationSource cancelSource;
  };

  folly::F14FastMap<SubKey, SubscriptionState, SubKey::Hash>
      activeSubscriptions_;
  // Owned timekeeper for inter-object delays. Avoids the global Timekeeper
  // singleton, which can crash if used during process teardown.
  folly::ThreadWheelTimekeeper timekeeper_;
  bool includeTimestampExtension_{false};
};

} // namespace moxygen

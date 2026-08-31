/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/container/F14Map.h>
#include <folly/coro/SharedPromise.h>
#include <folly/futures/ThreadWheelTimekeeper.h>
#include "moxygen/Publisher.h"
#include "moxygen/moqtest/Types.h"
#include "moxygen/relay/MoQForwarder.h"

namespace moxygen {

class MoQTestFetchHandle : public Publisher::FetchHandle {
 public:
  MoQTestFetchHandle(
      const FetchOk& ok,
      std::shared_ptr<folly::CancellationSource> cancellationSource)
      : Publisher::FetchHandle(ok),
        fetchOk_(ok),
        cancelSource_(std::move(cancellationSource)) {}

  virtual void fetchCancel() override;
  using RequestUpdateResult = folly::Expected<RequestOk, RequestError>;
  virtual folly::coro::Task<RequestUpdateResult> requestUpdate(
      RequestUpdate reqUpdate) override;

 private:
  FetchOk fetchOk_;
  std::shared_ptr<folly::CancellationSource> cancelSource_;
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

  // Cancels in-flight send and fetch coroutines so they stop co_await'ing on
  // the timekeeper, which would otherwise crash if recreated during teardown,
  // and releases any publishTrack that is still paused waiting for the peer to
  // ask for data -- otherwise it would hold the session open forever.
  void cancelAll();

  void removeSubscription(SubKey key);

  // Subscribing Methods
  virtual folly::coro::Task<SubscribeResult> subscribe(
      SubscribeRequest sub,
      std::shared_ptr<TrackConsumer> callback) override;

  folly::coro::Task<void> onSubscribe(
      SubscribeRequest sub,
      std::shared_ptr<TrackConsumer> callback);

  // Emits the track according to the forwarding preference, then publishDone.
  // Shared by the SUBSCRIBE path and by callers that drive a server-initiated
  // PUBLISH.
  folly::coro::Task<void> sendTrackData(
      MoQTestParameters params,
      RequestID requestID,
      std::shared_ptr<TrackConsumer> callback);

  // Sends PUBLISH for the track on the given session and waits for PUBLISH_OK.
  // The returned task streams the objects once the peer unpauses the track, so
  // a caller that must act after the peer has accepted the PUBLISH -- such as
  // sending the SUBSCRIBE_TRACKS that triggers the unpause -- can do so in
  // between. No prior PUBLISH_NAMESPACE is required. Throws on failure.
  folly::coro::Task<folly::coro::Task<void>> startPublishTrack(
      const std::shared_ptr<MoQSession>& session,
      FullTrackName ftn,
      MoQTestParameters params,
      RequestID requestID);

  // startPublishTrack for callers with nothing to do in between.
  folly::coro::Task<void> publishTrack(
      const std::shared_ptr<MoQSession>& session,
      FullTrackName ftn,
      MoQTestParameters params,
      RequestID requestID);

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
      RequestID requestID,
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
  // Tracks one publishTrack that is paused waiting for the peer to ask for
  // data. Registered so cancelAll() can release it during shutdown, where it
  // completes with OperationCancelled and unwinds the publish.
  struct PendingUnpause : public MoQForwarder::Callback {
    folly::coro::SharedPromise<void> unpaused;

    void onEmpty(MoQForwarder*) override {}

    // forwardChanged can fire more than once as the flag toggles; only the
    // first unpause matters here.
    void forwardChanged(MoQForwarder*, bool forward) override {
      if (forward && !unpaused.isFulfilled()) {
        unpaused.setValue();
      }
    }

    void cancel() {
      if (!unpaused.isFulfilled()) {
        unpaused.setException(
            folly::make_exception_wrapper<folly::OperationCancelled>());
      }
    }
  };

  // Second phase of startPublishTrack.
  folly::coro::Task<void> streamPublishedTrack(
      std::shared_ptr<PendingUnpause> unpauseCb,
      std::shared_ptr<MoQForwarder> forwarder,
      MoQTestParameters params,
      RequestID requestID);

  // Runs onFetch and drops the fetch's cancellation source from
  // activeFetches_ however it ends.
  folly::coro::Task<void> runFetch(
      std::shared_ptr<folly::CancellationSource> cancelSource,
      Fetch fetch,
      std::shared_ptr<FetchConsumer> callback);

  // Inter-object delay using the publisher-owned timekeeper.
  folly::coro::Task<void> delay(uint64_t ms);

  struct SubscriptionState {
    std::shared_ptr<MoQForwarder> forwarder;
    folly::CancellationSource cancelSource;
  };

  folly::F14FastMap<SubKey, SubscriptionState, SubKey::Hash>
      activeSubscriptions_;
  std::vector<std::shared_ptr<PendingUnpause>> pendingUnpauses_;
  // Cancellation sources for fetches that are still generating objects, so
  // cancelAll() reaches them the way it reaches subscriptions.
  std::vector<std::shared_ptr<folly::CancellationSource>> activeFetches_;
  // Owned timekeeper for inter-object delays. Avoids the global Timekeeper
  // singleton, which can crash if used during process teardown.
  folly::ThreadWheelTimekeeper timekeeper_;
  bool includeTimestampExtension_{false};
};

} // namespace moxygen

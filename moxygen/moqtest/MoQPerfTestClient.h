/*
 *  Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 *  This source code is licensed under the MIT license found in the LICENSE
 *  file in the root directory of this source tree.
 *
 */

#pragma once

#include <memory>
#include <vector>

#include <folly/container/F14Map.h>
#include <folly/io/async/EventBase.h>
#include <proxygen/lib/utils/URL.h>

#include "moxygen/MoQClient.h"
#include "moxygen/MoQWebTransportClient.h"
#include "moxygen/ObjectReceiver.h"
#include "moxygen/events/MoQFollyExecutorImpl.h"
#include "moxygen/moqtest/Types.h"

namespace moxygen {

class MoQPerfTestClient;

// Tracks state and statistics for a single subscriber
class SubscriberState {
 public:
  SubscriberState(
      MoQPerfTestClient& testClient,
      size_t id,
      std::shared_ptr<MoQFollyExecutorImpl> executor,
      const proxygen::URL& url,
      bool useQuicTransport);

  ~SubscriberState();

  // Deleted copy/move to keep pointers stable
  SubscriberState(const SubscriberState&) = delete;
  SubscriberState& operator=(const SubscriberState&) = delete;
  SubscriberState(SubscriberState&&) = delete;
  SubscriberState& operator=(SubscriberState&&) = delete;

  folly::coro::Task<void> connect();
  folly::coro::Task<void> subscribe(
      const MoQTestParameters& params,
      uint32_t deliveryTimeoutMs);

  // Drain the MoQ session
  void drain();

  // Statistics
  MoQPerfTestClient& testClient_;
  size_t id_;
  uint64_t objectsReceived_{0};
  uint64_t bytesReceived_{0};
  bool hasError_{false};

 private:
  // ObjectReceiverCallback implementation
  class Callback : public ObjectReceiverCallback {
   public:
    explicit Callback(SubscriberState& state) : state_(state) {}

    FlowControlState onObject(
        std::optional<TrackAlias> trackAlias,
        const ObjectHeader& objHeader,
        Payload payload) override;

    void onObjectStatus(
        std::optional<TrackAlias> trackAlias,
        const ObjectHeader& objHeader) override;

    void onEndOfStream() override;
    void onError(ResetStreamErrorCode code) override;
    void onPublishDone(PublishDone done) override;
    void onAllDataReceived() override;

   private:
    SubscriberState& state_;
  };

  Callback callback_{*this};
  std::shared_ptr<MoQFollyExecutorImpl> moqExecutor_;
  std::unique_ptr<MoQClientBase> moqClient_;
  std::shared_ptr<ObjectReceiver> receiver_;
  std::shared_ptr<Publisher::SubscriptionHandle> subHandle_;
};

// Main performance test client that manages multiple subscribers
class MoQPerfTestClient {
 public:
  MoQPerfTestClient(
      folly::EventBase* evb,
      proxygen::URL url,
      bool useQuicTransport,
      uint32_t durationSeconds,
      uint32_t maxSubscribersPerSecond,
      uint32_t maxSubscribers,
      uint32_t firstObjectSize,
      uint32_t otherObjectSize,
      uint32_t deliveryTimeoutMs,
      uint32_t objectsPerGroup);

  ~MoQPerfTestClient() = default;

  // Run the performance test
  folly::coro::Task<void> run();

  // Get test results
  struct TestResults {
    size_t subscribersReached{0}; // peak
    size_t currentSubscribers{0}; // current active count
    uint64_t totalObjects{0};
    uint64_t totalBytes{0};
    uint32_t totalResets{0};
    uint32_t totalFailures{0};
    uint32_t durationSeconds{0};
    bool trackEnded{false};
  };

  TestResults getResults() const;

  void completed();
  void recordReset();
  void recordFailure();
  void recordTrackRestart();
  void removeSubscriber(size_t id);
  void updateLargestObjectSeen(const AbsoluteLocation& location);
  std::optional<AbsoluteLocation> getLargestObjectSeen() const;

 private:
  // Add a new subscriber
  folly::coro::Task<void> addSubscriber();

  // Configuration
  folly::EventBase* evb_;
  proxygen::URL url_;
  bool useQuicTransport_;
  uint32_t durationSeconds_;
  uint32_t maxSubscribersPerSecond_;
  uint32_t maxSubscribers_;
  uint32_t deliveryTimeoutMs_;

  // Shared executor for all subscribers
  std::shared_ptr<MoQFollyExecutorImpl> sharedExecutor_;

  // MoQ test parameters
  MoQTestParameters params_;

  // Single-threaded state (only accessed on client thread)
  size_t subscribersAdded_{0};
  folly::F14FastMap<size_t, std::unique_ptr<SubscriberState>> subscribers_;
  uint32_t resetsInCurrentInterval_{0};
  uint32_t failuresInCurrentInterval_{0};
  bool trackRestarted_{false};
  std::optional<AbsoluteLocation> largestObjectSeen_;
  std::chrono::steady_clock::time_point startTime_;

  // Atomic cross-thread state (accessed from aggregation thread in getResults)
  std::atomic<size_t> peakSubscribers_{0};
  std::atomic<uint32_t> totalResets_{0};
  std::atomic<uint32_t> totalFailures_{0};
  std::atomic<uint32_t> numCompleted_{0};
  std::atomic<uint64_t> cumulativeObjects_{0};
  std::atomic<uint64_t> cumulativeBytes_{0};
};

} // namespace moxygen

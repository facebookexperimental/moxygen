/*
 *  Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 *  This source code is licensed under the MIT license found in the LICENSE
 *  file in the root directory of this source tree.
 *
 */

#pragma once

#include <folly/CancellationToken.h>
#include <folly/io/async/ScopedEventBaseThread.h>
#include <folly/synchronization/Baton.h>
#include <moxygen/MoQFramer.h>
#include <moxygen/MoQSession.h>
#include <moxygen/Publisher.h>
#include <moxygen/Subscriber.h>
#include <moxygen/events/MoQFollyExecutorImpl.h>
#include <moxygen/relay/MoQForwarder.h>
#include <moxygen/relay/MoQRelayClient.h>
#include <atomic>
#include <memory>
#include <mutex>
#include <unordered_map>

namespace moxygen {

class MoQRelayClient;
struct MediaItem;

// Audio-only publisher extracted from the original MoQVideoPublisher
class MoQAudioPublisher
    : public Publisher,
      public std::enable_shared_from_this<MoQAudioPublisher> {
 public:
  explicit MoQAudioPublisher(FullTrackName fullAudioTrackName)
      : evbThread_(std::make_unique<folly::ScopedEventBaseThread>()),
        moqExecutor_(
            std::make_shared<MoQFollyExecutorImpl>(evbThread_->getEventBase())),
        audioForwarder_(std::move(fullAudioTrackName)) {}

  // Setup the relay client and MoQ session. Optionally install a Subscriber
  // so that the publisher session can also accept inbound PUBLISH (e.g., echo)
  bool setup(
      const std::string& connectURL,
      std::shared_ptr<Subscriber> subscriber = nullptr);

  void publishAudioFrame(
      std::chrono::microseconds ptsUs,
      uint64_t flags,
      Payload payload);

  void endPublish();

  // Publish a reliable zero-length control object to signal end-of-utterance
  void signalEndOfUtterance();
  // Overload that stamps client PTT release (monotonic µs)
  void signalEndOfUtterance(uint64_t clientReleaseUs);

  folly::coro::Task<SubscribeResult> subscribe(
      SubscribeRequest sub,
      std::shared_ptr<TrackConsumer> callback) override;

  // Returns {srtt_us, lrtt_us} for the current session transport. 0 if unknown.
  std::pair<uint64_t, uint64_t> getRttMicros();

  // Optional: enable/disable vendor timestamp extensions (t0 on publish)
  void setUseTimestampExtensions(bool enable) {
    useTimestampExt_.store(enable, std::memory_order_relaxed);
  }

  // Record client-send timestamp (t0) keyed by audio PTS (microseconds)
  void noteClientAudioSendTs(uint64_t ptsUs, uint64_t t0Us);

 private:
  void publishAudioFrameToMoQ(std::unique_ptr<MediaItem> item);
  void publishAudioFrameImpl(
      std::chrono::microseconds ptsUs,
      uint64_t flags,
      Payload payload);

  // Proper member function to handle initial audio PUBLISH on the EB
  folly::coro::Task<void> initialAudioPublish(
      std::shared_ptr<MoQSession> session,
      FullTrackName ftn);

  std::unique_ptr<folly::ScopedEventBaseThread> evbThread_;
  std::shared_ptr<MoQFollyExecutorImpl> moqExecutor_;
  std::unique_ptr<MoQRelayClient> relayClient_;
  MoQForwarder audioForwarder_;
  AbsoluteLocation largestAudio_{0, 0};
  std::shared_ptr<TrackConsumer> audioTrackPublisher_;
  bool audioPublishReady_{false};
  bool running_{false};
  folly::CancellationSource cancel_;

  uint64_t audioSeqId_{0};
  folly::Optional<uint64_t> lastAudioPts_;

  // Relay run coordination: ensure we don't destroy on EB thread and we can
  // wait for run() to finish on stop
  folly::Baton<> runDone_;
  std::atomic<bool> relayStarted_{false};

  // Optional vendor timestamp extensions (default disabled)
  std::atomic<bool> useTimestampExt_{false};

  // Temporary map of PTS->client-send ts (t0), filled at JNI boundary
  std::mutex t0Mutex_;
  std::unordered_map<uint64_t, uint64_t> t0ByPts_;
};

} // namespace moxygen

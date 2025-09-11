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

namespace moxygen {

class MoQRelayClient;
struct MediaItem;

class MoQVideoPublisher
    : public Publisher,
      public std::enable_shared_from_this<MoQVideoPublisher> {
 public:
  MoQVideoPublisher(
      FullTrackName fullVideoTrackName,
      FullTrackName fullAudioTrackName,
      uint64_t timescale = 30)
      : evbThread_(std::make_unique<folly::ScopedEventBaseThread>()),
        moqExecutor_(evbThread_->getEventBase()),
        videoForwarder_(std::move(fullVideoTrackName)),
        audioForwarder_(std::move(fullAudioTrackName)) {}

  // Setup the relay client and MoQ session. Optionally install a Subscriber
  // so that the publisher session can also accept inbound PUBLISH (e.g., echo)
  bool setup(
      const std::string& connectURL,
      std::shared_ptr<Subscriber> subscriber = nullptr);

  /**
   * Publishes a single frame of the video stream.
   *
   * @param payload The frame data to be published.
   */
  void publishVideoFrame(
      std::chrono::microseconds ptsUs,
      uint64_t flags,
      Payload payload);

  void publishAudioFrame(
      std::chrono::microseconds ptsUs,
      uint64_t flags,
      Payload payload);

  /**
   * Ends publishing the video stream.
   */
  void endPublish();

  folly::coro::Task<SubscribeResult> subscribe(
      SubscribeRequest sub,
      std::shared_ptr<TrackConsumer> callback) override;

  // Returns {srtt_us, lrtt_us} for the current session transport. 0 if unknown.
  std::pair<uint64_t, uint64_t> getRttMicros();

 private:
  void publishFrameToMoQ(std::unique_ptr<MediaItem> item);
  void publishFrameImpl(
      std::chrono::microseconds ptsUs,
      uint64_t flags,
      Payload payload);
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
  MoQFollyExecutorImpl moqExecutor_;
  std::unique_ptr<MoQRelayClient> relayClient_;
  // uint64_t timescale_{30};
  MoQForwarder videoForwarder_;
  MoQForwarder audioForwarder_;
  AbsoluteLocation largestVideo_{0, 0};
  std::shared_ptr<TrackConsumer> audioTrackPublisher_;
  bool audioPublishReady_{false};
  bool running_{false};
  folly::CancellationSource cancel_;

  AbsoluteLocation largestAudio_{0, 0};
  std::shared_ptr<SubgroupConsumer> videoSgPub_;
  std::shared_ptr<SubgroupConsumer> audioSgPub_;
  uint64_t videoSeqId_{0};
  uint64_t audioSeqId_{0};
  folly::Optional<uint64_t> lastVideoPts_;
  folly::Optional<uint64_t> lastAudioPts_;
  std::unique_ptr<folly::IOBuf> savedMetadata_;

  // Relay run coordination: ensure we don't destroy on EB thread and we can
  // wait for run() to finish on stop
  folly::Baton<> runDone_;
  std::atomic<bool> relayStarted_{false};
};

} // namespace moxygen

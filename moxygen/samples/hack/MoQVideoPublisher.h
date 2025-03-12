// MoQVideoStreamPublisher.h

#pragma once

#include <folly/io/async/ScopedEventBaseThread.h>
#include <moxygen/MoQFramer.h>
#include <moxygen/Publisher.h>
#include <moxygen/relay/MoQForwarder.h>
#include <moxygen/relay/MoQRelayClient.h>

namespace moxygen {

class MoQRelayClient;
struct MediaItem;

class MoQVideoPublisher
    : public Publisher,
      public std::enable_shared_from_this<MoQVideoPublisher> {
 public:
  MoQVideoPublisher(FullTrackName fullVideoTrackName, uint64_t timescale = 30)
      : timescale_(timescale), videoForwarder_(std::move(fullVideoTrackName)) {
    evbThread_ = std::make_unique<folly::ScopedEventBaseThread>();
  }

  bool setup(const std::string& connectURL);

  /**
   * Publishes a single frame of the video stream.
   *
   * @param payload The frame data to be published.
   */
  void publishVideoFrame(
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

 private:
  void publishFrameToMoQ(std::unique_ptr<MediaItem> item);
  void publishFrameImpl(
      std::chrono::microseconds ptsUs,
      uint64_t flags,
      Payload payload);

  std::unique_ptr<folly::ScopedEventBaseThread> evbThread_;
  std::unique_ptr<MoQRelayClient> relayClient_;
  uint64_t timescale_{30};
  MoQForwarder videoForwarder_;
  AbsoluteLocation latestVideo_{0, 0};
  std::shared_ptr<SubgroupConsumer> videoSgPub_;
  uint64_t videoSeqId_{0};
  folly::Optional<uint64_t> lastVideoPts_;
  std::unique_ptr<folly::IOBuf> savedMetadata_;
};

} // namespace moxygen

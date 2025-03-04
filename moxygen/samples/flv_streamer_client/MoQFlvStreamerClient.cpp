/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <folly/init/Init.h>
#include <folly/io/async/AsyncSignalHandler.h>
#include <folly/portability/GFlags.h>
#include <signal.h>
#include <filesystem>
#include "moxygen/MoQClient.h"
#include "moxygen/flv_parser/FlvSequentialReader.h"
#include "moxygen/moq_mi/MoQMi.h"

DEFINE_string(input_flv_file, "", "FLV input fifo file");
DEFINE_string(
    connect_url,
    "https://localhost:4433/moq",
    "URL for webtransport server");
DEFINE_string(track_namespace, "flvstreamer", "Track Namespace");
DEFINE_string(track_namespace_delimiter, "/", "Track Namespace Delimiter");
DEFINE_string(video_track_name, "video0", "Video track Name");
DEFINE_string(audio_track_name, "audio0", "Audio track Name");
DEFINE_int32(connect_timeout, 1000, "Connect timeout (ms)");
DEFINE_int32(transaction_timeout, 120, "Transaction timeout (s)");
DEFINE_bool(quic_transport, false, "Use raw QUIC transport");

namespace {
using namespace moxygen;

class MoQFlvStreamerClient
    : public Publisher,
      public std::enable_shared_from_this<MoQFlvStreamerClient> {
 public:
  MoQFlvStreamerClient(
      folly::EventBase* evb,
      proxygen::URL url,
      FullTrackName fvtn,
      FullTrackName fatn)
      : moqClient_(
            evb,
            std::move(url),
            (FLAGS_quic_transport ? MoQClient::TransportType::QUIC
                                  : MoQClient::TransportType::H3_WEBTRANSPORT)),
        fullVideoTrackName_(std::move(fvtn)),
        fullAudioTrackName_(std::move(fatn)) {}

  folly::coro::Task<void> run(Announce ann) noexcept {
    XLOG(INFO) << __func__;
    auto g =
        folly::makeGuard([func = __func__] { XLOG(INFO) << "exit " << func; });
    try {
      // Create session
      co_await moqClient_.setupMoQSession(
          std::chrono::milliseconds(FLAGS_connect_timeout),
          std::chrono::seconds(FLAGS_transaction_timeout),
          /*publishHandler=*/shared_from_this(),
          /*subscribeHandler=*/nullptr);
      // Announce
      auto annResp = co_await moqClient_.moqSession_->announce(std::move(ann));
      if (annResp.hasValue()) {
        announceHandle_ = std::move(annResp.value());
        folly::getGlobalIOExecutor()->add([this] { publishLoop(); });
      } else {
        XLOG(INFO) << "Announce error trackNamespace="
                   << annResp.error().trackNamespace << " code="
                   << folly::to_underlying(annResp.error().errorCode)
                   << " reason=" << annResp.error().reasonPhrase;
      }
    } catch (const std::exception& ex) {
      XLOG(ERR) << folly::exceptionStr(ex);
      co_return;
    }
    XLOG(INFO) << __func__ << " done";
  }

  void stop() {
    XLOG(INFO) << __func__;
    if (announceHandle_) {
      announceHandle_->unannounce();
    }
    if (moqClient_.moqSession_) {
      moqClient_.moqSession_->close(SessionCloseErrorCode::NO_ERROR);
    }
  }

  void publishLoop() {
    XLOG(INFO) << __func__;
    auto g =
        folly::makeGuard([func = __func__] { XLOG(INFO) << "exit " << func; });
    folly::Executor::KeepAlive keepAlive(moqClient_.getEventBase());
    flv::FlvSequentialReader flvSeqReader(FLAGS_input_flv_file);
    while (moqClient_.moqSession_) {
      auto item = flvSeqReader.getNextItem();
      if (item == nullptr) {
        XLOG(ERR) << "Error reading FLV file";
        break;
      }
      for (auto& sub : subscriptions_) {
        XLOG(DBG1) << "Evaluating to send item: " << item->id
                   << ", type: " << folly::to_underlying(item->type)
                   << ", to subID-TrackAlias: "
                   << sub.second->subscribeOk().subscribeID << "-"
                   << sub.second->trackAlias;

        if (videoPub_ && sub.second->consumer == videoPub_.get()) {
          if (item->data &&
              (item->type == flv::FlvSequentialReader::MediaType::VIDEO ||
               item->isEOF)) {
            // Send audio data in a thread (stream per object). Clone it since
            // we can have multiple subscribers
            auto itemClone = item->clone();
            moqClient_.getEventBase()->runInEventBaseThread(
                [self(this), itemClone(std::move(itemClone))]() mutable {
                  self->publishVideo(std::move(itemClone));
                });
          }
        }
        if (audioPub_ && sub.second->consumer == audioPub_.get()) {
          // Audio
          if (item->data &&
              (item->type == flv::FlvSequentialReader::MediaType::AUDIO ||
               item->isEOF)) {
            // Send audio data in a thread (stream per object). Clone it since
            // we can have multiple subscribers
            auto itemClone = item->clone();
            moqClient_.getEventBase()->runInEventBaseThread(
                [self(this),
                 trackAlias(sub.second->trackAlias),
                 itemClone(std::move(itemClone))]() mutable {
                  self->publishAudio(trackAlias, std::move(itemClone));
                });
          }
        }
      }
      if (item->isEOF) {
        XLOG(INFO) << "FLV file EOF";
        break;
      }
    }
  }

  folly::coro::Task<SubscribeResult> subscribe(
      SubscribeRequest subscribeReq,
      std::shared_ptr<TrackConsumer> consumer) override {
    XLOG(INFO) << "SubscribeRequest track ns="
               << subscribeReq.fullTrackName.trackNamespace
               << " name=" << subscribeReq.fullTrackName.trackName
               << " subscribe id=" << subscribeReq.subscribeID
               << " track alias=" << subscribeReq.trackAlias;
    AbsoluteLocation latest;
    // Location mode not supported
    if (subscribeReq.locType != LocationType::LatestObject) {
      co_return folly::makeUnexpected(SubscribeError{
          subscribeReq.subscribeID,
          SubscribeErrorCode::NOT_SUPPORTED,
          "Only location LatestObject mode supported"});
    }
    // Track not available
    auto consumerPtr = consumer.get();
    if (subscribeReq.fullTrackName == fullVideoTrackName_) {
      latest = latestVideo_;
      videoPub_ = std::move(consumer);
    } else if (subscribeReq.fullTrackName == fullAudioTrackName_) {
      latest = latestAudio_;
      audioPub_ = std::move(consumer);
    } else {
      co_return folly::makeUnexpected(SubscribeError{
          subscribeReq.subscribeID,
          SubscribeErrorCode::TRACK_NOT_EXIST,
          "Full trackname NOT available"});
    }
    // Save subscribe
    auto subscription = std::make_shared<Subscription>(
        SubscribeOk{
            subscribeReq.subscribeID,
            std::chrono::milliseconds(0),
            MoQSession::resolveGroupOrder(
                GroupOrder::OldestFirst, subscribeReq.groupOrder),
            latest,
            {}},
        subscribeReq.trackAlias,
        consumerPtr,
        *this);
    subscriptions_.emplace(subscribeReq.subscribeID, subscription);
    XLOG(INFO) << "Subscribed " << subscribeReq.subscribeID;

    co_return subscription;
  }

  void publishAudio(
      TrackAlias trackAlias,
      std::unique_ptr<flv::FlvSequentialReader::MediaItem> item) {
    if (item->isEOF) {
      XLOG(INFO) << "FLV audio received EOF";
      return;
    }
    auto objPayload = encodeToMoQMi(std::move(item));
    if (!objPayload) {
      XLOG(ERR) << "Failed to encode audio frame";
      return;
    }
    ObjectHeader objHeader = ObjectHeader{
        trackAlias,
        latestAudio_.group++,
        /*subgroup=*/0,
        latestAudio_.object,
        AUDIO_STREAM_PRIORITY,
        ObjectStatus::NORMAL};

    XLOG(DBG1) << "Sending audio frame" << objHeader
               << ", payload size: " << objPayload->computeChainDataLength();
    audioPub_->objectStream(objHeader, std::move(objPayload));
  }

  void publishVideo(std::unique_ptr<flv::FlvSequentialReader::MediaItem> item) {
    if (item->isEOF) {
      XLOG(INFO) << "FLV video received EOF";
      if (videoPub_ && videoSgPub_) {
        videoSgPub_->endOfGroup(latestVideo_.object);
        videoSgPub_.reset();

        latestVideo_.group++;
        latestVideo_.object = 0;
      }
      return;
    }

    if (!item->isIdr && !videoSgPub_) {
      XLOG(INFO) << "Discarding non-IDR frame before subgroup started";
      return;
    }

    auto isIdr = item->isIdr;
    auto objPayload = encodeToMoQMi(std::move(item));
    if (!objPayload) {
      XLOG(ERR) << "Failed to encode video frame";
      return;
    }

    if (isIdr) {
      if (videoSgPub_) {
        // Close previous subgroup
        videoSgPub_->endOfGroup(latestVideo_.object);
        videoSgPub_.reset();
        latestVideo_.group++;
        latestVideo_.object = 0;
      }
      // Open new subgroup
      auto res = videoPub_->beginSubgroup(
          latestVideo_.group, 0, VIDEO_STREAM_PRIORITY);
      if (!res) {
        XLOG(ERR) << "Error creating subgroup";
      }
      videoSgPub_ = std::move(res.value());
    }

    // Send video data
    if (videoSgPub_) {
      XLOG(DBG1) << "Sending video frame. grp-obj: " << latestVideo_.group
                 << "-" << latestVideo_.object
                 << ". Payload size: " << objPayload->computeChainDataLength();
      videoSgPub_->object(latestVideo_.object++, std::move(objPayload));
    } else {
      XLOG(ERR) << "Should not happen";
    }
  }

 private:
  static const uint8_t AUDIO_STREAM_PRIORITY = 100; /* Lower is higher pri */
  static const uint8_t VIDEO_STREAM_PRIORITY = 200;

  static std::unique_ptr<folly::IOBuf> encodeToMoQMi(
      std::unique_ptr<flv::FlvSequentialReader::MediaItem> item) {
    if (item->type == flv::FlvSequentialReader::MediaType::VIDEO) {
      auto dataToEncode = std::make_unique<MoQMi::VideoH264AVCCWCPData>(
          item->id,
          item->pts,
          item->timescale,
          item->duration,
          item->wallclock,
          std::move(item->data),
          std::move(item->metadata),
          item->dts);

      return MoQMi::toObjectPayload(std::move(dataToEncode));

    } else if (item->type == flv::FlvSequentialReader::MediaType::AUDIO) {
      auto dataToEncode = std::make_unique<MoQMi::AudioAACMP4LCWCPData>(
          item->id,
          item->pts,
          item->timescale,
          item->duration,
          item->wallclock,
          std::move(item->data),
          item->sampleFreq,
          item->numChannels);
      return MoQMi::toObjectPayload(std::move(dataToEncode));
    }
    return nullptr;
  }

  MoQClient moqClient_;
  std::shared_ptr<Subscriber::AnnounceHandle> announceHandle_;
  FullTrackName fullVideoTrackName_;
  FullTrackName fullAudioTrackName_;

  AbsoluteLocation latestVideo_{0, 0};
  AbsoluteLocation latestAudio_{0, 0};

  struct Subscription : public Publisher::SubscriptionHandle {
    Subscription(
        SubscribeOk ok,
        TrackAlias alias,
        TrackConsumer* consumerPtr,
        MoQFlvStreamerClient& client)
        : SubscriptionHandle(std::move(ok)),
          trackAlias(alias),
          consumer(consumerPtr),
          client_(client) {}

    const TrackAlias trackAlias;
    const TrackConsumer* consumer{nullptr};

    void subscribeUpdate(SubscribeUpdate) override {}
    void unsubscribe() override {
      auto subscribeID = subscribeOk_->subscribeID;
      XLOG(INFO) << "Unsubscribe id=" << subscribeID;
      // Delete subscribe/this
      client_.subscriptions_.erase(subscribeID);
      XLOG(INFO) << "Unsubscribed id=" << subscribeID;
    }

   private:
    MoQFlvStreamerClient& client_;
  };
  std::map<SubscribeID, std::shared_ptr<Subscription>> subscriptions_;
  std::shared_ptr<TrackConsumer> audioPub_;
  std::shared_ptr<TrackConsumer> videoPub_;
  std::shared_ptr<SubgroupConsumer> videoSgPub_;
};
} // namespace

using namespace moxygen;
namespace {

} // namespace

int main(int argc, char* argv[]) {
  folly::Init init(&argc, &argv, false);
  folly::EventBase eventBase;
  proxygen::URL url(FLAGS_connect_url);
  if (!url.hasHost() || !url.isValid()) {
    XLOGF(ERR, "Invalid url: {}", FLAGS_connect_url);
    return 1;
  }
  if (!std::filesystem::exists(FLAGS_input_flv_file)) {
    XLOGF(ERR, "The input file {} does not exists", FLAGS_input_flv_file);
    return 1;
  }

  XLOGF(
      INFO,
      "Starting publisher that will use: Stream(subGroup) per object for audio and Stream(subGroup) per GOP for video. Input file/pipe: {}",
      FLAGS_input_flv_file);

  TrackNamespace ns =
      TrackNamespace(FLAGS_track_namespace, FLAGS_track_namespace_delimiter);
  auto streamerClient = std::make_shared<MoQFlvStreamerClient>(
      &eventBase,
      std::move(url),
      moxygen::FullTrackName({ns, FLAGS_video_track_name}),
      moxygen::FullTrackName({ns, FLAGS_audio_track_name}));

  class SigHandler : public folly::AsyncSignalHandler {
   public:
    explicit SigHandler(folly::EventBase* evb, std::function<void(int)> fn)
        : folly::AsyncSignalHandler(evb), fn_(std::move(fn)) {
      registerSignalHandler(SIGTERM);
      registerSignalHandler(SIGINT);
    }
    void signalReceived(int signum) noexcept override {
      XLOG(INFO) << __func__ << " signum=" << signum;
      fn_(signum);
      unreg();
    }

    void unreg() {
      unregisterSignalHandler(SIGTERM);
      unregisterSignalHandler(SIGINT);
    }

   private:
    std::function<void(int)> fn_;
  };

  // TODO this does NOT work, we do not get the signal
  SigHandler handler(
      &eventBase, [&streamerClient](int) mutable { streamerClient->stop(); });

  streamerClient->run({{std::move(ns)}, {}})
      .scheduleOn(&eventBase)
      .start()
      .via(&eventBase)
      .thenTry([&handler](auto) { handler.unreg(); });
  if (!eventBase.loop()) {
    return 1;
  }
  return 0;
}

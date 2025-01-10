/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <folly/portability/GFlags.h>
#include "moxygen/MoQClient.h"
#include "moxygen/ObjectReceiver.h"

#include <folly/init/Init.h>
#include <folly/io/async/AsyncSignalHandler.h>
#include <signal.h>
#include "moxygen/flv_parser/FlvWriter.h"
#include "moxygen/moq_mi/MoQMi.h"

DEFINE_string(
    connect_url,
    "https://localhost:4433/moq",
    "URL for webtransport server");
DEFINE_string(
    flv_outpath,
    "",
    "File name to save the received FLV file to (ex: /tmp/test.flv)");
DEFINE_string(track_namespace, "flvstreamer", "Track Namespace");
DEFINE_string(track_namespace_delimiter, "/", "Track Namespace Delimiter");
DEFINE_string(video_track_name, "video0", "Video track Name");
DEFINE_string(audio_track_name, "audio0", "Track Name");
DEFINE_int32(connect_timeout, 1000, "Connect timeout (ms)");
DEFINE_int32(transaction_timeout, 120, "Transaction timeout (s)");
DEFINE_bool(quic_transport, false, "Use raw QUIC transport");
DEFINE_bool(fetch, false, "Use fetch rather than subscribe");

namespace {
using namespace moxygen;

class TrackType {
 public:
  enum MediaType { Audio, Video };

  explicit TrackType(TrackType::MediaType mediaType) : mediaType_(mediaType) {}

  std::string toStr() {
    if (mediaType_ == TrackType::MediaType::Audio) {
      return "audio";
    } else if (mediaType_ == TrackType::MediaType::Video) {
      return "video";
    }
    return "unknown";
  }

 private:
  TrackType::MediaType mediaType_;
};

class FlvWriterShared : flv::FlvWriter {
 public:
  explicit FlvWriterShared(const std::string& flvOutPath)
      : flv::FlvWriter(flvOutPath) {}

  bool writeMoqMiPayload(MoQMi::MoqMiTag moqMiTag) {
    bool ret = false;
    if (moqMiTag.index() == MoQMi::MoqMITagTypeIndex::MOQMI_TAG_INDEX_READCMD) {
      return ret;
    }

    if (moqMiTag.index() ==
        MoQMi::MoqMITagTypeIndex::MOQMI_TAG_INDEX_VIDEO_H264_AVC) {
      auto moqv = std::move(
          std::get<MoQMi::MoqMITagTypeIndex::MOQMI_TAG_INDEX_VIDEO_H264_AVC>(
              moqMiTag));

      uint32_t pts = static_cast<uint32_t>(moqv->pts);
      if (moqv->pts > std::numeric_limits<uint32_t>::max()) {
        XLOG_EVERY_N(WARNING, 1000) << "PTS truncated! Rolling over. From "
                                    << moqv->pts << ", to: " << pts;
      }
      CHECK_GE(moqv->pts, moqv->dts);
      uint32_t compositionTime = moqv->pts - moqv->dts;

      if (moqv->metadata != nullptr && !videoHeaderWritten_) {
        XLOG(INFO) << "Writing video header";
        auto vhtag = flv::createVideoTag(
            moqv->pts, 1, 7, 0, compositionTime, std::move(moqv->metadata));
        ret = writeTag(std::move(vhtag));
        if (!ret) {
          return ret;
        }
        videoHeaderWritten_ = true;
      }
      bool isIdr = moqv->isIdr();
      if (videoHeaderWritten_ && moqv->data != nullptr &&
          moqv->data->computeChainDataLength() > 0) {
        if ((!firstIDRWritten_ && isIdr) || firstIDRWritten_) {
          // Write frame
          uint8_t frameType = isIdr ? 1 : 0;
          XLOG(DBG1) << "Writing video frame, type: " << frameType;
          auto vtag = flv::createVideoTag(
              moqv->pts,
              frameType,
              7,
              1,
              compositionTime,
              std::move(moqv->data));
          ret = writeTag(std::move(vtag));
          if (isIdr && !firstIDRWritten_) {
            firstIDRWritten_ = true;
            XLOG(INFO) << "Wrote first IDR frame";
          }
        }
      }
    } else if (
        moqMiTag.index() ==
        MoQMi::MoqMITagTypeIndex::MOQMI_TAG_INDEX_AUDIO_AAC_LC) {
      auto moqa = std::move(
          std::get<MoQMi::MoqMITagTypeIndex::MOQMI_TAG_INDEX_AUDIO_AAC_LC>(
              moqMiTag));
      if (!audioHeaderWritten_) {
        XLOG(INFO) << "Writing audio header";
        auto ascHeader = moqa->getAscHeader();
        auto ahtag = flv::createAudioTag(
            moqa->pts, 10, 3, 1, 1, 0, std::move(ascHeader));
        ret = writeTag(std::move(ahtag));
        if (!ret) {
          return ret;
        }
        audioHeaderWritten_ = true;
      }
      if (audioHeaderWritten_) {
        XLOG(DBG1) << "Writing audio frame";
        auto atag = flv::createAudioTag(
            moqa->pts, 10, 3, 1, 1, 1, std::move(moqa->data));
        ret = writeTag(std::move(atag));
      }
    }
    return ret;
  }

 private:
  bool writeTag(flv::FlvTag tag) {
    std::lock_guard<std::mutex> g(mutex_);
    return flv::FlvWriter::writeTag(std::move(tag));
  }

  std::mutex mutex_;
  bool videoHeaderWritten_{false};
  bool audioHeaderWritten_{false};
  bool firstIDRWritten_{false};
};

class TrackReceiverHandler : public ObjectReceiverCallback {
 public:
  explicit TrackReceiverHandler(TrackType::MediaType mediaType)
      : trackMediaType_(TrackType(mediaType)) {}
  ~TrackReceiverHandler() override = default;
  FlowControlState onObject(const ObjectHeader&, Payload payload) override {
    // TODO: Add jitter buffer to fix out of order packets, we will need latency
    // parameter to determine how much to buffer we want
    if (payload) {
      auto payloadSize = payload->computeChainDataLength();
      XLOG(DBG1) << trackMediaType_.toStr()
                 << " Received payload. Size=" << payloadSize;

      auto payloadDecodedData = MoQMi::fromObjectPayload(std::move(payload));
      if (payloadDecodedData.index() ==
          MoQMi::MoqMITagTypeIndex::MOQMI_TAG_INDEX_VIDEO_H264_AVC) {
        XLOG(DBG1)
            << trackMediaType_.toStr() << " payloadDecodedData: "
            << *std::get<
                   MoQMi::MoqMITagTypeIndex::MOQMI_TAG_INDEX_VIDEO_H264_AVC>(
                   payloadDecodedData);
      } else if (
          payloadDecodedData.index() ==
          MoQMi::MoqMITagTypeIndex::MOQMI_TAG_INDEX_AUDIO_AAC_LC) {
        XLOG(DBG1)
            << trackMediaType_.toStr() << " payloadDecodedData: "
            << *std::get<
                   MoQMi::MoqMITagTypeIndex::MOQMI_TAG_INDEX_AUDIO_AAC_LC>(
                   payloadDecodedData);
      }
      if (flvw_) {
        if (flvw_->writeMoqMiPayload(std::move(payloadDecodedData))) {
          XLOG(DBG1) << trackMediaType_.toStr() << " Wrote payload to output";
        } else {
          XLOG(WARNING) << trackMediaType_.toStr() << " Payload write failed";
        }
      }
    }
    return FlowControlState::UNBLOCKED;
  }
  void onObjectStatus(const ObjectHeader& objHeader) override {
    std::cout << trackMediaType_.toStr()
              << " ObjectStatus=" << uint32_t(objHeader.status) << std::endl;
  }
  void onEndOfStream() override {}
  void onError(ResetStreamErrorCode error) override {
    std::cout << trackMediaType_.toStr()
              << " Stream Error=" << folly::to_underlying(error) << std::endl;
    ;
  }
  void onSubscribeDone(SubscribeDone) override {
    baton.post();
  }

  folly::coro::Baton baton;

  void setFlvWriterShared(std::shared_ptr<FlvWriterShared> flvw) {
    flvw_ = flvw;
  }

 private:
  TrackType trackMediaType_;
  std::shared_ptr<FlvWriterShared> flvw_;
};

class MoQFlvReceiverClient {
 public:
  MoQFlvReceiverClient(
      folly::EventBase* evb,
      proxygen::URL url,
      bool useQuic,
      const std::string& flvOutPath)
      : moqClient_(
            evb,
            std::move(url),
            (useQuic ? MoQClient::TransportType::QUIC
                     : MoQClient::TransportType::H3_WEBTRANSPORT)),
        flvOutPath_(flvOutPath) {}

  folly::coro::Task<void> run(
      SubscribeRequest subAudio,
      SubscribeRequest subVideo) noexcept {
    XLOG(INFO) << __func__;
    auto g =
        folly::makeGuard([func = __func__] { XLOG(INFO) << "exit " << func; });
    try {
      co_await moqClient_.setupMoQSession(
          std::chrono::milliseconds(FLAGS_connect_timeout),
          std::chrono::seconds(FLAGS_transaction_timeout),
          Role::SUBSCRIBER);
      auto exec = co_await folly::coro::co_current_executor;
      controlReadLoop().scheduleOn(exec).start();

      // Create output file
      flvw_ = std::make_shared<FlvWriterShared>(flvOutPath_);
      trackReceiverHandlerAudio_.setFlvWriterShared(flvw_);
      trackReceiverHandlerVideo_.setFlvWriterShared(flvw_);

      // Subscribe to audio
      subRxHandlerAudio_ = std::make_shared<ObjectReceiver>(
          ObjectReceiver::SUBSCRIBE, &trackReceiverHandlerAudio_);
      auto trackAudio = co_await moqClient_.moqSession_->subscribe(
          subAudio, subRxHandlerAudio_);
      if (trackAudio.hasValue()) {
        subscribeIDAudio_ = trackAudio->subscribeID;
        XLOG(DBG1) << "Audio subscribeID=" << subscribeIDAudio_;
        auto latest = trackAudio->latest;
        if (latest) {
          XLOG(INFO) << "Audio Latest={" << latest->group << ", "
                     << latest->object << "}";
        }
      } else {
        XLOG(WARNING) << "Audio SubscribeError id="
                      << trackAudio.error().subscribeID
                      << " code=" << trackAudio.error().errorCode
                      << " reason=" << trackAudio.error().reasonPhrase;
      }

      // Subscribe to video
      subRxHandlerVideo_ = std::make_shared<ObjectReceiver>(
          ObjectReceiver::SUBSCRIBE, &trackReceiverHandlerVideo_);
      auto trackVideo = co_await moqClient_.moqSession_->subscribe(
          subVideo, subRxHandlerVideo_);
      if (trackVideo.hasValue()) {
        subscribeIDVideo_ = trackVideo->subscribeID;
        XLOG(DBG1) << "Video subscribeID=" << subscribeIDVideo_;
        auto latest = trackVideo->latest;
        if (latest) {
          XLOG(INFO) << "Video Latest={" << latest->group << ", "
                     << latest->object << "}";
        }
      } else {
        XLOG(WARNING) << "Video SubscribeError id="
                      << trackVideo.error().subscribeID
                      << " code=" << trackVideo.error().errorCode
                      << " reason=" << trackVideo.error().reasonPhrase;
      }
      moqClient_.moqSession_->drain();
    } catch (const std::exception& ex) {
      XLOG(ERR) << ex.what();
      co_return;
    }
    co_await trackReceiverHandlerAudio_.baton;
    XLOG(INFO) << __func__ << " done";
  }

  void stop() {
    moqClient_.moqSession_->unsubscribe({subscribeIDAudio_});
    moqClient_.moqSession_->unsubscribe({subscribeIDVideo_});
    moqClient_.moqSession_->close(SessionCloseErrorCode::NO_ERROR);
  }

  folly::coro::Task<void> controlReadLoop() {
    class ControlVisitor : public MoQSession::ControlVisitor {
     public:
      explicit ControlVisitor(MoQFlvReceiverClient& client) : client_(client) {}

      void operator()(Announce announce) const override {
        XLOG(WARN) << "Announce ns=" << announce.trackNamespace;
        // text client doesn't expect server or relay to announce anything,
        // but announce OK anyways
        client_.moqClient_.moqSession_->announceOk({announce.trackNamespace});
      }

      void operator()(SubscribeRequest subscribeReq) const override {
        XLOG(INFO) << "SubscribeRequest";
        client_.moqClient_.moqSession_->subscribeError(
            {subscribeReq.subscribeID, 404, "don't care"});
      }

      void operator()(Goaway) const override {
        XLOG(INFO) << "Goaway";
        client_.moqClient_.moqSession_->unsubscribe(
            {client_.subscribeIDAudio_});
        client_.moqClient_.moqSession_->unsubscribe(
            {client_.subscribeIDVideo_});
      }

     private:
      MoQFlvReceiverClient& client_;
    };
    XLOG(INFO) << __func__;
    auto g =
        folly::makeGuard([func = __func__] { XLOG(INFO) << "exit " << func; });
    ControlVisitor visitor(*this);
    MoQSession::ControlVisitor* vptr(&visitor);
    while (auto msg =
               co_await moqClient_.moqSession_->controlMessages().next()) {
      boost::apply_visitor(*vptr, msg.value());
    }
  }

 private:
  MoQClient moqClient_;
  SubscribeID subscribeIDAudio_{0};
  SubscribeID subscribeIDVideo_{0};
  std::string flvOutPath_;
  std::shared_ptr<FlvWriterShared> flvw_;
  TrackReceiverHandler trackReceiverHandlerAudio_ =
      TrackReceiverHandler(TrackType::MediaType::Audio);
  std::shared_ptr<ObjectReceiver> subRxHandlerAudio_;
  TrackReceiverHandler trackReceiverHandlerVideo_ =
      TrackReceiverHandler(TrackType::MediaType::Video);
  std::shared_ptr<ObjectReceiver> subRxHandlerVideo_;
};
} // namespace

using namespace moxygen;

int main(int argc, char* argv[]) {
  folly::Init init(&argc, &argv, false);
  folly::EventBase eventBase;
  proxygen::URL url(FLAGS_connect_url);
  if (!url.isValid() || !url.hasHost()) {
    XLOG(ERR) << "Invalid url: " << FLAGS_connect_url;
  }

  XLOGF(INFO, "Starting consumer from URL: {}", FLAGS_connect_url);

  MoQFlvReceiverClient flvReceiverClient(
      &eventBase, std::move(url), FLAGS_quic_transport, FLAGS_flv_outpath);

  class SigHandler : public folly::AsyncSignalHandler {
   public:
    explicit SigHandler(folly::EventBase* evb, std::function<void(int)> fn)
        : folly::AsyncSignalHandler(evb), fn_(std::move(fn)) {
      registerSignalHandler(SIGTERM);
      registerSignalHandler(SIGINT);
    }
    void signalReceived(int signum) noexcept override {
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
  SigHandler handler(&eventBase, [&flvReceiverClient](int) mutable {
    flvReceiverClient.stop();
  });
  const auto subscribeIDAudio = 0;
  const auto trackAliasAudio = 1;
  const auto subscribeIDVideo = 1;
  const auto trackAliasVideo = 2;
  flvReceiverClient
      .run(
          {subscribeIDAudio,
           trackAliasAudio,
           moxygen::FullTrackName(
               {{TrackNamespace(
                    FLAGS_track_namespace, FLAGS_track_namespace_delimiter)},
                FLAGS_audio_track_name}),
           0,
           GroupOrder::OldestFirst,
           LocationType::LatestObject,
           folly::none,
           folly::none,
           {}},
          {subscribeIDVideo,
           trackAliasVideo,
           moxygen::FullTrackName(
               {TrackNamespace(
                    FLAGS_track_namespace, FLAGS_track_namespace_delimiter),
                FLAGS_video_track_name}),
           0,
           GroupOrder::OldestFirst,
           LocationType::LatestObject,
           folly::none,
           folly::none,
           {}})
      .scheduleOn(&eventBase)
      .start()
      .via(&eventBase)
      .thenTry([&handler](auto) { handler.unreg(); });
  eventBase.loop();
}

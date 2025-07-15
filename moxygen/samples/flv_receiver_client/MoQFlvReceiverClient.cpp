/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <folly/portability/GFlags.h>
#include "moxygen/MoQClient.h"
#include "moxygen/MoQWebTransportClient.h"
#include "moxygen/ObjectReceiver.h"

#include <folly/init/Init.h>
#include <folly/io/async/AsyncSignalHandler.h>
#include <signal.h>
#include "moxygen/dejitter/DeJitter.h"
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
DEFINE_int32(
    dejitter_buffer_size_ms,
    300,
    "Dejitter buffer size in ms (this translates to added latency)");
DEFINE_bool(quic_transport, false, "Use raw QUIC transport");
DEFINE_bool(fetch, false, "Use fetch rather than subscribe");
DEFINE_string(auth, "secret", "MOQ subscription auth string");
DEFINE_bool(v11Plus, true, "Negotiate versions 11 or higher");

namespace {
using namespace moxygen;

class TrackType {
 public:
  enum MediaType { Audio, Video };

  explicit TrackType(TrackType::MediaType mediaType) : mediaType_(mediaType) {}

  std::string toStr() const {
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

  bool writeMoqMiPayload(MoQMi::MoqMiItem moqMiItem) {
    bool ret = false;
    if (moqMiItem.index() ==
        MoQMi::MoqMIItemTypeIndex::MOQMI_ITEM_INDEX_READCMD) {
      return ret;
    }

    if (moqMiItem.index() ==
        MoQMi::MoqMIItemTypeIndex::MOQMI_ITEM_INDEX_VIDEO_H264_AVC) {
      auto moqv = std::move(
          std::get<MoQMi::MoqMIItemTypeIndex::MOQMI_ITEM_INDEX_VIDEO_H264_AVC>(
              moqMiItem));

      // Since PTS >= DTS lets check 1st PTS for rollover
      uint32_t pts =
          static_cast<int32_t>(moqv->pts & 0x7FFFFFFF); // 31 lower bits
      uint32_t dts =
          static_cast<int32_t>(moqv->dts & 0x7FFFFFFF); // 31 lower bits
      // This already handles the case where PTS rolled over and DTS NOT yet
      int32_t compositionTime = pts - dts;
      if (moqv->pts > std::numeric_limits<int32_t>::max()) {
        XLOG_EVERY_N(WARNING, 1000)
            << "PTS video truncated! Rolling over. From " << moqv->pts
            << ", to: " << pts << ", compositionTime: " << compositionTime;
      }
      CHECK_GE(compositionTime, 0);

      if (moqv->metadata != nullptr && !videoHeaderWritten_) {
        XLOG(INFO) << "Writing video header";
        auto vhtag = flv::createVideoTag(
            pts, 1, 7, 0, compositionTime, std::move(moqv->metadata));
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
        moqMiItem.index() ==
        MoQMi::MoqMIItemTypeIndex::MOQMI_ITEM_INDEX_AUDIO_AAC_LC) {
      auto moqa = std::move(
          std::get<MoQMi::MoqMIItemTypeIndex::MOQMI_ITEM_INDEX_AUDIO_AAC_LC>(
              moqMiItem));
      if (!audioHeaderWritten_) {
        XLOG(INFO) << "Writing audio header";
        auto ascHeader = moqa->getAscHeader();
        uint32_t pts =
            static_cast<int32_t>(moqa->pts & 0x7FFFFFFF); // 31 lower bits
        if (moqa->pts > std::numeric_limits<int32_t>::max()) {
          XLOG_EVERY_N(WARNING, 1000)
              << "PTS audio truncated! Rolling over. From " << moqa->pts
              << ", to: " << pts;
        }
        auto ahtag =
            flv::createAudioTag(pts, 10, 3, 1, 1, 0, std::move(ascHeader));
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
  explicit TrackReceiverHandler(
      TrackType::MediaType mediaType,
      uint32_t dejitterBufferSizeMs)
      : trackMediaType_(TrackType(mediaType)),
        dejitterBufferSizeMs_(dejitterBufferSizeMs) {}
  ~TrackReceiverHandler() override = default;
  FlowControlState onObject(const ObjectHeader& objHeader, Payload payload)
      override {
    if (payload) {
      std::tuple<
          folly::Optional<MoQMi::MoqMiItem>,
          dejitter::DeJitter<MoQMi::MoqMiItem>::GapInfo>
          deJitterData;

      auto payloadSize = payload->computeChainDataLength();
      XLOG(DBG1) << trackMediaType_.toStr()
                 << " Received payload. Size=" << payloadSize;

      auto payloadDecodedData =
          MoQMi::decodeMoQMi(std::make_unique<MoQMi::MoqMiObject>(
              objHeader.extensions, std::move(payload)));
      logData(payloadDecodedData);
      if (payloadDecodedData.index() ==
              MoQMi::MoqMIItemTypeIndex::MOQMI_ITEM_INDEX_VIDEO_H264_AVC ||
          payloadDecodedData.index() ==
              MoQMi::MoqMIItemTypeIndex::MOQMI_ITEM_INDEX_AUDIO_AAC_LC) {
        // Create deJitter if not already created
        if (!deJitter_) {
          deJitter_ = std::make_unique<dejitter::DeJitter<MoQMi::MoqMiItem>>(
              dejitterBufferSizeMs_);
        }

        // Dejitter frames
        auto seqId = getSeqId(payloadDecodedData);
        if (!seqId) {
          XLOG(ERR) << trackMediaType_.toStr()
                    << " No seqId found skipping frame";
        } else {
          auto sDurMs = getDurationMs(payloadDecodedData);
          if (!sDurMs.has_value()) {
            XLOG(WARN)
                << trackMediaType_.toStr()
                << " No duration found, this affects dejitter buffer size, assuming 0ms";
          }
          deJitterData = deJitter_->insertItem(
              seqId.value(), sDurMs.value_or(0), std::move(payloadDecodedData));
          if (std::get<1>(deJitterData).gapType ==
              dejitter::DeJitter<MoQMi::MoqMiItem>::GapType::FILLING_BUFFER) {
            XLOG(DBG1) << trackMediaType_.toStr()
                       << " Filling buffer for seqId: " << seqId.value();
          } else if (
              std::get<1>(deJitterData).gapType ==
              dejitter::DeJitter<MoQMi::MoqMiItem>::GapType::ARRIVED_LATE) {
            XLOG(WARN) << trackMediaType_.toStr()
                       << " Dropped, because arrived late. seqId: "
                       << seqId.value();
          } else if (
              std::get<1>(deJitterData).gapType ==
              dejitter::DeJitter<MoQMi::MoqMiItem>::GapType::GAP) {
            XLOG(WARN) << trackMediaType_.toStr()
                       << " GAP PASSED to decoder, size: "
                       << std::get<1>(deJitterData).gapSize
                       << ", seqId: " << seqId.value();
          } else if (
              std::get<1>(deJitterData).gapType ==
              dejitter::DeJitter<MoQMi::MoqMiItem>::GapType::INTERNAL_ERROR) {
            XLOG(ERR) << trackMediaType_.toStr()
                      << " INTERNAL ERROR dejittering, seqId: "
                      << seqId.value();
          } else {
            XLOG_EVERY_N(INFO, 60)
                << trackMediaType_.toStr() << " For seqId: " << seqId.value()
                << ", Dejitter size: " << deJitter_->size() << "("
                << deJitter_->sizeMs() << "ms)";
          }
        }
      }

      if (flvw_ && std::get<0>(deJitterData).has_value()) {
        if (flvw_->writeMoqMiPayload(
                std::move(std::get<0>(deJitterData).value()))) {
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
  void logData(const MoQMi::MoqMiItem& payloadDecodedData) const {
    if (payloadDecodedData.index() ==
        MoQMi::MoqMIItemTypeIndex::MOQMI_ITEM_INDEX_VIDEO_H264_AVC) {
      XLOG(DBG1)
          << trackMediaType_.toStr() << " payloadDecodedData: "
          << *std::get<
                 MoQMi::MoqMIItemTypeIndex::MOQMI_ITEM_INDEX_VIDEO_H264_AVC>(
                 payloadDecodedData);
    } else if (
        payloadDecodedData.index() ==
        MoQMi::MoqMIItemTypeIndex::MOQMI_ITEM_INDEX_AUDIO_AAC_LC) {
      XLOG(DBG1)
          << trackMediaType_.toStr() << " payloadDecodedData: "
          << *std::get<
                 MoQMi::MoqMIItemTypeIndex::MOQMI_ITEM_INDEX_AUDIO_AAC_LC>(
                 payloadDecodedData);
    }
  }

  folly::Optional<uint64_t> getSeqId(
      const MoQMi::MoqMiItem& payloadDecodedData) const {
    if (payloadDecodedData.index() ==
        MoQMi::MoqMIItemTypeIndex::MOQMI_ITEM_INDEX_VIDEO_H264_AVC) {
      return std::get<
                 MoQMi::MoqMIItemTypeIndex::MOQMI_ITEM_INDEX_VIDEO_H264_AVC>(
                 payloadDecodedData)
          ->seqId;
    } else if (
        payloadDecodedData.index() ==
        MoQMi::MoqMIItemTypeIndex::MOQMI_ITEM_INDEX_AUDIO_AAC_LC) {
      return std::get<MoQMi::MoqMIItemTypeIndex::MOQMI_ITEM_INDEX_AUDIO_AAC_LC>(
                 payloadDecodedData)
          ->seqId;
    }
    return folly::none;
  }

  folly::Optional<uint64_t> getDurationMs(
      const MoQMi::MoqMiItem& payloadDecodedData) const {
    folly::Optional<uint64_t> dur;
    folly::Optional<uint64_t> timeScale;

    if (payloadDecodedData.index() ==
        MoQMi::MoqMIItemTypeIndex::MOQMI_ITEM_INDEX_VIDEO_H264_AVC) {
      dur =
          std::get<MoQMi::MoqMIItemTypeIndex::MOQMI_ITEM_INDEX_VIDEO_H264_AVC>(
              payloadDecodedData)
              ->duration;
      timeScale =
          std::get<MoQMi::MoqMIItemTypeIndex::MOQMI_ITEM_INDEX_VIDEO_H264_AVC>(
              payloadDecodedData)
              ->timescale;
    } else if (
        payloadDecodedData.index() ==
        MoQMi::MoqMIItemTypeIndex::MOQMI_ITEM_INDEX_AUDIO_AAC_LC) {
      dur = std::get<MoQMi::MoqMIItemTypeIndex::MOQMI_ITEM_INDEX_AUDIO_AAC_LC>(
                payloadDecodedData)
                ->duration;
      timeScale =
          std::get<MoQMi::MoqMIItemTypeIndex::MOQMI_ITEM_INDEX_AUDIO_AAC_LC>(
              payloadDecodedData)
              ->timescale;
    }
    if (!dur.has_value() || !timeScale.has_value()) {
      return folly::none;
    }
    return dur.value() * 1000 / timeScale.value();
  }

  std::shared_ptr<FlvWriterShared> flvw_;
  TrackType trackMediaType_;
  std::unique_ptr<dejitter::DeJitter<MoQMi::MoqMiItem>> deJitter_;
  uint32_t dejitterBufferSizeMs_;
};

class MoQFlvReceiverClient
    : public Subscriber,
      public std::enable_shared_from_this<MoQFlvReceiverClient> {
 public:
  MoQFlvReceiverClient(
      folly::EventBase* evb,
      proxygen::URL url,
      bool useQuic,
      const std::string& flvOutPath)
      : moqClient_(makeMoQClient(evb, std::move(url), useQuic)),
        flvOutPath_(flvOutPath) {}

  folly::coro::Task<void> run() noexcept {
    XLOG(INFO) << __func__;
    auto g =
        folly::makeGuard([func = __func__] { XLOG(INFO) << "exit " << func; });
    try {
      co_await moqClient_->setupMoQSession(
          std::chrono::milliseconds(FLAGS_connect_timeout),
          std::chrono::seconds(FLAGS_transaction_timeout),
          /*publishHandler=*/nullptr,
          /*subscribeHandler=*/shared_from_this(),
          FLAGS_v11Plus);
      // Create output file
      flvw_ = std::make_shared<FlvWriterShared>(flvOutPath_);
      trackReceiverHandlerAudio_->setFlvWriterShared(flvw_);
      trackReceiverHandlerVideo_->setFlvWriterShared(flvw_);

      uint64_t negotiatedVersion =
          *(moqClient_->moqSession_->getNegotiatedVersion());

      auto subAudio = SubscribeRequest::make(
          moxygen::FullTrackName(
              {{TrackNamespace(
                   FLAGS_track_namespace, FLAGS_track_namespace_delimiter)},
               FLAGS_audio_track_name}),
          0,
          GroupOrder::OldestFirst,
          true,
          LocationType::LargestObject,
          folly::none,
          0,
          {getAuthParam(negotiatedVersion, FLAGS_auth)});
      auto subVideo = SubscribeRequest::make(
          moxygen::FullTrackName(
              {TrackNamespace(
                   FLAGS_track_namespace, FLAGS_track_namespace_delimiter),
               FLAGS_video_track_name}),
          0,
          GroupOrder::OldestFirst,
          true,
          LocationType::LargestObject,
          folly::none,
          0,
          {getAuthParam(negotiatedVersion, FLAGS_auth)});

      // Subscribe to audio
      subRxHandlerAudio_ = std::make_shared<ObjectReceiver>(
          ObjectReceiver::SUBSCRIBE, trackReceiverHandlerAudio_);
      auto trackAudio = co_await moqClient_->moqSession_->subscribe(
          subAudio, subRxHandlerAudio_);
      if (trackAudio.hasValue()) {
        audioSubscribeHandle_ = std::move(trackAudio.value());
        XLOG(DBG1) << "Audio requestID="
                   << audioSubscribeHandle_->subscribeOk().requestID;
        auto largest = audioSubscribeHandle_->subscribeOk().largest;
        if (largest) {
          XLOG(INFO) << "Audio Largest={" << largest->group << ", "
                     << largest->object << "}";
        }
      } else {
        XLOG(WARNING) << "Audio SubscribeError id="
                      << trackAudio.error().requestID << " code="
                      << folly::to_underlying(trackAudio.error().errorCode)
                      << " reason=" << trackAudio.error().reasonPhrase;
      }

      // Subscribe to video
      subRxHandlerVideo_ = std::make_shared<ObjectReceiver>(
          ObjectReceiver::SUBSCRIBE, trackReceiverHandlerVideo_);
      auto trackVideo = co_await moqClient_->moqSession_->subscribe(
          subVideo, subRxHandlerVideo_);
      if (trackVideo.hasValue()) {
        videoSubscribeHandle_ = std::move(trackVideo.value());
        XLOG(DBG1) << "Video requestID="
                   << videoSubscribeHandle_->subscribeOk().requestID;
        auto largest = videoSubscribeHandle_->subscribeOk().largest;
        if (largest) {
          XLOG(INFO) << "Video Largest={" << largest->group << ", "
                     << largest->object << "}";
        }
      } else {
        XLOG(WARNING) << "Video SubscribeError id="
                      << trackVideo.error().requestID << " code="
                      << folly::to_underlying(trackVideo.error().errorCode)
                      << " reason=" << trackVideo.error().reasonPhrase;
      }

      moqClient_->moqSession_->drain();
    } catch (const std::exception& ex) {
      XLOG(ERR) << ex.what();
      co_return;
    }
    // TODO: should we co_await collectAll(trackReceiverHandlerAudio_.baton,
    // trackReceiverHandlerVideo_.baton);
    co_await trackReceiverHandlerAudio_->baton;
    co_await trackReceiverHandlerVideo_->baton;
    XLOG(INFO) << __func__ << " done";
  }

  folly::coro::Task<AnnounceResult> announce(
      Announce announce,
      std::shared_ptr<AnnounceCallback>) override {
    XLOG(INFO) << "Announce ns=" << announce.trackNamespace;
    // receiver client doesn't expect server or relay to announce anything, but
    // announce OK anyways
    return folly::coro::makeTask<AnnounceResult>(
        std::make_shared<AnnounceHandle>(
            AnnounceOk{announce.requestID, announce.trackNamespace}));
  }

  void goaway(Goaway goaway) override {
    XLOG(INFO) << "Goaway uri=" << goaway.newSessionUri;
    stop();
  }

  void stop() {
    if (audioSubscribeHandle_) {
      audioSubscribeHandle_->unsubscribe();
      audioSubscribeHandle_.reset();
    }
    if (videoSubscribeHandle_) {
      videoSubscribeHandle_->unsubscribe();
      videoSubscribeHandle_.reset();
    }
    moqClient_->moqSession_->close(SessionCloseErrorCode::NO_ERROR);
  }

 private:
  std::unique_ptr<MoQClient> moqClient_;
  std::shared_ptr<Publisher::SubscriptionHandle> audioSubscribeHandle_;
  std::shared_ptr<Publisher::SubscriptionHandle> videoSubscribeHandle_;
  std::string flvOutPath_;
  std::shared_ptr<FlvWriterShared> flvw_;
  std::shared_ptr<TrackReceiverHandler> trackReceiverHandlerAudio_ =
      std::make_shared<TrackReceiverHandler>(
          TrackType::MediaType::Audio,
          FLAGS_dejitter_buffer_size_ms);
  std::shared_ptr<ObjectReceiver> subRxHandlerAudio_;
  std::shared_ptr<TrackReceiverHandler> trackReceiverHandlerVideo_ =
      std::make_shared<TrackReceiverHandler>(
          TrackType::MediaType::Video,
          FLAGS_dejitter_buffer_size_ms);
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

  auto flvReceiverClient = std::make_shared<MoQFlvReceiverClient>(
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
    flvReceiverClient->stop();
  });

  flvReceiverClient->run()
      .scheduleOn(&eventBase)
      .start()
      .via(&eventBase)
      .thenTry([&handler](auto) { handler.unreg(); });
  eventBase.loop();
}

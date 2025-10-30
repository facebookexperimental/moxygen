/*
 *  Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 *  This source code is licensed under the MIT license found in the LICENSE
 *  file in the root directory of this source tree.
 *
 */

// MoQVideoPublisher.cpp

#include <folly/coro/BlockingWait.h>
#include <folly/io/Cursor.h>
#include <folly/io/IOBuf.h>
#include <proxygen/lib/utils/URL.h>
#include <moxygen/moq_mi/MoQMi.h>
#include <moxygen/relay/MoQRelayClient.h>
#include <moxygen/samples/hack/MoQVideoPublisher.h>

constexpr std::chrono::milliseconds kConnectTimeout = std::chrono::seconds(5);
constexpr std::chrono::seconds kTransactionTimeout = std::chrono::seconds(60);

namespace {
uint64_t currentTimeMilliseconds() {
  auto now = std::chrono::system_clock::now();
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             now.time_since_epoch())
      .count();
}

enum class BufferFlags {
  KEY_FRAME = 0x1,
  CODEC_CONFIG = 0x2,
  END_OF_STREAM = 0x4,
  PARTIAL_FRAME = 0x8,
  DECODE_ONLY = 0x20,
};

std::pair<std::vector<folly::IOBuf>, std::vector<folly::IOBuf>> parseSPSandPPS(
    const folly::IOBuf& buffer) {
  std::vector<folly::IOBuf> spsNalus;
  std::vector<folly::IOBuf> ppsNalus;

  folly::io::Cursor cursor(&buffer);
  while (cursor.canAdvance(4)) {
    uint32_t naluLength = cursor.readBE<uint32_t>();
    if (!cursor.canAdvance(naluLength)) {
      XLOG(ERR) << "Buffer underflow: expected " << naluLength << " bytes";
      break;
    }

    auto br = cursor.peekBytes();
    if (br.empty()) {
      break;
    }

    // First byte contains the NAL unit type in its lower 5 bits
    uint8_t naluType = br[0] & 0x1F;

    auto naluBuf = std::make_unique<folly::IOBuf>(
        folly::IOBuf::COPY_BUFFER, br.subpiece(0, naluLength));

    if (naluType == 7) { // SPS
      spsNalus.emplace_back(std::move(*naluBuf));
    } else if (naluType == 8) { // PPS
      ppsNalus.emplace_back(std::move(*naluBuf));
    }

    cursor.skip(naluLength);
  }
  return {std::move(spsNalus), std::move(ppsNalus)};
}

std::unique_ptr<folly::IOBuf> serializeAVCDecoderConfigurationRecord(
    const std::vector<folly::IOBuf>& spsNalus,
    const std::vector<folly::IOBuf>& ppsNalus) {
  if (spsNalus.empty()) {
    XLOG(ERR) << "No SPS NALUs provided";
    return nullptr;
  }
  if (ppsNalus.empty()) {
    XLOG(ERR) << "No PPS NALUs provided";
    return nullptr;
  }
  if (spsNalus.size() > 31) {
    XLOG(ERR) << "Too many SPS NALUs: " << spsNalus.size();
    return nullptr;
  }
  if (ppsNalus.size() > 255) {
    XLOG(ERR) << "Too many PPS NALUs: " << ppsNalus.size();
    return nullptr;
  }

  auto configRecord = std::make_unique<folly::IOBuf>();
  folly::io::Appender appender(configRecord.get(), 1024);

  // Configuration record header (ISO/IEC 14496-15 AVC file format)
  appender.writeBE<uint8_t>(1); // configurationVersion_

  // Get profile/level from first SPS
  folly::io::Cursor spsCursor(spsNalus.data());
  spsCursor.skip(1); // Skip NAL header
  uint8_t AVCProfileIndication = spsCursor.read<uint8_t>();
  uint8_t profile_compatibility = spsCursor.read<uint8_t>();
  uint8_t AVCLevelIndication = spsCursor.read<uint8_t>();

  appender.writeBE<uint8_t>(AVCProfileIndication);
  appender.writeBE<uint8_t>(profile_compatibility);
  appender.writeBE<uint8_t>(AVCLevelIndication);

  // 6 bits reserved (111111 = 0xFC) + 2 bits NAL length size - 1
  const uint8_t lengthSizeMinusOne = 3; // Using 4 byte NAL length size
  appender.writeBE<uint8_t>(0xFC | lengthSizeMinusOne);

  // 3 bits reserved (111 = 0xE0) + 5 bits number of SPS NALUs
  appender.writeBE<uint8_t>(0xE0 | (spsNalus.size() & 0x1F));

  // Write SPS NALUs
  for (const auto& sps : spsNalus) {
    appender.writeBE<uint16_t>(sps.computeChainDataLength());
    appender.push(sps.data(), sps.computeChainDataLength());
  }

  // Write PPS count and NALUs
  appender.writeBE<uint8_t>(ppsNalus.size());
  for (const auto& pps : ppsNalus) {
    appender.writeBE<uint16_t>(pps.computeChainDataLength());
    appender.push(pps.data(), pps.computeChainDataLength());
  }

  // Add extended data for high profiles
  if (AVCProfileIndication != 66 && // Baseline
      AVCProfileIndication != 77 && // Main
      AVCProfileIndication != 88) { // Extended
    // 6 bits reserved (111111) + 2 bits chroma format (typically 1 = 4:2:0)
    appender.writeBE<uint8_t>(0xFC | 1);
    // 5 bits reserved (11111) + 3 bits bit depth luma minus 8 (typically 0)
    appender.writeBE<uint8_t>(0xF8);
    // 5 bits reserved (11111) + 3 bits bit depth chroma minus 8 (typically 0)
    appender.writeBE<uint8_t>(0xF8);
    // Number of SPS Ext NALUs (typically 0)
    appender.writeBE<uint8_t>(0);
  }

  return configRecord;
}

// Mirror note: changes under fbcode/ are mirrored to xplat/ on amend/commit.

// RTT plumbing: expose current SRTT/LRTT for UI polling

// Returns {srtt_us, lrtt_us}; queries session transport info on EB thread

std::unique_ptr<folly::IOBuf> convertMetadata(
    std::unique_ptr<folly::IOBuf> metadata) {
  auto [spsNalus, ppsNalus] = parseSPSandPPS(*metadata);
  return serializeAVCDecoderConfigurationRecord(spsNalus, ppsNalus);
}

} // namespace

namespace moxygen {
class LocalSubscriptionHandle : public SubscriptionHandle {
 public:
  explicit LocalSubscriptionHandle(RequestID rid, TrackAlias alias) {
    SubscribeOk ok{/*requestID=*/rid,
                   /*trackAlias=*/alias,
                   /*expires=*/std::chrono::milliseconds(0),
                   /*groupOrder=*/GroupOrder::OldestFirst,
                   /*largest=*/folly::none,
                   /*params=*/{}};
    setSubscribeOk(std::move(ok));
  }
  void unsubscribe() override {}
  void subscribeUpdate(SubscribeUpdate) override {}
};

const uint8_t AUDIO_STREAM_PRIORITY = 100; /* Lower is higher pri */
const uint8_t VIDEO_STREAM_PRIORITY = 200;

std::pair<uint64_t, uint64_t> MoQVideoPublisher::getRttMicros() {
  uint64_t srtt = 0;
  uint64_t lrtt = 0;
  auto* evb = evbThread_ ? evbThread_->getEventBase() : nullptr;
  if (!evb || !relayClient_) {
    return {srtt, lrtt};
  }
  // Query on EB thread to avoid races
  evb->runInEventBaseThreadAndWait([&]() {
    if (auto session = relayClient_->getSession()) {
      auto ti = session->getTransportInfo();
      srtt = static_cast<uint64_t>(ti.srtt.count());
      lrtt = static_cast<uint64_t>(ti.lrtt.count());
    }
  });
  return {srtt, lrtt};
}

void MoQVideoPublisher::noteClientAudioSendTs(uint64_t ptsUs, uint64_t t0Us) {
  std::lock_guard<std::mutex> g(t0Mutex_);
  // Keep map bounded: prune if grows too large
  if (t0ByPts_.size() > 1024) {
    t0ByPts_.clear();
  }
  t0ByPts_[ptsUs] = t0Us;
}

// Implementation of setup function
bool MoQVideoPublisher::setup(
    const std::string& connectURL,
    std::shared_ptr<Subscriber> subscriber,
    bool useLegacySetup,
    std::shared_ptr<fizz::CertificateVerifier> verifier) {
  proxygen::URL url(connectURL);
  if (!url.isValid() || !url.hasHost()) {
    XLOG(ERR) << "Invalid url: " << connectURL;
    return false;
  }
  relayClient_ = std::make_unique<MoQRelayClient>(
      std::make_unique<MoQClient>(moqExecutor_, url, std::move(verifier)));

  std::vector<std::string> alpns;
  if (useLegacySetup) {
    alpns = {std::string(kAlpnMoqtLegacy)};
  } else {
    alpns = {std::string(kAlpnMoqtDraft15), std::string(kAlpnMoqtLegacy)};
  }

  cancel_ = folly::CancellationSource();
  running_ = true;
  folly::coro::blockingWait(co_withExecutor(
                                evbThread_->getEventBase(),
                                relayClient_->setup(
                                    /*publisher=*/shared_from_this(),
                                    /*subscriber=*/subscriber,
                                    kConnectTimeout,
                                    kTransactionTimeout,
                                    quic::TransportSettings(),
                                    alpns))
                                .start());
  {
    auto* evb = evbThread_->getEventBase();
    relayStarted_.store(true, std::memory_order_relaxed);
    std::weak_ptr<MoQVideoPublisher> selfWeak = shared_from_this();
    auto selfPub =
        shared_from_this(); // publish callbacks may need publisher reference
    auto ns = videoForwarder_.fullTrackName().trackNamespace;
    co_withExecutor(
        evb,
        folly::coro::co_invoke(
            [selfWeak, selfPub, ns]() -> folly::coro::Task<void> {
              auto selfOwner = selfWeak.lock();
              if (!selfOwner) {
                co_return;
              }
              auto relay = selfOwner->relayClient_.get();
              if (!relay) {
                selfOwner->runDone_.post();
                co_return;
              }
              try {
                co_await folly::coro::co_withCancellation(
                    selfOwner->cancel_.getToken(), relay->run(selfPub, {ns}));
              } catch (const folly::OperationCancelled&) {
                XLOG(DBG1) << "relay->run cancelled";
              }
              if (selfOwner) {
                selfOwner->runDone_.post();
              }
              co_return;
            }))
        .start();
  }
  // Initiate PUBLISH for audio track on the EventBase thread so server can
  // receive data without requiring a downstream SUBSCRIBE
  if (auto session = relayClient_->getSession()) {
    auto ftn = audioForwarder_.fullTrackName();
    auto* evb = evbThread_->getEventBase();
    co_withExecutor(evb, initialAudioPublish(session, ftn)).start();
  } else {
    XLOG(ERR) << "No session available for audio publish";
  }

  return true;
}

folly::coro::Task<void> MoQVideoPublisher::initialAudioPublish(
    std::shared_ptr<MoQSession> session,
    FullTrackName ftn) {
  PublishRequest pub;
  pub.fullTrackName = std::move(ftn);
  pub.groupOrder = GroupOrder::OldestFirst;
  pub.forward = true;

  auto handle =
      std::make_shared<LocalSubscriptionHandle>(RequestID(0), TrackAlias(0));
  auto res = session->publish(std::move(pub), handle);
  if (!res) {
    XLOG(ERR) << "PUBLISH(audio) failed: code="
              << folly::to_underlying(res.error().errorCode)
              << " reason=" << res.error().reasonPhrase;
    co_return;
  }
  audioTrackPublisher_ = std::move(res->consumer);
  audioPublishReady_ = true;
  // TODO: reply can be success but set forward=0. In that case, we should
  // set audioPublishReady_ back to false and wait for a SUBSCRIBE_UPDATE
  // to re-enable it when forward becomes 1 again.
  // Await reply in background on EB and clear readiness on error
  {
    auto replyTask = folly::coro::co_withCancellation(
        cancel_.getToken(), std::move(res->reply));
    auto* replyEvb = evbThread_->getEventBase();
    std::weak_ptr<MoQVideoPublisher> selfWeak = shared_from_this();
    co_withExecutor(
        replyEvb,
        folly::coro::co_invoke(
            [selfWeak, replyTask = std::move(replyTask)]() mutable
                -> folly::coro::Task<void> {
              try {
                auto reply = co_await std::move(replyTask);
                if (reply.hasError()) {
                  auto self = selfWeak.lock();
                  if (!self) {
                    co_return;
                  }
                  self->audioPublishReady_ = false;
                  self->audioTrackPublisher_.reset();
                } else {
                  // TODO: if reply->forward == false, set publishReady=false
                  // and wait for SUBSCRIBE_UPDATE to re-enable it.
                }
              } catch (const std::exception&) {
                auto self = selfWeak.lock();
                if (!self) {
                  co_return;
                }
                self->audioPublishReady_ = false;
                self->audioTrackPublisher_.reset();
              }
              co_return;
            }))
        .start();
  }
  co_return;
}

folly::coro::Task<Publisher::SubscribeResult> MoQVideoPublisher::subscribe(
    SubscribeRequest sub,
    std::shared_ptr<TrackConsumer> callback) {
  if (sub.fullTrackName == videoForwarder_.fullTrackName()) {
    co_return videoForwarder_.addSubscriber(
        MoQSession::getRequestSession(), sub, std::move(callback));
  }

  if (sub.fullTrackName == audioForwarder_.fullTrackName()) {
    co_return audioForwarder_.addSubscriber(
        MoQSession::getRequestSession(), sub, std::move(callback));
  }

  if ((sub.fullTrackName != videoForwarder_.fullTrackName()) &&
      (sub.fullTrackName != audioForwarder_.fullTrackName())) {
    XLOG(ERR) << "Unknown track " << sub.fullTrackName;
    co_return folly::makeUnexpected(
        SubscribeError{
            sub.requestID,
            SubscribeErrorCode::TRACK_NOT_EXIST,
            "Unknown track"});
  }
  if ((sub.fullTrackName == videoForwarder_.fullTrackName()) &&
      !videoForwarder_.empty()) {
    XLOG(ERR) << "Already subscribed to video track "
              << videoForwarder_.fullTrackName();
    co_return folly::makeUnexpected(
        SubscribeError{
            sub.requestID,
            SubscribeErrorCode::INTERNAL_ERROR,
            "Already subscribed"});
  }

  if ((sub.fullTrackName == audioForwarder_.fullTrackName()) &&
      !audioForwarder_.empty()) {
    XLOG(ERR) << "Already subscribed to audio track "
              << audioForwarder_.fullTrackName();
    co_return folly::makeUnexpected(
        SubscribeError{
            sub.requestID,
            SubscribeErrorCode::INTERNAL_ERROR,
            "Already subscribed"});
  }

  co_return folly::makeUnexpected(
      SubscribeError{
          sub.requestID, SubscribeErrorCode::TRACK_NOT_EXIST, "Unknown track"});
}

void MoQVideoPublisher::publishVideoFrame(
    std::chrono::microseconds ptsUs,
    uint64_t flags,
    Payload payload) {
  std::weak_ptr<MoQVideoPublisher> selfWeak = shared_from_this();
  evbThread_->getEventBase()->add(
      [selfWeak, ptsUs, flags, payload = std::move(payload)]() mutable {
        auto self = selfWeak.lock();
        if (!self) {
          return;
        }
        if (!self->running_) {
          return;
        }
        self->publishFrameImpl(ptsUs, flags, std::move(payload));
      });
}

void MoQVideoPublisher::publishFrameImpl(
    std::chrono::microseconds ptsUs,
    uint64_t flags,
    Payload payload) {
  if (!savedMetadata_ &&
      (flags & folly::to_underlying(BufferFlags::CODEC_CONFIG))) {
    savedMetadata_ = convertMetadata(std::move(payload));
    payload = savedMetadata_->clone();
    savedMetadata_ = payload->clone();
  }
  if (videoForwarder_.empty()) {
    XLOG(ERR) << "No subscriber for track " << videoForwarder_.fullTrackName();
    return;
  }

  auto item = std::make_unique<MediaItem>();
  item->type = MediaType::VIDEO;
  item->id = videoSeqId_++;
  item->pts = ptsUs.count();
  // item->pts = (ptsUs.count() * timescale_) / 1000000;
  item->dts = item->pts; // wrong if B-frames are used
  item->timescale = 1000000;
  if (lastVideoPts_) {
    item->duration = item->pts - *lastVideoPts_;
  } else {
    item->duration = 1;
  }
  item->wallclock = currentTimeMilliseconds();
  item->isIdr = flags & folly::to_underlying(BufferFlags::KEY_FRAME);
  item->isEOF = flags & folly::to_underlying(BufferFlags::END_OF_STREAM);
  if (flags & folly::to_underlying(BufferFlags::CODEC_CONFIG)) {
    item->metadata = std::move(payload);
  } else if (item->isIdr && savedMetadata_) {
    // New IDR frame, send saved metadata
    item->metadata = savedMetadata_->clone();
    item->data = std::move(payload);
    lastVideoPts_ = item->pts;
  } else {
    // video data
    item->data = std::move(payload);
    lastVideoPts_ = item->pts;
  }
  publishFrameToMoQ(std::move(item));
}

void MoQVideoPublisher::endPublish() {
  // Ensure we cancel and wait for relay->run() coroutine exit before tearing
  // down the EB thread, to avoid join() deadlock.
  std::weak_ptr<MoQVideoPublisher> selfWeak = shared_from_this();
  evbThread_->getEventBase()->add([selfWeak] {
    if (auto self = selfWeak.lock()) {
      self->running_ = false;
      // Request cancellation for any in-flight EB coroutines
      self->cancel_.requestCancellation();
    }
  });

  // Give relay->run() a chance to observe cancellation and exit its loop
  // before we continue shutdown on the EB thread.
  evbThread_->getEventBase()->runInEventBaseThreadAndWait([selfWeak] {
    if (auto self = selfWeak.lock()) {
      if (!self->videoForwarder_.empty()) {
        self->videoForwarder_.subscribeDone(
            {0, SubscribeDoneStatusCode::TRACK_ENDED, 0, "end of track"});
      }
      self->audioPublishReady_ = false;
      self->audioTrackPublisher_.reset();
    }
  });

  // Wait for the relay run loop to finish so that destruction doesn't happen
  // on the EB thread and join() is safe.
  if (relayStarted_.load(std::memory_order_relaxed)) {
    runDone_.wait();
  }

  // After run loop has stopped, tear down session on EB thread to avoid races
  if (relayClient_) {
    std::weak_ptr<MoQVideoPublisher> selfWeak2 = shared_from_this();
    evbThread_->getEventBase()->runInEventBaseThreadAndWait([selfWeak2] {
      if (auto self2 = selfWeak2.lock()) {
        self2->relayClient_->shutdown();
      }
    });
  }
}

void MoQVideoPublisher::publishFrameToMoQ(std::unique_ptr<MediaItem> item) {
  if (item->isEOF || item->isIdr) {
    XLOG(INFO) << "Ending group";
    if (videoSgPub_) {
      videoSgPub_->endOfGroup(largestVideo_.object);
      videoSgPub_.reset();

      largestVideo_.group++;
      largestVideo_.object = 0;
    }
    if (item->isEOF) {
      return;
    }
  }

  if (!item->isIdr && !item->metadata && !videoSgPub_) {
    XLOG(INFO) << "Discarding non-IDR/metadata frame before subgroup started";
    return;
  }

  auto moqMiObj = MoQMi::encodeToMoQMi(std::move(item));
  if (!moqMiObj) {
    XLOG(ERR) << "Failed to encode video frame";
    return;
  }

  if (!videoSgPub_) {
    // Open new subgroup
    auto res = videoForwarder_.beginSubgroup(
        largestVideo_.group, 0, VIDEO_STREAM_PRIORITY);
    if (!res) {
      XLOG(ERR) << "Error creating subgroup";
    }
    videoSgPub_ = std::move(res.value());
  }

  // Send video data
  if (videoSgPub_) {
    XLOG(DBG1) << "Sending video frame. grp-obj: " << largestVideo_.group << "-"
               << largestVideo_.object << ". Payload size: "
               << (moqMiObj->payload
                       ? moqMiObj->payload->computeChainDataLength()
                       : 0);
    videoSgPub_->object(
        largestVideo_.object++,
        std::move(moqMiObj->payload),
        Extensions(std::move(moqMiObj->extensions), {}));
  } else {
    XLOG(ERR) << "Should not happen";
  }
}

void MoQVideoPublisher::publishAudioFrame(
    std::chrono::microseconds ptsUs,
    uint64_t flags,
    Payload payload) {
  std::weak_ptr<MoQVideoPublisher> selfWeak = shared_from_this();
  evbThread_->getEventBase()->add(
      [selfWeak, ptsUs, flags, payload = std::move(payload)]() mutable {
        auto self = selfWeak.lock();
        if (!self) {
          return;
        }
        if (!self->running_) {
          return;
        }
        self->publishAudioFrameImpl(ptsUs, flags, std::move(payload));
      });
}

void MoQVideoPublisher::publishAudioFrameImpl(
    std::chrono::microseconds ptsUs,
    uint64_t flags,
    Payload payload) {
  if (!audioTrackPublisher_ || !audioPublishReady_) {
    XLOG(DBG1) << "Audio publish path not established yet";
    return;
  }

  auto item = std::make_unique<MediaItem>();
  item->type = MediaType::AUDIO;
  item->id = audioSeqId_++;
  item->timescale = 1000000;
  item->pts = ptsUs.count();
  item->dts = item->pts;
  item->sampleFreq = 44100;
  item->numChannels = 1;

  // Calculate duration dynamically based on previous frame
  if (lastAudioPts_) {
    item->duration = item->pts - *lastAudioPts_;

    // Sanity check: if duration is unreasonably large or zero, use a
    // default
    if (item->duration <= 0 || item->duration > 100000) {
      // Default frame duration for AAC at 44.1kHz (approximately 23.2ms per
      // frame) = (1024 samples per AAC frame) * 1000000 / 44100 sample rate
      item->duration = 23220;
      XLOG(WARN) << "Invalid audio duration: " << item->duration
                 << ", using default instead";
    }
  } else {
    // For first frame, use a reasonable default based on AAC frame size
    item->duration = 23220; // 23.22ms - standard for AAC frame at 44.1kHz
  }

  lastAudioPts_ = item->pts;
  item->wallclock = currentTimeMilliseconds();
  item->isIdr = false;
  item->isEOF = false;
  item->data = std::move(payload);

  publishAudioFrameToMoQ(std::move(item));
}

void MoQVideoPublisher::publishAudioFrameToMoQ(
    std::unique_ptr<MediaItem> item) {
  auto id = item->id;
  // auto pts = item->pts;
  auto moqMiObj = MoQMi::encodeToMoQMi(std::move(item));
  if (!moqMiObj) {
    XLOG(ERR) << "Failed to encode audio frame";
    return;
  }

  ObjectHeader objHeader =
      ObjectHeader{/*groupIn=*/id,
                   /*subgroupIn=*/0,
                   /*idIn=*/0,
                   AUDIO_STREAM_PRIORITY,
                   ObjectStatus::NORMAL,
                   Extensions(std::move(moqMiObj->extensions), {})};

  if (auto res = audioTrackPublisher_->objectStream(
          objHeader, std::move(moqMiObj->payload));
      !res) {
    XLOG(ERR) << "audio objectStream error: " << res.error().describe();
  }
}

} // namespace moxygen

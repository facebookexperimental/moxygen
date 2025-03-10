// MoQVideoPublisher.cpp

#include <folly/io/Cursor.h>
#include <folly/io/IOBuf.h>
#include <folly/io/IOBufQueue.h>
#include <proxygen/lib/utils/URL.h>
#include <moxygen/moq_mi/MoQMi.h>
#include <moxygen/relay/MoQRelayClient.h>
#include <moxygen/samples/hack/MoQVideoPublisher.h>

constexpr std::chrono::milliseconds kConnectTimeout = std::chrono::seconds(5);
constexpr std::chrono::seconds kTransactionTimeout = std::chrono::seconds(60);

namespace {
uint64_t currentTimeMilliseconds() {
  auto now = std::chrono::high_resolution_clock::now();
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

std::unique_ptr<folly::IOBuf> convertMetadata(
    std::unique_ptr<folly::IOBuf> metadata) {
  auto [spsNalus, ppsNalus] = parseSPSandPPS(*metadata);
  return serializeAVCDecoderConfigurationRecord(spsNalus, ppsNalus);
}

} // namespace

namespace moxygen {
// Implementation of setup function
bool MoQVideoPublisher::setup(const std::string& connectURL) {
  proxygen::URL url(connectURL);
  if (!url.isValid() || !url.hasHost()) {
    XLOG(ERR) << "Invalid url: " << connectURL;
    return false;
  }
  relayClient_ = std::make_unique<MoQRelayClient>(
      std::make_unique<MoQClient>(evbThread_->getEventBase(), url));
  relayClient_
      ->run(
          /*publisher=*/shared_from_this(),
          /*subscriber=*/nullptr,
          {forwarder_.fullTrackName().trackNamespace},
          kConnectTimeout,
          kTransactionTimeout)
      .scheduleOn(evbThread_->getEventBase())
      .start();
  return true;
}

folly::coro::Task<Publisher::SubscribeResult> MoQVideoPublisher::subscribe(
    SubscribeRequest sub,
    std::shared_ptr<TrackConsumer> callback) {
  if (sub.fullTrackName != forwarder_.fullTrackName()) {
    XLOG(ERR) << "Unknown track " << sub.fullTrackName;
    co_return folly::makeUnexpected(SubscribeError{
        sub.subscribeID, SubscribeErrorCode::TRACK_NOT_EXIST, "Unknown track"});
  }
  if (!forwarder_.empty()) {
    XLOG(ERR) << "Already subscribed to track " << forwarder_.fullTrackName();
    co_return folly::makeUnexpected(SubscribeError{
        sub.subscribeID,
        SubscribeErrorCode::INTERNAL_ERROR,
        "Already subscribed"});
  }
  co_return forwarder_.addSubscriber(
      MoQSession::getRequestSession(), sub, std::move(callback));
}

void MoQVideoPublisher::publishFrame(
    std::chrono::microseconds ptsUs,
    uint64_t flags,
    Payload payload) {
  evbThread_->getEventBase()->runInEventBaseThread(
      [this, ptsUs, flags, payload = std::move(payload)]() mutable {
        publishFrameImpl(ptsUs, flags, std::move(payload));
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
  if (forwarder_.empty()) {
    XLOG(ERR) << "No subscriber for track " << forwarder_.fullTrackName();
    return;
  }

  auto item = std::make_unique<MediaItem>();
  item->type = MediaType::VIDEO;
  item->id = seqId_++;
  item->pts = (ptsUs.count() * timescale_) / 1000000;
  item->dts = item->pts; // wrong if B-frames are used
  item->timescale = timescale_;
  if (lastPts_) {
    item->duration = item->pts - *lastPts_;
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
    publishFrameToMoQ(std::move(item));
    auto md = std::move(savedMetadata_);
    publishFrameImpl(ptsUs, flags, std::move(payload));
    savedMetadata_ = std::move(md);
    return;
  } else {
    // video data
    item->data = std::move(payload);
    lastPts_ = item->pts;
  }
  publishFrameToMoQ(std::move(item));
}

void MoQVideoPublisher::endPublish() {
  evbThread_->getEventBase()->runInEventBaseThread([this] {
    forwarder_.subscribeDone(
        {0,
         SubscribeDoneStatusCode::TRACK_ENDED,
         0,
         "end of track",
         folly::none});
  });
}

void MoQVideoPublisher::publishFrameToMoQ(std::unique_ptr<MediaItem> item) {
  if (item->isEOF || item->isIdr) {
    XLOG(INFO) << "Ending group";
    if (videoSgPub_) {
      videoSgPub_->endOfGroup(latestVideo_.object);
      videoSgPub_.reset();

      latestVideo_.group++;
      latestVideo_.object = 0;
    }
    if (item->isEOF) {
      return;
    }
  }

  if (!item->isIdr && !item->metadata && !videoSgPub_) {
    XLOG(INFO) << "Discarding non-IDR/metadata frame before subgroup started";
    return;
  }

  auto objPayload = MoQMi::encodeToMoQMi(std::move(item));
  if (!objPayload) {
    XLOG(ERR) << "Failed to encode video frame";
    return;
  }

  if (!videoSgPub_) {
    // Open new subgroup
    auto res =
        forwarder_.beginSubgroup(latestVideo_.group, 0, kDefaultPriority);
    if (!res) {
      XLOG(ERR) << "Error creating subgroup";
    }
    videoSgPub_ = std::move(res.value());
  }

  // Send video data
  if (videoSgPub_) {
    XLOG(DBG1) << "Sending video frame. grp-obj: " << latestVideo_.group << "-"
               << latestVideo_.object
               << ". Payload size: " << objPayload->computeChainDataLength();
    videoSgPub_->object(latestVideo_.object++, std::move(objPayload));
  } else {
    XLOG(ERR) << "Should not happen";
  }
}
} // namespace moxygen

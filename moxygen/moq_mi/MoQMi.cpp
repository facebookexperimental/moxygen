/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/moq_mi/MoQMi.h"
#include <quic/codec/QuicInteger.h>

namespace moxygen {

std::unique_ptr<folly::IOBuf> MoQMi::toObjectPayload(
    std::unique_ptr<VideoH264AVCCWCPData> videoData) noexcept {
  folly::IOBufQueue buffQueue{folly::IOBufQueue::cacheChainLength()};
  size_t size = 0;
  bool error = false;

  writeVarint(
      buffQueue,
      folly::to_underlying(PayloadType::VideoH264AVCCWCP),
      size,
      error);
  writeVarint(buffQueue, videoData->seqId, size, error);
  writeVarint(buffQueue, videoData->pts, size, error);
  writeVarint(buffQueue, videoData->dts, size, error);
  writeVarint(buffQueue, videoData->timescale, size, error);
  writeVarint(buffQueue, videoData->duration, size, error);
  writeVarint(buffQueue, videoData->wallclock, size, error);
  if (videoData->metadata) {
    writeVarint(
        buffQueue, videoData->metadata->computeChainDataLength(), size, error);
    writeBuffer(buffQueue, std::move(videoData->metadata), size, error);
  } else {
    writeVarint(buffQueue, 0, size, error);
  }
  writeBuffer(buffQueue, std::move(videoData->data), size, error);

  if (error) {
    return nullptr;
  }
  return buffQueue.move();
}

std::unique_ptr<folly::IOBuf> MoQMi::toObjectPayload(
    std::unique_ptr<AudioAACMP4LCWCPData> audioData) noexcept {
  folly::IOBufQueue buffQueue{folly::IOBufQueue::cacheChainLength()};
  size_t size = 0;
  bool error = false;

  writeVarint(
      buffQueue,
      folly::to_underlying(PayloadType::AudioAACMP4LCWCP),
      size,
      error);
  writeVarint(buffQueue, audioData->seqId, size, error);
  writeVarint(buffQueue, audioData->pts, size, error);
  writeVarint(buffQueue, audioData->timescale, size, error);
  writeVarint(buffQueue, audioData->sampleFreq, size, error);
  writeVarint(buffQueue, audioData->numChannels, size, error);
  writeVarint(buffQueue, audioData->duration, size, error);
  writeVarint(buffQueue, audioData->wallclock, size, error);
  writeBuffer(buffQueue, std::move(audioData->data), size, error);

  if (error) {
    return nullptr;
  }
  return buffQueue.move();
}

MoQMi::MoqMiTag MoQMi::fromObjectPayload(
    std::unique_ptr<folly::IOBuf> payload) noexcept {
  folly::io::Cursor cursor(payload.get());

  auto mediaType = quic::decodeQuicInteger(cursor);
  if (!mediaType) {
    return MoQMi::MoqMiReadCmd::MOQMI_ERR;
  }

  if (mediaType->first == folly::to_underlying(PayloadType::VideoH264AVCCWCP)) {
    VideoH264AVCCWCPData videoData;
    auto seqId = quic::decodeQuicInteger(cursor);
    if (!seqId) {
      return MoQMi::MoqMiReadCmd::MOQMI_ERR;
    }
    auto pts = quic::decodeQuicInteger(cursor);
    if (!pts) {
      return MoQMi::MoqMiReadCmd::MOQMI_ERR;
    }
    auto dts = quic::decodeQuicInteger(cursor);
    if (!dts) {
      return MoQMi::MoqMiReadCmd::MOQMI_ERR;
    }
    auto timescale = quic::decodeQuicInteger(cursor);
    if (!timescale) {
      return MoQMi::MoqMiReadCmd::MOQMI_ERR;
    }
    auto duration = quic::decodeQuicInteger(cursor);
    if (!duration) {
      return MoQMi::MoqMiReadCmd::MOQMI_ERR;
    }
    auto wallclock = quic::decodeQuicInteger(cursor);
    if (!wallclock) {
      return MoQMi::MoqMiReadCmd::MOQMI_ERR;
    }
    auto metadataSize = quic::decodeQuicInteger(cursor);
    if (!metadataSize) {
      return MoQMi::MoqMiReadCmd::MOQMI_ERR;
    }
    std::unique_ptr<folly::IOBuf> metadata;
    if (metadataSize->first > 0) {
      if (!cursor.canAdvance(metadataSize->first)) {
        return MoQMi::MoqMiReadCmd::MOQMI_ERR;
      }
      cursor.clone(metadata, metadataSize->first);
    }
    std::unique_ptr<folly::IOBuf> data;
    cursor.clone(data, cursor.totalLength());
    return std::make_unique<VideoH264AVCCWCPData>(
        seqId->first,
        pts->first,
        timescale->first,
        duration->first,
        wallclock->first,
        std::move(data),
        std::move(metadata),
        dts->first);
  }

  if (mediaType->first == folly::to_underlying(PayloadType::AudioAACMP4LCWCP)) {
    AudioAACMP4LCWCPData audioData;

    auto seqId = quic::decodeQuicInteger(cursor);
    if (!seqId) {
      return MoQMi::MoqMiReadCmd::MOQMI_ERR;
    }
    auto pts = quic::decodeQuicInteger(cursor);
    if (!pts) {
      return MoQMi::MoqMiReadCmd::MOQMI_ERR;
    }
    auto timescale = quic::decodeQuicInteger(cursor);
    if (!timescale) {
      return MoQMi::MoqMiReadCmd::MOQMI_ERR;
    }
    auto sampleFreq = quic::decodeQuicInteger(cursor);
    if (!sampleFreq) {
      return MoQMi::MoqMiReadCmd::MOQMI_ERR;
    }
    auto numChannels = quic::decodeQuicInteger(cursor);
    if (!numChannels) {
      return MoQMi::MoqMiReadCmd::MOQMI_ERR;
    }
    auto duration = quic::decodeQuicInteger(cursor);
    if (!duration) {
      return MoQMi::MoqMiReadCmd::MOQMI_ERR;
    }
    auto wallclock = quic::decodeQuicInteger(cursor);
    if (!wallclock) {
      return MoQMi::MoqMiReadCmd::MOQMI_ERR;
    }
    std::unique_ptr<folly::IOBuf> data;
    cursor.clone(data, cursor.totalLength());
    return std::make_unique<AudioAACMP4LCWCPData>(
        seqId->first,
        pts->first,
        timescale->first,
        duration->first,
        wallclock->first,
        std::move(data),
        sampleFreq->first,
        numChannels->first);
  }

  // Not implemented payload type
  return MoQMi::MoqMiReadCmd::MOQMI_UNKNOWN;
}

void MoQMi::writeBuffer(
    folly::IOBufQueue& buf,
    std::unique_ptr<folly::IOBuf> data,
    size_t& size,
    bool& error) noexcept {
  if (error || data == nullptr) {
    return;
  }
  size += data->computeChainDataLength();
  buf.append(std::move(data));
}

void MoQMi::writeVarint(
    folly::IOBufQueue& buf,
    uint64_t value,
    size_t& size,
    bool& error) noexcept {
  if (error) {
    return;
  }
  folly::io::QueueAppender appender(&buf, MoQMi::kMaxQuicIntSize);
  auto appenderOp = [appender = std::move(appender)](auto val) mutable {
    appender.writeBE(val);
  };
  auto res = quic::encodeQuicInteger(value, appenderOp);
  if (res.hasError()) {
    error = true;
  } else {
    size += *res;
  }
}

} // namespace moxygen

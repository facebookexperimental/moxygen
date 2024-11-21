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

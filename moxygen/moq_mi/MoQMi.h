/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

// TODO: We need to opensource (or re implememt) FLV reader
#include <folly/io/IOBuf.h>
#include <folly/io/IOBufQueue.h>
#include <cstdint>

namespace moxygen {

class MoQMi {
 public:
  enum class PayloadType : uint64_t {
    VideoH264AVCCWCP = 0x0,
    AudioOpusWCP = 0x1,
    RAW = 0x2,
    AudioAACMP4LCWCP = 0x3,
  };

  struct CommonData {
    std::unique_ptr<folly::IOBuf> data;
    uint64_t seqId;
    uint64_t pts;
    uint64_t timescale;
    uint64_t duration;
    uint64_t wallclock;
    CommonData()
        : data(nullptr),
          seqId(0),
          pts(0),
          timescale(0),
          duration(0),
          wallclock(0) {}
    CommonData(
        uint64_t seqId,
        uint64_t pts,
        uint64_t timescale,
        uint64_t duration,
        uint64_t wallclock,
        std::unique_ptr<folly::IOBuf> data)
        : data(std::move(data)),
          seqId(seqId),
          pts(pts),
          timescale(timescale),
          duration(duration),
          wallclock(wallclock) {}
  };

  struct VideoH264AVCCWCPData : public CommonData {
    std::unique_ptr<folly::IOBuf> metadata;
    uint64_t dts;
    VideoH264AVCCWCPData() : CommonData(), metadata(nullptr), dts(0) {}
    VideoH264AVCCWCPData(
        uint64_t seqId,
        uint64_t pts,
        uint64_t timescale,
        uint64_t duration,
        uint64_t wallclock,
        std::unique_ptr<folly::IOBuf> data,
        std::unique_ptr<folly::IOBuf> metadata,
        uint64_t dts)
        : CommonData(
              seqId,
              pts,
              timescale,
              duration,
              wallclock,
              std::move(data)),
          metadata(std::move(metadata)),
          dts(dts) {}

    friend std::ostream& operator<<(
        std::ostream& os,
        const VideoH264AVCCWCPData& v);
  };

  struct AudioAACMP4LCWCPData : public CommonData {
    uint64_t sampleFreq;
    uint64_t numChannels;
    AudioAACMP4LCWCPData() : CommonData(), sampleFreq(0), numChannels(0) {}
    AudioAACMP4LCWCPData(
        uint64_t seqId,
        uint64_t pts,
        uint64_t timescale,
        uint64_t duration,
        uint64_t wallclock,
        std::unique_ptr<folly::IOBuf> data,
        uint64_t sampleFreq,
        uint64_t numChannels)
        : CommonData(
              seqId,
              pts,
              timescale,
              duration,
              wallclock,
              std::move(data)),
          sampleFreq(sampleFreq),
          numChannels(numChannels) {}

    friend std::ostream& operator<<(
        std::ostream& os,
        const AudioAACMP4LCWCPData& a);
  };

  explicit MoQMi() {}
  virtual ~MoQMi() {}

  static std::unique_ptr<folly::IOBuf> toObjectPayload(
      std::unique_ptr<VideoH264AVCCWCPData> data) noexcept;
  static std::unique_ptr<folly::IOBuf> toObjectPayload(
      std::unique_ptr<AudioAACMP4LCWCPData> data) noexcept;

  static std::tuple<
      std::unique_ptr<MoQMi::VideoH264AVCCWCPData>,
      std::unique_ptr<MoQMi::AudioAACMP4LCWCPData>>
  fromObjectPayload(std::unique_ptr<folly::IOBuf> data) noexcept;

 private:
  static const size_t kMaxQuicIntSize = 32;

  static void writeVarint(
      folly::IOBufQueue& buf,
      uint64_t value,
      size_t& size,
      bool& error) noexcept;

  static void writeBuffer(
      folly::IOBufQueue& buf,
      std::unique_ptr<folly::IOBuf> data,
      size_t& size,
      bool& error) noexcept;
};

std::ostream& operator<<(
    std::ostream& os,
    MoQMi::VideoH264AVCCWCPData const& v) {
  auto metadataSize = v.metadata != nullptr ? v.metadata->length() : 0;
  auto dataSize = v.data != nullptr ? v.data->length() : 0;
  os << "VideoH264. id: " << v.seqId << ", pts: " << v.pts << ", dts: " << v.dts
     << ", timescale: " << v.timescale << ", duration: " << v.duration
     << ", wallclock: " << v.wallclock << ", metadata length: " << metadataSize
     << ", data length: " << dataSize;
  return os;
}

std::ostream& operator<<(
    std::ostream& os,
    MoQMi::AudioAACMP4LCWCPData const& a) {
  auto dataSize = a.data != nullptr ? a.data->length() : 0;
  os << "AudioAAC. id: " << a.seqId << ", pts: " << a.pts
     << ", sampleFreq: " << a.sampleFreq << ", numChannels: " << a.numChannels
     << ", timescale: " << a.timescale << ", duration: " << a.duration
     << ", wallclock: " << a.wallclock << ", data length: " << dataSize;
  return os;
}

} // namespace moxygen

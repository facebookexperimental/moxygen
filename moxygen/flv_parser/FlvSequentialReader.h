/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/Optional.h>
#include <folly/io/IOBuf.h>
#include <folly/logging/xlog.h>
#include "moxygen/flv_parser/FlvReader.h"

namespace moxygen {

class FlvSequentialReader {
 public:
  // Audio ASC sampling frequency mapping
  const uint32_t kAscFreqSamplingIndexMapping[16]{
      96000, // 0
      88200, // 1
      64000, // 2
      48000, // 3
      44100, // 4
      32000, // 5
      24000, // 6
      22050, // 7
      16000, // 8
      12000, // 9
      11025, // 10
      8000,  // 11
      7350,  // 12
      13,    // 13 - reserved
      14,    // 14 - reserved
      15,    // 15 - escape value
  };

  // Helper class to read bits from a buffer
  class BitReader {
   public:
    explicit BitReader(std::unique_ptr<folly::IOBuf> data)
        : data_(std::move(data)) {
      XLOG(INFO) << __func__;
    }
    ~BitReader() {}

    uint64_t getNextBits(uint8_t numBits) {
      uint64_t result = 0;

      if (numBits > 64) {
        throw std::runtime_error("Can not read more than 64 bits");
      }

      for (uint64_t i = 0; i < numBits; i++) {
        uint64_t bytePos = bitOffset_ / 8;
        uint64_t bitPos = bitOffset_ % 8;
        if (bytePos >= data_->computeChainDataLength()) {
          throw std::runtime_error("Can not read more bits, short");
        }
        uint8_t tmp = ((uint8_t)data_->data()[bytePos]) &
            (uint8_t)std::pow(2, 7 - bitPos);
        result = result << 1;
        if (tmp > 0) {
          result |= 1;
        } else {
          result |= 0;
        }
        bitOffset_++;
      }
      return result;
    }

   private:
    std::unique_ptr<folly::IOBuf> data_;
    uint64_t bitOffset_{0};
  };

  // FLV timebase
  const uint32_t kFlvTimeScale = 1000;

  // FLV media types
  enum class MediaType : uint8_t {
    VIDEO = 0x0,
    AUDIO = 0x1,
    NONE = 0xff,
  };

  // Data to return
  struct MediaItem {
    std::unique_ptr<folly::IOBuf> data;
    std::unique_ptr<folly::IOBuf> metadata;
    MediaType type;
    uint64_t id;
    uint64_t pts;
    uint64_t dts;
    uint64_t timescale;
    uint64_t duration;
    uint64_t wallclock;
    uint64_t sampleFreq;
    uint64_t numChannels;
    bool isIdr;
    bool isEOF;
    MediaItem()
        : data(nullptr),
          metadata(nullptr),
          type(MediaType::NONE),
          id(0),
          pts(0),
          dts(0),
          timescale(0),
          duration(0),
          wallclock(0),
          sampleFreq(0),
          numChannels(0),
          isIdr{false},
          isEOF{false} {}
  };

  explicit FlvSequentialReader(const std::string& file_path)
      : reader_(file_path),
        file_path_(file_path),
        videoFrameId_(0),
        audioFrameId_(0) {
    XLOG(INFO) << __func__;
  }
  ~FlvSequentialReader() {
    XLOG(INFO) << __func__;
  }

  std::shared_ptr<MediaItem> getNextItem();

 private:
  bool parseAscHeader(std::unique_ptr<folly::IOBuf>);

  FlvReader reader_;
  std::string file_path_;

  std::unique_ptr<folly::IOBuf> avcDecoderRecord_;

  folly::Optional<uint32_t> aot_;
  folly::Optional<uint32_t> sampleFreq_;
  folly::Optional<uint32_t> numChannels_;

  uint64_t videoFrameId_;
  uint64_t audioFrameId_;

  folly::Optional<uint64_t> lastVideoPts_;
  folly::Optional<uint64_t> lastAudioPts_;
};

} // namespace moxygen

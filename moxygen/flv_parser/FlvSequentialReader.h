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

namespace moxygen::flv {

class FlvSequentialReader {
 public:
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
    explicit MediaItem(const MediaItem& m)
        : data(nullptr),
          metadata(nullptr),
          type(m.type),
          id(m.id),
          pts(m.pts),
          dts(m.dts),
          timescale(m.timescale),
          duration(m.duration),
          wallclock(m.wallclock),
          sampleFreq(m.sampleFreq),
          numChannels(m.numChannels),
          isIdr(m.isIdr),
          isEOF(m.isEOF) {}

    std::unique_ptr<MediaItem> clone() {
      auto clone = std::make_unique<MediaItem>(*this);
      clone->data = data != nullptr ? data->clone() : nullptr;
      clone->metadata = metadata != nullptr ? metadata->clone() : nullptr;
      return clone;
    }
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

  std::unique_ptr<MediaItem> getNextItem();

 private:
  FlvReader reader_;
  std::string file_path_;

  std::unique_ptr<folly::IOBuf> avcDecoderRecord_;

  AscHeaderData ascHeader_;

  uint64_t videoFrameId_;
  uint64_t audioFrameId_;

  folly::Optional<uint64_t> lastVideoPts_;
  folly::Optional<uint64_t> lastAudioPts_;
};

} // namespace moxygen::flv

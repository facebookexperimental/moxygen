#pragma once

#include <folly/io/IOBuf.h>
#include <cstdint>
#include <memory>

namespace moxygen {

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
  MediaItem(
      std::unique_ptr<folly::IOBuf> data,
      std::unique_ptr<folly::IOBuf> metadata,
      MediaType type,
      uint64_t id,
      uint64_t pts,
      uint64_t dts,
      uint64_t timescale,
      uint64_t duration,
      uint64_t wallclock,
      bool isIdr,
      bool isEOF)
      : data(std::move(data)),
        metadata(std::move(metadata)),
        type(type),
        id(id),
        pts(pts),
        dts(dts),
        timescale(timescale),
        duration(duration),
        wallclock(wallclock),
        sampleFreq(0),
        numChannels(0),
        isIdr{isIdr},
        isEOF{isEOF} {}
  MediaItem(
      std::unique_ptr<folly::IOBuf> data,
      MediaType type,
      uint64_t id,
      uint64_t pts,
      uint64_t timescale,
      uint64_t duration,
      uint64_t wallclock,
      uint64_t sampleFreq,
      uint64_t numChannels,
      bool isEOF)
      : data(std::move(data)),
        metadata(nullptr),
        type(type),
        id(id),
        pts(pts),
        dts(0),
        timescale(timescale),
        duration(duration),
        wallclock(wallclock),
        sampleFreq(sampleFreq),
        numChannels(numChannels),
        isIdr{true},
        isEOF{isEOF} {}
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

} // namespace moxygen

// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#pragma once

#include <folly/Optional.h>
#include <folly/io/Cursor.h>
#include <folly/io/IOBuf.h>
#include <cstdint>
#include <variant>

namespace moxygen::flv {

// Audio sampling frequency mapping
const std::array<uint32_t, 4> kAudioFreqSamplingIndexMapping{
    5500,  // 0
    11000, // 1
    22000, // 2
    44100, // 3
};
// Audio sampling frequency mapping
const std::array<uint32_t, 2> kAudioBitsPerSampleMapping{
    8,  // 0
    16, // 1
};
// Audio sampling frequency mapping
const std::array<uint32_t, 2> kAudioChannels{
    1, // 0
    2, // 1
};

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
    if (data_ != nullptr) {
      cursor_ = std::make_unique<folly::io::Cursor>(data_.get());
    }
  }

  uint64_t getNextBits(uint8_t numBits) {
    if (data_ == nullptr || cursor_ == nullptr) {
      throw std::runtime_error("Can not read from empty buffer");
    }
    uint64_t result = 0;
    if (numBits > 64) {
      throw std::runtime_error("Can not read more than 64 bits");
    }
    for (int i = 0; i < numBits; i++) {
      uint64_t bitPos = bitOffset_ % 8;
      if (bitPos == 0) {
        if (!cursor_->canAdvance(1)) {
          throw std::runtime_error("Can not read more bits, short");
        }
        currentByte_ = cursor_->readBE<uint8_t>();
      }
      CHECK_EQ(currentByte_.has_value(), true);
      uint8_t tmp = currentByte_.value_or(0) & (uint8_t)std::pow(2, 7 - bitPos);
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
  std::unique_ptr<folly::io::Cursor> cursor_;
  folly::Optional<uint8_t> currentByte_{folly::none};
  uint64_t bitOffset_{0};
};

// ASC functions

uint8_t getAscFreqIndex(uint32_t sampleFreq);

struct AscHeaderData {
  bool valid;
  uint8_t aot;
  uint8_t freqIndex;
  uint32_t sampleFreq;
  uint8_t channels;
  AscHeaderData()
      : valid(false), aot(0), freqIndex(0xff), sampleFreq(0), channels(0) {}
  AscHeaderData(uint8_t aot, uint32_t sampleFreq, uint8_t channels)
      : valid(true),
        aot(aot),
        freqIndex(0xff),
        sampleFreq(sampleFreq),
        channels(channels) {
    freqIndex = getAscFreqIndex(sampleFreq);
  }
  bool operator==(const AscHeaderData& other) {
    return (
        this->valid == other.valid && this->aot == other.aot &&
        this->freqIndex == other.freqIndex &&
        this->sampleFreq == other.sampleFreq &&
        this->channels == other.channels);
  }
  friend std::ostream& operator<<(std::ostream& os, const AscHeaderData& v);
};

AscHeaderData parseAscHeader(std::unique_ptr<folly::IOBuf> buf);
std::unique_ptr<folly::IOBuf>
createAscheader(uint8_t aot, uint32_t sampleFreq, uint8_t channels);

struct FlvTagBase {
  uint8_t type;
  uint32_t size;
  uint32_t timestamp;
  uint32_t streamId;
  FlvTagBase() : type(0), size(0), timestamp(0), streamId(0) {}
  FlvTagBase(uint8_t type, uint32_t size, uint32_t timestamp, uint32_t streamId)
      : type(type), size(size), timestamp(timestamp), streamId(streamId) {}
  explicit FlvTagBase(const FlvTagBase& tag)
      : type(tag.type),
        size(tag.size),
        timestamp(tag.timestamp),
        streamId(tag.streamId) {}
  bool operator==(const FlvTagBase& other) {
    return (
        this->type == other.type && this->size == other.size &&
        this->timestamp == other.timestamp && this->streamId == other.streamId);
  }
};
struct FlvVideoTag : public FlvTagBase {
  uint8_t frameType;
  uint8_t codecId;
  uint8_t avcPacketType;
  uint32_t compositionTime;
  std::unique_ptr<folly::IOBuf> data;
  explicit FlvVideoTag(const FlvTagBase& tag)
      : FlvTagBase(tag),
        frameType(0),
        codecId(0),
        avcPacketType(0),
        compositionTime(0) {}
  FlvVideoTag(
      const FlvTagBase& tag,
      uint8_t frameType,
      uint8_t codecId,
      uint8_t avcPacketType,
      uint32_t compositionTime,
      std::unique_ptr<folly::IOBuf> data)
      : FlvTagBase(tag),
        frameType(frameType),
        codecId(codecId),
        avcPacketType(avcPacketType),
        compositionTime(compositionTime),
        data(std::move(data)) {}
  bool operator==(const FlvVideoTag& other) {
    auto base = dynamic_cast<FlvTagBase*>(this);
    auto baseOther = dynamic_cast<const FlvTagBase*>(&other);
    if (*base != *baseOther) {
      return false;
    }
    folly::IOBufEqualTo eq;
    if (!eq(data, other.data)) {
      return false;
    }
    return (
        this->frameType == other.frameType && this->codecId == other.codecId &&
        this->avcPacketType == other.avcPacketType &&
        this->compositionTime == other.compositionTime);
  }

  std::unique_ptr<FlvVideoTag> clone() {
    auto clone = std::make_unique<FlvVideoTag>(
        *this, frameType, codecId, avcPacketType, compositionTime, nullptr);
    clone->data = data != nullptr ? data->clone() : nullptr;
    return clone;
  }
};
struct FlvAudioTag : public FlvTagBase {
  uint8_t soundFormat;
  uint32_t soundRate;
  uint8_t soundSize;
  uint8_t soundType; // Channels
  uint8_t aacPacketType;
  std::unique_ptr<folly::IOBuf> data;
  explicit FlvAudioTag(const FlvTagBase& tag)
      : FlvTagBase(tag),
        soundFormat(0),
        soundRate(0),
        soundSize(0),
        soundType(0),
        aacPacketType(0) {}
  FlvAudioTag(
      const FlvTagBase& tag,
      uint8_t soundFormat,
      uint32_t soundRate,
      uint8_t soundSize,
      uint8_t soundType,
      uint8_t aacPacketType,
      std::unique_ptr<folly::IOBuf> data)
      : FlvTagBase(tag),
        soundFormat(soundFormat),
        soundRate(soundRate),
        soundSize(soundSize),
        soundType(soundType),
        aacPacketType(aacPacketType),
        data(std::move(data)) {}
  bool operator==(const FlvAudioTag& other) {
    auto base = dynamic_cast<FlvTagBase*>(this);
    auto baseOther = dynamic_cast<const FlvTagBase*>(&other);
    if (*base != *baseOther) {
      return false;
    }
    folly::IOBufEqualTo eq;
    if (!eq(data, other.data)) {
      return false;
    }
    return (
        this->soundFormat == other.soundFormat &&
        this->soundRate == other.soundRate &&
        this->soundSize == other.soundSize &&
        this->soundType == other.soundType &&
        this->aacPacketType == other.aacPacketType);
  }
  std::unique_ptr<FlvAudioTag> clone() {
    auto clone = std::make_unique<FlvAudioTag>(
        *this,
        soundFormat,
        soundRate,
        soundSize,
        soundType,
        aacPacketType,
        nullptr);
    clone->data = data != nullptr ? data->clone() : nullptr;
    return clone;
  }
  uint32_t getSamplingFreq() {
    return kAudioFreqSamplingIndexMapping[soundRate];
  }
  uint32_t getBitsPerSample() {
    return kAudioBitsPerSampleMapping[soundSize];
  }
  uint32_t getChannels() {
    return kAudioChannels[soundType];
  }
};
struct FlvScriptTag : public FlvTagBase {
  std::unique_ptr<folly::IOBuf> data;
  explicit FlvScriptTag(const FlvTagBase& tag) : FlvTagBase(tag) {}
  FlvScriptTag(const FlvTagBase& tag, std::unique_ptr<folly::IOBuf> data)
      : FlvTagBase(tag), data(std::move(data)) {}
  bool operator==(const FlvScriptTag& other) {
    auto base = dynamic_cast<FlvTagBase*>(this);
    auto baseOther = dynamic_cast<const FlvTagBase*>(&other);
    if (*base != *baseOther) {
      return false;
    }
    folly::IOBufEqualTo eq;
    if (!eq(data, other.data)) {
      return false;
    }
    return true;
  }
  std::unique_ptr<FlvScriptTag> clone() {
    auto clone = std::make_unique<FlvScriptTag>(*this, nullptr);
    clone->data = data != nullptr ? data->clone() : nullptr;
    return clone;
  }
};

enum FlvReadCmd { FLV_EOF = 0xff, FLV_UNKNOWN_TAG = 0x1 };
enum FlvTagTypeIndex {
  FLV_TAG_INDEX_SCRIPT = 0,
  FLV_TAG_INDEX_VIDEO = 1,
  FLV_TAG_INDEX_AUDIO = 2,
  FLV_TAG_INDEX_READCMD = 3
};
using FlvTag = std::variant<
    std::unique_ptr<FlvScriptTag>,
    std::unique_ptr<FlvVideoTag>,
    std::unique_ptr<FlvAudioTag>,
    FlvReadCmd>;

std::unique_ptr<flv::FlvScriptTag> createScriptTag(
    uint32_t ts,
    std::unique_ptr<folly::IOBuf> data);

std::unique_ptr<flv::FlvVideoTag> createVideoTag(
    uint32_t ts,
    uint8_t frameType,
    uint8_t codecId,
    uint8_t avcPacketType,
    uint32_t compositionTime,
    std::unique_ptr<folly::IOBuf> data);

std::unique_ptr<flv::FlvAudioTag> createAudioTag(
    uint32_t ts,
    uint8_t soundFormat,
    uint32_t soundRate,
    uint8_t soundSize,
    uint8_t soundType,
    uint8_t aacPacketType,
    std::unique_ptr<folly::IOBuf> data);
} // namespace moxygen::flv

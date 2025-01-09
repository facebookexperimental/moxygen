// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#pragma once

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
typedef std::variant<
    std::unique_ptr<FlvScriptTag>,
    std::unique_ptr<FlvVideoTag>,
    std::unique_ptr<FlvAudioTag>,
    FlvReadCmd>
    FlvTag;

} // namespace moxygen::flv

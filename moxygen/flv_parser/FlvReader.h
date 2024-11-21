/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/io/IOBuf.h>
#include <cstdint>
#include <fstream> // std::ifstream

namespace moxygen {

class FlvReader {
 public:
  // Audio sampling frequency mapping
  const uint32_t kAudioFreqSamplingIndexMapping[4]{
      5500,  // 0
      11000, // 1
      22000, // 2
      44100, // 3
  };
  // Audio sampling frequency mapping
  const uint32_t kAudioBitsPerSampleMapping[2]{
      8,  // 0
      16, // 1
  };
  // Audio sampling frequency mapping
  const uint32_t kAudioChannels[2]{
      1, // 0
      2, // 1
  };

  struct FlvTag {
    uint8_t type;
    uint32_t size;
    uint32_t timestamp;
    uint32_t streamId;
    FlvTag() : type(0), size(0), timestamp(0), streamId(0) {}
    explicit FlvTag(const FlvTag& tag)
        : type(tag.type),
          size(tag.size),
          timestamp(tag.timestamp),
          streamId(tag.streamId) {}
    FlvTag& operator=(const FlvTag& other) {
      if (this != &other) {
        // not a self-assignment
        type = other.type;
        size = other.size;
        timestamp = other.timestamp;
        streamId = other.streamId;
      }
      return *this;
    }
  };
  struct FlvVideoTag : public FlvTag {
    uint8_t frameType;
    uint8_t codecId;
    uint8_t avcPacketType;
    uint8_t compositionTime;
    std::unique_ptr<folly::IOBuf> data;
    explicit FlvVideoTag(const FlvTag& tag)
        : FlvTag(tag),
          frameType(0),
          codecId(0),
          avcPacketType(0),
          compositionTime(0) {}
  };
  struct FlvAudioTag : public FlvTag {
    uint8_t soundFormat;
    uint32_t soundRate;
    uint8_t soundSize;
    uint8_t soundType; // Channels
    uint8_t aacPacketType;
    std::unique_ptr<folly::IOBuf> data;
    explicit FlvAudioTag(const FlvTag& tag)
        : FlvTag(tag),
          soundFormat(0),
          soundRate(0),
          soundSize(0),
          soundType(0),
          aacPacketType(0) {}
  };

  explicit FlvReader(const std::string& filename)
      : f_(filename, std::ifstream::binary) {}

  ~FlvReader() {
    f_.close();
  }

  std::tuple<
      std::unique_ptr<FlvTag>,
      std::unique_ptr<FlvVideoTag>,
      std::unique_ptr<FlvAudioTag>>
  readNextTag();

 private:
  uint8_t read1Byte();
  uint32_t read3Bytes();
  uint32_t read4Bytes();
  std::unique_ptr<folly::IOBuf> readBytes(size_t n);

  std::ifstream f_;

  std::unique_ptr<folly::IOBuf> header_;
};

} // namespace moxygen

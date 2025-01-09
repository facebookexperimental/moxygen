/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/flv_parser/FlvReader.h"
#include <netinet/in.h>

namespace moxygen::flv {

FlvTag FlvReader::readNextTag() {
  if (!header_) {
    // Read header
    header_ = readBytes(9);
  }

  // Prev tag size
  read4Bytes();

  if (f_.peek() == EOF) {
    // Clean end of file
    return FlvReadCmd::FLV_EOF;
  }

  std::unique_ptr<FlvTagBase> tag = std::make_unique<FlvTagBase>();
  std::unique_ptr<FlvAudioTag> audioTag;
  std::unique_ptr<FlvVideoTag> videoTag;
  std::unique_ptr<FlvScriptTag> scriptTag;
  tag->type = read1Byte();

  // Tag data size
  tag->size = read3Bytes();

  // Timestamp
  auto ltimestamp = read3Bytes();
  auto htimestamp = read1Byte();
  tag->timestamp = htimestamp << 24 | ltimestamp;

  // Stream id
  tag->streamId = read3Bytes();

  if (tag->type == 0x08) {
    // Audio tag
    audioTag = std::make_unique<FlvAudioTag>(*tag);

    auto tmp = read1Byte();
    audioTag->soundFormat = (tmp >> 4) & 0x0f;
    audioTag->soundRate = (tmp >> 2) & 0x3;
    if (audioTag->soundRate > 3) {
      throw std::runtime_error(fmt::format(
          "Unsupported audio sampling rate. soundRateIndex {}",
          audioTag->soundRate));
    }
    // audioTag->soundRate = kAudioFreqSamplingIndexMapping[soundRateIndex];
    audioTag->soundSize = (tmp >> 1) & 0x1;
    if (audioTag->soundSize > 1) {
      throw std::runtime_error(fmt::format(
          "Unsupported audio bits per sample. soundSizeIndex {}",
          audioTag->soundSize));
    }
    // audioTag->soundSize = kAudioBitsPerSampleMapping[soundSizeIndex];
    audioTag->soundType = tmp & 0x1;
    if (audioTag->soundType > 1) {
      throw std::runtime_error(fmt::format(
          "Unsupported number of channels. soundSizeIndex {}",
          audioTag->soundType));
    }
    // audioTag->soundType = kAudioChannels[soundTypeIndex];
    if (audioTag->soundFormat != 10 || audioTag->soundRate != 3 ||
        audioTag->soundSize != 1 || audioTag->soundType != 1) {
      throw std::runtime_error(fmt::format(
          "Unsupported audio format, only AAC is supported. soundFormat {}, soundRate {}, soundSize {}, soundType {} (byte: {})",
          audioTag->soundFormat,
          audioTag->soundRate,
          audioTag->soundSize,
          audioTag->soundType,
          tmp));
    }

    audioTag->aacPacketType = read1Byte();
    if (audioTag->aacPacketType > 1) {
      throw std::runtime_error(fmt::format(
          "Unsupported AAC packet type. packetType {}",
          audioTag->aacPacketType));
    }

    // Read data
    if (tag->size > 2) {
      audioTag->data = readBytes(tag->size - 2);
    }

    return audioTag;
  }

  if (tag->type == 0x09) {
    // Video tag
    videoTag = std::make_unique<FlvVideoTag>(*tag);

    auto tmp = read1Byte();
    videoTag->frameType = (tmp >> 4) & 0x0f;
    videoTag->codecId = tmp & 0x0f;
    if (videoTag->codecId != 0x07) {
      throw std::runtime_error(fmt::format(
          "Unsupported video codec. Only h264 supported. CodecId {}",
          videoTag->codecId));
    }
    videoTag->avcPacketType = read1Byte();
    if (videoTag->avcPacketType > 2) {
      throw std::runtime_error(fmt::format(
          "Unsupported AVC packet type. packetType {}",
          videoTag->avcPacketType));
    }
    videoTag->compositionTime = read3Bytes();

    // Read data
    if (tag->size > 5) {
      videoTag->data = readBytes(tag->size - 5);
    }

    return videoTag;
  }

  if (tag->type == 0x12) {
    // Script tag
    scriptTag = std::make_unique<FlvScriptTag>(*tag);
    // Read data
    if (tag->size > 0) {
      scriptTag->data = readBytes(tag->size);
    }
    return scriptTag;
  }

  // Skip data
  readBytes(tag->size);

  return FlvReadCmd::FLV_UNKNOWN_TAG;
}

uint8_t FlvReader::read1Byte() {
  uint8_t ret = 0;
  char tmp = 0;
  const int64_t bytesToRead = 1;
  f_.read(&tmp, bytesToRead);
  if (f_.gcount() == bytesToRead && f_.rdstate() == std::ios::goodbit) {
    ret = tmp;
  } else {
    // TODO: handle EOF
    if (f_.eof()) {
      throw std::runtime_error("EOF!");
    }
    throw std::runtime_error(fmt::format(
        "Failed to read 1 byte at offset {}", static_cast<int>(f_.tellg())));
  }
  return ret;
}

uint32_t FlvReader::read3Bytes() {
  uint32_t ret = 0;
  uint32_t readData = 0;
  f_.read(reinterpret_cast<char*>(&readData), 3);
  if (f_.gcount() == 3 && f_.rdstate() == std::ios::goodbit) {
    ret = ntohl(readData << 8);
  } else {
    throw std::runtime_error(fmt::format(
        "Failed to read 3 byte at offset {}", static_cast<int>(f_.tellg())));
  }
  return ret;
}

uint32_t FlvReader::read4Bytes() {
  uint32_t ret = 0;
  uint32_t readData;
  f_.read(reinterpret_cast<char*>(&readData), 4);
  if (f_.gcount() == 4 && f_.rdstate() == std::ios::goodbit) {
    ret = ntohl(readData);

  } else {
    throw std::runtime_error(fmt::format(
        "Failed to read 4 byte at offset {}. bytesRead: {}, rdstate: {}",
        static_cast<int>(f_.tellg()),
        static_cast<size_t>(f_.gcount()),
        static_cast<int>(f_.rdstate())));
  }
  return ret;
}

std::unique_ptr<folly::IOBuf> FlvReader::readBytes(size_t n) {
  std::unique_ptr<folly::IOBuf> ret;
  if (n > std::numeric_limits<int32_t>::max()) {
    throw std::runtime_error(
        fmt::format("Cannot read more than int64_t::max {}", n));
  }
  const int64_t bytesToRead = n;
  uint8_t tmp[bytesToRead];
  f_.read((char*)tmp, bytesToRead);
  if (f_.gcount() == bytesToRead && f_.rdstate() == std::ios::goodbit) {
    ret = folly::IOBuf::copyBuffer(tmp, bytesToRead);
  } else {
    throw std::runtime_error(fmt::format(
        "Failed to read {} bytes at offset {}",
        n,
        static_cast<int>(f_.tellg())));
  }
  return ret;
}

} // namespace moxygen::flv

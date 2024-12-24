/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/flv_parser/FlvReader.h"

namespace moxygen {

std::tuple<
    std::unique_ptr<FlvReader::FlvTag>,
    std::unique_ptr<FlvReader::FlvVideoTag>,
    std::unique_ptr<FlvReader::FlvAudioTag>>
FlvReader::readNextTag() {
  if (!header_) {
    // Read header
    header_ = readBytes(9);
  }

  // Prev tag size
  read4Bytes();

  if (f_.peek() == EOF) {
    // Clean end of file
    return {
        nullptr,
        nullptr,
        nullptr,
    };
  }

  std::unique_ptr<FlvTag> tag = std::make_unique<FlvTag>();
  std::unique_ptr<FlvAudioTag> audioTag;
  std::unique_ptr<FlvVideoTag> videoTag;

  // Tag type
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
    auto soundRateIndex = (tmp >> 2) & 0x3;
    if (soundRateIndex > 3) {
      throw std::runtime_error(fmt::format(
          "Unsupported audio sampling rate. soundRateIndex {}",
          soundRateIndex));
    }
    audioTag->soundRate =
        FlvReader::kAudioFreqSamplingIndexMapping[soundRateIndex];
    auto soundSizeIndex = (tmp >> 1) & 0x1;
    if (soundSizeIndex > 1) {
      throw std::runtime_error(fmt::format(
          "Unsupported audio bits per sample. soundSizeIndex {}",
          soundSizeIndex));
    }
    audioTag->soundSize = kAudioBitsPerSampleMapping[soundSizeIndex];
    auto soundTypeIndex = tmp & 0x1;
    if (soundTypeIndex > 1) {
      throw std::runtime_error(fmt::format(
          "Unsupported number of channels. soundSizeIndex {}", soundTypeIndex));
    }
    audioTag->soundType = kAudioChannels[soundTypeIndex];
    if (audioTag->soundFormat != 10 || audioTag->soundRate != 44100 ||
        audioTag->soundSize != 16 || audioTag->soundType != 2) {
      throw std::runtime_error(fmt::format(
          "Unsupported audio format, only AAC is supported. soundFormat {}, soundRate {}, soundSize {}, soundType {}",
          audioTag->soundFormat,
          audioTag->soundRate,
          audioTag->soundSize,
          audioTag->soundType));
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
  } else if (tag->type == 0x09) {
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
  } else {
    readBytes(tag->size);
  }

  return {
      std::move(tag),
      std::move(videoTag),
      std::move(audioTag),
  };
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
  const int64_t bytesToRead = 3;
  uint8_t tmp[bytesToRead];
  f_.read((char*)tmp, bytesToRead);
  if (f_.gcount() == bytesToRead && f_.rdstate() == std::ios::goodbit) {
    uint32_t tmpFin = 0;
    if (std::endian::native == std::endian::little) {
      tmpFin = (tmp[0] << 16 | tmp[1] << 8 | tmp[2]);
    } else {
      tmpFin = (tmp[2] << 16 | tmp[1] << 8 | tmp[0]);
    }
    ret = tmpFin;
  } else {
    throw std::runtime_error(fmt::format(
        "Failed to read 3 byte at offset {}", static_cast<int>(f_.tellg())));
  }
  return ret;
}

uint32_t FlvReader::read4Bytes() {
  uint32_t ret = 0;
  const int64_t bytesToRead = 4;
  uint8_t tmp[bytesToRead];
  f_.read((char*)tmp, bytesToRead);
  if (f_.gcount() == bytesToRead && f_.rdstate() == std::ios::goodbit) {
    uint32_t tmpFin = 0;
    if (std::endian::native == std::endian::little) {
      tmpFin = tmp[0] << 24 | tmp[1] << 16 | tmp[2] << 8 | tmp[3];
    } else {
      tmpFin = tmp[3] << 24 | tmp[2] << 16 | tmp[1] << 8 | tmp[0];
    }
    ret = tmpFin;
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
  if (n > std::numeric_limits<int64_t>::max()) {
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

} // namespace moxygen

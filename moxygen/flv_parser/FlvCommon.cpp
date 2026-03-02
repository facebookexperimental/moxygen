/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/flv_parser/FlvCommon.h"

namespace {
const uint32_t kAudioTagHeaderSize = 2;
const uint32_t kVideoTagHeaderSize = 5;
} // namespace

namespace moxygen::flv {

std::unique_ptr<flv::FlvScriptTag> createScriptTag(
    uint32_t ts,
    std::unique_ptr<folly::IOBuf> data) {
  flv::FlvTagBase tag(18, data->computeChainDataLength(), ts, 0);
  std::unique_ptr<flv::FlvScriptTag> scriptTag =
      std::make_unique<flv::FlvScriptTag>(tag, std::move(data));
  return scriptTag;
}

std::unique_ptr<flv::FlvVideoTag> createVideoTag(
    uint32_t ts,
    uint8_t frameType,
    uint8_t codecId,
    uint8_t avcPacketType,
    uint32_t compositionTime,
    std::unique_ptr<folly::IOBuf> data) {
  flv::FlvTagBase tag(9, data->computeChainDataLength() + 5, ts, 0);
  std::unique_ptr<flv::FlvVideoTag> videoTag =
      std::make_unique<flv::FlvVideoTag>(
          tag,
          frameType,
          codecId,
          avcPacketType,
          compositionTime & 0xFFFFFF,
          std::move(data));
  return videoTag;
}

std::unique_ptr<flv::FlvAudioTag> createAudioTag(
    uint32_t ts,
    uint8_t soundFormat,
    uint32_t soundRate,
    uint8_t soundSize,
    uint8_t soundType,
    uint8_t aacPacketType,
    std::unique_ptr<folly::IOBuf> data) {
  flv::FlvTagBase tag(8, data->computeChainDataLength() + 2, ts, 0);
  std::unique_ptr<flv::FlvAudioTag> audioTag =
      std::make_unique<flv::FlvAudioTag>(
          tag,
          soundFormat,
          soundRate,
          soundSize,
          soundType,
          aacPacketType,
          std::move(data));
  return audioTag;
}

flv::FlvTag createTag(
    std::unique_ptr<flv::FlvTagBase> tag,
    std::unique_ptr<folly::IOBuf> data) {
  std::unique_ptr<FlvAudioTag> audioTag;
  std::unique_ptr<FlvVideoTag> videoTag;
  std::unique_ptr<FlvScriptTag> scriptTag;

  auto cursor = folly::io::Cursor(data.get());

  if (tag->type == 0x08) {
    // Audio tag
    audioTag = std::make_unique<FlvAudioTag>(*tag);

    auto tmp = read1Byte(cursor);
    audioTag->soundFormat = (tmp >> 4) & 0x0f;
    audioTag->soundRate = (tmp >> 2) & 0x3;
    if (audioTag->soundRate > 3) {
      throw std::runtime_error(
          fmt::format(
              "Unsupported audio sampling rate. soundRateIndex {}",
              audioTag->soundRate));
    }
    // audioTag->soundRate = kAudioFreqSamplingIndexMapping[soundRateIndex];
    audioTag->soundSize = (tmp >> 1) & 0x1;
    if (audioTag->soundSize > 1) {
      throw std::runtime_error(
          fmt::format(
              "Unsupported audio bits per sample. soundSizeIndex {}",
              audioTag->soundSize));
    }
    // audioTag->soundSize = kAudioBitsPerSampleMapping[soundSizeIndex];
    audioTag->soundType = tmp & 0x1;
    if (audioTag->soundType > 1) {
      throw std::runtime_error(
          fmt::format(
              "Unsupported number of channels. soundSizeIndex {}",
              audioTag->soundType));
    }
    // audioTag->soundType = kAudioChannels[soundTypeIndex];
    if (audioTag->soundFormat != 10 || audioTag->soundRate != 3 ||
        audioTag->soundSize != 1 || audioTag->soundType != 1) {
      throw std::runtime_error(
          fmt::format(
              "Unsupported audio format, only AAC is supported. soundFormat {}, soundRate {}, soundSize {}, soundType {} (byte: {})",
              audioTag->soundFormat,
              audioTag->soundRate,
              audioTag->soundSize,
              audioTag->soundType,
              tmp));
    }

    audioTag->aacPacketType = read1Byte(cursor);
    if (audioTag->aacPacketType > 1) {
      throw std::runtime_error(
          fmt::format(
              "Unsupported AAC packet type. packetType {}",
              audioTag->aacPacketType));
    }

    // Read data
    if (tag->size > kAudioTagHeaderSize) {
      auto trimmed = std::make_unique<folly::IOBuf>();
      // Tag size includes the AudioTagHeader = 2 bytes which we have already
      // parsed. Extract the rest but trim any padding bytes.
      cursor.clone(trimmed, tag->size - kAudioTagHeaderSize);
      audioTag->data = std::move(trimmed);
    }
    return audioTag;
  }

  if (tag->type == 0x09) {
    // Video tag
    videoTag = std::make_unique<FlvVideoTag>(*tag);

    auto tmp = read1Byte(cursor);
    videoTag->frameType = (tmp >> 4) & 0x0f;
    videoTag->codecId = tmp & 0x0f;
    if (videoTag->codecId != 0x07) {
      throw std::runtime_error(
          fmt::format(
              "Unsupported video codec. Only h264 supported. CodecId {}",
              videoTag->codecId));
    }
    videoTag->avcPacketType = read1Byte(cursor);
    if (videoTag->avcPacketType > 2) {
      throw std::runtime_error(
          fmt::format(
              "Unsupported AVC packet type. packetType {}",
              videoTag->avcPacketType));
    }
    videoTag->compositionTime = read3Bytes(cursor);

    // Read data
    if (tag->size > kVideoTagHeaderSize) {
      auto trimmed = std::make_unique<folly::IOBuf>();
      // Tag size includes the VideoTagHeader which we have already
      // parsed. Extract the rest but trim any padding bytes.
      cursor.clone(trimmed, tag->size - kVideoTagHeaderSize);
      videoTag->data = std::move(trimmed);
    }

    return videoTag;
  }

  if (tag->type == 0x12) {
    // Script tag
    scriptTag = std::make_unique<FlvScriptTag>(*tag);
    // Read data
    if (tag->size > 0) {
      auto trimmed = std::make_unique<folly::IOBuf>();
      // No header to parse. Tag size indicates the payload size.
      cursor.clone(trimmed, tag->size);
      scriptTag->data = std::move(trimmed);
    }
    return scriptTag;
  }
  return FlvReadCmd::FLV_UNKNOWN_TAG;
}

std::unique_ptr<FlvTagBase> flvParseTagBase(folly::io::Cursor& cursor) {
  auto tag = std::make_unique<FlvTagBase>();
  tag->type = read1Byte(cursor);
  tag->size = read3Bytes(cursor);
  auto ltimestamp = read3Bytes(cursor);
  auto htimestamp = read1Byte(cursor);
  tag->timestamp = htimestamp << 24 | ltimestamp;
  tag->streamId = read3Bytes(cursor);
  return tag;
}

flv::FlvTag flvParse(std::unique_ptr<folly::IOBuf> data) {
  auto cursor = folly::io::Cursor(data.get());
  auto tag = flvParseTagBase(cursor);
  data->trimStart(11);
  return createTag(std::move(tag), std::move(data));
}

std::ostream& operator<<(std::ostream& os, flv::AscHeaderData const& v) {
  os << "ASC header. valid: " << v.valid << ", aot: " << v.aot
     << ", freqIndex: " << v.freqIndex << "(" << v.sampleFreq
     << "), channels: " << v.channels;
  return os;
}

AscHeaderData parseAscHeader(std::unique_ptr<folly::IOBuf> buf) {
  AscHeaderData ret;
  ret.valid = false;
  if (buf == nullptr) {
    return ret;
  }
  try {
    BitReader br(std::move(buf));

    ret.aot = br.getNextBits(5); // audioObjectType
    if (ret.aot == 31) {
      ret.aot = 32 + br.getNextBits(6);
    }

    ret.freqIndex = br.getNextBits(4); // sampleFrequencyIndex
    if (ret.freqIndex >= 0x0f) {
      ret.sampleFreq = br.getNextBits(24); // sampleFrequency
    } else {
      ret.sampleFreq = kAscFreqSamplingIndexMapping[ret.freqIndex];
    }
    ret.channels = br.getNextBits(4); // numChannels
    ret.valid = true;
  } catch (std::exception& _) {
    return ret;
  }
  return ret;
}

uint8_t getAscFreqIndex(uint32_t sampleFreq) {
  uint8_t fi = 0xff;
  for (uint8_t i = 0; i < 13; i++) {
    if (sampleFreq == kAscFreqSamplingIndexMapping[i]) {
      fi = i;
      break;
    }
  }
  return fi;
}

std::unique_ptr<folly::IOBuf>
createAscheader(uint8_t aot, uint32_t sampleFreq, uint8_t channels) {
  std::unique_ptr<folly::IOBuf> ret;
  uint8_t freqIndex = getAscFreqIndex(sampleFreq);
  if (freqIndex == 0xff) {
    uint8_t sampleFreqb0 = (sampleFreq >> 16) & 0xff;
    uint8_t sampleFreqb1 = (sampleFreq >> 8) & 0xff;
    uint8_t sampleFreqb2 = sampleFreq & 0xff;

    ret = folly::IOBuf::create(5);
    ret->writableData()[0] = (aot << 3) | 0b111;
    ret->writableData()[1] = 0b10000000 | sampleFreqb0 >> 1;
    ret->writableData()[2] = sampleFreqb0 << 7 | sampleFreqb1 >> 1;
    ret->writableData()[3] = sampleFreqb1 << 7 | sampleFreqb2 >> 1;
    ret->writableData()[4] = sampleFreqb2 << 7 | (channels & 0b1111) << 3;
    ret->append(5);
  } else {
    ret = folly::IOBuf::create(2);
    ret->writableData()[0] = (aot << 3) | (freqIndex & 0b1111) >> 1;
    ret->writableData()[1] =
        (freqIndex & 0b1111) << 7 | (channels & 0b1111) << 3;
    ret->append(2);
  }
  return ret;
}

uint8_t read1Byte(folly::io::Cursor& cursor) {
  return cursor.read<uint8_t>();
}

uint32_t read3Bytes(folly::io::Cursor& cursor) {
  uint32_t data = 0;
  cursor.pull(&data, 3);
  return folly::Endian::big(data << 8);
}

uint32_t read4Bytes(folly::io::Cursor& cursor) {
  return cursor.readBE<uint32_t>();
}

std::unique_ptr<folly::IOBuf> readNBytes(
    folly::io::Cursor& cursor,
    uint32_t size) {
  std::unique_ptr<folly::IOBuf> ret;
  cursor.clone(ret, size);

  return ret;
}

} // namespace moxygen::flv

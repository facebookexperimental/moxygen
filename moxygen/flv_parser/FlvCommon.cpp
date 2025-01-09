// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#include "moxygen/flv_parser/FlvCommon.h"

namespace moxygen::flv {

std::unique_ptr<flv::FlvScriptTag> createScriptTag(
    uint32_t ts,
    std::unique_ptr<folly::IOBuf> data) {
  flv::FlvTagBase tag(18, data->length(), ts, 0);
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
  flv::FlvTagBase tag(9, data->length() + 5, ts, 0);
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
  flv::FlvTagBase tag(8, data->length() + 2, ts, 0);
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

} // namespace moxygen::flv

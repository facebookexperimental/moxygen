/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/moq_mi/MoQMi.h"
#include <quic/codec/QuicInteger.h>

namespace moxygen {

std::unique_ptr<MoQMi::MoqMiObject> MoQMi::encodeToMoQMi(
    std::unique_ptr<MediaItem> item) noexcept {
  if (!item) {
    return nullptr;
  }
  auto ret = std::make_unique<MoQMi::MoqMiObject>(std::move(item->data));
  if (item->type == MediaType::VIDEO) {
    // Specify media type
    ret->extensions.emplace_back(
        folly::to_underlying(
            HeaderExtensionsTypeIDs::MOQ_EXT_HEADER_TYPE_MOQMI_MEDIA_TYPE),
        folly::to_underlying(
            HeaderExtensionMediaTypeValues::
                MOQ_EXT_HEADER_TYPE_MOQMI_MEDIA_VALUE_VIDEO_H264_IN_AVCC));

    // Add metadata
    auto extBuff = encodeMoqMiAVCCMetadata(*item);
    ret->extensions.emplace_back(
        folly::to_underlying(
            HeaderExtensionsTypeIDs::
                MOQ_EXT_HEADER_TYPE_MOQMI_VIDEO_H264_IN_AVCC_METADATA),
        std::move(extBuff));

    if (item->metadata) {
      // Add extradata (AVCDecoderConfigurationRecord)
      ret->extensions.emplace_back(
          folly::to_underlying(
              HeaderExtensionsTypeIDs::
                  MOQ_EXT_HEADER_TYPE_MOQMI_VIDEO_H264_IN_AVCC_EXTRADATA),
          std::move(item->metadata));
    }
  } else if (item->type == MediaType::AUDIO) {
    // Check codecType to determine which codec to use
    if (item->codecType == "opus") {
      // Specify media type for Opus
      ret->extensions.emplace_back(
          folly::to_underlying(
              HeaderExtensionsTypeIDs::MOQ_EXT_HEADER_TYPE_MOQMI_MEDIA_TYPE),
          folly::to_underlying(
              HeaderExtensionMediaTypeValues::
                  MOQ_EXT_HEADER_TYPE_MOQMI_MEDIA_VALUE_AUDIO_AUDIO_OPUS));

      // Add Opus metadata
      auto extBuff = encodeMoqMiOpusMetadata(*item);
      ret->extensions.emplace_back(
          folly::to_underlying(
              HeaderExtensionsTypeIDs::
                  MOQ_EXT_HEADER_TYPE_MOQMI_AUDIO_OPUS_METADATA),
          std::move(extBuff));
    } else {
      // Default to AAC for backwards compatibility
      // Specify media type
      ret->extensions.emplace_back(
          folly::to_underlying(
              HeaderExtensionsTypeIDs::MOQ_EXT_HEADER_TYPE_MOQMI_MEDIA_TYPE),
          folly::to_underlying(
              HeaderExtensionMediaTypeValues::
                  MOQ_EXT_HEADER_TYPE_MOQMI_MEDIA_VALUE_AUDIO_AUDIO_AACLC_MPEG4));

      // Add metadata
      auto extBuff = encodeMoqMiAACLCMetadata(*item);
      ret->extensions.emplace_back(
          folly::to_underlying(
              HeaderExtensionsTypeIDs::
                  MOQ_EXT_HEADER_TYPE_MOQMI_AUDIO_AACLC_MPEG4_METADATA),
          std::move(extBuff));
    }
  } else {
    // Unknown media type
    return nullptr;
  }
  return ret;
}

MoQMi::MoqMiItem MoQMi::decodeMoQMi(
    std::unique_ptr<MoQMi::MoqMiObject> obj) noexcept {
  if (!obj) {
    return MoQMi::MoqMiReadCmd::MOQMI_UNKNOWN;
  }
  HeaderExtensionMediaTypeValues mediaType = HeaderExtensionMediaTypeValues::
      MOQ_EXT_HEADER_TYPE_MOQMI_MEDIA_VALUE_UNKNOWN;
  std::unique_ptr<folly::IOBuf> extradata;
  std::unique_ptr<VideoH264AVCCWCPData> videoH264AVCCWCPData;
  std::unique_ptr<AudioAACMP4LCWCPData> audioAACMP4LCWCPData;
  std::unique_ptr<AudioOpusWCPData> audioOpusWCPData;

  // Parse extensions
  for (const Extension& ext : obj->extensions) {
    // Media type
    if (ext.type ==
        folly::to_underlying(
            HeaderExtensionsTypeIDs::MOQ_EXT_HEADER_TYPE_MOQMI_MEDIA_TYPE)) {
      if (mediaType !=
          HeaderExtensionMediaTypeValues::
              MOQ_EXT_HEADER_TYPE_MOQMI_MEDIA_VALUE_UNKNOWN) {
        // Multiple media types
        return MoQMi::MoqMiReadCmd::MOQMI_ERR;
      }
      if (ext.intValue ==
          folly::to_underlying(
              HeaderExtensionMediaTypeValues::
                  MOQ_EXT_HEADER_TYPE_MOQMI_MEDIA_VALUE_VIDEO_H264_IN_AVCC)) {
        mediaType = HeaderExtensionMediaTypeValues::
            MOQ_EXT_HEADER_TYPE_MOQMI_MEDIA_VALUE_VIDEO_H264_IN_AVCC;
      } else if (
          ext.intValue ==
          folly::to_underlying(
              HeaderExtensionMediaTypeValues::
                  MOQ_EXT_HEADER_TYPE_MOQMI_MEDIA_VALUE_AUDIO_AUDIO_AACLC_MPEG4)) {
        mediaType = HeaderExtensionMediaTypeValues::
            MOQ_EXT_HEADER_TYPE_MOQMI_MEDIA_VALUE_AUDIO_AUDIO_AACLC_MPEG4;
      } else if (
          ext.intValue ==
          folly::to_underlying(
              HeaderExtensionMediaTypeValues::
                  MOQ_EXT_HEADER_TYPE_MOQMI_MEDIA_VALUE_AUDIO_AUDIO_OPUS)) {
        mediaType = HeaderExtensionMediaTypeValues::
            MOQ_EXT_HEADER_TYPE_MOQMI_MEDIA_VALUE_AUDIO_AUDIO_OPUS;
      } else {
        // Unknown media type
        return MoQMi::MoqMiReadCmd::MOQMI_ERR;
      }
    }

    // Extradata
    if (ext.type ==
        folly::to_underlying(
            HeaderExtensionsTypeIDs::
                MOQ_EXT_HEADER_TYPE_MOQMI_VIDEO_H264_IN_AVCC_EXTRADATA)) {
      if (extradata) {
        // Multiple extradata
        return MoQMi::MoqMiReadCmd::MOQMI_ERR;
      }
      if (!ext.arrayValue) {
        // AVCC extradata empty
        return MoQMi::MoqMiReadCmd::MOQMI_ERR;
      }
      extradata = ext.arrayValue->clone();
    }

    // Metadata AVCC
    if (ext.type ==
        folly::to_underlying(
            HeaderExtensionsTypeIDs::
                MOQ_EXT_HEADER_TYPE_MOQMI_VIDEO_H264_IN_AVCC_METADATA)) {
      if (videoH264AVCCWCPData) {
        // Multiple AVCC metadata
        return MoQMi::MoqMiReadCmd::MOQMI_ERR;
      }
      if (ext.arrayValue == nullptr) {
        // Empty AVCC metadata
        return MoQMi::MoqMiReadCmd::MOQMI_ERR;
      }
      videoH264AVCCWCPData = MoQMi::decodeMoqMiAVCCMetadata(*ext.arrayValue);
      if (!videoH264AVCCWCPData) {
        // Error parsing h264 AVCC metadata
        return MoQMi::MoqMiReadCmd::MOQMI_ERR;
      }
    }

    // Metadata AAC
    if (ext.type ==
        folly::to_underlying(
            HeaderExtensionsTypeIDs::
                MOQ_EXT_HEADER_TYPE_MOQMI_AUDIO_AACLC_MPEG4_METADATA)) {
      if (audioAACMP4LCWCPData) {
        // Multiple AACLC metadata
        return MoQMi::MoqMiReadCmd::MOQMI_ERR;
      }
      if (ext.arrayValue == nullptr) {
        // Empty AACLC metadata
        return MoQMi::MoqMiReadCmd::MOQMI_ERR;
      }
      audioAACMP4LCWCPData = decodeMoqMiAACLCMetadata(*ext.arrayValue);
      if (!audioAACMP4LCWCPData) {
        // Error parsing AACLC metadata
        return MoQMi::MoqMiReadCmd::MOQMI_ERR;
      }
    }

    // Metadata Opus
    if (ext.type ==
        folly::to_underlying(
            HeaderExtensionsTypeIDs::
                MOQ_EXT_HEADER_TYPE_MOQMI_AUDIO_OPUS_METADATA)) {
      if (audioOpusWCPData) {
        // Multiple Opus metadata
        return MoQMi::MoqMiReadCmd::MOQMI_ERR;
      }
      if (ext.arrayValue == nullptr) {
        // Empty Opus metadata
        return MoQMi::MoqMiReadCmd::MOQMI_ERR;
      }
      audioOpusWCPData = decodeMoqMiOpusMetadata(*ext.arrayValue);
      if (!audioOpusWCPData) {
        // Error parsing Opus metadata
        return MoQMi::MoqMiReadCmd::MOQMI_ERR;
      }
    }
  }

  // Compose return value
  if (mediaType ==
      HeaderExtensionMediaTypeValues::
          MOQ_EXT_HEADER_TYPE_MOQMI_MEDIA_VALUE_VIDEO_H264_IN_AVCC) {
    if (!videoH264AVCCWCPData) {
      return MoQMi::MoqMiReadCmd::MOQMI_ERR;
    }
    videoH264AVCCWCPData->data = std::move(obj->payload);
    videoH264AVCCWCPData->metadata = std::move(extradata);
    return videoH264AVCCWCPData;
  }
  if (mediaType ==
      HeaderExtensionMediaTypeValues::
          MOQ_EXT_HEADER_TYPE_MOQMI_MEDIA_VALUE_AUDIO_AUDIO_AACLC_MPEG4) {
    if (!audioAACMP4LCWCPData) {
      return MoQMi::MoqMiReadCmd::MOQMI_ERR;
    }
    audioAACMP4LCWCPData->data = std::move(obj->payload);
    return audioAACMP4LCWCPData;
  }
  if (mediaType ==
      HeaderExtensionMediaTypeValues::
          MOQ_EXT_HEADER_TYPE_MOQMI_MEDIA_VALUE_AUDIO_AUDIO_OPUS) {
    if (!audioOpusWCPData) {
      return MoQMi::MoqMiReadCmd::MOQMI_ERR;
    }
    audioOpusWCPData->data = std::move(obj->payload);
    return audioOpusWCPData;
  }
  return MoQMi::MoqMiReadCmd::MOQMI_ERR;
}

std::unique_ptr<folly::IOBuf> MoQMi::encodeMoqMiAVCCMetadata(
    const MediaItem& item) noexcept {
  folly::IOBufQueue buffQueue{folly::IOBufQueue::cacheChainLength()};
  size_t size = 0;
  bool error = false;

  writeVarint(buffQueue, item.id, size, error);
  writeVarint(buffQueue, item.pts, size, error);
  writeVarint(buffQueue, item.dts, size, error);
  writeVarint(buffQueue, item.timescale, size, error);
  writeVarint(buffQueue, item.duration, size, error);
  writeVarint(buffQueue, item.wallclock, size, error);
  if (error) {
    return nullptr;
  }
  return buffQueue.move();
}

std::unique_ptr<folly::IOBuf> MoQMi::encodeMoqMiAACLCMetadata(
    const MediaItem& item) noexcept {
  folly::IOBufQueue buffQueue{folly::IOBufQueue::cacheChainLength()};
  size_t size = 0;
  bool error = false;

  writeVarint(buffQueue, item.id, size, error);
  writeVarint(buffQueue, item.pts, size, error);
  writeVarint(buffQueue, item.timescale, size, error);
  writeVarint(buffQueue, item.sampleFreq, size, error);
  writeVarint(buffQueue, item.numChannels, size, error);
  writeVarint(buffQueue, item.duration, size, error);
  writeVarint(buffQueue, item.wallclock, size, error);
  if (error) {
    return nullptr;
  }
  return buffQueue.move();
}

std::unique_ptr<folly::IOBuf> MoQMi::encodeMoqMiOpusMetadata(
    const MediaItem& item) noexcept {
  folly::IOBufQueue buffQueue{folly::IOBufQueue::cacheChainLength()};
  size_t size = 0;
  bool error = false;

  writeVarint(buffQueue, item.id, size, error);
  writeVarint(buffQueue, item.pts, size, error);
  writeVarint(buffQueue, item.timescale, size, error);
  writeVarint(buffQueue, item.sampleFreq, size, error);
  writeVarint(buffQueue, item.numChannels, size, error);
  writeVarint(buffQueue, item.duration, size, error);
  writeVarint(buffQueue, item.wallclock, size, error);
  if (error) {
    return nullptr;
  }
  return buffQueue.move();
}

std::unique_ptr<MoQMi::VideoH264AVCCWCPData> MoQMi::decodeMoqMiAVCCMetadata(
    const folly::IOBuf& extValue) noexcept {
  folly::io::Cursor cursor(&extValue);
  auto seqId = quic::follyutils::decodeQuicInteger(cursor);
  if (!seqId) {
    return nullptr;
  }
  auto pts = quic::follyutils::decodeQuicInteger(cursor);
  if (!pts) {
    return nullptr;
  }
  auto dts = quic::follyutils::decodeQuicInteger(cursor);
  if (!dts) {
    return nullptr;
  }
  auto timescale = quic::follyutils::decodeQuicInteger(cursor);
  if (!timescale) {
    return nullptr;
  }
  auto duration = quic::follyutils::decodeQuicInteger(cursor);
  if (!duration) {
    return nullptr;
  }
  auto wallclock = quic::follyutils::decodeQuicInteger(cursor);
  if (!wallclock) {
    return nullptr;
  }
  return std::make_unique<VideoH264AVCCWCPData>(
      seqId->first,
      pts->first,
      timescale->first,
      duration->first,
      wallclock->first,
      nullptr,
      nullptr,
      dts->first);
}

std::unique_ptr<MoQMi::AudioAACMP4LCWCPData> MoQMi::decodeMoqMiAACLCMetadata(
    const folly::IOBuf& extValue) noexcept {
  folly::io::Cursor cursor(&extValue);
  auto seqId = quic::follyutils::decodeQuicInteger(cursor);
  if (!seqId) {
    return nullptr;
  }
  auto pts = quic::follyutils::decodeQuicInteger(cursor);
  if (!pts) {
    return nullptr;
  }
  auto timescale = quic::follyutils::decodeQuicInteger(cursor);
  if (!timescale) {
    return nullptr;
  }
  auto sampleFreq = quic::follyutils::decodeQuicInteger(cursor);
  if (!sampleFreq) {
    return nullptr;
  }
  auto numChannels = quic::follyutils::decodeQuicInteger(cursor);
  if (!numChannels) {
    return nullptr;
  }
  auto duration = quic::follyutils::decodeQuicInteger(cursor);
  if (!duration) {
    return nullptr;
  }
  auto wallclock = quic::follyutils::decodeQuicInteger(cursor);
  if (!wallclock) {
    return nullptr;
  }
  return std::make_unique<AudioAACMP4LCWCPData>(
      seqId->first,
      pts->first,
      timescale->first,
      duration->first,
      wallclock->first,
      nullptr,
      sampleFreq->first,
      numChannels->first);
}

std::unique_ptr<MoQMi::AudioOpusWCPData> MoQMi::decodeMoqMiOpusMetadata(
    const folly::IOBuf& extValue) noexcept {
  folly::io::Cursor cursor(&extValue);
  auto seqId = quic::follyutils::decodeQuicInteger(cursor);
  if (!seqId) {
    return nullptr;
  }
  auto pts = quic::follyutils::decodeQuicInteger(cursor);
  if (!pts) {
    return nullptr;
  }
  auto timescale = quic::follyutils::decodeQuicInteger(cursor);
  if (!timescale) {
    return nullptr;
  }
  auto sampleFreq = quic::follyutils::decodeQuicInteger(cursor);
  if (!sampleFreq) {
    return nullptr;
  }
  auto numChannels = quic::follyutils::decodeQuicInteger(cursor);
  if (!numChannels) {
    return nullptr;
  }
  auto duration = quic::follyutils::decodeQuicInteger(cursor);
  if (!duration) {
    return nullptr;
  }
  auto wallclock = quic::follyutils::decodeQuicInteger(cursor);
  if (!wallclock) {
    return nullptr;
  }
  return std::make_unique<AudioOpusWCPData>(
      seqId->first,
      pts->first,
      timescale->first,
      duration->first,
      wallclock->first,
      nullptr,
      sampleFreq->first,
      numChannels->first);
}

void MoQMi::writeBuffer(
    folly::IOBufQueue& buf,
    std::unique_ptr<folly::IOBuf> data,
    size_t& size,
    bool& error) noexcept {
  if (error || data == nullptr) {
    return;
  }
  size += data->computeChainDataLength();
  buf.append(std::move(data));
}

void MoQMi::writeVarint(
    folly::IOBufQueue& buf,
    uint64_t value,
    size_t& size,
    bool& error) noexcept {
  if (error) {
    return;
  }
  folly::io::QueueAppender appender(&buf, MoQMi::kMaxQuicIntSize);
  auto appenderOp = [appender = std::move(appender)](auto val) mutable {
    appender.writeBE(val);
  };
  auto res = quic::encodeQuicInteger(value, appenderOp);
  if (res.hasError()) {
    error = true;
  } else {
    size += *res;
  }
}

std::ostream& operator<<(
    std::ostream& os,
    MoQMi::VideoH264AVCCWCPData const& v) {
  auto metadataSize =
      v.metadata != nullptr ? v.metadata->computeChainDataLength() : 0;
  auto dataSize = v.data != nullptr ? v.data->computeChainDataLength() : 0;
  os << "VideoH264. id: " << v.seqId << ", pts: " << v.pts << ", dts: " << v.dts
     << ", timescale: " << v.timescale << ", duration: " << v.duration
     << ", wallclock: " << v.wallclock << ", metadata length: " << metadataSize
     << ", data length: " << dataSize;
  return os;
}

std::ostream& operator<<(
    std::ostream& os,
    MoQMi::AudioAACMP4LCWCPData const& a) {
  auto dataSize = a.data != nullptr ? a.data->computeChainDataLength() : 0;
  os << "AudioAAC. id: " << a.seqId << ", pts: " << a.pts
     << ", sampleFreq: " << a.sampleFreq << ", numChannels: " << a.numChannels
     << ", timescale: " << a.timescale << ", duration: " << a.duration
     << ", wallclock: " << a.wallclock << ", data length: " << dataSize;
  return os;
}

std::ostream& operator<<(std::ostream& os, MoQMi::AudioOpusWCPData const& o) {
  auto dataSize = o.data != nullptr ? o.data->computeChainDataLength() : 0;
  os << "AudioOpus. id: " << o.seqId << ", pts: " << o.pts
     << ", sampleFreq: " << o.sampleFreq << ", numChannels: " << o.numChannels
     << ", timescale: " << o.timescale << ", duration: " << o.duration
     << ", wallclock: " << o.wallclock << ", data length: " << dataSize;
  return os;
}

} // namespace moxygen

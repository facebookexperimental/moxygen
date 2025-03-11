/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/io/Cursor.h>
#include <folly/io/IOBuf.h>
#include <folly/io/IOBufQueue.h>
#include <gtest/gtest.h>
#include <moxygen/moq_mi/MediaItem.h>
#include <cstdint>
#include <variant>
#include "moxygen/MoQFramer.h"
#include "moxygen/flv_parser/FlvCommon.h"

namespace moxygen {

class MoQMi {
 public:
  enum class PayloadType : uint64_t {
    VideoH264AVCCWCP = 0x0,
    AudioOpusWCP = 0x1,
    RAW = 0x2,
    AudioAACMP4LCWCP = 0x3,
  };

  enum class HeaderExtensionsTypeIDs : uint64_t {
    MOQ_EXT_HEADER_TYPE_MOQMI_MEDIA_TYPE = 0x0A,
    MOQ_EXT_HEADER_TYPE_MOQMI_VIDEO_H264_IN_AVCC_METADATA = 0x0B,
    MOQ_EXT_HEADER_TYPE_MOQMI_VIDEO_H264_IN_AVCC_EXTRADATA = 0x0D,
    MOQ_EXT_HEADER_TYPE_MOQMI_AUDIO_AACLC_MPEG4_METADATA = 0x13,
  };
  enum class HeaderExtensionMediaTypeValues : uint64_t {
    MOQ_EXT_HEADER_TYPE_MOQMI_MEDIA_VALUE_VIDEO_H264_IN_AVCC = 0x00,
    MOQ_EXT_HEADER_TYPE_MOQMI_MEDIA_VALUE_AUDIO_AUDIO_AACLC_MPEG4 = 0x03,
    MOQ_EXT_HEADER_TYPE_MOQMI_MEDIA_VALUE_UNKNOWN = 0xFF,
  };

  enum MoqMiReadCmd { MOQMI_EOF = 0xff, MOQMI_UNKNOWN = 0x1, MOQMI_ERR = 0xff };

  struct CommonData {
    std::unique_ptr<folly::IOBuf> data;
    uint64_t seqId;
    uint64_t pts;
    uint64_t timescale;
    uint64_t duration;
    uint64_t wallclock;
    CommonData()
        : data(nullptr),
          seqId(0),
          pts(0),
          timescale(0),
          duration(0),
          wallclock(0) {}
    CommonData(
        uint64_t seqId,
        uint64_t pts,
        uint64_t timescale,
        uint64_t duration,
        uint64_t wallclock,
        std::unique_ptr<folly::IOBuf> data)
        : data(std::move(data)),
          seqId(seqId),
          pts(pts),
          timescale(timescale),
          duration(duration),
          wallclock(wallclock) {}
  };

  struct VideoH264AVCCWCPData : public CommonData {
    std::unique_ptr<folly::IOBuf> metadata;
    uint64_t dts;
    VideoH264AVCCWCPData() : CommonData(), metadata(nullptr), dts(0) {}
    VideoH264AVCCWCPData(
        uint64_t seqId,
        uint64_t pts,
        uint64_t timescale,
        uint64_t duration,
        uint64_t wallclock,
        std::unique_ptr<folly::IOBuf> data,
        std::unique_ptr<folly::IOBuf> metadata,
        uint64_t dts)
        : CommonData(
              seqId,
              pts,
              timescale,
              duration,
              wallclock,
              std::move(data)),
          metadata(std::move(metadata)),
          dts(dts) {}

    friend std::ostream& operator<<(
        std::ostream& os,
        const VideoH264AVCCWCPData& v);

    bool isIdr() const {
      bool ret = false;
      // Assuming AVCC 4bytes NALU header
      if (data == nullptr) {
        return ret;
      }

      folly::IOBufQueue queue{folly::IOBufQueue::cacheChainLength()};
      queue.append(*data);
      folly::io::Cursor cursor(queue.front());

      // We expect h264 payload to be in AVCC format and the NALUs to be
      // prefixed by 4 bytes field representing length of the NALU
      int32_t naluLength = 0;
      while (cursor.canAdvance(sizeof(naluLength)) &&
             cursor.tryReadBE(naluLength)) {
        if (naluLength == 0 || !cursor.canAdvance(naluLength)) {
          break;
        }
        uint8_t nh = cursor.read<uint8_t>();
        cursor.retreat(1);

        if ((nh & 0x1f) == 5) {
          // IDR
          ret = true;
          break;
        } else {
          cursor.skip(naluLength);
        }
      }
      return ret;
    }
  };

  struct AudioAACMP4LCWCPData : public CommonData {
    uint64_t sampleFreq;
    uint64_t numChannels;
    AudioAACMP4LCWCPData() : CommonData(), sampleFreq(0), numChannels(0) {}
    AudioAACMP4LCWCPData(
        uint64_t seqId,
        uint64_t pts,
        uint64_t timescale,
        uint64_t duration,
        uint64_t wallclock,
        std::unique_ptr<folly::IOBuf> data,
        uint64_t sampleFreq,
        uint64_t numChannels)
        : CommonData(
              seqId,
              pts,
              timescale,
              duration,
              wallclock,
              std::move(data)),
          sampleFreq(sampleFreq),
          numChannels(numChannels) {}

    friend std::ostream& operator<<(
        std::ostream& os,
        const AudioAACMP4LCWCPData& a);

    std::unique_ptr<folly::IOBuf> getAscHeader() {
      return flv::createAscheader(2, sampleFreq, numChannels);
    }
  };

  struct MoqMiObject {
    std::vector<Extension> extensions;
    std::unique_ptr<folly::IOBuf> payload;
    MoqMiObject() : extensions(), payload(nullptr) {}
    explicit MoqMiObject(std::unique_ptr<folly::IOBuf> payload)
        : extensions(), payload(std::move(payload)) {}
    MoqMiObject(
        std::vector<Extension> extensions,
        std::unique_ptr<folly::IOBuf> payload)
        : extensions(std::move(extensions)), payload(std::move(payload)) {}
  };

  enum MoqMIItemTypeIndex {
    MOQMI_ITEM_INDEX_VIDEO_H264_AVC = 0,
    MOQMI_ITEM_INDEX_AUDIO_AAC_LC = 1,
    MOQMI_ITEM_INDEX_READCMD = 2
  };
  using MoqMiItem = std::variant<
      std::unique_ptr<MoQMi::VideoH264AVCCWCPData>,
      std::unique_ptr<MoQMi::AudioAACMP4LCWCPData>,
      MoqMiReadCmd>;

  explicit MoQMi() {}
  virtual ~MoQMi() {}

  static MoqMiItem decodeMoQMi(std::unique_ptr<MoqMiObject> obj) noexcept;
  static std::unique_ptr<MoqMiObject> encodeToMoQMi(
      std::unique_ptr<MediaItem> item) noexcept;

 private:
  static const size_t kMaxQuicIntSize = 32;

  static std::unique_ptr<folly::IOBuf> encodeMoqMiAVCCMetadata(
      const MediaItem& item) noexcept;
  static std::unique_ptr<folly::IOBuf> encodeMoqMiAACLCMetadata(
      const MediaItem& item) noexcept;

  static std::unique_ptr<MoQMi::VideoH264AVCCWCPData> decodeMoqMiAVCCMetadata(
      const std::vector<uint8_t>& extValue) noexcept;
  static std::unique_ptr<MoQMi::AudioAACMP4LCWCPData> decodeMoqMiAACLCMetadata(
      const std::vector<uint8_t>& extValue) noexcept;

  static void writeVarint(
      folly::IOBufQueue& buf,
      uint64_t value,
      size_t& size,
      bool& error) noexcept;

  static void writeBuffer(
      folly::IOBufQueue& buf,
      std::unique_ptr<folly::IOBuf> data,
      size_t& size,
      bool& error) noexcept;

  static std::vector<uint8_t> IOBufToVector(
      std::unique_ptr<folly::IOBuf> data) noexcept;
  static std::unique_ptr<folly::IOBuf> VectorToIOBuf(
      const std::vector<uint8_t>& data) noexcept;

  FRIEND_TEST(MoQMiTest, EncodeVideoH264TestNoMetadata);
  FRIEND_TEST(MoQMiTest, EncodeVideoH264TestWithMetadata);
  FRIEND_TEST(MoQMiTest, EncodeAudioAAC);
  FRIEND_TEST(MoQMiTest, DecodeVideoH264TestWithExtradata);
  FRIEND_TEST(MoQMiTest, DecodeVideoH264TestNoMetadata);
  FRIEND_TEST(MoQMiTest, DecodeAudioAAC);
};

} // namespace moxygen

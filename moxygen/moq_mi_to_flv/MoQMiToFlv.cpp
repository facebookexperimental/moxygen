/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/moq_mi_to_flv/MoQMiToFlv.h"
#include <folly/logging/xlog.h>

namespace moxygen {

std::list<flv::FlvTag> MoQMiToFlv::MoQMiToFlvPayload(
    MoQMi::MoqMiItem moqMiItem) {
  std::list<flv::FlvTag> ret;
  if (moqMiItem.index() ==
      MoQMi::MoqMIItemTypeIndex::MOQMI_ITEM_INDEX_READCMD) {
    return ret;
  }

  if (moqMiItem.index() ==
      MoQMi::MoqMIItemTypeIndex::MOQMI_ITEM_INDEX_VIDEO_H264_AVC) {
    auto moqv = std::move(
        std::get<MoQMi::MoqMIItemTypeIndex::MOQMI_ITEM_INDEX_VIDEO_H264_AVC>(
            moqMiItem));

    // Since PTS >= DTS lets check 1st PTS for rollover
    uint32_t pts =
        static_cast<int32_t>(moqv->pts & 0x7FFFFFFF); // 31 lower bits
    uint32_t dts =
        static_cast<int32_t>(moqv->dts & 0x7FFFFFFF); // 31 lower bits
    // This already handles the case where PTS rolled over and DTS NOT yet
    int32_t compositionTime = pts - dts;
    if (moqv->pts > std::numeric_limits<int32_t>::max()) {
      XLOG_EVERY_N(WARNING, 1000)
          << "PTS video truncated! Rolling over. From " << moqv->pts
          << ", to: " << pts << ", compositionTime: " << compositionTime;
    }
    CHECK_GE(compositionTime, 0);

    if (moqv->metadata != nullptr && !videoHeaderSeen_) {
      XLOG(INFO) << "Writing video header";
      auto vhtag = flv::createVideoTag(
          pts, 1, 7, 0, compositionTime, std::move(moqv->metadata));
      if (vhtag) {
        ret.emplace_back(std::move(vhtag));
      }
      videoHeaderSeen_ = true;
    }
    bool isIdr = moqv->isIdr();
    if (videoHeaderSeen_ && moqv->data != nullptr &&
        moqv->data->computeChainDataLength() > 0) {
      if ((!firstIDRSeen_ && isIdr) || firstIDRSeen_) {
        // Write frame
        uint8_t frameType = isIdr ? 1 : 0;
        XLOG(DBG1) << "Writing video frame, type: " << frameType;
        auto vtag = flv::createVideoTag(
            static_cast<uint32_t>(moqv->pts),
            frameType,
            7,
            1,
            compositionTime,
            std::move(moqv->data));
        if (vtag) {
          ret.emplace_back(std::move(vtag));

          if (isIdr && !firstIDRSeen_) {
            firstIDRSeen_ = true;
            XLOG(INFO) << "Wrote first IDR frame";
          }
        }
      }
    }
  } else if (
      moqMiItem.index() ==
      MoQMi::MoqMIItemTypeIndex::MOQMI_ITEM_INDEX_AUDIO_AAC_LC) {
    auto moqa = std::move(
        std::get<MoQMi::MoqMIItemTypeIndex::MOQMI_ITEM_INDEX_AUDIO_AAC_LC>(
            moqMiItem));
    if (!audioHeaderSeen_) {
      XLOG(INFO) << "Writing audio header";
      auto ascHeader = moqa->getAscHeader();
      uint32_t pts =
          static_cast<int32_t>(moqa->pts & 0x7FFFFFFF); // 31 lower bits
      if (moqa->pts > std::numeric_limits<int32_t>::max()) {
        XLOG_EVERY_N(WARNING, 1000)
            << "PTS audio truncated! Rolling over. From " << moqa->pts
            << ", to: " << pts;
      }
      auto ahtag =
          flv::createAudioTag(pts, 10, 3, 1, 1, 0, std::move(ascHeader));
      if (ahtag) {
        ret.emplace_back(std::move(ahtag));
      }
      audioHeaderSeen_ = true;
    }
    if (audioHeaderSeen_) {
      XLOG(DBG1) << "Writing audio frame";
      auto atag = flv::createAudioTag(
          static_cast<uint32_t>(moqa->pts),
          10,
          3,
          1,
          1,
          1,
          std::move(moqa->data));
      if (atag) {
        ret.emplace_back(std::move(atag));
      }
    }
  }
  return ret;
};

} // namespace moxygen

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

    auto flv_pts = convertTsToFlv(moqv->pts, moqv->timescale);
    auto flv_dts = convertTsToFlv(moqv->dts, moqv->timescale);

    int32_t compositionTime = flv_pts - flv_dts;
    CHECK_GE(compositionTime, 0);

    if (moqv->metadata != nullptr && !videoHeader_) {
      XLOG(INFO) << "Writing video header";
      videoHeader_ = flv::createVideoTag(
          flv_pts, 1, 7, 0, compositionTime, std::move(moqv->metadata));
      if (videoHeader_) {
        saveAndWriteHeaderIfNeeded(ret, true);
      }
    }
    if (moqv->data != nullptr && moqv->data->computeChainDataLength() > 0) {
      if ((!writeHeadersFirst_ && videoHeader_) ||
          (writeHeadersFirst_ && headersWritten_)) {
        bool isIdr = moqv->isIdr();
        if ((!firstIDRSeen_ && isIdr) || firstIDRSeen_) {
          // Write frame
          uint8_t frameType = isIdr ? 1 : 0;
          XLOG(DBG1) << "Writing video frame, type: " << frameType;
          auto flv_ts = convertTsToFlv(moqv->pts, moqv->timescale);
          auto vtag = flv::createVideoTag(
              flv_ts, frameType, 7, 1, compositionTime, std::move(moqv->data));
          if (vtag) {
            ret.emplace_back(std::move(vtag));

            if (isIdr && !firstIDRSeen_) {
              firstIDRSeen_ = true;
              XLOG(INFO) << "Wrote first IDR frame";
            }
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
    if (!audioHeader_) {
      XLOG(INFO) << "Writing audio header";
      auto ascHeader = moqa->getAscHeader();
      auto flv_ts = convertTsToFlv(moqa->pts, moqa->timescale);
      audioHeader_ =
          flv::createAudioTag(flv_ts, 10, 3, 1, 1, 0, ascHeader->clone());
      if (audioHeader_) {
        saveAndWriteHeaderIfNeeded(ret, false);
      }
    }
    if ((!writeHeadersFirst_ && audioHeader_) ||
        (writeHeadersFirst_ && headersWritten_)) {
      XLOG(DBG1) << "Writing audio frame";
      auto flv_ts = convertTsToFlv(moqa->pts, moqa->timescale);
      auto atag =
          flv::createAudioTag(flv_ts, 10, 3, 1, 1, 1, std::move(moqa->data));
      if (atag) {
        ret.emplace_back(std::move(atag));
      }
    }
  }
  return ret;
}

const uint32_t MoQMiToFlv::convertTsToFlv(uint64_t ts, uint64_t timescale) {
  uint64_t flv_ts = static_cast<uint64_t>((ts * 1000) / timescale);
  uint32_t final_ts =
      static_cast<int32_t>(flv_ts & 0x7FFFFFFF); // 31 lower bits
  if (flv_ts > std::numeric_limits<int32_t>::max()) {
    XLOG_EVERY_N(WARNING, 1000) << "TS truncated! Rolling over. From " << flv_ts
                                << ", to: " << final_ts;
  }
  return final_ts;
}

void MoQMiToFlv::saveAndWriteHeaderIfNeeded(
    std::list<flv::FlvTag>& ret,
    bool isVideoHeader) {
  if (!writeHeadersFirst_) {
    if (isVideoHeader) {
      ret.emplace_back(videoHeader_->clone());
    } else {
      ret.emplace_back(audioHeader_->clone());
    }
  } else {
    if (videoHeader_ && audioHeader_ && !headersWritten_) {
      ret.emplace_back(videoHeader_->clone());
      ret.emplace_back(audioHeader_->clone());
      headersWritten_ = true;
    }
  }
}
} // namespace moxygen

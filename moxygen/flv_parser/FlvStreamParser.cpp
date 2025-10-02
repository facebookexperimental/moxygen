/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/flv_parser/FlvStreamParser.h"
#include <folly/logging/xlog.h>
#include <chrono>

namespace moxygen::flv {

std::unique_ptr<FlvStreamParser::MediaItem> FlvStreamParser::parse(
    FlvTag& tag) {
  std::unique_ptr<MediaItem> ret = nullptr;
  XLOG(DBG1) << __func__;
  std::unique_ptr<MediaItem> localItem = std::make_unique<MediaItem>();

  if (tag.index() == FlvTagTypeIndex::FLV_TAG_INDEX_READCMD) {
    auto readsCmd = std::get<flv::FlvReadCmd>(tag);
    if (readsCmd == flv::FlvReadCmd::FLV_EOF) {
      XLOG(DBG1) << "End of flv file";
      localItem->isEOF = true;
    } else if (readsCmd == flv::FlvReadCmd::FLV_UNKNOWN_TAG) {
      XLOG(WARNING) << "Unknown tag";
    }
    return localItem;
  }

  // Set FLV timebase
  localItem->timescale = kFlvTimeScale;
  // Set wallclock here (NOT exact, but close enough in real live video)
  localItem->wallclock =
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::system_clock::now().time_since_epoch())
          .count();

  if (tag.index() == FlvTagTypeIndex::FLV_TAG_INDEX_VIDEO) {
    XLOG(DBG1) << "Read tag VIDEO at frame " << videoFrameId_;
    localItem->type = MediaType::VIDEO;
    localItem->id = videoFrameId_;

    auto videoTag =
        std::move(std::get<FlvTagTypeIndex::FLV_TAG_INDEX_VIDEO>(tag));

    // Not B frames for now
    localItem->pts = localItem->dts = videoTag->timestamp;
    localItem->duration = 0;
    if (videoTag->codecId != 7) {
      XLOG(WARN) << "Not supported video tag codecID: " << videoTag->codecId
                 << ", VideoSize: " << videoTag->size;

      return nullptr;
    }

    // Update last pts (to calculate duration)
    if (lastVideoPts_) {
      if (videoTag->timestamp < lastVideoPts_.value()) {
        XLOG(WARN)
            << "Video pts out of order, this could indicate B frames present (not supported for now) at: "
            << videoTag->timestamp;
        return nullptr;
      } else {
        localItem->duration = videoTag->timestamp - lastVideoPts_.value();
      }
    }
    lastVideoPts_ = videoTag->timestamp;

    if (videoTag->avcPacketType == 0x0) {
      // Update video metadata (AVCDecoderRecord)
      XLOG(DBG1) << "Saved AVCDecoderRecord header, size: " << videoTag->size;
      avcDecoderRecord_ = std::move(videoTag->data);
    } else if (videoTag->avcPacketType == 0x1) {
      localItem->isIdr = videoTag->frameType == 1;
      if (localItem->isIdr && avcDecoderRecord_) {
        // Add metadata on IDR
        XLOG(DBG1) << "Added AVCDecoderRecord header";
        localItem->metadata = avcDecoderRecord_->clone();
      }
      localItem->data = std::move(videoTag->data);
      videoFrameId_++;

      // Return the item
      ret = std::move(localItem);
    }
  } else if (tag.index() == FlvTagTypeIndex::FLV_TAG_INDEX_AUDIO) {
    XLOG(DBG1) << "Read tag AUDIO at frame " << audioFrameId_;
    localItem->type = MediaType::AUDIO;
    localItem->id = audioFrameId_;

    auto audioTag =
        std::move(std::get<FlvTagTypeIndex::FLV_TAG_INDEX_AUDIO>(tag));

    if (audioTag->soundFormat != 10) {
      XLOG(WARN) << "Not supported audio format codecID: "
                 << audioTag->soundFormat << ". AudioSize: " << audioTag->size;
      return nullptr;
    }

    localItem->pts = localItem->dts = audioTag->timestamp;
    localItem->duration = 0;
    if (lastAudioPts_) {
      if (audioTag->timestamp < lastAudioPts_.value()) {
        XLOG(ERR) << "Audio pts out of order at: " << audioTag->timestamp;
        return nullptr;
      } else {
        localItem->duration = audioTag->timestamp - lastAudioPts_.value();
      }
    }
    lastAudioPts_ = audioTag->timestamp;

    if (audioTag->aacPacketType == 0x0) {
      // Update AAC metadata (ASC sequence header detected)
      XLOG(DBG1) << "Saving new ASC header, size: " << audioTag->size;
      ascHeader_ = parseAscHeader(std::move(audioTag->data));
      if (!ascHeader_.valid) {
        XLOG(ERR) << "ASC header is corrupted at: " << audioTag->timestamp;
        return nullptr;
      }
      XLOG(DBG1) << "Parsed ASC header " << ascHeader_;
    } else {
      localItem->sampleFreq = ascHeader_.sampleFreq;
      localItem->numChannels = ascHeader_.channels;

      localItem->isIdr = true;
      localItem->data = std::move(audioTag->data);
      audioFrameId_++;

      // Return the item
      ret = std::move(localItem);
    }
  } else if (tag.index() == FlvTagTypeIndex::FLV_TAG_INDEX_SCRIPT) {
    XLOG(DBG1) << "Read tag SCRIPTDATAOBJECT";
  }
  return ret;
}

std::unique_ptr<FlvStreamParser::MediaItem> FlvStreamParser::parse(
    std::unique_ptr<folly::IOBuf> data) {
  auto tag = flvParse(std::move(data));
  return parse(tag);
}

} // namespace moxygen::flv

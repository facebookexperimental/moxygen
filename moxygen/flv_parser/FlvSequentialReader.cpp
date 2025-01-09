/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/flv_parser/FlvSequentialReader.h"
#include <folly/logging/xlog.h>
#include <chrono>

namespace moxygen::flv {

std::unique_ptr<FlvSequentialReader::MediaItem>
FlvSequentialReader::getNextItem() {
  std::unique_ptr<MediaItem> ret;
  XLOG(DBG1) << __func__;

  while (!ret) {
    try {
      std::unique_ptr<MediaItem> locaItem = std::make_unique<MediaItem>();
      auto tag = reader_.readNextTag();
      if (tag.index() == FlvTagTypeIndex::FLV_TAG_INDEX_READCMD) {
        auto readsCmd = std::get<flv::FlvReadCmd>(tag);
        if (readsCmd == flv::FlvReadCmd::FLV_EOF) {
          XLOG(INFO) << "End of flv file";
          locaItem->isEOF = true;
          return locaItem;
        } else if (readsCmd == flv::FlvReadCmd::FLV_UNKNOWN_TAG) {
          XLOG(WARNING) << "Unknown tag";
          return locaItem;
        }
      }

      // Set FLV timebase
      locaItem->timescale = kFlvTimeScale;
      // Set wallclock here (NOT exact, but close enough in real live video)
      locaItem->wallclock =
          std::chrono::duration_cast<std::chrono::milliseconds>(
              std::chrono::system_clock::now().time_since_epoch())
              .count();

      if (tag.index() == FlvTagTypeIndex::FLV_TAG_INDEX_VIDEO) {
        XLOG(DBG1) << "Read tag VIDEO at frame " << videoFrameId_;
        locaItem->type = MediaType::VIDEO;
        locaItem->id = videoFrameId_;

        auto videoTag =
            std::move(std::get<FlvTagTypeIndex::FLV_TAG_INDEX_VIDEO>(tag));

        // Not B frames for now
        locaItem->pts = locaItem->dts = videoTag->timestamp;
        locaItem->duration = 0;
        if (videoTag->codecId != 7) {
          XLOG(WARN) << "Not supported video tag codecID: " << videoTag->codecId
                     << ", VideoSize: " << videoTag->size;

          continue;
        }

        // Update last pts (to calculate duration)
        if (lastVideoPts_) {
          if (videoTag->timestamp < lastVideoPts_.value()) {
            XLOG(WARN)
                << "Video pts out of order, this could indicate B frames present (not supported for now) at: "
                << videoTag->timestamp;
            continue;
          } else {
            locaItem->duration = videoTag->timestamp - lastVideoPts_.value();
          }
        }
        lastVideoPts_ = videoTag->timestamp;

        if (videoTag->avcPacketType == 0x0) {
          // Update video metadata (AVCDecoderRecord)
          XLOG(DBG1) << "Saved AVCDecoderRecord header, size: "
                     << videoTag->size;
          avcDecoderRecord_ = std::move(videoTag->data);
        } else if (videoTag->avcPacketType == 0x1) {
          locaItem->isIdr = videoTag->frameType == 1;
          if (locaItem->isIdr && avcDecoderRecord_) {
            // Add metadata on IDR
            XLOG(DBG1) << "Added AVCDecoderRecord header";
            locaItem->metadata = avcDecoderRecord_->clone();
          }
          locaItem->data = std::move(videoTag->data);
          videoFrameId_++;

          // Return the item
          ret = std::move(locaItem);
        }
      } else if (tag.index() == FlvTagTypeIndex::FLV_TAG_INDEX_AUDIO) {
        XLOG(DBG1) << "Read tag AUDIO at frame " << audioFrameId_;
        locaItem->type = MediaType::AUDIO;
        locaItem->id = audioFrameId_;

        auto audioTag =
            std::move(std::get<FlvTagTypeIndex::FLV_TAG_INDEX_AUDIO>(tag));

        if (audioTag->soundFormat != 10) {
          XLOG(WARN) << "Not supported audio format codecID: "
                     << audioTag->soundFormat
                     << ". AudioSize: " << audioTag->size;
          continue;
        }

        locaItem->pts = locaItem->dts = audioTag->timestamp;
        locaItem->duration = 0;
        if (lastAudioPts_) {
          if (audioTag->timestamp < lastAudioPts_.value()) {
            XLOG(ERR) << "Audio pts out of order at: " << audioTag->timestamp;
            continue;
          } else {
            locaItem->duration = audioTag->timestamp - lastAudioPts_.value();
          }
        }
        lastAudioPts_ = audioTag->timestamp;

        if (audioTag->aacPacketType == 0x0) {
          // Update AAC metadata (ASC sequence header detected)
          XLOG(DBG1) << "Saving new ASC header, size: " << audioTag->size;
          ascHeader_ = parseAscHeader(std::move(audioTag->data));
          if (!ascHeader_.valid) {
            XLOG(ERR) << "ASC header is corrupted at: " << audioTag->timestamp;
            continue;
          }
          XLOG(INFO) << "Parsed ASC header " << ascHeader_;
        } else {
          locaItem->sampleFreq = ascHeader_.sampleFreq;
          locaItem->numChannels = ascHeader_.channels;

          locaItem->isIdr = true;
          locaItem->data = std::move(audioTag->data);
          audioFrameId_++;

          // Return the item
          ret = std::move(locaItem);
        }
      } else if (tag.index() == FlvTagTypeIndex::FLV_TAG_INDEX_SCRIPT) {
        XLOG(DBG1) << "Read tag SCRIPTDATAOBJECT";
      }
    } catch (std::exception& ex) {
      XLOG(ERR) << "Error processing tag. Ex: " << folly::exceptionStr(ex);
      ret = nullptr;
      break;
    }
  }

  return ret;
}

} // namespace moxygen::flv

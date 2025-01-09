/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/flv_parser/FlvReader.h"
#include "moxygen/flv_parser/test/FlvTestUtils.h"

#include <folly/logging/xlog.h>
#include <folly/portability/GTest.h>

using namespace moxygen::flv;
using namespace moxygen::test;

namespace {
const std::string kTestDir = getContainingDirectory(XLOG_FILENAME).str();
const std::string kFlvOkTestFilePath = "resources/testOK1s.flv";
} // namespace

TEST(FlvReaderTest, ReadOk) {
  std::string flvTestFilePath = kTestDir + "/" + kFlvOkTestFilePath;
  XLOG(INFO) << "Reading file: " << flvTestFilePath;
  FlvReader flvr(flvTestFilePath);
  uint32_t numDetectedAscHeader = 0;
  uint32_t numDetectedAVCDecoderRecord = 0;
  uint32_t numVideoFrames = 0;
  uint32_t numAudioFrames = 0;

  bool exit = false;
  while (exit == false) {
    auto composedTag = flvr.readNextTag();

    if (composedTag.index() == FlvTagTypeIndex::FLV_TAG_INDEX_READCMD) {
      auto readsCmd = std::get<FlvReadCmd>(composedTag);
      if (readsCmd == FlvReadCmd::FLV_EOF) {
        exit = true;
      }
    } else if (composedTag.index() == FlvTagTypeIndex::FLV_TAG_INDEX_SCRIPT) {
      auto scriptTag = std::move(
          std::get<FlvTagTypeIndex::FLV_TAG_INDEX_SCRIPT>(composedTag));
      XLOG(INFO) << "READ tag script=" << (uint32_t)scriptTag->type
                 << " size=" << scriptTag->size
                 << " ts=" << scriptTag->timestamp;
    } else if (composedTag.index() == FlvTagTypeIndex::FLV_TAG_INDEX_VIDEO) {
      auto videoTag = std::move(
          std::get<FlvTagTypeIndex::FLV_TAG_INDEX_VIDEO>(composedTag));
      XLOG(INFO) << "tag=" << (uint32_t)videoTag->type
                 << " size=" << videoTag->size << " ts=" << videoTag->timestamp;

      EXPECT_EQ(videoTag->type, 0x09);
      EXPECT_EQ(videoTag->codecId, 7);
      EXPECT_EQ(videoTag->compositionTime, 0); // No B Frames accepted for now
      if (videoTag->avcPacketType == 0) {
        EXPECT_NE(videoTag->data, nullptr);
        numDetectedAVCDecoderRecord++;
      } else if (videoTag->avcPacketType == 1) {
        EXPECT_NE(videoTag->data, nullptr);
        numVideoFrames++;
      }
      XLOG(INFO) << "Read tag VIDEO at frame " << numVideoFrames
                 << ", avcPacketType: " << (uint32_t)videoTag->avcPacketType
                 << ", dataSize: "
                 << ((videoTag->data) ? videoTag->data->computeChainDataLength()
                                      : static_cast<size_t>(0));
    } else if (composedTag.index() == FlvTagTypeIndex::FLV_TAG_INDEX_AUDIO) {
      auto audioTag = std::move(
          std::get<FlvTagTypeIndex::FLV_TAG_INDEX_AUDIO>(composedTag));
      XLOG(INFO) << "tag=" << (uint32_t)audioTag->type
                 << " size=" << audioTag->size << " ts=" << audioTag->timestamp;

      EXPECT_EQ(audioTag->type, 0x08);
      EXPECT_NE(audioTag->data, nullptr);
      EXPECT_EQ(audioTag->soundRate, 3);
      EXPECT_EQ(audioTag->soundSize, 1);
      EXPECT_EQ(audioTag->soundType, 1);
      EXPECT_EQ(audioTag->getSamplingFreq(), 44100);
      EXPECT_EQ(audioTag->getBitsPerSample(), 16);
      EXPECT_EQ(audioTag->getChannels(), 2);
      if (audioTag->aacPacketType == 0) {
        numDetectedAscHeader++;
      } else {
        numAudioFrames++;
      }
      XLOG(INFO) << "Read tag AUDIO at frame " << numAudioFrames
                 << ", aacPacketType: " << (uint32_t)audioTag->aacPacketType
                 << ", dataSize: "
                 << ((audioTag->data) ? audioTag->data->computeChainDataLength()
                                      : 0);
    }
  }
  EXPECT_EQ(numDetectedAscHeader, 1);
  EXPECT_EQ(numDetectedAVCDecoderRecord, 1);
  EXPECT_EQ(numVideoFrames, 30);
  EXPECT_EQ(
      numAudioFrames,
      49); // 48000 / 1024 = 47.6 + rounding + 1 primimng = 49
}

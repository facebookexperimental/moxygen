/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/flv_parser/FlvWriter.h"
#include "common/files/FileUtil.h"
#include "moxygen/flv_parser/FlvReader.h"

#include <folly/logging/xlog.h>
#include <folly/portability/GTest.h>

using namespace moxygen::flv;

class FlvWriterTest : public ::testing::Test {
 public:
  FlvWriterTest() {
    dir_ = facebook::files::FileUtil::recreateRandomTempDir(
        "MoxygenFlvWriterTest");
  }
  ~FlvWriterTest() override {
    facebook::files::FileUtil::removeAll(dir_);
  }

 protected:
  std::string dir_;
};

TEST_F(FlvWriterTest, WriteOK) {
  std::string flvTestFilePath = dir_ + "/" + "witeOK.flv";

  auto tagScript = createScriptTag(10, folly::IOBuf::copyBuffer("testScript"));
  auto tagVideoHeader = createVideoTag(
      0x0FFFFFF1,
      1,
      7,
      0,
      0x0FFFFFF1,
      folly::IOBuf::copyBuffer("testVideoHeader"));
  auto tagVideoFrameIDR = createVideoTag(
      0x0FFFFFF1,
      1,
      7,
      1,
      0x0FFFFFF1,
      folly::IOBuf::copyBuffer("testVideoIDR"));
  auto tagVideoFrame1 = createVideoTag(
      0x0FFFFFF2,
      2,
      7,
      1,
      0x0FFFFFF2,
      folly::IOBuf::copyBuffer("testVideoFrame"));
  auto tagAudioHeader = createAudioTag(
      0x0FFFFFF1, 10, 3, 1, 1, 0, folly::IOBuf::copyBuffer("testAudioHeader"));
  auto tagAudioFrame1 = createAudioTag(
      0x0FFFFFF1, 10, 3, 1, 1, 1, folly::IOBuf::copyBuffer("testAudioFrame"));

  {
    std::unique_ptr<FlvWriter> flvw =
        std::make_unique<FlvWriter>(flvTestFilePath);

    flvw->writeTag(tagScript->clone());
    flvw->writeTag(tagVideoHeader->clone());
    flvw->writeTag(tagVideoFrameIDR->clone());
    flvw->writeTag(tagVideoFrame1->clone());
    flvw->writeTag(tagAudioHeader->clone());
    flvw->writeTag(tagAudioFrame1->clone());
  }

  XLOG(INFO) << "Wrote file: " << flvTestFilePath;

  std::ifstream ftest(flvTestFilePath, std::fstream::binary);

  size_t bufSize = 300;
  char buf[bufSize];
  ftest.read(buf, bufSize);
  auto readBytes = ftest.gcount();
  ftest.close();

  XLOG(INFO) << "READ: " << folly::hexDump(buf, readBytes);

  XLOG(INFO) << "Reading file: " << flvTestFilePath;
  FlvReader flvr(flvTestFilePath);

  bool exit = false;
  std::list<std::unique_ptr<FlvScriptTag>> readScriptTags = {};
  std::list<std::unique_ptr<FlvVideoTag>> readVideoTags = {};
  std::list<std::unique_ptr<FlvAudioTag>> readAudioTags = {};
  while (exit == false) {
    auto composedTag = flvr.readNextTag();
    XLOG(INFO) << "Read new tag";
    if (composedTag.index() == FlvTagTypeIndex::FLV_TAG_INDEX_READCMD) {
      XLOG(INFO) << "cmd tag";
      auto readsCmd = std::get<FlvReadCmd>(composedTag);
      if (readsCmd == FlvReadCmd::FLV_EOF) {
        XLOG(INFO) << "Exiting read loop";
        exit = true;
      }
    } else {
      if (composedTag.index() == FlvTagTypeIndex::FLV_TAG_INDEX_SCRIPT) {
        auto scriptTag = std::move(
            std::get<FlvTagTypeIndex::FLV_TAG_INDEX_SCRIPT>(composedTag));
        XLOG(INFO) << "Script tag data: " << (int)scriptTag->type << ", "
                   << scriptTag->timestamp << ", " << scriptTag->streamId
                   << ", " << scriptTag->size << ", " << scriptTag->data;
        readScriptTags.push_back(std::move(scriptTag));
      } else if (composedTag.index() == FlvTagTypeIndex::FLV_TAG_INDEX_VIDEO) {
        auto videoTag = std::move(
            std::get<FlvTagTypeIndex::FLV_TAG_INDEX_VIDEO>(composedTag));
        XLOG(INFO) << "Video tag data: " << (int)videoTag->type << ", "
                   << (int)videoTag->timestamp << ", "
                   << (int)videoTag->streamId << ", " << (int)videoTag->size
                   << ", " << (int)videoTag->frameType << ","
                   << (int)videoTag->codecId << ", "
                   << (int)videoTag->avcPacketType << ", "
                   << (int)videoTag->compositionTime << ", " << videoTag->data;
        readVideoTags.push_back(std::move(videoTag));
      } else if (composedTag.index() == FlvTagTypeIndex::FLV_TAG_INDEX_AUDIO) {
        auto audioTag = std::move(
            std::get<FlvTagTypeIndex::FLV_TAG_INDEX_AUDIO>(composedTag));
        XLOG(INFO) << "Audio tag data: " << (int)audioTag->type << ", "
                   << (int)audioTag->timestamp << ", "
                   << (int)audioTag->streamId << ", " << (int)audioTag->size
                   << ", " << (int)audioTag->soundFormat << ","
                   << (int)audioTag->soundRate << ", "
                   << (int)audioTag->soundSize << ", "
                   << (int)audioTag->soundType << ", "
                   << (int)audioTag->aacPacketType << ", " << audioTag->data;
        readAudioTags.push_back(std::move(audioTag));
      }
    }
  }
  EXPECT_EQ(readScriptTags.size(), 1);
  EXPECT_EQ(readVideoTags.size(), 3);
  EXPECT_EQ(readAudioTags.size(), 2);

  // Check script tag
  auto scriptTagRead = std::move(readScriptTags.front());
  readScriptTags.pop_front();
  EXPECT_TRUE(*tagScript == *scriptTagRead);

  // Check video tags
  auto videoHeaderTagRead = std::move(readVideoTags.front());
  readVideoTags.pop_front();
  EXPECT_TRUE(*tagVideoHeader == *videoHeaderTagRead);
  auto tagVideoFrameIDRRead = std::move(readVideoTags.front());
  readVideoTags.pop_front();
  EXPECT_TRUE(*tagVideoFrameIDR == *tagVideoFrameIDRRead);
  auto tagVideoFrame1Read = std::move(readVideoTags.front());
  readVideoTags.pop_front();
  EXPECT_TRUE(*tagVideoFrame1 == *tagVideoFrame1Read);

  // Check audio tags
  auto tagAudioHeaderRead = std::move(readAudioTags.front());
  readAudioTags.pop_front();
  EXPECT_TRUE(*tagAudioHeader == *tagAudioHeaderRead);
  auto tagAudioFrame1Read = std::move(readAudioTags.front());
  readAudioTags.pop_front();
  EXPECT_TRUE(*tagAudioFrame1 == *tagAudioFrame1Read);
}

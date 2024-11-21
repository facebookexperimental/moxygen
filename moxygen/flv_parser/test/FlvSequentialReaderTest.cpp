/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/flv_parser/FlvSequentialReader.h"
#include <folly/logging/xlog.h>
#include <folly/portability/GTest.h>
#include "moxygen/flv_parser/test/FlvTestUtils.h"

using namespace moxygen;

namespace {
const std::string kTestDir = test::getContainingDirectory(XLOG_FILENAME).str();
const std::string kFlvOkTestFilePath = "resources/testOK1s.flv";
} // namespace

TEST(FlvSequentialReader, ReadOk) {
  std::string flvTestFilePath = kTestDir + "/" + kFlvOkTestFilePath;
  XLOG(INFO) << "Reading file: " << flvTestFilePath;
  FlvSequentialReader flvsr(flvTestFilePath);
  uint32_t numVideoTags = 0;
  uint32_t numAudioTags = 0;

  while (true) {
    auto tag = flvsr.getNextItem();
    XLOG(INFO) << "Read TAG, type: " << (uint32_t)tag->type
               << ", ts: " << tag->pts;
    if (tag->isEOF) {
      break;
    } else {
      EXPECT_EQ(tag->timescale, 1000);
      EXPECT_NE(tag->wallclock, 0);
      if (tag->type == FlvSequentialReader::MediaType::VIDEO) {
        EXPECT_NE(tag->data, nullptr);
        if (tag->isIdr) {
          EXPECT_NE(
              tag->metadata,
              nullptr); // We should send AVCDecoderConfigRecord in each IDR
        }
        numVideoTags++;
      } else if (tag->type == FlvSequentialReader::MediaType::AUDIO) {
        EXPECT_NE(tag->data, nullptr);
        // We should send channel and sampleFreq info in each frame
        EXPECT_EQ(tag->numChannels, 1);
        EXPECT_EQ(tag->sampleFreq, 48000);
        numAudioTags++;
      }
    }
  }
  EXPECT_EQ(numVideoTags, 30);
  EXPECT_EQ(
      numAudioTags, 49); // 48000 / 1024 = 47.6 + rounding + 1 primimng = 49
}

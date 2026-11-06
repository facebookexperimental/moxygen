/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <folly/logging/xlog.h>
#include <folly/portability/GTest.h>
#include <string>
#ifndef FLV_TEST_RESOURCE_DIR
#include <folly/testing/TestUtil.h>
#endif
#include "moxygen/flv_parser/FlvReader.h"
#include "moxygen/flv_parser/FlvStreamParser.h"
#include "moxygen/flv_parser/test/FlvTestUtils.h"

using namespace moxygen::flv;
using namespace moxygen::test;

#ifndef FLV_TEST_RESOURCE_DIR
namespace {
constexpr auto kFlvOkTestResource =
    "ti/experimental/moxygen/flv_parser/test/resources/testOK1s.flv";
} // namespace
#endif

TEST(FlvSequentialReader, ReadOk) {
#ifdef FLV_TEST_RESOURCE_DIR
  const std::string flvTestFilePath = FLV_TEST_RESOURCE_DIR "/testOK1s.flv";
#else
  const auto flvTestFilePath =
      folly::test::find_resource(kFlvOkTestResource).string();
#endif
  XLOG(INFO) << "Reading file: " << flvTestFilePath;
  FlvReader flvsr(flvTestFilePath);
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
      if (tag->type == FlvStreamParser::MediaType::VIDEO) {
        EXPECT_NE(tag->data, nullptr);
        if (tag->isIdr) {
          EXPECT_NE(
              tag->metadata,
              nullptr); // We should send AVCDecoderConfigRecord in each IDR
        }
        numVideoTags++;
      } else if (tag->type == FlvStreamParser::MediaType::AUDIO) {
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

/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "moxygen/moq_mi_to_flv/MoQMiToFlv.h"

#include <folly/io/IOBuf.h>
#include <folly/portability/GTest.h>

namespace moxygen { namespace {

MoQMi::MoqMiItem videoItem(uint64_t seqId, bool isIdr, bool includeMetadata) {
  const uint8_t nal[] = {0, 0, 0, 1, isIdr ? uint8_t{0x65} : uint8_t{0x41}};
  auto frame = std::make_unique<MoQMi::VideoH264AVCCWCPData>(
      seqId,
      seqId * 33,
      1000,
      33,
      0,
      folly::IOBuf::copyBuffer(nal, sizeof(nal)),
      includeMetadata ? folly::IOBuf::copyBuffer("avcc") : nullptr,
      seqId * 33);
  return MoQMi::MoqMiItem(std::move(frame));
}

TEST(MoQMiToFlvTest, UsesSpecifiedFrameTypesForKeyAndInterFrames) {
  MoQMiToFlv converter;

  auto keyTags = converter.MoQMiToFlvPayload(videoItem(0, true, true));
  ASSERT_EQ(keyTags.size(), 2);
  auto keyFrame = std::move(keyTags.back());
  ASSERT_EQ(keyFrame.index(), flv::FlvTagTypeIndex::FLV_TAG_INDEX_VIDEO);
  EXPECT_EQ(
      std::get<flv::FlvTagTypeIndex::FLV_TAG_INDEX_VIDEO>(keyFrame)->frameType,
      1);

  auto interTags = converter.MoQMiToFlvPayload(videoItem(1, false, false));
  ASSERT_EQ(interTags.size(), 1);
  auto interFrame = std::move(interTags.front());
  ASSERT_EQ(interFrame.index(), flv::FlvTagTypeIndex::FLV_TAG_INDEX_VIDEO);
  EXPECT_EQ(
      std::get<flv::FlvTagTypeIndex::FLV_TAG_INDEX_VIDEO>(interFrame)
          ->frameType,
      2);
}

}} // namespace moxygen

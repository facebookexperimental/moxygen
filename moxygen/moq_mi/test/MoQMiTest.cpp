/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/moq_mi/MoQMi.h"

#include <folly/logging/xlog.h>
#include <folly/portability/GTest.h>

using namespace moxygen;

const auto kTestData = folly::IOBuf::copyBuffer("Data: Hello world");
const auto kTestMetadata = folly::IOBuf::copyBuffer("Metadata: Hello world");

TEST(MoQMi, EncodeVideoH264TestNoMetadata) {
  // Expected wire format:
  uint8_t expectedWire[67] = {
      0x00, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x00, 0xff, 0xff, 0xff,
      0xff, 0xff, 0xff, 0xff, 0x01, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
      0x02, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x04, 0xff, 0xff, 0xff,
      0xff, 0xff, 0xff, 0xff, 0x03, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
      0x05, 0x00, 0x44, 0x61, 0x74, 0x61, 0x3a, 0x20, 0x48, 0x65, 0x6c, 0x6c,
      0x6f, 0x20, 0x77, 0x6f, 0x72, 0x6c, 0x64};

  // Test VideoH264AVCCWCPData without metadata
  auto dataToEncode = std::make_unique<MoQMi::VideoH264AVCCWCPData>(
      0x3FFFFFFFFFFFFF00, // SeqId
      0x3FFFFFFFFFFFFF01, // Pts
      0x3FFFFFFFFFFFFF04, // Timescale
      0x3FFFFFFFFFFFFF03, // Duration
      0x3FFFFFFFFFFFFF05, // Wallclock
      kTestData->clone(), // Data
      nullptr,            // Metadata
      0x3FFFFFFFFFFFFF02  // Dts
  );
  // Total: 1+8+8+8+8+8+8+1+17 = 67 bytes

  XLOG(INFO) << "dataToEncode->data->length()="
             << dataToEncode->data->computeChainDataLength();

  auto mi = MoQMi::toObjectPayload(std::move(dataToEncode));

  EXPECT_NE(mi, nullptr);
  EXPECT_EQ(mi->computeChainDataLength(), 67);

  folly::IOBufEqualTo eq;
  EXPECT_TRUE(eq(mi, folly::IOBuf::copyBuffer(expectedWire, 67)));
}

TEST(MoQMi, EncodeVideoH264TestWithMetadata) {
  // Expected wire format:
  uint8_t expectedWire[88] = {
      0x00, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x00, 0xff, 0xff,
      0xff, 0xff, 0xff, 0xff, 0xff, 0x01, 0xff, 0xff, 0xff, 0xff, 0xff,
      0xff, 0xff, 0x02, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x04,
      0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x03, 0xff, 0xff, 0xff,
      0xff, 0xff, 0xff, 0xff, 0x05, 0x15, 0x4d, 0x65, 0x74, 0x61, 0x64,
      0x61, 0x74, 0x61, 0x3a, 0x20, 0x48, 0x65, 0x6c, 0x6c, 0x6f, 0x20,
      0x77, 0x6f, 0x72, 0x6c, 0x64, 0x44, 0x61, 0x74, 0x61, 0x3a, 0x20,
      0x48, 0x65, 0x6c, 0x6c, 0x6f, 0x20, 0x77, 0x6f, 0x72, 0x6c, 0x64};

  // Test VideoH264AVCCWCPData with metadata
  auto dataToEncode = std::make_unique<MoQMi::VideoH264AVCCWCPData>(
      0x3FFFFFFFFFFFFF00,     // SeqId
      0x3FFFFFFFFFFFFF01,     // Pts
      0x3FFFFFFFFFFFFF04,     // Timescale
      0x3FFFFFFFFFFFFF03,     // Duration
      0x3FFFFFFFFFFFFF05,     // Wallclock
      kTestData->clone(),     // Data
      kTestMetadata->clone(), // Metadata
      0x3FFFFFFFFFFFFF02      // Dts
  );
  // Total: 1+8+8+8+8+8+8+21+1+17 = 88 bytes

  XLOG(INFO) << "dataToEncode->metadata->length()="
             << dataToEncode->metadata->length();
  XLOG(INFO) << "dataToEncode->data->length()=" << dataToEncode->data->length();

  auto mi = MoQMi::toObjectPayload(std::move(dataToEncode));

  EXPECT_NE(mi, nullptr);
  EXPECT_EQ(mi->computeChainDataLength(), 88);

  folly::IOBufEqualTo eq;
  EXPECT_TRUE(eq(mi, folly::IOBuf::copyBuffer(expectedWire, 88)));
}

TEST(MoQMi, EncodeAudioAAC) {
  // Expected wire format:
  uint8_t expectedWire[74] = {
      0x03, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x00, 0xff, 0xff,
      0xff, 0xff, 0xff, 0xff, 0xff, 0x01, 0xff, 0xff, 0xff, 0xff, 0xff,
      0xff, 0xff, 0x04, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x06,
      0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x07, 0xff, 0xff, 0xff,
      0xff, 0xff, 0xff, 0xff, 0x03, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
      0xff, 0x05, 0x44, 0x61, 0x74, 0x61, 0x3a, 0x20, 0x48, 0x65, 0x6c,
      0x6c, 0x6f, 0x20, 0x77, 0x6f, 0x72, 0x6c, 0x64};

  // Test AudioAACMP4LCWCPData
  auto dataToEncode = std::make_unique<MoQMi::AudioAACMP4LCWCPData>(
      0x3FFFFFFFFFFFFF00, // SeqId
      0x3FFFFFFFFFFFFF01, // Pts
      0x3FFFFFFFFFFFFF04, // Timescale
      0x3FFFFFFFFFFFFF03, // Duration
      0x3FFFFFFFFFFFFF05, // Wallclock
      kTestData->clone(), // Data
      0x3FFFFFFFFFFFFF06, // SampleFreq
      0x3FFFFFFFFFFFFF07  // NumChannels
  );
  // Total: 1+8+8+8+8+8+8+8+17 = 74 bytes

  XLOG(INFO) << "dataToEncode->data->length()="
             << dataToEncode->data->computeChainDataLength();

  auto mi = MoQMi::toObjectPayload(std::move(dataToEncode));

  EXPECT_NE(mi, nullptr);
  EXPECT_EQ(mi->computeChainDataLength(), 74);

  folly::IOBufEqualTo eq;
  EXPECT_TRUE(eq(mi, folly::IOBuf::copyBuffer(expectedWire, 74)));
}

TEST(MoQMi, DecodeVideoH264TestWithMetadata) {
  uint8_t fromWire[88] = {
      0x00, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x00, 0xff, 0xff,
      0xff, 0xff, 0xff, 0xff, 0xff, 0x01, 0xff, 0xff, 0xff, 0xff, 0xff,
      0xff, 0xff, 0x02, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x04,
      0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x03, 0xff, 0xff, 0xff,
      0xff, 0xff, 0xff, 0xff, 0x05, 0x15, 0x4d, 0x65, 0x74, 0x61, 0x64,
      0x61, 0x74, 0x61, 0x3a, 0x20, 0x48, 0x65, 0x6c, 0x6c, 0x6f, 0x20,
      0x77, 0x6f, 0x72, 0x6c, 0x64, 0x44, 0x61, 0x74, 0x61, 0x3a, 0x20,
      0x48, 0x65, 0x6c, 0x6c, 0x6f, 0x20, 0x77, 0x6f, 0x72, 0x6c, 0x64};

  auto buff = folly::IOBuf::copyBuffer(fromWire, sizeof(fromWire));

  // Test VideoH264AVCCWCPData with metadata
  auto res = MoQMi::fromObjectPayload(buff->clone());

  EXPECT_NE(std::get<0>(res), nullptr);
  EXPECT_EQ(std::get<1>(res), nullptr);
  EXPECT_EQ(std::get<0>(res)->seqId, 0x3FFFFFFFFFFFFF00);
  EXPECT_EQ(std::get<0>(res)->pts, 0x3FFFFFFFFFFFFF01);
  EXPECT_EQ(std::get<0>(res)->timescale, 0x3FFFFFFFFFFFFF04);
  EXPECT_EQ(std::get<0>(res)->duration, 0x3FFFFFFFFFFFFF03);
  EXPECT_EQ(std::get<0>(res)->wallclock, 0x3FFFFFFFFFFFFF05);
  EXPECT_EQ(std::get<0>(res)->dts, 0x3FFFFFFFFFFFFF02);
  folly::IOBufEqualTo eq;
  EXPECT_TRUE(eq(std::get<0>(res)->data, kTestData));
  EXPECT_TRUE(eq(std::get<0>(res)->metadata, kTestMetadata));
}

TEST(MoQMi, DecodeVideoH264TestNoMetadata) {
  uint8_t fromWire[67] = {
      0x00, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x00, 0xff, 0xff, 0xff,
      0xff, 0xff, 0xff, 0xff, 0x01, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
      0x02, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x04, 0xff, 0xff, 0xff,
      0xff, 0xff, 0xff, 0xff, 0x03, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
      0x05, 0x00, 0x44, 0x61, 0x74, 0x61, 0x3a, 0x20, 0x48, 0x65, 0x6c, 0x6c,
      0x6f, 0x20, 0x77, 0x6f, 0x72, 0x6c, 0x64};

  auto buff = folly::IOBuf::copyBuffer(fromWire, sizeof(fromWire));

  // Test VideoH264AVCCWCPData without metadata
  auto res = MoQMi::fromObjectPayload(buff->clone());

  EXPECT_NE(std::get<0>(res), nullptr);
  EXPECT_EQ(std::get<1>(res), nullptr);
  EXPECT_EQ(std::get<0>(res)->seqId, 0x3FFFFFFFFFFFFF00);
  EXPECT_EQ(std::get<0>(res)->pts, 0x3FFFFFFFFFFFFF01);
  EXPECT_EQ(std::get<0>(res)->timescale, 0x3FFFFFFFFFFFFF04);
  EXPECT_EQ(std::get<0>(res)->duration, 0x3FFFFFFFFFFFFF03);
  EXPECT_EQ(std::get<0>(res)->wallclock, 0x3FFFFFFFFFFFFF05);
  EXPECT_EQ(std::get<0>(res)->dts, 0x3FFFFFFFFFFFFF02);
  EXPECT_EQ(std::get<0>(res)->metadata, nullptr);
  folly::IOBufEqualTo eq;
  EXPECT_TRUE(eq(std::get<0>(res)->data, kTestData));
}

TEST(MoQMi, DecodeAudioAAC) {
  uint8_t fromWire[74] = {
      0x03, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x00, 0xff, 0xff,
      0xff, 0xff, 0xff, 0xff, 0xff, 0x01, 0xff, 0xff, 0xff, 0xff, 0xff,
      0xff, 0xff, 0x04, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x06,
      0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x07, 0xff, 0xff, 0xff,
      0xff, 0xff, 0xff, 0xff, 0x03, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
      0xff, 0x05, 0x44, 0x61, 0x74, 0x61, 0x3a, 0x20, 0x48, 0x65, 0x6c,
      0x6c, 0x6f, 0x20, 0x77, 0x6f, 0x72, 0x6c, 0x64};

  auto buff = folly::IOBuf::copyBuffer(fromWire, sizeof(fromWire));

  // Test AudioAACMP4LCWCPData
  auto res = MoQMi::fromObjectPayload(buff->clone());

  EXPECT_EQ(std::get<0>(res), nullptr);
  EXPECT_NE(std::get<1>(res), nullptr);
  EXPECT_EQ(std::get<1>(res)->seqId, 0x3FFFFFFFFFFFFF00);
  EXPECT_EQ(std::get<1>(res)->pts, 0x3FFFFFFFFFFFFF01);
  EXPECT_EQ(std::get<1>(res)->timescale, 0x3FFFFFFFFFFFFF04);
  EXPECT_EQ(std::get<1>(res)->duration, 0x3FFFFFFFFFFFFF03);
  EXPECT_EQ(std::get<1>(res)->wallclock, 0x3FFFFFFFFFFFFF05);
  EXPECT_EQ(std::get<1>(res)->sampleFreq, 0x3FFFFFFFFFFFFF06);
  EXPECT_EQ(std::get<1>(res)->numChannels, 0x3FFFFFFFFFFFFF07);
  folly::IOBufEqualTo eq;
  EXPECT_TRUE(eq(std::get<1>(res)->data, kTestData));
}

TEST(MoQMi, OverrideStreamOpVideoH264) {
  std::string expected =
      "VideoH264. id: 1, pts: 2, dts: 6, timescale: 3, duration: 4, wallclock: 5, metadata length: 21, data length: 17";

  auto dataVideo = std::make_unique<MoQMi::VideoH264AVCCWCPData>(
      1,                      // SeqId
      2,                      // Pts
      3,                      // Timescale
      4,                      // Duration
      5,                      // Wallclock
      kTestData->clone(),     // Data
      kTestMetadata->clone(), // Metadata
      6                       // Dts
  );

  std::stringstream ss;

  ss << *dataVideo;
  EXPECT_EQ(ss.str(), expected);
}

TEST(MoQMi, OverrideStreamOpAudioAAC) {
  std::string expected =
      "AudioAAC. id: 1, pts: 2, sampleFreq: 6, numChannels: 7, timescale: 3, duration: 4, wallclock: 5, data length: 17";

  auto dataAudio = std::make_unique<MoQMi::AudioAACMP4LCWCPData>(
      1,                  // SeqId
      2,                  // Pts
      3,                  // Timescale
      4,                  // Duration
      5,                  // Wallclock
      kTestData->clone(), // Data
      6,                  // SampleFreq
      7                   // NumChannels
  );

  std::stringstream ss;

  ss << *dataAudio;
  EXPECT_EQ(ss.str(), expected);
}

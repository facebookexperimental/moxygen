/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/moq_mi/MoQMi.h"

#include <folly/String.h>
#include <folly/logging/xlog.h>
#include <folly/portability/GTest.h>
#include <proxygen/lib/utils/Logging.h>

namespace moxygen {

using namespace testing;

const auto kTestData =
    folly::IOBuf::copyBuffer("Data: Hello world"); // Length 17
const auto kTestExtradata = folly::IOBuf::copyBuffer("Metadata: Hello world");

uint8_t idrNaluBuff[5] = {0x00, 0x00, 0x00, 0x01, 0x05};
const auto kTestVideoDataIDR =
    folly::IOBuf::copyBuffer(idrNaluBuff, sizeof(idrNaluBuff));
uint8_t noIdrNaluBuff[5] = {0x00, 0x00, 0x00, 0x01, 0x01};
const auto kTestVideoDataNoIDR =
    folly::IOBuf::copyBuffer(noIdrNaluBuff, sizeof(noIdrNaluBuff));

uint8_t expectedh264AVCCMetadataExtensionWire[48] = {
    0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x00, 0xff, 0xff, 0xff, 0xff,
    0xff, 0xff, 0xff, 0x01, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x02,
    0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x04, 0xff, 0xff, 0xff, 0xff,
    0xff, 0xff, 0xff, 0x03, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x05};
const auto kTestExpectedh264AVCCMetadataExtensionWire =
    folly::IOBuf::copyBuffer(
        expectedh264AVCCMetadataExtensionWire,
        sizeof(expectedh264AVCCMetadataExtensionWire));

uint8_t expectedAACLCMetadataExtensionWire[56] = {
    0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x00, 0xff, 0xff, 0xff, 0xff,
    0xff, 0xff, 0xff, 0x01, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x02,
    0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x03, 0xff, 0xff, 0xff, 0xff,
    0xff, 0xff, 0xff, 0x04, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x05,
    0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x06};
const auto kTestExpectedhAACLCMetadataExtensionWire = folly::IOBuf::copyBuffer(
    expectedAACLCMetadataExtensionWire,
    sizeof(expectedAACLCMetadataExtensionWire));

TEST(MoQMiTest, EncodeVideoH264TestNoMetadata) {
  auto item = std::make_unique<moxygen::MediaItem>(
      kTestData->clone(),
      nullptr /* metadata */,
      MediaType::VIDEO,
      0x3FFFFFFFFFFFFF00 /* id */,
      0x3FFFFFFFFFFFFF01 /* pts*/,
      0x3FFFFFFFFFFFFF02 /* dts*/,
      0x3FFFFFFFFFFFFF04 /* timescale */,
      0x3FFFFFFFFFFFFF03 /* duration */,
      0x3FFFFFFFFFFFFF05 /* wallclock*/,
      false /* isIDR */,
      false /* EOF*/);

  auto mi = MoQMi::encodeToMoQMi(item->clone());

  EXPECT_NE(mi, nullptr);
  // Check extensions
  EXPECT_EQ(mi->extensions.size(), 2);
  EXPECT_EQ(
      mi->extensions[0].type,
      folly::to_underlying(MoQMi::HeaderExtensionsTypeIDs::
                               MOQ_EXT_HEADER_TYPE_MOQMI_MEDIA_TYPE));
  EXPECT_EQ(
      mi->extensions[0].intValue,
      folly::to_underlying(
          MoQMi::HeaderExtensionMediaTypeValues::
              MOQ_EXT_HEADER_TYPE_MOQMI_MEDIA_VALUE_VIDEO_H264_IN_AVCC));
  EXPECT_EQ(
      mi->extensions[1].type,
      folly::to_underlying(
          MoQMi::HeaderExtensionsTypeIDs::
              MOQ_EXT_HEADER_TYPE_MOQMI_VIDEO_H264_IN_AVCC_METADATA));
  XLOG(INFO) << "MOQ_EXT_HEADER_TYPE_MOQMI_VIDEO_H264_IN_AVCC_METADATA: "
             << proxygen::IOBufPrinter::printHexFolly(
                    mi->extensions[1].arrayValue.get(), true);
  folly::IOBufEqualTo eq;
  EXPECT_TRUE(
      eq(mi->extensions[1].arrayValue,
         kTestExpectedh264AVCCMetadataExtensionWire));

  // Check payload
  EXPECT_NE(mi->payload, nullptr);
  EXPECT_EQ(mi->payload->computeChainDataLength(), 17);
  EXPECT_TRUE(eq(mi->payload, kTestData->clone()));
}

TEST(MoQMiTest, EncodeVideoH264TestWithMetadata) {
  auto item = std::make_unique<moxygen::MediaItem>(
      kTestData->clone(),
      kTestExtradata->clone() /* extradata */,
      MediaType::VIDEO,
      0x3FFFFFFFFFFFFF00 /* id */,
      0x3FFFFFFFFFFFFF01 /* pts*/,
      0x3FFFFFFFFFFFFF02 /* dts*/,
      0x3FFFFFFFFFFFFF04 /* timescale */,
      0x3FFFFFFFFFFFFF03 /* duration */,
      0x3FFFFFFFFFFFFF05 /* wallclock*/,
      true /* isIDR */,
      false /* EOF*/);

  auto mi = MoQMi::encodeToMoQMi(item->clone());

  EXPECT_NE(mi, nullptr);
  // Check extensions
  EXPECT_EQ(mi->extensions.size(), 3);
  EXPECT_EQ(
      mi->extensions[0].type,
      folly::to_underlying(MoQMi::HeaderExtensionsTypeIDs::
                               MOQ_EXT_HEADER_TYPE_MOQMI_MEDIA_TYPE));
  EXPECT_EQ(
      mi->extensions[0].intValue,
      folly::to_underlying(
          MoQMi::HeaderExtensionMediaTypeValues::
              MOQ_EXT_HEADER_TYPE_MOQMI_MEDIA_VALUE_VIDEO_H264_IN_AVCC));

  EXPECT_EQ(
      mi->extensions[1].type,
      folly::to_underlying(
          MoQMi::HeaderExtensionsTypeIDs::
              MOQ_EXT_HEADER_TYPE_MOQMI_VIDEO_H264_IN_AVCC_METADATA));

  XLOG(INFO) << "MOQ_EXT_HEADER_TYPE_MOQMI_VIDEO_H264_IN_AVCC_METADATA: "
             << proxygen::IOBufPrinter::printHexFolly(
                    mi->extensions[1].arrayValue.get(), true);
  folly::IOBufEqualTo eq;
  EXPECT_TRUE(
      eq(mi->extensions[1].arrayValue,
         kTestExpectedh264AVCCMetadataExtensionWire));

  EXPECT_EQ(
      mi->extensions[2].type,
      folly::to_underlying(
          MoQMi::HeaderExtensionsTypeIDs::
              MOQ_EXT_HEADER_TYPE_MOQMI_VIDEO_H264_IN_AVCC_EXTRADATA));
  XLOG(INFO) << "MOQ_EXT_HEADER_TYPE_MOQMI_VIDEO_H264_IN_AVCC_EXTRADATA: "
             << proxygen::IOBufPrinter::printHexFolly(
                    mi->extensions[2].arrayValue.get(), true);
  EXPECT_TRUE(eq(mi->extensions[2].arrayValue, kTestExtradata));
}

TEST(MoQMiTest, EncodeAudioAAC) {
  auto item = std::make_unique<moxygen::MediaItem>(
      kTestData->clone(),
      MediaType::AUDIO,
      0x3FFFFFFFFFFFFF00 /* id */,
      0x3FFFFFFFFFFFFF01 /* pts*/,
      0x3FFFFFFFFFFFFF02 /* timescale */,
      0x3FFFFFFFFFFFFF05 /* duration */,
      0x3FFFFFFFFFFFFF06 /* wallclock */,
      0x3FFFFFFFFFFFFF03 /* sampleFreq */,
      0x3FFFFFFFFFFFFF04 /* NumChannels */,
      false /* EOF*/);

  auto mi = MoQMi::encodeToMoQMi(item->clone());

  EXPECT_NE(mi, nullptr);
  // Check extensions
  EXPECT_EQ(mi->extensions.size(), 2);
  EXPECT_EQ(
      mi->extensions[0].type,
      folly::to_underlying(MoQMi::HeaderExtensionsTypeIDs::
                               MOQ_EXT_HEADER_TYPE_MOQMI_MEDIA_TYPE));
  EXPECT_EQ(
      mi->extensions[0].intValue,
      folly::to_underlying(
          MoQMi::HeaderExtensionMediaTypeValues::
              MOQ_EXT_HEADER_TYPE_MOQMI_MEDIA_VALUE_AUDIO_AUDIO_AACLC_MPEG4));

  EXPECT_EQ(
      mi->extensions[1].type,
      folly::to_underlying(
          MoQMi::HeaderExtensionsTypeIDs::
              MOQ_EXT_HEADER_TYPE_MOQMI_AUDIO_AACLC_MPEG4_METADATA));

  XLOG(INFO) << "MOQ_EXT_HEADER_TYPE_MOQMI_AUDIO_AACLC_MPEG4_METADATA: "
             << proxygen::IOBufPrinter::printHexFolly(
                    mi->extensions[1].arrayValue.get(), true);
  folly::IOBufEqualTo eq;
  EXPECT_TRUE(eq(
      mi->extensions[1].arrayValue, kTestExpectedhAACLCMetadataExtensionWire));
}

TEST(MoQMiTest, DecodeVideoH264TestWithExtradata) {
  std::vector<Extension> extensions;
  extensions.emplace_back(
      folly::to_underlying(
          MoQMi::HeaderExtensionsTypeIDs::MOQ_EXT_HEADER_TYPE_MOQMI_MEDIA_TYPE),
      folly::to_underlying(
          MoQMi::HeaderExtensionMediaTypeValues::
              MOQ_EXT_HEADER_TYPE_MOQMI_MEDIA_VALUE_VIDEO_H264_IN_AVCC)); // MOQMI_MEDIA_TYPE

  extensions.emplace_back(
      folly::to_underlying(
          MoQMi::HeaderExtensionsTypeIDs::
              MOQ_EXT_HEADER_TYPE_MOQMI_VIDEO_H264_IN_AVCC_METADATA),
      kTestExpectedh264AVCCMetadataExtensionWire
          ->clone()); // MOQMI_AVCC_METADATA
  extensions.emplace_back(
      folly::to_underlying(
          MoQMi::HeaderExtensionsTypeIDs::
              MOQ_EXT_HEADER_TYPE_MOQMI_VIDEO_H264_IN_AVCC_EXTRADATA),
      kTestExtradata->clone()); // MOQMI_AVCC_EXTRADATA

  auto obj =
      std::make_unique<MoQMi::MoqMiObject>(extensions, kTestData->clone());

  auto res = MoQMi::decodeMoQMi(std::move(obj));

  EXPECT_NE(std::get<0>(res), nullptr);
  EXPECT_EQ(std::get<0>(res)->seqId, 0x3FFFFFFFFFFFFF00);
  EXPECT_EQ(std::get<0>(res)->pts, 0x3FFFFFFFFFFFFF01);
  EXPECT_EQ(std::get<0>(res)->timescale, 0x3FFFFFFFFFFFFF04);
  EXPECT_EQ(std::get<0>(res)->duration, 0x3FFFFFFFFFFFFF03);
  EXPECT_EQ(std::get<0>(res)->wallclock, 0x3FFFFFFFFFFFFF05);
  EXPECT_EQ(std::get<0>(res)->dts, 0x3FFFFFFFFFFFFF02);
  folly::IOBufEqualTo eq;
  EXPECT_TRUE(eq(std::get<0>(res)->data, kTestData));
  EXPECT_TRUE(eq(std::get<0>(res)->metadata, kTestExtradata));
}

TEST(MoQMiTest, DecodeVideoH264TestNoMetadata) {
  std::vector<Extension> extensions;
  extensions.emplace_back(
      folly::to_underlying(
          MoQMi::HeaderExtensionsTypeIDs::MOQ_EXT_HEADER_TYPE_MOQMI_MEDIA_TYPE),
      folly::to_underlying(
          MoQMi::HeaderExtensionMediaTypeValues::
              MOQ_EXT_HEADER_TYPE_MOQMI_MEDIA_VALUE_VIDEO_H264_IN_AVCC)); // MOQMI_MEDIA_TYPE

  extensions.emplace_back(
      folly::to_underlying(
          MoQMi::HeaderExtensionsTypeIDs::
              MOQ_EXT_HEADER_TYPE_MOQMI_VIDEO_H264_IN_AVCC_METADATA),
      kTestExpectedh264AVCCMetadataExtensionWire
          ->clone()); // MOQMI_AVCC_METADATA

  auto obj =
      std::make_unique<MoQMi::MoqMiObject>(extensions, kTestData->clone());

  auto res = MoQMi::decodeMoQMi(std::move(obj));

  EXPECT_NE(std::get<0>(res), nullptr);
  EXPECT_EQ(std::get<0>(res)->seqId, 0x3FFFFFFFFFFFFF00);
  EXPECT_EQ(std::get<0>(res)->pts, 0x3FFFFFFFFFFFFF01);
  EXPECT_EQ(std::get<0>(res)->timescale, 0x3FFFFFFFFFFFFF04);
  EXPECT_EQ(std::get<0>(res)->duration, 0x3FFFFFFFFFFFFF03);
  EXPECT_EQ(std::get<0>(res)->wallclock, 0x3FFFFFFFFFFFFF05);
  EXPECT_EQ(std::get<0>(res)->dts, 0x3FFFFFFFFFFFFF02);
  folly::IOBufEqualTo eq;
  EXPECT_TRUE(eq(std::get<0>(res)->data, kTestData));
  EXPECT_EQ(std::get<0>(res)->metadata, nullptr);
}

TEST(MoQMiTest, DecodeAudioAAC) {
  std::vector<Extension> extensions;
  extensions.emplace_back(
      folly::to_underlying(
          MoQMi::HeaderExtensionsTypeIDs::MOQ_EXT_HEADER_TYPE_MOQMI_MEDIA_TYPE),
      folly::to_underlying(
          MoQMi::HeaderExtensionMediaTypeValues::
              MOQ_EXT_HEADER_TYPE_MOQMI_MEDIA_VALUE_AUDIO_AUDIO_AACLC_MPEG4)); // MOQMI_MEDIA_TYPE

  extensions.emplace_back(
      folly::to_underlying(
          MoQMi::HeaderExtensionsTypeIDs::
              MOQ_EXT_HEADER_TYPE_MOQMI_AUDIO_AACLC_MPEG4_METADATA),
      kTestExpectedhAACLCMetadataExtensionWire
          ->clone()); // MOQMI_AACLC_METADATA

  auto obj =
      std::make_unique<MoQMi::MoqMiObject>(extensions, kTestData->clone());

  auto res = MoQMi::decodeMoQMi(std::move(obj));

  EXPECT_NE(std::get<1>(res), nullptr);
  EXPECT_EQ(std::get<1>(res)->seqId, 0x3FFFFFFFFFFFFF00);
  EXPECT_EQ(std::get<1>(res)->pts, 0x3FFFFFFFFFFFFF01);
  EXPECT_EQ(std::get<1>(res)->timescale, 0x3FFFFFFFFFFFFF02);
  EXPECT_EQ(std::get<1>(res)->duration, 0x3FFFFFFFFFFFFF05);
  EXPECT_EQ(std::get<1>(res)->wallclock, 0x3FFFFFFFFFFFFF06);
  EXPECT_EQ(std::get<1>(res)->sampleFreq, 0x3FFFFFFFFFFFFF03);
  EXPECT_EQ(std::get<1>(res)->numChannels, 0x3FFFFFFFFFFFFF04);
  folly::IOBufEqualTo eq;
  EXPECT_TRUE(eq(std::get<1>(res)->data, kTestData));
}

TEST(MoQMi, OverrideStreamOpVideoH264) {
  std::string expected =
      "VideoH264. id: 1, pts: 2, dts: 6, timescale: 3, duration: 4, wallclock: 5, metadata length: 21, data length: 17";

  auto dataVideo = std::make_unique<MoQMi::VideoH264AVCCWCPData>(
      1,                       // SeqId
      2,                       // Pts
      3,                       // Timescale
      4,                       // Duration
      5,                       // Wallclock
      kTestData->clone(),      // Data
      kTestExtradata->clone(), // Extradata
      6                        // Dts
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

TEST(MoQMi, IsIdr) {
  auto dataVideoIdr = std::make_unique<MoQMi::VideoH264AVCCWCPData>(
      1,                          // SeqId
      2,                          // Pts
      3,                          // Timescale
      4,                          // Duration
      5,                          // Wallclock
      kTestVideoDataIDR->clone(), // Data
      kTestExtradata->clone(),    // Extradata
      6                           // Dts
  );
  auto dataVideoNoIdr = std::make_unique<MoQMi::VideoH264AVCCWCPData>(
      1,                            // SeqId
      2,                            // Pts
      3,                            // Timescale
      10,                           // Duration
      5,                            // Wallclock
      kTestVideoDataNoIDR->clone(), // Data
      kTestExtradata->clone(),      // Extradata
      6                             // Dts
  );

  XLOG(INFO) << "dataVideoIdr: " << *dataVideoIdr;
  XLOG(INFO) << "dataVideoIdr->data: "
             << proxygen::IOBufPrinter::printHexFolly(
                    dataVideoIdr->data.get(), true);

  XLOG(INFO) << "dataVideoNoIdr: " << *dataVideoNoIdr;
  XLOG(INFO) << "dataVideoNoIdr->data: "
             << proxygen::IOBufPrinter::printHexFolly(
                    dataVideoNoIdr->data.get(), true);

  EXPECT_TRUE(dataVideoIdr->isIdr());
  EXPECT_FALSE(dataVideoNoIdr->isIdr());
}

TEST(MoQMi, AscAudioHeader) {
  flv::AscHeaderData expectedAsc = flv::AscHeaderData(2, 48000, 2);
  auto audioData = std::make_unique<MoQMi::AudioAACMP4LCWCPData>(
      0x3FFFFFFFFFFFFF00,     // SeqId
      0x3FFFFFFFFFFFFF01,     // Pts
      0x3FFFFFFFFFFFFF04,     // Timescale
      0x3FFFFFFFFFFFFF03,     // Duration
      0x3FFFFFFFFFFFFF05,     // Wallclock
      kTestData->clone(),     // Data
      expectedAsc.sampleFreq, // SampleFreq
      expectedAsc.channels    // NumChannels
  );

  auto ascHeaderData = audioData->getAscHeader();
  XLOG(INFO) << "ascHeaderData: "
             << proxygen::IOBufPrinter::printHexFolly(
                    ascHeaderData.get(), true);
  auto ascHeaderDataDecoded = flv::parseAscHeader(ascHeaderData->clone());
  XLOG(INFO) << "expectedAsc: " << expectedAsc;
  XLOG(INFO) << "ascHeaderDataDecoded: " << ascHeaderDataDecoded;

  EXPECT_TRUE(ascHeaderDataDecoded == expectedAsc);
}

} // namespace moxygen

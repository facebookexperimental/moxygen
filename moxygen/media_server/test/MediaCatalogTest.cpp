/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <moxygen/media_server/MediaCatalog.h>

#include <folly/json.h>
#include <folly/portability/GTest.h>

#include <cstdint>
#include <string>

namespace moxygen::media_server { namespace {

folly::ByteRange asByteRange(const std::string& value) {
  return folly::ByteRange{
      reinterpret_cast<const uint8_t*>(value.data()), value.size()};
}

TEST(MediaCatalogTest, SerializeAndParseRoundTrip) {
  MediaCatalog catalog;
  catalog.version = "draft-01";
  catalog.generatedAt = 1720000000;
  catalog.tracks.push_back(
      CatalogTrack{
          .name = "video-main",
          .role = "video",
          .packaging = "cmaf",
          .isLive = true,
          .initRef = "video-init",
          .altGroup = 7,
          .codec = "avc1.640028",
          .mimeType = "video/mp4",
          .framerate = 30,
          .bitrate = 4000000,
          .width = 1920,
          .height = 1080,
          .sourceFile = "video.mp4",
      });
  catalog.initDataList.push_back(
      CatalogInitData{
          .id = "video-init",
          .type = "inline",
          .data = "AAAAIGZ0eXA=",
      });

  auto serialized = serializeCatalog(catalog);
  auto expected = folly::parseJson(R"JSON({
    "version": "draft-01",
    "generatedAt": 1720000000,
    "tracks": [{
      "name": "video-main",
      "role": "video",
      "packaging": "cmaf",
      "isLive": true,
      "initRef": "video-init",
      "altGroup": 7,
      "codec": "avc1.640028",
      "mimeType": "video/mp4",
      "framerate": 30,
      "bitrate": 4000000,
      "width": 1920,
      "height": 1080
    }],
    "initDataList": [{
      "id": "video-init",
      "type": "inline",
      "data": "AAAAIGZ0eXA="
    }]
  })JSON");
  EXPECT_EQ(folly::parseJson(serialized), expected);

  auto parsed = parseCatalog(asByteRange(serialized));
  ASSERT_TRUE(parsed.has_value());
  EXPECT_EQ(parsed->version, catalog.version);
  EXPECT_EQ(parsed->generatedAt, catalog.generatedAt);
  ASSERT_EQ(parsed->tracks.size(), 1);
  const auto& track = parsed->tracks.front();
  EXPECT_EQ(track.name, "video-main");
  EXPECT_EQ(track.role, "video");
  EXPECT_EQ(track.packaging, "cmaf");
  EXPECT_TRUE(track.isLive);
  EXPECT_EQ(track.initRef, "video-init");
  EXPECT_EQ(track.altGroup, 7);
  EXPECT_EQ(track.codec, "avc1.640028");
  EXPECT_EQ(track.mimeType, "video/mp4");
  EXPECT_EQ(track.framerate, 30);
  EXPECT_EQ(track.bitrate, 4000000);
  EXPECT_EQ(track.width, 1920);
  EXPECT_EQ(track.height, 1080);
  EXPECT_FALSE(track.samplerate.has_value());
  EXPECT_TRUE(track.channelConfig.empty());
  EXPECT_TRUE(track.sourceFile.empty());
  ASSERT_EQ(parsed->initDataList.size(), 1);
  EXPECT_EQ(parsed->initDataList.front().id, "video-init");
  EXPECT_EQ(parsed->initDataList.front().type, "inline");
  EXPECT_EQ(parsed->initDataList.front().data, "AAAAIGZ0eXA=");
}

TEST(MediaCatalogTest, ParseAppliesDefaultsAndIgnoresUnknownFields) {
  const std::string json = R"JSON({
    "tracks": [{
      "name": "audio-main",
      "sourceFile": "audio.mp4",
      "unknownTrackField": "ignored"
    }],
    "initDataList": [{
      "id": "audio-init"
    }],
    "unknownCatalogField": "ignored"
  })JSON";

  auto parsed = parseCatalog(asByteRange(json));
  ASSERT_TRUE(parsed.has_value());
  EXPECT_EQ(parsed->version, "draft-01");
  EXPECT_FALSE(parsed->generatedAt.has_value());
  ASSERT_EQ(parsed->tracks.size(), 1);
  EXPECT_EQ(parsed->tracks.front().name, "audio-main");
  EXPECT_TRUE(parsed->tracks.front().isLive);
  EXPECT_EQ(parsed->tracks.front().sourceFile, "audio.mp4");
  ASSERT_EQ(parsed->initDataList.size(), 1);
  EXPECT_EQ(parsed->initDataList.front().id, "audio-init");
  EXPECT_EQ(parsed->initDataList.front().type, "inline");
  EXPECT_TRUE(parsed->initDataList.front().data.empty());
}

TEST(MediaCatalogTest, ParseRejectsMalformedJson) {
  const std::string json = R"JSON({"tracks": [})JSON";
  EXPECT_FALSE(parseCatalog(asByteRange(json)).has_value());
}

}} // namespace moxygen::media_server

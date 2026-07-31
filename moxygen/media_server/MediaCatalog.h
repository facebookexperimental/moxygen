/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/Range.h>

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace moxygen::media_server {

// Well-known track name carrying a namespace's catalog, per the MOQT Streaming
// Format (draft-ietf-moq-msf) and its CMAF profile (draft-ietf-moq-cmsf). A
// client subscribes to this track first (with a joining FETCH at offset 0) to
// obtain the current catalog, which lists the media tracks and carries each
// track's initialization segment inline, then subscribes to the media tracks.
inline constexpr std::string_view kCatalogTrackName = "catalog";

// One track entry in the catalog (MSF section 5.2). Only the fields we populate
// today are modeled; parsing ignores unknown fields, so the schema can grow
// toward the full MSF/CMSF set without breaking older readers.
struct CatalogTrack {
  std::string name;                // 5.2.3 (required) MOQT track name
  std::string role;                // 5.2.6 "video" | "audio" | "caption" | ...
  std::string packaging;           // 5.2.4 "cmaf" (CMSF) | "loc" | ...
  bool isLive{true};               // 5.2.7 (required)
  std::string initRef;             // 5.2.13 -> id in the catalog's initDataList
  std::optional<int32_t> altGroup; // 5.2.12 switching-set (ABR) id
  std::string codec;               // 5.2.18
  std::string mimeType;            // 5.2.19
  std::optional<int32_t> framerate;  // 5.2.20
  std::optional<int64_t> bitrate;    // 5.2.22
  std::optional<int32_t> width;      // 5.2.26
  std::optional<int32_t> height;     // 5.2.27
  std::optional<int32_t> samplerate; // 5.2.28
  std::string channelConfig;         // 5.2.29
  // Demo-only (not part of MSF, not serialized): the fragmented-MP4 file
  // backing this track, resolved relative to the catalog file.
  std::string sourceFile;
};

// One initialization-data entry (MSF section 5.1.7). This version of the spec
// defines only type "inline": `data` is the base64 of the init segment (the
// CMAF header, i.e. ftyp+moov). Tracks reference an entry via initRef.
struct CatalogInitData {
  std::string id;
  std::string type{"inline"};
  std::string data; // base64 for type "inline"
};

// A catalog document (MSF section 5.1). Serialized as JSON on the catalog
// track.
struct MediaCatalog {
  std::string version{"draft-01"};           // 5.1.1 (required)
  std::optional<int64_t> generatedAt;        // 5.1.2
  std::vector<CatalogTrack> tracks;          // 5.1.4 (required)
  std::vector<CatalogInitData> initDataList; // 5.1.7
};

// Serialize to the catalog JSON document (tracks first, initDataList last, per
// the spec's readability note).
std::string serializeCatalog(const MediaCatalog& catalog);

// Parse a catalog JSON document; std::nullopt on malformed input.
std::optional<MediaCatalog> parseCatalog(folly::ByteRange json);

} // namespace moxygen::media_server

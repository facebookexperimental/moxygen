/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <moxygen/msf/MediaCatalog.h>

#include <folly/dynamic.h>
#include <folly/json.h>
#include <folly/logging/xlog.h>

namespace moxygen {

namespace {
void putIfSet(folly::dynamic& obj, const char* key, const std::string& v) {
  if (!v.empty()) {
    obj[key] = v;
  }
}
} // namespace

std::string serializeCatalog(const MediaCatalog& catalog) {
  folly::dynamic tracks = folly::dynamic::array;
  for (const auto& track : catalog.tracks) {
    folly::dynamic obj = folly::dynamic::object;
    obj["name"] = track.name;
    putIfSet(obj, "role", track.role);
    putIfSet(obj, "packaging", track.packaging);
    obj["isLive"] = track.isLive;
    putIfSet(obj, "initRef", track.initRef);
    if (track.renderGroup) {
      obj["renderGroup"] = *track.renderGroup;
    }
    if (track.altGroup) {
      obj["altGroup"] = *track.altGroup;
    }
    putIfSet(obj, "codec", track.codec);
    putIfSet(obj, "mimeType", track.mimeType);
    if (track.framerate) {
      obj["framerate"] = *track.framerate;
    }
    if (track.bitrate) {
      obj["bitrate"] = *track.bitrate;
    }
    if (track.avgBitrate) {
      obj["avgBitrate"] = *track.avgBitrate;
    }
    if (track.width) {
      obj["width"] = *track.width;
    }
    if (track.height) {
      obj["height"] = *track.height;
    }
    if (track.samplerate) {
      obj["samplerate"] = *track.samplerate;
    }
    putIfSet(obj, "channelConfig", track.channelConfig);
    tracks.push_back(std::move(obj));
  }

  folly::dynamic initList = folly::dynamic::array;
  for (const auto& initData : catalog.initDataList) {
    folly::dynamic obj = folly::dynamic::object;
    obj["id"] = initData.id;
    obj["type"] = initData.type;
    obj["data"] = initData.data;
    initList.push_back(std::move(obj));
  }

  folly::dynamic root = folly::dynamic::object;
  root["version"] = catalog.version;
  if (catalog.generatedAt) {
    root["generatedAt"] = *catalog.generatedAt;
  }
  root["tracks"] = std::move(tracks);
  if (!catalog.initDataList.empty()) {
    root["initDataList"] = std::move(initList);
  }
  return folly::toJson(root);
}

std::optional<MediaCatalog> parseCatalog(folly::ByteRange json) {
  try {
    auto root = folly::parseJson(
        folly::StringPiece(
            reinterpret_cast<const char*>(json.data()), json.size()));
    MediaCatalog catalog;
    catalog.version = root.getDefault("version", "draft-01").asString();
    if (const auto* g = root.get_ptr("generatedAt")) {
      catalog.generatedAt = g->asInt();
    }
    for (const auto& track : root["tracks"]) {
      CatalogTrack info;
      info.name = track["name"].asString();
      info.role = track.getDefault("role", "").asString();
      info.packaging = track.getDefault("packaging", "").asString();
      info.isLive = track.getDefault("isLive", true).asBool();
      info.initRef = track.getDefault("initRef", "").asString();
      if (const auto* r = track.get_ptr("renderGroup")) {
        info.renderGroup = static_cast<int32_t>(r->asInt());
      }
      if (const auto* a = track.get_ptr("altGroup")) {
        info.altGroup = static_cast<int32_t>(a->asInt());
      }
      info.codec = track.getDefault("codec", "").asString();
      info.mimeType = track.getDefault("mimeType", "").asString();
      if (const auto* f = track.get_ptr("framerate")) {
        info.framerate = static_cast<int32_t>(f->asInt());
      }
      if (const auto* b = track.get_ptr("bitrate")) {
        info.bitrate = b->asInt();
      }
      if (const auto* b = track.get_ptr("avgBitrate")) {
        info.avgBitrate = b->asInt();
      }
      if (const auto* w = track.get_ptr("width")) {
        info.width = static_cast<int32_t>(w->asInt());
      }
      if (const auto* h = track.get_ptr("height")) {
        info.height = static_cast<int32_t>(h->asInt());
      }
      if (const auto* s = track.get_ptr("samplerate")) {
        info.samplerate = static_cast<int32_t>(s->asInt());
      }
      info.channelConfig = track.getDefault("channelConfig", "").asString();
      info.sourceFile = track.getDefault("sourceFile", "").asString();
      catalog.tracks.push_back(std::move(info));
    }
    if (const auto* initList = root.get_ptr("initDataList")) {
      for (const auto& initData : *initList) {
        CatalogInitData init;
        init.id = initData["id"].asString();
        init.type = initData.getDefault("type", "inline").asString();
        init.data = initData.getDefault("data", "").asString();
        catalog.initDataList.push_back(std::move(init));
      }
    }
    return catalog;
  } catch (const std::exception& ex) {
    XLOG(ERR) << "[MediaCatalog] parse failed: " << ex.what();
    return std::nullopt;
  }
}

} // namespace moxygen

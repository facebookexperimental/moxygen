/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <moxygen/media_server/MediaCatalog.h>

#include <folly/dynamic.h>
#include <folly/json.h>
#include <folly/logging/xlog.h>

namespace moxygen::media_server {

namespace {
void putIfSet(folly::dynamic& obj, const char* key, const std::string& v) {
  if (!v.empty()) {
    obj[key] = v;
  }
}
} // namespace

std::string serializeCatalog(const MediaCatalog& catalog) {
  folly::dynamic tracks = folly::dynamic::array;
  for (const auto& t : catalog.tracks) {
    folly::dynamic obj = folly::dynamic::object;
    obj["name"] = t.name;
    putIfSet(obj, "role", t.role);
    putIfSet(obj, "packaging", t.packaging);
    obj["isLive"] = t.isLive;
    putIfSet(obj, "initRef", t.initRef);
    if (t.altGroup) {
      obj["altGroup"] = *t.altGroup;
    }
    putIfSet(obj, "codec", t.codec);
    putIfSet(obj, "mimeType", t.mimeType);
    if (t.framerate) {
      obj["framerate"] = *t.framerate;
    }
    if (t.bitrate) {
      obj["bitrate"] = *t.bitrate;
    }
    if (t.width) {
      obj["width"] = *t.width;
    }
    if (t.height) {
      obj["height"] = *t.height;
    }
    if (t.samplerate) {
      obj["samplerate"] = *t.samplerate;
    }
    putIfSet(obj, "channelConfig", t.channelConfig);
    tracks.push_back(std::move(obj));
  }

  folly::dynamic initList = folly::dynamic::array;
  for (const auto& i : catalog.initDataList) {
    folly::dynamic obj = folly::dynamic::object;
    obj["id"] = i.id;
    obj["type"] = i.type;
    obj["data"] = i.data;
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
    for (const auto& t : root["tracks"]) {
      CatalogTrack info;
      info.name = t["name"].asString();
      info.role = t.getDefault("role", "").asString();
      info.packaging = t.getDefault("packaging", "").asString();
      info.isLive = t.getDefault("isLive", true).asBool();
      info.initRef = t.getDefault("initRef", "").asString();
      if (const auto* a = t.get_ptr("altGroup")) {
        info.altGroup = static_cast<int32_t>(a->asInt());
      }
      info.codec = t.getDefault("codec", "").asString();
      info.mimeType = t.getDefault("mimeType", "").asString();
      if (const auto* f = t.get_ptr("framerate")) {
        info.framerate = static_cast<int32_t>(f->asInt());
      }
      if (const auto* b = t.get_ptr("bitrate")) {
        info.bitrate = b->asInt();
      }
      if (const auto* w = t.get_ptr("width")) {
        info.width = static_cast<int32_t>(w->asInt());
      }
      if (const auto* h = t.get_ptr("height")) {
        info.height = static_cast<int32_t>(h->asInt());
      }
      if (const auto* s = t.get_ptr("samplerate")) {
        info.samplerate = static_cast<int32_t>(s->asInt());
      }
      info.channelConfig = t.getDefault("channelConfig", "").asString();
      info.sourceFile = t.getDefault("sourceFile", "").asString();
      catalog.tracks.push_back(std::move(info));
    }
    if (const auto* initList = root.get_ptr("initDataList")) {
      for (const auto& i : *initList) {
        CatalogInitData init;
        init.id = i["id"].asString();
        init.type = i.getDefault("type", "inline").asString();
        init.data = i.getDefault("data", "").asString();
        catalog.initDataList.push_back(std::move(init));
      }
    }
    return catalog;
  } catch (const std::exception& ex) {
    XLOG(ERR) << "[MediaCatalog] parse failed: " << ex.what();
    return std::nullopt;
  }
}

} // namespace moxygen::media_server

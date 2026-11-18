/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/io/IOBuf.h>

#include <algorithm>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace moxygen::lane_manager {

// One lane of a switching set: a track the edge can splice into a virtual
// track. Shaped like MoQ Track Properties, so a catalog-less reader can fill it
// in place of the catalog.
struct LaneInfo {
  std::string trackName;
  std::string codec;
  std::optional<uint64_t> bitrateBps; // peak
  std::optional<uint64_t> avgBitrateBps;
  std::optional<uint32_t> width;
  std::optional<uint32_t> height;
  std::optional<uint32_t> sampleRate;
  // The lane's CMAF header, sent to the client when the lane starts on the
  // virtual track. Never parsed.
  std::shared_ptr<const folly::IOBuf> init;
};

// The lanes one subscription set can switch between, lowest first: by
// `bitrateBps` when every lane has one, otherwise by resolution (video) or
// sample rate (audio), with ties in catalog order.
struct SwitchingSet {
  int32_t id{0};
  std::vector<LaneInfo> lanes;
  // Bits per second a video policy leaves for audio, from the catalog's audio
  // track.
  std::optional<uint64_t> audioAllowanceBps;

  const LaneInfo* find(std::string_view trackName) const {
    auto it =
        std::find_if(lanes.begin(), lanes.end(), [&](const LaneInfo& lane) {
          return lane.trackName == trackName;
        });
    return it == lanes.end() ? nullptr : &*it;
  }
};

} // namespace moxygen::lane_manager

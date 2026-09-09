/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace moxygen::media_server {

struct ChunkedCmafObject {
  std::string bytes;
  bool isSync{false};
};

struct ChunkedCmafFragment {
  uint64_t baseMediaDecodeTime{0};
  std::vector<ChunkedCmafObject> objects;
};

// Repackages a single-track CMAF fragment into one independently parseable
// moof+mdat chunk per encoded sample. The original fragment remains the MoQ
// group; the returned chunks become consecutive objects in that group.
class CmafFrameChunker {
 public:
  explicit CmafFrameChunker(std::string_view initializationSegment);

  std::optional<ChunkedCmafFragment> chunk(std::string_view fragment);

 private:
  struct TrackDefaults {
    uint32_t trackId{0};
    uint32_t sampleDescriptionIndex{1};
    uint32_t sampleDuration{0};
    uint32_t sampleSize{0};
    uint32_t sampleFlags{0};
  };

  std::optional<TrackDefaults> defaults_;
  uint32_t nextSequenceNumber_{1};
};

} // namespace moxygen::media_server

/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/moq_mi/MediaItem.h"

namespace moxygen {

std::ostream& operator<<(std::ostream& os, MediaItem const& a) {
  std::string mediaTypeStr = "Unknown";
  if (a.type == MediaType::VIDEO) {
    mediaTypeStr = "VIDEO";
  } else if (a.type == MediaType::AUDIO) {
    mediaTypeStr = "AUDIO";
  }
  auto metadataSize =
      a.metadata != nullptr ? a.metadata->computeChainDataLength() : 0;
  auto dataSize = a.data != nullptr ? a.data->computeChainDataLength() : 0;

  os << "Type: " << mediaTypeStr << ". id: " << a.id << ", pts: " << a.pts
     << ", dts: " << a.dts << ", timescale: " << a.timescale
     << ", duration: " << a.duration << ", wallclock: " << a.wallclock
     << ", metadata length: " << metadataSize << ", data length: " << dataSize;
  return os;
}

} // namespace moxygen

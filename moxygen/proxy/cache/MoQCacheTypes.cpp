/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/proxy/cache/MoQCacheTypes.h"

#include <limits>

namespace moxygen {
namespace {

constexpr size_t kGroupMetadataBytes = 256;
constexpr size_t kObjectMetadataBytes = 96;
constexpr size_t kExtensionMetadataBytes = 32;
constexpr size_t kPlanMetadataBytes = 64;
constexpr size_t kPlanEntryBytes = sizeof(MoQReplayPlan::Entry);

size_t saturatingAdd(size_t a, size_t b) {
  return a > std::numeric_limits<size_t>::max() - b
      ? std::numeric_limits<size_t>::max()
      : a + b;
}

size_t extensionBytes(const std::vector<Extension>& extensions) {
  size_t bytes = 0;
  for (const auto& extension : extensions) {
    bytes = saturatingAdd(bytes, kExtensionMetadataBytes);
    if (extension.arrayValue) {
      bytes =
          saturatingAdd(bytes, extension.arrayValue->computeChainDataLength());
    }
  }
  return bytes;
}

} // namespace

size_t groupMetadataChargeBytes() {
  return kGroupMetadataBytes;
}

size_t objectChargeBytes(uint64_t payloadLength, const Extensions& extensions) {
  return saturatingAdd(
      saturatingAdd(kObjectMetadataBytes, payloadLength),
      saturatingAdd(
          extensionBytes(extensions.getMutableExtensions()),
          extensionBytes(extensions.getImmutableExtensions())));
}

size_t planChargeBytes(size_t entryCount) {
  const size_t entries =
      entryCount > std::numeric_limits<size_t>::max() / kPlanEntryBytes
      ? std::numeric_limits<size_t>::max()
      : entryCount * kPlanEntryBytes;
  return saturatingAdd(kPlanMetadataBytes, entries);
}

} // namespace moxygen

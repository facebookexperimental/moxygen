/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <moxygen/MoQVersions.h>

#include <folly/Conv.h>
#include <algorithm>

namespace {

bool isDraftVariant(uint64_t version) {
  return (version & 0x00ff0000);
}

} // namespace

namespace moxygen {

uint64_t getDraftMajorVersion(uint64_t version) {
  if (isDraftVariant(version)) {
    return (version & 0x00ff0000) >> 16;
  } else {
    return (version & 0x0000ffff);
  }
}

bool isSupportedVersion(uint64_t version) {
  return (
      std::find(
          kSupportedVersions.begin(), kSupportedVersions.end(), version) !=
      kSupportedVersions.end());
}

bool isLegacyAlpn(std::string_view alpn) {
  return alpn == kAlpnMoqtLegacy;
}

std::vector<uint64_t> getSupportedLegacyVersions() {
  std::vector<uint64_t> supportedLegacyVers;
  for (auto& version : kSupportedVersions) {
    if (getDraftMajorVersion(version) < 15) {
      supportedLegacyVers.push_back(version);
    }
  }
  return supportedLegacyVers;
}

std::optional<uint64_t> getVersionFromAlpn(std::string_view alpn) {
  // Parse "[moqt-{N} | moqt-{N}-meta-{NN}]" format (for draft 15+)
  if (alpn.starts_with("moqt-")) {
    auto draftStr = alpn.substr(5); // skip "moqt-"

    // Extract just the draft number (first 1-2 digits before any hyphen or end)
    auto hyphenPos = draftStr.find('-');
    if (hyphenPos != std::string::npos) {
      draftStr = draftStr.substr(0, hyphenPos);
    }

    auto draftNum = folly::tryTo<uint64_t>(draftStr);
    if (draftNum.hasValue() && draftNum.value() >= 15) {
      return 0xff000000 | draftNum.value();
    }
  }
  return std::nullopt;
}

std::optional<std::string> getAlpnFromVersion(uint64_t version) {
  uint64_t draftNum = getDraftMajorVersion(version);

  // Drafts < 15 use legacy ALPN "moq-00"
  if (draftNum < 15) {
    return std::string(kAlpnMoqtLegacy);
  }
  if (draftNum == 15) {
    return std::string(kAlpnMoqtDraft15Latest);
  }
  return std::string(kAlpnMoqtDraft16Latest);
}

std::vector<std::string> getDefaultMoqtProtocols(bool includeExperimental) {
  std::vector<std::string> protocols;
  if (includeExperimental) {
    protocols.emplace_back(kAlpnMoqtDraft15Latest);
    protocols.emplace_back(kAlpnMoqtDraft16Latest);
  }
  protocols.emplace_back(kAlpnMoqtLegacy);
  return protocols;
}

std::string getSupportedVersionsString() {
  std::string result;
  for (size_t i = 0; i < kSupportedVersions.size(); ++i) {
    if (i > 0) {
      result += ",";
    }
    result += folly::to<std::string>(kSupportedVersions[i]);
  }
  return result;
}

} // namespace moxygen

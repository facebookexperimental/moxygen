/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <moxygen/MoQVersions.h>

#include <folly/Conv.h>
#include <folly/String.h>
#include <folly/logging/xlog.h>
#include <algorithm>
#include <stdexcept>

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

std::optional<uint64_t> getVersionFromAlpn(std::string_view alpn) {
  // Parse "[moqt-{N} | moqt-{N}-meta-{NN}]" format
  if (alpn.starts_with("moqt-")) {
    auto draftStr = alpn.substr(5); // skip "moqt-"

    // Extract just the draft number (first 1-2 digits before any hyphen or end)
    auto hyphenPos = draftStr.find('-');
    if (hyphenPos != std::string::npos) {
      draftStr = draftStr.substr(0, hyphenPos);
    }

    auto draftNum = folly::tryTo<uint64_t>(draftStr);
    if (draftNum.hasValue()) {
      uint64_t version = 0xff000000 | draftNum.value();
      if (isSupportedVersion(version)) {
        return version;
      }
    }
  }
  return std::nullopt;
}

std::optional<std::string> getAlpnFromVersion(
    uint64_t version,
    bool useStandard) {
  if (!isSupportedVersion(version)) {
    return std::nullopt;
  }
  uint64_t draftNum = getDraftMajorVersion(version);
  // Only part of draft 22 is implemented, so it never claims the standard
  // ALPN, whatever the caller asked for.
  if (draftNum == 22) {
    return std::string(kAlpnMoqtDraft22Latest);
  }
  if (useStandard) {
    return "moqt-" + folly::to<std::string>(draftNum);
  }
  if (draftNum == 18) {
    return std::string(kAlpnMoqtDraft18Latest);
  }
  if (draftNum == 16) {
    return std::string(kAlpnMoqtDraft16Latest);
  }
  return std::nullopt;
}

std::vector<std::string> getDefaultMoqtProtocols(
    bool includeExperimental,
    bool useStandard) {
  // Highest version first so TLS ALPN negotiation prefers it.
  std::vector<std::string> protocols;
  if (includeExperimental) {
    protocols.push_back(
        getAlpnFromVersion(kVersionDraft18, useStandard).value());
  }
  protocols.push_back(getAlpnFromVersion(kVersionDraft16, useStandard).value());
  if (includeExperimental) {
    // Out of order on purpose: draft 22 is still partial, so offer it for
    // peers that ask but never prefer it.
    protocols.push_back(
        getAlpnFromVersion(kVersionDraft22, useStandard).value());
  }
  return protocols;
}

std::vector<std::string> getMoqtProtocols(
    const std::string& versions,
    bool useStandard) {
  if (versions.empty()) {
    return getDefaultMoqtProtocols(true, useStandard);
  }
  std::vector<std::string> protocols;
  std::vector<folly::StringPiece> parts;
  folly::split(',', versions, parts);
  for (auto& part : parts) {
    auto trimmed = folly::trimWhitespace(part);
    auto draftNum = folly::tryTo<int>(trimmed);
    if (draftNum.hasValue()) {
      uint64_t versionCode =
          0xff000000 | static_cast<uint64_t>(draftNum.value());
      auto alpn = getAlpnFromVersion(versionCode, useStandard);
      if (!alpn.has_value()) {
        XLOG(ERR) << "Skipping unsupported MoQT draft version " << trimmed;
        continue;
      }
      if (std::find(protocols.begin(), protocols.end(), alpn.value()) ==
          protocols.end()) {
        protocols.push_back(alpn.value());
      }
    }
  }
  if (protocols.empty()) {
    throw std::invalid_argument(
        "no supported MoQT draft version in '" + versions + "'");
  }
  return protocols;
}

bool useUniControlStreams(uint64_t version) {
  return getDraftMajorVersion(version) >= 18;
}

bool useBidiRequestStreams(uint64_t version) {
  return getDraftMajorVersion(version) >= 18;
}

bool useFillFetch(uint64_t version) {
  return getDraftMajorVersion(version) >= 22;
}

bool useLocationFilter(uint64_t version) {
  return getDraftMajorVersion(version) >= 22;
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

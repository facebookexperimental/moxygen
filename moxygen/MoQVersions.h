/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <array>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace moxygen {

constexpr uint64_t kVersionDraft16 = 0xff000010;
constexpr uint64_t kVersionDraft17 = 0xff000011;
constexpr uint64_t kVersionDraft18 = 0xff000012;
constexpr uint64_t kVersionDraft19 = 0xff000013;
constexpr uint64_t kVersionDraft20 = 0xff000014;
constexpr uint64_t kVersionDraft21 = 0xff000015;
constexpr uint64_t kVersionDraft22 = 0xff000016;

constexpr uint64_t kVersionDraftCurrent = kVersionDraft16;

// ALPN constants for draft 16 version negotiations
constexpr std::string_view kAlpnMoqtDraft16Meta00 = "moqt-16-meta-00";
constexpr std::string_view kAlpnMoqtDraft16Latest = kAlpnMoqtDraft16Meta00;

// ALPN constants for draft 18 (uni control streams)
constexpr std::string_view kAlpnMoqtDraft18 = "moqt-18";
constexpr std::string_view kAlpnMoqtDraft18Latest = kAlpnMoqtDraft18;

// Meta-scoped draft 22 ALPNs, since only part of the draft is implemented.
// Each wire-breaking step adds a revision and moves kAlpnMoqtDraft22Latest.
constexpr std::string_view kAlpnMoqtDraft22Meta00 = "moqt-22-meta-00";
constexpr std::string_view kAlpnMoqtDraft22Latest = kAlpnMoqtDraft22Meta00;

constexpr std::array<uint64_t, 3> kSupportedVersions{
    kVersionDraft16,
    kVersionDraft18,
    kVersionDraft22};

// In the terminology I'm using for this function, each draft has a "major"
// and a "minor" version. For example, 0xff080002 has the major version 8 and
// minor version 2.
uint64_t getDraftMajorVersion(uint64_t version);

// ALPN utility functions
// Returns the supported MoQT version an ALPN or WT protocol names, if any.
std::optional<uint64_t> getVersionFromAlpn(std::string_view alpn);
std::optional<std::string> getAlpnFromVersion(
    uint64_t version,
    bool useStandard = false);

// Returns the default list of supported MoQT protocols
// includeExperimental: if true, includes experimental/draft protocols
// useStandard: if true, uses standard ALPNs (moqt-NN) instead of Meta-specific
std::vector<std::string> getDefaultMoqtProtocols(
    bool includeExperimental = false,
    bool useStandard = false);

// Returns a list of MoQT ALPNs for the given draft versions.
// versions: comma-separated draft numbers (e.g. "16,18"). Empty = all
// supported; unsupported versions are skipped, and a list with none supported
// throws std::invalid_argument. useStandard: if true, uses standard ALPNs
// (moqt-NN) instead of Meta-specific
std::vector<std::string> getMoqtProtocols(
    const std::string& versions = "",
    bool useStandard = false);

bool isSupportedVersion(uint64_t version);

// Returns true if the given version uses unidirectional control streams
bool useUniControlStreams(uint64_t version);

// True if each request travels on its own bidi stream (draft >= 18). QUIC
// bidi stream limits govern request flow control (MAX_REQUEST_ID /
// REQUESTS_BLOCKED and the MAX_REQUEST_ID setup parameter were removed in
// draft 18); also disables request-order-dependent encodings (e.g. auth
// token aliasing) and request-frame-on-control-stream patterns.
bool useBidiRequestStreams(uint64_t version);

// True if a subscription can request a fill with FILL_PARAMETERS.
bool useFillFetch(uint64_t version);

// True if the request range is carried in LOCATION_FILTER (draft 22+).
bool useLocationFilter(uint64_t version);

// Returns a comma-separated list of supported versions, useful for logging.
std::string getSupportedVersionsString();

} // namespace moxygen

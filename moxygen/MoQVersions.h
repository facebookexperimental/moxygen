/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
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

constexpr uint64_t kVersionDraft01 = 0xff000001;
constexpr uint64_t kVersionDraft02 = 0xff000002;
constexpr uint64_t kVersionDraft03 = 0xff000003;
constexpr uint64_t kVersionDraft04 = 0xff000004;
constexpr uint64_t kVersionDraft05 = 0xff000005;
constexpr uint64_t kVersionDraft06 = 0xff000006;
constexpr uint64_t kVersionDraft06_exp =
    0xff060004; // Draft 6 in progress version
constexpr uint64_t kVersionDraft07 = 0xff000007;
constexpr uint64_t kVersionDraft07_exp = 0xff070001; // Draft 7 FETCH support
constexpr uint64_t kVersionDraft07_exp2 =
    0xff070002; // Draft 7 FETCH + removal of Subscribe ID on objects
constexpr uint64_t kVersionDraft08 = 0xff000008;
constexpr uint64_t kVersionDraft08_exp1 = 0xff080001; // Draft 8 no ROLE
// PUBLISH_DONE stream count
constexpr uint64_t kVersionDraft08_exp2 = 0xff080002;
constexpr uint64_t kVersionDraft08_exp3 = 0xff080003; // Draft 8 datagram status
constexpr uint64_t kVersionDraft08_exp4 = 0xff080004; // Draft 8 END_OF_TRACK
constexpr uint64_t kVersionDraft08_exp5 = 0xff080005; // Draft 8 Joining FETCH
constexpr uint64_t kVersionDraft08_exp6 = 0xff080006; // Draft 8 End Group
constexpr uint64_t kVersionDraft08_exp7 = 0xff080007; // Draft 8 Error Codes
constexpr uint64_t kVersionDraft08_exp8 = 0xff080008; // Draft 8 Sub Done codes
constexpr uint64_t kVersionDraft08_exp9 = 0xff080009; // Draft 8 Extensions

constexpr uint64_t kVersionDraft09 = 0xff000009;
constexpr uint64_t kVersionDraft10 = 0xff00000A;
constexpr uint64_t kVersionDraft12 = 0xff00000C;
constexpr uint64_t kVersionDraft13 = 0xff00000D;
constexpr uint64_t kVersionDraft14 = 0xff00000E;
constexpr uint64_t kVersionDraft15 = 0xff00000F;
constexpr uint64_t kVersionDraft16 = 0xff000010;

constexpr uint64_t kVersionDraftCurrent = kVersionDraft14;

// ALPN constants for version negotiation
constexpr std::string_view kAlpnMoqtLegacy = "moq-00";
constexpr std::string_view kAlpnMoqtDraft15Meta00 = "moqt-15-meta-00";
constexpr std::string_view kAlpnMoqtDraft15Meta01 = "moqt-15-meta-01";
constexpr std::string_view kAlpnMoqtDraft15Meta02 = "moqt-15-meta-02";
constexpr std::string_view kAlpnMoqtDraft15Meta03 = "moqt-15-meta-03";
constexpr std::string_view kAlpnMoqtDraft15Meta04 = "moqt-15-meta-04";
constexpr std::string_view kAlpnMoqtDraft15Meta05 = "moqt-15-meta-05";
constexpr std::string_view kAlpnMoqtDraft15Latest = kAlpnMoqtDraft15Meta05;

// ALPN constants for draft 16 version negotiations
constexpr std::string_view kAlpnMoqtDraft16Meta00 = "moqt-16-meta-00";
constexpr std::string_view kAlpnMoqtDraft16Latest = kAlpnMoqtDraft16Meta00;

constexpr std::array<uint64_t, 3> kSupportedVersions{
    kVersionDraft14,
    kVersionDraft15,
    kVersionDraft16};

// In the terminology I'm using for this function, each draft has a "major"
// and a "minor" version. For example, kVersionDraft08_exp2 has the major
// version 8 and minor version 2.
uint64_t getDraftMajorVersion(uint64_t version);

// ALPN utility functions
bool isLegacyAlpn(std::string_view alpn);
std::vector<uint64_t> getSupportedLegacyVersions();
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
// versions: comma-separated draft numbers (e.g. "14,16"). Empty = all
// supported. useStandard: if true, uses standard ALPNs (moqt-NN) instead of
// Meta-specific
std::vector<std::string> getMoqtProtocols(
    const std::string& versions = "",
    bool useStandard = false);

bool isSupportedVersion(uint64_t version);

// Returns a comma-separated list of supported versions, useful for logging.
std::string getSupportedVersionsString();

} // namespace moxygen

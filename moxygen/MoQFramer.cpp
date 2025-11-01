/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/MoQFramer.h"
#include <folly/lang/Bits.h>
#include <folly/logging/xlog.h>

namespace {
constexpr uint64_t kMaxExtensionLength = 1024;

bool isDraftVariant(uint64_t version) {
  return (version & 0x00ff0000);
}

uint64_t getLocationTypeValue(
    moxygen::LocationType locationType,
    uint64_t version) {
  if (locationType != moxygen::LocationType::LargestGroup) {
    return folly::to_underlying(locationType);
  }

  return folly::to_underlying(locationType);
}

// Used in draft 11 and above
} // namespace

namespace moxygen {

// Forward declarations for iOS.
enum class ParamsType { ClientSetup, ServerSetup, Request };
folly::Expected<folly::Unit, ErrorCode> parseParams(
    folly::io::Cursor& cursor,
    size_t& length,
    uint64_t version,
    size_t numParams,
    std::vector<Parameter>& params,
    MoQTokenCache& tokenCache,
    ParamsType paramsType);
folly::Expected<folly::Optional<AuthToken>, ErrorCode> parseToken(
    folly::io::Cursor& cursor,
    size_t length,
    MoQTokenCache& tokenCache,
    ParamsType paramsType) noexcept;
folly::Expected<folly::Optional<Parameter>, ErrorCode> parseVariableParam(
    folly::io::Cursor& cursor,
    size_t& length,
    uint64_t version,
    uint64_t key,
    MoQTokenCache& tokenCache,
    ParamsType paramsType);
folly::Expected<folly::Optional<Parameter>, ErrorCode> parseIntParam(
    folly::io::Cursor& cursor,
    size_t& length,
    uint64_t version,
    uint64_t key);
bool datagramTypeHasExtensions(uint64_t version, DatagramType streamType);
bool datagramTypeIsStatus(uint64_t version, DatagramType streamType);
bool datagramObjectIdZero(uint64_t version, DatagramType datagramType);

void writeFixedString(
    folly::IOBufQueue& writeBuf,
    const std::string& str,
    size_t& size,
    bool& error);
void writeFixedTuple(
    folly::IOBufQueue& writeBuf,
    const std::vector<std::string>& tup,
    size_t& size,
    bool& error);
void writeTrackNamespace(
    folly::IOBufQueue& writeBuf,
    const TrackNamespace& tn,
    size_t& size,
    bool& error);
uint16_t*
writeFrameHeader(folly::IOBufQueue& writeBuf, FrameType frameType, bool& error);
void writeSize(uint16_t* sizePtr, size_t size, bool& error, uint64_t versionIn);

void writeFullTrackName(
    folly::IOBufQueue& writeBuf,
    const FullTrackName& fullTrackName,
    size_t& size,
    bool error);
bool includeSetupParam(uint64_t version, SetupKey key);

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

bool isLegacyAlpn(folly::StringPiece alpn) {
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

folly::Optional<uint64_t> getVersionFromAlpn(folly::StringPiece alpn) {
  // Parse "moqt-{N}" format
  if (alpn.startsWith("moqt-")) {
    auto draftStr = alpn.subpiece(5); // skip "moqt-"
    auto draftNum = folly::tryTo<uint64_t>(draftStr);
    if (draftNum.hasValue() && draftNum.value() >= 15) {
      return 0xff000000 | draftNum.value();
    }
  }
  return folly::none;
}

folly::Optional<std::string> getAlpnFromVersion(uint64_t version) {
  uint64_t draftNum = getDraftMajorVersion(version);

  // Drafts < 15 use legacy ALPN "moq-00"
  if (draftNum < 15) {
    return std::string(kAlpnMoqtLegacy);
  }

  // Draft 15+ use "moqt-{N}" format
  return folly::to<std::string>("moqt-", draftNum);
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

folly::Optional<uint64_t> getFirstIntParam(
    const std::vector<TrackRequestParameter>& params,
    TrackRequestParamKey key) {
  auto keyValue = folly::to_underlying(key);
  for (const auto& param : params) {
    if (param.key == keyValue) {
      return param.asUint64;
    }
  }
  return folly::none;
}

std::string toString(LocationType loctype) {
  switch (loctype) {
    case LocationType::NextGroupStart: {
      return "NextGroupStart";
    }
    case LocationType::AbsoluteStart: {
      return "AbsoluteStart";
    }
    case LocationType::LargestObject: {
      return "LargestObject";
    }
    case LocationType::AbsoluteRange: {
      return "AbsoluteRange";
    }
    case LocationType::LargestGroup: {
      return "LargestGroup";
    }
    default: {
      return "Unknown";
    }
  }
}

folly::Expected<std::string, ErrorCode> parseFixedString(
    folly::io::Cursor& cursor,
    size_t& length) {
  auto strLength = quic::follyutils::decodeQuicInteger(cursor, length);
  if (!strLength) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= strLength->second;
  if (strLength->first > length) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  auto res = cursor.readFixedString(strLength->first);
  length -= strLength->first;
  return res;
}

folly::Expected<folly::Optional<AuthToken>, ErrorCode> parseToken(
    folly::io::Cursor& cursor,
    size_t length,
    MoQTokenCache& tokenCache,
    ParamsType paramsType) noexcept {
  folly::Optional<AuthToken> token;
  token.emplace(); // plan for success
  auto aliasType = quic::follyutils::decodeQuicInteger(cursor, length);
  if (!aliasType) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  if (aliasType->first > folly::to_underlying(AliasType::USE_VALUE)) {
    XLOG(ERR) << "aliasType > USE_VALUE =" << aliasType->first;
    return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
  }
  auto aliasTypeVal = static_cast<AliasType>(aliasType->first);
  length -= aliasType->second;

  switch (aliasTypeVal) {
    case AliasType::DELETE:
    case AliasType::USE_ALIAS: {
      if (paramsType == ParamsType::ClientSetup) {
        XLOG(ERR) << "Can't delete/use-alias in client setup";
        return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
      }
      auto tokenAlias = quic::follyutils::decodeQuicInteger(cursor, length);
      if (!tokenAlias) {
        return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
      }
      length -= tokenAlias->second;
      token->alias = tokenAlias->first;

      if (aliasTypeVal == AliasType::DELETE) {
        auto deleteRes = tokenCache.deleteToken(*token->alias);
        if (!deleteRes) {
          XLOG(ERR) << "Unknown Auth Token Alias for delete, alias="
                    << *token->alias
                    << ", paramsType=" << folly::to_underlying(paramsType);
          return folly::makeUnexpected(ErrorCode::UNKNOWN_AUTH_TOKEN_ALIAS);
        }
        token.reset();
      } else {
        auto lookupRes = tokenCache.getTokenForAlias(*token->alias);
        if (!lookupRes) {
          XLOG(ERR) << "Unknown Auth Token Alias for use_alias, alias="
                    << *token->alias
                    << ", paramsType=" << folly::to_underlying(paramsType);
          return folly::makeUnexpected(ErrorCode::UNKNOWN_AUTH_TOKEN_ALIAS);
        }
        token->tokenType = lookupRes->tokenType;
        token->tokenValue = std::move(lookupRes->tokenValue);
      }
    } break;
    case AliasType::REGISTER: {
      auto tokenAlias = quic::follyutils::decodeQuicInteger(cursor, length);
      if (!tokenAlias) {
        return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
      }
      length -= tokenAlias->second;
      token->alias = tokenAlias->first;

      auto tokenType = quic::follyutils::decodeQuicInteger(cursor, length);
      if (!tokenType) {
        return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
      }
      length -= tokenType->second;
      token->tokenType = tokenType->first;

      token->tokenValue = cursor.readFixedString(length);
      length -= token->tokenValue.size();
      // ClientSetup is allowed to send REGISTERs that exceed the server's
      // limit, which are treated as USE_VALUE
      if (paramsType != ParamsType::ClientSetup ||
          tokenCache.canRegister(token->tokenType, token->tokenValue)) {
        auto registerRes = tokenCache.registerToken(
            *token->alias, token->tokenType, token->tokenValue);
        if (!registerRes) {
          if (registerRes.error() ==
              MoQTokenCache::ErrorCode::DUPLICATE_ALIAS) {
            XLOG(ERR) << "Duplicate token alias registered alias="
                      << *token->alias;
            return folly::makeUnexpected(ErrorCode::DUPLICATE_AUTH_TOKEN_ALIAS);
          } else if (
              registerRes.error() == MoQTokenCache::ErrorCode::LIMIT_EXCEEDED) {
            XLOG(ERR) << "Auth token cache overflow";
            return folly::makeUnexpected(ErrorCode::AUTH_TOKEN_CACHE_OVERFLOW);
          } else {
            XLOG(ERR) << "Unknown token registration error="
                      << uint32_t(registerRes.error());
            return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
          }
        }
      } else {
        XLOG(WARN)
            << "Converting too-large CLIENT_SETUP register to USE_VALUE alias="
            << *token->alias << " value=" << token->tokenValue;
      }
    } break;
    case AliasType::USE_VALUE: {
      auto tokenType = quic::follyutils::decodeQuicInteger(cursor, length);
      if (!tokenType) {
        return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
      }
      length -= tokenType->second;
      token->tokenType = tokenType->first;
      token->tokenValue = cursor.readFixedString(length);
      length -= token->tokenValue.size();
    } break;
    default:
      XLOG(ERR) << "Unknown Auth Token op code=" << aliasType->first;
      return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
  }

  if (length > 0) {
    XLOG(ERR) << "Invalid token length";
    return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
  }

  return token;
}

folly::Expected<folly::Optional<Parameter>, ErrorCode> parseVariableParam(
    folly::io::Cursor& cursor,
    size_t& length,
    uint64_t version,
    uint64_t key,
    MoQTokenCache& tokenCache,
    ParamsType paramsType) {
  Parameter p;
  p.key = key;
  // Formatted Auth Tokens from v11
  // Auth in setup from v12
  const auto authKey =
      folly::to_underlying(TrackRequestParamKey::AUTHORIZATION_TOKEN);
  if (key == authKey) {
    auto res = quic::follyutils::decodeQuicInteger(cursor, length);
    if (!res) {
      return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
    }
    length -= res->second;
    if (res->first > length || !cursor.canAdvance(res->first)) {
      return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
    }
    auto tokenRes = parseToken(cursor, res->first, tokenCache, paramsType);
    if (!tokenRes) {
      return folly::makeUnexpected(tokenRes.error());
    }
    length -= res->first;
    if (!tokenRes.value().has_value()) {
      // it was delete, don't export
      return folly::none;
    }
    p.asAuthToken = std::move(*tokenRes.value());
  } else {
    auto res = parseFixedString(cursor, length);
    if (!res) {
      return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
    }
    p.asString = std::move(res.value());
  }
  return p;
}

folly::Expected<folly::Optional<Parameter>, ErrorCode> parseIntParam(
    folly::io::Cursor& cursor,
    size_t& length,
    uint64_t version,
    uint64_t key) {
  Parameter p;
  p.key = key;
  auto res = quic::follyutils::decodeQuicInteger(cursor, length);
  if (!res) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= res->second;
  p.asUint64 = res->first;
  return p;
}

folly::Expected<folly::Unit, ErrorCode> parseParams(
    folly::io::Cursor& cursor,
    size_t& length,
    uint64_t version,
    size_t numParams,
    std::vector<Parameter>& params,
    MoQTokenCache& tokenCache,
    ParamsType paramsType) {
  for (auto i = 0u; i < numParams; i++) {
    auto key = quic::follyutils::decodeQuicInteger(cursor, length);
    if (!key) {
      return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
    }
    length -= key->second;
    folly::Expected<folly::Optional<Parameter>, ErrorCode> res;

    if ((paramsType == ParamsType::Request &&
         key->first ==
             folly::to_underlying(TrackRequestParamKey::DELIVERY_TIMEOUT)) ||
        ((key->first & 0x01) == 0 &&
         (paramsType != ParamsType::Request ||
          key->first !=
              folly::to_underlying(
                  TrackRequestParamKey::AUTHORIZATION_TOKEN)))) {
      res = parseIntParam(cursor, length, version, key->first);
    } else {
      res = parseVariableParam(
          cursor, length, version, key->first, tokenCache, paramsType);
    }
    if (!res) {
      return folly::makeUnexpected(res.error());
    }
    if (*res) {
      params.emplace_back(std::move(*res.value()));
    } // else the param was not an error but shouldn't be added to the set
  }
  if (length > 0) {
    XLOG(ERR) << "Invalid key-value length";
    return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
  }
  return folly::unit;
}

folly::Expected<ClientSetup, ErrorCode> MoQFrameParser::parseClientSetup(
    folly::io::Cursor& cursor,
    size_t length) noexcept {
  ClientSetup clientSetup;
  uint64_t serializationVersion = kVersionDraft12;

  // Only parse version array when version is not initialized, i.e. alpn did not
  // happen, or when version is initialized but is < 15 (in tests)
  if (!version_ || getDraftMajorVersion(*version_) < 15) {
    auto numVersions = quic::follyutils::decodeQuicInteger(cursor, length);
    if (!numVersions) {
      return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
    }
    length -= numVersions->second;
    for (auto i = 0ul; i < numVersions->first; i++) {
      auto version = quic::follyutils::decodeQuicInteger(cursor, length);
      if (!version) {
        return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
      }
      if (!isSupportedVersion(version->first)) {
        XLOG(WARN) << "Peer advertised unsupported version " << version->first
                   << ", supported versions are: "
                   << getSupportedVersionsString();
        return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
      }
      clientSetup.supportedVersions.push_back(version->first);
      length -= version->second;
    }
  } else {
    XLOG(DBG3)
        << "Skipped parsing versions from wire for alpn ClientSetup message";
    serializationVersion = *version_;
  }

  auto numParams = quic::follyutils::decodeQuicInteger(cursor, length);
  if (!numParams) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= numParams->second;
  auto res = parseParams(
      cursor,
      length,
      serializationVersion,
      numParams->first,
      clientSetup.params,
      tokenCache_,
      ParamsType::ClientSetup);
  if (res.hasError()) {
    return folly::makeUnexpected(res.error());
  }
  if (length > 0) {
    return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
  }
  return clientSetup;
}

folly::Expected<ServerSetup, ErrorCode> MoQFrameParser::parseServerSetup(
    folly::io::Cursor& cursor,
    size_t length) noexcept {
  ServerSetup serverSetup;

  // Only parse version when version is not initialized, i.e. alpn did not
  // happen, or when version is initialized but is < 15 (in tests)
  if (!version_ || getDraftMajorVersion(*version_) < 15) {
    auto version = quic::follyutils::decodeQuicInteger(cursor, length);
    if (!version) {
      return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
    }
    length -= version->second;
    if (!isSupportedVersion(version->first)) {
      XLOG(WARN) << "Peer advertised unsupported version " << version->first
                 << ", supported versions are: "
                 << getSupportedVersionsString();
      return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
    }
    serverSetup.selectedVersion = version->first;
  } else {
    XLOG(DBG3)
        << "Skipped parsing version from wire for alpn ServerSetup message";
  }

  auto numParams = quic::follyutils::decodeQuicInteger(cursor, length);
  if (!numParams) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= numParams->second;
  auto res = parseParams(
      cursor,
      length,
      version_ ? *version_ : serverSetup.selectedVersion,
      numParams->first,
      serverSetup.params,
      tokenCache_,
      ParamsType::ServerSetup);
  if (res.hasError()) {
    return folly::makeUnexpected(res.error());
  }
  if (length > 0) {
    return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
  }
  return serverSetup;
}

folly::Expected<RequestID, ErrorCode> MoQFrameParser::parseFetchHeader(
    folly::io::Cursor& cursor) const noexcept {
  auto requestID = quic::follyutils::decodeQuicInteger(cursor);
  if (!requestID) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  return RequestID(requestID->first);
}

bool datagramTypeHasExtensions(uint64_t version, DatagramType datagramType) {
  return (folly::to_underlying(datagramType) & 0x1);
}

bool datagramTypeIsStatus(uint64_t version, DatagramType datagramType) {
  return getDraftMajorVersion(version) < 12
      ? datagramType == DatagramType::OBJECT_DATAGRAM_STATUS_V11 ||
          datagramType == DatagramType::OBJECT_DATAGRAM_STATUS_EXT_V11
      : (folly::to_underlying(datagramType) & 0x20);
}

bool datagramObjectIdZero(uint64_t version, DatagramType datagramType) {
  // 0 objectID type bit only supported in ver-14 and above
  if (getDraftMajorVersion(version) < 14) {
    return false;
  }
  return (folly::to_underlying(datagramType) & DG_OBJECT_ID_ZERO);
}

folly::Expected<DatagramObjectHeader, ErrorCode>
MoQFrameParser::parseDatagramObjectHeader(
    folly::io::Cursor& cursor,
    DatagramType datagramType,
    size_t& length) const noexcept {
  ObjectHeader objectHeader;
  auto trackAlias = quic::follyutils::decodeQuicInteger(cursor, length);
  if (!trackAlias) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= trackAlias->second;

  auto group = quic::follyutils::decodeQuicInteger(cursor, length);
  if (!group) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= group->second;
  objectHeader.group = group->first;

  if (!datagramObjectIdZero(*version_, datagramType)) {
    auto id = quic::follyutils::decodeQuicInteger(cursor, length);
    if (!id) {
      return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
    }
    length -= id->second;
    objectHeader.id = id->first;
  } else {
    // objectID=0 is not on the wire
    objectHeader.id = 0;
  }

  if (length == 0 || !cursor.canAdvance(1)) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  objectHeader.priority = cursor.readBE<uint8_t>();
  length -= 1;
  if (datagramTypeHasExtensions(*version_, datagramType)) {
    auto ext = parseExtensions(cursor, length, objectHeader);
    if (!ext) {
      return folly::makeUnexpected(ext.error());
    }
  }

  if (datagramTypeIsStatus(*version_, datagramType)) {
    auto status = quic::follyutils::decodeQuicInteger(cursor, length);
    if (!status) {
      return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
    }
    length -= status->second;
    if (status->first > folly::to_underlying(ObjectStatus::END_OF_TRACK)) {
      XLOG(ERR) << "status > END_OF_TRACK =" << status->first;
      return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
    }
    objectHeader.status = ObjectStatus(status->first);
    objectHeader.length = 0;
    if (length != 0) {
      // MUST consume entire datagram
      XLOG(ERR) << "Non-zero length payload in OBJECT_DATAGRAM_STATUS";
      return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
    }
  } else {
    objectHeader.status = ObjectStatus::NORMAL;
    objectHeader.length = length;
  }
  return DatagramObjectHeader(
      TrackAlias(trackAlias->first), std::move(objectHeader));
}

folly::Expected<folly::Optional<TrackAlias>, ErrorCode>
MoQFrameParser::parseSubgroupTypeAndAlias(
    folly::io::Cursor& cursor,
    size_t length) const noexcept {
  CHECK(version_.hasValue())
      << "The version must be set before parsing subgroup type and alias";

  auto type = quic::follyutils::decodeQuicInteger(cursor);
  if (!type) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= type->second;

  if (getDraftMajorVersion(*version_) >= 12 &&
      (type->first & folly::to_underlying(StreamType::SUBGROUP_HEADER_MASK)) ==
          0) {
    return folly::none;
  } else if (
      getDraftMajorVersion(*version_) == 11 &&
      (type->first &
       folly::to_underlying(StreamType::SUBGROUP_HEADER_MASK_V11)) == 0) {
    return folly::none;
  }

  auto trackAlias = quic::follyutils::decodeQuicInteger(cursor, length);
  if (!trackAlias) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= trackAlias->second;
  return TrackAlias(trackAlias->first);
}

folly::Expected<MoQFrameParser::SubgroupHeaderResult, ErrorCode>
MoQFrameParser::parseSubgroupHeader(
    folly::io::Cursor& cursor,
    SubgroupIDFormat format,
    bool /* includeExtensions*/) const noexcept {
  auto length = cursor.totalLength();
  SubgroupHeaderResult result;
  ObjectHeader& objectHeader = result.objectHeader;
  objectHeader.group = std::numeric_limits<uint64_t>::max(); // unset
  objectHeader.id = std::numeric_limits<uint64_t>::max();    // unset

  auto parsedTrackAlias = quic::follyutils::decodeQuicInteger(cursor, length);
  if (!parsedTrackAlias) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= parsedTrackAlias->second;
  result.trackAlias = TrackAlias(parsedTrackAlias->first);

  auto group = quic::follyutils::decodeQuicInteger(cursor, length);
  if (!group) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= group->second;
  objectHeader.group = group->first;

  bool parseObjectID = false;
  if (format == SubgroupIDFormat::Present) {
    auto subgroup = quic::follyutils::decodeQuicInteger(cursor, length);
    if (!subgroup) {
      return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
    }
    objectHeader.subgroup = subgroup->first;
    length -= subgroup->second;
  } else if (format == SubgroupIDFormat::Zero) {
    objectHeader.subgroup = 0;
  } else {
    parseObjectID = true;
  }
  if (length > 0 || cursor.canAdvance(1)) {
    objectHeader.priority = cursor.readBE<uint8_t>();
    length -= 1;
  } else {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  if (parseObjectID) {
    auto tmpCursor = cursor; // we reparse the object ID later
    auto id = quic::follyutils::decodeQuicInteger(tmpCursor, length);
    if (!id) {
      return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
    }
    objectHeader.subgroup = objectHeader.id = id->first;
  }
  return result;
}
folly::Expected<folly::Unit, ErrorCode>
MoQFrameParser::parseObjectStatusAndLength(
    folly::io::Cursor& cursor,
    size_t length,
    ObjectHeader& objectHeader) const noexcept {
  auto payloadLength = quic::follyutils::decodeQuicInteger(cursor, length);
  if (!payloadLength) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= payloadLength->second;
  objectHeader.length = payloadLength->first;

  if (objectHeader.length == 0) {
    auto objectStatus = quic::follyutils::decodeQuicInteger(cursor, length);
    if (!objectStatus) {
      return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
    }
    if (objectStatus->first >
        folly::to_underlying(ObjectStatus::END_OF_TRACK)) {
      XLOG(ERR) << "status > END_OF_TRACK =" << objectStatus->first;
      return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
    }
    objectHeader.status = ObjectStatus(objectStatus->first);
    length -= objectStatus->second;
  } else {
    objectHeader.status = ObjectStatus::NORMAL;
  }

  return folly::unit;
}

folly::Expected<ObjectHeader, ErrorCode> MoQFrameParser::parseFetchObjectHeader(
    folly::io::Cursor& cursor,
    const ObjectHeader& headerTemplate) const noexcept {
  // TODO get rid of this
  auto length = cursor.totalLength();
  ObjectHeader objectHeader = headerTemplate;

  auto group = quic::follyutils::decodeQuicInteger(cursor, length);
  if (!group) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= group->second;
  objectHeader.group = group->first;

  auto subgroup = quic::follyutils::decodeQuicInteger(cursor, length);
  if (!subgroup) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= subgroup->second;
  objectHeader.subgroup = subgroup->first;

  auto id = quic::follyutils::decodeQuicInteger(cursor, length);
  if (!id) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= id->second;
  objectHeader.id = id->first;

  if (length < 2) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  objectHeader.priority = cursor.readBE<uint8_t>();
  length--;

  auto ext = parseExtensions(cursor, length, objectHeader);
  if (!ext) {
    return folly::makeUnexpected(ext.error());
  }

  auto res = parseObjectStatusAndLength(cursor, length, objectHeader);
  if (!res) {
    return folly::makeUnexpected(res.error());
  }
  return objectHeader;
}

folly::Expected<ObjectHeader, ErrorCode>
MoQFrameParser::parseSubgroupObjectHeader(
    folly::io::Cursor& cursor,
    const ObjectHeader& headerTemplate,
    SubgroupIDFormat /*format*/,
    bool includeExtensions) const noexcept {
  // TODO get rid of this
  auto length = cursor.totalLength();
  ObjectHeader objectHeader = headerTemplate;
  auto id = quic::follyutils::decodeQuicInteger(cursor, length);
  if (!id) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= id->second;
  objectHeader.id = id->first;
  XCHECK(version_.hasValue())
      << "The version must be set before parsing subgroup object header";
  if (getDraftMajorVersion(*version_) >= 14) {
    // Delta encoded object ID
    uint64_t objectIDDelta = id->first;
    if (previousObjectID_.has_value()) {
      objectHeader.id = previousObjectID_.value() + objectIDDelta + 1;
    } else {
      objectHeader.id = objectIDDelta;
    }
    previousObjectID_ = objectHeader.id;
  }

  if (includeExtensions) {
    auto ext = parseExtensions(cursor, length, objectHeader);
    if (!ext) {
      return folly::makeUnexpected(ext.error());
    }
  }

  auto res = parseObjectStatusAndLength(cursor, length, objectHeader);
  if (!res) {
    return folly::makeUnexpected(res.error());
  }
  return objectHeader;
}

folly::Expected<folly::Unit, ErrorCode> MoQFrameParser::parseTrackRequestParams(
    folly::io::Cursor& cursor,
    size_t& length,
    size_t numParams,
    std::vector<TrackRequestParameter>& params) const noexcept {
  CHECK(version_.hasValue())
      << "The version must be set before parsing track request params";
  return parseParams(
      cursor,
      length,
      *version_,
      numParams,
      params,
      tokenCache_,
      ParamsType::Request);
}

folly::Expected<SubscribeRequest, ErrorCode>
MoQFrameParser::parseSubscribeRequest(folly::io::Cursor& cursor, size_t length)
    const noexcept {
  CHECK(version_.hasValue())
      << "The version must be set before parsing a subscribe request";
  SubscribeRequest subscribeRequest;
  auto requestID = quic::follyutils::decodeQuicInteger(cursor, length);
  if (!requestID) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= requestID->second;
  subscribeRequest.requestID = requestID->first;
  if (getDraftMajorVersion(*version_) < 12) {
    auto trackAlias = quic::follyutils::decodeQuicInteger(cursor, length);
    if (!trackAlias) {
      return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
    }
    length -= trackAlias->second;
    subscribeRequest.trackAlias = trackAlias->first;
  }
  auto res = parseFullTrackName(cursor, length);
  if (!res) {
    return folly::makeUnexpected(res.error());
  }
  subscribeRequest.fullTrackName = std::move(res.value());
  if (length < 2) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  subscribeRequest.priority = cursor.readBE<uint8_t>();
  auto order = cursor.readBE<uint8_t>();
  if (order > folly::to_underlying(GroupOrder::NewestFirst)) {
    XLOG(ERR) << "order > NewestFirst =" << order;
    return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
  }
  subscribeRequest.groupOrder = static_cast<GroupOrder>(order);
  length -= 2;
  if (length < 1) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  uint8_t forwardFlag = cursor.readBE<uint8_t>();
  if (forwardFlag > 1) {
    return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
  }
  subscribeRequest.forward = (forwardFlag == 1);
  length--;
  auto locType = quic::follyutils::decodeQuicInteger(cursor, length);
  if (!locType) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  switch (locType->first) {
    case folly::to_underlying(LocationType::LargestObject):
    case folly::to_underlying(LocationType::LargestGroup):
    case folly::to_underlying(LocationType::AbsoluteStart):
    case folly::to_underlying(LocationType::AbsoluteRange):
    case folly::to_underlying(LocationType::NextGroupStart):
      break;
    default:
      XLOG(ERR) << "Invalid locType =" << locType->first;
      return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
  }
  length -= locType->second;
  subscribeRequest.locType = LocationType(locType->first);
  if (subscribeRequest.locType == LocationType::AbsoluteStart ||
      subscribeRequest.locType == LocationType::AbsoluteRange) {
    auto location = parseAbsoluteLocation(cursor, length);
    if (!location) {
      return folly::makeUnexpected(location.error());
    }
    subscribeRequest.start = *location;
  }
  if (subscribeRequest.locType == LocationType::AbsoluteRange) {
    auto endGroup = quic::follyutils::decodeQuicInteger(cursor, length);
    if (!endGroup) {
      return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
    }
    subscribeRequest.endGroup = endGroup->first;
    length -= endGroup->second;
  }
  auto numParams = quic::follyutils::decodeQuicInteger(cursor, length);
  if (!numParams) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= numParams->second;
  auto res2 = parseTrackRequestParams(
      cursor, length, numParams->first, subscribeRequest.params);
  if (!res2) {
    return folly::makeUnexpected(res2.error());
  }
  if (length > 0) {
    return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
  }
  return subscribeRequest;
}

folly::Expected<SubscribeUpdate, ErrorCode>
MoQFrameParser::parseSubscribeUpdate(folly::io::Cursor& cursor, size_t length)
    const noexcept {
  CHECK(version_.hasValue())
      << "The version must be set before parsing a subscribe update";

  SubscribeUpdate subscribeUpdate;
  auto requestID = quic::follyutils::decodeQuicInteger(cursor, length);
  if (!requestID) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  subscribeUpdate.requestID = requestID->first;
  length -= requestID->second;

  if (getDraftMajorVersion(*version_) >= 14) {
    auto subscriptionRequestID =
        quic::follyutils::decodeQuicInteger(cursor, length);
    if (!subscriptionRequestID) {
      return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
    }
    subscribeUpdate.subscriptionRequestID = subscriptionRequestID->first;
    length -= subscriptionRequestID->second;
  }

  auto start = parseAbsoluteLocation(cursor, length);
  if (!start) {
    return folly::makeUnexpected(start.error());
  }
  subscribeUpdate.start = start.value();

  auto endGroup = quic::follyutils::decodeQuicInteger(cursor, length);
  if (!endGroup) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  subscribeUpdate.endGroup = endGroup->first;
  length -= endGroup->second;

  if (length < 2) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  subscribeUpdate.priority = cursor.readBE<uint8_t>();
  length--;

  if (length < 2) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  uint8_t forwardFlag = cursor.readBE<uint8_t>();
  if (forwardFlag > 1) {
    return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
  }
  subscribeUpdate.forward = (forwardFlag == 1);
  length--;

  auto numParams = quic::follyutils::decodeQuicInteger(cursor, length);
  if (!numParams) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= numParams->second;

  auto res2 = parseTrackRequestParams(
      cursor, length, numParams->first, subscribeUpdate.params);
  if (!res2) {
    return folly::makeUnexpected(res2.error());
  }
  if (length > 0) {
    return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
  }
  return subscribeUpdate;
}

folly::Expected<SubscribeOk, ErrorCode> MoQFrameParser::parseSubscribeOk(
    folly::io::Cursor& cursor,
    size_t length) const noexcept {
  SubscribeOk subscribeOk;
  auto requestID = quic::follyutils::decodeQuicInteger(cursor, length);
  if (!requestID) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= requestID->second;
  subscribeOk.requestID = requestID->first;
  if (getDraftMajorVersion(*version_) >= 12) {
    auto trackAlias = quic::follyutils::decodeQuicInteger(cursor, length);
    if (!trackAlias) {
      return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
    }
    length -= trackAlias->second;
    subscribeOk.trackAlias = trackAlias->first;
  } else {
    // Session will fill this in
    subscribeOk.trackAlias = 0;
  }

  auto expires = quic::follyutils::decodeQuicInteger(cursor, length);
  if (!expires) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= expires->second;
  subscribeOk.expires = std::chrono::milliseconds(expires->first);
  if (length < 2) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  auto order = cursor.readBE<uint8_t>();
  if (order == 0 || order > folly::to_underlying(GroupOrder::NewestFirst)) {
    XLOG(ERR) << "order > NewestFirst or order==0 =" << order;
    return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
  }
  length -= sizeof(uint8_t);
  subscribeOk.groupOrder = static_cast<GroupOrder>(order);
  auto contentExists = cursor.readBE<uint8_t>();
  length -= sizeof(uint8_t);
  if (contentExists) {
    auto res = parseAbsoluteLocation(cursor, length);
    if (!res) {
      return folly::makeUnexpected(res.error());
    }
    subscribeOk.largest = *res;
  }
  auto numParams = quic::follyutils::decodeQuicInteger(cursor, length);
  if (!numParams) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= numParams->second;
  auto res2 = parseTrackRequestParams(
      cursor, length, numParams->first, subscribeOk.params);
  if (!res2) {
    return folly::makeUnexpected(res2.error());
  }
  if (length > 0) {
    return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
  }
  return subscribeOk;
}

folly::Expected<Unsubscribe, ErrorCode> MoQFrameParser::parseUnsubscribe(
    folly::io::Cursor& cursor,
    size_t length) const noexcept {
  Unsubscribe unsubscribe;
  auto requestID = quic::follyutils::decodeQuicInteger(cursor, length);
  if (!requestID) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= requestID->second;
  unsubscribe.requestID = requestID->first;
  if (length > 0) {
    return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
  }
  return unsubscribe;
}

folly::Expected<SubscribeDone, ErrorCode> MoQFrameParser::parseSubscribeDone(
    folly::io::Cursor& cursor,
    size_t length) const noexcept {
  SubscribeDone subscribeDone;
  auto requestID = quic::follyutils::decodeQuicInteger(cursor, length);
  if (!requestID) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= requestID->second;
  subscribeDone.requestID = requestID->first;

  auto statusCode = quic::follyutils::decodeQuicInteger(cursor, length);
  if (!statusCode) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= statusCode->second;
  subscribeDone.statusCode = SubscribeDoneStatusCode(statusCode->first);

  auto streamCount = quic::follyutils::decodeQuicInteger(cursor, length);
  if (!streamCount) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= streamCount->second;
  subscribeDone.streamCount = streamCount->first;

  auto reas = parseFixedString(cursor, length);
  if (!reas) {
    return folly::makeUnexpected(reas.error());
  }
  subscribeDone.reasonPhrase = std::move(reas.value());

  CHECK(version_.hasValue())
      << "The version must be set before parsing SUBSCRIBE_DONE";
  if (getDraftMajorVersion(*version_) <= 9) {
    if (length == 0) {
      return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
    }
    auto contentExists = cursor.readBE<uint8_t>();
    length -= sizeof(uint8_t);
    if (contentExists) {
      auto res = parseAbsoluteLocation(cursor, length);
      if (!res) {
        return folly::makeUnexpected(res.error());
      }
    }
  }
  if (length > 0) {
    return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
  }
  return subscribeDone;
}

folly::Expected<PublishRequest, ErrorCode> MoQFrameParser::parsePublish(
    folly::io::Cursor& cursor,
    size_t length) const noexcept {
  CHECK(version_.hasValue())
      << "The version must be set before parsing a publish request";
  PublishRequest publish;
  auto requestID = quic::follyutils::decodeQuicInteger(cursor, length);
  if (!requestID) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= requestID->second;
  publish.requestID = requestID->first;

  auto res = parseFullTrackName(cursor, length);
  if (!res) {
    return folly::makeUnexpected(res.error());
  }
  publish.fullTrackName = res.value();

  auto trackAlias = quic::follyutils::decodeQuicInteger(cursor, length);
  if (!trackAlias) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= trackAlias->second;
  publish.trackAlias = trackAlias->first;

  if (length < 3) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  auto order = cursor.readBE<uint8_t>();
  if (order > folly::to_underlying(GroupOrder::NewestFirst)) {
    XLOG(ERR) << "order > NewestFirst =" << order;
    return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
  }
  publish.groupOrder = static_cast<GroupOrder>(order);

  uint8_t contentExists = cursor.readBE<uint8_t>();
  if (contentExists > 1) {
    return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
  }
  length -= 2;

  if (contentExists == 1) {
    auto location = parseAbsoluteLocation(cursor, length);
    if (!location) {
      return folly::makeUnexpected(location.error());
    }
    publish.largest = *location;
  } else {
    publish.largest = folly::none;
  }

  uint8_t forwardFlag = cursor.readBE<uint8_t>();
  if (forwardFlag > 1) {
    return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
  }
  publish.forward = (forwardFlag == 1);
  length--;

  auto numParams = quic::follyutils::decodeQuicInteger(cursor, length);
  if (!numParams) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= numParams->second;
  auto paramRes =
      parseTrackRequestParams(cursor, length, numParams->first, publish.params);
  if (!paramRes) {
    return folly::makeUnexpected(paramRes.error());
  }
  if (length > 0) {
    return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
  }
  return publish;
}

folly::Expected<PublishOk, ErrorCode> MoQFrameParser::parsePublishOk(
    folly::io::Cursor& cursor,
    size_t length) const noexcept {
  CHECK(version_.hasValue())
      << "The version must be set before parsing a publish ok";
  PublishOk publishOk;
  auto requestID = quic::follyutils::decodeQuicInteger(cursor, length);
  if (!requestID) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= requestID->second;
  publishOk.requestID = requestID->first;

  if (length < 3) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  uint8_t forwardFlag = cursor.readBE<uint8_t>();
  if (forwardFlag > 1) {
    return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
  }
  publishOk.forward = (forwardFlag == 1);

  publishOk.subscriberPriority = cursor.readBE<uint8_t>();

  auto order = cursor.readBE<uint8_t>();
  if (order > folly::to_underlying(GroupOrder::NewestFirst)) {
    XLOG(ERR) << "order > NewestFirst =" << order;
    return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
  }
  publishOk.groupOrder = static_cast<GroupOrder>(order);
  length -= 3;

  auto locType = quic::follyutils::decodeQuicInteger(cursor, length);
  if (!locType) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  switch (locType->first) {
    case folly::to_underlying(LocationType::LargestObject):
    case folly::to_underlying(LocationType::LargestGroup):
    case folly::to_underlying(LocationType::AbsoluteStart):
    case folly::to_underlying(LocationType::AbsoluteRange):
    case folly::to_underlying(LocationType::NextGroupStart):
      break;
    default:
      XLOG(ERR) << "Invalid locType =" << locType->first;
      return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
  }
  length -= locType->second;
  publishOk.locType = LocationType(locType->first);

  if (publishOk.locType == LocationType::AbsoluteStart ||
      publishOk.locType == LocationType::AbsoluteRange) {
    auto location = parseAbsoluteLocation(cursor, length);
    if (!location) {
      return folly::makeUnexpected(location.error());
    }
    publishOk.start = *location;
  }
  if (publishOk.locType == LocationType::AbsoluteRange) {
    auto endGroup = quic::follyutils::decodeQuicInteger(cursor, length);
    if (!endGroup) {
      return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
    }
    publishOk.endGroup = endGroup->first;
    length -= endGroup->second;
  } else {
    publishOk.endGroup = folly::none;
  }

  auto numParams = quic::follyutils::decodeQuicInteger(cursor, length);
  if (!numParams) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= numParams->second;
  auto paramRes = parseTrackRequestParams(
      cursor, length, numParams->first, publishOk.params);
  if (!paramRes) {
    return folly::makeUnexpected(paramRes.error());
  }
  if (length > 0) {
    return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
  }
  return publishOk;
}

folly::Expected<Announce, ErrorCode> MoQFrameParser::parseAnnounce(
    folly::io::Cursor& cursor,
    size_t length) const noexcept {
  Announce announce;
  if (getDraftMajorVersion(*version_) >= 11) {
    auto requestID = quic::follyutils::decodeQuicInteger(cursor, length);
    if (!requestID) {
      return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
    }
    length -= requestID->second;
    announce.requestID = requestID->first;
  } else {
    announce.requestID = 0;
  }

  auto res = parseFixedTuple(cursor, length);
  if (!res) {
    return folly::makeUnexpected(res.error());
  }
  announce.trackNamespace = TrackNamespace(std::move(res.value()));
  auto numParams = quic::follyutils::decodeQuicInteger(cursor, length);
  if (!numParams) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= numParams->second;
  auto res2 = parseTrackRequestParams(
      cursor, length, numParams->first, announce.params);
  if (!res2) {
    return folly::makeUnexpected(res2.error());
  }
  if (length > 0) {
    return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
  }
  return announce;
}

folly::Expected<AnnounceOk, ErrorCode> MoQFrameParser::parseAnnounceOk(
    folly::io::Cursor& cursor,
    size_t length) const noexcept {
  return parseRequestOk(cursor, length, FrameType::ANNOUNCE_OK);
}

folly::Expected<AnnounceOk, ErrorCode> MoQFrameParser::parseRequestOk(
    folly::io::Cursor& cursor,
    size_t length,
    FrameType frameType) const noexcept {
  RequestOk requestOk;
  auto requestID = quic::follyutils::decodeQuicInteger(cursor, length);
  if (!requestID) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= requestID->second;
  requestOk.requestID = requestID->first;
  if (getDraftMajorVersion(*version_) > 14) {
    // Parse track request params into requestOk.params
    auto numParams = quic::follyutils::decodeQuicInteger(cursor, length);
    if (!numParams) {
      return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
    }
    length -= numParams->second;
    if (numParams->first > 0) {
      if (frameType == FrameType::ANNOUNCE_OK ||
          frameType == FrameType::SUBSCRIBE_ANNOUNCES_OK) {
        // no params supported
        return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
      }
      auto res = parseTrackRequestParams(
          cursor, length, numParams->first, requestOk.params);
      if (!res) {
        return folly::makeUnexpected(res.error());
      }
    }
  }
  if (length > 0) {
    return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
  }
  return requestOk;
}

folly::Expected<Unannounce, ErrorCode> MoQFrameParser::parseUnannounce(
    folly::io::Cursor& cursor,
    size_t length) const noexcept {
  Unannounce unannounce;
  auto res = parseFixedTuple(cursor, length);
  if (!res) {
    return folly::makeUnexpected(res.error());
  }
  unannounce.trackNamespace = TrackNamespace(std::move(res.value()));
  if (length > 0) {
    return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
  }
  return unannounce;
}

folly::Expected<AnnounceCancel, ErrorCode> MoQFrameParser::parseAnnounceCancel(
    folly::io::Cursor& cursor,
    size_t length) const noexcept {
  AnnounceCancel announceCancel;
  auto res = parseFixedTuple(cursor, length);
  if (!res) {
    return folly::makeUnexpected(res.error());
  }
  announceCancel.trackNamespace = TrackNamespace(std::move(res.value()));

  auto errorCode = quic::follyutils::decodeQuicInteger(cursor, length);
  if (!errorCode) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  announceCancel.errorCode = AnnounceErrorCode(errorCode->first);
  length -= errorCode->second;

  auto res2 = parseFixedString(cursor, length);
  if (!res2) {
    return folly::makeUnexpected(res2.error());
  }
  announceCancel.reasonPhrase = std::move(res2.value());
  if (length > 0) {
    return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
  }
  return announceCancel;
}

folly::Expected<TrackStatus, ErrorCode> MoQFrameParser::parseTrackStatus(
    folly::io::Cursor& cursor,
    size_t length) const noexcept {
  CHECK(version_.hasValue()) << "version_ needs to be set to parse TrackStatus";

  if (getDraftMajorVersion(*version_) >= 14) {
    return parseSubscribeRequest(cursor, length);
  }
  TrackStatus trackStatus;

  // Fill in defaults for new fields added in v14
  trackStatus.priority = kDefaultPriority;
  trackStatus.groupOrder = GroupOrder::Default;
  trackStatus.forward = true;
  trackStatus.locType = LocationType::LargestGroup;
  trackStatus.start = folly::none;
  trackStatus.endGroup = 0;

  if (getDraftMajorVersion(*version_) >= 11) {
    auto requestID = quic::follyutils::decodeQuicInteger(cursor, length);
    if (!requestID) {
      return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
    }
    length -= requestID->second;
    trackStatus.requestID = requestID->first;
  } else {
    trackStatus.requestID = 0;
  }
  auto res = parseFullTrackName(cursor, length);
  if (!res) {
    return folly::makeUnexpected(res.error());
  }
  trackStatus.fullTrackName = std::move(res.value());

  if (getDraftMajorVersion(*version_) >= 11) {
    auto numParams = quic::follyutils::decodeQuicInteger(cursor, length);
    if (!numParams) {
      return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
    }
    length -= numParams->second;
    auto parseParamsResult = parseTrackRequestParams(
        cursor, length, numParams->first, trackStatus.params);
    if (!parseParamsResult) {
      return folly::makeUnexpected(parseParamsResult.error());
    }
  }
  if (length > 0) {
    return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
  }
  return trackStatus;
}

folly::Expected<TrackStatusOk, ErrorCode> MoQFrameParser::parseTrackStatusOk(
    folly::io::Cursor& cursor,
    size_t length) const noexcept {
  CHECK(version_.hasValue())
      << "version_ needs to be set to parse TrackStatusOk";

  if (getDraftMajorVersion(*version_) >= 14) {
    auto res = parseSubscribeOk(cursor, length);
    if (!res) {
      return folly::makeUnexpected(res.error());
    }
    SubscribeOk subOk = res.value();
    return TrackStatusOk(
        {subOk.requestID,
         subOk.trackAlias,
         subOk.expires,
         subOk.groupOrder,
         subOk.largest,
         subOk.params});
  }

  TrackStatusOk trackStatusOk;
  trackStatusOk.trackAlias = TrackAlias{0};
  trackStatusOk.expires = std::chrono::milliseconds(0);
  trackStatusOk.groupOrder = GroupOrder::OldestFirst;
  if (getDraftMajorVersion(*version_) >= 11) {
    auto requestID = quic::follyutils::decodeQuicInteger(cursor, length);
    if (!requestID) {
      return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
    }
    length -= requestID->second;
    trackStatusOk.requestID = requestID->first;
  } else {
    trackStatusOk.requestID = 0;
    auto res = parseFullTrackName(cursor, length);
    if (!res) {
      return folly::makeUnexpected(res.error());
    }
    trackStatusOk.fullTrackName = res.value();
  }
  auto statusCode = quic::follyutils::decodeQuicInteger(cursor, length);
  if (!statusCode) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  if (statusCode->first > folly::to_underlying(TrackStatusCode::UNKNOWN)) {
    XLOG(ERR) << "statusCode > UNKNOWN =" << statusCode->first;
    return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
  }
  trackStatusOk.statusCode = TrackStatusCode(statusCode->first);
  length -= statusCode->second;
  auto location = parseAbsoluteLocation(cursor, length);
  if (!location) {
    return folly::makeUnexpected(location.error());
  }
  trackStatusOk.largest = *location;

  if (getDraftMajorVersion(*version_) >= 11) {
    auto numParams = quic::follyutils::decodeQuicInteger(cursor, length);
    if (!numParams) {
      return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
    }
    length -= numParams->second;
    auto parseParamsResult = parseTrackRequestParams(
        cursor, length, numParams->first, trackStatusOk.params);
    if (!parseParamsResult) {
      return folly::makeUnexpected(parseParamsResult.error());
    }
  }
  if (length > 0) {
    return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
  }

  return trackStatusOk;
}

folly::Expected<TrackStatusError, ErrorCode>
MoQFrameParser::parseTrackStatusError(folly::io::Cursor& cursor, size_t length)
    const noexcept {
  return parseRequestError(cursor, length, FrameType::TRACK_STATUS_ERROR);
}

folly::Expected<Goaway, ErrorCode> MoQFrameParser::parseGoaway(
    folly::io::Cursor& cursor,
    size_t length) const noexcept {
  Goaway goaway;
  auto res = parseFixedString(cursor, length);
  if (!res) {
    return folly::makeUnexpected(res.error());
  }
  goaway.newSessionUri = std::move(res.value());
  if (length > 0) {
    return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
  }
  return goaway;
}

folly::Expected<MaxRequestID, ErrorCode> MoQFrameParser::parseMaxRequestID(
    folly::io::Cursor& cursor,
    size_t length) const noexcept {
  MaxRequestID maxRequestID;
  auto requestID = quic::follyutils::decodeQuicInteger(cursor, length);
  if (!requestID) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= requestID->second;
  maxRequestID.requestID = requestID->first;
  if (length > 0) {
    return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
  }
  return maxRequestID;
}

folly::Expected<RequestsBlocked, ErrorCode>
MoQFrameParser::parseRequestsBlocked(folly::io::Cursor& cursor, size_t length)
    const noexcept {
  RequestsBlocked subscribesBlocked;
  auto res = quic::follyutils::decodeQuicInteger(cursor, length);
  if (!res) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  subscribesBlocked.maxRequestID = res->first;
  length -= res->second;
  if (length > 0) {
    return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
  }
  return subscribesBlocked;
}

folly::Expected<Fetch, ErrorCode> MoQFrameParser::parseFetch(
    folly::io::Cursor& cursor,
    size_t length) const noexcept {
  Fetch fetch;
  auto res = quic::follyutils::decodeQuicInteger(cursor, length);
  if (!res) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  fetch.requestID = res->first;
  length -= res->second;

  if (length < 3) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }

  fetch.priority = cursor.readBE<uint8_t>();
  auto order = cursor.readBE<uint8_t>();
  if (order > folly::to_underlying(GroupOrder::NewestFirst)) {
    XLOG(ERR) << "order > NewestFirst =" << order;
    return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
  }
  fetch.groupOrder = static_cast<GroupOrder>(order);
  length -= 2 * sizeof(uint8_t);

  auto fetchType = quic::follyutils::decodeQuicInteger(cursor, length);
  if (!fetchType) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  if (fetchType->first == 0 ||
      fetchType->first > folly::to_underlying(FetchType::ABSOLUTE_JOINING)) {
    XLOG(ERR) << "fetchType = 0 or fetchType > JONING =" << fetchType->first;
    return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
  }
  length -= fetchType->second;

  FetchType fetchTypeEnum(static_cast<FetchType>(fetchType->first));
  if (fetchTypeEnum == FetchType::STANDALONE) {
    auto ftn = parseFullTrackName(cursor, length);
    if (!ftn) {
      return folly::makeUnexpected(ftn.error());
    }

    auto start = parseAbsoluteLocation(cursor, length);
    if (!start) {
      return folly::makeUnexpected(start.error());
    }

    auto end = parseAbsoluteLocation(cursor, length);
    if (!end) {
      return folly::makeUnexpected(end.error());
    }
    fetch.fullTrackName = std::move(ftn.value());
    fetch.args = StandaloneFetch(start.value(), end.value());
  } else {
    // Relative or absolute join
    auto jsid = quic::follyutils::decodeQuicInteger(cursor, length);
    if (!jsid) {
      return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
    }
    length -= jsid->second;

    auto joiningStart = quic::follyutils::decodeQuicInteger(cursor, length);
    if (!joiningStart) {
      return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
    }
    length -= joiningStart->second;
    // Note fetch.fullTrackName is empty at this point, the session fills it
    // in
    fetch.args = JoiningFetch(
        RequestID(jsid->first), joiningStart->first, fetchTypeEnum);
  }
  auto numParams = quic::follyutils::decodeQuicInteger(cursor, length);
  if (!numParams) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= numParams->second;
  auto res5 =
      parseTrackRequestParams(cursor, length, numParams->first, fetch.params);
  if (!res5) {
    return folly::makeUnexpected(res5.error());
  }
  if (length > 0) {
    return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
  }
  return fetch;
}

folly::Expected<FetchCancel, ErrorCode> MoQFrameParser::parseFetchCancel(
    folly::io::Cursor& cursor,
    size_t length) const noexcept {
  FetchCancel fetchCancel;
  auto res = quic::follyutils::decodeQuicInteger(cursor, length);
  if (!res) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  fetchCancel.requestID = res->first;
  length -= res->second;
  if (length > 0) {
    return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
  }
  return fetchCancel;
}

folly::Expected<FetchOk, ErrorCode> MoQFrameParser::parseFetchOk(
    folly::io::Cursor& cursor,
    size_t length) const noexcept {
  FetchOk fetchOk;
  auto res = quic::follyutils::decodeQuicInteger(cursor, length);
  if (!res) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  fetchOk.requestID = res->first;
  length -= res->second;

  // Check for next two bytes
  if (length < 2) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  auto order = cursor.readBE<uint8_t>();
  if (order > folly::to_underlying(GroupOrder::NewestFirst)) {
    XLOG(ERR) << "order = 0 or order > NewestFirst =" << order;
    return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
  }
  fetchOk.groupOrder = static_cast<GroupOrder>(order);
  fetchOk.endOfTrack = cursor.readBE<uint8_t>();
  length -= 2 * sizeof(uint8_t);

  auto res2 = parseAbsoluteLocation(cursor, length);
  if (!res2) {
    return folly::makeUnexpected(res2.error());
  }
  fetchOk.endLocation = std::move(res2.value());

  auto numParams = quic::follyutils::decodeQuicInteger(cursor, length);
  if (!numParams) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= numParams->second;
  auto res3 =
      parseTrackRequestParams(cursor, length, numParams->first, fetchOk.params);
  if (!res3) {
    return folly::makeUnexpected(res3.error());
  }
  if (length > 0) {
    return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
  }

  return fetchOk;
}

folly::Expected<SubscribeAnnounces, ErrorCode>
MoQFrameParser::parseSubscribeAnnounces(
    folly::io::Cursor& cursor,
    size_t length) const noexcept {
  auto res = parseAnnounce(cursor, length);
  if (!res) {
    return folly::makeUnexpected(res.error());
  }
  return SubscribeAnnounces(
      {res->requestID, std::move(res->trackNamespace), std::move(res->params)});
}

folly::Expected<SubscribeAnnouncesOk, ErrorCode>
MoQFrameParser::parseSubscribeAnnouncesOk(
    folly::io::Cursor& cursor,
    size_t length) const noexcept {
  return parseRequestOk(cursor, length, FrameType::SUBSCRIBE_ANNOUNCES_OK);
}

// Unified request error parsing function
folly::Expected<RequestError, ErrorCode> MoQFrameParser::parseRequestError(
    folly::io::Cursor& cursor,
    size_t length,
    FrameType frameType) const noexcept {
  RequestError requestError;
  // XCHECK that frameType is one of the allowed types for this function
  XCHECK(
      frameType == FrameType::SUBSCRIBE_ERROR ||
      frameType == FrameType::REQUEST_ERROR ||
      frameType == FrameType::ANNOUNCE_ERROR ||
      frameType == FrameType::SUBSCRIBE_ANNOUNCES_ERROR ||
      frameType == FrameType::PUBLISH_ERROR ||
      frameType == FrameType::FETCH_ERROR ||
      frameType == FrameType::TRACK_STATUS_ERROR)
      << "Invalid frameType passed to parseRequestError: "
      << static_cast<int>(frameType);

  // All error types follow the same pattern: requestID → errorCode →
  // reasonPhrase

  // Parse requestID
  auto requestID = quic::follyutils::decodeQuicInteger(cursor, length);
  if (!requestID) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= requestID->second;
  requestError.requestID = requestID->first;

  // Parse errorCode
  auto errorCode = quic::follyutils::decodeQuicInteger(cursor, length);
  if (!errorCode) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= errorCode->second;
  requestError.errorCode = RequestErrorCode(errorCode->first);

  // Parse reasonPhrase
  auto reasonPhrase = parseFixedString(cursor, length);
  if (!reasonPhrase) {
    return folly::makeUnexpected(reasonPhrase.error());
  }
  requestError.reasonPhrase = std::move(reasonPhrase.value());

  // Handle frame-specific additional fields
  if (frameType == FrameType::SUBSCRIBE_ERROR &&
      getDraftMajorVersion(*version_) < 12) {
    // v11 has retryAlias field that we parse and discard, v12 doesn't have it
    auto retryAlias = quic::follyutils::decodeQuicInteger(cursor, length);
    if (!retryAlias) {
      return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
    }
    length -= retryAlias->second;
    // Discard retryAlias - not stored in RequestError
  }

  // Check for leftover bytes
  if (length > 0) {
    return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
  }

  return requestError;
}

folly::Expected<UnsubscribeAnnounces, ErrorCode>
MoQFrameParser::parseUnsubscribeAnnounces(
    folly::io::Cursor& cursor,
    size_t length) const noexcept {
  UnsubscribeAnnounces unsubscribeAnnounces;
  auto res = parseFixedTuple(cursor, length);
  if (!res) {
    return folly::makeUnexpected(res.error());
  }
  unsubscribeAnnounces.trackNamespacePrefix =
      TrackNamespace(std::move(res.value()));
  if (length > 0) {
    return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
  }
  return unsubscribeAnnounces;
}

folly::Expected<FullTrackName, ErrorCode> MoQFrameParser::parseFullTrackName(
    folly::io::Cursor& cursor,
    size_t& length) const noexcept {
  FullTrackName fullTrackName;
  auto res = parseFixedTuple(cursor, length);
  if (!res) {
    return folly::makeUnexpected(res.error());
  }
  fullTrackName.trackNamespace = TrackNamespace(std::move(res.value()));

  auto res2 = parseFixedString(cursor, length);
  if (!res2) {
    return folly::makeUnexpected(res2.error());
  }
  fullTrackName.trackName = std::move(res2.value());
  return fullTrackName;
}

folly::Expected<AbsoluteLocation, ErrorCode>
MoQFrameParser::parseAbsoluteLocation(folly::io::Cursor& cursor, size_t& length)
    const noexcept {
  AbsoluteLocation location;
  auto group = quic::follyutils::decodeQuicInteger(cursor, length);
  if (!group) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  location.group = group->first;
  length -= group->second;

  auto object = quic::follyutils::decodeQuicInteger(cursor, length);
  if (!object) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  location.object = object->first;
  length -= object->second;

  return location;
}

folly::Expected<folly::Unit, ErrorCode> MoQFrameParser::parseExtensions(
    folly::io::Cursor& cursor,
    size_t& length,
    ObjectHeader& objectHeader) const noexcept {
  CHECK(version_.hasValue())
      << "The version must be set before parsing extensions";

  // Parse the length of the extension block
  auto extLen = quic::follyutils::decodeQuicInteger(cursor, length);
  if (!extLen) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= extLen->second;
  if (extLen->first > length) {
    XLOG(ERR) << "Extension block length provided exceeds remaining length";
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  // Parse the extensions
  size_t extensionBlockLength = extLen->first;
  auto parseExtensionKvPairsResult =
      parseExtensionKvPairs(cursor, objectHeader, extensionBlockLength, true);
  if (!parseExtensionKvPairsResult.hasValue()) {
    return folly::makeUnexpected(parseExtensionKvPairsResult.error());
  }
  length -= extLen->first;
  return folly::unit;
}

folly::Expected<folly::Unit, ErrorCode> MoQFrameParser::parseExtensionKvPairs(
    folly::io::Cursor& cursor,
    ObjectHeader& objectHeader,
    size_t extensionBlockLength,
    bool allowImmutable) const noexcept {
  while (extensionBlockLength > 0) {
    // This won't infinite loop because we're parsing out at least a
    // QuicInteger each time.

    auto parseExtensionResult = parseExtension(
        cursor, extensionBlockLength, objectHeader, allowImmutable);
    if (parseExtensionResult.hasError()) {
      return folly::makeUnexpected(parseExtensionResult.error());
    }
  }
  return folly::unit;
}

folly::Expected<folly::Unit, ErrorCode> MoQFrameParser::parseExtension(
    folly::io::Cursor& cursor,
    size_t& length,
    ObjectHeader& objectHeader,
    bool allowImmutable) const noexcept {
  auto type = quic::follyutils::decodeQuicInteger(cursor, length);
  if (!type) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= type->second;

  // We can't have an immutable extension nested within another
  // immutable extension.
  if (!allowImmutable && getDraftMajorVersion(*version_) >= 14 &&
      type->first == kImmutableExtensionType) {
    XLOG(ERR) << "Immutable extension encountered when not allowed";
    return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
  }

  Extension ext;
  ext.type = type->first;

  // Check if this extension is an immutable extensions container (type 0xB) in
  // draft >= 14
  const bool isImmutableContainer =
      (getDraftMajorVersion(*version_) >= 14 &&
       ext.type == kImmutableExtensionType);
  // We are inside an immutable context if the current caller disallows
  // immutable (i.e., we're parsing inside an immutable container)
  const bool inImmutableContext =
      (getDraftMajorVersion(*version_) >= 14 && !allowImmutable);

  if (ext.type & 0x1) {
    auto extLen = quic::follyutils::decodeQuicInteger(cursor, length);
    if (!extLen) {
      return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
    }
    length -= extLen->second;
    if (length < extLen->first) {
      return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
    }
    if (extLen->first > kMaxExtensionLength) {
      XLOG(ERR) << "extLen > kMaxExtensionLength =" << extLen->first;
      return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
    }

    // For immutable container, flatten its contents and do not append the
    // container itself to the extensions list
    if (isImmutableContainer) {
      folly::io::Cursor innerCursor = cursor;
      auto parseInnerResult = parseExtensionKvPairs(
          innerCursor,
          objectHeader,
          extLen->first,
          /*allowImmutable=*/false);
      if (parseInnerResult.hasError()) {
        return folly::makeUnexpected(parseInnerResult.error());
      }
      // Advance the outer cursor past the immutable container payload and
      // consume the bytes from the local length tracker
      cursor.skip(extLen->first);
      length -= extLen->first;
      // Do not push the container itself
      return folly::unit;
    }

    // Regular odd-type extension (byte array). Clone the value buffer
    cursor.clone(ext.arrayValue, extLen->first);
    length -= extLen->first;
  } else {
    // Even-type extension (integer value)
    auto iVal = quic::follyutils::decodeQuicInteger(cursor, length);
    if (!iVal) {
      return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
    }
    length -= iVal->second;
    ext.intValue = iVal->first;
  }

  // Insert extension into appropriate collection based on context
  if (inImmutableContext) {
    objectHeader.extensions.insertImmutableExtension(std::move(ext));
  } else {
    objectHeader.extensions.insertMutableExtension(std::move(ext));
  }
  return folly::unit;
}

folly::Expected<std::vector<std::string>, ErrorCode>
MoQFrameParser::parseFixedTuple(folly::io::Cursor& cursor, size_t& length)
    const noexcept {
  auto itemCount = quic::follyutils::decodeQuicInteger(cursor, length);
  if (!itemCount) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  if (itemCount->first > kMaxNamespaceLength) {
    XLOG(ERR) << "tuple length > kMaxNamespaceLength =" << itemCount->first;
    return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
  }
  length -= itemCount->second;
  std::vector<std::string> items;
  items.reserve(itemCount->first);
  for (auto i = 0u; i < itemCount->first; i++) {
    auto res = parseFixedString(cursor, length);
    if (!res) {
      return folly::makeUnexpected(res.error());
    }
    items.emplace_back(std::move(res.value()));
  }
  return items;
}

//// Egress ////

void writeVarint(
    folly::IOBufQueue& buf,
    uint64_t value,
    size_t& size,
    bool& error) noexcept {
  if (error) {
    return;
  }
  folly::io::QueueAppender appender(&buf, kMaxFrameHeaderSize);
  auto appenderOp = [appender = std::move(appender)](auto val) mutable {
    appender.writeBE(val);
  };
  auto res = quic::encodeQuicInteger(value, appenderOp);
  if (res.hasError()) {
    error = true;
  } else {
    size += *res;
  }
}

void writeFixedString(
    folly::IOBufQueue& writeBuf,
    const std::string& str,
    size_t& size,
    bool& error) {
  writeVarint(writeBuf, str.size(), size, error);
  // Avoid appending a zero-length string, which can lead to undefined behavior
  // on some platforms when passing a null data pointer with length 0.
  if (!error && !str.empty()) {
    writeBuf.append(str);
    size += str.size();
  }
}

void writeFixedTuple(
    folly::IOBufQueue& writeBuf,
    const std::vector<std::string>& tup,
    size_t& size,
    bool& error) {
  writeVarint(writeBuf, tup.size(), size, error);
  if (!error) {
    for (auto& str : tup) {
      writeFixedString(writeBuf, str, size, error);
    }
  }
}

void writeTrackNamespace(
    folly::IOBufQueue& writeBuf,
    const TrackNamespace& tn,
    size_t& size,
    bool& error) {
  writeFixedTuple(writeBuf, tn.trackNamespace, size, error);
}

uint16_t* writeFrameHeader(
    folly::IOBufQueue& writeBuf,
    FrameType frameType,
    bool& error) {
  size_t size = 0;
  writeVarint(writeBuf, folly::to_underlying(frameType), size, error);
  auto res = writeBuf.preallocate(2, 256);
  writeBuf.postallocate(2);
  CHECK_GE(res.second, 2);
  return static_cast<uint16_t*>(res.first);
}

void writeSize(
    uint16_t* sizePtr,
    size_t size,
    bool& error,
    uint64_t versionIn) {
  if (size > ((1 << 16) - 1)) {
    // Size check for versions >= 11
    LOG(ERROR) << "Control message size exceeds max sz=" << size;
    error = true;
    return;
  }
  uint16_t sizeVal = folly::Endian::big(uint16_t(size));
  memcpy(sizePtr, &sizeVal, 2);
}

void writeFullTrackName(
    folly::IOBufQueue& writeBuf,
    const FullTrackName& fullTrackName,
    size_t& size,
    bool error) {
  writeTrackNamespace(writeBuf, fullTrackName.trackNamespace, size, error);
  writeFixedString(writeBuf, fullTrackName.trackName, size, error);
}

std::string MoQFrameWriter::encodeUseAlias(uint64_t alias) const {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  size_t size = 0;
  bool error = false;
  writeVarint(
      writeBuf, folly::to_underlying(AliasType::USE_ALIAS), size, error);
  writeVarint(writeBuf, alias, size, error);
  XCHECK(!error) << "Alias too large";
  return writeBuf.move()->moveToFbString().toStdString();
}

std::string MoQFrameWriter::encodeDeleteTokenAlias(uint64_t alias) const {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  size_t size = 0;
  bool error = false;
  writeVarint(writeBuf, folly::to_underlying(AliasType::DELETE), size, error);
  writeVarint(writeBuf, alias, size, error);
  XCHECK(!error) << "Alias too large";
  return writeBuf.move()->moveToFbString().toStdString();
}

std::string MoQFrameWriter::encodeRegisterToken(
    uint64_t alias,
    uint64_t tokenType,
    const std::string& tokenValue) const {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  size_t size = 0;
  bool error = false;
  writeVarint(writeBuf, folly::to_underlying(AliasType::REGISTER), size, error);
  writeVarint(writeBuf, alias, size, error);
  writeVarint(writeBuf, tokenType, size, error);
  writeBuf.append(tokenValue);
  size += tokenValue.size();
  XCHECK(!error) << "Error encoding register token";
  return writeBuf.move()->moveToFbString().toStdString();
}

std::string MoQFrameWriter::encodeTokenValue(
    uint64_t tokenType,
    const std::string& tokenValue,
    const folly::Optional<uint64_t>& forceVersion) const {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  size_t size = 0;
  bool error = false;
  auto version = forceVersion ? forceVersion : version_;
  if (getDraftMajorVersion(*version) >= 11) {
    writeVarint(
        writeBuf, folly::to_underlying(AliasType::USE_VALUE), size, error);
    writeVarint(writeBuf, tokenType, size, error);
  }
  writeBuf.append(tokenValue);
  size += tokenValue.size();
  XCHECK(!error) << "Error encoding token value";
  return writeBuf.move()->moveToFbString().toStdString();
}

bool includeSetupParam(uint64_t version, SetupKey key) {
  return key == SetupKey::MAX_REQUEST_ID || key == SetupKey::PATH ||
      key == SetupKey::MAX_AUTH_TOKEN_CACHE_SIZE ||
      key == SetupKey::AUTHORIZATION_TOKEN;
}

WriteResult writeClientSetup(
    folly::IOBufQueue& writeBuf,
    const ClientSetup& clientSetup,
    uint64_t version) noexcept {
  size_t size = 0;
  bool error = false;

  FrameType frameType = FrameType::CLIENT_SETUP;
  auto sizePtr = writeFrameHeader(writeBuf, frameType, error);

  if (getDraftMajorVersion(version) < 15) {
    // Check that all supported versions are >= 12
    for (auto ver : clientSetup.supportedVersions) {
      XCHECK(isSupportedVersion(ver))
          << "Version " << ver << " is not supported. Supported versions are: "
          << getSupportedVersionsString();
    }
    // Only write version array in non-alpn mode
    writeVarint(writeBuf, clientSetup.supportedVersions.size(), size, error);
    for (auto ver : clientSetup.supportedVersions) {
      writeVarint(writeBuf, ver, size, error);
    }
  } else {
    XLOG(DBG3)
        << "Skipped writing versions to wire for alpn ClientSetup message";
  }

  // Count the number of params
  size_t paramCount = 0;
  for (const auto& param : clientSetup.params) {
    if (includeSetupParam(version, SetupKey(param.key))) {
      ++paramCount;
    }
  }
  writeVarint(writeBuf, paramCount, size, error);
  for (auto& param : clientSetup.params) {
    if (!includeSetupParam(version, SetupKey(param.key))) {
      continue;
    }
    writeVarint(writeBuf, param.key, size, error);
    if ((param.key & 0x01) == 0) {
      writeVarint(writeBuf, param.asUint64, size, error);
    } else {
      writeFixedString(writeBuf, param.asString, size, error);
    }
  }
  writeSize(sizePtr, size, error, version);
  if (error) {
    return folly::makeUnexpected(quic::TransportErrorCode::INTERNAL_ERROR);
  }
  return size;
}

WriteResult writeServerSetup(
    folly::IOBufQueue& writeBuf,
    const ServerSetup& serverSetup,
    uint64_t version) noexcept {
  size_t size = 0;
  bool error = false;

  FrameType frameType = FrameType::SERVER_SETUP;
  auto sizePtr = writeFrameHeader(writeBuf, frameType, error);

  // Only write selected version in non-alpn mode
  if (getDraftMajorVersion(version) < 15) {
    XCHECK(isSupportedVersion(serverSetup.selectedVersion))
        << "Supported version " << serverSetup.selectedVersion
        << ") is not supported. Supported versions are: "
        << getSupportedVersionsString();
    writeVarint(writeBuf, serverSetup.selectedVersion, size, error);
  } else {
    XLOG(DBG3)
        << "Skipped writing version to wire for alpn ClientSetup message";
  }

  // Count the number of params
  size_t paramCount = 0;
  for (const auto& param : serverSetup.params) {
    if (includeSetupParam(serverSetup.selectedVersion, SetupKey(param.key))) {
      ++paramCount;
    }
  }
  writeVarint(writeBuf, paramCount, size, error);
  for (auto& param : serverSetup.params) {
    if (!includeSetupParam(serverSetup.selectedVersion, SetupKey(param.key))) {
      continue;
    }
    writeVarint(writeBuf, param.key, size, error);
    if ((param.key & 0x01) == 0) {
      writeVarint(writeBuf, param.asUint64, size, error);
    } else {
      writeFixedString(writeBuf, param.asString, size, error);
    }
  }
  writeSize(sizePtr, size, error, version);
  if (error) {
    return folly::makeUnexpected(quic::TransportErrorCode::INTERNAL_ERROR);
  }
  return size;
}

WriteResult MoQFrameWriter::writeSubgroupHeader(
    folly::IOBufQueue& writeBuf,
    TrackAlias trackAlias,
    const ObjectHeader& objectHeader,
    SubgroupIDFormat format,
    bool includeExtensions) const noexcept {
  size_t size = 0;
  bool error = false;
  auto streamType = getSubgroupStreamType(
      *version_,
      objectHeader.subgroup == 0 ? SubgroupIDFormat::Zero : format,
      includeExtensions,
      /*endOfGroup=*/false);
  auto streamTypeInt = folly::to_underlying(streamType);
  writeVarint(writeBuf, streamTypeInt, size, error);
  writeVarint(writeBuf, trackAlias.value, size, error);
  writeVarint(writeBuf, objectHeader.group, size, error);
  if (streamTypeInt & SG_HAS_SUBGROUP_ID) {
    writeVarint(writeBuf, objectHeader.subgroup, size, error);
  }
  writeBuf.append(&objectHeader.priority, 1);
  size += 1;
  if (error) {
    return folly::makeUnexpected(quic::TransportErrorCode::INTERNAL_ERROR);
  }
  return size;
}

WriteResult MoQFrameWriter::writeFetchHeader(
    folly::IOBufQueue& writeBuf,
    RequestID requestID) const noexcept {
  size_t size = 0;
  bool error = false;
  writeVarint(
      writeBuf, folly::to_underlying(StreamType::FETCH_HEADER), size, error);
  writeVarint(writeBuf, requestID.value, size, error);
  if (error) {
    return folly::makeUnexpected(quic::TransportErrorCode::INTERNAL_ERROR);
  }
  return size;
}

WriteResult MoQFrameWriter::writeSingleObjectStream(
    folly::IOBufQueue& writeBuf,
    TrackAlias trackAlias,
    const ObjectHeader& objectHeader,
    std::unique_ptr<folly::IOBuf> objectPayload) const noexcept {
  bool hasExtensions = objectHeader.extensions.size() > 0;
  auto res = writeSubgroupHeader(
      writeBuf,
      trackAlias,
      objectHeader,
      objectHeader.subgroup == objectHeader.id ? SubgroupIDFormat::FirstObject
                                               : SubgroupIDFormat::Present,
      hasExtensions);
  if (res) {
    return writeStreamObject(
        writeBuf,
        hasExtensions ? StreamType::SUBGROUP_HEADER_SG_EXT
                      : StreamType::SUBGROUP_HEADER_SG,
        objectHeader,
        std::move(objectPayload));
  } else {
    return res;
  }
}

void MoQFrameWriter::writeKeyValuePairs(
    folly::IOBufQueue& writeBuf,
    const std::vector<Extension>& extensions,
    size_t& size,
    bool& error) const noexcept {
  for (const auto& ext : extensions) {
    writeVarint(writeBuf, ext.type, size, error);
    if (error) {
      return;
    }
    if (ext.isOddType()) {
      // odd = length prefix
      if (ext.arrayValue) {
        writeVarint(
            writeBuf, ext.arrayValue->computeChainDataLength(), size, error);
        if (error) {
          return;
        }
        writeBuf.append(ext.arrayValue->clone());
        size += ext.arrayValue->computeChainDataLength();
      } else {
        writeVarint(writeBuf, 0, size, error);
      }
    } else {
      // even = single varint
      writeVarint(writeBuf, ext.intValue, size, error);
    }
    if (error) {
      return;
    }
  }
}

void MoQFrameWriter::writeExtensions(
    folly::IOBufQueue& writeBuf,
    const Extensions& extensions,
    size_t& size,
    bool& error) const noexcept {
  // Calculate total extension length (mutable + immutable blob if present)
  auto mutableExtLen =
      calculateExtensionVectorSize(extensions.getMutableExtensions(), error);
  if (error) {
    return;
  }

  size_t immutableBlobLen = 0;
  size_t immutableExtensionsSize = 0; // Store calculated size for reuse
  if (getDraftMajorVersion(*version_) >= 14 &&
      !extensions.getImmutableExtensions().empty()) {
    // Calculate size of immutable extensions blob:
    // - Type (kImmutableExtensionType)
    // - Length
    // - Key-value pairs data
    immutableExtensionsSize = calculateExtensionVectorSize(
        extensions.getImmutableExtensions(), error);
    if (error) {
      return;
    }

    auto maybeTypeSize = quic::getQuicIntegerSize(kImmutableExtensionType);
    if (maybeTypeSize.hasError()) {
      error = true;
      return;
    }

    auto maybeLengthSize = quic::getQuicIntegerSize(immutableExtensionsSize);
    if (maybeLengthSize.hasError()) {
      error = true;
      return;
    }

    immutableBlobLen =
        *maybeTypeSize + *maybeLengthSize + immutableExtensionsSize;
  }

  auto totalExtLen = mutableExtLen + immutableBlobLen;
  writeVarint(writeBuf, totalExtLen, size, error);
  if (error) {
    return;
  }

  // Write mutable extensions first
  writeKeyValuePairs(writeBuf, extensions.getMutableExtensions(), size, error);
  if (error) {
    return;
  }

  // Write immutable extensions blob if present
  if (getDraftMajorVersion(*version_) >= 14 &&
      !extensions.getImmutableExtensions().empty()) {
    writeVarint(writeBuf, kImmutableExtensionType, size, error);
    if (error) {
      return;
    }

    // Use the previously calculated size (no need to recalculate)
    writeVarint(writeBuf, immutableExtensionsSize, size, error);
    if (error) {
      return;
    }

    // Write the immutable extensions as key-value pairs
    writeKeyValuePairs(
        writeBuf, extensions.getImmutableExtensions(), size, error);
  }
}

size_t MoQFrameWriter::calculateExtensionVectorSize(
    const std::vector<Extension>& extensions,
    bool& error) const noexcept {
  size_t size = 0;
  if (error) {
    return 0;
  }
  for (const auto& ext : extensions) {
    auto maybeTypeSize = quic::getQuicIntegerSize(ext.type);
    if (maybeTypeSize.hasError()) {
      error = true;
      return 0;
    }
    size += *maybeTypeSize;
    if (ext.type & 0x1) {
      // odd = length prefix
      auto dataLen =
          ext.arrayValue ? ext.arrayValue->computeChainDataLength() : 0;
      auto maybeDataLengthSize = quic::getQuicIntegerSize(dataLen);
      if (maybeDataLengthSize.hasError()) {
        error = true;
        return 0;
      }
      size += *maybeDataLengthSize;
      size += dataLen;
    } else {
      // even = single varint
      auto maybeValueSize = quic::getQuicIntegerSize(ext.intValue);
      if (maybeValueSize.hasError()) {
        error = true;
        return 0;
      }
      size += *maybeValueSize;
    }
  }
  return size;
}

TrackRequestParameter getAuthParam(
    uint64_t version,
    std::string token,
    uint64_t tokenType,
    folly::Optional<uint64_t> registerToken) {
  return TrackRequestParameter(
      {folly::to_underlying(TrackRequestParamKey::AUTHORIZATION_TOKEN),
       "",
       0,
       {tokenType, std::move(token), registerToken}});
}

void MoQFrameWriter::writeTrackRequestParams(
    folly::IOBufQueue& writeBuf,
    const std::vector<TrackRequestParameter>& params,
    size_t& size,
    bool& error) const noexcept {
  CHECK(*version_) << "Version must be set before writing track request params";
  writeVarint(writeBuf, params.size(), size, error);
  for (auto& param : params) {
    writeVarint(writeBuf, param.key, size, error);

    switch (static_cast<TrackRequestParamKey>(param.key)) {
      case TrackRequestParamKey::AUTHORIZATION_TOKEN:
        writeFixedString(writeBuf, param.asString, size, error);
        break;
      case TrackRequestParamKey::DELIVERY_TIMEOUT:
      case TrackRequestParamKey::MAX_CACHE_DURATION:
        writeVarint(writeBuf, param.asUint64, size, error);
        break;
      default:
        if ((param.key & 0x01) == 0) {
          writeVarint(writeBuf, param.asUint64, size, error);
        } else {
          writeFixedString(writeBuf, param.asString, size, error);
        }
        break;
    }
  }
}

WriteResult MoQFrameWriter::writeDatagramObject(
    folly::IOBufQueue& writeBuf,
    TrackAlias trackAlias,
    const ObjectHeader& objectHeader,
    std::unique_ptr<folly::IOBuf> objectPayload) const noexcept {
  size_t size = 0;
  bool error = false;
  bool hasLength = objectHeader.length && *objectHeader.length > 0;
  bool hasExtensions = objectHeader.extensions.size() > 0;

  // Set this only if version >= 14. Else ObjId is always written on the wire
  bool isObjectIdZero =
      (objectHeader.id == 0 && (getDraftMajorVersion(version_.value()) >= 14));

  CHECK(!hasLength || objectHeader.status == ObjectStatus::NORMAL)
      << "non-zero length objects require NORMAL status";
  if (objectHeader.status != ObjectStatus::NORMAL || !hasLength) {
    CHECK(!objectPayload || objectPayload->computeChainDataLength() == 0)
        << "non-empty objectPayload with no header length";
    writeVarint(
        writeBuf,
        folly::to_underlying(getDatagramType(
            *version_, true, hasExtensions, false, isObjectIdZero)),
        size,
        error);
    writeVarint(writeBuf, trackAlias.value, size, error);
    writeVarint(writeBuf, objectHeader.group, size, error);

    // Only put non-zero object ID on the wire
    if (!isObjectIdZero) {
      writeVarint(writeBuf, objectHeader.id, size, error);
    }

    writeBuf.append(&objectHeader.priority, 1);
    size += 1;
    if (hasExtensions) {
      writeExtensions(writeBuf, objectHeader.extensions, size, error);
    }
    writeVarint(
        writeBuf, folly::to_underlying(objectHeader.status), size, error);
  } else {
    writeVarint(
        writeBuf,
        folly::to_underlying(getDatagramType(
            *version_, false, hasExtensions, false, isObjectIdZero)),
        size,
        error);
    writeVarint(writeBuf, trackAlias.value, size, error);
    writeVarint(writeBuf, objectHeader.group, size, error);
    if (!isObjectIdZero) {
      writeVarint(writeBuf, objectHeader.id, size, error);
    }
    writeBuf.append(&objectHeader.priority, 1);
    size += 1;
    if (hasExtensions) {
      writeExtensions(writeBuf, objectHeader.extensions, size, error);
    }
    writeBuf.append(std::move(objectPayload));
  }
  if (error) {
    return folly::makeUnexpected(quic::TransportErrorCode::INTERNAL_ERROR);
  }
  return size;
}

WriteResult MoQFrameWriter::writeStreamObject(
    folly::IOBufQueue& writeBuf,
    StreamType streamType,
    const ObjectHeader& objectHeader,
    std::unique_ptr<folly::IOBuf> objectPayload) const noexcept {
  XCHECK(version_.hasValue())
      << "The version must be set before writing stream object";
  size_t size = 0;
  bool error = false;
  if (streamType == StreamType::FETCH_HEADER) {
    writeVarint(writeBuf, objectHeader.group, size, error);
    writeVarint(writeBuf, objectHeader.subgroup, size, error);
    writeVarint(writeBuf, objectHeader.id, size, error);
    writeBuf.append(&objectHeader.priority, 1);
    size += 1;
  } else {
    if (getDraftMajorVersion(*version_) >= 14) {
      // Delta encoding of object ID
      uint64_t objectIDDelta;
      if (previousObjectID_.has_value()) {
        if (objectHeader.id > previousObjectID_.value()) {
          objectIDDelta = objectHeader.id - previousObjectID_.value() - 1;
        } else {
          // received same or lower ObjectID, error
          return folly::makeUnexpected(
              quic::TransportErrorCode::PROTOCOL_VIOLATION);
        }
      } else {
        objectIDDelta = objectHeader.id;
      }
      previousObjectID_ = objectHeader.id;
      writeVarint(writeBuf, objectIDDelta, size, error);
    } else {
      writeVarint(writeBuf, objectHeader.id, size, error);
    }
  }
  if (folly::to_underlying(streamType) & 0x1) {
    // includes FETCH, watch out if we add more types!
    writeExtensions(writeBuf, objectHeader.extensions, size, error);
  }
  bool hasLength = objectHeader.length && *objectHeader.length > 0;
  CHECK(!hasLength || objectHeader.status == ObjectStatus::NORMAL)
      << "non-zero length objects require NORMAL status";
  if (hasLength) {
    writeVarint(writeBuf, *objectHeader.length, size, error);
    writeBuf.append(std::move(objectPayload));
    // TODO: adjust size?
  } else {
    CHECK(!objectPayload || objectPayload->computeChainDataLength() == 0)
        << "non-empty objectPayload with no header length";
    writeVarint(writeBuf, 0, size, error);
    writeVarint(
        writeBuf, folly::to_underlying(objectHeader.status), size, error);
  }
  if (error) {
    return folly::makeUnexpected(quic::TransportErrorCode::INTERNAL_ERROR);
  }
  return size;
}

WriteResult MoQFrameWriter::writeSubscribeRequest(
    folly::IOBufQueue& writeBuf,
    const SubscribeRequest& subscribeRequest) const noexcept {
  CHECK(version_.hasValue())
      << "Version needs to be set to write subscribe request";
  size_t size = 0;
  bool error = false;
  auto sizePtr = writeFrameHeader(writeBuf, FrameType::SUBSCRIBE, error);
  auto res = writeSubscribeRequestHelper(writeBuf, subscribeRequest);
  if (!res) {
    return res;
  }
  size += *res;
  writeSize(sizePtr, size, error, *version_);
  if (error) {
    return folly::makeUnexpected(quic::TransportErrorCode::INTERNAL_ERROR);
  }
  return size;
}

WriteResult MoQFrameWriter::writeSubscribeRequestHelper(
    folly::IOBufQueue& writeBuf,
    const SubscribeRequest& subscribeRequest) const noexcept {
  size_t size = 0;
  bool error = false;
  writeVarint(writeBuf, subscribeRequest.requestID.value, size, error);
  if (getDraftMajorVersion(version_.value()) < 12) {
    XCHECK(subscribeRequest.trackAlias.has_value()) << "Track alias required";
    writeVarint(writeBuf, subscribeRequest.trackAlias->value, size, error);
  }
  writeFullTrackName(writeBuf, subscribeRequest.fullTrackName, size, error);
  writeBuf.append(&subscribeRequest.priority, 1);
  size += 1;
  uint8_t order = folly::to_underlying(subscribeRequest.groupOrder);
  writeBuf.append(&order, 1);
  size += 1;
  if (getDraftMajorVersion(*version_) >= 11) {
    uint8_t forwardFlag = (subscribeRequest.forward) ? 1 : 0;
    writeBuf.append(&forwardFlag, 1);
    size += 1;
  }
  writeVarint(
      writeBuf,
      getLocationTypeValue(
          subscribeRequest.locType, getDraftMajorVersion(*version_)),
      size,
      error);

  switch (subscribeRequest.locType) {
    case LocationType::AbsoluteStart: {
      writeVarint(writeBuf, subscribeRequest.start->group, size, error);
      writeVarint(writeBuf, subscribeRequest.start->object, size, error);
      break;
    }

    case LocationType::AbsoluteRange: {
      writeVarint(writeBuf, subscribeRequest.start->group, size, error);
      writeVarint(writeBuf, subscribeRequest.start->object, size, error);
      writeVarint(writeBuf, subscribeRequest.endGroup, size, error);
      break;
    }

    default: {
      break;
    }
  }

  writeTrackRequestParams(writeBuf, subscribeRequest.params, size, error);
  return size;
}

WriteResult MoQFrameWriter::writeSubscribeUpdate(
    folly::IOBufQueue& writeBuf,
    const SubscribeUpdate& update) const noexcept {
  CHECK(version_.hasValue())
      << "Version needs to be set to write subscribe update";
  size_t size = 0;
  bool error = false;
  auto sizePtr = writeFrameHeader(writeBuf, FrameType::SUBSCRIBE_UPDATE, error);
  writeVarint(writeBuf, update.requestID.value, size, error);
  if (getDraftMajorVersion(*version_) >= 14) {
    writeVarint(writeBuf, update.subscriptionRequestID.value, size, error);
  }
  writeVarint(writeBuf, update.start.group, size, error);
  writeVarint(writeBuf, update.start.object, size, error);
  writeVarint(writeBuf, update.endGroup, size, error);
  writeBuf.append(&update.priority, 1);
  size += 1;
  if (getDraftMajorVersion(*version_) >= 11) {
    uint8_t forwardFlag = (update.forward) ? 1 : 0;
    writeBuf.append(&forwardFlag, 1);
    size += 1;
  }
  writeTrackRequestParams(writeBuf, update.params, size, error);
  writeSize(sizePtr, size, error, *version_);
  if (error) {
    return folly::makeUnexpected(quic::TransportErrorCode::INTERNAL_ERROR);
  }
  return size;
}

WriteResult MoQFrameWriter::writeSubscribeOk(
    folly::IOBufQueue& writeBuf,
    const SubscribeOk& subscribeOk) const noexcept {
  CHECK(version_.hasValue()) << "Version needs to be set to write subscribe ok";
  size_t size;
  bool error = false;
  auto sizePtr = writeFrameHeader(writeBuf, FrameType::SUBSCRIBE_OK, error);
  auto res = writeSubscribeOkHelper(writeBuf, subscribeOk);
  if (!res) {
    return res;
  }
  size = *res;
  writeSize(sizePtr, size, error, *version_);
  if (error) {
    return folly::makeUnexpected(quic::TransportErrorCode::INTERNAL_ERROR);
  }
  return size;
}

WriteResult MoQFrameWriter::writeSubscribeOkHelper(
    folly::IOBufQueue& writeBuf,
    const SubscribeOk& subscribeOk) const noexcept {
  size_t size = 0;
  bool error = false;
  writeVarint(writeBuf, subscribeOk.requestID.value, size, error);
  if (getDraftMajorVersion(*version_) >= 12) {
    writeVarint(writeBuf, subscribeOk.trackAlias.value, size, error);
  }
  writeVarint(writeBuf, subscribeOk.expires.count(), size, error);
  auto order = folly::to_underlying(subscribeOk.groupOrder);
  writeBuf.append(&order, 1);
  size += 1;
  uint8_t contentExists = (subscribeOk.largest) ? 1 : 0;
  writeBuf.append(&contentExists, 1);
  size += 1;
  if (subscribeOk.largest) {
    writeVarint(writeBuf, subscribeOk.largest->group, size, error);
    writeVarint(writeBuf, subscribeOk.largest->object, size, error);
  }
  writeTrackRequestParams(writeBuf, subscribeOk.params, size, error);
  return size;
}

WriteResult MoQFrameWriter::writeMaxRequestID(
    folly::IOBufQueue& writeBuf,
    const MaxRequestID& maxRequestID) const noexcept {
  CHECK(version_.hasValue())
      << "Version needs to be set to write max requestID";
  size_t size = 0;
  bool error = false;
  auto sizePtr = writeFrameHeader(writeBuf, FrameType::MAX_REQUEST_ID, error);
  writeVarint(writeBuf, maxRequestID.requestID.value, size, error);
  writeSize(sizePtr, size, error, *version_);
  if (error) {
    return folly::makeUnexpected(quic::TransportErrorCode::INTERNAL_ERROR);
  }
  return size;
}

WriteResult MoQFrameWriter::writeRequestsBlocked(
    folly::IOBufQueue& writeBuf,
    const RequestsBlocked& subscribesBlocked) const noexcept {
  CHECK(version_.hasValue())
      << "Version needs to be set to write subscribes blocked";
  size_t size = 0;
  bool error = false;
  auto sizePtr = writeFrameHeader(writeBuf, FrameType::REQUESTS_BLOCKED, error);
  writeVarint(writeBuf, subscribesBlocked.maxRequestID.value, size, error);
  writeSize(sizePtr, size, error, *version_);
  if (error) {
    return folly::makeUnexpected(quic::TransportErrorCode::INTERNAL_ERROR);
  }
  return size;
}

WriteResult MoQFrameWriter::writeUnsubscribe(
    folly::IOBufQueue& writeBuf,
    const Unsubscribe& unsubscribe) const noexcept {
  CHECK(version_.hasValue()) << "Version needs to be set to write unsubscribe";
  size_t size = 0;
  bool error = false;
  auto sizePtr = writeFrameHeader(writeBuf, FrameType::UNSUBSCRIBE, error);
  writeVarint(writeBuf, unsubscribe.requestID.value, size, error);
  writeSize(sizePtr, size, error, *version_);
  if (error) {
    return folly::makeUnexpected(quic::TransportErrorCode::INTERNAL_ERROR);
  }
  return size;
}

WriteResult MoQFrameWriter::writeSubscribeDone(
    folly::IOBufQueue& writeBuf,
    const SubscribeDone& subscribeDone) const noexcept {
  CHECK(version_.hasValue())
      << "Version needs to be set to write subscribe done";
  size_t size = 0;
  bool error = false;
  auto sizePtr = writeFrameHeader(writeBuf, FrameType::SUBSCRIBE_DONE, error);
  writeVarint(writeBuf, subscribeDone.requestID.value, size, error);
  writeVarint(
      writeBuf, folly::to_underlying(subscribeDone.statusCode), size, error);
  writeVarint(writeBuf, subscribeDone.streamCount, size, error);
  writeFixedString(writeBuf, subscribeDone.reasonPhrase, size, error);
  if (getDraftMajorVersion(*version_) <= 9) {
    writeVarint(writeBuf, 0, size, error);
  }
  writeSize(sizePtr, size, error, *version_);
  if (error) {
    return folly::makeUnexpected(quic::TransportErrorCode::INTERNAL_ERROR);
  }
  return size;
}

WriteResult MoQFrameWriter::writePublish(
    folly::IOBufQueue& writeBuf,
    const PublishRequest& publish) const noexcept {
  CHECK(version_.hasValue()) << "Version needs to be set to write publish";
  size_t size = 0;
  bool error = false;
  auto sizePtr = writeFrameHeader(writeBuf, FrameType::PUBLISH, error);
  writeVarint(writeBuf, publish.requestID.value, size, error);

  writeFullTrackName(writeBuf, publish.fullTrackName, size, error);

  writeVarint(writeBuf, publish.trackAlias.value, size, error);

  uint8_t order = folly::to_underlying(publish.groupOrder);
  writeBuf.append(&order, 1);
  size += 1;

  uint8_t contentExists = publish.largest.hasValue() ? 1 : 0;
  writeBuf.append(&contentExists, 1);
  size += 1;

  if (publish.largest.hasValue()) {
    writeVarint(writeBuf, publish.largest->group, size, error);
    writeVarint(writeBuf, publish.largest->object, size, error);
  }

  uint8_t forwardFlag = publish.forward ? 1 : 0;
  writeBuf.append(&forwardFlag, 1);
  size += 1;

  writeTrackRequestParams(writeBuf, publish.params, size, error);
  writeSize(sizePtr, size, error, *version_);
  if (error) {
    return folly::makeUnexpected(quic::TransportErrorCode::INTERNAL_ERROR);
  }
  return size;
}

WriteResult MoQFrameWriter::writePublishOk(
    folly::IOBufQueue& writeBuf,
    const PublishOk& publishOk) const noexcept {
  CHECK(version_.hasValue()) << "Version needs to be set to write publish ok";
  size_t size = 0;
  bool error = false;
  auto sizePtr = writeFrameHeader(writeBuf, FrameType::PUBLISH_OK, error);
  writeVarint(writeBuf, publishOk.requestID.value, size, error);

  uint8_t forwardFlag = publishOk.forward ? 1 : 0;
  writeBuf.append(&forwardFlag, 1);
  size += 1;

  writeBuf.append(&publishOk.subscriberPriority, 1);
  size += 1;

  uint8_t order = folly::to_underlying(publishOk.groupOrder);
  writeBuf.append(&order, 1);
  size += 1;

  writeVarint(
      writeBuf,
      getLocationTypeValue(publishOk.locType, getDraftMajorVersion(*version_)),
      size,
      error);

  switch (publishOk.locType) {
    case LocationType::AbsoluteStart: {
      if (publishOk.start.hasValue()) {
        writeVarint(writeBuf, publishOk.start->group, size, error);
        writeVarint(writeBuf, publishOk.start->object, size, error);
      }
      break;
    }
    case LocationType::AbsoluteRange: {
      if (publishOk.start.hasValue()) {
        writeVarint(writeBuf, publishOk.start->group, size, error);
        writeVarint(writeBuf, publishOk.start->object, size, error);
      }
      if (publishOk.endGroup.hasValue()) {
        writeVarint(writeBuf, publishOk.endGroup.value(), size, error);
      }
      break;
    }
    default: {
      break;
    }
  }

  writeTrackRequestParams(writeBuf, publishOk.params, size, error);
  writeSize(sizePtr, size, error, *version_);
  if (error) {
    return folly::makeUnexpected(quic::TransportErrorCode::INTERNAL_ERROR);
  }
  return size;
}

WriteResult MoQFrameWriter::writeAnnounce(
    folly::IOBufQueue& writeBuf,
    const Announce& announce) const noexcept {
  CHECK(version_.hasValue()) << "Version needs to be set to write announce";
  size_t size = 0;
  bool error = false;
  auto sizePtr = writeFrameHeader(writeBuf, FrameType::ANNOUNCE, error);
  if (getDraftMajorVersion(*version_) >= 11) {
    writeVarint(writeBuf, announce.requestID.value, size, error);
  }
  writeTrackNamespace(writeBuf, announce.trackNamespace, size, error);
  writeTrackRequestParams(writeBuf, announce.params, size, error);
  writeSize(sizePtr, size, error, *version_);
  if (error) {
    return folly::makeUnexpected(quic::TransportErrorCode::INTERNAL_ERROR);
  }
  return size;
}

WriteResult MoQFrameWriter::writeAnnounceOk(
    folly::IOBufQueue& writeBuf,
    const AnnounceOk& announceOk) const noexcept {
  return writeRequestOk(writeBuf, announceOk, FrameType::ANNOUNCE_OK);
}

WriteResult MoQFrameWriter::writeRequestOk(
    folly::IOBufQueue& writeBuf,
    const RequestOk& requestOk,
    FrameType frameType) const noexcept {
  CHECK(version_.hasValue()) << "Version needs to be set to write request ok";
  size_t size = 0;
  bool error = false;
  if (getDraftMajorVersion(*version_) > 14) {
    frameType = FrameType::REQUEST_OK;
  }
  auto sizePtr = writeFrameHeader(writeBuf, frameType, error);
  writeVarint(writeBuf, requestOk.requestID.value, size, error);
  if (getDraftMajorVersion(*version_) > 14) {
    if ((frameType == FrameType::ANNOUNCE_OK ||
         frameType == FrameType::SUBSCRIBE_ANNOUNCES_OK) &&
        !requestOk.params.empty()) {
      return folly::makeUnexpected(
          quic::TransportErrorCode::PROTOCOL_VIOLATION);
    }
    writeTrackRequestParams(writeBuf, requestOk.params, size, error);
  }
  writeSize(sizePtr, size, error, *version_);
  if (error) {
    return folly::makeUnexpected(quic::TransportErrorCode::INTERNAL_ERROR);
  }
  return size;
}

WriteResult MoQFrameWriter::writeUnannounce(
    folly::IOBufQueue& writeBuf,
    const Unannounce& unannounce) const noexcept {
  CHECK(version_.hasValue()) << "Version needs to be set to write unannounce";
  size_t size = 0;
  bool error = false;
  auto sizePtr = writeFrameHeader(writeBuf, FrameType::UNANNOUNCE, error);
  writeTrackNamespace(writeBuf, unannounce.trackNamespace, size, error);
  writeSize(sizePtr, size, error, *version_);
  if (error) {
    return folly::makeUnexpected(quic::TransportErrorCode::INTERNAL_ERROR);
  }
  return size;
}

WriteResult MoQFrameWriter::writeAnnounceCancel(
    folly::IOBufQueue& writeBuf,
    const AnnounceCancel& announceCancel) const noexcept {
  CHECK(version_.hasValue())
      << "Version needs to be set to write announce cancel";
  size_t size = 0;
  bool error = false;
  auto sizePtr = writeFrameHeader(writeBuf, FrameType::ANNOUNCE_CANCEL, error);
  writeTrackNamespace(writeBuf, announceCancel.trackNamespace, size, error);
  writeVarint(
      writeBuf, folly::to_underlying(announceCancel.errorCode), size, error);
  writeFixedString(writeBuf, announceCancel.reasonPhrase, size, error);
  writeSize(sizePtr, size, error, *version_);
  if (error) {
    return folly::makeUnexpected(quic::TransportErrorCode::INTERNAL_ERROR);
  }
  return size;
}

WriteResult MoQFrameWriter::writeTrackStatus(
    folly::IOBufQueue& writeBuf,
    const TrackStatus& trackStatus) const noexcept {
  CHECK(version_.hasValue())
      << "version_ needs to be set to write TrackStatusRequest";
  size_t size = 0;
  bool error = false;
  auto sizePtr = writeFrameHeader(writeBuf, FrameType::TRACK_STATUS, error);

  if (getDraftMajorVersion(*version_) >= 14) {
    auto res = writeSubscribeRequestHelper(writeBuf, trackStatus);
    if (!res) {
      return res;
    }
    size += *res;
  } else {
    if (getDraftMajorVersion(*version_) >= 11) {
      writeVarint(writeBuf, trackStatus.requestID.value, size, error);
    }
    writeFullTrackName(writeBuf, trackStatus.fullTrackName, size, error);
    if (getDraftMajorVersion(*version_) >= 11) {
      writeTrackRequestParams(writeBuf, trackStatus.params, size, error);
    }
  }
  writeSize(sizePtr, size, error, *version_);
  if (error) {
    return folly::makeUnexpected(quic::TransportErrorCode::INTERNAL_ERROR);
  }
  return size;
}

WriteResult MoQFrameWriter::writeTrackStatusOk(
    folly::IOBufQueue& writeBuf,
    const TrackStatusOk& trackStatusOk) const noexcept {
  CHECK(version_.hasValue()) << "version_ needs to be set to write TrackStatus";

  size_t size = 0;
  bool error = false;
  auto sizePtr = writeFrameHeader(writeBuf, FrameType::TRACK_STATUS_OK, error);
  if (getDraftMajorVersion(*version_) >= 14) {
    auto res = writeSubscribeOkHelper(
        writeBuf,
        SubscribeOk(
            {trackStatusOk.requestID,
             trackStatusOk.trackAlias,
             trackStatusOk.expires,
             trackStatusOk.groupOrder,
             trackStatusOk.largest,
             trackStatusOk.params}));
    if (!res) {
      return res;
    }
    size += *res;
  } else {
    if (getDraftMajorVersion(*version_) >= 11) {
      writeVarint(writeBuf, trackStatusOk.requestID.value, size, error);
    } else {
      writeFullTrackName(writeBuf, trackStatusOk.fullTrackName, size, error);
    }
    writeVarint(
        writeBuf, folly::to_underlying(trackStatusOk.statusCode), size, error);
    if (trackStatusOk.statusCode == TrackStatusCode::IN_PROGRESS) {
      writeVarint(writeBuf, trackStatusOk.largest->group, size, error);
      writeVarint(writeBuf, trackStatusOk.largest->object, size, error);
    } else {
      writeVarint(writeBuf, 0, size, error);
      writeVarint(writeBuf, 0, size, error);
    }
    if (getDraftMajorVersion(*version_) >= 11) {
      writeTrackRequestParams(writeBuf, trackStatusOk.params, size, error);
    }
  }
  writeSize(sizePtr, size, error, *version_);
  if (error) {
    return folly::makeUnexpected(quic::TransportErrorCode::INTERNAL_ERROR);
  }
  return size;
}

WriteResult MoQFrameWriter::writeTrackStatusError(
    folly::IOBufQueue& writeBuf,
    const TrackStatusError& trackStatusError) const noexcept {
  return writeRequestError(
      writeBuf, trackStatusError, FrameType::TRACK_STATUS_ERROR);
}

WriteResult MoQFrameWriter::writeGoaway(
    folly::IOBufQueue& writeBuf,
    const Goaway& goaway) const noexcept {
  CHECK(version_.hasValue()) << "Version needs to be set to write Goaway";
  size_t size = 0;
  bool error = false;
  auto sizePtr = writeFrameHeader(writeBuf, FrameType::GOAWAY, error);
  writeFixedString(writeBuf, goaway.newSessionUri, size, error);
  writeSize(sizePtr, size, error, *version_);
  if (error) {
    return folly::makeUnexpected(quic::TransportErrorCode::INTERNAL_ERROR);
  }
  return size;
}

WriteResult MoQFrameWriter::writeSubscribeAnnounces(
    folly::IOBufQueue& writeBuf,
    const SubscribeAnnounces& subscribeAnnounces) const noexcept {
  CHECK(version_.hasValue())
      << "Version needs to be set to write subscribe announces";
  size_t size = 0;
  bool error = false;
  auto sizePtr =
      writeFrameHeader(writeBuf, FrameType::SUBSCRIBE_ANNOUNCES, error);
  if (getDraftMajorVersion(*version_) >= 11) {
    writeVarint(writeBuf, subscribeAnnounces.requestID.value, size, error);
  }
  writeTrackNamespace(
      writeBuf, subscribeAnnounces.trackNamespacePrefix, size, error);
  writeTrackRequestParams(writeBuf, subscribeAnnounces.params, size, error);
  writeSize(sizePtr, size, error, *version_);
  if (error) {
    return folly::makeUnexpected(quic::TransportErrorCode::INTERNAL_ERROR);
  }
  return size;
}

WriteResult MoQFrameWriter::writeSubscribeAnnouncesOk(
    folly::IOBufQueue& writeBuf,
    const SubscribeAnnouncesOk& subscribeAnnouncesOk) const noexcept {
  return writeRequestOk(
      writeBuf, subscribeAnnouncesOk, FrameType::SUBSCRIBE_ANNOUNCES_OK);
}

WriteResult MoQFrameWriter::writeUnsubscribeAnnounces(
    folly::IOBufQueue& writeBuf,
    const UnsubscribeAnnounces& unsubscribeAnnounces) const noexcept {
  CHECK(version_.hasValue())
      << "Version needs to be set to write unsubscribe announces";
  size_t size = 0;
  bool error = false;
  auto sizePtr =
      writeFrameHeader(writeBuf, FrameType::UNSUBSCRIBE_ANNOUNCES, error);
  writeTrackNamespace(
      writeBuf, unsubscribeAnnounces.trackNamespacePrefix, size, error);
  writeSize(sizePtr, size, error, *version_);
  if (error) {
    return folly::makeUnexpected(quic::TransportErrorCode::INTERNAL_ERROR);
  }
  return size;
}

WriteResult MoQFrameWriter::writeFetch(
    folly::IOBufQueue& writeBuf,
    const Fetch& fetch) const noexcept {
  CHECK(version_.hasValue()) << "Version needs to be set to write fetch";
  size_t size = 0;
  bool error = false;
  auto sizePtr = writeFrameHeader(writeBuf, FrameType::FETCH, error);
  writeVarint(writeBuf, fetch.requestID.value, size, error);

  writeBuf.append(&fetch.priority, 1);
  size += 1;
  auto order = folly::to_underlying(fetch.groupOrder);
  writeBuf.append(&order, 1);
  size += 1;

  auto [standalone, joining] = fetchType(fetch);
  if (standalone) {
    writeVarint(
        writeBuf, folly::to_underlying(FetchType::STANDALONE), size, error);
    writeFullTrackName(writeBuf, fetch.fullTrackName, size, error);
    writeVarint(writeBuf, standalone->start.group, size, error);
    writeVarint(writeBuf, standalone->start.object, size, error);
    writeVarint(writeBuf, standalone->end.group, size, error);
    writeVarint(writeBuf, standalone->end.object, size, error);
  } else {
    CHECK(joining);

    writeVarint(
        writeBuf, folly::to_underlying(joining->fetchType), size, error);
    writeVarint(writeBuf, joining->joiningRequestID.value, size, error);
    writeVarint(writeBuf, joining->joiningStart, size, error);
  }
  writeTrackRequestParams(writeBuf, fetch.params, size, error);

  writeSize(sizePtr, size, error, *version_);
  if (error) {
    return folly::makeUnexpected(quic::TransportErrorCode::INTERNAL_ERROR);
  }
  return size;
}

WriteResult MoQFrameWriter::writeFetchCancel(
    folly::IOBufQueue& writeBuf,
    const FetchCancel& fetchCancel) const noexcept {
  CHECK(version_.hasValue()) << "Version needs to be set to write fetch cancel";
  size_t size = 0;
  bool error = false;
  auto sizePtr = writeFrameHeader(writeBuf, FrameType::FETCH_CANCEL, error);
  writeVarint(writeBuf, fetchCancel.requestID.value, size, error);
  writeSize(sizePtr, size, error, *version_);
  if (error) {
    return folly::makeUnexpected(quic::TransportErrorCode::INTERNAL_ERROR);
  }
  return size;
}

WriteResult MoQFrameWriter::writeFetchOk(
    folly::IOBufQueue& writeBuf,
    const FetchOk& fetchOk) const noexcept {
  CHECK(version_.hasValue()) << "Version needs to be set to write fetch ok";
  size_t size = 0;
  bool error = false;
  auto sizePtr = writeFrameHeader(writeBuf, FrameType::FETCH_OK, error);
  writeVarint(writeBuf, fetchOk.requestID.value, size, error);
  auto order = folly::to_underlying(fetchOk.groupOrder);
  writeBuf.append(&order, 1);
  size += 1;
  writeBuf.append(&fetchOk.endOfTrack, 1);
  size += 1;
  writeVarint(writeBuf, fetchOk.endLocation.group, size, error);
  writeVarint(writeBuf, fetchOk.endLocation.object, size, error);
  writeTrackRequestParams(writeBuf, fetchOk.params, size, error);
  writeSize(sizePtr, size, error, *version_);
  if (error) {
    return folly::makeUnexpected(quic::TransportErrorCode::INTERNAL_ERROR);
  }
  return size;
}

// Unified request error writing function
WriteResult MoQFrameWriter::writeRequestError(
    folly::IOBufQueue& writeBuf,
    const RequestError& requestError,
    FrameType frameType) const noexcept {
  CHECK(version_.hasValue())
      << "Version needs to be set to write request error";
  // XCHECK that frameType is one of the allowed types for this function
  XCHECK(
      frameType == FrameType::SUBSCRIBE_ERROR ||
      frameType == FrameType::REQUEST_ERROR ||
      frameType == FrameType::ANNOUNCE_ERROR ||
      frameType == FrameType::SUBSCRIBE_ANNOUNCES_ERROR ||
      frameType == FrameType::PUBLISH_ERROR ||
      frameType == FrameType::FETCH_ERROR ||
      frameType == FrameType::TRACK_STATUS_ERROR)
      << "Invalid frameType passed to writeRequestError: "
      << static_cast<int>(frameType);

  size_t size = 0;
  bool error = false;
  if (getDraftMajorVersion(*version_) > 14) {
    frameType = FrameType::REQUEST_ERROR;
  }
  auto sizePtr = writeFrameHeader(writeBuf, frameType, error);

  writeVarint(writeBuf, requestError.requestID.value, size, error);
  writeVarint(
      writeBuf, folly::to_underlying(requestError.errorCode), size, error);
  writeFixedString(writeBuf, requestError.reasonPhrase, size, error);

  // Handle different frame types without switch statement
  if (frameType == FrameType::SUBSCRIBE_ERROR) {
    // Only v11 needs retryAlias field (write as 0), v12 doesn't have it
    if (getDraftMajorVersion(*version_) < 12) {
      writeVarint(writeBuf, 0, size, error); // retryAlias = 0
    }
  }

  writeSize(sizePtr, size, error, *version_);
  if (error) {
    return folly::makeUnexpected(quic::TransportErrorCode::INTERNAL_ERROR);
  }
  return size;
}

namespace {
const char* getFrameTypeString(FrameType type) {
  switch (type) {
    case FrameType::LEGACY_CLIENT_SETUP:
      return "LEGACY_CLIENT_SETUP";
    case FrameType::LEGACY_SERVER_SETUP:
      return "LEGACY_SERVER_SETUP";
    case FrameType::SUBSCRIBE:
      return "SUBSCRIBE";
    case FrameType::SUBSCRIBE_OK:
      return "SUBSCRIBE_OK";
    case FrameType::SUBSCRIBE_ERROR:
      return "SUBSCRIBE_ERROR";
    case FrameType::SUBSCRIBE_DONE:
      return "SUBSCRIBE_DONE";
    case FrameType::MAX_REQUEST_ID:
      return "MAX_REQUEST_ID";
    case FrameType::UNSUBSCRIBE:
      return "UNSUBSCRIBE";
    case FrameType::ANNOUNCE:
      return "ANNOUNCE";
    case FrameType::ANNOUNCE_OK:
      return "ANNOUNCE_OK";
    case FrameType::ANNOUNCE_ERROR:
      return "ANNOUNCE_ERROR";
    case FrameType::UNANNOUNCE:
      return "UNANNOUNCE";
    case FrameType::GOAWAY:
      return "GOAWAY";
    default:
      // can happen when type was cast from uint8_t
      return "Unknown";
  }
  LOG(FATAL) << "Unreachable";
}

const char* getStreamTypeString(StreamType type) {
  switch (type) {
    case StreamType::SUBGROUP_HEADER_SG:
    case StreamType::SUBGROUP_HEADER_SG_EXT:
    case StreamType::SUBGROUP_HEADER_SG_FIRST:
    case StreamType::SUBGROUP_HEADER_SG_FIRST_EXT:
    case StreamType::SUBGROUP_HEADER_SG_ZERO:
    case StreamType::SUBGROUP_HEADER_SG_ZERO_EXT:
      return "SUBGROUP_HEADER";
    case StreamType::FETCH_HEADER:
      return "FETCH_HEADER";
    default:
      // can happen when type was cast from uint8_t
      return "Unknown";
  }
  LOG(FATAL) << "Unreachable";
}

const char* getObjectStatusString(ObjectStatus objectStatus) {
  switch (objectStatus) {
    case ObjectStatus::NORMAL:
      return "NORMAL";
    case ObjectStatus::OBJECT_NOT_EXIST:
      return "OBJECT_NOT_EXIST";
    case ObjectStatus::GROUP_NOT_EXIST:
      return "GROUP_NOT_EXIST";
    case ObjectStatus::END_OF_GROUP:
      return "END_OF_GROUP";
    case ObjectStatus::END_OF_TRACK:
      return "END_OF_TRACK";
    default:
      // can happen when type was cast from uint8_t
      return "Unknown";
  }
  LOG(FATAL) << "Unreachable";
}
} // namespace

std::ostream& operator<<(std::ostream& os, FrameType type) {
  os << getFrameTypeString(type);
  return os;
}

std::ostream& operator<<(std::ostream& os, StreamType type) {
  os << getStreamTypeString(type);
  return os;
}

std::ostream& operator<<(std::ostream& os, ObjectStatus status) {
  os << getObjectStatusString(status);
  return os;
}

std::ostream& operator<<(std::ostream& os, TrackAlias alias) {
  os << alias.value;
  return os;
}

std::ostream& operator<<(std::ostream& os, RequestID id) {
  os << id.value;
  return os;
}

std::ostream& operator<<(std::ostream& os, const ObjectHeader& header) {
  os << " group=" << header.group << " subgroup=" << header.subgroup
     << " id=" << header.id << " priority=" << uint32_t(header.priority)
     << " status=" << getObjectStatusString(header.status) << " length="
     << (header.length.hasValue() ? std::to_string(header.length.value())
                                  : "none");
  return os;
}

// Moved inline functions to reduce binary bloat
Extensions noExtensions() {
  return Extensions();
}

std::pair<StandaloneFetch*, JoiningFetch*> fetchType(Fetch& fetch) {
  auto standalone = std::get_if<StandaloneFetch>(&fetch.args);
  auto joining = std::get_if<JoiningFetch>(&fetch.args);
  return {standalone, joining};
}

std::pair<const StandaloneFetch*, const JoiningFetch*> fetchType(
    const Fetch& fetch) {
  auto standalone = std::get_if<StandaloneFetch>(&fetch.args);
  auto joining = std::get_if<JoiningFetch>(&fetch.args);
  return {standalone, joining};
}

bool isValidDatagramType(uint64_t version, uint64_t datagramType) {
  auto majorVersion = getDraftMajorVersion(version);
  if (majorVersion == 11) {
    return datagramType <=
        folly::to_underlying(DatagramType::OBJECT_DATAGRAM_STATUS_EXT_V11);
  } else {
    return (
        datagramType <= folly::to_underlying(
                            DatagramType::OBJECT_DATAGRAM_EXT_EOG_ID_ZERO) ||
        (datagramType >=
             folly::to_underlying(DatagramType::OBJECT_DATAGRAM_STATUS) &&
         datagramType <=
             folly::to_underlying(DatagramType::OBJECT_DATAGRAM_STATUS_EXT)));
  }
}

} // namespace moxygen

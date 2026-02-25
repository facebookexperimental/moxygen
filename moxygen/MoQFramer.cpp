/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/MoQFramer.h"
#include <folly/container/F14Map.h>
#include <folly/container/F14Set.h>
#include <folly/lang/Bits.h>
#include <folly/logging/xlog.h>

#include <utility>

namespace {
constexpr uint64_t kMaxExtensionLength = 1024;

enum class FetchHeaderSerializationBits : uint8_t {
  // Draft-15: 0xC0 reserved.
  // Draft-16+: only 0x80 reserved, 0x40 = datagram
  LEGACY_RESERVED_BITMASK = 0xC0, // Draft-15
  RESERVED_BITMASK = 0x80,        // Draft-16+
  DATAGRAM_BITMASK = 0x40,        // Datagram forwarding preference (draft-16+)
  GROUP_ID_BITMASK = 0x8,
  SUBGROUP_MODE_BITMASK = 0x3,
  SUBGROUP_ID_ZERO = 0x0,
  SUBGROUP_ID_SAME_AS_PRIOR = 0x1,
  SUBGROUP_ID_INC_BY_ONE = 0x2,
  OBJECT_ID_BITMASK = 0x4,
  PRIORITY_BITMASK = 0x10,
  EXTENSIONS_BITMASK = 0x20
};

uint64_t getLocationTypeValue(
    moxygen::LocationType locationType,
    uint64_t version) {
  if (locationType != moxygen::LocationType::LargestGroup) {
    return folly::to_underlying(locationType);
  }

  return folly::to_underlying(locationType);
}

bool isRequestSpecificParam(moxygen::TrackRequestParamKey key) {
  switch (key) {
    case moxygen::TrackRequestParamKey::SUBSCRIPTION_FILTER:
    case moxygen::TrackRequestParamKey::LARGEST_OBJECT:
    case moxygen::TrackRequestParamKey::EXPIRES:
    case moxygen::TrackRequestParamKey::GROUP_ORDER:
    case moxygen::TrackRequestParamKey::SUBSCRIBER_PRIORITY:
    case moxygen::TrackRequestParamKey::FORWARD:
      return true;
    default:
      return false;
  }
}

bool isValidGroupOrderParam(uint64_t value) {
  switch (value) {
    case folly::to_underlying(moxygen::GroupOrder::OldestFirst):
    case folly::to_underlying(moxygen::GroupOrder::NewestFirst):
      return true;
    default:
      return false;
  }
  return true;
}

bool isValidSubscriberPriorityParam(uint64_t value) {
  // Valid range is 0-255
  return value <= 255;
}

bool isValidForwardParam(uint64_t value) {
  // Valid values are 0 or 1
  return value <= 1;
}

bool isIntParamValid(uint64_t version, uint64_t key, uint64_t value) {
  if (moxygen::getDraftMajorVersion(version) >= 15) {
    switch (key) {
      case folly::to_underlying(moxygen::TrackRequestParamKey::GROUP_ORDER):
        return isValidGroupOrderParam(value);
      case folly::to_underlying(
          moxygen::TrackRequestParamKey::SUBSCRIBER_PRIORITY):
        return isValidSubscriberPriorityParam(value);
      case folly::to_underlying(moxygen::TrackRequestParamKey::FORWARD):
        return isValidForwardParam(value);
      default:
        return true;
    }
  }

  return true;
}

std::vector<moxygen::Parameter> sortParamsByKey(
    std::vector<moxygen::Parameter> params) {
  std::sort(
      params.begin(),
      params.end(),
      [](const moxygen::Parameter& a, const moxygen::Parameter& b) {
        return a.key < b.key;
      });
  return params;
}

std::vector<moxygen::Parameter> mergeAndSortParams(
    const std::vector<moxygen::Parameter>& requestSpecificParams,
    const moxygen::Parameters& params) {
  std::vector<moxygen::Parameter> allParams;
  allParams.reserve(requestSpecificParams.size() + params.size());

  for (const auto& param : requestSpecificParams) {
    allParams.push_back(param);
  }
  for (const auto& param : params) {
    allParams.push_back(param);
  }

  return sortParamsByKey(std::move(allParams));
}

std::vector<moxygen::Extension> sortExtensionsByType(
    std::vector<moxygen::Extension> extensions) {
  std::sort(
      extensions.begin(),
      extensions.end(),
      [](const moxygen::Extension& a, const moxygen::Extension& b) {
        return a.type < b.type;
      });
  return extensions;
}

// Helper for delta decoding with overflow check.
folly::Expected<uint64_t, moxygen::ErrorCode> decodeDelta(
    uint64_t previous,
    uint64_t delta) {
  if (delta > std::numeric_limits<uint64_t>::max() - previous) {
    return folly::makeUnexpected(moxygen::ErrorCode::PROTOCOL_VIOLATION);
  }
  return previous + delta;
}

} // namespace

namespace moxygen {

// Frame type sets for parameter allowlist
const folly::F14FastSet<FrameType> kAllowedFramesForAuthToken = {
    FrameType::PUBLISH,
    FrameType::SUBSCRIBE,
    FrameType::SUBSCRIBE_UPDATE,
    FrameType::SUBSCRIBE_NAMESPACE,
    FrameType::PUBLISH_NAMESPACE,
    FrameType::TRACK_STATUS,
    FrameType::FETCH};

const folly::F14FastSet<FrameType> kAllowedFramesForDeliveryTimeout = {
    FrameType::PUBLISH_OK,
    FrameType::SUBSCRIBE,
    FrameType::SUBSCRIBE_UPDATE};

const folly::F14FastSet<FrameType> kAllowedFramesForSubscriberPriority = {
    FrameType::SUBSCRIBE,
    FrameType::FETCH,
    FrameType::SUBSCRIBE_UPDATE,
    FrameType::PUBLISH_OK};

const folly::F14FastSet<FrameType> kAllowedFramesForSubscriptionFilter = {
    FrameType::SUBSCRIBE,
    FrameType::PUBLISH_OK,
    FrameType::SUBSCRIBE_UPDATE};

const folly::F14FastSet<FrameType> kAllowedFramesForExpires = {
    FrameType::SUBSCRIBE_OK,
    FrameType::PUBLISH,
    FrameType::PUBLISH_OK,
    FrameType::REQUEST_OK};

const folly::F14FastSet<FrameType> kAllowedFramesForGroupOrder = {
    FrameType::SUBSCRIBE,
    FrameType::PUBLISH_OK,
    FrameType::FETCH};

const folly::F14FastSet<FrameType> kAllowedFramesForLargestObject = {
    FrameType::SUBSCRIBE_OK,
    FrameType::PUBLISH,
    FrameType::REQUEST_OK};

const folly::F14FastSet<FrameType> kAllowedFramesForForward = {
    FrameType::SUBSCRIBE,
    FrameType::SUBSCRIBE_UPDATE,
    FrameType::PUBLISH,
    FrameType::PUBLISH_OK,
    FrameType::SUBSCRIBE_NAMESPACE};

// Allowlist mapping: TrackRequestParamKey -> set of allowed FrameTypes
// Empty set means allowed for all frame types
const folly::F14FastMap<TrackRequestParamKey, folly::F14FastSet<FrameType>>
    kParamAllowlist = {
        {TrackRequestParamKey::AUTHORIZATION_TOKEN, kAllowedFramesForAuthToken},
        {TrackRequestParamKey::DELIVERY_TIMEOUT,
         kAllowedFramesForDeliveryTimeout},
        {TrackRequestParamKey::MAX_CACHE_DURATION, {}},
        {TrackRequestParamKey::PUBLISHER_PRIORITY, {}},
        {TrackRequestParamKey::SUBSCRIBER_PRIORITY,
         kAllowedFramesForSubscriberPriority},
        {TrackRequestParamKey::SUBSCRIPTION_FILTER,
         kAllowedFramesForSubscriptionFilter},
        {TrackRequestParamKey::EXPIRES, kAllowedFramesForExpires},
        {TrackRequestParamKey::GROUP_ORDER, kAllowedFramesForGroupOrder},
        {TrackRequestParamKey::LARGEST_OBJECT, kAllowedFramesForLargestObject},
        {TrackRequestParamKey::FORWARD, kAllowedFramesForForward},
};

// Frame types that allow all parameters (no validation)
const folly::F14FastSet<FrameType> kAllowAllParamsFrameTypes = {
    FrameType::CLIENT_SETUP,
    FrameType::SERVER_SETUP,
    FrameType::LEGACY_CLIENT_SETUP,
    FrameType::LEGACY_SERVER_SETUP,
};

// Forward declarations for iOS.
enum class ParamsType { ClientSetup, ServerSetup, Request };
folly::Expected<folly::Unit, ErrorCode> parseParams(
    folly::io::Cursor& cursor,
    size_t& length,
    uint64_t version,
    size_t numParams,
    Parameters& params,
    std::vector<Parameter>& requestSpecificParams,
    MoQTokenCache& tokenCache,
    ParamsType paramsType);
folly::Expected<std::optional<AuthToken>, ErrorCode> parseToken(
    folly::io::Cursor& cursor,
    size_t length,
    MoQTokenCache& tokenCache,
    ParamsType paramsType) noexcept;
folly::Expected<std::optional<Parameter>, ErrorCode> parseVariableParam(
    folly::io::Cursor& cursor,
    size_t& length,
    uint64_t version,
    uint64_t key,
    MoQTokenCache& tokenCache,
    ParamsType paramsType);
folly::Expected<AbsoluteLocation, ErrorCode> parseAbsoluteLocation(
    folly::io::Cursor& cursor,
    size_t& length) noexcept;
folly::Expected<SubscriptionFilter, ErrorCode> parseSubscriptionFilter(
    folly::io::Cursor& cursor,
    size_t& length) noexcept;
folly::Expected<std::optional<Parameter>, ErrorCode> parseIntParam(
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

folly::Expected<std::string, ErrorCode> parseFixedString(
    folly::io::Cursor& cursor,
    size_t& length) {
  auto strLength = quic::follyutils::decodeQuicInteger(cursor, length);
  if (!strLength) {
    XLOG(DBG4) << "parseFixedString: UNDERFLOW on strLength";
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= strLength->second;
  if (strLength->first > length) {
    XLOG(DBG4) << "parseFixedString: UNDERFLOW on length check";
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  auto res = cursor.readFixedString(strLength->first);
  length -= strLength->first;
  return res;
}

folly::Expected<std::optional<AuthToken>, ErrorCode> parseToken(
    folly::io::Cursor& cursor,
    size_t length,
    MoQTokenCache& tokenCache,
    ParamsType paramsType) noexcept {
  std::optional<AuthToken> token;
  token.emplace(); // plan for success
  auto aliasType = quic::follyutils::decodeQuicInteger(cursor, length);
  if (!aliasType) {
    XLOG(DBG4) << "parseToken: UNDERFLOW on aliasType";
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  if (aliasType->first > folly::to_underlying(AliasType::USE_VALUE)) {
    XLOG(ERR) << "aliasType > USE_VALUE =" << aliasType->first;
    return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
  }
  auto aliasTypeVal = static_cast<AliasType>(aliasType->first);
  length -= aliasType->second;

  switch (aliasTypeVal) {
    case AliasType::DELETE_ALIAS:
    case AliasType::USE_ALIAS: {
      if (paramsType == ParamsType::ClientSetup) {
        XLOG(ERR) << "Can't delete/use-alias in client setup";
        return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
      }
      auto tokenAlias = quic::follyutils::decodeQuicInteger(cursor, length);
      if (!tokenAlias) {
        XLOG(DBG4) << "parseToken: UNDERFLOW on tokenAlias (DELETE/USE_ALIAS)";
        return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
      }
      length -= tokenAlias->second;
      token->alias = tokenAlias->first;

      if (aliasTypeVal == AliasType::DELETE_ALIAS) {
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
        XLOG(DBG4) << "parseToken: UNDERFLOW on tokenAlias (REGISTER)";
        return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
      }
      length -= tokenAlias->second;
      token->alias = tokenAlias->first;

      auto tokenType = quic::follyutils::decodeQuicInteger(cursor, length);
      if (!tokenType) {
        XLOG(DBG4) << "parseToken: UNDERFLOW on tokenType (REGISTER)";
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
        XLOG(DBG4) << "parseToken: UNDERFLOW on tokenType (USE_VALUE)";
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

folly::Expected<AbsoluteLocation, ErrorCode> parseAbsoluteLocation(
    folly::io::Cursor& cursor,
    size_t& length) noexcept {
  AbsoluteLocation location;
  auto group = quic::follyutils::decodeQuicInteger(cursor, length);
  if (!group) {
    XLOG(DBG4) << "parseAbsoluteLocation: UNDERFLOW on group";
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  location.group = group->first;
  length -= group->second;

  auto object = quic::follyutils::decodeQuicInteger(cursor, length);
  if (!object) {
    XLOG(DBG4) << "parseAbsoluteLocation: UNDERFLOW on object";
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  location.object = object->first;
  length -= object->second;

  return location;
}

folly::Expected<SubscriptionFilter, ErrorCode> parseSubscriptionFilter(
    folly::io::Cursor& cursor,
    size_t& length) noexcept {
  SubscriptionFilter filter;

  // Parse filter type
  auto filterType = quic::follyutils::decodeQuicInteger(cursor, length);
  if (!filterType) {
    XLOG(DBG4) << "parseSubscriptionFilter: UNDERFLOW on filterType";
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= filterType->second;

  // Validate filter type
  switch (filterType->first) {
    case folly::to_underlying(LocationType::NextGroupStart):
    case folly::to_underlying(LocationType::LargestObject):
    case folly::to_underlying(LocationType::AbsoluteStart):
    case folly::to_underlying(LocationType::AbsoluteRange):
    // Note: LargestGroup in SubscriptionFilter is non-spec at the
    // time of writing this (draft-15), but will be soon
    case folly::to_underlying(LocationType::LargestGroup):
      break;
    default:
      XLOG(ERR) << "Invalid filter type in parseSubscriptionFilter, type="
                << filterType->first;
      return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
  }

  filter.filterType = LocationType(filterType->first);

  // Parse start location if present (for AbsoluteStart and AbsoluteRange)
  if (filter.filterType == LocationType::AbsoluteStart ||
      filter.filterType == LocationType::AbsoluteRange) {
    auto location = parseAbsoluteLocation(cursor, length);
    if (!location) {
      return folly::makeUnexpected(location.error());
    }
    filter.location = *location;
  }

  // Parse end group if present (only for AbsoluteRange)
  if (filter.filterType == LocationType::AbsoluteRange) {
    auto endGroup = quic::follyutils::decodeQuicInteger(cursor, length);
    if (!endGroup) {
      XLOG(DBG4) << "parseSubscriptionFilter: UNDERFLOW on endGroup";
      return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
    }
    filter.endGroup = endGroup->first;
    length -= endGroup->second;
  }

  return filter;
}

folly::Expected<std::optional<Parameter>, ErrorCode> parseVariableParam(
    folly::io::Cursor& cursor,
    size_t& length,
    uint64_t version,
    uint64_t key,
    MoQTokenCache& tokenCache,
    ParamsType paramsType) {
  Parameter p;
  p.key = key;
  const auto authKey =
      folly::to_underlying(TrackRequestParamKey::AUTHORIZATION_TOKEN);
  const auto subscriptionFilterKey =
      folly::to_underlying(TrackRequestParamKey::SUBSCRIPTION_FILTER);
  if (key == authKey) {
    auto res = quic::follyutils::decodeQuicInteger(cursor, length);
    if (!res) {
      XLOG(DBG4) << "parseVariableParam: UNDERFLOW on authKey length";
      return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
    }
    length -= res->second;
    if (res->first > length || !cursor.canAdvance(res->first)) {
      XLOG(DBG4) << "parseVariableParam: UNDERFLOW on authKey data";
      return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
    }
    auto tokenRes = parseToken(cursor, res->first, tokenCache, paramsType);
    if (!tokenRes) {
      return folly::makeUnexpected(tokenRes.error());
    }
    length -= res->first;
    if (!tokenRes.value().has_value()) {
      // it was delete, don't export
      return std::nullopt;
    }
    p.asAuthToken = std::move(*tokenRes.value());
  } else if (
      key == subscriptionFilterKey && getDraftMajorVersion(version) >= 15) {
    // Read length prefix (odd key = length-prefixed)
    auto lenRes = quic::follyutils::decodeQuicInteger(cursor, length);
    if (!lenRes) {
      XLOG(DBG4)
          << "parseVariableParam: UNDERFLOW on subscriptionFilter length";
      return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
    }
    length -= lenRes->second;
    auto filterLen = lenRes->first;
    if (filterLen > length || !cursor.canAdvance(filterLen)) {
      XLOG(DBG4) << "parseVariableParam: UNDERFLOW on subscriptionFilter data";
      return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
    }
    size_t filterSize = filterLen;
    auto res = parseSubscriptionFilter(cursor, filterSize);
    if (!res) {
      return folly::makeUnexpected(res.error());
    }
    length -= lenRes->first;
    p.asSubscriptionFilter = res.value();
  }

  else {
    auto res = parseFixedString(cursor, length);
    if (!res) {
      XLOG(DBG4) << "parseVariableParam: UNDERFLOW on parseFixedString";
      return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
    }
    p.asString = std::move(res.value());
  }
  return p;
}

folly::Expected<std::optional<Parameter>, ErrorCode> parseIntParam(
    folly::io::Cursor& cursor,
    size_t& length,
    uint64_t version,
    uint64_t key) {
  Parameter p;
  p.key = key;
  auto res = quic::follyutils::decodeQuicInteger(cursor, length);
  if (!res) {
    XLOG(DBG4) << "parseIntParam: UNDERFLOW on integer decode";
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= res->second;
  p.asUint64 = res->first;

  if (!isIntParamValid(version, p.key, p.asUint64)) {
    return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
  }
  return p;
}

folly::Expected<folly::Unit, ErrorCode> parseParams(
    folly::io::Cursor& cursor,
    size_t& length,
    uint64_t version,
    size_t numParams,
    Parameters& params,
    std::vector<Parameter>& requestSpecificParams,
    MoQTokenCache& tokenCache,
    ParamsType paramsType) {
  uint64_t previousKey = 0;

  for (auto i = 0u; i < numParams; i++) {
    auto keyOrDelta = quic::follyutils::decodeQuicInteger(cursor, length);
    if (!keyOrDelta) {
      XLOG(DBG4) << "parseParams: UNDERFLOW on key";
      return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
    }
    length -= keyOrDelta->second;

    // For v16+: decode delta to get absolute key
    // For v15-: use absolute key directly
    uint64_t key;
    if (getDraftMajorVersion(version) >= 16) {
      auto decoded = decodeDelta(previousKey, keyOrDelta->first);
      if (decoded.hasError()) {
        return folly::makeUnexpected(decoded.error());
      }
      key = decoded.value();
      previousKey = key;
    } else {
      key = keyOrDelta->first;
    }

    if (getDraftMajorVersion(version) >= 16 &&
        paramsType == ParamsType::Request &&
        !Parameters::isKnownParamKey(key)) {
      XLOG(ERR) << "Unknown parameter key " << key << " in v16+";
      return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
    }

    folly::Expected<std::optional<Parameter>, ErrorCode> res;

    if ((paramsType == ParamsType::Request &&
         key == folly::to_underlying(TrackRequestParamKey::DELIVERY_TIMEOUT)) ||
        ((key & 0x01) == 0 &&
         (paramsType != ParamsType::Request ||
          key !=
              folly::to_underlying(
                  TrackRequestParamKey::AUTHORIZATION_TOKEN)))) {
      res = parseIntParam(cursor, length, version, key);
    } else if (
        key == folly::to_underlying(TrackRequestParamKey::LARGEST_OBJECT)) {
      if (getDraftMajorVersion(version) < 15) {
        XLOG(ERR) << "Invalid parameter LARGEST_OBJECT for version " << version;
        return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
      }

      // Read length prefix (odd key = length-prefixed)
      auto lenRes = quic::follyutils::decodeQuicInteger(cursor, length);
      if (!lenRes) {
        XLOG(DBG4) << "parseParams: UNDERFLOW on LARGEST_OBJECT length";
        return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
      }
      length -= lenRes->second;
      auto objLen = lenRes->first;
      if (objLen > length || !cursor.canAdvance(objLen)) {
        XLOG(DBG4) << "parseParams: UNDERFLOW on LARGEST_OBJECT data";
        return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
      }
      size_t objSize = objLen;
      auto largestLocation = parseAbsoluteLocation(cursor, objSize);
      if (!largestLocation) {
        XLOG(DBG4) << "parseParams: returning error from parseAbsoluteLocation";
        return folly::makeUnexpected(largestLocation.error());
      }
      length -= lenRes->first;
      res = Parameter(key, largestLocation.value());
    } else {
      res = parseVariableParam(
          cursor, length, version, key, tokenCache, paramsType);
    }
    if (!res) {
      XLOG(DBG4)
          << "parseParams: returning error from parseVariableParam/parseIntParam"
          << " at param index=" << i << ", key=" << key
          << ", version=" << version << ", length=" << length;
      return folly::makeUnexpected(res.error());
    }
    if (*res) {
      TrackRequestParamKey trackRequestParamKey = (TrackRequestParamKey)key;
      if (getDraftMajorVersion(version) >= 15 &&
          isRequestSpecificParam(trackRequestParamKey)) {
        requestSpecificParams.push_back(std::move(*res.value()));
      } else {
        auto insertResult = params.insertParam(std::move(*res.value()));
        if (insertResult.hasError()) {
          // Per spec: invalid params in received messages should be ignored
          XLOG(WARN) << "parseParams: ignoring param not allowed for frame type"
                     << " at param index=" << i << ", key=" << key;
        }
      }
    }
  }
  XLOG(DBG4) << "parseParams: returning success";
  return folly::unit;
}

folly::Expected<ClientSetup, ErrorCode> MoQFrameParser::parseClientSetup(
    folly::io::Cursor& cursor,
    size_t length) noexcept {
  ClientSetup clientSetup;
  uint64_t serializationVersion = kVersionDraft14;

  // Only parse version array when version is not initialized, i.e. alpn did not
  // happen, or when version is initialized but is < 15 (in tests)
  if (!version_ || getDraftMajorVersion(*version_) < 15) {
    auto numVersions = quic::follyutils::decodeQuicInteger(cursor, length);
    if (!numVersions) {
      XLOG(DBG4) << "parseClientSetup: UNDERFLOW on numVersions";
      return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
    }
    length -= numVersions->second;
    for (auto i = 0ul; i < numVersions->first; i++) {
      auto version = quic::follyutils::decodeQuicInteger(cursor, length);
      if (!version) {
        XLOG(DBG4) << "parseClientSetup: UNDERFLOW on version";
        return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
      }
      length -= version->second;
      if (!isSupportedVersion(version->first)) {
        XLOG(WARN) << "Peer advertised unsupported version " << version->first
                   << ", supported versions are: "
                   << getSupportedVersionsString();
        continue;
      }
      clientSetup.supportedVersions.push_back(version->first);
    }
  } else {
    XLOG(DBG3)
        << "Skipped parsing versions from wire for alpn ClientSetup message";
    serializationVersion = *version_;
  }

  auto numParams = quic::follyutils::decodeQuicInteger(cursor, length);
  if (!numParams) {
    XLOG(DBG4) << "parseClientSetup: UNDERFLOW on numParams";
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= numParams->second;
  std::vector<Parameter> requestSpecificParams;
  auto res = parseParams(
      cursor,
      length,
      serializationVersion,
      numParams->first,
      clientSetup.params,
      requestSpecificParams,
      *tokenCache_,
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
      XLOG(DBG4) << "parseServerSetup: UNDERFLOW on version";
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
    XLOG(DBG4) << "parseServerSetup: UNDERFLOW on numParams";
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= numParams->second;
  std::vector<Parameter> requestSpecificParams;
  auto res = parseParams(
      cursor,
      length,
      version_ ? *version_ : serverSetup.selectedVersion,
      numParams->first,
      serverSetup.params,
      requestSpecificParams,
      *tokenCache_,
      ParamsType::ServerSetup);
  if (res.hasError()) {
    return folly::makeUnexpected(res.error());
  }
  if (length > 0) {
    return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
  }
  return serverSetup;
}

folly::Expected<MoQFrameParser::ParseResultAndLength<RequestID>, ErrorCode>
MoQFrameParser::parseFetchHeader(folly::io::Cursor& cursor, size_t length)
    const noexcept {
  auto requestID = quic::follyutils::decodeQuicInteger(cursor, length);
  if (!requestID) {
    XLOG(DBG4) << "parseFetchHeader: UNDERFLOW on requestID";
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }

  // Reset context tracking at the start of each FETCH stream. This shouldn't
  // really be necessary since each stream has a separate MoQFrameParser, but
  // we're keeping it just for the sake of completeness.
  resetFetchContext();

  return ParseResultAndLength<RequestID>{
      RequestID(requestID->first), requestID->second};
}

void MoQFrameParser::resetFetchContext() const noexcept {
  previousFetchGroup_.reset();
  previousFetchSubgroup_.reset();
  previousObjectID_.reset();
  previousFetchPriority_.reset();
}

bool datagramTypeHasExtensions(uint64_t version, DatagramType datagramType) {
  return (folly::to_underlying(datagramType) & 0x1);
}

bool datagramTypeIsStatus(uint64_t version, DatagramType datagramType) {
  return (folly::to_underlying(datagramType) & 0x20);
}

bool datagramObjectIdZero(uint64_t version, DatagramType datagramType) {
  // 0 objectID type bit only supported in ver-14 and above
  if (getDraftMajorVersion(version) < 14) {
    return false;
  }
  return (folly::to_underlying(datagramType) & DG_OBJECT_ID_ZERO);
}

bool datagramPriorityPresent(uint64_t version, DatagramType datagramType) {
  // Priority is only conditionally present in version 15+
  if (getDraftMajorVersion(version) < 15) {
    return true; // Always present in older versions
  }
  return !(folly::to_underlying(datagramType) & DG_PRIORITY_NOT_PRESENT);
}

folly::Expected<DatagramObjectHeader, ErrorCode>
MoQFrameParser::parseDatagramObjectHeader(
    folly::io::Cursor& cursor,
    DatagramType datagramType,
    size_t& length) const noexcept {
  ObjectHeader objectHeader;
  auto trackAlias = quic::follyutils::decodeQuicInteger(cursor, length);
  if (!trackAlias) {
    XLOG(DBG4) << "parseDatagramObjectHeader: UNDERFLOW on trackAlias";
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= trackAlias->second;

  auto group = quic::follyutils::decodeQuicInteger(cursor, length);
  if (!group) {
    XLOG(DBG4) << "parseDatagramObjectHeader: UNDERFLOW on group";
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= group->second;
  objectHeader.group = group->first;

  if (!datagramObjectIdZero(*version_, datagramType)) {
    auto id = quic::follyutils::decodeQuicInteger(cursor, length);
    if (!id) {
      XLOG(DBG4) << "parseDatagramObjectHeader: UNDERFLOW on id";
      return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
    }
    length -= id->second;
    objectHeader.id = id->first;
  } else {
    // objectID=0 is not on the wire
    objectHeader.id = 0;
  }

  if (datagramPriorityPresent(*version_, datagramType)) {
    if (length == 0 || !cursor.canAdvance(1)) {
      XLOG(DBG4) << "parseDatagramObjectHeader: UNDERFLOW on priority";
      return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
    }
    objectHeader.priority = cursor.readBE<uint8_t>();
    length -= 1;
  } else {
    // Leave priority as std::nullopt if not present
    objectHeader.priority = std::nullopt;
  }
  if (datagramTypeHasExtensions(*version_, datagramType)) {
    auto ext = parseExtensions(cursor, length, objectHeader);
    if (!ext) {
      XLOG(DBG4) << "parseDatagramObjectHeader: error in parseExtensions: "
                 << uint64_t(ext.error());
      return folly::makeUnexpected(ext.error());
    }
  }

  if (datagramTypeIsStatus(*version_, datagramType)) {
    auto status = quic::follyutils::decodeQuicInteger(cursor, length);
    if (!status) {
      XLOG(DBG4) << "parseDatagramObjectHeader: UNDERFLOW on status";
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
  if (!isValidStatusForExtensions(objectHeader)) {
    return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
  }
  return DatagramObjectHeader(
      TrackAlias(trackAlias->first), std::move(objectHeader));
}

folly::Expected<
    MoQFrameParser::ParseResultAndLength<MoQFrameParser::SubgroupHeaderResult>,
    ErrorCode>
MoQFrameParser::parseSubgroupHeader(
    folly::io::Cursor& cursor,
    size_t length,
    const SubgroupOptions& options) const noexcept {
  CHECK(version_.has_value())
      << "The version must be set before parsing subgroup header";
  auto startLength = length;
  SubgroupHeaderResult result;
  ObjectHeader& objectHeader = result.objectHeader;
  objectHeader.group = std::numeric_limits<uint64_t>::max(); // unset
  objectHeader.id = std::numeric_limits<uint64_t>::max();    // unset

  auto parsedTrackAlias = quic::follyutils::decodeQuicInteger(cursor, length);
  if (!parsedTrackAlias) {
    XLOG(DBG4) << "parseSubgroupHeader: UNDERFLOW on trackAlias";
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= parsedTrackAlias->second;
  result.trackAlias = TrackAlias(parsedTrackAlias->first);

  auto group = quic::follyutils::decodeQuicInteger(cursor, length);
  if (!group) {
    XLOG(DBG4) << "parseSubgroupHeader: UNDERFLOW on group";
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= group->second;
  objectHeader.group = group->first;

  bool parseObjectID = false;
  if (options.subgroupIDFormat == SubgroupIDFormat::Present) {
    auto subgroup = quic::follyutils::decodeQuicInteger(cursor, length);
    if (!subgroup) {
      XLOG(DBG4) << "parseSubgroupHeader: UNDERFLOW on subgroup";
      return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
    }
    objectHeader.subgroup = subgroup->first;
    length -= subgroup->second;
  } else if (options.subgroupIDFormat == SubgroupIDFormat::Zero) {
    objectHeader.subgroup = 0;
  } else {
    parseObjectID = true;
  }
  // Conditionally parse priority based on version and stream type
  if (options.priorityPresent) {
    if (length == 0 || !cursor.canAdvance(1)) {
      XLOG(DBG4) << "parseSubgroupHeader: UNDERFLOW on priority";
      return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
    }
    objectHeader.priority = cursor.readBE<uint8_t>();
    length -= 1;
  } else {
    // Leave priority as std::nullopt if not present
    XCHECK_GE(getDraftMajorVersion(*version_), 15u);
    objectHeader.priority = std::nullopt;
  }
  if (parseObjectID) {
    auto tmpCursor = cursor; // we reparse the object ID later
    auto id = quic::follyutils::decodeQuicInteger(tmpCursor, length);
    if (!id) {
      XLOG(DBG4) << "parseSubgroupHeader: UNDERFLOW on id";
      return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
    }
    objectHeader.subgroup = objectHeader.id = id->first;
  }
  return ParseResultAndLength<SubgroupHeaderResult>{
      result, startLength - length};
}
folly::Expected<ObjectHeader, ErrorCode>
MoQFrameParser::parseFetchObjectHeaderLegacy(
    folly::io::Cursor& cursor,
    size_t& length,
    const ObjectHeader& headerTemplate) const noexcept {
  // Legacy FETCH object format (draft <= 14): all fields explicit
  auto remainingLength = length;
  ObjectHeader objectHeader = headerTemplate;

  // Group ID (varint)
  auto group = quic::follyutils::decodeQuicInteger(cursor, remainingLength);
  if (!group) {
    XLOG(DBG4) << "parseFetchObjectHeaderLegacy: UNDERFLOW on group";
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  remainingLength -= group->second;
  objectHeader.group = group->first;

  // Subgroup ID (varint)
  auto subgroup = quic::follyutils::decodeQuicInteger(cursor, remainingLength);
  if (!subgroup) {
    XLOG(DBG4) << "parseFetchObjectHeaderLegacy: UNDERFLOW on subgroup";
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  remainingLength -= subgroup->second;
  objectHeader.subgroup = subgroup->first;

  // Object ID (varint)
  auto id = quic::follyutils::decodeQuicInteger(cursor, remainingLength);
  if (!id) {
    XLOG(DBG4) << "parseFetchObjectHeaderLegacy: UNDERFLOW on id";
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  remainingLength -= id->second;
  objectHeader.id = id->first;

  // Priority (8-bit)
  if (remainingLength < 1) {
    XLOG(DBG4) << "parseFetchObjectHeaderLegacy: UNDERFLOW on priority";
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  objectHeader.priority = cursor.readBE<uint8_t>();
  remainingLength -= 1;

  // Extensions (if present)
  auto ext = parseExtensions(cursor, remainingLength, objectHeader);
  if (!ext) {
    XLOG(DBG4) << "parseFetchObjectHeaderLegacy: error in parseExtensions: "
               << folly::to_underlying(ext.error());
    return folly::makeUnexpected(ext.error());
  }

  // Object status and payload length
  auto res = parseObjectStatusAndLength(cursor, remainingLength, objectHeader);
  if (!res) {
    XLOG(DBG4)
        << "parseFetchObjectHeaderLegacy: error in parseObjectStatusAndLength: "
        << folly::to_underlying(res.error());
    return folly::makeUnexpected(res.error());
  }
  if (!isValidStatusForExtensions(objectHeader)) {
    return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
  }

  length = remainingLength;

  return objectHeader;
}

folly::Expected<MoQFrameParser::FetchObjectParseResult, ErrorCode>
MoQFrameParser::parseFetchObjectDraft15(
    folly::io::Cursor& cursor,
    size_t& length,
    const ObjectHeader& headerTemplate) const noexcept {
  // Draft-15+ parser with Serialization Flags
  auto remainingLength = length;
  ObjectHeader objectHeader = headerTemplate;

  // Read Serialization Flags - varint for v16+, single byte for v15
  uint64_t flags;
  if (getDraftMajorVersion(*version_) >= 16) {
    auto flagsResult =
        quic::follyutils::decodeQuicInteger(cursor, remainingLength);
    if (!flagsResult) {
      XLOG(DBG4) << "parseFetchObjectDraft15: UNDERFLOW on flags";
      return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
    }
    flags = flagsResult->first;
    remainingLength -= flagsResult->second;
  } else {
    // v15: single byte
    if (remainingLength < 1 || !cursor.canAdvance(1)) {
      XLOG(DBG4) << "parseFetchObjectDraft15: UNDERFLOW on flags";
      return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
    }
    flags = cursor.readBE<uint8_t>();
    remainingLength -= 1;
  }

  // Check for End of Range markers (0x8C = non-existent, 0x10C = unknown)
  // End of Range markers are only supported in v16+
  if (getDraftMajorVersion(*version_) >= 16 &&
      (flags == kSerializationFlagEndOfNonExistentRange ||
       flags == kSerializationFlagEndOfUnknownRange)) {
    // End of Range: parse only Group ID and Object ID
    auto groupId = quic::follyutils::decodeQuicInteger(cursor, remainingLength);
    if (!groupId) {
      XLOG(DBG4) << "parseFetchObjectDraft15: UNDERFLOW on End of Range group";
      return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
    }
    remainingLength -= groupId->second;

    auto objectId =
        quic::follyutils::decodeQuicInteger(cursor, remainingLength);
    if (!objectId) {
      XLOG(DBG4) << "parseFetchObjectDraft15: UNDERFLOW on End of Range object";
      return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
    }
    remainingLength -= objectId->second;

    length = remainingLength;
    return EndOfRangeMarker{
        groupId->first,
        objectId->first,
        flags == kSerializationFlagEndOfUnknownRange};
  }

  // Check reserved bits - version dependent
  bool forwardingPreferenceIsDatagram = false;
  if (getDraftMajorVersion(*version_) >= 16) {
    // Draft-16+: Only 0x80 is reserved, 0x40 indicates datagram
    if (flags &
        folly::to_underlying(FetchHeaderSerializationBits::RESERVED_BITMASK)) {
      XLOG(ERR) << "parseFetchObjectDraft15: Reserved bit 0x80 set in flags: 0x"
                << std::hex << static_cast<int>(flags);
      return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
    }
    // Check if this object has datagram forwarding preference
    forwardingPreferenceIsDatagram =
        (flags &
         folly::to_underlying(
             FetchHeaderSerializationBits::DATAGRAM_BITMASK)) != 0;
  } else {
    // Draft-15: Both 0x40 and 0x80 are reserved
    if (flags &
        folly::to_underlying(
            FetchHeaderSerializationBits::LEGACY_RESERVED_BITMASK)) {
      XLOG(ERR) << "parseFetchObjectDraft15: Reserved bits set in flags: 0x"
                << std::hex << static_cast<int>(flags);
      return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
    }
  }

  // flags < 128: interpret as bit flags for normal object
  uint8_t bitFlags = static_cast<uint8_t>(flags);

  // Decode Group ID (flags & 0x08)
  if (bitFlags &
      folly::to_underlying(FetchHeaderSerializationBits::GROUP_ID_BITMASK)) {
    // Group ID field is present
    auto group = quic::follyutils::decodeQuicInteger(cursor, remainingLength);
    if (!group) {
      XLOG(DBG4) << "parseFetchObjectDraft15: UNDERFLOW on group";
      return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
    }
    remainingLength -= group->second;
    objectHeader.group = group->first;
  } else {
    // Group ID is same as previous
    if (!previousFetchGroup_.has_value()) {
      XLOG(ERR) << "parseFetchObjectDraft15: First object must have explicit "
                   "group ID";
      return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
    }
    objectHeader.group = previousFetchGroup_.value();
  }

  // Decode Subgroup ID (flags & 0x03)
  // Ignore if datagram
  if (forwardingPreferenceIsDatagram) {
    // Datagram forwarding preference: ignore subgroup mode bits, use 0
    objectHeader.subgroup = 0;
  } else {
    uint8_t subgroupMode =
        bitFlags &
        folly::to_underlying(
            FetchHeaderSerializationBits::SUBGROUP_MODE_BITMASK);
    switch (subgroupMode) {
      case folly::to_underlying(FetchHeaderSerializationBits::SUBGROUP_ID_ZERO):
        // Subgroup ID is zero
        objectHeader.subgroup = 0;
        break;
      case folly::to_underlying(
          FetchHeaderSerializationBits::SUBGROUP_ID_SAME_AS_PRIOR):
        // Subgroup ID is the prior Object's Subgroup ID
        if (!previousFetchSubgroup_.has_value()) {
          XLOG(ERR) << "parseFetchObjectDraft15: First object cannot reference "
                       "prior subgroup";
          return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
        }
        objectHeader.subgroup = previousFetchSubgroup_.value();
        break;
      case folly::to_underlying(
          FetchHeaderSerializationBits::SUBGROUP_ID_INC_BY_ONE):
        // Subgroup ID is the prior Object's Subgroup ID plus one
        if (!previousFetchSubgroup_.has_value()) {
          XLOG(ERR) << "parseFetchObjectDraft15: First object cannot reference "
                       "prior subgroup";
          return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
        }
        objectHeader.subgroup = previousFetchSubgroup_.value() + 1;
        break;
      case folly::to_underlying(
          FetchHeaderSerializationBits::SUBGROUP_MODE_BITMASK):
        // Subgroup ID field is present
        auto subgroup =
            quic::follyutils::decodeQuicInteger(cursor, remainingLength);
        if (!subgroup) {
          XLOG(DBG4) << "parseFetchObjectDraft15: UNDERFLOW on subgroup";
          return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
        }
        remainingLength -= subgroup->second;
        objectHeader.subgroup = subgroup->first;
        break;
    }
  }

  // Decode Object ID (flags & 0x04)
  if (bitFlags &
      folly::to_underlying(FetchHeaderSerializationBits::OBJECT_ID_BITMASK)) {
    // Object ID field is present
    auto id = quic::follyutils::decodeQuicInteger(cursor, remainingLength);
    if (!id) {
      XLOG(DBG4) << "parseFetchObjectDraft15: UNDERFLOW on id";
      return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
    }
    remainingLength -= id->second;
    objectHeader.id = id->first;
  } else {
    // Object ID is the prior Object's ID plus one
    if (!previousObjectID_.has_value()) {
      XLOG(ERR) << "parseFetchObjectDraft15: First object must have explicit "
                   "object ID";
      return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
    }
    objectHeader.id = previousObjectID_.value() + 1;
  }

  // Decode Priority (flags & 0x10)
  if (bitFlags &
      folly::to_underlying(FetchHeaderSerializationBits::PRIORITY_BITMASK)) {
    // Priority field is present
    if (remainingLength < 1) {
      XLOG(DBG4) << "parseFetchObjectDraft15: UNDERFLOW on priority";
      return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
    }
    objectHeader.priority = cursor.readBE<uint8_t>();
    remainingLength--;
  } else {
    // Priority is the prior Object's Priority
    if (!previousFetchPriority_.has_value()) {
      XLOG(ERR) << "parseFetchObjectDraft15: First object must have explicit "
                   "priority";
      return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
    }
    objectHeader.priority = previousFetchPriority_.value();
  }

  // Decode Extensions (flags & 0x20)
  if (bitFlags &
      folly::to_underlying(FetchHeaderSerializationBits::EXTENSIONS_BITMASK)) {
    // Extensions field is present
    auto ext = parseExtensions(cursor, remainingLength, objectHeader);
    if (!ext) {
      XLOG(DBG4) << "parseFetchObjectDraft15: error in parseExtensions: "
                 << folly::to_underlying(ext.error());
      return folly::makeUnexpected(ext.error());
    }
  }
  // If flag not set, no extensions (extensions remain empty)

  // Parse Object Status and Length
  auto res = parseObjectStatusAndLength(cursor, remainingLength, objectHeader);
  if (!res) {
    XLOG(DBG4)
        << "parseFetchObjectDraft15: error in parseObjectStatusAndLength: "
        << folly::to_underlying(res.error());
    return folly::makeUnexpected(res.error());
  }
  if (!isValidStatusForExtensions(objectHeader)) {
    return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
  }

  // Update context for next object
  previousFetchGroup_ = objectHeader.group;
  previousFetchSubgroup_ = objectHeader.subgroup;
  previousObjectID_ = objectHeader.id;
  previousFetchPriority_ = objectHeader.priority;

  // Set the forwarding preference in the object header
  objectHeader.forwardingPreferenceIsDatagram = forwardingPreferenceIsDatagram;

  length = remainingLength;

  return objectHeader;
}

folly::Expected<folly::Unit, ErrorCode>
MoQFrameParser::parseObjectStatusAndLength(
    folly::io::Cursor& cursor,
    size_t& length,
    ObjectHeader& objectHeader) const noexcept {
  auto payloadLength = quic::follyutils::decodeQuicInteger(cursor, length);
  if (!payloadLength) {
    XLOG(DBG4) << "parseObjectStatusAndLength: UNDERFLOW on payloadLength";
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= payloadLength->second;
  objectHeader.length = payloadLength->first;

  if (objectHeader.length == 0) {
    auto objectStatus = quic::follyutils::decodeQuicInteger(cursor, length);
    if (!objectStatus) {
      XLOG(DBG4) << "parseObjectStatusAndLength: UNDERFLOW on objectStatus";
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

bool MoQFrameParser::isValidStatusForExtensions(
    const ObjectHeader& objectHeader) const noexcept {
  if (version_.has_value() && *version_ >= kVersionDraft15 &&
      objectHeader.status != ObjectStatus::NORMAL &&
      !objectHeader.extensions.empty()) {
    XLOG(ERR) << "Extensions present on non-NORMAL status object: status="
              << folly::to_underlying(objectHeader.status);
    return false;
  }
  return true;
}

folly::Expected<
    MoQFrameParser::ParseResultAndLength<
        MoQFrameParser::FetchObjectParseResult>,
    ErrorCode>
MoQFrameParser::parseFetchObjectHeader(
    folly::io::Cursor& cursor,
    size_t length,
    const ObjectHeader& headerTemplate) const noexcept {
  auto startLength = length;

  if (getDraftMajorVersion(*version_) >= 15) {
    auto draft15Result =
        parseFetchObjectDraft15(cursor, length, headerTemplate);
    if (!draft15Result) {
      return folly::makeUnexpected(draft15Result.error());
    }

    auto v15Consumed = startLength - length;

    return ParseResultAndLength<FetchObjectParseResult>{
        std::move(draft15Result.value()), v15Consumed};
  } else {
    auto objectHeader =
        parseFetchObjectHeaderLegacy(cursor, length, headerTemplate);
    if (!objectHeader) {
      return folly::makeUnexpected(objectHeader.error());
    }

    auto legacyConsumed = startLength - length;

    // Legacy path always returns ObjectHeader, wrap in variant
    return ParseResultAndLength<FetchObjectParseResult>{
        FetchObjectParseResult{std::move(objectHeader.value())},
        legacyConsumed};
  }
}

folly::Expected<MoQFrameParser::ParseResultAndLength<ObjectHeader>, ErrorCode>
MoQFrameParser::parseSubgroupObjectHeader(
    folly::io::Cursor& cursor,
    size_t length,
    const ObjectHeader& headerTemplate,
    const SubgroupOptions& options) const noexcept {
  auto startLength = length;
  ObjectHeader objectHeader = headerTemplate;
  auto id = quic::follyutils::decodeQuicInteger(cursor, length);
  if (!id) {
    XLOG(DBG4) << "parseSubgroupObjectHeader: UNDERFLOW on id";
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= id->second;
  objectHeader.id = id->first;
  XCHECK(version_.has_value())
      << "The version must be set before parsing subgroup object header";
  if (getDraftMajorVersion(*version_) >= 14) {
    // Delta encoded object ID
    uint64_t objectIDDelta = id->first;
    if (previousObjectID_.has_value()) {
      auto decoded = decodeDelta(previousObjectID_.value(), objectIDDelta + 1);
      if (decoded.hasError()) {
        return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
      }
      objectHeader.id = decoded.value();
    } else {
      objectHeader.id = objectIDDelta;
    }
  }

  if (options.hasExtensions) {
    auto ext = parseExtensions(cursor, length, objectHeader);
    if (!ext) {
      XLOG(DBG4) << "parseSubgroupObjectHeader: error in parseExtensions: "
                 << folly::to_underlying(ext.error());
      return folly::makeUnexpected(ext.error());
    }
  }

  auto res = parseObjectStatusAndLength(cursor, length, objectHeader);
  if (!res) {
    XLOG(DBG4)
        << "parseSubgroupObjectHeader: error in parseObjectStatusAndLength: "
        << folly::to_underlying(res.error());
    return folly::makeUnexpected(res.error());
  }
  if (!isValidStatusForExtensions(objectHeader)) {
    return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
  }
  if (getDraftMajorVersion(*version_) >= 14) {
    previousObjectID_ = objectHeader.id;
  }
  return ParseResultAndLength<ObjectHeader>{objectHeader, startLength - length};
}

folly::Expected<folly::Unit, ErrorCode> MoQFrameParser::parseTrackRequestParams(
    folly::io::Cursor& cursor,
    size_t& length,
    size_t numParams,
    TrackRequestParameters& params,
    std::vector<Parameter>& requestSpecificParams) const noexcept {
  CHECK(version_.has_value())
      << "The version must be set before parsing track request params";
  return parseParams(
      cursor,
      length,
      *version_,
      numParams,
      params,
      requestSpecificParams,
      *tokenCache_,
      ParamsType::Request);
}

std::optional<SubscriptionFilter> MoQFrameParser::extractSubscriptionFilter(
    const std::vector<Parameter>& requestSpecificParams) const noexcept {
  for (const auto& param : requestSpecificParams) {
    if (param.key ==
        folly::to_underlying(TrackRequestParamKey::SUBSCRIPTION_FILTER)) {
      return param.asSubscriptionFilter;
    }
  }
  return std::nullopt;
}

folly::Expected<SubscribeRequest, ErrorCode>
MoQFrameParser::parseSubscribeRequest(folly::io::Cursor& cursor, size_t length)
    const noexcept {
  CHECK(version_.has_value())
      << "The version must be set before parsing a subscribe request";
  SubscribeRequest subscribeRequest;
  auto requestID = quic::follyutils::decodeQuicInteger(cursor, length);
  if (!requestID) {
    XLOG(DBG4) << "parseSubscribeRequest: UNDERFLOW on requestID";
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= requestID->second;
  subscribeRequest.requestID = requestID->first;
  auto res = parseFullTrackName(cursor, length);
  if (!res) {
    XLOG(DBG4) << "parseSubscribeRequest: Failed to parse track name";
    return folly::makeUnexpected(res.error());
  }
  subscribeRequest.fullTrackName = std::move(res.value());

  if (getDraftMajorVersion(*version_) < 15) {
    if (length < 1) {
      XLOG(DBG4) << "parseSubscribeRequest: UNDERFLOW on priority";
      return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
    }
    subscribeRequest.priority = cursor.readBE<uint8_t>();
    length -= 1;
  } else {
    // For draft >= 15, set default priority to 128
    // It will be overridden in handleRequestSpecificParams if present
    subscribeRequest.priority = kDefaultPriority;
  }

  if (getDraftMajorVersion(*version_) < 15) {
    if (length < 1) {
      XLOG(DBG4) << "parseSubscribeRequest: UNDERFLOW on order";
      return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
    }

    auto order = cursor.readBE<uint8_t>();
    if (order > folly::to_underlying(GroupOrder::NewestFirst)) {
      XLOG(ERR) << "order > NewestFirst =" << order;
      return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
    }
    subscribeRequest.groupOrder = static_cast<GroupOrder>(order);
    length -= 1;

    if (length < 1) {
      XLOG(DBG4) << "parseSubscribeRequest: UNDERFLOW on forwardFlag";
      return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
    }
    uint8_t forwardFlag = cursor.readBE<uint8_t>();
    if (forwardFlag > 1) {
      XLOG(ERR) << "parseSubscribeRequest: Invalid forward";
      return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
    }
    subscribeRequest.forward = (forwardFlag == 1);
    length--;
  } else {
    // For draft >= 15, set default forward to true
    // It will be overridden in handleRequestSpecificParams if present
    subscribeRequest.forward = true;
  }

  if (getDraftMajorVersion(*version_) < 15) {
    auto locType = quic::follyutils::decodeQuicInteger(cursor, length);
    if (!locType) {
      XLOG(DBG4) << "parseSubscribeRequest: UNDERFLOW on locType";
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
        XLOG(ERR) << "parseSubscribeRequest: error in parseAbsoluteLocation: "
                  << folly::to_underlying(location.error());
        return folly::makeUnexpected(location.error());
      }
      subscribeRequest.start = *location;
    }
    if (subscribeRequest.locType == LocationType::AbsoluteRange) {
      auto endGroup = quic::follyutils::decodeQuicInteger(cursor, length);
      if (!endGroup) {
        XLOG(DBG4) << "parseSubscribeRequest: UNDERFLOW on endGroup";
        return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
      }
      subscribeRequest.endGroup = endGroup->first;
      length -= endGroup->second;
    }
  }
  auto numParams = quic::follyutils::decodeQuicInteger(cursor, length);
  if (!numParams) {
    XLOG(DBG4) << "parseSubscribeRequest: UNDERFLOW on numParams";
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= numParams->second;
  std::vector<Parameter> requestSpecificParams;
  auto res2 = parseTrackRequestParams(
      cursor,
      length,
      numParams->first,
      subscribeRequest.params,
      requestSpecificParams);
  if (!res2) {
    XLOG(ERR) << "parseSubscribeRequest: error in parseTrackRequestParams: "
              << folly::to_underlying(res2.error());
    return folly::makeUnexpected(res2.error());
  }
  handleRequestSpecificParams(subscribeRequest, requestSpecificParams);
  if (length > 0) {
    XLOG(ERR) << "parseSubscribeRequest: leftover bytes after parsing: "
              << length;
    return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
  }
  return subscribeRequest;
}

void MoQFrameParser::handleRequestSpecificParams(
    SubscribeRequest& subscribeRequest,
    const std::vector<Parameter>& requestSpecificParams) const noexcept {
  if (getDraftMajorVersion(*version_) >= 15) {
    // SUBSCRIPTION_FILTER
    auto filter = extractSubscriptionFilter(requestSpecificParams);
    if (filter.has_value()) {
      subscribeRequest.locType = filter->filterType;
      subscribeRequest.start = filter->location;
      if (filter->endGroup.has_value()) {
        subscribeRequest.endGroup = filter->endGroup.value();
      }
    } else {
      // Set defaults indicating an unfiltered subscribe
      subscribeRequest.locType = LocationType::AbsoluteStart;
      subscribeRequest.start = AbsoluteLocation{0, 0};
      subscribeRequest.endGroup = 0; // ignored for AbsoluteStart
    }

    // GROUP_ORDER
    handleGroupOrderParam(
        subscribeRequest.groupOrder,
        requestSpecificParams,
        GroupOrder::Default);

    // SUBSCRIBER_PRIORITY
    handleSubscriberPriorityParam(
        subscribeRequest.priority, requestSpecificParams);

    // FORWARD
    handleForwardParam(subscribeRequest.forward, requestSpecificParams);
  }
}

folly::Expected<RequestUpdate, ErrorCode> MoQFrameParser::parseRequestUpdate(
    folly::io::Cursor& cursor,
    size_t length) const noexcept {
  CHECK(version_.has_value())
      << "The version must be set before parsing a request update";

  RequestUpdate requestUpdate;
  auto requestID = quic::follyutils::decodeQuicInteger(cursor, length);
  if (!requestID) {
    XLOG(DBG4) << "parseRequestUpdate: UNDERFLOW on requestID";
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  requestUpdate.requestID = requestID->first;
  length -= requestID->second;

  if (getDraftMajorVersion(*version_) >= 14) {
    auto existingRequestID =
        quic::follyutils::decodeQuicInteger(cursor, length);
    if (!existingRequestID) {
      XLOG(DBG4) << "parseRequestUpdate: UNDERFLOW on existingRequestID";
      return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
    }
    requestUpdate.existingRequestID = existingRequestID->first;
    length -= existingRequestID->second;
  }

  if (getDraftMajorVersion(*version_) < 15) {
    auto start = parseAbsoluteLocation(cursor, length);
    if (!start) {
      return folly::makeUnexpected(start.error());
    }
    requestUpdate.start = start.value();

    auto endGroup = quic::follyutils::decodeQuicInteger(cursor, length);
    if (!endGroup) {
      XLOG(DBG4) << "parseRequestUpdate: UNDERFLOW on endGroup";
      return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
    }
    requestUpdate.endGroup = endGroup->first;
    length -= endGroup->second;
  }

  if (getDraftMajorVersion(*version_) < 15) {
    if (length < 1) {
      XLOG(DBG4) << "parseRequestUpdate: UNDERFLOW on priority";
      return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
    }
    requestUpdate.priority = cursor.readBE<uint8_t>();
    length--;

    if (length < 1) {
      XLOG(DBG4) << "parseRequestUpdate: UNDERFLOW on forwardFlag";
      return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
    }
    uint8_t forwardFlag = cursor.readBE<uint8_t>();
    if (forwardFlag > 1) {
      return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
    }
    requestUpdate.forward = (forwardFlag == 1);
    length--;
  } else {
    // For draft >= 15, set default priority to 128
    // It will be overridden in handleRequestSpecificParams if present
    requestUpdate.priority = kDefaultPriority;
    // For draft >= 15, forward field is left unset (std::nullopt) by default
    // It will be set in handleRequestSpecificParams only if FORWARD param
    // present This allows existing forward state to be preserved when param is
    // absent
  }

  auto numParams = quic::follyutils::decodeQuicInteger(cursor, length);
  if (!numParams) {
    XLOG(DBG4) << "parseRequestUpdate: UNDERFLOW on numParams";
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= numParams->second;

  std::vector<Parameter> requestSpecificParams;
  auto res2 = parseTrackRequestParams(
      cursor,
      length,
      numParams->first,
      requestUpdate.params,
      requestSpecificParams);
  if (!res2) {
    return folly::makeUnexpected(res2.error());
  }
  handleRequestSpecificParams(requestUpdate, requestSpecificParams);
  if (length > 0) {
    return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
  }
  return requestUpdate;
}

void MoQFrameParser::handleRequestSpecificParams(
    RequestUpdate& requestUpdate,
    const std::vector<Parameter>& requestSpecificParams) const noexcept {
  if (getDraftMajorVersion(*version_) >= 15) {
    auto filter = extractSubscriptionFilter(requestSpecificParams);
    if (filter.has_value()) {
      if (filter->location.has_value()) {
        requestUpdate.start = filter->location.value();
      }
      if (filter->endGroup.has_value()) {
        requestUpdate.endGroup = filter->endGroup.value() + 1;
      } else if (filter->filterType == LocationType::AbsoluteStart) {
        requestUpdate.endGroup = 0;
      }
    }

    // SUBSCRIBER_PRIORITY
    handleSubscriberPriorityParam(
        requestUpdate.priority, requestSpecificParams);

    // FORWARD
    handleForwardParam(requestUpdate.forward, requestSpecificParams);
  }
}

folly::Expected<SubscribeOk, ErrorCode> MoQFrameParser::parseSubscribeOk(
    folly::io::Cursor& cursor,
    size_t length) const noexcept {
  SubscribeOk subscribeOk;
  auto requestID = quic::follyutils::decodeQuicInteger(cursor, length);
  if (!requestID) {
    XLOG(DBG4) << "parseSubscribeOk: UNDERFLOW on requestID";
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= requestID->second;
  subscribeOk.requestID = requestID->first;
  auto trackAlias = quic::follyutils::decodeQuicInteger(cursor, length);
  if (!trackAlias) {
    XLOG(DBG4) << "parseSubscribeOk: UNDERFLOW on trackAlias";
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= trackAlias->second;
  subscribeOk.trackAlias = trackAlias->first;

  // For < v15: parse expires and groupOrder from fixed fields
  if (getDraftMajorVersion(*version_) < 15) {
    auto expires = quic::follyutils::decodeQuicInteger(cursor, length);
    if (!expires) {
      XLOG(DBG4) << "parseSubscribeOk: UNDERFLOW on expires";
      return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
    }
    length -= expires->second;
    subscribeOk.expires = std::chrono::milliseconds(expires->first);

    if (length < 1) {
      XLOG(DBG4) << "parseSubscribeOk: UNDERFLOW on order";
      return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
    }

    auto order = cursor.readBE<uint8_t>();
    if (order == 0 || order > folly::to_underlying(GroupOrder::NewestFirst)) {
      XLOG(ERR) << "order > NewestFirst or order==0 =" << order;
      return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
    }
    length -= sizeof(uint8_t);
    subscribeOk.groupOrder = static_cast<GroupOrder>(order);
  }

  if (getDraftMajorVersion(*version_) < 16) {
    if (length < 1) {
      XLOG(DBG4) << "parseSubscribeOk: UNDERFLOW on contentExists";
      return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
    }
    auto contentExists = cursor.readBE<uint8_t>();
    length -= sizeof(uint8_t);
    if (contentExists) {
      auto res = parseAbsoluteLocation(cursor, length);
      if (!res) {
        return folly::makeUnexpected(res.error());
      }
      subscribeOk.largest = *res;
    }
  }

  auto numParams = quic::follyutils::decodeQuicInteger(cursor, length);
  if (!numParams) {
    XLOG(DBG4) << "parseSubscribeOk: UNDERFLOW on numParams";
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= numParams->second;
  std::vector<Parameter> requestSpecificParams;
  auto res2 = parseTrackRequestParams(
      cursor,
      length,
      numParams->first,
      subscribeOk.params,
      requestSpecificParams);
  if (!res2) {
    XLOG(DBG4) << "parseSubscribeOk: parseTrackRequestParams failed";
    return folly::makeUnexpected(res2.error());
  }

  if (getDraftMajorVersion(*version_) >= 15) {
    // Set defaults for v15+ when parameters are absent
    subscribeOk.expires = std::chrono::milliseconds(0);
    subscribeOk.groupOrder = GroupOrder::OldestFirst;
    // Override from parameters if present
    handleRequestSpecificParams(subscribeOk, requestSpecificParams);
  }

  // Draft 16+: Parse extensions (bare key-value pairs, no length prefix)
  if (getDraftMajorVersion(*version_) >= 16) {
    ObjectHeader tempHeader;
    auto ext = parseExtensionKvPairs(cursor, tempHeader, length, true);
    if (!ext) {
      XLOG(DBG4) << "parseSubscribeOk: error in parseExtensions: "
                 << folly::to_underlying(ext.error());
      return folly::makeUnexpected(ext.error());
    }
    length = 0;
    subscribeOk.extensions = std::move(tempHeader.extensions);
  }

  if (length > 0) {
    XLOG(DBG4) << "parseSubscribeOk: excess length";
    return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
  }

  // For < v16: convert track property params to extensions for uniform access
  if (getDraftMajorVersion(*version_) < 16) {
    convertTrackPropertyParamsToExtensions(
        subscribeOk.params, subscribeOk.extensions);
  }

  return subscribeOk;
}

void MoQFrameParser::handleRequestSpecificParams(
    SubscribeOk& subscribeOk,
    const std::vector<Parameter>& requestSpecificParams) const noexcept {
  // Process request-specific parameters
  for (const auto& param : requestSpecificParams) {
    switch (static_cast<TrackRequestParamKey>(param.key)) {
      case TrackRequestParamKey::EXPIRES:
        subscribeOk.expires = std::chrono::milliseconds(param.asUint64);
        break;
      case TrackRequestParamKey::GROUP_ORDER:
        subscribeOk.groupOrder = static_cast<GroupOrder>(param.asUint64);
        break;
      default:
        // Ignore unknown request-specific parameters
        break;
    }
  }
}

folly::Expected<Unsubscribe, ErrorCode> MoQFrameParser::parseUnsubscribe(
    folly::io::Cursor& cursor,
    size_t length) const noexcept {
  Unsubscribe unsubscribe;
  auto requestID = quic::follyutils::decodeQuicInteger(cursor, length);
  if (!requestID) {
    XLOG(DBG4) << "parseUnsubscribe: UNDERFLOW on requestID";
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= requestID->second;
  unsubscribe.requestID = requestID->first;
  if (length > 0) {
    return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
  }
  return unsubscribe;
}

folly::Expected<PublishDone, ErrorCode> MoQFrameParser::parsePublishDone(
    folly::io::Cursor& cursor,
    size_t length) const noexcept {
  PublishDone publishDone;
  auto requestID = quic::follyutils::decodeQuicInteger(cursor, length);
  if (!requestID) {
    XLOG(DBG4) << "parsePublishDone: UNDERFLOW on requestID";
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= requestID->second;
  publishDone.requestID = requestID->first;

  auto statusCode = quic::follyutils::decodeQuicInteger(cursor, length);
  if (!statusCode) {
    XLOG(DBG4) << "parsePublishDone: UNDERFLOW on statusCode";
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= statusCode->second;
  publishDone.statusCode = PublishDoneStatusCode(statusCode->first);

  auto streamCount = quic::follyutils::decodeQuicInteger(cursor, length);
  if (!streamCount) {
    XLOG(DBG4) << "parsePublishDone: UNDERFLOW on streamCount";
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= streamCount->second;
  publishDone.streamCount = streamCount->first;

  auto reas = parseFixedString(cursor, length);
  if (!reas) {
    return folly::makeUnexpected(reas.error());
  }
  publishDone.reasonPhrase = std::move(reas.value());

  CHECK(version_.has_value())
      << "The version must be set before parsing PUBLISH_DONE";
  if (getDraftMajorVersion(*version_) <= 9) {
    if (length == 0) {
      XLOG(DBG4) << "parsePublishDone: UNDERFLOW on contentExists";
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
  return publishDone;
}

folly::Expected<PublishRequest, ErrorCode> MoQFrameParser::parsePublish(
    folly::io::Cursor& cursor,
    size_t length) const noexcept {
  CHECK(version_.has_value())
      << "The version must be set before parsing a publish request";
  PublishRequest publish;
  auto requestID = quic::follyutils::decodeQuicInteger(cursor, length);
  if (!requestID) {
    XLOG(DBG4) << "parsePublish: UNDERFLOW on requestID";
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
    XLOG(DBG4) << "parsePublish: UNDERFLOW on trackAlias";
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= trackAlias->second;
  publish.trackAlias = trackAlias->first;

  if (getDraftMajorVersion(*version_) < 15) {
    if (length < 1) {
      XLOG(DBG4) << "parsePublish: UNDERFLOW on order";
      return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
    }

    auto order = cursor.readBE<uint8_t>();
    if (order > folly::to_underlying(GroupOrder::NewestFirst)) {
      XLOG(ERR) << "order > NewestFirst =" << order;
      return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
    }
    publish.groupOrder = static_cast<GroupOrder>(order);
    length--;
  }

  if (getDraftMajorVersion(*version_) < 15) {
    if (length < 1) {
      XLOG(DBG4) << "parsePublish: UNDERFLOW on contentExists";
      return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
    }
    uint8_t contentExists = cursor.readBE<uint8_t>();
    if (contentExists > 1) {
      return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
    }
    length--;

    if (contentExists == 1) {
      auto location = parseAbsoluteLocation(cursor, length);
      if (!location) {
        return folly::makeUnexpected(location.error());
      }
      publish.largest = *location;
    } else {
      publish.largest = std::nullopt;
    }
  }

  if (getDraftMajorVersion(*version_) < 15) {
    if (length < 1) {
      XLOG(DBG4) << "parsePublish: UNDERFLOW on forward";
      return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
    }

    uint8_t forwardFlag = cursor.readBE<uint8_t>();
    if (forwardFlag > 1) {
      return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
    }
    publish.forward = (forwardFlag == 1);
    length--;
  } else {
    // For draft >= 15, set default forward to true
    // It will be overridden in handleRequestSpecificParams if present
    publish.forward = true;
  }

  auto numParams = quic::follyutils::decodeQuicInteger(cursor, length);
  if (!numParams) {
    XLOG(DBG4) << "parsePublish: UNDERFLOW on numParams";
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= numParams->second;
  std::vector<Parameter> requestSpecificParams;
  auto paramRes = parseTrackRequestParams(
      cursor, length, numParams->first, publish.params, requestSpecificParams);
  if (!paramRes) {
    return folly::makeUnexpected(paramRes.error());
  }

  if (getDraftMajorVersion(*version_) >= 15) {
    // From the spec: If omitted from PUBLISH, the receiver uses Ascending
    // (0x1). So, we set the groupOrder to be OldestFirst (aka Ascending), and
    // this might be overridden in handleRequestSpecificParams.
    publish.groupOrder = GroupOrder::OldestFirst;
    handleRequestSpecificParams(publish, requestSpecificParams);
  }

  // Draft 16+: Parse extensions (bare key-value pairs, no length prefix)
  if (getDraftMajorVersion(*version_) >= 16) {
    ObjectHeader tempHeader;
    auto ext = parseExtensionKvPairs(cursor, tempHeader, length, true);
    if (!ext) {
      XLOG(DBG4) << "parsePublish: error in parseExtensions: "
                 << folly::to_underlying(ext.error());
      return folly::makeUnexpected(ext.error());
    }
    length = 0;
    publish.extensions = std::move(tempHeader.extensions);
  }

  if (length > 0) {
    return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
  }

  // For < v16: convert track property params to extensions for uniform access
  if (getDraftMajorVersion(*version_) < 16) {
    convertTrackPropertyParamsToExtensions(publish.params, publish.extensions);
  }

  return publish;
}

void MoQFrameParser::handleRequestSpecificParams(
    PublishRequest& publishRequest,
    const std::vector<Parameter>& requestSpecificParams) const noexcept {
  // GROUP_ORDER
  handleGroupOrderParam(
      publishRequest.groupOrder,
      requestSpecificParams,
      GroupOrder::OldestFirst);

  // FORWARD
  handleForwardParam(publishRequest.forward, requestSpecificParams);

  // LARGEST_OBJECT
  for (const auto& param : requestSpecificParams) {
    if (param.key ==
        folly::to_underlying(TrackRequestParamKey::LARGEST_OBJECT)) {
      publishRequest.largest = param.largestObject;
      break;
    }
  }
}

folly::Expected<PublishOk, ErrorCode> MoQFrameParser::parsePublishOk(
    folly::io::Cursor& cursor,
    size_t length) const noexcept {
  CHECK(version_.has_value())
      << "The version must be set before parsing a publish ok";
  PublishOk publishOk;
  auto requestID = quic::follyutils::decodeQuicInteger(cursor, length);
  if (!requestID) {
    XLOG(DBG4) << "parsePublishOk: UNDERFLOW on requestID";
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= requestID->second;
  publishOk.requestID = requestID->first;

  if (getDraftMajorVersion(*version_) < 15) {
    if (length < 1) {
      XLOG(DBG4) << "parsePublishOk: UNDERFLOW on forward/priority/order";
      return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
    }
    uint8_t forwardFlag = cursor.readBE<uint8_t>();
    length--;
    if (forwardFlag > 1) {
      return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
    }
    publishOk.forward = (forwardFlag == 1);

    if (length < 1) {
      return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
    }
    publishOk.subscriberPriority = cursor.readBE<uint8_t>();
    length--;
  } else {
    // For draft >= 15, set default forward to true
    // It will be overridden in handleRequestSpecificParams if present
    publishOk.forward = true;
    // For draft >= 15, set default priority to 128
    // It will be overridden in handleRequestSpecificParams if present
    publishOk.subscriberPriority = kDefaultPriority;
  }

  if (getDraftMajorVersion(*version_) < 15) {
    if (length < 1) {
      XLOG(DBG4) << "parsePublishOk: UNDERFLOW on order";
      return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
    }
    auto order = cursor.readBE<uint8_t>();
    length--;
    if (order > folly::to_underlying(GroupOrder::NewestFirst)) {
      XLOG(ERR) << "order > NewestFirst =" << order;
      return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
    }
    publishOk.groupOrder = static_cast<GroupOrder>(order);
  }

  if (getDraftMajorVersion(*version_) < 15) {
    auto locType = quic::follyutils::decodeQuicInteger(cursor, length);
    if (!locType) {
      XLOG(DBG4) << "parsePublishOk: UNDERFLOW on locType";
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
        XLOG(DBG4) << "parsePublishOk: UNDERFLOW on endGroup";
        return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
      }
      publishOk.endGroup = endGroup->first;
      length -= endGroup->second;
    } else {
      publishOk.endGroup = std::nullopt;
    }
  }

  auto numParams = quic::follyutils::decodeQuicInteger(cursor, length);
  if (!numParams) {
    XLOG(DBG4) << "parsePublishOk: UNDERFLOW on numParams";
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= numParams->second;
  std::vector<Parameter> requestSpecificParams;
  auto paramRes = parseTrackRequestParams(
      cursor,
      length,
      numParams->first,
      publishOk.params,
      requestSpecificParams);
  if (!paramRes) {
    return folly::makeUnexpected(paramRes.error());
  }
  handleRequestSpecificParams(publishOk, requestSpecificParams);
  if (length > 0) {
    return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
  }
  return publishOk;
}

void MoQFrameParser::handleRequestSpecificParams(
    PublishOk& publishOk,
    const std::vector<Parameter>& requestSpecificParams) const noexcept {
  if (getDraftMajorVersion(*version_) >= 15) {
    // SUBSCRIPTION_FILTER
    auto filter = extractSubscriptionFilter(requestSpecificParams);
    if (filter.has_value()) {
      publishOk.locType = filter->filterType;
      publishOk.start = filter->location;
      if (filter->endGroup.has_value()) {
        publishOk.endGroup = filter->endGroup.value();
      } else {
        publishOk.endGroup = std::nullopt;
      }
    } else {
      // Set defaults
      publishOk.locType = LocationType::AbsoluteStart;
      publishOk.start = AbsoluteLocation{0, 0};
      publishOk.endGroup = std::nullopt;
    }

    // GROUP_ORDER
    handleGroupOrderParam(
        publishOk.groupOrder, requestSpecificParams, GroupOrder::Default);

    // SUBSCRIBER_PRIORITY
    handleSubscriberPriorityParam(
        publishOk.subscriberPriority, requestSpecificParams);

    // FORWARD
    handleForwardParam(publishOk.forward, requestSpecificParams);
  }
}

void MoQFrameParser::handleGroupOrderParam(
    GroupOrder& groupOrderField,
    const std::vector<Parameter>& requestSpecificParams,
    GroupOrder defaultGroupOrder) const noexcept {
  auto maybeGroupOrder = getFirstIntParam(
      requestSpecificParams, TrackRequestParamKey::GROUP_ORDER);
  if (maybeGroupOrder.has_value()) {
    groupOrderField = (GroupOrder)*maybeGroupOrder;
  } else {
    groupOrderField = defaultGroupOrder;
  }
}

void MoQFrameParser::handleSubscriberPriorityParam(
    uint8_t& priorityField,
    const std::vector<Parameter>& requestSpecificParams) const noexcept {
  auto maybePriority = getFirstIntParam(
      requestSpecificParams, TrackRequestParamKey::SUBSCRIBER_PRIORITY);
  if (maybePriority.has_value()) {
    priorityField = (uint8_t)*maybePriority;
  }
}

void MoQFrameParser::handleForwardParam(
    bool& forwardField,
    const std::vector<Parameter>& requestSpecificParams) const noexcept {
  auto maybeForward =
      getFirstIntParam(requestSpecificParams, TrackRequestParamKey::FORWARD);
  if (maybeForward.has_value()) {
    forwardField = (*maybeForward == 1);
  }
}

// Overload for Optional<bool> - used by SubscribeUpdate to allow
// preserving existing forward state when parameter is absent
void MoQFrameParser::handleForwardParam(
    std::optional<bool>& forwardField,
    const std::vector<Parameter>& requestSpecificParams) const noexcept {
  auto maybeForward =
      getFirstIntParam(requestSpecificParams, TrackRequestParamKey::FORWARD);
  if (maybeForward.has_value()) {
    forwardField = (*maybeForward == 1);
  }
}

folly::Expected<PublishNamespace, ErrorCode>
MoQFrameParser::parsePublishNamespace(folly::io::Cursor& cursor, size_t length)
    const noexcept {
  PublishNamespace publishNamespace;
  auto requestID = quic::follyutils::decodeQuicInteger(cursor, length);
  if (!requestID) {
    XLOG(DBG4) << "parsePublishNamespace: UNDERFLOW on requestID";
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= requestID->second;
  publishNamespace.requestID = requestID->first;

  auto res = parseFixedTuple(cursor, length);
  if (!res) {
    return folly::makeUnexpected(res.error());
  }
  publishNamespace.trackNamespace = TrackNamespace(std::move(res.value()));
  auto numParams = quic::follyutils::decodeQuicInteger(cursor, length);
  if (!numParams) {
    XLOG(DBG4) << "parsePublishNamespace: UNDERFLOW on numParams";
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= numParams->second;
  std::vector<Parameter> requestSpecificParams;
  auto res2 = parseTrackRequestParams(
      cursor,
      length,
      numParams->first,
      publishNamespace.params,
      requestSpecificParams);
  if (!res2) {
    return folly::makeUnexpected(res2.error());
  }
  if (length > 0) {
    return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
  }
  return publishNamespace;
}

folly::Expected<PublishNamespaceOk, ErrorCode>
MoQFrameParser::parsePublishNamespaceOk(
    folly::io::Cursor& cursor,
    size_t length) const noexcept {
  return parseRequestOk(cursor, length, FrameType::PUBLISH_NAMESPACE_OK);
}

folly::Expected<PublishNamespaceOk, ErrorCode> MoQFrameParser::parseRequestOk(
    folly::io::Cursor& cursor,
    size_t length,
    FrameType frameType) const noexcept {
  RequestOk requestOk;
  auto requestID = quic::follyutils::decodeQuicInteger(cursor, length);
  if (!requestID) {
    XLOG(DBG4) << "parseRequestOk: UNDERFLOW on requestID";
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= requestID->second;
  requestOk.requestID = requestID->first;
  if (getDraftMajorVersion(*version_) > 14) {
    // Parse track request params into requestOk.params
    auto numParams = quic::follyutils::decodeQuicInteger(cursor, length);
    if (!numParams) {
      XLOG(DBG4) << "parseRequestOk: UNDERFLOW on numParams";
      return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
    }
    length -= numParams->second;
    if (numParams->first > 0) {
      if (frameType == FrameType::SUBSCRIBE_NAMESPACE_OK) {
        // no params supported
        return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
      }
      auto res = parseTrackRequestParams(
          cursor,
          length,
          numParams->first,
          requestOk.params,
          requestOk.requestSpecificParams);
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

folly::Expected<PublishNamespaceDone, ErrorCode>
MoQFrameParser::parsePublishNamespaceDone(
    folly::io::Cursor& cursor,
    size_t length) const noexcept {
  PublishNamespaceDone publishNamespaceDone;

  if (getDraftMajorVersion(*version_) >= 16) {
    // v16+: Parse Request ID
    auto requestID = quic::follyutils::decodeQuicInteger(cursor, length);
    if (!requestID) {
      XLOG(DBG4) << "parsePublishNamespaceDone: UNDERFLOW on requestID";
      return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
    }
    length -= requestID->second;
    publishNamespaceDone.requestID = RequestID(requestID->first);
  } else {
    // v15 and below: Parse TrackNamespace
    auto res = parseFixedTuple(cursor, length);
    if (!res) {
      return folly::makeUnexpected(res.error());
    }
    publishNamespaceDone.trackNamespace =
        TrackNamespace(std::move(res.value()));
  }

  if (length > 0) {
    return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
  }
  return publishNamespaceDone;
}

folly::Expected<PublishNamespaceCancel, ErrorCode>
MoQFrameParser::parsePublishNamespaceCancel(
    folly::io::Cursor& cursor,
    size_t length) const noexcept {
  PublishNamespaceCancel publishNamespaceCancel;

  if (getDraftMajorVersion(*version_) >= 16) {
    // v16+: Parse Request ID
    auto requestID = quic::follyutils::decodeQuicInteger(cursor, length);
    if (!requestID) {
      XLOG(DBG4) << "parsePublishNamespaceCancel: UNDERFLOW on requestID";
      return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
    }
    length -= requestID->second;
    publishNamespaceCancel.requestID = RequestID(requestID->first);
  } else {
    // v15 and below: Parse TrackNamespace
    auto res = parseFixedTuple(cursor, length);
    if (!res) {
      return folly::makeUnexpected(res.error());
    }
    publishNamespaceCancel.trackNamespace =
        TrackNamespace(std::move(res.value()));
  }

  auto errorCode = quic::follyutils::decodeQuicInteger(cursor, length);
  if (!errorCode) {
    XLOG(DBG4) << "parsePublishNamespaceCancel: UNDERFLOW on errorCode";
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  publishNamespaceCancel.errorCode =
      PublishNamespaceErrorCode(errorCode->first);
  length -= errorCode->second;

  auto res2 = parseFixedString(cursor, length);
  if (!res2) {
    return folly::makeUnexpected(res2.error());
  }
  publishNamespaceCancel.reasonPhrase = std::move(res2.value());
  if (length > 0) {
    return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
  }
  return publishNamespaceCancel;
}

folly::Expected<TrackStatus, ErrorCode> MoQFrameParser::parseTrackStatus(
    folly::io::Cursor& cursor,
    size_t length) const noexcept {
  CHECK(version_.has_value())
      << "version_ needs to be set to parse TrackStatus";

  if (getDraftMajorVersion(*version_) >= 14) {
    return parseSubscribeRequest(cursor, length);
  }
  TrackStatus trackStatus;

  // Fill in defaults for new fields added in v14
  trackStatus.priority = kDefaultPriority;
  trackStatus.groupOrder = GroupOrder::Default;
  trackStatus.forward = true;
  trackStatus.locType = LocationType::LargestGroup;
  trackStatus.start = std::nullopt;
  trackStatus.endGroup = 0;

  auto requestID = quic::follyutils::decodeQuicInteger(cursor, length);
  if (!requestID) {
    XLOG(DBG4) << "parseTrackStatus: UNDERFLOW on requestID";
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= requestID->second;
  trackStatus.requestID = requestID->first;
  auto res = parseFullTrackName(cursor, length);
  if (!res) {
    return folly::makeUnexpected(res.error());
  }
  trackStatus.fullTrackName = std::move(res.value());

  auto numParams = quic::follyutils::decodeQuicInteger(cursor, length);
  if (!numParams) {
    XLOG(DBG4) << "parseTrackStatus: UNDERFLOW on numParams";
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= numParams->second;
  std::vector<Parameter> requestSpecificParams;
  auto parseParamsResult = parseTrackRequestParams(
      cursor,
      length,
      numParams->first,
      trackStatus.params,
      requestSpecificParams);
  if (!parseParamsResult) {
    return folly::makeUnexpected(parseParamsResult.error());
  }
  if (length > 0) {
    return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
  }
  return trackStatus;
}

folly::Expected<TrackStatusOk, ErrorCode> MoQFrameParser::parseTrackStatusOk(
    folly::io::Cursor& cursor,
    size_t length) const noexcept {
  CHECK(version_.has_value())
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
  auto requestID = quic::follyutils::decodeQuicInteger(cursor, length);
  if (!requestID) {
    XLOG(DBG4) << "parseTrackStatusOk: UNDERFLOW on requestID";
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= requestID->second;
  trackStatusOk.requestID = requestID->first;
  auto statusCode = quic::follyutils::decodeQuicInteger(cursor, length);
  if (!statusCode) {
    XLOG(DBG4) << "parseTrackStatusOk: UNDERFLOW on statusCode";
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

  auto numParams = quic::follyutils::decodeQuicInteger(cursor, length);
  if (!numParams) {
    XLOG(DBG4) << "parseTrackStatusOk: UNDERFLOW on numParams";
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= numParams->second;
  std::vector<Parameter> requestSpecificParams;
  auto parseParamsResult = parseTrackRequestParams(
      cursor,
      length,
      numParams->first,
      trackStatusOk.params,
      requestSpecificParams);
  if (!parseParamsResult) {
    return folly::makeUnexpected(parseParamsResult.error());
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
    XLOG(DBG4) << "parseMaxRequestID: UNDERFLOW on requestID";
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
    XLOG(DBG4) << "parseRequestsBlocked: UNDERFLOW on maxRequestID";
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
    XLOG(DBG4) << "parseFetch: UNDERFLOW on requestID";
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  fetch.requestID = res->first;
  length -= res->second;

  if (getDraftMajorVersion(*version_) < 15) {
    if (length < 1) {
      XLOG(DBG4) << "parseFetch: UNDERFLOW on priority";
      return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
    }
    fetch.priority = cursor.readBE<uint8_t>();
    length -= sizeof(uint8_t);

    if (length < 1) {
      XLOG(DBG4) << "parseFetch: UNDERFLOW on order";
      return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
    }

    auto order = cursor.readBE<uint8_t>();
    if (order > folly::to_underlying(GroupOrder::NewestFirst)) {
      XLOG(ERR) << "order > NewestFirst =" << order;
      return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
    }
    fetch.groupOrder = static_cast<GroupOrder>(order);
    length -= sizeof(uint8_t);
  } else {
    // For draft >= 15 these will be overridden by handleRequestSpecificParams.
    // We set the defaults as appropriate so that we conform to what the spec
    // says these values should be when the fields are omitted.
    fetch.priority = kDefaultPriority;
    fetch.groupOrder = GroupOrder::OldestFirst;
  }

  if (length < 1) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }

  auto fetchType = quic::follyutils::decodeQuicInteger(cursor, length);
  if (!fetchType) {
    XLOG(DBG4) << "parseFetch: UNDERFLOW on fetchType";
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
      XLOG(DBG4) << "parseFetch: UNDERFLOW on jsid";
      return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
    }
    length -= jsid->second;

    auto joiningStart = quic::follyutils::decodeQuicInteger(cursor, length);
    if (!joiningStart) {
      XLOG(DBG4) << "parseFetch: UNDERFLOW on joiningStart";
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
    XLOG(DBG4) << "parseFetch: UNDERFLOW on numParams";
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= numParams->second;
  std::vector<Parameter> requestSpecificParams;
  auto res5 = parseTrackRequestParams(
      cursor, length, numParams->first, fetch.params, requestSpecificParams);
  if (!res5) {
    return folly::makeUnexpected(res5.error());
  }
  if (length > 0) {
    return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
  }

  if (getDraftMajorVersion(*version_) >= 15) {
    // From the spec: If omitted from FETCH, the receiver uses Ascending
    // (0x1). So, we set the groupOrder to be OldestFirst (aka Ascending), and
    // this might be overridden in handleRequestSpecificParams.
    fetch.groupOrder = GroupOrder::OldestFirst;
    handleRequestSpecificParams(fetch, requestSpecificParams);
  }

  return fetch;
}

void MoQFrameParser::handleRequestSpecificParams(
    Fetch& fetchRequest,
    const std::vector<Parameter>& requestSpecificParams) const noexcept {
  // GROUP_ORDER
  handleGroupOrderParam(
      fetchRequest.groupOrder, requestSpecificParams, GroupOrder::OldestFirst);
}

folly::Expected<FetchCancel, ErrorCode> MoQFrameParser::parseFetchCancel(
    folly::io::Cursor& cursor,
    size_t length) const noexcept {
  FetchCancel fetchCancel;
  auto res = quic::follyutils::decodeQuicInteger(cursor, length);
  if (!res) {
    XLOG(DBG4) << "parseFetchCancel: UNDERFLOW on requestID";
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
    XLOG(DBG4) << "parseFetchOk: UNDERFLOW on requestID";
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  fetchOk.requestID = res->first;
  length -= res->second;

  // Check for next two bytes
  if (length < 2) {
    XLOG(DBG4) << "parseFetchOk: UNDERFLOW on order/endOfTrack";
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
    XLOG(DBG4) << "parseFetchOk: UNDERFLOW on numParams";
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= numParams->second;
  std::vector<Parameter> requestSpecificParams;
  auto res3 = parseTrackRequestParams(
      cursor, length, numParams->first, fetchOk.params, requestSpecificParams);
  if (!res3) {
    return folly::makeUnexpected(res3.error());
  }

  // Draft 16+: Parse extensions (bare key-value pairs, no length prefix)
  if (getDraftMajorVersion(*version_) >= 16) {
    ObjectHeader tempHeader;
    auto ext = parseExtensionKvPairs(cursor, tempHeader, length, true);
    if (!ext) {
      XLOG(DBG4) << "parseFetchOk: error in parseExtensions: "
                 << folly::to_underlying(ext.error());
      return folly::makeUnexpected(ext.error());
    }
    length = 0;
    fetchOk.extensions = std::move(tempHeader.extensions);
  }

  if (length > 0) {
    return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
  }

  // For < v16: convert track property params to extensions for uniform access
  if (getDraftMajorVersion(*version_) < 16) {
    convertTrackPropertyParamsToExtensions(fetchOk.params, fetchOk.extensions);
  }

  return fetchOk;
}

folly::Expected<SubscribeNamespace, ErrorCode>
MoQFrameParser::parseSubscribeNamespace(
    folly::io::Cursor& cursor,
    size_t length) const noexcept {
  SubscribeNamespace subscribeNamespace;

  // Parse Request ID
  auto requestID = quic::follyutils::decodeQuicInteger(cursor, length);
  if (!requestID) {
    XLOG(DBG4) << "parseSubscribeNamespace: UNDERFLOW on requestID";
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= requestID->second;
  subscribeNamespace.requestID = requestID->first;

  // Parse Track Namespace Prefix
  auto res = parseFixedTuple(cursor, length);
  if (!res) {
    return folly::makeUnexpected(res.error());
  }
  subscribeNamespace.trackNamespacePrefix =
      TrackNamespace(std::move(res.value()));

  // Draft 16+: Parse Subscribe Options field
  if (getDraftMajorVersion(*version_) >= 16) {
    auto options = quic::follyutils::decodeQuicInteger(cursor, length);
    if (!options) {
      XLOG(DBG4) << "parseSubscribeNamespace: UNDERFLOW on options";
      return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
    }
    length -= options->second;
    subscribeNamespace.options =
        static_cast<SubscribeNamespaceOptions>(options->first);
  } else {
    subscribeNamespace.options = SubscribeNamespaceOptions::BOTH;
  }

  // Parse Parameters
  auto numParams = quic::follyutils::decodeQuicInteger(cursor, length);
  if (!numParams) {
    XLOG(DBG4) << "parseSubscribeNamespace: UNDERFLOW on numParams";
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= numParams->second;
  std::vector<Parameter> requestSpecificParams;
  auto res2 = parseTrackRequestParams(
      cursor,
      length,
      numParams->first,
      subscribeNamespace.params,
      requestSpecificParams);
  if (!res2) {
    return folly::makeUnexpected(res2.error());
  }
  if (length > 0) {
    return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
  }

  // Draft 15+: Extract forward field from request-specific parameters
  // Default is true; only set to false if FORWARD param is present with value 0
  if (getDraftMajorVersion(*version_) >= 15) {
    handleForwardParam(subscribeNamespace.forward, requestSpecificParams);
  }

  return subscribeNamespace;
}

folly::Expected<SubscribeNamespaceOk, ErrorCode>
MoQFrameParser::parseSubscribeNamespaceOk(
    folly::io::Cursor& cursor,
    size_t length) const noexcept {
  return parseRequestOk(cursor, length, FrameType::SUBSCRIBE_NAMESPACE_OK);
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
      frameType == FrameType::PUBLISH_NAMESPACE_ERROR ||
      frameType == FrameType::SUBSCRIBE_NAMESPACE_ERROR ||
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
    XLOG(DBG4) << "parseRequestError: UNDERFLOW on requestID";
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= requestID->second;
  requestError.requestID = requestID->first;

  // Parse errorCode
  auto errorCode = quic::follyutils::decodeQuicInteger(cursor, length);
  if (!errorCode) {
    XLOG(DBG4) << "parseRequestError: UNDERFLOW on errorCode";
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= errorCode->second;
  requestError.errorCode = RequestErrorCode(errorCode->first);

  // Parse retryInterval (version 16+)
  if (getDraftMajorVersion(*version_) >= 16) {
    auto retryInterval = quic::follyutils::decodeQuicInteger(cursor, length);
    if (!retryInterval) {
      XLOG(DBG4) << "parseRequestError: UNDERFLOW on retryInterval";
      return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
    }
    length -= retryInterval->second;
    requestError.retryInterval =
        std::chrono::milliseconds(retryInterval->first);
  }

  // Parse reasonPhrase
  auto reasonPhrase = parseFixedString(cursor, length);
  if (!reasonPhrase) {
    return folly::makeUnexpected(reasonPhrase.error());
  }
  requestError.reasonPhrase = std::move(reasonPhrase.value());

  // Check for leftover bytes
  if (length > 0) {
    return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
  }

  return requestError;
}

folly::Expected<UnsubscribeNamespace, ErrorCode>
MoQFrameParser::parseUnsubscribeNamespace(
    folly::io::Cursor& cursor,
    size_t length) const noexcept {
  UnsubscribeNamespace unsubscribeNamespace;

  // v15+: Parse Request ID
  if (getDraftMajorVersion(*version_) >= 15) {
    auto requestID = quic::follyutils::decodeQuicInteger(cursor, length);
    if (!requestID) {
      return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
    }
    length -= requestID->second;
    unsubscribeNamespace.requestID = RequestID(requestID->first);
  } else {
    // <v15: Parse Track Namespace Prefix
    auto res = parseFixedTuple(cursor, length);
    if (!res) {
      return folly::makeUnexpected(res.error());
    }
    unsubscribeNamespace.trackNamespacePrefix =
        TrackNamespace(std::move(res.value()));
  }

  if (length > 0) {
    return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
  }
  return unsubscribeNamespace;
}

folly::Expected<Namespace, ErrorCode> MoQFrameParser::parseNamespace(
    folly::io::Cursor& cursor,
    size_t length) const noexcept {
  CHECK(version_) << "Need to have version_ set in order to parse NAMESPACE";
  CHECK_GE(getDraftMajorVersion(*version_), 16)
      << "NAMESPACE message doesn't exist for version 15 and below, this function "
      << "shouldn't be called";
  Namespace ns;

  // Parse Track Namespace Suffix
  auto res = parseFixedTuple(cursor, length);
  if (!res) {
    XLOG(DBG4) << "parseNamespace: error parsing track namespace suffix";
    return folly::makeUnexpected(res.error());
  }
  ns.trackNamespaceSuffix = TrackNamespace(std::move(res.value()));

  if (length > 0) {
    return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
  }
  return ns;
}

folly::Expected<NamespaceDone, ErrorCode> MoQFrameParser::parseNamespaceDone(
    folly::io::Cursor& cursor,
    size_t length) const noexcept {
  CHECK(version_)
      << "Need to have version_ set in order to parse NAMESPACE_DONE";
  CHECK_GE(getDraftMajorVersion(*version_), 16)
      << "NAMESPACE_DONE message doesn't exist for version 15 and below, this function "
      << "shouldn't be called";
  NamespaceDone namespaceDone;

  // Parse Track Namespace Suffix
  auto res = parseFixedTuple(cursor, length);
  if (!res) {
    XLOG(DBG4) << "parseNamespaceDone: error parsing track namespace suffix";
    return folly::makeUnexpected(res.error());
  }
  namespaceDone.trackNamespaceSuffix = TrackNamespace(std::move(res.value()));

  if (length > 0) {
    return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
  }
  return namespaceDone;
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

folly::Expected<folly::Unit, ErrorCode> MoQFrameParser::parseExtensions(
    folly::io::Cursor& cursor,
    size_t& length,
    ObjectHeader& objectHeader) const noexcept {
  CHECK(version_.has_value())
      << "The version must be set before parsing extensions";

  // Parse the length of the extension block
  auto extLen = quic::follyutils::decodeQuicInteger(cursor, length);
  if (!extLen) {
    XLOG(DBG4) << "parseExtensions: UNDERFLOW on extLen";
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= extLen->second;
  if (extLen->first > length) {
    XLOG(DBG4) << "Extension block length provided exceeds remaining length";
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  // Parse the extensions
  size_t extensionBlockLength = extLen->first;
  auto parseExtensionKvPairsResult =
      parseExtensionKvPairs(cursor, objectHeader, extensionBlockLength, true);
  if (!parseExtensionKvPairsResult.hasValue()) {
    XLOG(DBG4) << "parseExtensions: error in parseExtensionKvPairs: "
               << folly::to_underlying(parseExtensionKvPairsResult.error())
               << " group=" << objectHeader.group
               << " subgroup=" << objectHeader.subgroup
               << " id=" << objectHeader.id;
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
  // Reset previous extension type for delta decoding
  if (getDraftMajorVersion(*version_) >= 16) {
    previousExtensionType_ = 0;
  }

  while (extensionBlockLength > 0) {
    // This won't infinite loop because we're parsing out at least a
    // QuicInteger each time.

    auto parseExtensionResult = parseExtension(
        cursor, extensionBlockLength, objectHeader, allowImmutable);
    if (parseExtensionResult.hasError()) {
      XLOG(DBG4) << "parseExtensionKvPairs: error in parseExtension: "
                 << folly::to_underlying(parseExtensionResult.error());
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
    XLOG(DBG4) << "parseExtension: UNDERFLOW on type";
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= type->second;

  // Delta decode type for v16+
  uint64_t actualType = type->first;
  if (getDraftMajorVersion(*version_) >= 16) {
    auto decoded = decodeDelta(previousExtensionType_, type->first);
    if (decoded.hasError()) {
      return folly::makeUnexpected(decoded.error());
    }
    actualType = decoded.value();
    previousExtensionType_ = actualType;
  }

  // We can't have an immutable extension nested within another
  // immutable extension.
  if (!allowImmutable && getDraftMajorVersion(*version_) >= 14 &&
      actualType == kImmutableExtensionType) {
    XLOG(ERR) << "Immutable extension encountered when not allowed";
    return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
  }

  Extension ext;
  ext.type = actualType;

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
      XLOG(DBG4) << "parseExtension: UNDERFLOW on extLen, ext.type=" << ext.type
                 << " length=" << length;
      return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
    }
    length -= extLen->second;
    if (length < extLen->first) {
      XLOG(DBG4) << "parseExtension: UNDERFLOW on ext array value"
                 << " ext.type=" << ext.type << " length=" << length
                 << " extLen=" << extLen->first;
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
        XLOG(DBG4)
            << "parseExtension: error in parseExtensionKvPairs (immutable): "
            << folly::to_underlying(parseInnerResult.error())
            << " ext.type=" << ext.type << " length=" << length
            << " extLen=" << extLen->first;
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
      XLOG(DBG4) << "parseExtension: UNDERFLOW on intValue"
                 << " ext.type=" << ext.type << " length=" << length;
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
    XLOG(DBG4) << "parseFixedTuple: UNDERFLOW on itemCount";
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

//// Transforms /////
TrackStatusOk RequestOk::toTrackStatusOk() const {
  TrackStatusOk trackStatusOk;
  trackStatusOk.requestID = requestID;
  trackStatusOk.params = params;

  // There may or may not be any value in attempting to convert the full object
  // since we only need the request Id to resolve the promise, except for
  // logging. We still do the best we can here to move all fields

  // In v15+, extra fields (expires, groupOrder, largest) are encoded as params
  // Track Alias is NOT USED per spec
  trackStatusOk.trackAlias = TrackAlias{0};

  // Go through request specific params and assign the fields
  for (const auto& param : requestSpecificParams) {
    switch (static_cast<TrackRequestParamKey>(param.key)) {
      case TrackRequestParamKey::EXPIRES:
        trackStatusOk.expires = std::chrono::milliseconds(param.asUint64);
        break;
      case TrackRequestParamKey::GROUP_ORDER:
        trackStatusOk.groupOrder = static_cast<GroupOrder>(param.asUint64);
        break;
      case TrackRequestParamKey::LARGEST_OBJECT:
        trackStatusOk.largest = param.largestObject;
        break;
      default:
        break;
    }
  }
  return trackStatusOk;
}

// static
RequestOk RequestOk::fromTrackStatusOk(const TrackStatusOk& trackStatusOk) {
  RequestOk requestOk;
  requestOk.requestID = trackStatusOk.requestID;
  requestOk.params = trackStatusOk.params;

  // Add expires parameter
  requestOk.requestSpecificParams.emplace_back(
      folly::to_underlying(TrackRequestParamKey::EXPIRES),
      static_cast<uint64_t>(trackStatusOk.expires.count()));

  // Add group order parameter
  requestOk.requestSpecificParams.emplace_back(
      folly::to_underlying(TrackRequestParamKey::GROUP_ORDER),
      folly::to_underlying(trackStatusOk.groupOrder));

  // Add the LARGEST_OBJECT param if present
  if (trackStatusOk.largest) {
    requestOk.requestSpecificParams.emplace_back(
        folly::to_underlying(TrackRequestParamKey::LARGEST_OBJECT),
        trackStatusOk.largest.value());
  }
  return requestOk;
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
  writeVarint(
      writeBuf, folly::to_underlying(AliasType::DELETE_ALIAS), size, error);
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
    const std::optional<uint64_t>& forceVersion) const {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  size_t size = 0;
  bool error = false;
  writeVarint(
      writeBuf, folly::to_underlying(AliasType::USE_VALUE), size, error);
  writeVarint(writeBuf, tokenType, size, error);
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
    // Check that all versions are supported
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

  // Collect params that should be included
  std::vector<Parameter> filteredParams;
  for (const auto& param : clientSetup.params) {
    if (includeSetupParam(version, SetupKey(param.key))) {
      filteredParams.push_back(param);
    }
  }

  // Sort params by key for delta encoding (v16+)
  if (getDraftMajorVersion(version) >= 16) {
    filteredParams = sortParamsByKey(std::move(filteredParams));
  }

  writeVarint(writeBuf, filteredParams.size(), size, error);

  uint64_t previousKey = 0;
  for (const auto& param : filteredParams) {
    auto keyToWrite = param.key;
    if (getDraftMajorVersion(version) >= 16) {
      keyToWrite = param.key - previousKey;
      previousKey = param.key;
    }
    writeVarint(writeBuf, keyToWrite, size, error);
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

  // Collect params that should be included
  std::vector<Parameter> filteredParams;
  for (const auto& param : serverSetup.params) {
    if (includeSetupParam(serverSetup.selectedVersion, SetupKey(param.key))) {
      filteredParams.push_back(param);
    }
  }

  // Sort params by key for delta encoding (v16+)
  if (getDraftMajorVersion(version) >= 16) {
    filteredParams = sortParamsByKey(std::move(filteredParams));
  }

  writeVarint(writeBuf, filteredParams.size(), size, error);

  uint64_t previousKey = 0;
  for (const auto& param : filteredParams) {
    auto keyToWrite = param.key;
    if (getDraftMajorVersion(version) >= 16) {
      keyToWrite = param.key - previousKey;
      previousKey = param.key;
    }
    writeVarint(writeBuf, keyToWrite, size, error);
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

  bool priorityPresent = objectHeader.priority.has_value();
  if (getDraftMajorVersion(version_.value()) < 15 && !priorityPresent) {
    XLOG(ERR) << "Priority must be set for Draft-14 and earlier versions";
    return folly::makeUnexpected(quic::TransportErrorCode::INTERNAL_ERROR);
  }

  auto streamType = getSubgroupStreamType(
      *version_,
      objectHeader.subgroup == 0 ? SubgroupIDFormat::Zero : format,
      includeExtensions,
      /*endOfGroup=*/false,
      priorityPresent);
  auto streamTypeInt = folly::to_underlying(streamType);
  writeVarint(writeBuf, streamTypeInt, size, error);
  writeVarint(writeBuf, trackAlias.value, size, error);
  writeVarint(writeBuf, objectHeader.group, size, error);
  if (streamTypeInt & SG_HAS_SUBGROUP_ID) {
    writeVarint(writeBuf, objectHeader.subgroup, size, error);
  }
  // Only write priority if present
  if (priorityPresent) {
    uint8_t priority = objectHeader.priority.value_or(kDefaultPriority);
    writeBuf.append(&priority, 1);
    size += 1;
  }
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

  // Reset writer context at the start of each FETCH stream. This shouldn't
  // really be necessary since we create a MoQFrameWriter per-stream, but
  // putting this here for completeness.
  resetWriterFetchContext();

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
  // Sort extensions by type for v16+ delta encoding
  std::vector<Extension> sortedExtensions;
  const std::vector<Extension>* extensionsToWrite = &extensions;
  if (getDraftMajorVersion(*version_) >= 16) {
    sortedExtensions = sortExtensionsByType(extensions);
    extensionsToWrite = &sortedExtensions;
  }

  uint64_t previousType = 0;
  for (const auto& ext : *extensionsToWrite) {
    // Delta encode type for v16+
    uint64_t typeToWrite = ext.type;
    if (getDraftMajorVersion(*version_) >= 16) {
      typeToWrite = ext.type - previousType;
      previousType = ext.type;
    }
    writeVarint(writeBuf, typeToWrite, size, error);
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
    bool& error,
    bool withLengthPrefix) const noexcept {
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
  if (withLengthPrefix) {
    writeVarint(writeBuf, totalExtLen, size, error);
    if (error) {
      return;
    }
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
    std::optional<uint64_t> registerToken) {
  return TrackRequestParameter(
      {folly::to_underlying(TrackRequestParamKey::AUTHORIZATION_TOKEN),
       {tokenType, std::move(token), std::move(registerToken)}});
}

void MoQFrameWriter::writeTrackRequestParams(
    folly::IOBufQueue& writeBuf,
    const TrackRequestParameters& params,
    const std::vector<Parameter>& requestSpecificParams,
    size_t& size,
    bool& error) const noexcept {
  CHECK(*version_) << "Version must be set before writing track request params";
  // Write total count of all parameters
  writeVarint(
      writeBuf, params.size() + requestSpecificParams.size(), size, error);

  if (getDraftMajorVersion(*version_) >= 16) {
    // v16+: Merge, sort, and delta encode
    auto allParams = mergeAndSortParams(requestSpecificParams, params);

    uint64_t previousKey = 0;
    for (const auto& param : allParams) {
      writeVarint(writeBuf, param.key - previousKey, size, error);
      previousKey = param.key;
      writeParamValue(writeBuf, param, size, error);
    }
  } else {
    // v15 and below, no delta encoding
    // Write request-specific params (draft 15 only)
    if (getDraftMajorVersion(*version_) >= 15) {
      for (const auto& param : requestSpecificParams) {
        writeVarint(writeBuf, param.key, size, error);
        writeParamValue(writeBuf, param, size, error);
      }
    }

    // Write regular params
    for (const auto& param : params) {
      writeVarint(writeBuf, param.key, size, error);
      writeParamValue(writeBuf, param, size, error);
    }
  }
}

void MoQFrameWriter::writeParamValue(
    folly::IOBufQueue& writeBuf,
    const Parameter& param,
    size_t& size,
    bool& error) const noexcept {
  const auto subscriptionFilterKey =
      folly::to_underlying(TrackRequestParamKey::SUBSCRIPTION_FILTER);
  const auto largestObjectKey =
      folly::to_underlying(TrackRequestParamKey::LARGEST_OBJECT);
  const auto expiresKey = folly::to_underlying(TrackRequestParamKey::EXPIRES);
  const auto groupOrderKey =
      folly::to_underlying(TrackRequestParamKey::GROUP_ORDER);

  if (param.key == subscriptionFilterKey) {
    // Subscription filter key is odd, so it needs a length prefix.
    // Write to a temporary buffer to compute the length first.
    folly::IOBufQueue tmpBuf{folly::IOBufQueue::cacheChainLength()};
    size_t tmpSize = 0;
    writeSubscriptionFilter(tmpBuf, param.asSubscriptionFilter, tmpSize, error);
    if (!error) {
      writeVarint(writeBuf, tmpSize, size, error);
      writeBuf.append(tmpBuf.move());
      size += tmpSize;
    }
  } else if (param.key == largestObjectKey) {
    // Largest object key is odd, so it needs a length prefix.
    folly::IOBufQueue tmpBuf{folly::IOBufQueue::cacheChainLength()};
    size_t tmpSize = 0;
    writeVarint(tmpBuf, param.largestObject->group, tmpSize, error);
    writeVarint(tmpBuf, param.largestObject->object, tmpSize, error);
    if (!error) {
      writeVarint(writeBuf, tmpSize, size, error);
      writeBuf.append(tmpBuf.move());
      size += tmpSize;
    }
  } else if (param.key == expiresKey || param.key == groupOrderKey) {
    writeVarint(writeBuf, param.asUint64, size, error);
  } else if (
      param.key ==
      folly::to_underlying(TrackRequestParamKey::AUTHORIZATION_TOKEN)) {
    writeFixedString(writeBuf, param.asString, size, error);
  } else if (
      param.key ==
          folly::to_underlying(TrackRequestParamKey::DELIVERY_TIMEOUT) ||
      param.key ==
          folly::to_underlying(TrackRequestParamKey::MAX_CACHE_DURATION)) {
    writeVarint(writeBuf, param.asUint64, size, error);
  } else if ((param.key & 0x01) == 0) {
    writeVarint(writeBuf, param.asUint64, size, error);
  } else {
    writeFixedString(writeBuf, param.asString, size, error);
  }
}

void MoQFrameWriter::writeSubscriptionFilter(
    folly::IOBufQueue& writeBuf,
    const SubscriptionFilter& filter,
    size_t& size,
    bool& error) const noexcept {
  CHECK(version_.has_value())
      << "Version must be set before writing subscription filter";

  // Write filter type
  writeVarint(
      writeBuf,
      getLocationTypeValue(filter.filterType, getDraftMajorVersion(*version_)),
      size,
      error);

  // Write start location for AbsoluteStart and AbsoluteRange
  if (filter.filterType == LocationType::AbsoluteStart ||
      filter.filterType == LocationType::AbsoluteRange) {
    if (filter.location.has_value()) {
      writeVarint(writeBuf, filter.location->group, size, error);
      writeVarint(writeBuf, filter.location->object, size, error);
    } else {
      error = true;
    }
  }

  // Write end group for AbsoluteRange
  if (filter.filterType == LocationType::AbsoluteRange) {
    if (filter.endGroup.has_value()) {
      writeVarint(writeBuf, *filter.endGroup, size, error);
    } else {
      error = true;
    }
  }
}

WriteResult MoQFrameWriter::writeDatagramObject(
    folly::IOBufQueue& writeBuf,
    TrackAlias trackAlias,
    const ObjectHeader& objectHeader,
    std::unique_ptr<folly::IOBuf> objectPayload,
    bool endOfGroup) const noexcept {
  size_t size = 0;
  bool error = false;
  bool hasLength = objectHeader.length && *objectHeader.length > 0;
  bool hasExtensions = objectHeader.extensions.size() > 0;

  if (getDraftMajorVersion(version_.value()) < 15 &&
      !objectHeader.priority.has_value()) {
    XLOG(ERR) << "Priority must be set for Draft-14 and earlier versions";
    return folly::makeUnexpected(quic::TransportErrorCode::INTERNAL_ERROR);
  }
  bool priorityPresent = objectHeader.priority.has_value();

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
            *version_,
            true,
            hasExtensions,
            endOfGroup,
            isObjectIdZero,
            priorityPresent)),
        size,
        error);
    writeVarint(writeBuf, trackAlias.value, size, error);
    writeVarint(writeBuf, objectHeader.group, size, error);

    // Only put non-zero object ID on the wire
    if (!isObjectIdZero) {
      writeVarint(writeBuf, objectHeader.id, size, error);
    }

    if (priorityPresent) {
      uint8_t priority = objectHeader.priority.value_or(kDefaultPriority);
      writeBuf.append(&priority, 1);
      size += 1;
    }
    if (hasExtensions) {
      writeExtensions(writeBuf, objectHeader.extensions, size, error);
    }
    writeVarint(
        writeBuf, folly::to_underlying(objectHeader.status), size, error);
  } else {
    writeVarint(
        writeBuf,
        folly::to_underlying(getDatagramType(
            *version_,
            false,
            hasExtensions,
            endOfGroup,
            isObjectIdZero,
            priorityPresent)),
        size,
        error);
    writeVarint(writeBuf, trackAlias.value, size, error);
    writeVarint(writeBuf, objectHeader.group, size, error);
    if (!isObjectIdZero) {
      writeVarint(writeBuf, objectHeader.id, size, error);
    }
    // Only write priority if present
    if (priorityPresent) {
      uint8_t priority = objectHeader.priority.value_or(kDefaultPriority);
      writeBuf.append(&priority, 1);
      size += 1;
    }
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

void MoQFrameWriter::writeFetchObjectHeaderLegacy(
    folly::IOBufQueue& writeBuf,
    const ObjectHeader& objectHeader,
    size_t& size,
    bool& error) const noexcept {
  // Legacy FETCH object format (draft <= 14): all fields explicit
  writeVarint(writeBuf, objectHeader.group, size, error);
  writeVarint(writeBuf, objectHeader.subgroup, size, error);
  writeVarint(writeBuf, objectHeader.id, size, error);
  writeBuf.append(&objectHeader.priority, 1);
  size += 1;
}

void MoQFrameWriter::writeFetchObjectDraft15(
    folly::IOBufQueue& writeBuf,
    const ObjectHeader& objectHeader,
    size_t& size,
    bool& error,
    bool forwardingPreferenceIsDatagram) const noexcept {
  // Draft-15+ FETCH object format with Serialization Flags
  uint8_t flags = 0;

  if (forwardingPreferenceIsDatagram && getDraftMajorVersion(*version_) >= 16) {
    // Set datagram bit
    flags |=
        folly::to_underlying(FetchHeaderSerializationBits::DATAGRAM_BITMASK);
  } else {
    // Determine Subgroup ID mode (bits 0-1) - only for non-datagram objects
    if (objectHeader.subgroup == 0) {
      // Mode 0x00: Subgroup ID is zero
      flags |=
          folly::to_underlying(FetchHeaderSerializationBits::SUBGROUP_ID_ZERO);
    } else if (
        previousFetchSubgroup_.has_value() &&
        objectHeader.subgroup == previousFetchSubgroup_.value()) {
      // Mode 0x01: Same as prior
      flags |= folly::to_underlying(
          FetchHeaderSerializationBits::SUBGROUP_ID_SAME_AS_PRIOR);
    } else if (
        previousFetchSubgroup_.has_value() &&
        objectHeader.subgroup == previousFetchSubgroup_.value() + 1) {
      // Mode 0x02: Prior + 1
      flags |= folly::to_underlying(
          FetchHeaderSerializationBits::SUBGROUP_ID_INC_BY_ONE);
    } else {
      // Mode 0x03: Explicit field
      flags |= folly::to_underlying(
          FetchHeaderSerializationBits::SUBGROUP_MODE_BITMASK);
    }
  }

  // Bit 2 (0x04): Object ID present
  if (!previousObjectID_.has_value() ||
      objectHeader.id != previousObjectID_.value() + 1) {
    flags |=
        folly::to_underlying(FetchHeaderSerializationBits::OBJECT_ID_BITMASK);
  }

  // Bit 3 (0x08): Group ID present
  if (!previousFetchGroup_.has_value() ||
      objectHeader.group != previousFetchGroup_.value()) {
    flags |=
        folly::to_underlying(FetchHeaderSerializationBits::GROUP_ID_BITMASK);
  }

  // Bit 4 (0x10): Priority present
  if (!previousFetchPriority_.has_value() ||
      objectHeader.priority != previousFetchPriority_.value()) {
    flags |=
        folly::to_underlying(FetchHeaderSerializationBits::PRIORITY_BITMASK);
  }

  // Bit 5 (0x20): Extensions present
  // Note: For FETCH streams, extensions are always written by
  // writeStreamObject(), so we set this flag based on whether extensions exist,
  // but it's informational.
  if (!objectHeader.extensions.empty()) {
    flags |=
        folly::to_underlying(FetchHeaderSerializationBits::EXTENSIONS_BITMASK);
  }

  // Write Serialization Flags - single byte for v15, varint for v16+
  if (getDraftMajorVersion(*version_) >= 16) {
    writeVarint(writeBuf, flags, size, error);
  } else {
    writeBuf.append(&flags, 1);
    size += 1;
  }

  // Write Group ID if flag set
  if (flags &
      folly::to_underlying(FetchHeaderSerializationBits::GROUP_ID_BITMASK)) {
    writeVarint(writeBuf, objectHeader.group, size, error);
  }

  // Write Subgroup ID if mode is 0x03
  if ((flags &
       folly::to_underlying(
           FetchHeaderSerializationBits::SUBGROUP_MODE_BITMASK)) ==
      folly::to_underlying(
          FetchHeaderSerializationBits::SUBGROUP_MODE_BITMASK)) {
    writeVarint(writeBuf, objectHeader.subgroup, size, error);
  }

  // Write Object ID if flag set
  if (flags &
      folly::to_underlying(FetchHeaderSerializationBits::OBJECT_ID_BITMASK)) {
    writeVarint(writeBuf, objectHeader.id, size, error);
  }

  // Write Priority if flag set
  if (flags &
      folly::to_underlying(FetchHeaderSerializationBits::PRIORITY_BITMASK)) {
    writeBuf.append(&objectHeader.priority, 1);
    size += 1;
  }

  // Note: Extensions, status, and length are written by writeStreamObject(),
  // not here. The 0x20 flag tells the parser whether extensions are present,
  // but writeStreamObject() handles the actual writing.

  // Update context for next object
  previousFetchGroup_ = objectHeader.group;
  previousFetchSubgroup_ = objectHeader.subgroup;
  previousObjectID_ = objectHeader.id;
  previousFetchPriority_ = objectHeader.priority;
}

void MoQFrameWriter::resetWriterFetchContext() const noexcept {
  previousFetchGroup_.reset();
  previousFetchSubgroup_.reset();
  previousObjectID_.reset();
  previousFetchPriority_.reset();
}

WriteResult MoQFrameWriter::writeStreamObject(
    folly::IOBufQueue& writeBuf,
    StreamType streamType,
    const ObjectHeader& objectHeader,
    std::unique_ptr<folly::IOBuf> objectPayload,
    bool forwardingPreferenceIsDatagram) const noexcept {
  XCHECK(version_.has_value())
      << "The version must be set before writing stream object";
  size_t size = 0;
  bool error = false;
  if (streamType == StreamType::FETCH_HEADER) {
    // Dispatch to appropriate FETCH object writer based on version
    if (getDraftMajorVersion(*version_) >= 15) {
      writeFetchObjectDraft15(
          writeBuf, objectHeader, size, error, forwardingPreferenceIsDatagram);
    } else {
      writeFetchObjectHeaderLegacy(writeBuf, objectHeader, size, error);
    }
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
  bool shouldWriteExtensions = folly::to_underlying(streamType) & 0x1;
  if (streamType == StreamType::FETCH_HEADER &&
      getDraftMajorVersion(*version_) >= 15) {
    // Draft-15 FETCH streams only carry an extensions section when the
    // serialization flags advertise it. Skip emitting the zero-length
    // placeholder so the parser stays aligned with the flags we set.
    shouldWriteExtensions = !objectHeader.extensions.empty();
  }
  if (shouldWriteExtensions) {
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
  CHECK(version_.has_value())
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
  writeFullTrackName(writeBuf, subscribeRequest.fullTrackName, size, error);

  if (getDraftMajorVersion(*version_) < 15) {
    writeBuf.append(&subscribeRequest.priority, 1);
    size += 1;

    uint8_t order = folly::to_underlying(subscribeRequest.groupOrder);
    writeBuf.append(&order, 1);
    size += 1;

    uint8_t forwardFlag = (subscribeRequest.forward) ? 1 : 0;
    writeBuf.append(&forwardFlag, 1);
    size += 1;
  }

  std::vector<Parameter> requestSpecificParams;
  if (getDraftMajorVersion(*version_) >= 15) {
    Parameter subscriptionFilterParam;
    subscriptionFilterParam.key =
        folly::to_underlying(TrackRequestParamKey::SUBSCRIPTION_FILTER);
    subscriptionFilterParam.asSubscriptionFilter = SubscriptionFilter(
        subscribeRequest.locType,
        subscribeRequest.start,
        subscribeRequest.locType == LocationType::AbsoluteRange
            ? std::optional<uint64_t>(subscribeRequest.endGroup)
            : std::nullopt);
    requestSpecificParams.push_back(subscriptionFilterParam);

    if (subscribeRequest.priority != kDefaultPriority) {
      Parameter priorityParam;
      priorityParam.key =
          folly::to_underlying(TrackRequestParamKey::SUBSCRIBER_PRIORITY);
      priorityParam.asUint64 = subscribeRequest.priority;
      requestSpecificParams.push_back(priorityParam);
    }

    if (subscribeRequest.groupOrder != GroupOrder::Default) {
      Parameter groupOrderParam;
      groupOrderParam.key =
          folly::to_underlying(TrackRequestParamKey::GROUP_ORDER);
      groupOrderParam.asUint64 =
          folly::to_underlying(subscribeRequest.groupOrder);
      requestSpecificParams.push_back(groupOrderParam);
    }

    if (subscribeRequest.forward == 0) {
      // The forward param defaults to 1 if not specified, so we only need
      // to insert the parameter if forward is 0.
      Parameter forwardParam;
      forwardParam.key = folly::to_underlying(TrackRequestParamKey::FORWARD);
      forwardParam.asUint64 = 0;
      requestSpecificParams.push_back(forwardParam);
    }
  } else {
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
  }

  writeTrackRequestParams(
      writeBuf, subscribeRequest.params, requestSpecificParams, size, error);
  return size;
}

WriteResult MoQFrameWriter::writeRequestUpdate(
    folly::IOBufQueue& writeBuf,
    const RequestUpdate& update) const noexcept {
  CHECK(version_.has_value())
      << "Version needs to be set to write request update";
  size_t size = 0;
  bool error = false;
  auto sizePtr = writeFrameHeader(writeBuf, FrameType::SUBSCRIBE_UPDATE, error);
  writeVarint(writeBuf, update.requestID.value, size, error);
  if (getDraftMajorVersion(*version_) >= 14) {
    writeVarint(writeBuf, update.existingRequestID.value, size, error);
  }

  std::vector<Parameter> requestSpecificParams;
  if (getDraftMajorVersion(*version_) >= 15) {
    if (update.start.has_value() || update.endGroup.has_value()) {
      Parameter subscriptionFilterParam;
      subscriptionFilterParam.key =
          folly::to_underlying(TrackRequestParamKey::SUBSCRIPTION_FILTER);
      // Here, we're trying to keep in line with the SubscribeUpdate usage, in
      // that update.endGroup is the end group id + 1. If update.endGroup == 0,
      // that means that the subscription is open ended.
      LocationType locationType =
          (!update.endGroup.has_value() || *update.endGroup == 0)
          ? LocationType::AbsoluteStart
          : LocationType::AbsoluteRange;

      std::optional<uint64_t> endGroup = std::nullopt;
      if (update.endGroup.has_value() && *update.endGroup > 0) {
        endGroup = *update.endGroup - 1;
      }

      subscriptionFilterParam.asSubscriptionFilter =
          SubscriptionFilter(locationType, update.start, endGroup);
      requestSpecificParams.push_back(subscriptionFilterParam);
    }

    if (update.priority != kDefaultPriority) {
      Parameter priorityParam;
      priorityParam.key =
          folly::to_underlying(TrackRequestParamKey::SUBSCRIBER_PRIORITY);
      priorityParam.asUint64 = update.priority;
      requestSpecificParams.push_back(priorityParam);
    }

    // Only add FORWARD parameter if it's explicitly set (has value)
    // When absent, the receiver preserves existing forward state per draft 15+
    if (update.forward.has_value()) {
      Parameter forwardParam;
      forwardParam.key = folly::to_underlying(TrackRequestParamKey::FORWARD);
      forwardParam.asUint64 = *update.forward ? 1 : 0;
      requestSpecificParams.push_back(forwardParam);
    }
  } else {
    // For draft < 15, start and endGroup are mandatory
    XCHECK(update.start.has_value()) << "start is required for draft < 15";
    XCHECK(update.endGroup.has_value())
        << "endGroup is required for draft < 15";

    writeVarint(writeBuf, update.start->group, size, error);
    writeVarint(writeBuf, update.start->object, size, error);
    writeVarint(writeBuf, *update.endGroup, size, error);

    writeBuf.append(&update.priority, 1);
    size += 1;

    // For draft < 15, forward is mandatory and always set during parsing
    uint8_t forwardFlag = update.forward.value_or(true) ? 1 : 0;
    writeBuf.append(&forwardFlag, 1);
    size += 1;
  }
  writeTrackRequestParams(
      writeBuf, update.params, requestSpecificParams, size, error);
  writeSize(sizePtr, size, error, *version_);
  if (error) {
    return folly::makeUnexpected(quic::TransportErrorCode::INTERNAL_ERROR);
  }
  return size;
}

WriteResult MoQFrameWriter::writeSubscribeOk(
    folly::IOBufQueue& writeBuf,
    const SubscribeOk& subscribeOk) const noexcept {
  CHECK(version_.has_value())
      << "Version needs to be set to write subscribe ok";
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
  writeVarint(writeBuf, subscribeOk.trackAlias.value, size, error);

  // For < v15: write expires and groupOrder as fixed fields
  if (getDraftMajorVersion(*version_) < 15) {
    writeVarint(writeBuf, subscribeOk.expires.count(), size, error);
    auto order = folly::to_underlying(subscribeOk.groupOrder);
    writeBuf.append(&order, 1);
    size += 1;
  }

  if (getDraftMajorVersion(*version_) < 16) {
    uint8_t contentExists = (subscribeOk.largest) ? 1 : 0;
    writeBuf.append(&contentExists, 1);
    size += 1;
    if (subscribeOk.largest) {
      writeVarint(writeBuf, subscribeOk.largest->group, size, error);
      writeVarint(writeBuf, subscribeOk.largest->object, size, error);
    }
  }

  // Make a mutable copy of params for potential extension->param conversion
  TrackRequestParameters params = subscribeOk.params;

  // For < v16: convert track property extensions to params
  if (getDraftMajorVersion(*version_) < 16) {
    convertTrackPropertyExtensionsToParams(subscribeOk.extensions, params);
  }

  std::vector<Parameter> requestSpecificParams;
  if (getDraftMajorVersion(*version_) >= 15) {
    // Add EXPIRES parameter (only if non-zero)
    if (subscribeOk.expires.count() != 0) {
      Parameter expiresParam;
      expiresParam.key = folly::to_underlying(TrackRequestParamKey::EXPIRES);
      expiresParam.asUint64 =
          static_cast<uint64_t>(subscribeOk.expires.count());
      requestSpecificParams.push_back(expiresParam);
    }

    // Add GROUP_ORDER parameter (only if non-default)
    if (subscribeOk.groupOrder != GroupOrder::Default) {
      Parameter groupOrderParam;
      groupOrderParam.key =
          folly::to_underlying(TrackRequestParamKey::GROUP_ORDER);
      groupOrderParam.asUint64 = folly::to_underlying(subscribeOk.groupOrder);
      requestSpecificParams.push_back(groupOrderParam);
    }
  }
  writeTrackRequestParams(writeBuf, params, requestSpecificParams, size, error);

  // Draft 16+: Write extensions
  if (getDraftMajorVersion(*version_) >= 16) {
    writeExtensions(
        writeBuf,
        subscribeOk.extensions,
        size,
        error,
        /*withLengthPrefix=*/false);
  }
  return size;
}

WriteResult MoQFrameWriter::writeMaxRequestID(
    folly::IOBufQueue& writeBuf,
    const MaxRequestID& maxRequestID) const noexcept {
  CHECK(version_.has_value())
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
  CHECK(version_.has_value())
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
  CHECK(version_.has_value()) << "Version needs to be set to write unsubscribe";
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

WriteResult MoQFrameWriter::writePublishDone(
    folly::IOBufQueue& writeBuf,
    const PublishDone& publishDone) const noexcept {
  CHECK(version_.has_value())
      << "Version needs to be set to write subscribe done";
  size_t size = 0;
  bool error = false;
  auto sizePtr = writeFrameHeader(writeBuf, FrameType::PUBLISH_DONE, error);
  writeVarint(writeBuf, publishDone.requestID.value, size, error);
  writeVarint(
      writeBuf, folly::to_underlying(publishDone.statusCode), size, error);
  writeVarint(writeBuf, publishDone.streamCount, size, error);
  writeFixedString(writeBuf, publishDone.reasonPhrase, size, error);
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
  CHECK(version_.has_value()) << "Version needs to be set to write publish";
  size_t size = 0;
  bool error = false;
  auto sizePtr = writeFrameHeader(writeBuf, FrameType::PUBLISH, error);
  writeVarint(writeBuf, publish.requestID.value, size, error);

  writeFullTrackName(writeBuf, publish.fullTrackName, size, error);

  writeVarint(writeBuf, publish.trackAlias.value, size, error);

  if (getDraftMajorVersion(*version_) < 15) {
    uint8_t order = folly::to_underlying(publish.groupOrder);
    writeBuf.append(&order, 1);
    size += 1;
  }

  if (getDraftMajorVersion(*version_) < 15) {
    uint8_t contentExists = publish.largest.has_value() ? 1 : 0;
    writeBuf.append(&contentExists, 1);
    size += 1;

    if (publish.largest.has_value()) {
      writeVarint(writeBuf, publish.largest->group, size, error);
      writeVarint(writeBuf, publish.largest->object, size, error);
    }
  }

  std::vector<Parameter> requestSpecificParams;
  if (getDraftMajorVersion(*version_) >= 15) {
    if (publish.groupOrder != GroupOrder::Default) {
      Parameter groupOrderParam;
      groupOrderParam.key =
          folly::to_underlying(TrackRequestParamKey::GROUP_ORDER);
      groupOrderParam.asUint64 = folly::to_underlying(publish.groupOrder);
      requestSpecificParams.push_back(groupOrderParam);
    }

    if (publish.forward == 0) {
      // The forward param defaults to 1 if not specified, so we only need
      // to insert the parameter if forward is 0.
      Parameter forwardParam;
      forwardParam.key = folly::to_underlying(TrackRequestParamKey::FORWARD);
      forwardParam.asUint64 = 0;
      requestSpecificParams.push_back(forwardParam);
    }

    if (publish.largest.has_value()) {
      requestSpecificParams.push_back(Parameter(
          folly::to_underlying(TrackRequestParamKey::LARGEST_OBJECT),
          publish.largest));
    }
  } else {
    uint8_t forwardFlag = publish.forward ? 1 : 0;
    writeBuf.append(&forwardFlag, 1);
    size += 1;
  }

  // Make a mutable copy of params for potential extension->param conversion
  TrackRequestParameters params = publish.params;

  // For < v16: convert track property extensions to params
  if (getDraftMajorVersion(*version_) < 16) {
    convertTrackPropertyExtensionsToParams(publish.extensions, params);
  }

  writeTrackRequestParams(writeBuf, params, requestSpecificParams, size, error);

  // Draft 16+: Write extensions
  if (getDraftMajorVersion(*version_) >= 16) {
    writeExtensions(
        writeBuf, publish.extensions, size, error, /*withLengthPrefix=*/false);
  }
  writeSize(sizePtr, size, error, *version_);
  if (error) {
    return folly::makeUnexpected(quic::TransportErrorCode::INTERNAL_ERROR);
  }
  return size;
}

WriteResult MoQFrameWriter::writePublishOk(
    folly::IOBufQueue& writeBuf,
    const PublishOk& publishOk) const noexcept {
  CHECK(version_.has_value()) << "Version needs to be set to write publish ok";
  size_t size = 0;
  bool error = false;
  auto sizePtr = writeFrameHeader(writeBuf, FrameType::PUBLISH_OK, error);
  writeVarint(writeBuf, publishOk.requestID.value, size, error);

  if (getDraftMajorVersion(*version_) < 15) {
    uint8_t forwardFlag = publishOk.forward ? 1 : 0;
    writeBuf.append(&forwardFlag, 1);
    size += 1;

    writeBuf.append(&publishOk.subscriberPriority, 1);
    size += 1;

    uint8_t order = folly::to_underlying(publishOk.groupOrder);
    writeBuf.append(&order, 1);
    size += 1;
  }

  std::vector<Parameter> requestSpecificParams;
  if (getDraftMajorVersion(*version_) >= 15) {
    Parameter subscriptionFilterParam;
    subscriptionFilterParam.key =
        folly::to_underlying(TrackRequestParamKey::SUBSCRIPTION_FILTER);
    subscriptionFilterParam.asSubscriptionFilter = SubscriptionFilter(
        publishOk.locType,
        publishOk.start,
        publishOk.locType == LocationType::AbsoluteRange ? publishOk.endGroup
                                                         : std::nullopt);
    requestSpecificParams.push_back(subscriptionFilterParam);

    if (publishOk.subscriberPriority != kDefaultPriority) {
      Parameter priorityParam;
      priorityParam.key =
          folly::to_underlying(TrackRequestParamKey::SUBSCRIBER_PRIORITY);
      priorityParam.asUint64 = publishOk.subscriberPriority;
      requestSpecificParams.push_back(priorityParam);
    }

    if (publishOk.groupOrder != GroupOrder::Default) {
      Parameter groupOrderParam;
      groupOrderParam.key =
          folly::to_underlying(TrackRequestParamKey::GROUP_ORDER);
      groupOrderParam.asUint64 = folly::to_underlying(publishOk.groupOrder);
      requestSpecificParams.push_back(groupOrderParam);
    }

    if (publishOk.forward == 0) {
      // The forward param defaults to 1 if not specified, so we only need
      // to insert the parameter if forward is 0.
      Parameter forwardParam;
      forwardParam.key = folly::to_underlying(TrackRequestParamKey::FORWARD);
      forwardParam.asUint64 = 0;
      requestSpecificParams.push_back(forwardParam);
    }
  } else {
    writeVarint(
        writeBuf,
        getLocationTypeValue(
            publishOk.locType, getDraftMajorVersion(*version_)),
        size,
        error);

    switch (publishOk.locType) {
      case LocationType::AbsoluteStart: {
        if (publishOk.start.has_value()) {
          writeVarint(writeBuf, publishOk.start->group, size, error);
          writeVarint(writeBuf, publishOk.start->object, size, error);
        }
        break;
      }
      case LocationType::AbsoluteRange: {
        if (publishOk.start.has_value()) {
          writeVarint(writeBuf, publishOk.start->group, size, error);
          writeVarint(writeBuf, publishOk.start->object, size, error);
        }
        if (publishOk.endGroup.has_value()) {
          writeVarint(writeBuf, publishOk.endGroup.value(), size, error);
        }
        break;
      }
      default: {
        break;
      }
    }
  }

  writeTrackRequestParams(
      writeBuf, publishOk.params, requestSpecificParams, size, error);
  writeSize(sizePtr, size, error, *version_);
  if (error) {
    return folly::makeUnexpected(quic::TransportErrorCode::INTERNAL_ERROR);
  }
  return size;
}

WriteResult MoQFrameWriter::writePublishNamespace(
    folly::IOBufQueue& writeBuf,
    const PublishNamespace& publishNamespace) const noexcept {
  CHECK(version_.has_value())
      << "Version needs to be set to write publishNamespace";
  size_t size = 0;
  bool error = false;
  auto sizePtr =
      writeFrameHeader(writeBuf, FrameType::PUBLISH_NAMESPACE, error);
  writeVarint(writeBuf, publishNamespace.requestID.value, size, error);
  writeTrackNamespace(writeBuf, publishNamespace.trackNamespace, size, error);
  writeTrackRequestParams(writeBuf, publishNamespace.params, {}, size, error);
  writeSize(sizePtr, size, error, *version_);
  if (error) {
    return folly::makeUnexpected(quic::TransportErrorCode::INTERNAL_ERROR);
  }
  return size;
}

WriteResult MoQFrameWriter::writePublishNamespaceOk(
    folly::IOBufQueue& writeBuf,
    const PublishNamespaceOk& publishNamespaceOk) const noexcept {
  return writeRequestOk(
      writeBuf, publishNamespaceOk, FrameType::PUBLISH_NAMESPACE_OK);
}

WriteResult MoQFrameWriter::writeRequestOk(
    folly::IOBufQueue& writeBuf,
    const RequestOk& requestOk,
    FrameType frameType) const noexcept {
  CHECK(version_.has_value()) << "Version needs to be set to write request ok";
  size_t size = 0;
  bool error = false;
  if (getDraftMajorVersion(*version_) > 14) {
    frameType = FrameType::REQUEST_OK;
  }
  auto sizePtr = writeFrameHeader(writeBuf, frameType, error);
  writeVarint(writeBuf, requestOk.requestID.value, size, error);
  if (getDraftMajorVersion(*version_) > 14) {
    if (frameType == FrameType::SUBSCRIBE_NAMESPACE_OK &&
        !requestOk.params.empty()) {
      return folly::makeUnexpected(
          quic::TransportErrorCode::PROTOCOL_VIOLATION);
    }
    writeTrackRequestParams(
        writeBuf,
        requestOk.params,
        requestOk.requestSpecificParams,
        size,
        error);
  }
  writeSize(sizePtr, size, error, *version_);
  if (error) {
    return folly::makeUnexpected(quic::TransportErrorCode::INTERNAL_ERROR);
  }
  return size;
}

WriteResult MoQFrameWriter::writePublishNamespaceDone(
    folly::IOBufQueue& writeBuf,
    const PublishNamespaceDone& publishNamespaceDone) const noexcept {
  CHECK(version_.has_value())
      << "Version needs to be set to write publishNamespaceDone";
  size_t size = 0;
  bool error = false;
  auto sizePtr =
      writeFrameHeader(writeBuf, FrameType::PUBLISH_NAMESPACE_DONE, error);

  if (getDraftMajorVersion(*version_) >= 16) {
    // v16+: Write Request ID
    CHECK(publishNamespaceDone.requestID.has_value())
        << "RequestID required for v16+ PublishNamespaceDone";
    writeVarint(writeBuf, publishNamespaceDone.requestID->value, size, error);
  } else {
    // v15 and below: Write TrackNamespace
    writeTrackNamespace(
        writeBuf, publishNamespaceDone.trackNamespace, size, error);
  }

  writeSize(sizePtr, size, error, *version_);
  if (error) {
    return folly::makeUnexpected(quic::TransportErrorCode::INTERNAL_ERROR);
  }
  return size;
}

WriteResult MoQFrameWriter::writePublishNamespaceCancel(
    folly::IOBufQueue& writeBuf,
    const PublishNamespaceCancel& publishNamespaceCancel) const noexcept {
  CHECK(version_.has_value())
      << "Version needs to be set to write publishNamespace cancel";
  size_t size = 0;
  bool error = false;
  auto sizePtr =
      writeFrameHeader(writeBuf, FrameType::PUBLISH_NAMESPACE_CANCEL, error);

  if (getDraftMajorVersion(*version_) >= 16) {
    // v16+: Write Request ID
    CHECK(publishNamespaceCancel.requestID.has_value())
        << "RequestID required for v16+ PublishNamespaceCancel";
    writeVarint(writeBuf, publishNamespaceCancel.requestID->value, size, error);
  } else {
    // v15 and below: Write TrackNamespace
    writeTrackNamespace(
        writeBuf, publishNamespaceCancel.trackNamespace, size, error);
  }

  writeVarint(
      writeBuf,
      folly::to_underlying(publishNamespaceCancel.errorCode),
      size,
      error);
  writeFixedString(writeBuf, publishNamespaceCancel.reasonPhrase, size, error);
  writeSize(sizePtr, size, error, *version_);
  if (error) {
    return folly::makeUnexpected(quic::TransportErrorCode::INTERNAL_ERROR);
  }
  return size;
}

WriteResult MoQFrameWriter::writeTrackStatus(
    folly::IOBufQueue& writeBuf,
    const TrackStatus& trackStatus) const noexcept {
  CHECK(version_.has_value())
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
    writeVarint(writeBuf, trackStatus.requestID.value, size, error);
    writeFullTrackName(writeBuf, trackStatus.fullTrackName, size, error);
    writeTrackRequestParams(writeBuf, trackStatus.params, {}, size, error);
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
  CHECK(version_.has_value())
      << "version_ needs to be set to write TrackStatus";

  size_t size = 0;
  bool error = false;

  if (getDraftMajorVersion(*version_) >= 15) {
    auto requestOk = RequestOk::fromTrackStatusOk(trackStatusOk);
    return writeRequestOk(writeBuf, requestOk, FrameType::REQUEST_OK);
  }

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
             Extensions{},
             trackStatusOk.params}));
    if (!res) {
      return res;
    }
    size += *res;
  } else {
    writeVarint(writeBuf, trackStatusOk.requestID.value, size, error);
    writeVarint(
        writeBuf, folly::to_underlying(trackStatusOk.statusCode), size, error);
    if (trackStatusOk.statusCode == TrackStatusCode::IN_PROGRESS) {
      writeVarint(writeBuf, trackStatusOk.largest->group, size, error);
      writeVarint(writeBuf, trackStatusOk.largest->object, size, error);
    } else {
      writeVarint(writeBuf, 0, size, error);
      writeVarint(writeBuf, 0, size, error);
    }
    writeTrackRequestParams(writeBuf, trackStatusOk.params, {}, size, error);
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
  CHECK(version_.has_value()) << "Version needs to be set to write Goaway";
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

WriteResult MoQFrameWriter::writeSubscribeNamespace(
    folly::IOBufQueue& writeBuf,
    const SubscribeNamespace& subscribeNamespace) const noexcept {
  CHECK(version_.has_value())
      << "Version needs to be set to write subscribeNamespace";
  size_t size = 0;
  bool error = false;
  auto sizePtr =
      writeFrameHeader(writeBuf, FrameType::SUBSCRIBE_NAMESPACE, error);
  writeVarint(writeBuf, subscribeNamespace.requestID.value, size, error);
  writeTrackNamespace(
      writeBuf, subscribeNamespace.trackNamespacePrefix, size, error);

  // Draft 16+: Write Subscribe Options field
  if (getDraftMajorVersion(*version_) >= 16) {
    writeVarint(
        writeBuf,
        folly::to_underlying(subscribeNamespace.options),
        size,
        error);
  }

  // Draft 15+: Write Forward field as a parameter (only if forward == 0,
  // since 1 is the default)
  std::vector<Parameter> requestSpecificParams;
  if (getDraftMajorVersion(*version_) >= 15 && !subscribeNamespace.forward) {
    Parameter forwardParam;
    forwardParam.key = folly::to_underlying(TrackRequestParamKey::FORWARD);
    forwardParam.asUint64 = 0;
    requestSpecificParams.push_back(forwardParam);
  }

  writeTrackRequestParams(
      writeBuf, subscribeNamespace.params, requestSpecificParams, size, error);
  writeSize(sizePtr, size, error, *version_);
  if (error) {
    return folly::makeUnexpected(quic::TransportErrorCode::INTERNAL_ERROR);
  }
  return size;
}

WriteResult MoQFrameWriter::writeSubscribeNamespaceOk(
    folly::IOBufQueue& writeBuf,
    const SubscribeNamespaceOk& subscribeNamespaceOk) const noexcept {
  return writeRequestOk(
      writeBuf, subscribeNamespaceOk, FrameType::SUBSCRIBE_NAMESPACE_OK);
}

WriteResult MoQFrameWriter::writeUnsubscribeNamespace(
    folly::IOBufQueue& writeBuf,
    const UnsubscribeNamespace& unsubscribeNamespace) const noexcept {
  CHECK(version_.has_value())
      << "Version needs to be set to write unsubscribeNamespace";
  size_t size = 0;
  bool error = false;
  auto sizePtr =
      writeFrameHeader(writeBuf, FrameType::UNSUBSCRIBE_NAMESPACE, error);

  // v15+: Write Request ID
  if (getDraftMajorVersion(*version_) >= 15) {
    writeVarint(
        writeBuf, unsubscribeNamespace.requestID.value().value, size, error);
  } else {
    writeTrackNamespace(
        writeBuf,
        unsubscribeNamespace.trackNamespacePrefix.value(),
        size,
        error);
  }

  writeSize(sizePtr, size, error, *version_);
  if (error) {
    return folly::makeUnexpected(quic::TransportErrorCode::INTERNAL_ERROR);
  }
  return size;
}

WriteResult MoQFrameWriter::writeNamespace(
    folly::IOBufQueue& writeBuf,
    const Namespace& ns) const noexcept {
  CHECK(version_.has_value()) << "Version needs to be set to write namespace";
  CHECK_GE(getDraftMajorVersion(*version_), 16)
      << "NAMESPACE message doesn't exist for version 15 and below, this function "
      << "shouldn't be called";
  size_t size = 0;
  bool error = false;
  auto sizePtr = writeFrameHeader(writeBuf, FrameType::NAMESPACE, error);
  writeTrackNamespace(writeBuf, ns.trackNamespaceSuffix, size, error);
  writeSize(sizePtr, size, error, *version_);
  if (error) {
    return folly::makeUnexpected(quic::TransportErrorCode::INTERNAL_ERROR);
  }
  return size;
}

WriteResult MoQFrameWriter::writeNamespaceDone(
    folly::IOBufQueue& writeBuf,
    const NamespaceDone& namespaceDone) const noexcept {
  CHECK(version_.has_value())
      << "Version needs to be set to write namespace done";
  CHECK_GE(getDraftMajorVersion(*version_), 16)
      << "NAMESPACE_DONE message doesn't exist for version 15 and below, this function "
      << "shouldn't be called";
  size_t size = 0;
  bool error = false;
  auto sizePtr = writeFrameHeader(writeBuf, FrameType::NAMESPACE_DONE, error);
  writeTrackNamespace(
      writeBuf, namespaceDone.trackNamespaceSuffix, size, error);
  writeSize(sizePtr, size, error, *version_);
  if (error) {
    return folly::makeUnexpected(quic::TransportErrorCode::INTERNAL_ERROR);
  }
  return size;
}

WriteResult MoQFrameWriter::writeFetch(
    folly::IOBufQueue& writeBuf,
    const Fetch& fetch) const noexcept {
  CHECK(version_.has_value()) << "Version needs to be set to write fetch";
  size_t size = 0;
  bool error = false;
  auto sizePtr = writeFrameHeader(writeBuf, FrameType::FETCH, error);
  writeVarint(writeBuf, fetch.requestID.value, size, error);

  if (getDraftMajorVersion(*version_) < 15) {
    writeBuf.append(&fetch.priority, 1);
    size += 1;

    auto order = folly::to_underlying(fetch.groupOrder);
    writeBuf.append(&order, 1);
    size += 1;
  }

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

  std::vector<Parameter> requestSpecificParams;
  if (getDraftMajorVersion(*version_) >= 15) {
    if (fetch.priority != kDefaultPriority) {
      Parameter priorityParam;
      priorityParam.key =
          folly::to_underlying(TrackRequestParamKey::SUBSCRIBER_PRIORITY);
      priorityParam.asUint64 = fetch.priority;
      requestSpecificParams.push_back(priorityParam);
    }

    if (fetch.groupOrder != GroupOrder::Default) {
      Parameter groupOrderParam;
      groupOrderParam.key =
          folly::to_underlying(TrackRequestParamKey::GROUP_ORDER);
      groupOrderParam.asUint64 = folly::to_underlying(fetch.groupOrder);
      requestSpecificParams.push_back(groupOrderParam);
    }
  }
  writeTrackRequestParams(
      writeBuf, fetch.params, requestSpecificParams, size, error);

  writeSize(sizePtr, size, error, *version_);
  if (error) {
    return folly::makeUnexpected(quic::TransportErrorCode::INTERNAL_ERROR);
  }
  return size;
}

WriteResult MoQFrameWriter::writeFetchCancel(
    folly::IOBufQueue& writeBuf,
    const FetchCancel& fetchCancel) const noexcept {
  CHECK(version_.has_value())
      << "Version needs to be set to write fetch cancel";
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
  CHECK(version_.has_value()) << "Version needs to be set to write fetch ok";
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

  // Make a mutable copy of params for potential extension->param conversion
  TrackRequestParameters params = fetchOk.params;

  // For < v16: convert track property extensions to params
  if (getDraftMajorVersion(*version_) < 16) {
    convertTrackPropertyExtensionsToParams(fetchOk.extensions, params);
  }

  writeTrackRequestParams(writeBuf, params, {}, size, error);

  // Draft 16+: Write extensions
  if (getDraftMajorVersion(*version_) >= 16) {
    writeExtensions(
        writeBuf, fetchOk.extensions, size, error, /*withLengthPrefix=*/false);
  }
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
  CHECK(version_.has_value())
      << "Version needs to be set to write request error";
  // XCHECK that frameType is one of the allowed types for this function
  XCHECK(
      frameType == FrameType::SUBSCRIBE_ERROR ||
      frameType == FrameType::REQUEST_ERROR ||
      frameType == FrameType::PUBLISH_NAMESPACE_ERROR ||
      frameType == FrameType::SUBSCRIBE_NAMESPACE_ERROR ||
      frameType == FrameType::PUBLISH_ERROR ||
      frameType == FrameType::FETCH_ERROR ||
      frameType == FrameType::TRACK_STATUS_ERROR ||
      frameType == FrameType::SUBSCRIBE_UPDATE)
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
  // Write retryInterval for version 16+
  if (getDraftMajorVersion(*version_) >= 16) {
    writeVarint(
        writeBuf,
        requestError.retryInterval ? requestError.retryInterval->count() : 0,
        size,
        error);
  }
  writeFixedString(writeBuf, requestError.reasonPhrase, size, error);

  writeSize(sizePtr, size, error, *version_);
  if (error) {
    return folly::makeUnexpected(quic::TransportErrorCode::INTERNAL_ERROR);
  }
  return size;
}

bool isValidSubgroupType(uint64_t version, uint64_t streamType) {
  if ((streamType & 0x10) == 0) { // subgroup bit
    return false;
  }
  if ((streamType & 0x06) == 0x06) { // invalid subgroup type
    return false;
  }
  uint64_t max = 0x3D;
  if (getDraftMajorVersion(version) < 15) {
    max = 0x1D;
  }
  return (streamType <= max);
}

bool isValidDatagramType(uint64_t version, uint64_t datagramType) {
  auto majorVersion = getDraftMajorVersion(version);
  if (majorVersion < 15) {
    // v14: types 0x00-0x07 (payload) and 0x20-0x21 (status)
    return (
        datagramType <= folly::to_underlying(
                            DatagramType::OBJECT_DATAGRAM_EXT_EOG_ID_ZERO) ||
        (datagramType >=
             folly::to_underlying(DatagramType::OBJECT_DATAGRAM_STATUS) &&
         datagramType <=
             folly::to_underlying(DatagramType::OBJECT_DATAGRAM_STATUS_EXT)));
  } else {
    // v15+: types 0x00-0x0F (payload) and 0x20-0x25, 0x28-0x2D (status)
    return (
        datagramType <=
            folly::to_underlying(
                DatagramType::OBJECT_DATAGRAM_EXT_EOG_ID_ZERO_NO_PRI) ||
        (datagramType >=
             folly::to_underlying(DatagramType::OBJECT_DATAGRAM_STATUS) &&
         datagramType <=
             folly::to_underlying(
                 DatagramType::OBJECT_DATAGRAM_STATUS_EXT_ID_ZERO)) ||
        (datagramType >= folly::to_underlying(
                             DatagramType::OBJECT_DATAGRAM_STATUS_NO_PRI) &&
         datagramType <=
             folly::to_underlying(
                 DatagramType::OBJECT_DATAGRAM_STATUS_EXT_ID_ZERO_NO_PRI)));
  }
}

std::optional<FrameType> getFrameType(const folly::IOBufQueue& readBuf) {
  if (readBuf.empty()) {
    return std::nullopt;
  }
  folly::io::Cursor cursor(readBuf.front());
  auto frameType = quic::follyutils::decodeQuicInteger(cursor);
  if (!frameType) {
    return std::nullopt;
  }
  return static_cast<FrameType>(frameType->first);
}

// Version translation helpers for track property extensions <-> params
// These are used when communicating with < v16 peers

void MoQFrameWriter::convertTrackPropertyExtensionsToParams(
    const Extensions& extensions,
    TrackRequestParameters& params) const noexcept {
  // Convert track property extensions to params for < v16 compatibility
  // Properties: DELIVERY_TIMEOUT, MAX_CACHE_DURATION, PUBLISHER_PRIORITY,
  //             GROUP_ORDER, DYNAMIC_GROUPS

  auto checkAndAddIfPresent = [&params](uint64_t paramKey, auto val) {
    if (val) {
      params.insertParam(Parameter(paramKey, *val));
    }
  };

  checkAndAddIfPresent(
      folly::to_underlying(TrackRequestParamKey::DELIVERY_TIMEOUT),
      extensions.getIntExtension(kDeliveryTimeoutExtensionType));

  checkAndAddIfPresent(
      folly::to_underlying(TrackRequestParamKey::MAX_CACHE_DURATION),
      extensions.getIntExtension(kMaxCacheDurationExtensionType));

  checkAndAddIfPresent(
      folly::to_underlying(TrackRequestParamKey::PUBLISHER_PRIORITY),
      extensions.getIntExtension(kPublisherPriorityExtensionType));

  // Note: GROUP_ORDER and DYNAMIC_GROUPS are v16+ only extensions,
  // they don't have param equivalents in older versions
}

void MoQFrameParser::convertTrackPropertyParamsToExtensions(
    const TrackRequestParameters& params,
    Extensions& extensions) const noexcept {
  // Convert track property params to extensions for uniform access
  // Properties: DELIVERY_TIMEOUT, MAX_CACHE_DURATION, PUBLISHER_PRIORITY

  for (const auto& param : params) {
    switch (param.key) {
      case folly::to_underlying(TrackRequestParamKey::DELIVERY_TIMEOUT):
        extensions.insertMutableExtension(
            Extension{kDeliveryTimeoutExtensionType, param.asUint64});
        break;
      case folly::to_underlying(TrackRequestParamKey::MAX_CACHE_DURATION):
        extensions.insertMutableExtension(
            Extension{kMaxCacheDurationExtensionType, param.asUint64});
        break;
      case folly::to_underlying(TrackRequestParamKey::PUBLISHER_PRIORITY):
        extensions.insertMutableExtension(
            Extension{kPublisherPriorityExtensionType, param.asUint64});
        break;
      default:
        // Other params are not track properties, skip
        break;
    }
  }
}

} // namespace moxygen

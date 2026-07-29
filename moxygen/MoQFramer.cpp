/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/MoQFramer.h"
#include <folly/lang/Bits.h>
#include <folly/logging/xlog.h>

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <limits>
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

// Per-key value encoding for draft-18 Message Parameters (§10.2).
enum class ParamValueEncoding {
  Uint8,
  Varint,
  Location,
  LengthPrefixed,
};

// Caller must reject unknown keys (Parameters::isKnownParamKey) before calling.
ParamValueEncoding paramEncodingV18(uint64_t key) {
  using K = moxygen::TrackRequestParamKey;
  switch (static_cast<K>(key)) {
    case K::SUBSCRIBER_PRIORITY:
    case K::GROUP_ORDER:
    case K::FORWARD:
      return ParamValueEncoding::Uint8;
    case K::OBJECT_DELIVERY_TIMEOUT:
    case K::RENDEZVOUS_TIMEOUT:
    case K::SUBGROUP_DELIVERY_TIMEOUT:
    case K::FILL_TIMEOUT:
    case K::EXPIRES:
    case K::NEW_GROUP_REQUEST:
    // PUBLISHER_PRIORITY is extensions-only in v16+; parsed as varint so the
    // caller's allowlist check can reject it cleanly.
    case K::PUBLISHER_PRIORITY:
      return ParamValueEncoding::Varint;
    case K::LARGEST_OBJECT:
      return ParamValueEncoding::Location;
    case K::AUTHORIZATION_TOKEN:
    case K::SUBSCRIPTION_FILTER:
    case K::TRACK_NAMESPACE_PREFIX:
    // TRACK_FILTER (0x29) is a fork-local active proposal; length-prefixed
    // value decoded via parseVariableParam (see parseTrackFilter).
    case K::TRACK_FILTER:
      return ParamValueEncoding::LengthPrefixed;
  }
  XLOG(DFATAL) << "paramEncodingV18: unknown key " << key;
  return ParamValueEncoding::Varint;
}

bool isRequestSpecificParam(moxygen::TrackRequestParamKey key) {
  switch (key) {
    case moxygen::TrackRequestParamKey::SUBSCRIPTION_FILTER:
    case moxygen::TrackRequestParamKey::LARGEST_OBJECT:
    case moxygen::TrackRequestParamKey::EXPIRES:
    case moxygen::TrackRequestParamKey::GROUP_ORDER:
    case moxygen::TrackRequestParamKey::SUBSCRIBER_PRIORITY:
    case moxygen::TrackRequestParamKey::FORWARD:
    case moxygen::TrackRequestParamKey::NEW_GROUP_REQUEST:
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

// Draft <= 16 forbids a DELIVERY_TIMEOUT of 0
folly::Expected<folly::Unit, moxygen::ErrorCode>
validateDeliveryTimeoutExtension(
    const moxygen::Extensions& extensions,
    uint64_t version) {
  if (moxygen::getDraftMajorVersion(version) <= 16) {
    auto val =
        extensions.getIntExtension(moxygen::kDeliveryTimeoutExtensionType);
    if (val && *val == 0) {
      return folly::makeUnexpected(moxygen::ErrorCode::PROTOCOL_VIOLATION);
    }
  }
  return folly::unit;
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

bool isPublishOkRequestSpecificParam(moxygen::TrackRequestParamKey key) {
  switch (key) {
    case moxygen::TrackRequestParamKey::SUBSCRIPTION_FILTER:
    case moxygen::TrackRequestParamKey::EXPIRES:
    case moxygen::TrackRequestParamKey::GROUP_ORDER:
    case moxygen::TrackRequestParamKey::SUBSCRIBER_PRIORITY:
    case moxygen::TrackRequestParamKey::FORWARD:
    case moxygen::TrackRequestParamKey::NEW_GROUP_REQUEST:
      return true;
    default:
      return false;
  }
}

void appendIntRequestSpecificParam(
    std::vector<moxygen::Parameter>& requestSpecificParams,
    moxygen::TrackRequestParamKey key,
    std::optional<uint64_t> value) {
  if (!value.has_value()) {
    return;
  }
  requestSpecificParams.emplace_back(folly::to_underlying(key), *value);
}

void insertPublishOkIntParamIfMissing(
    moxygen::TrackRequestParameters& params,
    moxygen::TrackRequestParamKey key,
    uint64_t value,
    const char* context) {
  if (moxygen::getFirstIntParam(params, key).has_value()) {
    return;
  }
  auto insertResult =
      params.insertParam(moxygen::Parameter(folly::to_underlying(key), value));
  if (insertResult.hasError()) {
    XLOG(WARN) << context
               << ": ignoring param key=" << folly::to_underlying(key);
  }
}

std::vector<moxygen::Parameter> getPublishOkRequestSpecificParams(
    const moxygen::PublishOk& publishOk,
    bool includeRequestOkOnlyParams = false) {
  std::vector<moxygen::Parameter> requestSpecificParams;

  moxygen::Parameter subscriptionFilterParam;
  subscriptionFilterParam.key =
      folly::to_underlying(moxygen::TrackRequestParamKey::SUBSCRIPTION_FILTER);
  subscriptionFilterParam.asSubscriptionFilter = moxygen::SubscriptionFilter(
      publishOk.locType,
      publishOk.start,
      publishOk.locType == moxygen::LocationType::AbsoluteRange
          ? publishOk.endGroup
          : std::nullopt);
  requestSpecificParams.push_back(subscriptionFilterParam);

  if (publishOk.subscriberPriority != moxygen::kDefaultPriority) {
    moxygen::Parameter priorityParam;
    priorityParam.key = folly::to_underlying(
        moxygen::TrackRequestParamKey::SUBSCRIBER_PRIORITY);
    priorityParam.asUint64 = publishOk.subscriberPriority;
    requestSpecificParams.push_back(priorityParam);
  }

  if (publishOk.groupOrder != moxygen::GroupOrder::Default) {
    moxygen::Parameter groupOrderParam;
    groupOrderParam.key =
        folly::to_underlying(moxygen::TrackRequestParamKey::GROUP_ORDER);
    groupOrderParam.asUint64 = folly::to_underlying(publishOk.groupOrder);
    requestSpecificParams.push_back(groupOrderParam);
  }

  if (!publishOk.forward) {
    moxygen::Parameter forwardParam;
    forwardParam.key =
        folly::to_underlying(moxygen::TrackRequestParamKey::FORWARD);
    forwardParam.asUint64 = 0;
    requestSpecificParams.push_back(forwardParam);
  }

  if (includeRequestOkOnlyParams) {
    appendIntRequestSpecificParam(
        requestSpecificParams,
        moxygen::TrackRequestParamKey::EXPIRES,
        moxygen::getFirstIntParam(
            publishOk.params, moxygen::TrackRequestParamKey::EXPIRES));
  }
  appendIntRequestSpecificParam(
      requestSpecificParams,
      moxygen::TrackRequestParamKey::NEW_GROUP_REQUEST,
      moxygen::getFirstIntParam(
          publishOk.params, moxygen::TrackRequestParamKey::NEW_GROUP_REQUEST));

  return requestSpecificParams;
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

folly::Expected<uint64_t, moxygen::ErrorCode> resolveFetchGroupID(
    const std::optional<uint64_t>& groupIDField,
    bool useFetchObjectDeltas,
    const std::optional<uint64_t>& previousFetchGroup,
    moxygen::GroupOrder fetchGroupOrder) {
  if (!groupIDField.has_value()) {
    if (!previousFetchGroup.has_value()) {
      XLOG(ERR) << "resolveFetchGroupID: First object must have explicit "
                   "group ID";
      return folly::makeUnexpected(moxygen::ErrorCode::PROTOCOL_VIOLATION);
    }
    return previousFetchGroup.value();
  }

  if (!useFetchObjectDeltas) {
    return groupIDField.value();
  }

  auto groupIDDelta = groupIDField.value();
  if (!previousFetchGroup.has_value()) {
    return groupIDDelta;
  }

  if (fetchGroupOrder == moxygen::GroupOrder::NewestFirst) {
    if (groupIDDelta == std::numeric_limits<uint64_t>::max() ||
        previousFetchGroup.value() < groupIDDelta + 1) {
      return folly::makeUnexpected(moxygen::ErrorCode::PROTOCOL_VIOLATION);
    }
    return previousFetchGroup.value() - (groupIDDelta + 1);
  }

  if (groupIDDelta == std::numeric_limits<uint64_t>::max() ||
      previousFetchGroup.value() >
          std::numeric_limits<uint64_t>::max() - groupIDDelta - 1) {
    return folly::makeUnexpected(moxygen::ErrorCode::PROTOCOL_VIOLATION);
  }
  return previousFetchGroup.value() + groupIDDelta + 1;
}

folly::Expected<uint64_t, moxygen::ErrorCode> resolveFetchObjectID(
    const std::optional<uint64_t>& objectIDField,
    bool draft18GroupIDDeltaPresent,
    bool useFetchObjectDeltas,
    const std::optional<uint64_t>& previousObjectID) {
  DCHECK(useFetchObjectDeltas || !draft18GroupIDDeltaPresent);
  if (objectIDField.has_value()) {
    if (!useFetchObjectDeltas || draft18GroupIDDeltaPresent) {
      return objectIDField.value();
    }

    if (!previousObjectID.has_value()) {
      XLOG(ERR) << "resolveFetchObjectID: First object cannot reference "
                   "prior object ID";
      return folly::makeUnexpected(moxygen::ErrorCode::PROTOCOL_VIOLATION);
    }
    return decodeDelta(previousObjectID.value(), objectIDField.value());
  }

  if (!previousObjectID.has_value()) {
    XLOG(ERR) << "resolveFetchObjectID: First object must have explicit "
                 "object ID";
    return folly::makeUnexpected(moxygen::ErrorCode::PROTOCOL_VIOLATION);
  }
  if (useFetchObjectDeltas &&
      previousObjectID.value() == std::numeric_limits<uint64_t>::max()) {
    return folly::makeUnexpected(moxygen::ErrorCode::PROTOCOL_VIOLATION);
  }
  return previousObjectID.value() + 1;
}

struct FetchObjectDeltaFields {
  std::optional<uint64_t> groupIDDelta;
  std::optional<uint64_t> objectIDDelta;
};

std::optional<FetchObjectDeltaFields> computeFetchObjectDeltaFieldsForWrite(
    const moxygen::ObjectHeader& objectHeader,
    const std::optional<uint64_t>& previousFetchGroup,
    const std::optional<uint64_t>& previousObjectID,
    moxygen::GroupOrder fetchGroupOrder) {
  FetchObjectDeltaFields fields;

  if (!previousFetchGroup.has_value()) {
    fields.groupIDDelta = objectHeader.group;
  } else if (objectHeader.group != previousFetchGroup.value()) {
    if (fetchGroupOrder == moxygen::GroupOrder::NewestFirst) {
      if (objectHeader.group >= previousFetchGroup.value()) {
        return std::nullopt;
      }
      fields.groupIDDelta = previousFetchGroup.value() - objectHeader.group - 1;
    } else {
      if (objectHeader.group <= previousFetchGroup.value()) {
        return std::nullopt;
      }
      fields.groupIDDelta = objectHeader.group - previousFetchGroup.value() - 1;
    }
  }
  const bool groupIDDeltaPresent = fields.groupIDDelta.has_value();

  bool objectIDIsNext = false;
  if (previousObjectID.has_value() &&
      previousObjectID.value() != std::numeric_limits<uint64_t>::max()) {
    objectIDIsNext = objectHeader.id == previousObjectID.value() + 1;
  }
  if (!previousObjectID.has_value()) {
    fields.objectIDDelta = objectHeader.id;
  } else if (groupIDDeltaPresent) {
    if (!objectIDIsNext) {
      fields.objectIDDelta = objectHeader.id;
    }
  } else if (!objectIDIsNext) {
    if (objectHeader.id < previousObjectID.value()) {
      return std::nullopt;
    }
    fields.objectIDDelta = objectHeader.id - previousObjectID.value();
  }

  return fields;
}

void appendZeroPadding(
    folly::IOBufQueue& writeBuf,
    uint64_t paddingLength,
    size_t& size,
    bool& error) {
  constexpr size_t kPaddingChunkSize = 4096;

  if (paddingLength >
      static_cast<uint64_t>(std::numeric_limits<size_t>::max() - size)) {
    error = true;
    return;
  }
  while (paddingLength > 0) {
    auto chunkSize = static_cast<size_t>(
        std::min<uint64_t>(paddingLength, kPaddingChunkSize));
    auto padding = folly::IOBuf::create(chunkSize);
    padding->append(chunkSize);
    std::memset(padding->writableData(), 0, chunkSize);
    writeBuf.append(std::move(padding));
    paddingLength -= chunkSize;
    size += chunkSize;
  }
}

} // namespace

namespace moxygen {

// Forward declarations for iOS.
bool datagramTypeHasExtensions(uint64_t version, DatagramType streamType);
bool datagramTypeIsStatus(uint64_t version, DatagramType streamType);
bool datagramObjectIdZero(uint64_t version, DatagramType datagramType);

void writeSize(uint16_t* sizePtr, size_t size, bool& error, uint64_t versionIn);

void writeTrackFilter(
    folly::IOBufQueue& writeBuf,
    const TrackFilter& filter,
    size_t& size,
    bool& error) noexcept;

bool includeSetupParam(uint64_t version, SetupKey key);

bool isPaddingStreamType(uint64_t version, uint64_t streamType) {
  return getDraftMajorVersion(version) >= 18 &&
      streamType == folly::to_underlying(StreamType::PADDING);
}

bool isPaddingDatagramType(uint64_t version, uint64_t datagramType) {
  return getDraftMajorVersion(version) >= 18 &&
      datagramType == folly::to_underlying(DatagramType::PADDING);
}

folly::Expected<folly::Unit, ErrorCode> parsePaddingData(
    folly::io::Cursor& cursor,
    size_t& length) noexcept {
  while (length > 0) {
    if (!cursor.canAdvance(1)) {
      return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
    }
    if (cursor.readBE<uint8_t>() != 0) {
      return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
    }
    --length;
  }
  return folly::unit;
}

// Test-only helper: QUIC varint length prefix + fixed string. Production code
// should call MoQFrameParser::parseFixedString to dispatch on negotiated
// version.
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

folly::Expected<std::string, ErrorCode> MoQFrameParser::parseFixedString(
    folly::io::Cursor& cursor,
    size_t& length) const noexcept {
  auto strLength = decodeVarint(cursor, length);
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

folly::Expected<std::optional<AuthToken>, ErrorCode>
MoQFrameParser::parseAuthToken(
    folly::io::Cursor& cursor,
    size_t length,
    ParamsType paramsType) const noexcept {
  auto& tokenCache = *tokenCache_;
  std::optional<AuthToken> token;
  token.emplace(); // plan for success
  auto aliasType = decodeVarint(cursor, length);
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
      auto tokenAlias = decodeVarint(cursor, length);
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
      auto tokenAlias = decodeVarint(cursor, length);
      if (!tokenAlias) {
        XLOG(DBG4) << "parseToken: UNDERFLOW on tokenAlias (REGISTER)";
        return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
      }
      length -= tokenAlias->second;
      token->alias = tokenAlias->first;

      auto tokenType = decodeVarint(cursor, length);
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
      auto tokenType = decodeVarint(cursor, length);
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

folly::Expected<AbsoluteLocation, ErrorCode>
MoQFrameParser::parseAbsoluteLocation(folly::io::Cursor& cursor, size_t& length)
    const noexcept {
  AbsoluteLocation location;
  auto group = decodeVarint(cursor, length);
  if (!group) {
    XLOG(DBG4) << "parseAbsoluteLocation: UNDERFLOW on group";
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  location.group = group->first;
  length -= group->second;

  auto object = decodeVarint(cursor, length);
  if (!object) {
    XLOG(DBG4) << "parseAbsoluteLocation: UNDERFLOW on object";
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  location.object = object->first;
  length -= object->second;

  return location;
}

folly::Expected<SubscriptionFilter, ErrorCode>
MoQFrameParser::parseSubscriptionFilter(
    folly::io::Cursor& cursor,
    size_t& length) const noexcept {
  SubscriptionFilter filter;

  // Parse filter type
  auto filterType = decodeVarint(cursor, length);
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
    auto endGroup = decodeVarint(cursor, length);
    if (!endGroup) {
      XLOG(DBG4) << "parseSubscriptionFilter: UNDERFLOW on endGroup";
      return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
    }
    // In draft-18+, EndGroup is encoded as a delta from StartLocation.group.
    // In draft-17 and earlier, EndGroup is an absolute group number.
    if (getDraftMajorVersion(*version_) >= 18) {
      if (endGroup->first > kEightByteLimit - filter.location->group) {
        XLOG(ERR) << "parseSubscriptionFilter: EndGroup delta wraps "
                  << "(start.group=" << filter.location->group
                  << " + delta=" << endGroup->first << " > kEightByteLimit)";
        return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
      }
      filter.endGroup = endGroup->first + filter.location->group;
    } else {
      filter.endGroup = endGroup->first;
    }
    length -= endGroup->second;
  }

  return filter;
}

folly::Expected<TrackFilter, ErrorCode> parseTrackFilter(
    folly::io::Cursor& cursor,
    size_t& length) noexcept {
  TrackFilter filter;

  // Parse propertyType
  auto propertyType = quic::follyutils::decodeQuicInteger(cursor, length);
  if (!propertyType) {
    XLOG(DBG4) << "parseTrackFilter: UNDERFLOW on propertyType";
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= propertyType->second;
  filter.propertyType = propertyType->first;

  // Parse maxSelected (N)
  auto maxSelected = quic::follyutils::decodeQuicInteger(cursor, length);
  if (!maxSelected) {
    XLOG(DBG4) << "parseTrackFilter: UNDERFLOW on maxSelected";
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= maxSelected->second;
  filter.maxSelected = maxSelected->first;

  return filter;
}

folly::Expected<std::optional<Parameter>, ErrorCode>
MoQFrameParser::parseVariableParam(
    folly::io::Cursor& cursor,
    size_t& length,
    uint64_t version,
    uint64_t key,
    ParamsType paramsType) const noexcept {
  Parameter p;
  p.key = key;
  const auto authKey =
      folly::to_underlying(TrackRequestParamKey::AUTHORIZATION_TOKEN);
  const auto subscriptionFilterKey =
      folly::to_underlying(TrackRequestParamKey::SUBSCRIPTION_FILTER);
  const auto trackFilterKey =
      folly::to_underlying(TrackRequestParamKey::TRACK_FILTER);
  if (key == authKey) {
    auto res = decodeVarint(cursor, length);
    if (!res) {
      XLOG(DBG4) << "parseVariableParam: UNDERFLOW on authKey length";
      return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
    }
    length -= res->second;
    if (res->first > length || !cursor.canAdvance(res->first)) {
      XLOG(DBG4) << "parseVariableParam: UNDERFLOW on authKey data";
      return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
    }
    auto tokenRes = parseAuthToken(cursor, res->first, paramsType);
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
    auto lenRes = decodeVarint(cursor, length);
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
    if (filterSize > 0) {
      XLOG(DBG4) << "parseVariableParam: subscription filter did not consume"
                 << " all declared bytes, remaining=" << filterSize;
      return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
    }
    length -= lenRes->first;
    p.asSubscriptionFilter = res.value();
  } else if (key == trackFilterKey && getDraftMajorVersion(version) >= 16) {
    // TRACK_FILTER (key=0x29, odd = length-prefixed)
    // NOTE: TRACK_FILTER is an active proposal, not yet landed
    // in the core specification. Using draft-16+ check as a placeholder until
    // the feature is formally specified.
    auto lenRes = quic::follyutils::decodeQuicInteger(cursor, length);
    if (!lenRes) {
      XLOG(DBG4) << "parseVariableParam: UNDERFLOW on trackFilter length";
      return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
    }
    length -= lenRes->second;
    auto filterLen = lenRes->first;
    if (filterLen > length || !cursor.canAdvance(filterLen)) {
      XLOG(DBG4) << "parseVariableParam: UNDERFLOW on trackFilter data";
      return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
    }
    size_t filterSize = filterLen;
    auto res = parseTrackFilter(cursor, filterSize);
    if (!res) {
      return folly::makeUnexpected(res.error());
    }
    length -= lenRes->first;
    p.asTrackFilter = res.value();
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

folly::Expected<std::optional<Parameter>, ErrorCode>
MoQFrameParser::parseIntParam(
    folly::io::Cursor& cursor,
    size_t& length,
    uint64_t version,
    uint64_t key,
    ParamsType paramsType) const noexcept {
  Parameter p;
  p.key = key;
  auto res = decodeVarint(cursor, length);
  if (!res) {
    XLOG(DBG4) << "parseIntParam: UNDERFLOW on integer decode";
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= res->second;
  p.asUint64 = res->first;

  if (!isIntParamValid(version, p.key, p.asUint64)) {
    return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
  }

  if (paramsType == ParamsType::Request &&
      getDraftMajorVersion(version) <= 16 &&
      key == folly::to_underlying(TrackRequestParamKey::DELIVERY_TIMEOUT) &&
      p.asUint64 == 0) {
    return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
  }
  return p;
}

folly::Expected<std::optional<Parameter>, ErrorCode>
MoQFrameParser::parseV18ParamValue(
    folly::io::Cursor& cursor,
    size_t& length,
    uint64_t version,
    uint64_t key,
    ParamsType paramsType) const noexcept {
  switch (paramEncodingV18(key)) {
    case ParamValueEncoding::Uint8: {
      if (length < 1 || !cursor.canAdvance(1)) {
        XLOG(DBG4) << "parseV18ParamValue: UNDERFLOW on uint8, key=" << key;
        return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
      }
      uint8_t value = cursor.read<uint8_t>();
      length -= 1;
      if (!isIntParamValid(version, key, value)) {
        return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
      }
      return Parameter(key, static_cast<uint64_t>(value));
    }
    case ParamValueEncoding::Varint:
      return parseIntParam(cursor, length, version, key, paramsType);
    case ParamValueEncoding::Location: {
      auto loc = parseAbsoluteLocation(cursor, length);
      if (!loc) {
        return folly::makeUnexpected(loc.error());
      }
      return Parameter(key, std::optional<AbsoluteLocation>(loc.value()));
    }
    case ParamValueEncoding::LengthPrefixed:
      return parseVariableParam(cursor, length, version, key, paramsType);
  }
  XLOG(DFATAL) << "parseV18ParamValue: unreachable, key=" << key;
  return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
}

folly::Expected<folly::Unit, ErrorCode> MoQFrameParser::parseParams(
    folly::io::Cursor& cursor,
    size_t& length,
    uint64_t version,
    std::optional<size_t> numParams,
    Parameters& params,
    std::vector<Parameter>& requestSpecificParams,
    ParamsType paramsType) const noexcept {
  uint64_t previousKey = 0;

  // numParams == std::nullopt means "consume options until the declared
  // message length is exhausted" (draft-17+ SETUP has no Number-of-Options
  // field on the wire).
  for (auto i = 0u; numParams ? i < *numParams : length > 0; i++) {
    auto keyOrDelta = decodeVarint(cursor, length);
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
        !Parameters::isKnownParamKey(key, getDraftMajorVersion(version))) {
      XLOG(ERR) << "Unknown parameter key " << key << " in v16+";
      return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
    }

    folly::Expected<std::optional<Parameter>, ErrorCode> res;

    if (getDraftMajorVersion(version) >= 18 &&
        paramsType == ParamsType::Request) {
      res = parseV18ParamValue(cursor, length, version, key, paramsType);
    } else if (
        (paramsType == ParamsType::Request &&
         key == folly::to_underlying(TrackRequestParamKey::DELIVERY_TIMEOUT)) ||
        ((key & 0x01) == 0 &&
         (paramsType != ParamsType::Request ||
          key !=
              folly::to_underlying(
                  TrackRequestParamKey::AUTHORIZATION_TOKEN)))) {
      res = parseIntParam(cursor, length, version, key, paramsType);
    } else if (
        key == folly::to_underlying(TrackRequestParamKey::LARGEST_OBJECT)) {
      if (getDraftMajorVersion(version) < 15) {
        XLOG(ERR) << "Invalid parameter LARGEST_OBJECT for version " << version;
        return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
      }

      // Read length prefix (odd key = length-prefixed)
      auto lenRes = decodeVarint(cursor, length);
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
      if (objSize > 0) {
        XLOG(DBG4) << "parseParams: LARGEST_OBJECT did not consume"
                   << " all declared bytes, remaining=" << objSize;
        return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
      }
      length -= lenRes->first;
      res = Parameter(key, largestLocation.value());
    } else {
      res = parseVariableParam(cursor, length, version, key, paramsType);
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
          // In draft 18+, receiving parameters in a message in which it isn't
          // allowed is a protocol violation.
          if (getDraftMajorVersion(version) >= 18) {
            XLOG(ERR) << "parseParams: param not allowed for frame type in "
                      << "v18+ at param index=" << i << ", key=" << key;
            return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
          }
          XLOG(WARN) << "parseParams: ignoring param not allowed for frame type"
                     << " at param index=" << i << ", key=" << key;
        }
      }
    }
  }
  XLOG(DBG4) << "parseParams: returning success";
  return folly::unit;
}

folly::Expected<Setup, ErrorCode> MoQFrameParser::parseClientSetup(
    folly::io::Cursor& cursor,
    size_t length) noexcept {
  Setup clientSetup;
  uint64_t serializationVersion = kVersionDraft14;

  // Only parse version array when version is not initialized, i.e. alpn did not
  // happen, or when version is initialized but is < 15 (in tests)
  if (!version_ || getDraftMajorVersion(*version_) < 15) {
    auto numVersions = decodeVarint(cursor, length);
    if (!numVersions) {
      XLOG(DBG4) << "parseClientSetup: UNDERFLOW on numVersions";
      return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
    }
    length -= numVersions->second;
    bool foundDraft14 = false;
    for (auto i = 0ul; i < numVersions->first; i++) {
      auto version = decodeVarint(cursor, length);
      if (!version) {
        XLOG(DBG4) << "parseClientSetup: UNDERFLOW on version";
        return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
      }
      length -= version->second;
      if (getDraftMajorVersion(version->first) == 14) {
        foundDraft14 = true;
      }
    }
    if (!foundDraft14) {
      XLOG(ERR) << "Draft-14 not found in ClientSetup version array"
                   " (legacy mode only supports draft-14)";
      return folly::makeUnexpected(ErrorCode::VERSION_NEGOTIATION_FAILED);
    }
  } else {
    XLOG(DBG3)
        << "Skipped parsing versions from wire for alpn ClientSetup message";
    serializationVersion = *version_;
  }

  // Draft-17 removed the Number-of-Options field; options span the rest of
  // the message length. Older drafts still carry an explicit count.
  std::optional<size_t> numParams;
  if (getDraftMajorVersion(serializationVersion) < 17) {
    auto decoded = decodeVarint(cursor, length);
    if (!decoded) {
      XLOG(DBG4) << "parseClientSetup: UNDERFLOW on numParams";
      return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
    }
    length -= decoded->second;
    numParams = decoded->first;
  }
  std::vector<Parameter> requestSpecificParams;
  auto res = parseParams(
      cursor,
      length,
      serializationVersion,
      numParams,
      clientSetup.params,
      requestSpecificParams,
      ParamsType::ClientSetup);
  if (res.hasError()) {
    return folly::makeUnexpected(res.error());
  }
  if (length > 0) {
    return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
  }
  return clientSetup;
}

folly::Expected<Setup, ErrorCode> MoQFrameParser::parseServerSetup(
    folly::io::Cursor& cursor,
    size_t length) noexcept {
  Setup serverSetup;
  uint64_t serializationVersion = kVersionDraft14;

  // Only parse version when version is not initialized, i.e. alpn did not
  // happen, or when version is initialized but is < 15 (in tests)
  if (!version_ || getDraftMajorVersion(*version_) < 15) {
    auto version = decodeVarint(cursor, length);
    if (!version) {
      XLOG(DBG4) << "parseServerSetup: UNDERFLOW on version";
      return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
    }
    length -= version->second;
    if (getDraftMajorVersion(version->first) != 14) {
      XLOG(ERR) << "Server selected version draft-"
                << getDraftMajorVersion(version->first)
                << " but we only offer draft-14 in legacy mode";
      return folly::makeUnexpected(ErrorCode::VERSION_NEGOTIATION_FAILED);
    }
    serializationVersion = version->first;
  } else {
    XLOG(DBG3)
        << "Skipped parsing version from wire for alpn ServerSetup message";
    serializationVersion = *version_;
  }

  // Draft-17 removed the Number-of-Options field; options span the rest of
  // the message length. Older drafts still carry an explicit count.
  std::optional<size_t> numParams;
  if (getDraftMajorVersion(serializationVersion) < 17) {
    auto decoded = decodeVarint(cursor, length);
    if (!decoded) {
      XLOG(DBG4) << "parseServerSetup: UNDERFLOW on numParams";
      return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
    }
    length -= decoded->second;
    numParams = decoded->first;
  }
  std::vector<Parameter> requestSpecificParams;
  auto res = parseParams(
      cursor,
      length,
      serializationVersion,
      numParams,
      serverSetup.params,
      requestSpecificParams,
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
  auto requestID = decodeVarint(cursor, length);
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

void MoQFrameParser::setFetchGroupOrder(GroupOrder groupOrder) noexcept {
  fetchGroupOrder_ =
      groupOrder == GroupOrder::Default ? GroupOrder::OldestFirst : groupOrder;
}

void MoQFrameParser::resetFetchContext() const noexcept {
  previousFetchGroup_.reset();
  previousFetchSubgroup_.reset();
  previousObjectID_.reset();
  previousFetchPriority_.reset();
  fetchGroupOrder_ = GroupOrder::OldestFirst;
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
  auto trackAlias = decodeVarint(cursor, length);
  if (!trackAlias) {
    XLOG(DBG4) << "parseDatagramObjectHeader: UNDERFLOW on trackAlias";
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= trackAlias->second;

  auto group = decodeVarint(cursor, length);
  if (!group) {
    XLOG(DBG4) << "parseDatagramObjectHeader: UNDERFLOW on group";
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= group->second;
  objectHeader.group = group->first;

  if (!datagramObjectIdZero(*version_, datagramType)) {
    auto id = decodeVarint(cursor, length);
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
    auto status = decodeVarint(cursor, length);
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
  XCHECK(version_.has_value())
      << "The version must be set before parsing subgroup header";
  auto startLength = length;
  SubgroupHeaderResult result;
  ObjectHeader& objectHeader = result.objectHeader;
  objectHeader.group = std::numeric_limits<uint64_t>::max(); // unset
  objectHeader.id = std::numeric_limits<uint64_t>::max();    // unset

  auto parsedTrackAlias = decodeVarint(cursor, length);
  if (!parsedTrackAlias) {
    XLOG(DBG4) << "parseSubgroupHeader: UNDERFLOW on trackAlias";
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= parsedTrackAlias->second;
  result.trackAlias = TrackAlias(parsedTrackAlias->first);

  auto group = decodeVarint(cursor, length);
  if (!group) {
    XLOG(DBG4) << "parseSubgroupHeader: UNDERFLOW on group";
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= group->second;
  objectHeader.group = group->first;

  bool parseObjectID = false;
  if (options.subgroupIDFormat == SubgroupIDFormat::Present) {
    auto subgroup = decodeVarint(cursor, length);
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
    auto id = decodeVarint(tmpCursor, length);
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
  auto group = decodeVarint(cursor, remainingLength);
  if (!group) {
    XLOG(DBG4) << "parseFetchObjectHeaderLegacy: UNDERFLOW on group";
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  remainingLength -= group->second;
  objectHeader.group = group->first;

  // Subgroup ID (varint)
  auto subgroup = decodeVarint(cursor, remainingLength);
  if (!subgroup) {
    XLOG(DBG4) << "parseFetchObjectHeaderLegacy: UNDERFLOW on subgroup";
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  remainingLength -= subgroup->second;
  objectHeader.subgroup = subgroup->first;

  // Object ID (varint)
  auto id = decodeVarint(cursor, remainingLength);
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
    auto flagsResult = decodeVarint(cursor, remainingLength);
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
    auto groupId = decodeVarint(cursor, remainingLength);
    if (!groupId) {
      XLOG(DBG4) << "parseFetchObjectDraft15: UNDERFLOW on End of Range group";
      return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
    }
    remainingLength -= groupId->second;

    auto objectId = decodeVarint(cursor, remainingLength);
    if (!objectId) {
      XLOG(DBG4) << "parseFetchObjectDraft15: UNDERFLOW on End of Range object";
      return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
    }
    remainingLength -= objectId->second;

    previousFetchGroup_ = groupId->first;
    previousObjectID_ = objectId->first;

    length = remainingLength;
    return EndOfRangeMarker{
        groupId->first,
        objectId->first,
        flags == kSerializationFlagEndOfUnknownRange};
  }

  if (flags >= 128) {
    XLOG(ERR) << "parseFetchObjectDraft15: Invalid serialization flags: 0x"
              << std::hex << flags;
    return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
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
  const bool useFetchObjectDeltas = getDraftMajorVersion(*version_) >= 18;
  const bool groupIDFieldPresent = bitFlags &
      folly::to_underlying(FetchHeaderSerializationBits::GROUP_ID_BITMASK);
  const bool objectIDFieldPresent = bitFlags &
      folly::to_underlying(FetchHeaderSerializationBits::OBJECT_ID_BITMASK);
  const bool groupIDDeltaPresent = useFetchObjectDeltas && groupIDFieldPresent;
  const bool objectIDDeltaPresent =
      useFetchObjectDeltas && objectIDFieldPresent;
  const bool hasPreviousFetchGroup = previousFetchGroup_.has_value();
  const bool hasPreviousObjectID = previousObjectID_.has_value();
  if (hasPreviousFetchGroup != hasPreviousObjectID) {
    XLOG(ERR) << "parseFetchObjectDraft15: Inconsistent prior FETCH object "
                 "state";
    return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
  }
  const bool firstFetchObject = !hasPreviousFetchGroup;

  if (useFetchObjectDeltas && firstFetchObject &&
      (!groupIDDeltaPresent || !objectIDDeltaPresent)) {
    XLOG(ERR) << "parseFetchObjectDraft15: First draft-18 FETCH object must "
                 "include group and object deltas";
    return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
  }

  // Decode Group ID (flags & 0x08). In draft 18+, this is Group ID Delta.
  std::optional<uint64_t> groupIDField;
  if (groupIDFieldPresent) {
    auto parsedGroupIDField = decodeVarint(cursor, remainingLength);
    if (!parsedGroupIDField) {
      XLOG(DBG4) << "parseFetchObjectDraft15: UNDERFLOW on group";
      return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
    }
    remainingLength -= parsedGroupIDField->second;
    groupIDField = parsedGroupIDField->first;
  }
  auto groupID = resolveFetchGroupID(
      groupIDField,
      useFetchObjectDeltas,
      previousFetchGroup_,
      fetchGroupOrder_);
  if (groupID.hasError()) {
    return folly::makeUnexpected(groupID.error());
  }
  objectHeader.group = groupID.value();

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
        auto subgroup = decodeVarint(cursor, remainingLength);
        if (!subgroup) {
          XLOG(DBG4) << "parseFetchObjectDraft15: UNDERFLOW on subgroup";
          return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
        }
        remainingLength -= subgroup->second;
        objectHeader.subgroup = subgroup->first;
        break;
    }
  }

  // Decode Object ID (flags & 0x04). In draft 18+, this is Object ID Delta.
  std::optional<uint64_t> objectIDField;
  if (objectIDFieldPresent) {
    auto parsedObjectIDField = decodeVarint(cursor, remainingLength);
    if (!parsedObjectIDField) {
      XLOG(DBG4) << "parseFetchObjectDraft15: UNDERFLOW on id";
      return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
    }
    remainingLength -= parsedObjectIDField->second;
    objectIDField = parsedObjectIDField->first;
  }
  auto objectID = resolveFetchObjectID(
      objectIDField,
      groupIDDeltaPresent,
      useFetchObjectDeltas,
      previousObjectID_);
  if (objectID.hasError()) {
    return folly::makeUnexpected(objectID.error());
  }
  objectHeader.id = objectID.value();

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
  auto payloadLength = decodeVarint(cursor, length);
  if (!payloadLength) {
    XLOG(DBG4) << "parseObjectStatusAndLength: UNDERFLOW on payloadLength";
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= payloadLength->second;
  objectHeader.length = payloadLength->first;

  if (objectHeader.length == 0) {
    auto objectStatus = decodeVarint(cursor, length);
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
  auto id = decodeVarint(cursor, length);
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
  XCHECK(version_.has_value())
      << "The version must be set before parsing track request params";
  params.setMajorVersion(getDraftMajorVersion(*version_));
  return parseParams(
      cursor,
      length,
      *version_,
      numParams,
      params,
      requestSpecificParams,
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

std::optional<TrackFilter> MoQFrameParser::extractTrackFilter(
    const std::vector<Parameter>& requestSpecificParams) const noexcept {
  for (const auto& param : requestSpecificParams) {
    if (param.key ==
        folly::to_underlying(TrackRequestParamKey::TRACK_FILTER)) {
      return param.asTrackFilter;
    }
  }
  return std::nullopt;
}

folly::Expected<SubscribeRequest, ErrorCode>
MoQFrameParser::parseSubscribeRequest(folly::io::Cursor& cursor, size_t length)
    const noexcept {
  XCHECK(version_.has_value())
      << "The version must be set before parsing a subscribe request";
  SubscribeRequest subscribeRequest;
  auto requestID = decodeVarint(cursor, length);
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
    auto locType = decodeVarint(cursor, length);
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
      auto endGroup = decodeVarint(cursor, length);
      if (!endGroup) {
        XLOG(DBG4) << "parseSubscribeRequest: UNDERFLOW on endGroup";
        return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
      }
      subscribeRequest.endGroup = endGroup->first;
      length -= endGroup->second;
    }
  }
  auto numParams = decodeVarint(cursor, length);
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
  XCHECK(version_.has_value())
      << "The version must be set before parsing a request update";

  RequestUpdate requestUpdate;
  auto requestID = decodeVarint(cursor, length);
  if (!requestID) {
    XLOG(DBG4) << "parseRequestUpdate: UNDERFLOW on requestID";
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  requestUpdate.requestID = requestID->first;
  length -= requestID->second;

  if (getDraftMajorVersion(*version_) >= 14 &&
      getDraftMajorVersion(*version_) < 18) {
    auto existingRequestID = decodeVarint(cursor, length);
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

    auto endGroup = decodeVarint(cursor, length);
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

  auto numParams = decodeVarint(cursor, length);
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
  // Draft 18+: requestID is implicit from the bidi request stream context.
  if (getDraftMajorVersion(*version_) < 18) {
    auto requestID = decodeVarint(cursor, length);
    if (!requestID) {
      XLOG(DBG4) << "parseSubscribeOk: UNDERFLOW on requestID";
      return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
    }
    length -= requestID->second;
    subscribeOk.requestID = requestID->first;
  }
  auto trackAlias = decodeVarint(cursor, length);
  if (!trackAlias) {
    XLOG(DBG4) << "parseSubscribeOk: UNDERFLOW on trackAlias";
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= trackAlias->second;
  subscribeOk.trackAlias = trackAlias->first;

  // For < v15: parse expires and groupOrder from fixed fields
  if (getDraftMajorVersion(*version_) < 15) {
    auto expires = decodeVarint(cursor, length);
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

  auto numParams = decodeVarint(cursor, length);
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
  } else {
    // For v16+: extract GROUP_ORDER from extensions into struct field
    auto go = subscribeOk.extensions.getIntExtension(
        kPublisherGroupOrderExtensionType);
    if (go) {
      subscribeOk.groupOrder = static_cast<GroupOrder>(*go);
    }
  }

  if (auto res =
          validateDeliveryTimeoutExtension(subscribeOk.extensions, *version_);
      !res) {
    XLOG(DBG4) << "parseSubscribeOk: invalid DELIVERY_TIMEOUT extension";
    return folly::makeUnexpected(res.error());
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
      case TrackRequestParamKey::LARGEST_OBJECT:
        subscribeOk.largest = param.largestObject;
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
  auto requestID = decodeVarint(cursor, length);
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
  // Draft 18+: requestID is implicit from the bidi request stream context.
  if (getDraftMajorVersion(*version_) < 18) {
    auto requestID = decodeVarint(cursor, length);
    if (!requestID) {
      XLOG(DBG4) << "parsePublishDone: UNDERFLOW on requestID";
      return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
    }
    length -= requestID->second;
    publishDone.requestID = requestID->first;
  }

  auto statusCode = decodeVarint(cursor, length);
  if (!statusCode) {
    XLOG(DBG4) << "parsePublishDone: UNDERFLOW on statusCode";
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= statusCode->second;
  publishDone.statusCode = PublishDoneStatusCode(statusCode->first);

  auto streamCount = decodeVarint(cursor, length);
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

  XCHECK(version_.has_value())
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
  XCHECK(version_.has_value())
      << "The version must be set before parsing a publish request";
  PublishRequest publish;
  auto requestID = decodeVarint(cursor, length);
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

  auto trackAlias = decodeVarint(cursor, length);
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

  auto numParams = decodeVarint(cursor, length);
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
  } else {
    auto go =
        publish.extensions.getIntExtension(kPublisherGroupOrderExtensionType);
    if (go) {
      publish.groupOrder = static_cast<GroupOrder>(*go);
    }
  }

  if (auto validateRes =
          validateDeliveryTimeoutExtension(publish.extensions, *version_);
      !validateRes) {
    XLOG(DBG4) << "parsePublish: invalid DELIVERY_TIMEOUT extension";
    return folly::makeUnexpected(validateRes.error());
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
  XCHECK(version_.has_value())
      << "The version must be set before parsing a publish ok";
  if (getDraftMajorVersion(*version_) >= 18) {
    return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
  }
  PublishOk publishOk;
  auto requestID = decodeVarint(cursor, length);
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
    auto locType = decodeVarint(cursor, length);
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
      auto endGroup = decodeVarint(cursor, length);
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

  auto numParams = decodeVarint(cursor, length);
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
  auto requestID = decodeVarint(cursor, length);
  if (!requestID) {
    XLOG(DBG4) << "parsePublishNamespace: UNDERFLOW on requestID";
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= requestID->second;
  publishNamespace.requestID = requestID->first;

  auto res = parseNamespaceTuple(cursor, length);
  if (!res) {
    return folly::makeUnexpected(res.error());
  }
  publishNamespace.trackNamespace = TrackNamespace(std::move(res.value()));
  auto numParams = decodeVarint(cursor, length);
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
  // Draft 18+: requestID is implicit from the bidi request stream context.
  if (getDraftMajorVersion(*version_) < 18) {
    auto requestID = decodeVarint(cursor, length);
    if (!requestID) {
      XLOG(DBG4) << "parseRequestOk: UNDERFLOW on requestID";
      return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
    }
    length -= requestID->second;
    requestOk.requestID = requestID->first;
  }
  if (getDraftMajorVersion(*version_) > 14) {
    // Parse track request params into requestOk.params
    auto numParams = decodeVarint(cursor, length);
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

  if (getDraftMajorVersion(*version_) >= 18 && length > 0) {
    ObjectHeader tempHeader;
    auto ext = parseExtensionKvPairs(cursor, tempHeader, length, true);
    if (!ext) {
      XLOG(DBG4) << "parseRequestOk: error in parseExtensionKvPairs: "
                 << folly::to_underlying(ext.error());
      return folly::makeUnexpected(ext.error());
    }
    requestOk.trackProperties = std::move(tempHeader.extensions);
    length = 0;
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
    auto requestID = decodeVarint(cursor, length);
    if (!requestID) {
      XLOG(DBG4) << "parsePublishNamespaceDone: UNDERFLOW on requestID";
      return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
    }
    length -= requestID->second;
    publishNamespaceDone.requestID = RequestID(requestID->first);
  } else {
    // v15 and below: Parse TrackNamespace
    auto res = parseNamespaceTuple(cursor, length);
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
    auto requestID = decodeVarint(cursor, length);
    if (!requestID) {
      XLOG(DBG4) << "parsePublishNamespaceCancel: UNDERFLOW on requestID";
      return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
    }
    length -= requestID->second;
    publishNamespaceCancel.requestID = RequestID(requestID->first);
  } else {
    // v15 and below: Parse TrackNamespace
    auto res = parseNamespaceTuple(cursor, length);
    if (!res) {
      return folly::makeUnexpected(res.error());
    }
    publishNamespaceCancel.trackNamespace =
        TrackNamespace(std::move(res.value()));
  }

  auto errorCode = decodeVarint(cursor, length);
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
  XCHECK(version_.has_value())
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

  auto requestID = decodeVarint(cursor, length);
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

  auto numParams = decodeVarint(cursor, length);
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
  XCHECK(version_.has_value())
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
  auto requestID = decodeVarint(cursor, length);
  if (!requestID) {
    XLOG(DBG4) << "parseTrackStatusOk: UNDERFLOW on requestID";
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= requestID->second;
  trackStatusOk.requestID = requestID->first;
  auto statusCode = decodeVarint(cursor, length);
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

  auto numParams = decodeVarint(cursor, length);
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
  if (getDraftMajorVersion(*version_) >= 18) {
    auto timeout = decodeVarint(cursor, length);
    if (!timeout) {
      XLOG(DBG4) << "parseGoaway: UNDERFLOW on timeout";
      return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
    }
    length -= timeout->second;
    goaway.timeout = timeout->first;

    auto requestID = decodeVarint(cursor, length);
    if (!requestID) {
      XLOG(DBG4) << "parseGoaway: UNDERFLOW on requestID";
      return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
    }
    length -= requestID->second;
    goaway.requestID = RequestID(requestID->first);
  }
  if (length > 0) {
    return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
  }
  return goaway;
}

folly::Expected<MaxRequestID, ErrorCode> MoQFrameParser::parseMaxRequestID(
    folly::io::Cursor& cursor,
    size_t length) const noexcept {
  MaxRequestID maxRequestID;
  auto requestID = decodeVarint(cursor, length);
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
  auto res = decodeVarint(cursor, length);
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
  auto res = decodeVarint(cursor, length);
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

  auto fetchType = decodeVarint(cursor, length);
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
    auto jsid = decodeVarint(cursor, length);
    if (!jsid) {
      XLOG(DBG4) << "parseFetch: UNDERFLOW on jsid";
      return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
    }
    length -= jsid->second;

    auto joiningStart = decodeVarint(cursor, length);
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
  auto numParams = decodeVarint(cursor, length);
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
  auto res = decodeVarint(cursor, length);
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
  // Draft 18+: requestID is implicit from the bidi request stream context.
  if (getDraftMajorVersion(*version_) < 18) {
    auto res = decodeVarint(cursor, length);
    if (!res) {
      XLOG(DBG4) << "parseFetchOk: UNDERFLOW on requestID";
      return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
    }
    fetchOk.requestID = res->first;
    length -= res->second;
  }

  // Check for next two bytes
  if (getDraftMajorVersion(*version_) < 15) {
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
  } else {
    if (length < 1) {
      XLOG(DBG4) << "parseFetchOk: UNDERFLOW on endOfTrack";
      return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
    }
    fetchOk.endOfTrack = cursor.readBE<uint8_t>();
    length -= sizeof(uint8_t);
  }

  auto res2 = parseAbsoluteLocation(cursor, length);
  if (!res2) {
    return folly::makeUnexpected(res2.error());
  }
  fetchOk.endLocation = std::move(res2.value());

  auto numParams = decodeVarint(cursor, length);
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

  // For v15+: GROUP_ORDER comes from request-specific params (not fixed field)
  if (getDraftMajorVersion(*version_) >= 15) {
    fetchOk.groupOrder = GroupOrder::OldestFirst;
    handleGroupOrderParam(
        fetchOk.groupOrder, requestSpecificParams, GroupOrder::OldestFirst);
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
  } else {
    auto go =
        fetchOk.extensions.getIntExtension(kPublisherGroupOrderExtensionType);
    if (go) {
      fetchOk.groupOrder = static_cast<GroupOrder>(*go);
    }
  }

  return fetchOk;
}

folly::Expected<SubscribeNamespace, ErrorCode>
MoQFrameParser::parseSubscribeNamespace(
    folly::io::Cursor& cursor,
    size_t length) const noexcept {
  SubscribeNamespace subscribeNamespace;

  // Parse Request ID
  auto requestID = decodeVarint(cursor, length);
  if (!requestID) {
    XLOG(DBG4) << "parseSubscribeNamespace: UNDERFLOW on requestID";
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= requestID->second;
  subscribeNamespace.requestID = requestID->first;

  // Parse Track Namespace Prefix
  auto res = parseNamespaceTuple(cursor, length);
  if (!res) {
    return folly::makeUnexpected(res.error());
  }
  subscribeNamespace.trackNamespacePrefix =
      TrackNamespace(std::move(res.value()));

  auto majorVersion = getDraftMajorVersion(*version_);

  // The SUBSCRIBE_NAMESPACE message has an "options" field only in drafts
  // 16 and 17. In draft 18+, SUBSCRIBE_NAMESPACE is NAMESPACE-only — peers
  // wanting PUBLISH fan-out use the new SUBSCRIBE_TRACKS message. Prior to
  // draft 16 the field doesn't exist either, but the legacy behavior is BOTH.
  if (majorVersion >= 16 && majorVersion < 18) {
    auto options = decodeVarint(cursor, length);
    if (!options) {
      XLOG(DBG4) << "parseSubscribeNamespace: UNDERFLOW on options";
      return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
    }
    length -= options->second;
    subscribeNamespace.options =
        static_cast<SubscribeNamespaceOptions>(options->first);
  } else if (majorVersion >= 18) {
    subscribeNamespace.options = SubscribeNamespaceOptions::NAMESPACE;
  } else {
    subscribeNamespace.options = SubscribeNamespaceOptions::BOTH;
  }

  // Parse Parameters
  auto numParams = decodeVarint(cursor, length);
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

  // FORWARD is only present as a parameter for drafts 15, 16, and 17.
  if (majorVersion >= 15 && majorVersion < 18) {
    handleForwardParam(subscribeNamespace.forward, requestSpecificParams);
  }

  return subscribeNamespace;
}

folly::Expected<SubscribeTracks, ErrorCode>
MoQFrameParser::parseSubscribeTracks(folly::io::Cursor& cursor, size_t length)
    const noexcept {
  XCHECK_GE(getDraftMajorVersion(*version_), 18u)
      << "SUBSCRIBE_TRACKS is draft 18+ only";
  SubscribeTracks subscribeTracks;

  auto requestID = decodeVarint(cursor, length);
  if (!requestID) {
    XLOG(DBG4) << "parseSubscribeTracks: UNDERFLOW on requestID";
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= requestID->second;
  subscribeTracks.requestID = requestID->first;

  auto res = parseNamespaceTuple(cursor, length);
  if (!res) {
    return folly::makeUnexpected(res.error());
  }
  subscribeTracks.trackNamespacePrefix = TrackNamespace(std::move(res.value()));

  auto numParams = decodeVarint(cursor, length);
  if (!numParams) {
    XLOG(DBG4) << "parseSubscribeTracks: UNDERFLOW on numParams";
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= numParams->second;
  std::vector<Parameter> requestSpecificParams;
  auto res2 = parseTrackRequestParams(
      cursor,
      length,
      numParams->first,
      subscribeTracks.params,
      requestSpecificParams);
  if (!res2) {
    return folly::makeUnexpected(res2.error());
  }
  if (length > 0) {
    return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
  }

  handleForwardParam(subscribeTracks.forward, requestSpecificParams);
  return subscribeTracks;
}

folly::Expected<PublishBlocked, ErrorCode> MoQFrameParser::parsePublishBlocked(
    folly::io::Cursor& cursor,
    size_t length) const noexcept {
  XCHECK(version_)
      << "Need to have version_ set in order to parse PUBLISH_BLOCKED";
  XCHECK_GE(getDraftMajorVersion(*version_), 18u)
      << "PUBLISH_BLOCKED is draft 18+ only";
  PublishBlocked publishBlocked;

  // Track Namespace Suffix
  auto res = parseNamespaceTuple(cursor, length);
  if (!res) {
    XLOG(DBG4) << "parsePublishBlocked: error parsing track namespace suffix";
    return folly::makeUnexpected(res.error());
  }
  publishBlocked.trackNamespaceSuffix = TrackNamespace(std::move(res.value()));

  // Track Name Length + Track Name
  auto trackName = parseFixedString(cursor, length);
  if (!trackName) {
    XLOG(DBG4) << "parsePublishBlocked: UNDERFLOW on trackName";
    return folly::makeUnexpected(trackName.error());
  }
  publishBlocked.trackName = std::move(trackName.value());

  if (length > 0) {
    return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
  }
  return publishBlocked;
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

  // Draft 18+: requestID is implicit from the bidi request stream context.
  if (getDraftMajorVersion(*version_) < 18) {
    auto requestID = decodeVarint(cursor, length);
    if (!requestID) {
      XLOG(DBG4) << "parseRequestError: UNDERFLOW on requestID";
      return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
    }
    length -= requestID->second;
    requestError.requestID = requestID->first;
  }

  // Parse errorCode
  auto errorCode = decodeVarint(cursor, length);
  if (!errorCode) {
    XLOG(DBG4) << "parseRequestError: UNDERFLOW on errorCode";
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= errorCode->second;
  requestError.errorCode = RequestErrorCode(errorCode->first);

  // Parse retryInterval (version 16+)
  if (getDraftMajorVersion(*version_) >= 16) {
    auto retryInterval = decodeVarint(cursor, length);
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

  if (requestError.errorCode == RequestErrorCode::REDIRECT) {
    if (getDraftMajorVersion(*version_) < 18) {
      XLOG(DBG4) << "REDIRECT errorCode received on pre-v18 draft";
      return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
    }
  }

  if (getDraftMajorVersion(*version_) >= 18 &&
      requestError.errorCode == RequestErrorCode::REDIRECT) {
    Redirect redirect;

    auto connectUri = parseFixedString(cursor, length);
    if (!connectUri) {
      XLOG(DBG4) << "parseRequestError: UNDERFLOW on Redirect Connect URI";
      return folly::makeUnexpected(connectUri.error());
    }
    redirect.connectUri = std::move(connectUri.value());

    auto fullTrackName = parseFullTrackName(cursor, length);
    if (!fullTrackName) {
      XLOG(DBG4) << "parseRequestError: error parsing Redirect Full Track Name";
      return folly::makeUnexpected(fullTrackName.error());
    }
    redirect.fullTrackName = std::move(fullTrackName.value());

    requestError.redirect = std::move(redirect);
  }

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
    auto requestID = decodeVarint(cursor, length);
    if (!requestID) {
      return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
    }
    length -= requestID->second;
    unsubscribeNamespace.requestID = RequestID(requestID->first);
  } else {
    // <v15: Parse Track Namespace Prefix
    auto res = parseNamespaceTuple(cursor, length);
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
  XCHECK(version_) << "Need to have version_ set in order to parse NAMESPACE";
  XCHECK_GE(getDraftMajorVersion(*version_), 16)
      << "NAMESPACE message doesn't exist for version 15 and below, this function "
      << "shouldn't be called";
  Namespace ns;

  // Parse Track Namespace Suffix
  auto res = parseNamespaceTuple(cursor, length);
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
  XCHECK(version_)
      << "Need to have version_ set in order to parse NAMESPACE_DONE";
  XCHECK_GE(getDraftMajorVersion(*version_), 16)
      << "NAMESPACE_DONE message doesn't exist for version 15 and below, this function "
      << "shouldn't be called";
  NamespaceDone namespaceDone;

  // Parse Track Namespace Suffix
  auto res = parseNamespaceTuple(cursor, length);
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
  auto res = parseNamespaceTuple(cursor, length);
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
  XCHECK(version_.has_value())
      << "The version must be set before parsing extensions";

  // Parse the length of the extension block
  auto extLen = decodeVarint(cursor, length);
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
  auto type = decodeVarint(cursor, length);
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
    auto extLen = decodeVarint(cursor, length);
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
      // Reset delta encoding state post-immutable.
      previousExtensionType_ = kImmutableExtensionType;
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
    auto iVal = decodeVarint(cursor, length);
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
MoQFrameParser::parseNamespaceTuple(folly::io::Cursor& cursor, size_t& length)
    const noexcept {
  auto itemCount = decodeVarint(cursor, length);
  if (!itemCount) {
    XLOG(DBG4) << "parseNamespaceTuple: UNDERFLOW on itemCount";
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
    if (version_ && getDraftMajorVersion(*version_) >= 16 &&
        res.value().empty()) {
      XLOG(ERR)
          << "parseNamespaceTuple: empty namespace field value in draft >= 16";
      return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
    }
    items.emplace_back(std::move(res.value()));
  }
  return items;
}

/*static*/ folly::Expected<TrackNamespace, ErrorCode>
MoQFrameParser::parseTrackNamespacePrefixParam(
    const std::string& value,
    uint64_t version) {
  XCHECK_GE(getDraftMajorVersion(version), 18u);
  MoQFrameParser parser;
  parser.initializeVersion(version);
  auto buf = folly::IOBuf::wrapBufferAsValue(value.data(), value.size());
  folly::io::Cursor cursor(&buf);
  size_t length = value.size();
  auto tuple = parser.parseNamespaceTuple(cursor, length);
  if (!tuple) {
    return folly::makeUnexpected(tuple.error());
  }
  if (length != 0) {
    XLOG(DBG4) << "parseTrackNamespacePrefixParam: trailing bytes in value";
    return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
  }
  return TrackNamespace(std::move(tuple.value()));
}

/*static*/ Parameter MoQFrameWriter::encodeTrackNamespacePrefixParam(
    const TrackNamespace& trackNamespacePrefix,
    uint64_t version) {
  XCHECK_GE(getDraftMajorVersion(version), 18u);
  MoQFrameWriter writer;
  writer.initializeVersion(version);
  folly::IOBufQueue buf{folly::IOBufQueue::cacheChainLength()};
  size_t size = 0;
  bool error = false;
  writer.writeTrackNamespace(buf, trackNamespacePrefix, size, error);
  XCHECK(!error)
      << "encodeTrackNamespacePrefixParam: failed to encode namespace";
  auto tuple = buf.move();
  std::string value =
      tuple ? tuple->moveToFbString().toStdString() : std::string();
  return Parameter(
      folly::to_underlying(TrackRequestParamKey::TRACK_NAMESPACE_PREFIX),
      value);
}

//// Transforms /////
TrackStatusOk RequestOk::toTrackStatusOk() const {
  TrackStatusOk trackStatusOk;
  trackStatusOk.requestID = requestID;
  trackStatusOk.params = params;
  trackStatusOk.trackProperties = trackProperties;

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
  requestOk.trackProperties = trackStatusOk.trackProperties;

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

static folly::Expected<PublishOk, ErrorCode> requestOkToPublishOk(
    const RequestOk& requestOk,
    uint64_t majorVersion) {
  PublishOk publishOk;
  publishOk.requestID = requestOk.requestID;
  publishOk.params = TrackRequestParameters(FrameType::PUBLISH_OK);
  publishOk.params.setMajorVersion(majorVersion);

  for (const auto& param : requestOk.params) {
    auto insertResult = publishOk.params.insertParam(param);
    if (insertResult.hasError()) {
      return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
    }
  }

  for (const auto& param : requestOk.requestSpecificParams) {
    if (!isPublishOkRequestSpecificParam(
            static_cast<TrackRequestParamKey>(param.key))) {
      return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
    }
  }

  std::optional<SubscriptionFilter> filter;
  for (const auto& param : requestOk.requestSpecificParams) {
    if (param.key ==
        folly::to_underlying(TrackRequestParamKey::SUBSCRIPTION_FILTER)) {
      filter = param.asSubscriptionFilter;
      break;
    }
  }
  if (filter.has_value()) {
    publishOk.locType = filter->filterType;
    publishOk.start = filter->location;
    publishOk.endGroup = filter->endGroup;
  } else {
    publishOk.locType = LocationType::AbsoluteStart;
    publishOk.start = AbsoluteLocation{0, 0};
    publishOk.endGroup = std::nullopt;
  }

  auto maybeGroupOrder = getFirstIntParam(
      requestOk.requestSpecificParams, TrackRequestParamKey::GROUP_ORDER);
  publishOk.groupOrder = maybeGroupOrder.has_value()
      ? static_cast<GroupOrder>(*maybeGroupOrder)
      : GroupOrder::Default;

  auto maybePriority = getFirstIntParam(
      requestOk.requestSpecificParams,
      TrackRequestParamKey::SUBSCRIBER_PRIORITY);
  publishOk.subscriberPriority = maybePriority.has_value()
      ? static_cast<uint8_t>(*maybePriority)
      : kDefaultPriority;

  auto maybeForward = getFirstIntParam(
      requestOk.requestSpecificParams, TrackRequestParamKey::FORWARD);
  publishOk.forward = maybeForward.has_value() ? (*maybeForward == 1) : true;

  auto maybeExpires = getFirstIntParam(
      requestOk.requestSpecificParams, TrackRequestParamKey::EXPIRES);
  if (maybeExpires.has_value()) {
    insertPublishOkIntParamIfMissing(
        publishOk.params,
        TrackRequestParamKey::EXPIRES,
        *maybeExpires,
        "toPublishOk");
  }

  auto maybeNewGroupRequest = getFirstIntParam(
      requestOk.requestSpecificParams, TrackRequestParamKey::NEW_GROUP_REQUEST);
  if (maybeNewGroupRequest.has_value()) {
    insertPublishOkIntParamIfMissing(
        publishOk.params,
        TrackRequestParamKey::NEW_GROUP_REQUEST,
        *maybeNewGroupRequest,
        "toPublishOk");
  }

  return publishOk;
}

folly::Expected<PublishOk, ErrorCode> RequestOk::toPublishOk(
    uint64_t majorVersion) const {
  return requestOkToPublishOk(*this, majorVersion);
}

// static
RequestOk RequestOk::fromPublishOk(
    const PublishOk& publishOk,
    uint64_t majorVersion) {
  RequestOk requestOk;
  requestOk.requestID = publishOk.requestID;
  requestOk.params = TrackRequestParameters(FrameType::PUBLISH_OK);
  requestOk.params.setMajorVersion(majorVersion);
  for (const auto& param : publishOk.params) {
    if (isPublishOkRequestSpecificParam(
            static_cast<TrackRequestParamKey>(param.key))) {
      continue;
    }
    auto insertResult = requestOk.params.insertParam(param);
    if (insertResult.hasError()) {
      XLOG(WARN) << "fromPublishOk: ignoring param not allowed for PUBLISH_OK"
                 << " key=" << param.key;
    }
  }
  requestOk.requestSpecificParams =
      getPublishOkRequestSpecificParams(publishOk, true);
  return requestOk;
}

//// Egress ////

// Test-only helper. Always emits a QUIC varint. Production paths use
// MoQFrameWriter::writeVarint for version-aware dispatch.
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
    appender.writeBE(folly::tag<decltype(val)>, val);
  };
  auto res = quic::encodeQuicInteger(value, appenderOp);
  if (res.hasError()) {
    error = true;
  } else {
    size += *res;
  }
}

void MoQFrameWriter::writeFixedString(
    folly::IOBufQueue& writeBuf,
    const std::string& str,
    size_t& size,
    bool& error) const noexcept {
  writeVarint(writeBuf, str.size(), size, error);
  // Avoid appending a zero-length string, which can lead to undefined behavior
  // on some platforms when passing a null data pointer with length 0.
  if (!error && !str.empty()) {
    writeBuf.append(str);
    size += str.size();
  }
}

void MoQFrameWriter::writeFixedTuple(
    folly::IOBufQueue& writeBuf,
    const std::vector<std::string>& tup,
    size_t& size,
    bool& error) const noexcept {
  writeVarint(writeBuf, tup.size(), size, error);
  if (!error) {
    for (auto& str : tup) {
      writeFixedString(writeBuf, str, size, error);
    }
  }
}

void MoQFrameWriter::writeTrackNamespace(
    folly::IOBufQueue& writeBuf,
    const TrackNamespace& tn,
    size_t& size,
    bool& error) const noexcept {
  writeFixedTuple(writeBuf, tn.trackNamespace, size, error);
}

uint16_t* MoQFrameWriter::writeFrameHeader(
    folly::IOBufQueue& writeBuf,
    FrameType frameType,
    bool& error) const noexcept {
  size_t size = 0;
  writeVarint(writeBuf, folly::to_underlying(frameType), size, error);
  auto res = writeBuf.preallocate(2, 256);
  writeBuf.postallocate(2);
  XCHECK_GE(res.second, 2);
  return static_cast<uint16_t*>(res.first);
}

void writeSize(
    uint16_t* sizePtr,
    size_t size,
    bool& error,
    uint64_t versionIn) {
  if (size > ((1 << 16) - 1)) {
    XLOG(ERR) << "Control message size exceeds max sz=" << size;
    error = true;
    return;
  }
  uint16_t sizeVal = folly::Endian::big(uint16_t(size));
  memcpy(sizePtr, &sizeVal, 2);
}

void MoQFrameWriter::writeFullTrackName(
    folly::IOBufQueue& writeBuf,
    const FullTrackName& fullTrackName,
    size_t& size,
    bool error) const noexcept {
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
  // Draft 18+ delivers requests on independent bidi streams, so auth token
  // aliasing (which relies on request ordering) is disabled. Strip the param.
  if (key == SetupKey::MAX_AUTH_TOKEN_CACHE_SIZE &&
      useBidiRequestStreams(version)) {
    return false;
  }
  return key == SetupKey::MAX_REQUEST_ID || key == SetupKey::PATH ||
      key == SetupKey::MAX_AUTH_TOKEN_CACHE_SIZE ||
      key == SetupKey::AUTHORIZATION_TOKEN;
}

WriteResult writeSetup(
    folly::IOBufQueue& writeBuf,
    const Setup& setup,
    uint64_t version,
    bool isClient) noexcept {
  // Setup is version-agnostic, so we spin up a local MoQFrameWriter to
  // dispatch to version-aware writeVarint / writeFrameHeader /
  // writeFixedString.
  MoQFrameWriter writer;
  writer.initializeVersion(version);
  size_t size = 0;
  bool error = false;

  FrameType frameType;
  if (getDraftMajorVersion(version) >= 17) {
    frameType = FrameType::SETUP;
  } else {
    frameType = isClient ? FrameType::CLIENT_SETUP : FrameType::SERVER_SETUP;
  }
  auto sizePtr = writer.writeFrameHeader(writeBuf, frameType, error);

  // Pre-ALPN: write version(s) to wire
  if (getDraftMajorVersion(version) < 15) {
    XCHECK_EQ(getDraftMajorVersion(version), 14u)
        << "Legacy mode only supports draft-14, got draft-"
        << getDraftMajorVersion(version);
    if (isClient) {
      writer.writeVarint(writeBuf, 1, size, error); // version count
    }
    writer.writeVarint(writeBuf, kVersionDraft14, size, error);
  }

  // Collect params that should be included
  std::vector<Parameter> filteredParams;
  for (const auto& param : setup.params) {
    if (includeSetupParam(version, SetupKey(param.key))) {
      filteredParams.push_back(param);
    }
  }

  // Sort params by key for delta encoding (v16+)
  if (getDraftMajorVersion(version) >= 16) {
    filteredParams = sortParamsByKey(std::move(filteredParams));
  }

  // Draft-17 removed the Number-of-Options field; options span the rest of
  // the message length. Older drafts still carry an explicit count.
  if (getDraftMajorVersion(version) < 17) {
    writer.writeVarint(writeBuf, filteredParams.size(), size, error);
  }

  uint64_t previousKey = 0;
  for (const auto& param : filteredParams) {
    auto keyToWrite = param.key;
    if (getDraftMajorVersion(version) >= 16) {
      keyToWrite = param.key - previousKey;
      previousKey = param.key;
    }
    writer.writeVarint(writeBuf, keyToWrite, size, error);
    if ((param.key & 0x01) == 0) {
      writer.writeVarint(writeBuf, param.asUint64, size, error);
    } else {
      writer.writeFixedString(writeBuf, param.asString, size, error);
    }
  }
  writeSize(sizePtr, size, error, version);
  if (error) {
    return folly::makeUnexpected(quic::TransportErrorCode::INTERNAL_ERROR);
  }
  return size;
}

WriteResult writeClientSetup(
    folly::IOBufQueue& writeBuf,
    const Setup& clientSetup,
    uint64_t version) noexcept {
  return writeSetup(writeBuf, clientSetup, version, /*isClient=*/true);
}

WriteResult writeServerSetup(
    folly::IOBufQueue& writeBuf,
    const Setup& serverSetup,
    uint64_t version) noexcept {
  return writeSetup(writeBuf, serverSetup, version, /*isClient=*/false);
}

WriteResult MoQFrameWriter::writeSubgroupHeader(
    folly::IOBufQueue& writeBuf,
    TrackAlias trackAlias,
    const ObjectHeader& objectHeader,
    SubgroupIDFormat format,
    bool includeExtensions,
    bool beginsWithFirstObject) const noexcept {
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
      priorityPresent,
      beginsWithFirstObject);
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

WriteResult MoQFrameWriter::writePaddingStream(
    folly::IOBufQueue& writeBuf,
    uint64_t paddingLength) const noexcept {
  XCHECK(version_.has_value()) << "The version must be set before writing";
  if (!isPaddingStreamType(
          *version_, folly::to_underlying(StreamType::PADDING))) {
    return folly::makeUnexpected(quic::TransportErrorCode::INTERNAL_ERROR);
  }
  size_t size = 0;
  bool error = false;
  writeVarint(writeBuf, folly::to_underlying(StreamType::PADDING), size, error);
  appendZeroPadding(writeBuf, paddingLength, size, error);
  if (error) {
    return folly::makeUnexpected(quic::TransportErrorCode::INTERNAL_ERROR);
  }
  return size;
}

WriteResult MoQFrameWriter::writePaddingDatagram(
    folly::IOBufQueue& writeBuf,
    uint64_t paddingLength) const noexcept {
  XCHECK(version_.has_value()) << "The version must be set before writing";
  if (!isPaddingDatagramType(
          *version_, folly::to_underlying(DatagramType::PADDING))) {
    return folly::makeUnexpected(quic::TransportErrorCode::INTERNAL_ERROR);
  }
  size_t size = 0;
  bool error = false;
  writeVarint(
      writeBuf, folly::to_underlying(DatagramType::PADDING), size, error);
  appendZeroPadding(writeBuf, paddingLength, size, error);
  if (error) {
    return folly::makeUnexpected(quic::TransportErrorCode::INTERNAL_ERROR);
  }
  return size;
}

void MoQFrameWriter::setFetchGroupOrder(GroupOrder groupOrder) noexcept {
  fetchGroupOrder_ =
      groupOrder == GroupOrder::Default ? GroupOrder::OldestFirst : groupOrder;
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
      hasExtensions,
      /*beginsWithFirstObject=*/true);
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
  // Get immutable length, if any.
  const bool hasImmutableBlob = getDraftMajorVersion(*version_) >= 14 &&
      !extensions.getImmutableExtensions().empty();
  size_t immutableBodySize = 0;
  if (hasImmutableBlob) {
    immutableBodySize = calculateExtensionVectorSize(
        extensions.getImmutableExtensions(), error);
    if (error) {
      return;
    }
  }

  folly::IOBufQueue blockBuf{folly::IOBufQueue::cacheChainLength()};
  size_t blockSize = 0;

  auto writeImmutableContainer = [&](uint64_t typeToWrite) {
    writeVarint(blockBuf, typeToWrite, blockSize, error);
    if (error) {
      return;
    }
    writeVarint(blockBuf, immutableBodySize, blockSize, error);
    if (error) {
      return;
    }
    writeKeyValuePairs(
        blockBuf, extensions.getImmutableExtensions(), blockSize, error);
  };

  if (getDraftMajorVersion(*version_) >= 16) {
    auto sortedMutable =
        sortExtensionsByType(extensions.getMutableExtensions());
    uint64_t previousType = 0;
    bool containerPlaced = false;
    for (const auto& ext : sortedMutable) {
      // Do we need to prepend immutable before going further?
      if (hasImmutableBlob && !containerPlaced &&
          ext.type > kImmutableExtensionType) {
        writeImmutableContainer(kImmutableExtensionType - previousType);
        if (error) {
          return;
        }
        previousType = kImmutableExtensionType;
        containerPlaced = true;
      }

      // Write the current mutable extension.
      writeVarint(blockBuf, ext.type - previousType, blockSize, error);
      if (error) {
        return;
      }
      previousType = ext.type;
      if (ext.isOddType()) {
        auto dataLen =
            ext.arrayValue ? ext.arrayValue->computeChainDataLength() : 0;
        writeVarint(blockBuf, dataLen, blockSize, error);
        if (error) {
          return;
        }
        if (ext.arrayValue) {
          blockBuf.append(ext.arrayValue->clone());
          blockSize += dataLen;
        }
      } else {
        writeVarint(blockBuf, ext.intValue, blockSize, error);
        if (error) {
          return;
        }
      }
    }

    // Otherwise, immutable is at the end.
    if (hasImmutableBlob && !containerPlaced) {
      writeImmutableContainer(kImmutableExtensionType - previousType);
      if (error) {
        return;
      }
    }
  } else {
    // v<16: no delta encoding, mutable extensions then immutable.
    writeKeyValuePairs(
        blockBuf, extensions.getMutableExtensions(), blockSize, error);
    if (error) {
      return;
    }
    if (hasImmutableBlob) {
      writeImmutableContainer(kImmutableExtensionType);
      if (error) {
        return;
      }
    }
  }

  if (withLengthPrefix) {
    writeVarint(writeBuf, blockSize, size, error);
    if (error) {
      return;
    }
  }
  writeBuf.append(blockBuf.move());
  size += blockSize;
}

size_t MoQFrameWriter::calculateExtensionVectorSize(
    const std::vector<Extension>& extensions,
    bool& error) const noexcept {
  size_t size = 0;
  if (error) {
    return 0;
  }

  // For v16+ delta encoding, sort and compute delta-encoded type sizes
  // to match what writeKeyValuePairs actually writes.
  std::vector<Extension> sortedExtensions;
  const std::vector<Extension>* extensionsToUse = &extensions;
  if (getDraftMajorVersion(*version_) >= 16) {
    sortedExtensions = sortExtensionsByType(extensions);
    extensionsToUse = &sortedExtensions;
  }

  uint64_t previousType = 0;
  for (const auto& ext : *extensionsToUse) {
    uint64_t typeToSize = ext.type;
    if (getDraftMajorVersion(*version_) >= 16) {
      typeToSize = ext.type - previousType;
      previousType = ext.type;
    }
    size += getVarintSize(typeToSize, error);
    if (error) {
      return 0;
    }
    if (ext.type & 0x1) {
      // odd = length prefix
      auto dataLen =
          ext.arrayValue ? ext.arrayValue->computeChainDataLength() : 0;
      size += getVarintSize(dataLen, error);
      if (error) {
        return 0;
      }
      size += dataLen;
    } else {
      // even = single varint
      size += getVarintSize(ext.intValue, error);
      if (error) {
        return 0;
      }
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
  XCHECK(*version_)
      << "Version must be set before writing track request params";
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

void MoQFrameWriter::writeV18ParamValue(
    folly::IOBufQueue& writeBuf,
    const Parameter& param,
    size_t& size,
    bool& error) const noexcept {
  switch (paramEncodingV18(param.key)) {
    case ParamValueEncoding::Uint8: {
      if (param.asUint64 > 0xff) {
        error = true;
        return;
      }
      auto byte = static_cast<uint8_t>(param.asUint64);
      writeBuf.append(&byte, 1);
      size += 1;
      return;
    }
    case ParamValueEncoding::Varint:
      writeVarint(writeBuf, param.asUint64, size, error);
      return;
    case ParamValueEncoding::Location:
      if (!param.largestObject) {
        error = true;
        return;
      }
      writeVarint(writeBuf, param.largestObject->group, size, error);
      writeVarint(writeBuf, param.largestObject->object, size, error);
      return;
    case ParamValueEncoding::LengthPrefixed: {
      if (param.key ==
          folly::to_underlying(TrackRequestParamKey::SUBSCRIPTION_FILTER)) {
        folly::IOBufQueue tmpBuf{folly::IOBufQueue::cacheChainLength()};
        size_t tmpSize = 0;
        writeSubscriptionFilter(
            tmpBuf, param.asSubscriptionFilter, tmpSize, error);
        if (!error) {
          writeVarint(writeBuf, tmpSize, size, error);
          writeBuf.append(tmpBuf.move());
          size += tmpSize;
        }
      } else if (
          param.key ==
          folly::to_underlying(TrackRequestParamKey::TRACK_FILTER)) {
        // TRACK_FILTER (0x29) is a fork-local length-prefixed param; its value
        // lives in asTrackFilter, not asString. Mirror the draft-16 path in
        // writeParamValue so the v18 wire form round-trips (see parseTrackFilter
        // via parseV18ParamValue -> parseVariableParam).
        folly::IOBufQueue tmpBuf{folly::IOBufQueue::cacheChainLength()};
        size_t tmpSize = 0;
        writeTrackFilter(tmpBuf, param.asTrackFilter, tmpSize, error);
        if (!error) {
          writeVarint(writeBuf, tmpSize, size, error);
          writeBuf.append(tmpBuf.move());
          size += tmpSize;
        }
      } else {
        writeFixedString(writeBuf, param.asString, size, error);
      }
      return;
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
  const auto trackFilterKey =
      folly::to_underlying(TrackRequestParamKey::TRACK_FILTER);
  const auto largestObjectKey =
      folly::to_underlying(TrackRequestParamKey::LARGEST_OBJECT);
  const auto expiresKey = folly::to_underlying(TrackRequestParamKey::EXPIRES);
  const auto groupOrderKey =
      folly::to_underlying(TrackRequestParamKey::GROUP_ORDER);

  if (version_.has_value() && getDraftMajorVersion(*version_) >= 18) {
    writeV18ParamValue(writeBuf, param, size, error);
    return;
  }

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
  } else if (param.key == trackFilterKey) {
    // Track filter key is odd (0x29), so it needs a length prefix.
    // Write to a temporary buffer to compute the length first.
    folly::IOBufQueue tmpBuf{folly::IOBufQueue::cacheChainLength()};
    size_t tmpSize = 0;
    writeTrackFilter(tmpBuf, param.asTrackFilter, tmpSize, error);
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
    XCHECK(
        param.key !=
            folly::to_underlying(TrackRequestParamKey::DELIVERY_TIMEOUT) ||
        param.asUint64 != 0 || !version_.has_value() ||
        getDraftMajorVersion(*version_) >= 17)
        << "Cannot write a DELIVERY_TIMEOUT of 0 for draft versions <= 16";
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
  XCHECK(version_.has_value())
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

  // Write end group for AbsoluteRange.
  // In draft-18+, EndGroup is encoded as a delta from StartLocation.group.
  // In draft-17 and earlier, EndGroup is an absolute group number.
  if (filter.filterType == LocationType::AbsoluteRange) {
    if (filter.endGroup.has_value() && filter.location.has_value()) {
      uint64_t toWrite = *filter.endGroup;
      if (getDraftMajorVersion(*version_) >= 18) {
        if (*filter.endGroup < filter.location->group) {
          error = true;
          return;
        }
        toWrite = *filter.endGroup - filter.location->group;
      }
      writeVarint(writeBuf, toWrite, size, error);
    } else {
      error = true;
    }
  }
}

void writeTrackFilter(
    folly::IOBufQueue& writeBuf,
    const TrackFilter& filter,
    size_t& size,
    bool& error) noexcept {
  // Write propertyType
  writeVarint(writeBuf, filter.propertyType, size, error);
  // Write maxSelected (N)
  writeVarint(writeBuf, filter.maxSelected, size, error);
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

  XCHECK(!hasLength || objectHeader.status == ObjectStatus::NORMAL)
      << "non-zero length objects require NORMAL status";
  if (objectHeader.status != ObjectStatus::NORMAL || !hasLength) {
    XCHECK(!objectPayload || objectPayload->computeChainDataLength() == 0)
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
  const bool useFetchObjectDeltas = getDraftMajorVersion(*version_) >= 18;

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

  if (!useFetchObjectDeltas) {
    // Bit 2 (0x04): Object ID field is present.
    if (!previousObjectID_.has_value() ||
        objectHeader.id != previousObjectID_.value() + 1) {
      flags |=
          folly::to_underlying(FetchHeaderSerializationBits::OBJECT_ID_BITMASK);
    }

    // Bit 3 (0x08): Group ID field is present.
    if (!previousFetchGroup_.has_value() ||
        objectHeader.group != previousFetchGroup_.value()) {
      flags |=
          folly::to_underlying(FetchHeaderSerializationBits::GROUP_ID_BITMASK);
    }
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

  FetchObjectDeltaFields fetchObjectDeltaFields;

  if (useFetchObjectDeltas) {
    auto computedFields = computeFetchObjectDeltaFieldsForWrite(
        objectHeader, previousFetchGroup_, previousObjectID_, fetchGroupOrder_);
    if (!computedFields.has_value()) {
      error = true;
      return;
    }
    fetchObjectDeltaFields = computedFields.value();

    if (fetchObjectDeltaFields.groupIDDelta.has_value()) {
      flags |=
          folly::to_underlying(FetchHeaderSerializationBits::GROUP_ID_BITMASK);
    }
    if (fetchObjectDeltaFields.objectIDDelta.has_value()) {
      flags |=
          folly::to_underlying(FetchHeaderSerializationBits::OBJECT_ID_BITMASK);
    }
  }

  // Write Serialization Flags - single byte for v15, varint for v16+
  if (getDraftMajorVersion(*version_) >= 16) {
    writeVarint(writeBuf, flags, size, error);
  } else {
    writeBuf.append(&flags, 1);
    size += 1;
  }

  // Write Group ID Delta in draft 18+, otherwise Group ID.
  if (flags &
      folly::to_underlying(FetchHeaderSerializationBits::GROUP_ID_BITMASK)) {
    writeVarint(
        writeBuf,
        useFetchObjectDeltas ? *fetchObjectDeltaFields.groupIDDelta
                             : objectHeader.group,
        size,
        error);
  }

  // Write Subgroup ID if mode is 0x03
  if ((flags &
       folly::to_underlying(
           FetchHeaderSerializationBits::SUBGROUP_MODE_BITMASK)) ==
      folly::to_underlying(
          FetchHeaderSerializationBits::SUBGROUP_MODE_BITMASK)) {
    writeVarint(writeBuf, objectHeader.subgroup, size, error);
  }

  // Write Object ID Delta in draft 18+, otherwise Object ID.
  if (flags &
      folly::to_underlying(FetchHeaderSerializationBits::OBJECT_ID_BITMASK)) {
    writeVarint(
        writeBuf,
        useFetchObjectDeltas ? *fetchObjectDeltaFields.objectIDDelta
                             : objectHeader.id,
        size,
        error);
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
  XCHECK(!hasLength || objectHeader.status == ObjectStatus::NORMAL)
      << "non-zero length objects require NORMAL status";
  if (hasLength) {
    writeVarint(writeBuf, *objectHeader.length, size, error);
    writeBuf.append(std::move(objectPayload));
    // TODO: adjust size?
  } else {
    XCHECK(!objectPayload || objectPayload->computeChainDataLength() == 0)
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
  XCHECK(version_.has_value())
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

    auto newGroupRequestValue = getFirstIntParam(
        subscribeRequest.params, TrackRequestParamKey::NEW_GROUP_REQUEST);
    if (newGroupRequestValue.has_value()) {
      Parameter newGroupRequestParam;
      newGroupRequestParam.key =
          folly::to_underlying(TrackRequestParamKey::NEW_GROUP_REQUEST);
      newGroupRequestParam.asUint64 = *newGroupRequestValue;
      requestSpecificParams.push_back(newGroupRequestParam);
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
  XCHECK(version_.has_value())
      << "Version needs to be set to write request update";
  size_t size = 0;
  bool error = false;
  auto sizePtr = writeFrameHeader(writeBuf, FrameType::SUBSCRIBE_UPDATE, error);
  writeVarint(writeBuf, update.requestID.value, size, error);
  if (getDraftMajorVersion(*version_) >= 14 &&
      getDraftMajorVersion(*version_) < 18) {
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

    auto newGroupRequestValue = getFirstIntParam(
        update.params, TrackRequestParamKey::NEW_GROUP_REQUEST);
    if (newGroupRequestValue.has_value()) {
      Parameter newGroupRequestParam;
      newGroupRequestParam.key =
          folly::to_underlying(TrackRequestParamKey::NEW_GROUP_REQUEST);
      newGroupRequestParam.asUint64 = *newGroupRequestValue;
      requestSpecificParams.push_back(newGroupRequestParam);
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
  XCHECK(version_.has_value())
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
  // Draft 18+: requestID is implicit from the bidi request stream context.
  if (getDraftMajorVersion(*version_) < 18) {
    writeVarint(writeBuf, subscribeOk.requestID.value, size, error);
  }
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

    // Add GROUP_ORDER parameter (only if non-default, v15 only)
    if (getDraftMajorVersion(*version_) == 15 &&
        subscribeOk.groupOrder != GroupOrder::Default) {
      Parameter groupOrderParam;
      groupOrderParam.key =
          folly::to_underlying(TrackRequestParamKey::GROUP_ORDER);
      groupOrderParam.asUint64 = folly::to_underlying(subscribeOk.groupOrder);
      requestSpecificParams.push_back(groupOrderParam);
    }

    // Add LARGEST_OBJECT parameter (v16+ only, replaces fixed contentExists)
    if (getDraftMajorVersion(*version_) >= 16 &&
        subscribeOk.largest.has_value()) {
      requestSpecificParams.emplace_back(
          folly::to_underlying(TrackRequestParamKey::LARGEST_OBJECT),
          subscribeOk.largest);
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
  XCHECK(version_.has_value())
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
  XCHECK(version_.has_value())
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
  XCHECK(version_.has_value())
      << "Version needs to be set to write unsubscribe";
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
  XCHECK(version_.has_value())
      << "Version needs to be set to write subscribe done";
  size_t size = 0;
  bool error = false;
  auto sizePtr = writeFrameHeader(writeBuf, FrameType::PUBLISH_DONE, error);
  // Draft 18+: requestID is implicit from the bidi request stream context.
  if (getDraftMajorVersion(*version_) < 18) {
    writeVarint(writeBuf, publishDone.requestID.value, size, error);
  }
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
  XCHECK(version_.has_value()) << "Version needs to be set to write publish";
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
    if (getDraftMajorVersion(*version_) == 15 &&
        publish.groupOrder != GroupOrder::Default) {
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
      requestSpecificParams.emplace_back(
          folly::to_underlying(TrackRequestParamKey::LARGEST_OBJECT),
          publish.largest);
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
  XCHECK(version_.has_value()) << "Version needs to be set to write publish ok";
  auto majorVersion = getDraftMajorVersion(*version_);
  if (majorVersion >= 18) {
    return writeRequestOk(
        writeBuf,
        RequestOk::fromPublishOk(publishOk, majorVersion),
        FrameType::PUBLISH_OK);
  }

  size_t size = 0;
  bool error = false;
  auto sizePtr = writeFrameHeader(writeBuf, FrameType::PUBLISH_OK, error);
  writeVarint(writeBuf, publishOk.requestID.value, size, error);

  if (majorVersion < 15) {
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
  if (majorVersion >= 15) {
    requestSpecificParams = getPublishOkRequestSpecificParams(publishOk);
  } else {
    writeVarint(
        writeBuf,
        getLocationTypeValue(publishOk.locType, majorVersion),
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
  XCHECK(version_.has_value())
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
  XCHECK(version_.has_value()) << "Version needs to be set to write request ok";
  size_t size = 0;
  bool error = false;
  // Preserve the semantic frame type passed by the caller; we still need it
  // below to decide whether Track Properties are valid (draft 18+).
  const FrameType semanticFrameType = frameType;
  if (getDraftMajorVersion(*version_) > 14) {
    frameType = FrameType::REQUEST_OK;
  }
  auto sizePtr = writeFrameHeader(writeBuf, frameType, error);
  // Draft 18+: requestID is implicit from the bidi request stream context.
  if (getDraftMajorVersion(*version_) < 18) {
    writeVarint(writeBuf, requestOk.requestID.value, size, error);
  }
  if (getDraftMajorVersion(*version_) > 14) {
    if (semanticFrameType == FrameType::SUBSCRIBE_NAMESPACE_OK &&
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
  // Draft 18+: Track Properties (bare key-value pairs, no length prefix) are
  // only allowed for TRACK_STATUS_OK responses.
  if (getDraftMajorVersion(*version_) >= 18) {
    if (semanticFrameType == FrameType::TRACK_STATUS_OK) {
      writeExtensions(
          writeBuf,
          requestOk.trackProperties,
          size,
          error,
          /*withLengthPrefix=*/false);
    } else if (!requestOk.trackProperties.empty()) {
      // Caller populated Track Properties for a frame type that requires them
      // to be empty.
      return folly::makeUnexpected(
          quic::TransportErrorCode::PROTOCOL_VIOLATION);
    }
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
  XCHECK(version_.has_value())
      << "Version needs to be set to write publishNamespaceDone";
  size_t size = 0;
  bool error = false;
  auto sizePtr =
      writeFrameHeader(writeBuf, FrameType::PUBLISH_NAMESPACE_DONE, error);

  if (getDraftMajorVersion(*version_) >= 16) {
    // v16+: Write Request ID
    XCHECK(publishNamespaceDone.requestID.has_value())
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
  XCHECK(version_.has_value())
      << "Version needs to be set to write publishNamespace cancel";
  size_t size = 0;
  bool error = false;
  auto sizePtr =
      writeFrameHeader(writeBuf, FrameType::PUBLISH_NAMESPACE_CANCEL, error);

  if (getDraftMajorVersion(*version_) >= 16) {
    // v16+: Write Request ID
    XCHECK(publishNamespaceCancel.requestID.has_value())
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
  XCHECK(version_.has_value())
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
  XCHECK(version_.has_value())
      << "version_ needs to be set to write TrackStatus";

  size_t size = 0;
  bool error = false;

  if (getDraftMajorVersion(*version_) >= 15) {
    auto requestOk = RequestOk::fromTrackStatusOk(trackStatusOk);
    return writeRequestOk(writeBuf, requestOk, FrameType::TRACK_STATUS_OK);
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
  XCHECK(version_.has_value()) << "Version needs to be set to write Goaway";
  size_t size = 0;
  bool error = false;
  auto sizePtr = writeFrameHeader(writeBuf, FrameType::GOAWAY, error);
  writeFixedString(writeBuf, goaway.newSessionUri, size, error);
  if (getDraftMajorVersion(*version_) >= 18) {
    writeVarint(writeBuf, goaway.timeout, size, error);
    // Per draft 18, Request ID is present only when GOAWAY is sent on the
    // control stream. Callers signal request-stream GOAWAY by leaving
    // requestID unset.
    if (goaway.requestID.has_value()) {
      writeVarint(writeBuf, goaway.requestID->value, size, error);
    }
  }
  writeSize(sizePtr, size, error, *version_);
  if (error) {
    return folly::makeUnexpected(quic::TransportErrorCode::INTERNAL_ERROR);
  }
  return size;
}

WriteResult MoQFrameWriter::writeSubscribeNamespace(
    folly::IOBufQueue& writeBuf,
    const SubscribeNamespace& subscribeNamespace) const noexcept {
  XCHECK(version_.has_value())
      << "Version needs to be set to write subscribeNamespace";
  auto majorVersion = getDraftMajorVersion(*version_);
  size_t size = 0;
  bool error = false;
  // Draft 18 renumbered SUBSCRIBE_NAMESPACE on the wire from 0x11 to 0x50;
  // pick the right wire-level enumerator for the negotiated version. Both
  // serialise the same struct body.
  auto wireType = (majorVersion >= 18) ? FrameType::SUBSCRIBE_NAMESPACE
                                       : FrameType::LEGACY_SUBSCRIBE_NAMESPACE;
  auto sizePtr = writeFrameHeader(writeBuf, wireType, error);
  writeVarint(writeBuf, subscribeNamespace.requestID.value, size, error);
  writeTrackNamespace(
      writeBuf, subscribeNamespace.trackNamespacePrefix, size, error);

  // The SUBSCRIBE_NAMESPACE message has an "options" field only in drafts
  // 16 and 17.
  if (majorVersion >= 16 && majorVersion < 18) {
    writeVarint(
        writeBuf,
        folly::to_underlying(subscribeNamespace.options),
        size,
        error);
  }

  // FORWARD is only present as a parameter for drafts 15, 16, and 17.
  std::vector<Parameter> requestSpecificParams;
  if (majorVersion >= 15 && majorVersion < 18 && !subscribeNamespace.forward) {
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

WriteResult MoQFrameWriter::writeSubscribeTracks(
    folly::IOBufQueue& writeBuf,
    const SubscribeTracks& subscribeTracks) const noexcept {
  XCHECK(version_.has_value())
      << "Version needs to be set to write subscribeTracks";
  XCHECK_GE(getDraftMajorVersion(*version_), 18u)
      << "SUBSCRIBE_TRACKS is draft 18+ only";
  size_t size = 0;
  bool error = false;
  auto sizePtr = writeFrameHeader(writeBuf, FrameType::SUBSCRIBE_TRACKS, error);
  writeVarint(writeBuf, subscribeTracks.requestID.value, size, error);
  writeTrackNamespace(
      writeBuf, subscribeTracks.trackNamespacePrefix, size, error);

  // Forward is carried as a FORWARD parameter (only emitted when false; the
  // default is true and is signaled by the parameter's absence).
  std::vector<Parameter> requestSpecificParams;
  if (!subscribeTracks.forward) {
    Parameter forwardParam;
    forwardParam.key = folly::to_underlying(TrackRequestParamKey::FORWARD);
    forwardParam.asUint64 = 0;
    requestSpecificParams.push_back(forwardParam);
  }

  writeTrackRequestParams(
      writeBuf, subscribeTracks.params, requestSpecificParams, size, error);
  writeSize(sizePtr, size, error, *version_);
  if (error) {
    return folly::makeUnexpected(quic::TransportErrorCode::INTERNAL_ERROR);
  }
  return size;
}

WriteResult MoQFrameWriter::writePublishBlocked(
    folly::IOBufQueue& writeBuf,
    const PublishBlocked& publishBlocked) const noexcept {
  XCHECK(version_.has_value())
      << "Version needs to be set to write publishBlocked";
  XCHECK_GE(getDraftMajorVersion(*version_), 18u)
      << "PUBLISH_BLOCKED is draft 18+ only";
  size_t size = 0;
  bool error = false;
  auto sizePtr = writeFrameHeader(writeBuf, FrameType::PUBLISH_BLOCKED, error);
  writeTrackNamespace(
      writeBuf, publishBlocked.trackNamespaceSuffix, size, error);
  writeFixedString(writeBuf, publishBlocked.trackName, size, error);
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
  XCHECK(version_.has_value())
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
  XCHECK(version_.has_value()) << "Version needs to be set to write namespace";
  XCHECK_GE(getDraftMajorVersion(*version_), 16)
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
  XCHECK(version_.has_value())
      << "Version needs to be set to write namespace done";
  XCHECK_GE(getDraftMajorVersion(*version_), 16)
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
  XCHECK(version_.has_value()) << "Version needs to be set to write fetch";
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
    XCHECK(joining);

    writeVarint(
        writeBuf, folly::to_underlying(joining->fetchType), size, error);
    XCHECK(joining->joiningRequestID.has_value())
        << "joiningRequestID must be resolved before serialization";
    writeVarint(writeBuf, joining->joiningRequestID->value, size, error);
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
  XCHECK(version_.has_value())
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
  XCHECK(version_.has_value()) << "Version needs to be set to write fetch ok";
  size_t size = 0;
  bool error = false;
  auto sizePtr = writeFrameHeader(writeBuf, FrameType::FETCH_OK, error);
  // Draft 18+: requestID is implicit from the bidi request stream context.
  if (getDraftMajorVersion(*version_) < 18) {
    writeVarint(writeBuf, fetchOk.requestID.value, size, error);
  }
  if (getDraftMajorVersion(*version_) < 15) {
    auto order = folly::to_underlying(fetchOk.groupOrder);
    writeBuf.append(&order, 1);
    size += 1;
  }
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
  XCHECK(version_.has_value())
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

  if (requestError.errorCode == RequestErrorCode::REDIRECT) {
    if (getDraftMajorVersion(*version_) < 18) {
      XLOG(ERR) << "REDIRECT errorCode is only valid for draft 18+, version="
                << *version_;
      return folly::makeUnexpected(quic::TransportErrorCode::INTERNAL_ERROR);
    }
    if (!requestError.redirect) {
      XLOG(ERR) << "REDIRECT errorCode without a Redirect struct";
      return folly::makeUnexpected(quic::TransportErrorCode::INTERNAL_ERROR);
    }
  }

  size_t size = 0;
  bool error = false;
  if (getDraftMajorVersion(*version_) > 14) {
    frameType = FrameType::REQUEST_ERROR;
  }
  auto sizePtr = writeFrameHeader(writeBuf, frameType, error);

  // Draft 18+: requestID is implicit from the bidi request stream context.
  if (getDraftMajorVersion(*version_) < 18) {
    writeVarint(writeBuf, requestError.requestID.value, size, error);
  }
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

  if (getDraftMajorVersion(*version_) >= 18 &&
      requestError.errorCode == RequestErrorCode::REDIRECT) {
    const Redirect& redirect = *requestError.redirect;
    writeFixedString(writeBuf, redirect.connectUri, size, error);
    writeFullTrackName(writeBuf, redirect.fullTrackName, size, error);
  }

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
  auto majorVersion = getDraftMajorVersion(version);
  if (majorVersion < 18 && (streamType & SG_FIRST_OBJECT)) {
    return false;
  }
  uint64_t max = 0x3D;
  if (majorVersion < 15) {
    max = 0x1D;
  } else if (majorVersion >= 18) {
    max = 0x7D;
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

std::optional<FrameType> getFrameType(
    const folly::IOBufQueue& readBuf,
    std::optional<uint64_t> version) {
  if (readBuf.empty()) {
    return std::nullopt;
  }
  folly::io::Cursor cursor(readBuf.front());
  if (version && getDraftMajorVersion(*version) >= 17) {
    auto frameType = decodeMoQVarint(cursor);
    if (!frameType) {
      return std::nullopt;
    }
    return static_cast<FrameType>(frameType->first);
  }
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

  checkAndAddIfPresent(
      kDynamicGroupsExtensionType,
      extensions.getIntExtension(kDynamicGroupsExtensionType));

  // Note: GROUP_ORDER is intentionally not here — it's handled per-message
  // as a request-specific param for v15 and as a fixed field for < v15
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
      case kDynamicGroupsExtensionType:
        extensions.insertMutableExtension(
            Extension{kDynamicGroupsExtensionType, param.asUint64});
        break;
      default:
        // Other params are not track properties, skip
        break;
    }
  }
}

} // namespace moxygen

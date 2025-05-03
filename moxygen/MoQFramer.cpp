/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/MoQFramer.h"
#include <folly/lang/Bits.h>
#include <folly/logging/xlog.h>

namespace {
constexpr uint64_t kMaxExtensions = 16;
constexpr uint64_t kMaxExtensionLength = 1024;

bool isDraftVariant(uint64_t version) {
  return (version & 0x00ff0000);
}

uint64_t getLocationTypeValue(
    moxygen::LocationType locationType,
    uint64_t version) {
  if (locationType != moxygen::LocationType::LatestGroup) {
    return folly::to_underlying(locationType);
  }

  if (moxygen::getDraftMajorVersion(version) < 11) {
    // In draft < 11, LatestGroup maps to 1
    return 1;
  } else {
    return folly::to_underlying(locationType);
  }
}

// Used below draft 11
enum class LegacyTrackRequestParamKey : uint64_t {
  AUTHORIZATION = 2,
  DELIVERY_TIMEOUT = 3,
  MAX_CACHE_DURATION = 4,
};

// Used in draft 11 and above
enum class TrackRequestParamKey : uint64_t {
  AUTHORIZATION = 1,
  DELIVERY_TIMEOUT = 2,
  MAX_CACHE_DURATION = 4,
};
} // namespace

namespace moxygen {

uint64_t getDraftMajorVersion(uint64_t version) {
  if (isDraftVariant(version)) {
    return (version & 0x00ff0000) >> 16;
  } else {
    return (version & 0x0000ffff);
  }
}

uint64_t getAuthorizationParamKey(uint64_t version) {
  if (getDraftMajorVersion(version) >= 11) {
    return folly::to_underlying(TrackRequestParamKey::AUTHORIZATION);
  } else {
    return folly::to_underlying(LegacyTrackRequestParamKey::AUTHORIZATION);
  }
}

uint64_t getDeliveryTimeoutParamKey(uint64_t version) {
  if (getDraftMajorVersion(version) >= 11) {
    return folly::to_underlying(TrackRequestParamKey::DELIVERY_TIMEOUT);
  } else {
    return folly::to_underlying(LegacyTrackRequestParamKey::DELIVERY_TIMEOUT);
  }
}

uint64_t getMaxCacheDurationParamKey(uint64_t version) {
  if (getDraftMajorVersion(version) >= 11) {
    return folly::to_underlying(TrackRequestParamKey::MAX_CACHE_DURATION);
  } else {
    return folly::to_underlying(LegacyTrackRequestParamKey::MAX_CACHE_DURATION);
  }
}

folly::Expected<std::string, ErrorCode> parseFixedString(
    folly::io::Cursor& cursor,
    size_t& length) {
  auto strLength = quic::decodeQuicInteger(cursor, length);
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

folly::Expected<folly::Unit, ErrorCode> parseSetupParams(
    folly::io::Cursor& cursor,
    size_t& length,
    size_t numParams,
    std::vector<SetupParameter>& params) {
  for (auto i = 0u; i < numParams; i++) {
    auto key = quic::decodeQuicInteger(cursor, length);
    if (!key) {
      return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
    }
    length -= key->second;
    SetupParameter p;
    p.key = key->first;
    if (p.key == folly::to_underlying(SetupKey::MAX_SUBSCRIBE_ID)) {
      auto res = quic::decodeQuicInteger(cursor);
      if (!res) {
        return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
      }
      length -= res->second;
      res = quic::decodeQuicInteger(cursor, res->first);
      if (!res) {
        return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
      }
      p.asUint64 = res->first;
      length -= res->second;
    } else {
      auto res = parseFixedString(cursor, length);
      if (!res) {
        return folly::makeUnexpected(res.error());
      }
      p.asString = std::move(res.value());
    }
    params.emplace_back(std::move(p));
  }
  if (length > 0) {
    return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
  }
  return folly::unit;
}

folly::Expected<ClientSetup, ErrorCode> parseClientSetup(
    folly::io::Cursor& cursor,
    size_t length) noexcept {
  ClientSetup clientSetup;
  auto numVersions = quic::decodeQuicInteger(cursor, length);
  if (!numVersions) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= numVersions->second;
  for (auto i = 0ul; i < numVersions->first; i++) {
    auto version = quic::decodeQuicInteger(cursor, length);
    if (!version) {
      return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
    }
    clientSetup.supportedVersions.push_back(version->first);
    length -= version->second;
  }
  auto numParams = quic::decodeQuicInteger(cursor, length);
  if (!numParams) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= numParams->second;
  auto res =
      parseSetupParams(cursor, length, numParams->first, clientSetup.params);
  if (res.hasError()) {
    return folly::makeUnexpected(res.error());
  }
  if (length > 0) {
    return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
  }
  return clientSetup;
}

folly::Expected<ServerSetup, ErrorCode> parseServerSetup(
    folly::io::Cursor& cursor,
    size_t length) noexcept {
  ServerSetup serverSetup;
  auto version = quic::decodeQuicInteger(cursor, length);
  if (!version) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= version->second;
  serverSetup.selectedVersion = version->first;
  auto numParams = quic::decodeQuicInteger(cursor, length);
  if (!numParams) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= numParams->second;
  auto res =
      parseSetupParams(cursor, length, numParams->first, serverSetup.params);
  if (res.hasError()) {
    return folly::makeUnexpected(res.error());
  }
  if (length > 0) {
    return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
  }
  return serverSetup;
}

folly::Expected<SubscribeID, ErrorCode> MoQFrameParser::parseFetchHeader(
    folly::io::Cursor& cursor) const noexcept {
  auto subscribeID = quic::decodeQuicInteger(cursor);
  if (!subscribeID) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  return SubscribeID(subscribeID->first);
}

folly::Expected<ObjectHeader, ErrorCode>
MoQFrameParser::parseDatagramObjectHeader(
    folly::io::Cursor& cursor,
    StreamType streamType,
    size_t& length) const noexcept {
  ObjectHeader objectHeader;
  auto trackAlias = quic::decodeQuicInteger(cursor, length);
  if (!trackAlias) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= trackAlias->second;
  objectHeader.trackIdentifier = TrackAlias(trackAlias->first);
  auto group = quic::decodeQuicInteger(cursor, length);
  if (!group) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= group->second;
  objectHeader.group = group->first;
  auto id = quic::decodeQuicInteger(cursor, length);
  if (!id) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= id->second;
  objectHeader.id = id->first;
  if (!cursor.canAdvance(1)) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  objectHeader.priority = cursor.readBE<uint8_t>();
  length -= 1;
  auto ext = parseExtensions(cursor, length, objectHeader);
  if (!ext) {
    return folly::makeUnexpected(ext.error());
  }

  if (streamType == StreamType::OBJECT_DATAGRAM_STATUS) {
    auto status = quic::decodeQuicInteger(cursor, length);
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
    CHECK(streamType == StreamType::OBJECT_DATAGRAM);
    objectHeader.status = ObjectStatus::NORMAL;
    objectHeader.length = length;
  }
  return objectHeader;
}

folly::Expected<ObjectHeader, ErrorCode> MoQFrameParser::parseSubgroupHeader(
    folly::io::Cursor& cursor) const noexcept {
  auto length = cursor.totalLength();
  ObjectHeader objectHeader;
  objectHeader.group = std::numeric_limits<uint64_t>::max(); // unset
  objectHeader.id = std::numeric_limits<uint64_t>::max();    // unset
  auto trackAlias = quic::decodeQuicInteger(cursor, length);
  if (!trackAlias) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= trackAlias->second;
  objectHeader.trackIdentifier = TrackAlias(trackAlias->first);
  auto group = quic::decodeQuicInteger(cursor, length);
  if (!group) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= group->second;
  objectHeader.group = group->first;
  auto subgroup = quic::decodeQuicInteger(cursor, length);
  if (!subgroup) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  objectHeader.subgroup = subgroup->first;
  length -= subgroup->second;
  if (cursor.canAdvance(1)) {
    objectHeader.priority = cursor.readBE<uint8_t>();
    length -= 1;
  } else {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  return objectHeader;
}

folly::Expected<folly::Unit, ErrorCode>
MoQFrameParser::parseObjectStatusAndLength(
    folly::io::Cursor& cursor,
    size_t length,
    ObjectHeader& objectHeader) const noexcept {
  auto payloadLength = quic::decodeQuicInteger(cursor, length);
  if (!payloadLength) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= payloadLength->second;
  objectHeader.length = payloadLength->first;

  if (objectHeader.length == 0) {
    auto objectStatus = quic::decodeQuicInteger(cursor, length);
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

  auto group = quic::decodeQuicInteger(cursor, length);
  if (!group) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= group->second;
  objectHeader.group = group->first;

  auto subgroup = quic::decodeQuicInteger(cursor, length);
  if (!subgroup) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= subgroup->second;
  objectHeader.subgroup = subgroup->first;

  auto id = quic::decodeQuicInteger(cursor, length);
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
    const ObjectHeader& headerTemplate) const noexcept {
  // TODO get rid of this
  auto length = cursor.totalLength();
  ObjectHeader objectHeader = headerTemplate;
  auto id = quic::decodeQuicInteger(cursor, length);
  if (!id) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= id->second;
  objectHeader.id = id->first;

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

folly::Expected<folly::Unit, ErrorCode> MoQFrameParser::parseTrackRequestParams(
    folly::io::Cursor& cursor,
    size_t& length,
    size_t numParams,
    std::vector<TrackRequestParameter>& params) const noexcept {
  CHECK(version_.hasValue())
      << "The version must be set before parsing track request params";
  for (auto i = 0u; i < numParams; i++) {
    TrackRequestParameter p;
    auto key = quic::decodeQuicInteger(cursor, length);
    if (!key) {
      return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
    }
    length -= key->second;
    p.key = key->first;

    if (p.key == getAuthorizationParamKey(*version_)) {
      auto res = parseFixedString(cursor, length);
      if (!res) {
        return folly::makeUnexpected(res.error());
      }
      p.asString = std::move(res.value());
    } else {
      auto res = quic::decodeQuicInteger(cursor, length);
      if (!res) {
        return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
      }
      length -= res->second;
      res = quic::decodeQuicInteger(cursor, res->first);
      if (!res) {
        return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
      }
      length -= res->second;
      p.asUint64 = res->first;
    }
    params.emplace_back(std::move(p));
  }
  return folly::unit;
}

folly::Expected<SubscribeRequest, ErrorCode>
MoQFrameParser::parseSubscribeRequest(folly::io::Cursor& cursor, size_t length)
    const noexcept {
  CHECK(version_.hasValue())
      << "The version must be set before parsing a subscribe request";
  SubscribeRequest subscribeRequest;
  auto subscribeID = quic::decodeQuicInteger(cursor, length);
  if (!subscribeID) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= subscribeID->second;
  subscribeRequest.subscribeID = subscribeID->first;
  auto trackAlias = quic::decodeQuicInteger(cursor, length);
  if (!trackAlias) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= trackAlias->second;
  subscribeRequest.trackAlias = trackAlias->first;
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
  if (getDraftMajorVersion(*version_) >= 11) {
    if (length < 1) {
      return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
    }
    uint8_t forwardFlag = cursor.readBE<uint8_t>();
    if (forwardFlag > 1) {
      return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
    }
    subscribeRequest.forward = (forwardFlag == 1);
    length--;
  }
  auto locType = quic::decodeQuicInteger(cursor, length);
  if (!locType) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  // LocationType == 1 was present in draft 8 and below as LatestGroup. Draft 11
  // and above have LocationType::NextGroupStart. Draft 9 and 10 don't have
  // LocationType == 1, but we treat it as LatestGroup.
  if (locType->first == folly::to_underlying(LocationType::NextGroupStart) &&
      getDraftMajorVersion(*version_) < 11) {
    locType->first = folly::to_underlying(LocationType::LatestGroup);
  }
  switch (locType->first) {
    case folly::to_underlying(LocationType::LatestObject):
    case folly::to_underlying(LocationType::LatestGroup):
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
    auto endGroup = quic::decodeQuicInteger(cursor, length);
    if (!endGroup) {
      return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
    }
    subscribeRequest.endGroup = endGroup->first;
    length -= endGroup->second;
  }
  auto numParams = quic::decodeQuicInteger(cursor, length);
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
  auto subscribeID = quic::decodeQuicInteger(cursor, length);
  if (!subscribeID) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  subscribeUpdate.subscribeID = subscribeID->first;
  length -= subscribeID->second;
  auto start = parseAbsoluteLocation(cursor, length);
  if (!start) {
    return folly::makeUnexpected(start.error());
  }
  subscribeUpdate.start = start.value();

  auto endGroup = quic::decodeQuicInteger(cursor, length);
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

  if (getDraftMajorVersion(*version_) >= 11) {
    if (length < 2) {
      return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
    }
    uint8_t forwardFlag = cursor.readBE<uint8_t>();
    if (forwardFlag > 1) {
      return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
    }
    subscribeUpdate.forward = (forwardFlag == 1);
    length--;
  }

  auto numParams = quic::decodeQuicInteger(cursor, length);
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
  auto subscribeID = quic::decodeQuicInteger(cursor, length);
  if (!subscribeID) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= subscribeID->second;
  subscribeOk.subscribeID = subscribeID->first;

  auto expires = quic::decodeQuicInteger(cursor, length);
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
    subscribeOk.latest = *res;
  }
  auto numParams = quic::decodeQuicInteger(cursor, length);
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

folly::Expected<SubscribeError, ErrorCode> MoQFrameParser::parseSubscribeError(
    folly::io::Cursor& cursor,
    size_t length) const noexcept {
  SubscribeError subscribeError;
  auto subscribeID = quic::decodeQuicInteger(cursor, length);
  if (!subscribeID) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= subscribeID->second;
  subscribeError.subscribeID = subscribeID->first;

  auto errorCode = quic::decodeQuicInteger(cursor, length);
  if (!errorCode) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= errorCode->second;
  subscribeError.errorCode = SubscribeErrorCode(errorCode->first);

  auto reas = parseFixedString(cursor, length);
  if (!reas) {
    return folly::makeUnexpected(reas.error());
  }
  subscribeError.reasonPhrase = std::move(reas.value());

  auto retryAlias = quic::decodeQuicInteger(cursor, length);
  if (!retryAlias) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= retryAlias->second;
  if (subscribeError.errorCode == SubscribeErrorCode::RETRY_TRACK_ALIAS) {
    subscribeError.retryAlias = retryAlias->first;
  }
  if (length > 0) {
    return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
  }
  return subscribeError;
}

folly::Expected<Unsubscribe, ErrorCode> MoQFrameParser::parseUnsubscribe(
    folly::io::Cursor& cursor,
    size_t length) const noexcept {
  Unsubscribe unsubscribe;
  auto subscribeID = quic::decodeQuicInteger(cursor, length);
  if (!subscribeID) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= subscribeID->second;
  unsubscribe.subscribeID = subscribeID->first;
  if (length > 0) {
    return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
  }
  return unsubscribe;
}

folly::Expected<SubscribeDone, ErrorCode> MoQFrameParser::parseSubscribeDone(
    folly::io::Cursor& cursor,
    size_t length) const noexcept {
  SubscribeDone subscribeDone;
  auto subscribeID = quic::decodeQuicInteger(cursor, length);
  if (!subscribeID) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= subscribeID->second;
  subscribeDone.subscribeID = subscribeID->first;

  auto statusCode = quic::decodeQuicInteger(cursor, length);
  if (!statusCode) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= statusCode->second;
  subscribeDone.statusCode = SubscribeDoneStatusCode(statusCode->first);

  auto streamCount = quic::decodeQuicInteger(cursor, length);
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

folly::Expected<Announce, ErrorCode> MoQFrameParser::parseAnnounce(
    folly::io::Cursor& cursor,
    size_t length) const noexcept {
  Announce announce;
  auto res = parseFixedTuple(cursor, length);
  if (!res) {
    return folly::makeUnexpected(res.error());
  }
  announce.trackNamespace = TrackNamespace(std::move(res.value()));
  auto numParams = quic::decodeQuicInteger(cursor, length);
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
  AnnounceOk announceOk;
  auto res = parseFixedTuple(cursor, length);
  if (!res) {
    return folly::makeUnexpected(res.error());
  }
  announceOk.trackNamespace = TrackNamespace(std::move(res.value()));
  if (length > 0) {
    return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
  }
  return announceOk;
}

folly::Expected<AnnounceError, ErrorCode> MoQFrameParser::parseAnnounceError(
    folly::io::Cursor& cursor,
    size_t length) const noexcept {
  AnnounceError announceError;
  auto res = parseFixedTuple(cursor, length);
  if (!res) {
    return folly::makeUnexpected(res.error());
  }
  announceError.trackNamespace = TrackNamespace(std::move(res.value()));

  auto errorCode = quic::decodeQuicInteger(cursor, length);
  if (!errorCode) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= errorCode->second;
  announceError.errorCode = AnnounceErrorCode(errorCode->first);

  auto res2 = parseFixedString(cursor, length);
  if (!res2) {
    return folly::makeUnexpected(res2.error());
  }
  announceError.reasonPhrase = std::move(res2.value());

  if (length > 0) {
    return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
  }
  return announceError;
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

  auto errorCode = quic::decodeQuicInteger(cursor, length);
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

folly::Expected<TrackStatusRequest, ErrorCode>
MoQFrameParser::parseTrackStatusRequest(
    folly::io::Cursor& cursor,
    size_t length) const noexcept {
  CHECK(version_.hasValue())
      << "version_ needs to be set to parse TrackStatusRequest";

  TrackStatusRequest trackStatusRequest;
  auto res = parseFullTrackName(cursor, length);
  if (!res) {
    return folly::makeUnexpected(res.error());
  }
  trackStatusRequest.fullTrackName = std::move(res.value());

  if (getDraftMajorVersion(*version_) >= 11) {
    auto numParams = quic::decodeQuicInteger(cursor, length);
    if (!numParams) {
      return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
    }
    length -= numParams->second;
    auto parseParamsResult = parseTrackRequestParams(
        cursor, length, numParams->first, trackStatusRequest.params);
    if (!parseParamsResult) {
      return folly::makeUnexpected(parseParamsResult.error());
    }
  }
  if (length > 0) {
    return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
  }
  return trackStatusRequest;
}

folly::Expected<TrackStatus, ErrorCode> MoQFrameParser::parseTrackStatus(
    folly::io::Cursor& cursor,
    size_t length) const noexcept {
  CHECK(version_.hasValue()) << "version_ needs to be set to parse TrackStatus";

  TrackStatus trackStatus;
  auto res = parseFullTrackName(cursor, length);
  if (!res) {
    return folly::makeUnexpected(res.error());
  }
  trackStatus.fullTrackName = res.value();
  auto statusCode = quic::decodeQuicInteger(cursor, length);
  if (!statusCode) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  if (statusCode->first > folly::to_underlying(TrackStatusCode::UNKNOWN)) {
    XLOG(ERR) << "statusCode > UNKNOWN =" << statusCode->first;
    return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
  }
  trackStatus.statusCode = TrackStatusCode(statusCode->first);
  length -= statusCode->second;
  auto location = parseAbsoluteLocation(cursor, length);
  if (!location) {
    return folly::makeUnexpected(location.error());
  }
  trackStatus.latestGroupAndObject = *location;

  if (getDraftMajorVersion(*version_) >= 11) {
    auto numParams = quic::decodeQuicInteger(cursor, length);
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

folly::Expected<MaxSubscribeId, ErrorCode> MoQFrameParser::parseMaxSubscribeId(
    folly::io::Cursor& cursor,
    size_t length) const noexcept {
  MaxSubscribeId maxSubscribeId;
  auto subscribeID = quic::decodeQuicInteger(cursor, length);
  if (!subscribeID) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= subscribeID->second;
  maxSubscribeId.subscribeID = subscribeID->first;
  if (length > 0) {
    return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
  }
  return maxSubscribeId;
}

folly::Expected<SubscribesBlocked, ErrorCode>
MoQFrameParser::parseSubscribesBlocked(folly::io::Cursor& cursor, size_t length)
    const noexcept {
  SubscribesBlocked subscribesBlocked;
  auto res = quic::decodeQuicInteger(cursor, length);
  if (!res) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  subscribesBlocked.maxSubscribeID = res->first;
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
  auto res = quic::decodeQuicInteger(cursor, length);
  if (!res) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  fetch.subscribeID = res->first;
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

  auto fetchType = quic::decodeQuicInteger(cursor, length);
  if (!fetchType) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  if (fetchType->first == 0 ||
      fetchType->first > folly::to_underlying(FetchType::ABSOLUTE_JOINING)) {
    XLOG(ERR) << "fetchType = 0 or fetchType > JONING =" << fetchType->first;
    return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
  } else if (
      fetchType->first == folly::to_underlying(FetchType::ABSOLUTE_JOINING) &&
      getDraftMajorVersion(*version_) < 11) {
    // Absolute joining is only supported in draft-11 and later
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
    auto jsid = quic::decodeQuicInteger(cursor, length);
    if (!jsid) {
      return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
    }
    length -= jsid->second;

    auto joiningStart = quic::decodeQuicInteger(cursor, length);
    if (!joiningStart) {
      return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
    }
    length -= joiningStart->second;
    // Note fetch.fullTrackName is empty at this point, the session fills it in
    fetch.args = JoiningFetch(
        SubscribeID(jsid->first), joiningStart->first, fetchTypeEnum);
  }
  auto numParams = quic::decodeQuicInteger(cursor, length);
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
  auto res = quic::decodeQuicInteger(cursor, length);
  if (!res) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  fetchCancel.subscribeID = res->first;
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
  auto res = quic::decodeQuicInteger(cursor, length);
  if (!res) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  fetchOk.subscribeID = res->first;
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

  auto numParams = quic::decodeQuicInteger(cursor, length);
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

folly::Expected<FetchError, ErrorCode> MoQFrameParser::parseFetchError(
    folly::io::Cursor& cursor,
    size_t length) const noexcept {
  FetchError fetchError;
  auto res = quic::decodeQuicInteger(cursor, length);
  if (!res) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  fetchError.subscribeID = res->first;
  length -= res->second;

  auto res2 = quic::decodeQuicInteger(cursor, length);
  if (!res2) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  fetchError.errorCode = FetchErrorCode(res2->first);
  length -= res2->second;

  auto res3 = parseFixedString(cursor, length);
  if (!res3) {
    return folly::makeUnexpected(res3.error());
  }
  fetchError.reasonPhrase = std::move(res3.value());
  if (length > 0) {
    return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
  }

  return fetchError;
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
      {std::move(res->trackNamespace), std::move(res->params)});
}

folly::Expected<SubscribeAnnouncesOk, ErrorCode>
MoQFrameParser::parseSubscribeAnnouncesOk(
    folly::io::Cursor& cursor,
    size_t length) const noexcept {
  auto res = parseAnnounceOk(cursor, length);
  if (!res) {
    return folly::makeUnexpected(res.error());
  }
  return SubscribeAnnouncesOk({std::move(res->trackNamespace)});
}

folly::Expected<SubscribeAnnouncesError, ErrorCode>
MoQFrameParser::parseSubscribeAnnouncesError(
    folly::io::Cursor& cursor,
    size_t length) const noexcept {
  auto res = parseAnnounceError(cursor, length);
  if (!res) {
    return folly::makeUnexpected(res.error());
  }
  return SubscribeAnnouncesError(
      {std::move(res->trackNamespace),
       SubscribeAnnouncesErrorCode(folly::to_underlying(res->errorCode)),
       std::move(res->reasonPhrase)});
}

folly::Expected<UnsubscribeAnnounces, ErrorCode>
MoQFrameParser::parseUnsubscribeAnnounces(
    folly::io::Cursor& cursor,
    size_t length) const noexcept {
  auto res = parseAnnounceOk(cursor, length);
  if (!res) {
    return folly::makeUnexpected(res.error());
  }
  return UnsubscribeAnnounces({std::move(res->trackNamespace)});
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
  auto group = quic::decodeQuicInteger(cursor, length);
  if (!group) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  location.group = group->first;
  length -= group->second;

  auto object = quic::decodeQuicInteger(cursor, length);
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

  if (getDraftMajorVersion(*version_) <= 8) {
    // We're not using draft 9 or any of its sub-versions
    // Parse the number of extensions
    auto numExt = quic::decodeQuicInteger(cursor, length);
    if (!numExt) {
      return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
    }
    length -= numExt->second;
    if (numExt->first > kMaxExtensions) {
      XLOG(ERR) << "numExt > kMaxExtensions =" << numExt->first;
      return folly::makeUnexpected(ErrorCode::PROTOCOL_VIOLATION);
    }
    // Parse the extensions
    for (auto i = 0u; i < numExt->first; i++) {
      auto maybeExtension = parseExtension(cursor, length);
      if (maybeExtension.hasError()) {
        return folly::makeUnexpected(maybeExtension.error());
      }
      objectHeader.extensions.emplace_back(std::move(*maybeExtension));
    }
  } else {
    // Parse the length of the extension block
    auto extLen = quic::decodeQuicInteger(cursor, length);
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
    while (extensionBlockLength > 0) {
      // This won't infinite loop because we're parsing out at least a
      // QuicInteger each time.
      auto maybeExtension = parseExtension(cursor, extensionBlockLength);
      if (maybeExtension.hasError()) {
        return folly::makeUnexpected(maybeExtension.error());
      }
      objectHeader.extensions.emplace_back(std::move(*maybeExtension));
    }
    length -= extLen->first;
  }
  return folly::unit;
}

folly::Expected<Extension, ErrorCode> MoQFrameParser::parseExtension(
    folly::io::Cursor& cursor,
    size_t& length) const noexcept {
  auto type = quic::decodeQuicInteger(cursor, length);
  if (!type) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= type->second;
  Extension ext;
  ext.type = type->first;
  if (ext.type & 0x1) {
    auto extLen = quic::decodeQuicInteger(cursor, length);
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
    cursor.clone(ext.arrayValue, extLen->first);
    length -= extLen->first;
  } else {
    auto iVal = quic::decodeQuicInteger(cursor, length);
    if (!iVal) {
      return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
    }
    length -= iVal->second;
    ext.intValue = iVal->first;
  }
  return ext;
}

folly::Expected<std::vector<std::string>, ErrorCode>
MoQFrameParser::parseFixedTuple(folly::io::Cursor& cursor, size_t& length)
    const noexcept {
  auto itemCount = quic::decodeQuicInteger(cursor, length);
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
  if (!error) {
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
  if (getDraftMajorVersion(versionIn) < 11 && (size > 1 << 14)) {
    // Size check for versions < 11
    LOG(ERROR) << "Control message size exceeds max sz=" << size;
    error = true;
    return;
  } else if (
      getDraftMajorVersion(versionIn) >= 11 && (size > ((1 << 16) - 1))) {
    // Size check for versions >= 11
    LOG(ERROR) << "Control message size exceeds max sz=" << size;
    error = true;
    return;
  }
  size_t bitmask = (getDraftMajorVersion(versionIn) >= 11) ? 0 : 0x4000;
  uint16_t sizeVal = folly::Endian::big(uint16_t(bitmask | size));
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

WriteResult writeClientSetup(
    folly::IOBufQueue& writeBuf,
    const ClientSetup& clientSetup,
    uint64_t version) noexcept {
  size_t size = 0;
  bool error = false;
  FrameType frameType = (getDraftMajorVersion(version) >= 11)
      ? FrameType::CLIENT_SETUP
      : FrameType::LEGACY_CLIENT_SETUP;
  auto sizePtr = writeFrameHeader(writeBuf, frameType, error);

  writeVarint(writeBuf, clientSetup.supportedVersions.size(), size, error);
  for (auto version : clientSetup.supportedVersions) {
    writeVarint(writeBuf, version, size, error);
  }

  writeVarint(writeBuf, clientSetup.params.size(), size, error);
  for (auto& param : clientSetup.params) {
    writeVarint(writeBuf, param.key, size, error);
    if (param.key == folly::to_underlying(SetupKey::MAX_SUBSCRIBE_ID)) {
      auto ret = quic::getQuicIntegerSize(param.asUint64);
      if (ret.hasError()) {
        XLOG(ERR) << "Invalid max subscribe id: " << param.asUint64;
        return folly::makeUnexpected(quic::TransportErrorCode::INTERNAL_ERROR);
      }
      writeVarint(writeBuf, ret.value(), size, error);
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
  FrameType frameType = (getDraftMajorVersion(version) >= 11)
      ? FrameType::SERVER_SETUP
      : FrameType::LEGACY_SERVER_SETUP;
  auto sizePtr = writeFrameHeader(writeBuf, frameType, error);
  writeVarint(writeBuf, serverSetup.selectedVersion, size, error);
  writeVarint(writeBuf, serverSetup.params.size(), size, error);
  for (auto& param : serverSetup.params) {
    writeVarint(writeBuf, param.key, size, error);
    if (param.key == folly::to_underlying(SetupKey::MAX_SUBSCRIBE_ID)) {
      auto ret = quic::getQuicIntegerSize(param.asUint64);
      if (ret.hasError()) {
        XLOG(ERR) << "Invalid max subscribe id: " << param.asUint64;
        return folly::makeUnexpected(quic::TransportErrorCode::INTERNAL_ERROR);
      }
      writeVarint(writeBuf, ret.value(), size, error);
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
    const ObjectHeader& objectHeader) const noexcept {
  size_t size = 0;
  bool error = false;
  writeVarint(
      writeBuf, folly::to_underlying(StreamType::SUBGROUP_HEADER), size, error);
  writeVarint(writeBuf, value(objectHeader.trackIdentifier), size, error);
  writeVarint(writeBuf, objectHeader.group, size, error);
  writeVarint(writeBuf, objectHeader.subgroup, size, error);
  writeBuf.append(&objectHeader.priority, 1);
  size += 1;
  if (error) {
    return folly::makeUnexpected(quic::TransportErrorCode::INTERNAL_ERROR);
  }
  return size;
}

WriteResult MoQFrameWriter::writeFetchHeader(
    folly::IOBufQueue& writeBuf,
    SubscribeID subscribeID) const noexcept {
  size_t size = 0;
  bool error = false;
  writeVarint(
      writeBuf, folly::to_underlying(StreamType::FETCH_HEADER), size, error);
  writeVarint(writeBuf, subscribeID.value, size, error);
  if (error) {
    return folly::makeUnexpected(quic::TransportErrorCode::INTERNAL_ERROR);
  }
  return size;
}

WriteResult MoQFrameWriter::writeSingleObjectStream(
    folly::IOBufQueue& writeBuf,
    const ObjectHeader& objectHeader,
    std::unique_ptr<folly::IOBuf> objectPayload) const noexcept {
  auto res = writeSubgroupHeader(writeBuf, objectHeader);
  if (res) {
    return writeStreamObject(
        writeBuf,
        StreamType::SUBGROUP_HEADER,
        objectHeader,
        std::move(objectPayload));
  } else {
    return res;
  }
}

void MoQFrameWriter::writeExtensions(
    folly::IOBufQueue& writeBuf,
    const std::vector<Extension>& extensions,
    size_t& size,
    bool& error) const noexcept {
  if (getDraftMajorVersion(*version_) <= 8) {
    // Not draft 9 or any of its sub-versions. Write out the number of
    // extensions
    writeVarint(writeBuf, extensions.size(), size, error);
  } else {
    // This is draft 9 or one of its sub-versions. Write out the size of
    // the extension block.
    auto extLen = getExtensionSize(extensions, error);
    if (error) {
      return;
    }
    writeVarint(writeBuf, extLen, size, error);
  }
  for (const auto& ext : extensions) {
    writeVarint(writeBuf, ext.type, size, error);
    if (ext.isOddType()) {
      // odd = length prefix
      if (ext.arrayValue) {
        writeVarint(
            writeBuf, ext.arrayValue->computeChainDataLength(), size, error);
        writeBuf.append(ext.arrayValue->clone());
      } else {
        writeVarint(writeBuf, 0, size, error);
      }
    } else {
      // even = single varint
      writeVarint(writeBuf, ext.intValue, size, error);
    }
  }
  return;
}

size_t MoQFrameWriter::getExtensionSize(
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
      auto maybeDataLengthSize = quic::getQuicIntegerSize(ext.intValue);
      if (maybeDataLengthSize.hasError()) {
        error = true;
        return 0;
      }
      size += *maybeDataLengthSize;
      size += ext.arrayValue ? ext.arrayValue->computeChainDataLength() : 0;
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

void MoQFrameWriter::writeTrackRequestParams(
    folly::IOBufQueue& writeBuf,
    const std::vector<TrackRequestParameter>& params,
    size_t& size,
    bool& error) const noexcept {
  CHECK(*version_) << "Version must be set before writing track request params";
  writeVarint(writeBuf, params.size(), size, error);
  for (auto& param : params) {
    writeVarint(writeBuf, param.key, size, error);

    if (param.key == getAuthorizationParamKey(*version_)) {
      writeFixedString(writeBuf, param.asString, size, error);
    } else if (
        param.key == getDeliveryTimeoutParamKey(*version_) ||
        param.key == getMaxCacheDurationParamKey(*version_)) {
      auto res = quic::getQuicIntegerSize(param.asUint64);
      if (!res) {
        error = true;
        return;
      }
      writeVarint(writeBuf, res.value(), size, error);
      writeVarint(writeBuf, param.asUint64, size, error);
    }
  }
}

WriteResult MoQFrameWriter::writeDatagramObject(
    folly::IOBufQueue& writeBuf,
    const ObjectHeader& objectHeader,
    std::unique_ptr<folly::IOBuf> objectPayload) const noexcept {
  size_t size = 0;
  bool error = false;
  bool hasLength = objectHeader.length && *objectHeader.length > 0;
  CHECK(!hasLength || objectHeader.status == ObjectStatus::NORMAL)
      << "non-zero length objects require NORMAL status";
  if (objectHeader.status != ObjectStatus::NORMAL || !hasLength) {
    CHECK(!objectPayload || objectPayload->computeChainDataLength() == 0)
        << "non-empty objectPayload with no header length";
    writeVarint(
        writeBuf,
        folly::to_underlying(StreamType::OBJECT_DATAGRAM_STATUS),
        size,
        error);
    writeVarint(writeBuf, value(objectHeader.trackIdentifier), size, error);
    writeVarint(writeBuf, objectHeader.group, size, error);
    writeVarint(writeBuf, objectHeader.id, size, error);
    writeBuf.append(&objectHeader.priority, 1);
    size += 1;
    writeExtensions(writeBuf, objectHeader.extensions, size, error);
    writeVarint(
        writeBuf, folly::to_underlying(objectHeader.status), size, error);
  } else {
    writeVarint(
        writeBuf,
        folly::to_underlying(StreamType::OBJECT_DATAGRAM),
        size,
        error);
    writeVarint(writeBuf, value(objectHeader.trackIdentifier), size, error);
    writeVarint(writeBuf, objectHeader.group, size, error);
    writeVarint(writeBuf, objectHeader.id, size, error);
    writeBuf.append(&objectHeader.priority, 1);
    size += 1;
    writeExtensions(writeBuf, objectHeader.extensions, size, error);
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
  size_t size = 0;
  bool error = false;
  if (streamType == StreamType::FETCH_HEADER) {
    writeVarint(writeBuf, objectHeader.group, size, error);
    writeVarint(writeBuf, objectHeader.subgroup, size, error);
    writeVarint(writeBuf, objectHeader.id, size, error);
    writeBuf.append(&objectHeader.priority, 1);
    size += 1;
  } else {
    writeVarint(writeBuf, objectHeader.id, size, error);
  }
  writeExtensions(writeBuf, objectHeader.extensions, size, error);
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
  writeVarint(writeBuf, subscribeRequest.subscribeID.value, size, error);
  writeVarint(writeBuf, subscribeRequest.trackAlias.value, size, error);
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
  if (subscribeRequest.locType == LocationType::AbsoluteStart ||
      subscribeRequest.locType == LocationType::AbsoluteRange) {
    writeVarint(writeBuf, subscribeRequest.start->group, size, error);
    writeVarint(writeBuf, subscribeRequest.start->object, size, error);
  }
  if (subscribeRequest.locType == LocationType::AbsoluteRange) {
    writeVarint(writeBuf, subscribeRequest.endGroup, size, error);
  }
  writeTrackRequestParams(writeBuf, subscribeRequest.params, size, error);
  writeSize(sizePtr, size, error, *version_);
  if (error) {
    return folly::makeUnexpected(quic::TransportErrorCode::INTERNAL_ERROR);
  }
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
  writeVarint(writeBuf, update.subscribeID.value, size, error);
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
  size_t size = 0;
  bool error = false;
  auto sizePtr = writeFrameHeader(writeBuf, FrameType::SUBSCRIBE_OK, error);
  writeVarint(writeBuf, subscribeOk.subscribeID.value, size, error);
  writeVarint(writeBuf, subscribeOk.expires.count(), size, error);
  auto order = folly::to_underlying(subscribeOk.groupOrder);
  writeBuf.append(&order, 1);
  size += 1;
  uint8_t contentExists = (subscribeOk.latest) ? 1 : 0;
  writeBuf.append(&contentExists, 1);
  size += 1;
  if (subscribeOk.latest) {
    writeVarint(writeBuf, subscribeOk.latest->group, size, error);
    writeVarint(writeBuf, subscribeOk.latest->object, size, error);
  }
  writeTrackRequestParams(writeBuf, subscribeOk.params, size, error);
  writeSize(sizePtr, size, error, *version_);
  if (error) {
    return folly::makeUnexpected(quic::TransportErrorCode::INTERNAL_ERROR);
  }
  return size;
}

WriteResult MoQFrameWriter::writeSubscribeError(
    folly::IOBufQueue& writeBuf,
    const SubscribeError& subscribeError) const noexcept {
  CHECK(version_.hasValue())
      << "Version needs to be set to write subscribe error";
  size_t size = 0;
  bool error = false;
  auto sizePtr = writeFrameHeader(writeBuf, FrameType::SUBSCRIBE_ERROR, error);
  writeVarint(writeBuf, subscribeError.subscribeID.value, size, error);
  writeVarint(
      writeBuf, folly::to_underlying(subscribeError.errorCode), size, error);
  writeFixedString(writeBuf, subscribeError.reasonPhrase, size, error);
  writeVarint(writeBuf, subscribeError.retryAlias.value_or(0), size, error);
  writeSize(sizePtr, size, error, *version_);
  if (error) {
    return folly::makeUnexpected(quic::TransportErrorCode::INTERNAL_ERROR);
  }
  return size;
}

WriteResult MoQFrameWriter::writeMaxSubscribeId(
    folly::IOBufQueue& writeBuf,
    const MaxSubscribeId& maxSubscribeId) const noexcept {
  CHECK(version_.hasValue())
      << "Version needs to be set to write max subscribe id";
  size_t size = 0;
  bool error = false;
  auto sizePtr = writeFrameHeader(writeBuf, FrameType::MAX_SUBSCRIBE_ID, error);
  writeVarint(writeBuf, maxSubscribeId.subscribeID.value, size, error);
  writeSize(sizePtr, size, error, *version_);
  if (error) {
    return folly::makeUnexpected(quic::TransportErrorCode::INTERNAL_ERROR);
  }
  return size;
}

WriteResult MoQFrameWriter::writeSubscribesBlocked(
    folly::IOBufQueue& writeBuf,
    const SubscribesBlocked& subscribesBlocked) const noexcept {
  CHECK(version_.hasValue())
      << "Version needs to be set to write subscribes blocked";
  size_t size = 0;
  bool error = false;
  auto sizePtr =
      writeFrameHeader(writeBuf, FrameType::SUBSCRIBES_BLOCKED, error);
  writeVarint(writeBuf, subscribesBlocked.maxSubscribeID.value, size, error);
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
  writeVarint(writeBuf, unsubscribe.subscribeID.value, size, error);
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
  writeVarint(writeBuf, subscribeDone.subscribeID.value, size, error);
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

WriteResult MoQFrameWriter::writeAnnounce(
    folly::IOBufQueue& writeBuf,
    const Announce& announce) const noexcept {
  CHECK(version_.hasValue()) << "Version needs to be set to write announce";
  size_t size = 0;
  bool error = false;
  auto sizePtr = writeFrameHeader(writeBuf, FrameType::ANNOUNCE, error);
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
  CHECK(version_.hasValue()) << "Version needs to be set to write announce ok";
  size_t size = 0;
  bool error = false;
  auto sizePtr = writeFrameHeader(writeBuf, FrameType::ANNOUNCE_OK, error);
  writeTrackNamespace(writeBuf, announceOk.trackNamespace, size, error);
  writeSize(sizePtr, size, error, *version_);
  if (error) {
    return folly::makeUnexpected(quic::TransportErrorCode::INTERNAL_ERROR);
  }
  return size;
}

WriteResult MoQFrameWriter::writeAnnounceError(
    folly::IOBufQueue& writeBuf,
    const AnnounceError& announceError) const noexcept {
  CHECK(version_.hasValue())
      << "Version needs to be set to write announce error";
  size_t size = 0;
  bool error = false;
  auto sizePtr = writeFrameHeader(writeBuf, FrameType::ANNOUNCE_ERROR, error);
  writeTrackNamespace(writeBuf, announceError.trackNamespace, size, error);
  writeVarint(
      writeBuf, folly::to_underlying(announceError.errorCode), size, error);
  writeFixedString(writeBuf, announceError.reasonPhrase, size, error);
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

WriteResult MoQFrameWriter::writeTrackStatusRequest(
    folly::IOBufQueue& writeBuf,
    const TrackStatusRequest& trackStatusRequest) const noexcept {
  CHECK(version_.hasValue())
      << "version_ needs to be set to write TrackStatusRequest";

  size_t size = 0;
  bool error = false;
  auto sizePtr =
      writeFrameHeader(writeBuf, FrameType::TRACK_STATUS_REQUEST, error);
  writeFullTrackName(writeBuf, trackStatusRequest.fullTrackName, size, error);
  if (getDraftMajorVersion(*version_) >= 11) {
    writeTrackRequestParams(writeBuf, trackStatusRequest.params, size, error);
  }
  writeSize(sizePtr, size, error, *version_);
  if (error) {
    return folly::makeUnexpected(quic::TransportErrorCode::INTERNAL_ERROR);
  }
  return size;
}

WriteResult MoQFrameWriter::writeTrackStatus(
    folly::IOBufQueue& writeBuf,
    const TrackStatus& trackStatus) const noexcept {
  CHECK(version_.hasValue()) << "version_ needs to be set to write TrackStatus";

  size_t size = 0;
  bool error = false;
  auto sizePtr = writeFrameHeader(writeBuf, FrameType::TRACK_STATUS, error);
  writeFullTrackName(writeBuf, trackStatus.fullTrackName, size, error);
  writeVarint(
      writeBuf, folly::to_underlying(trackStatus.statusCode), size, error);
  if (trackStatus.statusCode == TrackStatusCode::IN_PROGRESS) {
    writeVarint(writeBuf, trackStatus.latestGroupAndObject->group, size, error);
    writeVarint(
        writeBuf, trackStatus.latestGroupAndObject->object, size, error);
  } else {
    writeVarint(writeBuf, 0, size, error);
    writeVarint(writeBuf, 0, size, error);
  }
  if (getDraftMajorVersion(*version_) >= 11) {
    writeTrackRequestParams(writeBuf, trackStatus.params, size, error);
  }
  writeSize(sizePtr, size, error, *version_);
  if (error) {
    return folly::makeUnexpected(quic::TransportErrorCode::INTERNAL_ERROR);
  }
  return size;
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
  CHECK(version_.hasValue())
      << "Version needs to be set to write subscribe announces ok";
  size_t size = 0;
  bool error = false;
  auto sizePtr =
      writeFrameHeader(writeBuf, FrameType::SUBSCRIBE_ANNOUNCES_OK, error);
  writeTrackNamespace(
      writeBuf, subscribeAnnouncesOk.trackNamespacePrefix, size, error);
  writeSize(sizePtr, size, error, *version_);
  if (error) {
    return folly::makeUnexpected(quic::TransportErrorCode::INTERNAL_ERROR);
  }
  return size;
}

WriteResult MoQFrameWriter::writeSubscribeAnnouncesError(
    folly::IOBufQueue& writeBuf,
    const SubscribeAnnouncesError& subscribeAnnouncesError) const noexcept {
  CHECK(version_.hasValue())
      << "Version needs to be set to write subscribe announces error";
  size_t size = 0;
  bool error = false;
  auto sizePtr =
      writeFrameHeader(writeBuf, FrameType::SUBSCRIBE_ANNOUNCES_ERROR, error);
  writeTrackNamespace(
      writeBuf, subscribeAnnouncesError.trackNamespacePrefix, size, error);
  writeVarint(
      writeBuf,
      folly::to_underlying(subscribeAnnouncesError.errorCode),
      size,
      error);
  writeFixedString(writeBuf, subscribeAnnouncesError.reasonPhrase, size, error);
  writeSize(sizePtr, size, error, *version_);
  if (error) {
    return folly::makeUnexpected(quic::TransportErrorCode::INTERNAL_ERROR);
  }
  return size;
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
  writeVarint(writeBuf, fetch.subscribeID.value, size, error);

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

    if (joining->fetchType == FetchType::ABSOLUTE_JOINING &&
        getDraftMajorVersion(*version_) < 11) {
      // Absolute joining is only supported in draft-11 and above
      return folly::makeUnexpected(quic::TransportErrorCode::INTERNAL_ERROR);
    }

    writeVarint(
        writeBuf, folly::to_underlying(joining->fetchType), size, error);
    writeVarint(writeBuf, joining->joiningSubscribeID.value, size, error);
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
  writeVarint(writeBuf, fetchCancel.subscribeID.value, size, error);
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
  writeVarint(writeBuf, fetchOk.subscribeID.value, size, error);
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

WriteResult MoQFrameWriter::writeFetchError(
    folly::IOBufQueue& writeBuf,
    const FetchError& fetchError) const noexcept {
  CHECK(version_.hasValue()) << "Version needs to be set to write fetch error";
  size_t size = 0;
  bool error = false;
  auto sizePtr = writeFrameHeader(writeBuf, FrameType::FETCH_ERROR, error);
  writeVarint(writeBuf, fetchError.subscribeID.value, size, error);
  writeVarint(
      writeBuf, folly::to_underlying(fetchError.errorCode), size, error);
  writeFixedString(writeBuf, fetchError.reasonPhrase, size, error);
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
    case FrameType::MAX_SUBSCRIBE_ID:
      return "MAX_SUBSCRIBE_ID";
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
  return "";
}

const char* getStreamTypeString(StreamType type) {
  switch (type) {
    case StreamType::OBJECT_DATAGRAM:
      return "OBJECT_DATAGRAM";
    case StreamType::SUBGROUP_HEADER:
      return "SUBGROUP_HEADER";
    case StreamType::FETCH_HEADER:
      return "FETCH_HEADER";
    default:
      // can happen when type was cast from uint8_t
      return "Unknown";
  }
  LOG(FATAL) << "Unreachable";
  return "";
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
    case ObjectStatus::END_OF_TRACK_AND_GROUP:
      return "END_OF_TRACK_AND_GROUP";
    case ObjectStatus::END_OF_TRACK:
      return "END_OF_TRACK";
    default:
      // can happen when type was cast from uint8_t
      return "Unknown";
  }
  LOG(FATAL) << "Unreachable";
  return "";
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

std::ostream& operator<<(std::ostream& os, SubscribeID id) {
  os << id.value;
  return os;
}

std::ostream& operator<<(std::ostream& os, const ObjectHeader& header) {
  os << " trackIdentifier=" << value(header.trackIdentifier)
     << " group=" << header.group << " subgroup=" << header.subgroup
     << " id=" << header.id << " priority=" << uint32_t(header.priority)
     << " status=" << getObjectStatusString(header.status) << " length="
     << (header.length.hasValue() ? std::to_string(header.length.value())
                                  : "none");
  return os;
}

} // namespace moxygen

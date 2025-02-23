/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/MoQFramer.h"
#include <folly/lang/Bits.h>
#include <folly/logging/xlog.h>

namespace moxygen {

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

folly::Expected<std::vector<std::string>, ErrorCode> parseFixedTuple(
    folly::io::Cursor& cursor,
    size_t& length) {
  auto itemCount = quic::decodeQuicInteger(cursor, length);
  if (!itemCount) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  if (itemCount->first > kMaxNamespaceLength) {
    return folly::makeUnexpected(ErrorCode::INVALID_MESSAGE);
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
      res = quic::decodeQuicInteger(cursor, res->first);
      if (!res) {
        return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
      }
      p.asUint64 = res->first;
    } else {
      auto res = parseFixedString(cursor, length);
      if (!res) {
        return folly::makeUnexpected(res.error());
      }
      p.asString = std::move(res.value());
    }
    params.emplace_back(std::move(p));
  }
  return folly::unit;
}

folly::Expected<FullTrackName, ErrorCode> parseFullTrackName(
    folly::io::Cursor& cursor,
    size_t& length) {
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

folly::Expected<AbsoluteLocation, ErrorCode> parseAbsoluteLocation(
    folly::io::Cursor& cursor,
    size_t& length) {
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
  return serverSetup;
}

folly::Expected<SubscribeID, ErrorCode> parseFetchHeader(
    folly::io::Cursor& cursor) noexcept {
  auto subscribeID = quic::decodeQuicInteger(cursor);
  if (!subscribeID) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  return SubscribeID(subscribeID->first);
}

folly::Expected<ObjectHeader, ErrorCode> parseDatagramObjectHeader(
    folly::io::Cursor& cursor,
    StreamType streamType,
    size_t length) noexcept {
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
  if (streamType == StreamType::OBJECT_DATAGRAM_STATUS) {
    auto status = quic::decodeQuicInteger(cursor, length);
    if (!status) {
      return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
    }
    length -= status->second;
    if (status->first > folly::to_underlying(ObjectStatus::END_OF_TRACK)) {
      return folly::makeUnexpected(ErrorCode::PARSE_ERROR);
    }
    objectHeader.status = ObjectStatus(status->first);
    objectHeader.length = 0;
    if (length != 0) {
      // MUST consume entire datagram
      return folly::makeUnexpected(ErrorCode::PARSE_ERROR);
    }
  } else {
    CHECK(streamType == StreamType::OBJECT_DATAGRAM);
    objectHeader.status = ObjectStatus::NORMAL;
    objectHeader.length = length;
  }
  return objectHeader;
}

folly::Expected<ObjectHeader, ErrorCode> parseSubgroupHeader(
    folly::io::Cursor& cursor) noexcept {
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

folly::Expected<folly::Unit, ErrorCode> parseObjectStatusAndLength(
    folly::io::Cursor& cursor,
    size_t length,
    ObjectHeader& objectHeader) {
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
      return folly::makeUnexpected(ErrorCode::PARSE_ERROR);
    }
    objectHeader.status = ObjectStatus(objectStatus->first);
    length -= objectStatus->second;
  } else {
    objectHeader.status = ObjectStatus::NORMAL;
  }

  return folly::unit;
}

folly::Expected<ObjectHeader, ErrorCode> parseFetchObjectHeader(
    folly::io::Cursor& cursor,
    const ObjectHeader& headerTemplate) noexcept {
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

  auto res = parseObjectStatusAndLength(cursor, length, objectHeader);
  if (!res) {
    return folly::makeUnexpected(res.error());
  }
  return objectHeader;
}

folly::Expected<ObjectHeader, ErrorCode> parseSubgroupObjectHeader(
    folly::io::Cursor& cursor,
    const ObjectHeader& headerTemplate) noexcept {
  // TODO get rid of this
  auto length = cursor.totalLength();
  ObjectHeader objectHeader = headerTemplate;
  auto id = quic::decodeQuicInteger(cursor, length);
  if (!id) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= id->second;
  objectHeader.id = id->first;

  auto res = parseObjectStatusAndLength(cursor, length, objectHeader);
  if (!res) {
    return folly::makeUnexpected(res.error());
  }
  return objectHeader;
}

folly::Expected<folly::Unit, ErrorCode> parseTrackRequestParams(
    folly::io::Cursor& cursor,
    size_t length,
    size_t numParams,
    std::vector<TrackRequestParameter>& params) {
  for (auto i = 0u; i < numParams; i++) {
    TrackRequestParameter p;
    auto key = quic::decodeQuicInteger(cursor, length);
    if (!key) {
      return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
    }
    length -= key->second;
    p.key = key->first;
    if (p.key == folly::to_underlying(TrackRequestParamKey::AUTHORIZATION)) {
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

folly::Expected<SubscribeRequest, ErrorCode> parseSubscribeRequest(
    folly::io::Cursor& cursor,
    size_t length) noexcept {
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
    return folly::makeUnexpected(ErrorCode::INVALID_MESSAGE);
  }
  length -= 2;
  subscribeRequest.groupOrder = static_cast<GroupOrder>(order);
  auto locType = quic::decodeQuicInteger(cursor, length);
  if (!locType) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  if (locType->first > folly::to_underlying(LocationType::AbsoluteRange)) {
    return folly::makeUnexpected(ErrorCode::PARSE_ERROR);
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
  return subscribeRequest;
}

folly::Expected<SubscribeUpdate, ErrorCode> parseSubscribeUpdate(
    folly::io::Cursor& cursor,
    size_t length) noexcept {
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
  return subscribeUpdate;
}

folly::Expected<SubscribeOk, ErrorCode> parseSubscribeOk(
    folly::io::Cursor& cursor,
    size_t length) noexcept {
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
    return folly::makeUnexpected(ErrorCode::INVALID_MESSAGE);
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

  return subscribeOk;
}

folly::Expected<SubscribeError, ErrorCode> parseSubscribeError(
    folly::io::Cursor& cursor,
    size_t length) noexcept {
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

  return subscribeError;
}

folly::Expected<Unsubscribe, ErrorCode> parseUnsubscribe(
    folly::io::Cursor& cursor,
    size_t length) noexcept {
  Unsubscribe unsubscribe;
  auto subscribeID = quic::decodeQuicInteger(cursor, length);
  if (!subscribeID) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= subscribeID->second;
  unsubscribe.subscribeID = subscribeID->first;

  return unsubscribe;
}

folly::Expected<SubscribeDone, ErrorCode> parseSubscribeDone(
    folly::io::Cursor& cursor,
    size_t length) noexcept {
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
    subscribeDone.finalObject = *res;
  }

  return subscribeDone;
}

folly::Expected<Announce, ErrorCode> parseAnnounce(
    folly::io::Cursor& cursor,
    size_t length) noexcept {
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
  return announce;
}

folly::Expected<AnnounceOk, ErrorCode> parseAnnounceOk(
    folly::io::Cursor& cursor,
    size_t length) noexcept {
  AnnounceOk announceOk;
  auto res = parseFixedTuple(cursor, length);
  if (!res) {
    return folly::makeUnexpected(res.error());
  }
  announceOk.trackNamespace = TrackNamespace(std::move(res.value()));
  return announceOk;
}

folly::Expected<AnnounceError, ErrorCode> parseAnnounceError(
    folly::io::Cursor& cursor,
    size_t length) noexcept {
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

  return announceError;
}

folly::Expected<Unannounce, ErrorCode> parseUnannounce(
    folly::io::Cursor& cursor,
    size_t length) noexcept {
  Unannounce unannounce;
  auto res = parseFixedTuple(cursor, length);
  if (!res) {
    return folly::makeUnexpected(res.error());
  }
  unannounce.trackNamespace = TrackNamespace(std::move(res.value()));
  return unannounce;
}

folly::Expected<AnnounceCancel, ErrorCode> parseAnnounceCancel(
    folly::io::Cursor& cursor,
    size_t length) noexcept {
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
  return announceCancel;
}

folly::Expected<TrackStatusRequest, ErrorCode> parseTrackStatusRequest(
    folly::io::Cursor& cursor,
    size_t length) noexcept {
  TrackStatusRequest trackStatusRequest;
  auto res = parseFullTrackName(cursor, length);
  if (!res) {
    return folly::makeUnexpected(res.error());
  }
  trackStatusRequest.fullTrackName = std::move(res.value());
  return trackStatusRequest;
}

folly::Expected<TrackStatus, ErrorCode> parseTrackStatus(
    folly::io::Cursor& cursor,
    size_t length) noexcept {
  TrackStatus trackStatus;
  auto res = parseFullTrackName(cursor, length);
  if (!res) {
    return folly::makeUnexpected(res.error());
  }
  auto statusCode = quic::decodeQuicInteger(cursor, length);
  if (!statusCode) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  if (statusCode->first > folly::to_underlying(TrackStatusCode::UNKNOWN)) {
    return folly::makeUnexpected(ErrorCode::INVALID_MESSAGE);
  }
  trackStatus.statusCode = TrackStatusCode(statusCode->first);
  length -= statusCode->second;
  auto location = parseAbsoluteLocation(cursor, length);
  if (!location) {
    return folly::makeUnexpected(location.error());
  }
  trackStatus.latestGroupAndObject = *location;
  return trackStatus;
}

folly::Expected<Goaway, ErrorCode> parseGoaway(
    folly::io::Cursor& cursor,
    size_t length) noexcept {
  Goaway goaway;
  auto res = parseFixedString(cursor, length);
  if (!res) {
    return folly::makeUnexpected(res.error());
  }
  goaway.newSessionUri = std::move(res.value());
  return goaway;
}

folly::Expected<MaxSubscribeId, ErrorCode> parseMaxSubscribeId(
    folly::io::Cursor& cursor,
    size_t length) noexcept {
  MaxSubscribeId maxSubscribeId;
  auto subscribeID = quic::decodeQuicInteger(cursor, length);
  if (!subscribeID) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= subscribeID->second;
  maxSubscribeId.subscribeID = subscribeID->first;
  return maxSubscribeId;
}

folly::Expected<Fetch, ErrorCode> parseFetch(
    folly::io::Cursor& cursor,
    size_t length) noexcept {
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
    return folly::makeUnexpected(ErrorCode::INVALID_MESSAGE);
  }
  fetch.groupOrder = static_cast<GroupOrder>(order);
  length -= 2 * sizeof(uint8_t);

  auto fetchType = quic::decodeQuicInteger(cursor, length);
  if (!fetchType) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  if (fetchType->first == 0 ||
      fetchType->first > folly::to_underlying(FetchType::JOINING)) {
    return folly::makeUnexpected(ErrorCode::INVALID_MESSAGE);
  }
  length -= fetchType->second;

  if (FetchType(fetchType->first) == FetchType::STANDALONE) {
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
    // JOINING
    auto jsid = quic::decodeQuicInteger(cursor, length);
    if (!jsid) {
      return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
    }
    length -= jsid->second;

    auto pgo = quic::decodeQuicInteger(cursor, length);
    if (!pgo) {
      return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
    }
    length -= pgo->second;
    // Note fetch.fullTrackName is empty at this point, the session fills it in
    fetch.args = JoiningFetch(SubscribeID(jsid->first), pgo->first);
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
  return fetch;
}

folly::Expected<FetchCancel, ErrorCode> parseFetchCancel(
    folly::io::Cursor& cursor,
    size_t length) noexcept {
  FetchCancel fetchCancel;
  auto res = quic::decodeQuicInteger(cursor, length);
  if (!res) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  fetchCancel.subscribeID = res->first;
  return fetchCancel;
}

folly::Expected<FetchOk, ErrorCode> parseFetchOk(
    folly::io::Cursor& cursor,
    size_t length) noexcept {
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
    return folly::makeUnexpected(ErrorCode::INVALID_MESSAGE);
  }
  fetchOk.groupOrder = static_cast<GroupOrder>(order);
  fetchOk.endOfTrack = cursor.readBE<uint8_t>();
  length -= 2 * sizeof(uint8_t);

  auto res2 = parseAbsoluteLocation(cursor, length);
  if (!res2) {
    return folly::makeUnexpected(res2.error());
  }
  fetchOk.latestGroupAndObject = std::move(res2.value());

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

  return fetchOk;
}

folly::Expected<FetchError, ErrorCode> parseFetchError(
    folly::io::Cursor& cursor,
    size_t length) noexcept {
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

  return fetchError;
}

folly::Expected<SubscribeAnnounces, ErrorCode> parseSubscribeAnnounces(
    folly::io::Cursor& cursor,
    size_t length) noexcept {
  auto res = parseAnnounce(cursor, length);
  if (!res) {
    return folly::makeUnexpected(res.error());
  }
  return SubscribeAnnounces(
      {std::move(res->trackNamespace), std::move(res->params)});
}

folly::Expected<SubscribeAnnouncesOk, ErrorCode> parseSubscribeAnnouncesOk(
    folly::io::Cursor& cursor,
    size_t length) noexcept {
  auto res = parseAnnounceOk(cursor, length);
  if (!res) {
    return folly::makeUnexpected(res.error());
  }
  return SubscribeAnnouncesOk({std::move(res->trackNamespace)});
}

folly::Expected<SubscribeAnnouncesError, ErrorCode>
parseSubscribeAnnouncesError(
    folly::io::Cursor& cursor,
    size_t length) noexcept {
  auto res = parseAnnounceError(cursor, length);
  if (!res) {
    return folly::makeUnexpected(res.error());
  }
  return SubscribeAnnouncesError(
      {std::move(res->trackNamespace),
       SubscribeAnnouncesErrorCode(folly::to_underlying(res->errorCode)),
       std::move(res->reasonPhrase)});
}

folly::Expected<UnsubscribeAnnounces, ErrorCode> parseUnsubscribeAnnounces(
    folly::io::Cursor& cursor,
    size_t length) noexcept {
  auto res = parseAnnounceOk(cursor, length);
  if (!res) {
    return folly::makeUnexpected(res.error());
  }
  return UnsubscribeAnnounces({std::move(res->trackNamespace)});
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

void writeSize(uint16_t* sizePtr, size_t size, bool& error) {
  if (size > 1 << 14) {
    LOG(ERROR) << "Control message size exceeds max sz=" << size;
    error = true;
    return;
  }
  uint16_t sizeVal = folly::Endian::big(uint16_t(0x4000 | size));
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

void writeTrackRequestParams(
    folly::IOBufQueue& writeBuf,
    const std::vector<TrackRequestParameter>& params,
    size_t& size,
    bool& error) noexcept {
  writeVarint(writeBuf, params.size(), size, error);
  for (auto& param : params) {
    writeVarint(writeBuf, param.key, size, error);
    switch (param.key) {
      case folly::to_underlying(TrackRequestParamKey::AUTHORIZATION):
        writeFixedString(writeBuf, param.asString, size, error);
        break;
      case folly::to_underlying(TrackRequestParamKey::DELIVERY_TIMEOUT):
      case folly::to_underlying(TrackRequestParamKey::MAX_CACHE_DURATION):
        auto res = quic::getQuicIntegerSize(param.asUint64);
        if (!res) {
          error = true;
          return;
        }
        writeVarint(writeBuf, res.value(), size, error);
        writeVarint(writeBuf, param.asUint64, size, error);
        break;
    }
  }
}

WriteResult writeClientSetup(
    folly::IOBufQueue& writeBuf,
    const ClientSetup& clientSetup) noexcept {
  size_t size = 0;
  bool error = false;
  auto sizePtr = writeFrameHeader(writeBuf, FrameType::CLIENT_SETUP, error);

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
  writeSize(sizePtr, size, error);
  if (error) {
    return folly::makeUnexpected(quic::TransportErrorCode::INTERNAL_ERROR);
  }
  return size;
}

WriteResult writeServerSetup(
    folly::IOBufQueue& writeBuf,
    const ServerSetup& serverSetup) noexcept {
  size_t size = 0;
  bool error = false;
  auto sizePtr = writeFrameHeader(writeBuf, FrameType::SERVER_SETUP, error);
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
  writeSize(sizePtr, size, error);
  if (error) {
    return folly::makeUnexpected(quic::TransportErrorCode::INTERNAL_ERROR);
  }
  return size;
}

WriteResult writeSubgroupHeader(
    folly::IOBufQueue& writeBuf,
    const ObjectHeader& objectHeader) noexcept {
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

WriteResult writeFetchHeader(
    folly::IOBufQueue& writeBuf,
    SubscribeID subscribeID) noexcept {
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

WriteResult writeSingleObjectStream(
    folly::IOBufQueue& writeBuf,
    const ObjectHeader& objectHeader,
    std::unique_ptr<folly::IOBuf> objectPayload) noexcept {
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

WriteResult writeDatagramObject(
    folly::IOBufQueue& writeBuf,
    const ObjectHeader& objectHeader,
    std::unique_ptr<folly::IOBuf> objectPayload) noexcept {
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
    writeBuf.append(std::move(objectPayload));
  }
  if (error) {
    return folly::makeUnexpected(quic::TransportErrorCode::INTERNAL_ERROR);
  }
  return size;
}

WriteResult writeStreamObject(
    folly::IOBufQueue& writeBuf,
    StreamType streamType,
    const ObjectHeader& objectHeader,
    std::unique_ptr<folly::IOBuf> objectPayload) noexcept {
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

WriteResult writeSubscribeRequest(
    folly::IOBufQueue& writeBuf,
    const SubscribeRequest& subscribeRequest) noexcept {
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
  writeVarint(
      writeBuf, folly::to_underlying(subscribeRequest.locType), size, error);
  if (subscribeRequest.locType == LocationType::AbsoluteStart ||
      subscribeRequest.locType == LocationType::AbsoluteRange) {
    writeVarint(writeBuf, subscribeRequest.start->group, size, error);
    writeVarint(writeBuf, subscribeRequest.start->object, size, error);
  }
  if (subscribeRequest.locType == LocationType::AbsoluteRange) {
    writeVarint(writeBuf, subscribeRequest.endGroup, size, error);
  }
  writeTrackRequestParams(writeBuf, subscribeRequest.params, size, error);
  writeSize(sizePtr, size, error);
  if (error) {
    return folly::makeUnexpected(quic::TransportErrorCode::INTERNAL_ERROR);
  }
  return size;
}

WriteResult writeSubscribeUpdate(
    folly::IOBufQueue& writeBuf,
    const SubscribeUpdate& update) noexcept {
  size_t size = 0;
  bool error = false;
  auto sizePtr = writeFrameHeader(writeBuf, FrameType::SUBSCRIBE_UPDATE, error);
  writeVarint(writeBuf, update.subscribeID.value, size, error);
  writeVarint(writeBuf, update.start.group, size, error);
  writeVarint(writeBuf, update.start.object, size, error);
  writeVarint(writeBuf, update.endGroup, size, error);
  writeBuf.append(&update.priority, 1);
  size += 1;
  writeTrackRequestParams(writeBuf, update.params, size, error);
  writeSize(sizePtr, size, error);
  if (error) {
    return folly::makeUnexpected(quic::TransportErrorCode::INTERNAL_ERROR);
  }
  return size;
}

WriteResult writeSubscribeOk(
    folly::IOBufQueue& writeBuf,
    const SubscribeOk& subscribeOk) noexcept {
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
  writeSize(sizePtr, size, error);
  if (error) {
    return folly::makeUnexpected(quic::TransportErrorCode::INTERNAL_ERROR);
  }
  return size;
}

WriteResult writeSubscribeError(
    folly::IOBufQueue& writeBuf,
    const SubscribeError& subscribeError) noexcept {
  size_t size = 0;
  bool error = false;
  auto sizePtr = writeFrameHeader(writeBuf, FrameType::SUBSCRIBE_ERROR, error);
  writeVarint(writeBuf, subscribeError.subscribeID.value, size, error);
  writeVarint(
      writeBuf, folly::to_underlying(subscribeError.errorCode), size, error);
  writeFixedString(writeBuf, subscribeError.reasonPhrase, size, error);
  writeVarint(writeBuf, subscribeError.retryAlias.value_or(0), size, error);
  writeSize(sizePtr, size, error);
  if (error) {
    return folly::makeUnexpected(quic::TransportErrorCode::INTERNAL_ERROR);
  }
  return size;
}

WriteResult writeMaxSubscribeId(
    folly::IOBufQueue& writeBuf,
    const MaxSubscribeId& maxSubscribeId) noexcept {
  size_t size = 0;
  bool error = false;
  auto sizePtr = writeFrameHeader(writeBuf, FrameType::MAX_SUBSCRIBE_ID, error);
  writeVarint(writeBuf, maxSubscribeId.subscribeID.value, size, error);
  writeSize(sizePtr, size, error);
  if (error) {
    return folly::makeUnexpected(quic::TransportErrorCode::INTERNAL_ERROR);
  }
  return size;
}

WriteResult writeUnsubscribe(
    folly::IOBufQueue& writeBuf,
    const Unsubscribe& unsubscribe) noexcept {
  size_t size = 0;
  bool error = false;
  auto sizePtr = writeFrameHeader(writeBuf, FrameType::UNSUBSCRIBE, error);
  writeVarint(writeBuf, unsubscribe.subscribeID.value, size, error);
  writeSize(sizePtr, size, error);
  if (error) {
    return folly::makeUnexpected(quic::TransportErrorCode::INTERNAL_ERROR);
  }
  return size;
}

WriteResult writeSubscribeDone(
    folly::IOBufQueue& writeBuf,
    const SubscribeDone& subscribeDone) noexcept {
  size_t size = 0;
  bool error = false;
  auto sizePtr = writeFrameHeader(writeBuf, FrameType::SUBSCRIBE_DONE, error);
  writeVarint(writeBuf, subscribeDone.subscribeID.value, size, error);
  writeVarint(
      writeBuf, folly::to_underlying(subscribeDone.statusCode), size, error);
  writeVarint(writeBuf, subscribeDone.streamCount, size, error);
  writeFixedString(writeBuf, subscribeDone.reasonPhrase, size, error);
  if (subscribeDone.finalObject) {
    writeVarint(writeBuf, 1, size, error);
    writeVarint(writeBuf, subscribeDone.finalObject->group, size, error);
    writeVarint(writeBuf, subscribeDone.finalObject->object, size, error);
  } else {
    writeVarint(writeBuf, 0, size, error);
  }
  writeSize(sizePtr, size, error);
  if (error) {
    return folly::makeUnexpected(quic::TransportErrorCode::INTERNAL_ERROR);
  }
  return size;
}

WriteResult writeAnnounce(
    folly::IOBufQueue& writeBuf,
    const Announce& announce) noexcept {
  size_t size = 0;
  bool error = false;
  auto sizePtr = writeFrameHeader(writeBuf, FrameType::ANNOUNCE, error);
  writeTrackNamespace(writeBuf, announce.trackNamespace, size, error);
  writeTrackRequestParams(writeBuf, announce.params, size, error);
  writeSize(sizePtr, size, error);
  if (error) {
    return folly::makeUnexpected(quic::TransportErrorCode::INTERNAL_ERROR);
  }
  return size;
}

WriteResult writeAnnounceOk(
    folly::IOBufQueue& writeBuf,
    const AnnounceOk& announceOk) noexcept {
  size_t size = 0;
  bool error = false;
  auto sizePtr = writeFrameHeader(writeBuf, FrameType::ANNOUNCE_OK, error);
  writeTrackNamespace(writeBuf, announceOk.trackNamespace, size, error);
  writeSize(sizePtr, size, error);
  if (error) {
    return folly::makeUnexpected(quic::TransportErrorCode::INTERNAL_ERROR);
  }
  return size;
}

WriteResult writeAnnounceError(
    folly::IOBufQueue& writeBuf,
    const AnnounceError& announceError) noexcept {
  size_t size = 0;
  bool error = false;
  auto sizePtr = writeFrameHeader(writeBuf, FrameType::ANNOUNCE_ERROR, error);
  writeTrackNamespace(writeBuf, announceError.trackNamespace, size, error);
  writeVarint(
      writeBuf, folly::to_underlying(announceError.errorCode), size, error);
  writeFixedString(writeBuf, announceError.reasonPhrase, size, error);
  writeSize(sizePtr, size, error);
  if (error) {
    return folly::makeUnexpected(quic::TransportErrorCode::INTERNAL_ERROR);
  }
  return size;
}

WriteResult writeUnannounce(
    folly::IOBufQueue& writeBuf,
    const Unannounce& unannounce) noexcept {
  size_t size = 0;
  bool error = false;
  auto sizePtr = writeFrameHeader(writeBuf, FrameType::UNANNOUNCE, error);
  writeTrackNamespace(writeBuf, unannounce.trackNamespace, size, error);
  writeSize(sizePtr, size, error);
  if (error) {
    return folly::makeUnexpected(quic::TransportErrorCode::INTERNAL_ERROR);
  }
  return size;
}

WriteResult writeAnnounceCancel(
    folly::IOBufQueue& writeBuf,
    const AnnounceCancel& announceCancel) noexcept {
  size_t size = 0;
  bool error = false;
  auto sizePtr = writeFrameHeader(writeBuf, FrameType::ANNOUNCE_CANCEL, error);
  writeTrackNamespace(writeBuf, announceCancel.trackNamespace, size, error);
  writeVarint(
      writeBuf, folly::to_underlying(announceCancel.errorCode), size, error);
  writeFixedString(writeBuf, announceCancel.reasonPhrase, size, error);
  writeSize(sizePtr, size, error);
  if (error) {
    return folly::makeUnexpected(quic::TransportErrorCode::INTERNAL_ERROR);
  }
  return size;
}

WriteResult writeTrackStatusRequest(
    folly::IOBufQueue& writeBuf,
    const TrackStatusRequest& trackStatusRequest) noexcept {
  size_t size = 0;
  bool error = false;
  auto sizePtr =
      writeFrameHeader(writeBuf, FrameType::TRACK_STATUS_REQUEST, error);
  writeFullTrackName(writeBuf, trackStatusRequest.fullTrackName, size, error);
  writeSize(sizePtr, size, error);
  if (error) {
    return folly::makeUnexpected(quic::TransportErrorCode::INTERNAL_ERROR);
  }
  return size;
}

WriteResult writeTrackStatus(
    folly::IOBufQueue& writeBuf,
    const TrackStatus& trackStatus) noexcept {
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
  writeSize(sizePtr, size, error);
  if (error) {
    return folly::makeUnexpected(quic::TransportErrorCode::INTERNAL_ERROR);
  }
  return size;
}

WriteResult writeGoaway(
    folly::IOBufQueue& writeBuf,
    const Goaway& goaway) noexcept {
  size_t size = 0;
  bool error = false;
  auto sizePtr = writeFrameHeader(writeBuf, FrameType::GOAWAY, error);
  writeFixedString(writeBuf, goaway.newSessionUri, size, error);
  writeSize(sizePtr, size, error);
  if (error) {
    return folly::makeUnexpected(quic::TransportErrorCode::INTERNAL_ERROR);
  }
  return size;
}

WriteResult writeSubscribeAnnounces(
    folly::IOBufQueue& writeBuf,
    const SubscribeAnnounces& subscribeAnnounces) noexcept {
  size_t size = 0;
  bool error = false;
  auto sizePtr =
      writeFrameHeader(writeBuf, FrameType::SUBSCRIBE_ANNOUNCES, error);
  writeTrackNamespace(
      writeBuf, subscribeAnnounces.trackNamespacePrefix, size, error);
  writeTrackRequestParams(writeBuf, subscribeAnnounces.params, size, error);
  writeSize(sizePtr, size, error);
  if (error) {
    return folly::makeUnexpected(quic::TransportErrorCode::INTERNAL_ERROR);
  }
  return size;
}

WriteResult writeSubscribeAnnouncesOk(
    folly::IOBufQueue& writeBuf,
    const SubscribeAnnouncesOk& subscribeAnnouncesOk) noexcept {
  size_t size = 0;
  bool error = false;
  auto sizePtr =
      writeFrameHeader(writeBuf, FrameType::SUBSCRIBE_ANNOUNCES_OK, error);
  writeTrackNamespace(
      writeBuf, subscribeAnnouncesOk.trackNamespacePrefix, size, error);
  writeSize(sizePtr, size, error);
  if (error) {
    return folly::makeUnexpected(quic::TransportErrorCode::INTERNAL_ERROR);
  }
  return size;
}

WriteResult writeSubscribeAnnouncesError(
    folly::IOBufQueue& writeBuf,
    const SubscribeAnnouncesError& subscribeAnnouncesError) noexcept {
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
  writeSize(sizePtr, size, error);
  if (error) {
    return folly::makeUnexpected(quic::TransportErrorCode::INTERNAL_ERROR);
  }
  return size;
}

WriteResult writeUnsubscribeAnnounces(
    folly::IOBufQueue& writeBuf,
    const UnsubscribeAnnounces& unsubscribeAnnounces) noexcept {
  size_t size = 0;
  bool error = false;
  auto sizePtr =
      writeFrameHeader(writeBuf, FrameType::UNSUBSCRIBE_ANNOUNCES, error);
  writeTrackNamespace(
      writeBuf, unsubscribeAnnounces.trackNamespacePrefix, size, error);
  writeSize(sizePtr, size, error);
  if (error) {
    return folly::makeUnexpected(quic::TransportErrorCode::INTERNAL_ERROR);
  }
  return size;
}

WriteResult writeFetch(
    folly::IOBufQueue& writeBuf,
    const Fetch& fetch) noexcept {
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
    writeVarint(
        writeBuf, folly::to_underlying(FetchType::JOINING), size, error);
    writeVarint(writeBuf, joining->joiningSubscribeID.value, size, error);
    writeVarint(writeBuf, joining->precedingGroupOffset, size, error);
  }
  writeTrackRequestParams(writeBuf, fetch.params, size, error);

  writeSize(sizePtr, size, error);
  if (error) {
    return folly::makeUnexpected(quic::TransportErrorCode::INTERNAL_ERROR);
  }
  return size;
}

WriteResult writeFetchCancel(
    folly::IOBufQueue& writeBuf,
    const FetchCancel& fetchCancel) noexcept {
  size_t size = 0;
  bool error = false;
  auto sizePtr = writeFrameHeader(writeBuf, FrameType::FETCH_CANCEL, error);
  writeVarint(writeBuf, fetchCancel.subscribeID.value, size, error);
  writeSize(sizePtr, size, error);
  if (error) {
    return folly::makeUnexpected(quic::TransportErrorCode::INTERNAL_ERROR);
  }
  return size;
}

WriteResult writeFetchOk(
    folly::IOBufQueue& writeBuf,
    const FetchOk& fetchOk) noexcept {
  size_t size = 0;
  bool error = false;
  auto sizePtr = writeFrameHeader(writeBuf, FrameType::FETCH_OK, error);
  writeVarint(writeBuf, fetchOk.subscribeID.value, size, error);
  auto order = folly::to_underlying(fetchOk.groupOrder);
  writeBuf.append(&order, 1);
  size += 1;
  writeBuf.append(&fetchOk.endOfTrack, 1);
  size += 1;
  writeVarint(writeBuf, fetchOk.latestGroupAndObject.group, size, error);
  writeVarint(writeBuf, fetchOk.latestGroupAndObject.object, size, error);
  writeTrackRequestParams(writeBuf, fetchOk.params, size, error);
  writeSize(sizePtr, size, error);
  if (error) {
    return folly::makeUnexpected(quic::TransportErrorCode::INTERNAL_ERROR);
  }
  return size;
}

WriteResult writeFetchError(
    folly::IOBufQueue& writeBuf,
    const FetchError& fetchError) noexcept {
  size_t size = 0;
  bool error = false;
  auto sizePtr = writeFrameHeader(writeBuf, FrameType::FETCH_ERROR, error);
  writeVarint(writeBuf, fetchError.subscribeID.value, size, error);
  writeVarint(
      writeBuf, folly::to_underlying(fetchError.errorCode), size, error);
  writeFixedString(writeBuf, fetchError.reasonPhrase, size, error);
  writeSize(sizePtr, size, error);
  if (error) {
    return folly::makeUnexpected(quic::TransportErrorCode::INTERNAL_ERROR);
  }
  return size;
}

namespace {
const char* getFrameTypeString(FrameType type) {
  switch (type) {
    case FrameType::CLIENT_SETUP:
      return "CLIENT_SETUP";
    case FrameType::SERVER_SETUP:
      return "SERVER_SETUP";
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

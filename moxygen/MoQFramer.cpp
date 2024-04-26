/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "moxygen/MoQFramer.h"

namespace moxygen {

folly::Expected<std::string, ErrorCode> parseFixedString(
    folly::io::Cursor& cursor,
    size_t& length) {
  auto strLength = quic::decodeQuicInteger(cursor, length);
  if (!strLength || strLength->first > length) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= strLength->second;
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
    if (p.key == folly::to_underlying(SetupKey::ROLE)) {
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
  auto res = parseFixedString(cursor, length);
  if (!res) {
    return folly::makeUnexpected(res.error());
  }
  fullTrackName.trackNamespace = std::move(res.value());
  res = parseFixedString(cursor, length);
  if (!res) {
    return folly::makeUnexpected(res.error());
  }
  fullTrackName.trackName = std::move(res.value());
  return fullTrackName;
}

folly::Expected<ClientSetup, ErrorCode> parseClientSetup(
    folly::io::Cursor& cursor) noexcept {
  auto length = cursor.totalLength();
  ClientSetup clientSetup;
  auto numVersions = quic::decodeQuicInteger(cursor, length);
  if (!numVersions) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= numVersions->second;
  for (auto i = 0; i < numVersions->first; i++) {
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
    folly::io::Cursor& cursor) noexcept {
  auto length = cursor.totalLength();
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

folly::Expected<ObjectHeader, ErrorCode> parseObjectHeader(
    folly::io::Cursor& cursor,
    FrameType frameType) noexcept {
  DCHECK(
      frameType == FrameType::OBJECT_STREAM ||
      frameType == FrameType::OBJECT_PREFER_DATAGRAM);
  auto length = cursor.totalLength();
  ObjectHeader objectHeader;
  auto subscribeID = quic::decodeQuicInteger(cursor, length);
  if (!subscribeID) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= subscribeID->second;
  objectHeader.subscribeID = subscribeID->first;
  auto trackAlias = quic::decodeQuicInteger(cursor, length);
  if (!trackAlias) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= trackAlias->second;
  objectHeader.trackAlias = trackAlias->first;
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
  auto sendOrder = quic::decodeQuicInteger(cursor, length);
  if (!sendOrder) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= sendOrder->second;
  objectHeader.sendOrder = sendOrder->first;
  if (frameType == FrameType::OBJECT_STREAM) {
    objectHeader.forwardPreference = ForwardPreference::Object;
    // length is not present and runs to the end of the stream
  } else {
    objectHeader.forwardPreference = ForwardPreference::Datagram;
    objectHeader.length = length;
  }
  return objectHeader;
}

folly::Expected<ObjectHeader, ErrorCode> parseStreamHeader(
    folly::io::Cursor& cursor,
    FrameType frameType) noexcept {
  DCHECK(
      frameType == FrameType::STREAM_HEADER_TRACK ||
      frameType == FrameType::STREAM_HEADER_GROUP);
  auto length = cursor.totalLength();
  ObjectHeader objectHeader;
  auto subscribeID = quic::decodeQuicInteger(cursor, length);
  if (!subscribeID) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= subscribeID->second;
  objectHeader.subscribeID = subscribeID->first;
  auto trackAlias = quic::decodeQuicInteger(cursor, length);
  if (!trackAlias) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= trackAlias->second;
  objectHeader.trackAlias = trackAlias->first;
  if (frameType == FrameType::STREAM_HEADER_GROUP) {
    auto group = quic::decodeQuicInteger(cursor, length);
    if (!group) {
      return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
    }
    length -= group->second;
    objectHeader.group = group->first;
    objectHeader.forwardPreference = ForwardPreference::Group;
  } else {
    objectHeader.forwardPreference = ForwardPreference::Track;
  }
  auto sendOrder = quic::decodeQuicInteger(cursor, length);
  if (!sendOrder) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= sendOrder->second;
  objectHeader.sendOrder = sendOrder->first;
  return objectHeader;
}

folly::Expected<ObjectHeader, ErrorCode> parseMultiObjectHeader(
    folly::io::Cursor& cursor,
    FrameType frameType,
    const ObjectHeader& headerTemplate) noexcept {
  DCHECK(
      frameType == FrameType::STREAM_HEADER_TRACK ||
      frameType == FrameType::STREAM_HEADER_GROUP);
  auto length = cursor.totalLength();
  ObjectHeader objectHeader = headerTemplate;
  if (frameType == FrameType::STREAM_HEADER_TRACK) {
    auto group = quic::decodeQuicInteger(cursor, length);
    if (!group) {
      return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
    }
    length -= group->second;
    objectHeader.group = group->first;
    objectHeader.forwardPreference = ForwardPreference::Track;
  } else {
    objectHeader.forwardPreference = ForwardPreference::Group;
  }
  auto id = quic::decodeQuicInteger(cursor, length);
  if (!id) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= id->second;
  objectHeader.id = id->first;
  auto payloadLength = quic::decodeQuicInteger(cursor, length);
  if (!payloadLength) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= payloadLength->second;
  objectHeader.length = payloadLength->first;

  return objectHeader;
}

folly::Expected<folly::Unit, ErrorCode> parseTrackRequestParams(
    folly::io::Cursor& cursor,
    size_t& length,
    size_t numParams,
    std::vector<TrackRequestParameter>& params) {
  for (auto i = 0u; i < numParams; i++) {
    auto key = quic::decodeQuicInteger(cursor, length);
    if (!key) {
      return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
    }
    length -= key->second;
    auto res = parseFixedString(cursor, length);
    if (!res) {
      return folly::makeUnexpected(res.error());
    }
    params.emplace_back(
        TrackRequestParameter({key->first, std::move(res.value())}));
  }
  return folly::unit;
}

folly::Expected<Location, ErrorCode> parseLocation(
    folly::io::Cursor& cursor,
    size_t& length) {
  Location loc;
  auto locType = quic::decodeQuicInteger(cursor, length);
  if (!locType) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  if (locType->first > folly::to_underlying(LocationType::RelativeNext)) {
    return folly::makeUnexpected(ErrorCode::PARSE_ERROR);
  }
  loc.locType = LocationType(locType->first);
  length -= locType->second;
  if (loc.locType != LocationType::None) {
    auto value = quic::decodeQuicInteger(cursor, length);
    if (!value) {
      return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
    }
    loc.value = value->first;
    length -= value->second;
  }
  return loc;
}

folly::Expected<SubscribeRequest, ErrorCode> parseSubscribeRequest(
    folly::io::Cursor& cursor) noexcept {
  auto length = cursor.totalLength();
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
  auto loc = parseLocation(cursor, length);
  if (!loc) {
    return folly::makeUnexpected(loc.error());
  }
  subscribeRequest.startGroup = *loc;
  loc = parseLocation(cursor, length);
  if (!loc) {
    return folly::makeUnexpected(loc.error());
  }
  subscribeRequest.startObject = *loc;
  loc = parseLocation(cursor, length);
  if (!loc) {
    return folly::makeUnexpected(loc.error());
  }
  subscribeRequest.endGroup = *loc;
  loc = parseLocation(cursor, length);
  if (!loc) {
    return folly::makeUnexpected(loc.error());
  }
  subscribeRequest.endObject = *loc;
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

folly::Expected<SubscribeOk, ErrorCode> parseSubscribeOk(
    folly::io::Cursor& cursor) noexcept {
  auto length = cursor.totalLength();
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

  return subscribeOk;
}

folly::Expected<SubscribeError, ErrorCode> parseSubscribeError(
    folly::io::Cursor& cursor) noexcept {
  auto length = cursor.totalLength();
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
  subscribeError.errorCode = errorCode->first;

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
  if (subscribeError.errorCode ==
      folly::to_underlying(SubscribeErrorCode::RETRY_TRACK_ALIAS)) {
    subscribeError.retryAlias = retryAlias->first;
  }

  return subscribeError;
}

folly::Expected<Unsubscribe, ErrorCode> parseUnsubscribe(
    folly::io::Cursor& cursor) noexcept {
  auto length = cursor.totalLength();
  Unsubscribe unsubscribe;
  auto subscribeID = quic::decodeQuicInteger(cursor, length);
  if (!subscribeID) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= subscribeID->second;
  unsubscribe.subscribeID = subscribeID->first;

  return unsubscribe;
}

folly::Expected<SubscribeFin, ErrorCode> parseSubscribeFin(
    folly::io::Cursor& cursor) noexcept {
  auto length = cursor.totalLength();
  SubscribeFin subscribeFin;
  auto subscribeID = quic::decodeQuicInteger(cursor, length);
  if (!subscribeID) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= subscribeID->second;
  subscribeFin.subscribeID = subscribeID->first;

  auto finalGroup = quic::decodeQuicInteger(cursor, length);
  if (!finalGroup) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= finalGroup->second;
  subscribeFin.finalGroup = finalGroup->first;

  auto finalObject = quic::decodeQuicInteger(cursor, length);
  if (!finalObject) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= finalObject->second;
  subscribeFin.finalObject = finalObject->first;

  return subscribeFin;
}

folly::Expected<SubscribeRst, ErrorCode> parseSubscribeRst(
    folly::io::Cursor& cursor) noexcept {
  auto length = cursor.totalLength();
  SubscribeRst subscribeRst;
  auto subscribeID = quic::decodeQuicInteger(cursor, length);
  if (!subscribeID) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= subscribeID->second;
  subscribeRst.subscribeID = subscribeID->first;

  auto errorCode = quic::decodeQuicInteger(cursor, length);
  if (!errorCode) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= errorCode->second;
  subscribeRst.errorCode = errorCode->first;

  auto reas = parseFixedString(cursor, length);
  if (!reas) {
    return folly::makeUnexpected(reas.error());
  }
  subscribeRst.reasonPhrase = std::move(reas.value());

  auto finalGroup = quic::decodeQuicInteger(cursor, length);
  if (!finalGroup) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= finalGroup->second;
  subscribeRst.finalGroup = finalGroup->first;

  auto finalObject = quic::decodeQuicInteger(cursor, length);
  if (!finalObject) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= finalObject->second;
  subscribeRst.finalObject = finalObject->first;

  return subscribeRst;
}

folly::Expected<Announce, ErrorCode> parseAnnounce(
    folly::io::Cursor& cursor) noexcept {
  auto length = cursor.totalLength();
  Announce announce;
  auto res = parseFixedString(cursor, length);
  if (!res) {
    return folly::makeUnexpected(res.error());
  }
  announce.trackNamespace = std::move(res.value());
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
    folly::io::Cursor& cursor) noexcept {
  auto length = cursor.totalLength();
  AnnounceOk announceOk;
  auto res = parseFixedString(cursor, length);
  if (!res) {
    return folly::makeUnexpected(res.error());
  }
  announceOk.trackNamespace = std::move(res.value());
  return announceOk;
}

folly::Expected<AnnounceError, ErrorCode> parseAnnounceError(
    folly::io::Cursor& cursor) noexcept {
  auto length = cursor.totalLength();
  AnnounceError announceError;
  auto res = parseFixedString(cursor, length);
  if (!res) {
    return folly::makeUnexpected(res.error());
  }
  announceError.trackNamespace = std::move(res.value());

  auto errorCode = quic::decodeQuicInteger(cursor, length);
  if (!errorCode) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }
  length -= errorCode->second;
  announceError.errorCode = errorCode->first;

  res = parseFixedString(cursor, length);
  if (!res) {
    return folly::makeUnexpected(res.error());
  }
  announceError.reasonPhrase = std::move(res.value());

  return announceError;
}

folly::Expected<Unannounce, ErrorCode> parseUnannounce(
    folly::io::Cursor& cursor) noexcept {
  auto length = cursor.totalLength();
  Unannounce unannounce;
  auto res = parseFixedString(cursor, length);
  if (!res) {
    return folly::makeUnexpected(res.error());
  }
  unannounce.trackNamespace = std::move(res.value());
  return unannounce;
}

folly::Expected<Goaway, ErrorCode> parseGoaway(
    folly::io::Cursor& cursor) noexcept {
  auto length = cursor.totalLength();
  Goaway goaway;
  auto res = parseFixedString(cursor, length);
  if (!res) {
    return folly::makeUnexpected(res.error());
  }
  goaway.newSessionUri = std::move(res.value());
  return goaway;
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
    bool error) {
  writeVarint(writeBuf, str.size(), size, error);
  if (!error) {
    writeBuf.append(str);
    size += str.size();
  }
}

void writeFullTrackName(
    folly::IOBufQueue& writeBuf,
    const FullTrackName& fullTrackName,
    size_t& size,
    bool error) {
  writeFixedString(writeBuf, fullTrackName.trackNamespace, size, error);
  writeFixedString(writeBuf, fullTrackName.trackName, size, error);
}

void writeLocation(
    folly::IOBufQueue& buf,
    const Location& loc,
    size_t& size,
    bool& error) noexcept {
  writeVarint(buf, folly::to_underlying(loc.locType), size, error);
  if (loc.locType != LocationType::None) {
    writeVarint(buf, loc.value, size, error);
  }
}

WriteResult writeClientSetup(
    folly::IOBufQueue& writeBuf,
    const ClientSetup& clientSetup) noexcept {
  size_t size = 0;
  bool error = false;
  writeVarint(
      writeBuf, folly::to_underlying(FrameType::CLIENT_SETUP), size, error);

  writeVarint(writeBuf, clientSetup.supportedVersions.size(), size, error);
  for (auto version : clientSetup.supportedVersions) {
    writeVarint(writeBuf, version, size, error);
  }

  writeVarint(writeBuf, clientSetup.params.size(), size, error);
  for (auto& param : clientSetup.params) {
    writeVarint(writeBuf, param.key, size, error);
    if (param.key == folly::to_underlying(SetupKey::ROLE)) {
      CHECK_LE(param.asUint64, folly::to_underlying(Role::BOTH));
      writeVarint(writeBuf, 1, size, error);
      writeVarint(writeBuf, param.asUint64, size, error);
    } else {
      writeFixedString(writeBuf, param.asString, size, error);
    }
  }
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
  writeVarint(
      writeBuf, folly::to_underlying(FrameType::SERVER_SETUP), size, error);
  writeVarint(writeBuf, serverSetup.selectedVersion, size, error);
  writeVarint(writeBuf, serverSetup.params.size(), size, error);
  for (auto& param : serverSetup.params) {
    writeVarint(writeBuf, param.key, size, error);
    if (param.key == folly::to_underlying(SetupKey::ROLE)) {
      CHECK_LE(param.asUint64, folly::to_underlying(Role::BOTH));
      writeVarint(writeBuf, 1, size, error);
      writeVarint(writeBuf, param.asUint64, size, error);
    } else {
      writeFixedString(writeBuf, param.asString, size, error);
    }
  }
  if (error) {
    return folly::makeUnexpected(quic::TransportErrorCode::INTERNAL_ERROR);
  }
  return size;
}

WriteResult writeStreamHeader(
    folly::IOBufQueue& writeBuf,
    const ObjectHeader& objectHeader) noexcept {
  size_t size = 0;
  bool error = false;

  if (objectHeader.forwardPreference == ForwardPreference::Track) {
    writeVarint(
        writeBuf,
        folly::to_underlying(FrameType::STREAM_HEADER_TRACK),
        size,
        error);
  } else if (objectHeader.forwardPreference == ForwardPreference::Group) {
    writeVarint(
        writeBuf,
        folly::to_underlying(FrameType::STREAM_HEADER_GROUP),
        size,
        error);
  } else {
    LOG(FATAL) << "Unsupported forward preference to stream header";
  }
  writeVarint(writeBuf, objectHeader.subscribeID, size, error);
  writeVarint(writeBuf, objectHeader.trackAlias, size, error);
  if (objectHeader.forwardPreference == ForwardPreference::Group) {
    writeVarint(writeBuf, objectHeader.group, size, error);
  }
  writeVarint(writeBuf, objectHeader.sendOrder, size, error);
  if (error) {
    return folly::makeUnexpected(quic::TransportErrorCode::INTERNAL_ERROR);
  }
  return size;
}

WriteResult writeObject(
    folly::IOBufQueue& writeBuf,
    const ObjectHeader& objectHeader,
    std::unique_ptr<folly::IOBuf> objectPayload) noexcept {
  size_t size = 0;
  bool error = false;
  bool multiObject = true;
  if (objectHeader.forwardPreference == ForwardPreference::Object) {
    writeVarint(
        writeBuf, folly::to_underlying(FrameType::OBJECT_STREAM), size, error);
    multiObject = false;

  } else if (objectHeader.forwardPreference == ForwardPreference::Datagram) {
    writeVarint(
        writeBuf,
        folly::to_underlying(FrameType::OBJECT_PREFER_DATAGRAM),
        size,
        error);
    multiObject = false;
  }
  if (!multiObject) {
    writeVarint(writeBuf, objectHeader.subscribeID, size, error);
    writeVarint(writeBuf, objectHeader.trackAlias, size, error);
  }
  if (objectHeader.forwardPreference != ForwardPreference::Group) {
    writeVarint(writeBuf, objectHeader.group, size, error);
  }
  writeVarint(writeBuf, objectHeader.id, size, error);
  if (!multiObject) {
    writeVarint(writeBuf, objectHeader.sendOrder, size, error);
  } else {
    CHECK(objectHeader.length) << "Multi-object streams require known length";
    writeVarint(writeBuf, *objectHeader.length, size, error);
  }
  if (error) {
    return folly::makeUnexpected(quic::TransportErrorCode::INTERNAL_ERROR);
  }
  if (objectPayload) {
    writeBuf.append(std::move(objectPayload));
  }
  return size;
}

WriteResult writeSubscribeRequest(
    folly::IOBufQueue& writeBuf,
    const SubscribeRequest& subscribeRequest) noexcept {
  size_t size = 0;
  bool error = false;
  writeVarint(
      writeBuf, folly::to_underlying(FrameType::SUBSCRIBE), size, error);
  writeVarint(writeBuf, subscribeRequest.subscribeID, size, error);
  writeVarint(writeBuf, subscribeRequest.trackAlias, size, error);
  writeFullTrackName(writeBuf, subscribeRequest.fullTrackName, size, error);
  writeLocation(writeBuf, subscribeRequest.startGroup, size, error);
  writeLocation(writeBuf, subscribeRequest.startObject, size, error);
  writeLocation(writeBuf, subscribeRequest.endGroup, size, error);
  writeLocation(writeBuf, subscribeRequest.endObject, size, error);
  writeVarint(writeBuf, subscribeRequest.params.size(), size, error);
  for (auto& param : subscribeRequest.params) {
    writeVarint(writeBuf, param.key, size, error);
    writeFixedString(writeBuf, param.value, size, error);
  }
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
  writeVarint(
      writeBuf, folly::to_underlying(FrameType::SUBSCRIBE_OK), size, error);
  writeVarint(writeBuf, subscribeOk.subscribeID, size, error);
  writeVarint(writeBuf, subscribeOk.expires.count(), size, error);
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
  writeVarint(
      writeBuf, folly::to_underlying(FrameType::SUBSCRIBE_ERROR), size, error);
  writeVarint(writeBuf, subscribeError.subscribeID, size, error);
  writeVarint(writeBuf, subscribeError.errorCode, size, error);
  writeFixedString(writeBuf, subscribeError.reasonPhrase, size, error);
  writeVarint(writeBuf, subscribeError.retryAlias.value_or(0), size, error);
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
  writeVarint(
      writeBuf, folly::to_underlying(FrameType::UNSUBSCRIBE), size, error);
  writeVarint(writeBuf, unsubscribe.subscribeID, size, error);
  if (error) {
    return folly::makeUnexpected(quic::TransportErrorCode::INTERNAL_ERROR);
  }
  return size;
}

WriteResult writeSubscribeFin(
    folly::IOBufQueue& writeBuf,
    const SubscribeFin& subscribeFin) noexcept {
  size_t size = 0;
  bool error = false;
  writeVarint(
      writeBuf, folly::to_underlying(FrameType::SUBSCRIBE_FIN), size, error);
  writeVarint(writeBuf, subscribeFin.subscribeID, size, error);
  writeVarint(writeBuf, subscribeFin.finalGroup, size, error);
  writeVarint(writeBuf, subscribeFin.finalObject, size, error);
  if (error) {
    return folly::makeUnexpected(quic::TransportErrorCode::INTERNAL_ERROR);
  }
  return size;
}

WriteResult writeSubscribeRst(
    folly::IOBufQueue& writeBuf,
    const SubscribeRst& subscribeRst) noexcept {
  size_t size = 0;
  bool error = false;
  writeVarint(
      writeBuf, folly::to_underlying(FrameType::SUBSCRIBE_RST), size, error);
  writeVarint(writeBuf, subscribeRst.subscribeID, size, error);
  writeVarint(writeBuf, subscribeRst.errorCode, size, error);
  writeFixedString(writeBuf, subscribeRst.reasonPhrase, size, error);
  writeVarint(writeBuf, subscribeRst.finalGroup, size, error);
  writeVarint(writeBuf, subscribeRst.finalObject, size, error);
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
  writeVarint(writeBuf, folly::to_underlying(FrameType::ANNOUNCE), size, error);
  writeFixedString(writeBuf, announce.trackNamespace, size, error);
  writeVarint(writeBuf, announce.params.size(), size, error);
  for (auto& param : announce.params) {
    writeVarint(writeBuf, param.key, size, error);
    writeFixedString(writeBuf, param.value, size, error);
  }
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
  writeVarint(
      writeBuf, folly::to_underlying(FrameType::ANNOUNCE_OK), size, error);
  writeFixedString(writeBuf, announceOk.trackNamespace, size, error);
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
  writeVarint(
      writeBuf, folly::to_underlying(FrameType::ANNOUNCE_ERROR), size, error);
  writeFixedString(writeBuf, announceError.trackNamespace, size, error);
  writeVarint(writeBuf, announceError.errorCode, size, error);
  writeFixedString(writeBuf, announceError.reasonPhrase, size, error);
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
  writeVarint(
      writeBuf, folly::to_underlying(FrameType::UNANNOUNCE), size, error);
  writeFixedString(writeBuf, unannounce.trackNamespace, size, error);
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
  writeVarint(writeBuf, folly::to_underlying(FrameType::GOAWAY), size, error);
  writeFixedString(writeBuf, goaway.newSessionUri, size, error);
  if (error) {
    return folly::makeUnexpected(quic::TransportErrorCode::INTERNAL_ERROR);
  }
  return size;
}

const char* getFrameTypeString(FrameType type) {
  switch (type) {
    case FrameType::OBJECT_STREAM:
      return "OBJECT_STREAM";
    case FrameType::OBJECT_PREFER_DATAGRAM:
      return "OBJECT_PREFER_DATAGRAM";
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
    case FrameType::SUBSCRIBE_FIN:
      return "SUBSCRIBE_FIN";
    case FrameType::SUBSCRIBE_RST:
      return "SUBSCRIBE_RST";
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
    case FrameType::STREAM_HEADER_TRACK:
      return "STREAM_HEADER_TRACK";
    case FrameType::STREAM_HEADER_GROUP:
      return "STREAM_HEADER_GROUP";
    default:
      // can happen when type was cast from uint8_t
      return "Unknown";
  }
  LOG(FATAL) << "Unreachable";
  return "";
}

std::ostream& operator<<(std::ostream& os, FrameType type) {
  os << getFrameTypeString(type);
  return os;
}

} // namespace moxygen

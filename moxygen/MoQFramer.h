/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/Optional.h>
#include <folly/Range.h>
#include <folly/hash/Hash.h>
#include <folly/io/Cursor.h>
#include <folly/io/IOBufQueue.h>

#include <quic/codec/QuicInteger.h>
#include <vector>

namespace moxygen {

//////// Constants ////////
const size_t kMaxFrameHeaderSize = 32;

//////// Types ////////

enum class ErrorCode : uint32_t {
  UNKNOWN = 0,
  PARSE_ERROR = 1,
  PARSE_UNDERFLOW = 2,
  INVALID_MESSAGE = 3,
  UNSUPPORTED_VERSION = 4
};

enum class SessionCloseErrorCode : uint32_t {
  NO_ERROR = 0,
  INTERNAL_ERROR = 1,
  UNAUTHORIZED = 2,
  PROTOCOL_VIOLATION = 3,
  DUPLICATE_TRACK_ALIAS = 4,
  PARAMETER_LENGTH_MISMATCH = 5,
  GOAWAY_TIMEOUT = 0x10
};

enum class SubscribeErrorCode : uint32_t {
  INTERNAL_ERROR = 0,
  INVALID_RANGE = 1,
  RETRY_TRACK_ALIAS = 2,
};

enum class SubscribeDoneStatusCode : uint32_t {
  UNSUBSCRIBED = 0x0,
  INTERNAL_ERROR = 0x1,
  UNAUTHORIZED = 0x2,
  TRACK_ENDED = 0x3,
  SUBSCRIPTION_ENDED = 0x4,
  GOING_AWAY = 0x5,
  EXPIRED = 0x6
};

enum class TrackStatusCode : uint32_t {
  IN_PROGRESS = 0x0,
  TRACK_NOT_EXIST = 0x1,
  TRACK_NOT_STARTED = 0x2,
  TRACK_ENDED = 0x3,
  UNKNOWN = 0x4
};

using WriteResult = folly::Expected<size_t, quic::TransportErrorCode>;

enum class FrameType : uint64_t {
  OBJECT_STREAM = 0,
  OBJECT_DATAGRAM = 1,
  SUBSCRIBE_UPDATE = 2,
  SUBSCRIBE = 3,
  SUBSCRIBE_OK = 4,
  SUBSCRIBE_ERROR = 5,
  ANNOUNCE = 6,
  ANNOUNCE_OK = 7,
  ANNOUNCE_ERROR = 8,
  UNANNOUNCE = 9,
  UNSUBSCRIBE = 0xA,
  SUBSCRIBE_DONE = 0xB,
  ANNOUNCE_CANCEL = 0xC,
  TRACK_STATUS_REQUEST = 0xD,
  TRACK_STATUS = 0xE,
  GOAWAY = 0x10,
  CLIENT_SETUP = 0x40,
  SERVER_SETUP = 0x41,
  STREAM_HEADER_TRACK = 0x50,
  STREAM_HEADER_GROUP = 0x51,
};

std::ostream& operator<<(std::ostream& os, FrameType type);

enum class SetupKey : uint64_t { ROLE = 0, PATH = 1 };

enum class Role : uint8_t { PUBLISHER = 1, SUBSCRIBER = 2, PUB_AND_SUB = 3 };

struct SetupParameter {
  uint64_t key;
  std::string asString;
  uint64_t asUint64;
};

constexpr uint64_t kVersionDraft01 = 0xff000001;
constexpr uint64_t kVersionDraft02 = 0xff000002;
constexpr uint64_t kVersionDraft03 = 0xff000003;
constexpr uint64_t kVersionDraft04 = 0xff000004;
constexpr uint64_t kVersionDraft05 = 0xff000005;
constexpr uint64_t kVersionDraftCurrent = kVersionDraft05;

struct ClientSetup {
  std::vector<uint64_t> supportedVersions;
  std::vector<SetupParameter> params;
};

struct ServerSetup {
  uint64_t selectedVersion;
  std::vector<SetupParameter> params;
};

folly::Expected<ClientSetup, ErrorCode> parseClientSetup(
    folly::io::Cursor& cursor) noexcept;

folly::Expected<ServerSetup, ErrorCode> parseServerSetup(
    folly::io::Cursor& cursor) noexcept;

enum class ForwardPreference : uint8_t { Track, Group, Object, Datagram };

enum class ObjectStatus : uint64_t {
  NORMAL = 0,
  OBJECT_NOT_EXIST = 1,
  GROUP_NOT_EXIST = 2,
  END_OF_GROUP = 3,
  END_OF_TRACK_AND_GROUP = 4
};

struct ObjectHeader {
  uint64_t subscribeID;
  uint64_t trackAlias;
  uint64_t group;
  uint64_t id;
  uint64_t priority;
  ForwardPreference forwardPreference;
  ObjectStatus status{ObjectStatus::NORMAL};
  folly::Optional<uint64_t> length{folly::none};
};

folly::Expected<ObjectHeader, ErrorCode> parseObjectHeader(
    folly::io::Cursor& cursor,
    FrameType frameType) noexcept;

folly::Expected<ObjectHeader, ErrorCode> parseStreamHeader(
    folly::io::Cursor& cursor,
    FrameType frameType) noexcept;

folly::Expected<ObjectHeader, ErrorCode> parseMultiObjectHeader(
    folly::io::Cursor& cursor,
    FrameType frameType,
    const ObjectHeader& headerTemplate) noexcept;

enum class TrackRequestParamKey : uint64_t {
  AUTHORIZATION = 2,
};

struct TrackRequestParameter {
  uint64_t key;
  std::string value;
};

enum class LocationType : uint8_t {
  LatestGroup = 1,
  LatestObject = 2,
  AbsoluteStart = 3,
  AbsoluteRange = 4
};

struct AbsoluteLocation {
  uint64_t group{0};
  uint64_t object{0};

  AbsoluteLocation() = default;
  constexpr AbsoluteLocation(uint64_t g, uint64_t o) : group(g), object(o) {}

  std::strong_ordering operator<=>(const AbsoluteLocation& other) const {
    if (group < other.group) {
      return std::strong_ordering::less;
    } else if (group == other.group) {
      if (object < other.object) {
        return std::strong_ordering::less;
      } else if (object == other.object) {
        return std::strong_ordering::equivalent;
      } else {
        return std::strong_ordering::greater;
      }
    } else {
      return std::strong_ordering::greater;
    }
  }
};

constexpr AbsoluteLocation kLocationMin;
constexpr AbsoluteLocation kLocationMax{
    std::numeric_limits<uint64_t>::max(),
    std::numeric_limits<uint64_t>::max()};

struct FullTrackName {
  std::string trackNamespace;
  std::string trackName;
  bool operator==(const FullTrackName& other) const {
    return trackNamespace == other.trackNamespace &&
        trackName == other.trackName;
  }
  bool operator<(const FullTrackName& other) const {
    return trackNamespace < other.trackNamespace ||
        (trackNamespace == other.trackNamespace && trackName < other.trackName);
  }
  struct hash {
    size_t operator()(const FullTrackName& ftn) const {
      return folly::hash::hash_combine(ftn.trackNamespace, ftn.trackName);
    }
  };
};

enum class GroupOrder : uint8_t {
  Default = 0x0,
  OldestFirst = 0x1,
  NewestFirst = 0x2
};

struct SubscribeRequest {
  uint64_t subscribeID;
  uint64_t trackAlias;
  FullTrackName fullTrackName;
  uint8_t priority;
  GroupOrder groupOrder;
  LocationType locType;
  folly::Optional<AbsoluteLocation> start;
  folly::Optional<AbsoluteLocation> end;
  std::vector<TrackRequestParameter> params;
};

folly::Expected<SubscribeRequest, ErrorCode> parseSubscribeRequest(
    folly::io::Cursor& cursor) noexcept;

struct SubscribeUpdate {
  uint64_t subscribeID;
  AbsoluteLocation start;
  AbsoluteLocation end;
  uint8_t priority;
  std::vector<TrackRequestParameter> params;
};

folly::Expected<SubscribeUpdate, ErrorCode> parseSubscribeUpdate(
    folly::io::Cursor& cursor) noexcept;

struct SubscribeOk {
  uint64_t subscribeID;
  std::chrono::milliseconds expires;
  GroupOrder groupOrder;
  // context exists is inferred from presence of latest
  folly::Optional<AbsoluteLocation> latest;
};

folly::Expected<SubscribeOk, ErrorCode> parseSubscribeOk(
    folly::io::Cursor& cursor) noexcept;

struct SubscribeError {
  uint64_t subscribeID;
  uint64_t errorCode;
  std::string reasonPhrase;
  folly::Optional<uint64_t> retryAlias{folly::none};
};

folly::Expected<SubscribeError, ErrorCode> parseSubscribeError(
    folly::io::Cursor& cursor) noexcept;

struct Unsubscribe {
  uint64_t subscribeID;
};

folly::Expected<Unsubscribe, ErrorCode> parseUnsubscribe(
    folly::io::Cursor& cursor) noexcept;

struct SubscribeDone {
  uint64_t subscribeID;
  SubscribeDoneStatusCode statusCode;
  std::string reasonPhrase;
  folly::Optional<AbsoluteLocation> finalObject;
};

folly::Expected<SubscribeDone, ErrorCode> parseSubscribeDone(
    folly::io::Cursor& cursor) noexcept;

struct Announce {
  std::string trackNamespace;
  std::vector<TrackRequestParameter> params;
};

folly::Expected<Announce, ErrorCode> parseAnnounce(
    folly::io::Cursor& cursor) noexcept;

struct AnnounceOk {
  std::string trackNamespace;
};

folly::Expected<AnnounceOk, ErrorCode> parseAnnounceOk(
    folly::io::Cursor& cursor) noexcept;

struct AnnounceError {
  std::string trackNamespace;
  uint64_t errorCode;
  std::string reasonPhrase;
};

folly::Expected<AnnounceError, ErrorCode> parseAnnounceError(
    folly::io::Cursor& cursor) noexcept;

struct Unannounce {
  std::string trackNamespace;
};

folly::Expected<Unannounce, ErrorCode> parseUnannounce(
    folly::io::Cursor& cursor) noexcept;

struct AnnounceCancel {
  std::string trackNamespace;
};

folly::Expected<AnnounceCancel, ErrorCode> parseAnnounceCancel(
    folly::io::Cursor& cursor) noexcept;

struct TrackStatusRequest {
  FullTrackName fullTrackName;
};

folly::Expected<TrackStatusRequest, ErrorCode> parseTrackStatusRequest(
    folly::io::Cursor& cursor) noexcept;

struct TrackStatus {
  FullTrackName fullTrackName;
  TrackStatusCode statusCode;
  folly::Optional<AbsoluteLocation> latestGroupAndObject;
};

folly::Expected<TrackStatus, ErrorCode> parseTrackStatus(
    folly::io::Cursor& cursor) noexcept;

struct Goaway {
  std::string newSessionUri;
};

folly::Expected<Goaway, ErrorCode> parseGoaway(
    folly::io::Cursor& cursor) noexcept;

//// Egress ////

WriteResult writeClientSetup(
    folly::IOBufQueue& writeBuf,
    const ClientSetup& clientSetup) noexcept;

WriteResult writeServerSetup(
    folly::IOBufQueue& writeBuf,
    const ServerSetup& serverSetup) noexcept;

WriteResult writeStreamHeader(
    folly::IOBufQueue& writeBuf,
    const ObjectHeader& objectHeader) noexcept;

WriteResult writeObject(
    folly::IOBufQueue& writeBuf,
    const ObjectHeader& objectHeader,
    std::unique_ptr<folly::IOBuf> objectPayload) noexcept;

WriteResult writeSubscribeRequest(
    folly::IOBufQueue& writeBuf,
    const SubscribeRequest& subscribeRequest) noexcept;

WriteResult writeSubscribeUpdate(
    folly::IOBufQueue& writeBuf,
    const SubscribeUpdate& update) noexcept;

WriteResult writeSubscribeOk(
    folly::IOBufQueue& writeBuf,
    const SubscribeOk& subscribeOk) noexcept;

WriteResult writeSubscribeError(
    folly::IOBufQueue& writeBuf,
    const SubscribeError& subscribeError) noexcept;

WriteResult writeSubscribeDone(
    folly::IOBufQueue& writeBuf,
    const SubscribeDone& subscribeDone) noexcept;

WriteResult writeUnsubscribe(
    folly::IOBufQueue& writeBuf,
    const Unsubscribe& unsubscribe) noexcept;

WriteResult writeAnnounce(
    folly::IOBufQueue& writeBuf,
    const Announce& announce) noexcept;

WriteResult writeAnnounceOk(
    folly::IOBufQueue& writeBuf,
    const AnnounceOk& announceOk) noexcept;

WriteResult writeAnnounceError(
    folly::IOBufQueue& writeBuf,
    const AnnounceError& announceError) noexcept;

WriteResult writeUnannounce(
    folly::IOBufQueue& writeBuf,
    const Unannounce& unannounce) noexcept;

WriteResult writeAnnounceCancel(
    folly::IOBufQueue& writeBuf,
    const AnnounceCancel& announceCancel) noexcept;

WriteResult writeTrackStatusRequest(
    folly::IOBufQueue& writeBuf,
    const TrackStatusRequest& trackStatusRequest) noexcept;

WriteResult writeTrackStatus(
    folly::IOBufQueue& writeBuf,
    const TrackStatus& trackStatus) noexcept;

WriteResult writeGoaway(
    folly::IOBufQueue& writeBuf,
    const Goaway& goaway) noexcept;

} // namespace moxygen

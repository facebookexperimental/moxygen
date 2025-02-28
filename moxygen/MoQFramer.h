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
#include <folly/logging/xlog.h>

#include <quic/codec/QuicInteger.h>
#include <vector>

namespace moxygen {

//////// Constants ////////
const size_t kMaxFrameHeaderSize = 32;
const size_t kMaxNamespaceLength = 32;

//////// Types ////////

using Payload = std::unique_ptr<folly::IOBuf>;
using Priority = uint8_t;

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
  TOO_MANY_SUBSCRIBES = 0x6,
  GOAWAY_TIMEOUT = 0x10
};

enum class SubscribeErrorCode : uint32_t {
  INTERNAL_ERROR = 0,
  INVALID_RANGE = 1,
  RETRY_TRACK_ALIAS = 2,
  TRACK_NOT_EXIST = 3,
  UNAUTHORIZED = 4,
  TIMEOUT = 5,
};

enum class SubscribeDoneStatusCode : uint32_t {
  UNSUBSCRIBED = 0x0,
  INTERNAL_ERROR = 0x1,
  UNAUTHORIZED = 0x2,
  TRACK_ENDED = 0x3,
  SUBSCRIPTION_ENDED = 0x4,
  GOING_AWAY = 0x5,
  EXPIRED = 0x6,
  //
  SESSION_CLOSED = std::numeric_limits<uint32_t>::max()
};

enum class TrackStatusCode : uint32_t {
  IN_PROGRESS = 0x0,
  TRACK_NOT_EXIST = 0x1,
  TRACK_NOT_STARTED = 0x2,
  TRACK_ENDED = 0x3,
  UNKNOWN = 0x4
};

enum class FetchErrorCode : uint32_t {
  INTERNAL_ERROR = 0,
  INVALID_RANGE = 1,
  TRACK_NOT_EXIST = 2,
  UNAUTHORIZED = 3,
  TIMEOUT = 4,
};

enum class ResetStreamErrorCode : uint32_t {
  INTERNAL_ERROR = 0,
  DELIVERY_TIMEOUT = 1,
  SESSION_CLOSED = 2,
  CANCELLED = 3, // received UNSUBSCRIBE / FETCH_CANCEL / STOP_SENDING
};

using WriteResult = folly::Expected<size_t, quic::TransportErrorCode>;

enum class FrameType : uint64_t {
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
  SUBSCRIBE_ANNOUNCES = 0x11,
  SUBSCRIBE_ANNOUNCES_OK = 0x12,
  SUBSCRIBE_ANNOUNCES_ERROR = 0x13,
  UNSUBSCRIBE_ANNOUNCES = 0x14,
  MAX_SUBSCRIBE_ID = 0x15,
  FETCH = 0x16,
  FETCH_CANCEL = 0x17,
  FETCH_OK = 0x18,
  FETCH_ERROR = 0x19,
  SUBSCRIBES_BLOCKED = 0x1A,
  CLIENT_SETUP = 0x40,
  SERVER_SETUP = 0x41,
};

enum class StreamType : uint64_t {
  OBJECT_DATAGRAM = 1,
  SUBGROUP_HEADER = 0x4,
  FETCH_HEADER = 0x5,
};

std::ostream& operator<<(std::ostream& os, FrameType type);

std::ostream& operator<<(std::ostream& os, StreamType type);

enum class SetupKey : uint64_t {
  PATH = 1,
  MAX_SUBSCRIBE_ID = 2,
};

struct Parameter {
  uint64_t key;
  std::string asString;
  uint64_t asUint64;
};

struct SetupParameter : public Parameter {};
struct TrackRequestParameter : public Parameter {};

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
// SUBSCRIBE_DONE stream count
constexpr uint64_t kVersionDraft08_exp2 = 0xff080002;
constexpr uint64_t kVersionDraftCurrent = kVersionDraft08_exp2;

struct ClientSetup {
  std::vector<uint64_t> supportedVersions;
  std::vector<SetupParameter> params;
};

struct ServerSetup {
  uint64_t selectedVersion;
  std::vector<SetupParameter> params;
};

folly::Expected<ClientSetup, ErrorCode> parseClientSetup(
    folly::io::Cursor& cursor,
    size_t length) noexcept;

folly::Expected<ServerSetup, ErrorCode> parseServerSetup(
    folly::io::Cursor& cursor,
    size_t length) noexcept;

enum class ObjectStatus : uint64_t {
  NORMAL = 0,
  OBJECT_NOT_EXIST = 1,
  GROUP_NOT_EXIST = 2,
  END_OF_GROUP = 3,
  END_OF_TRACK_AND_GROUP = 4,
  END_OF_SUBGROUP = 5,
};

std::ostream& operator<<(std::ostream& os, ObjectStatus type);

struct TrackAlias {
  /* implicit */ TrackAlias(uint64_t v) : value(v) {}
  TrackAlias() = default;
  uint64_t value{0};
  bool operator==(const TrackAlias& a) const {
    return value == a.value;
  }
  struct hash {
    size_t operator()(const TrackAlias& a) const {
      return std::hash<uint64_t>{}(a.value);
    }
  };
};
std::ostream& operator<<(std::ostream& os, TrackAlias alias);

struct SubscribeID {
  /* implicit */ SubscribeID(uint64_t v) : value(v) {}
  SubscribeID() = default;
  uint64_t value{0};
  bool operator==(const SubscribeID& s) const {
    return value == s.value;
  }
  std::strong_ordering operator<=>(const SubscribeID& other) const {
    return value <=> other.value;
  }
  struct hash {
    size_t operator()(const SubscribeID& s) const {
      return std::hash<uint64_t>{}(s.value);
    }
  };
};
std::ostream& operator<<(std::ostream& os, SubscribeID id);

struct UnitializedIdentifier {
  bool operator==(const UnitializedIdentifier&) const {
    return false;
  }
};
using TrackIdentifier =
    std::variant<UnitializedIdentifier, TrackAlias, SubscribeID>;
struct TrackIdentifierHash {
  size_t operator()(const TrackIdentifier& trackIdentifier) const {
    XCHECK_GT(trackIdentifier.index(), 0llu);
    auto trackAlias = std::get_if<TrackAlias>(&trackIdentifier);
    if (trackAlias) {
      return folly::hash::hash_combine(
          trackIdentifier.index(), trackAlias->value);
    } else {
      return folly::hash::hash_combine(
          trackIdentifier.index(),
          std::get<SubscribeID>(trackIdentifier).value);
    }
  }
};

inline uint64_t value(const TrackIdentifier& trackIdentifier) {
  return std::visit(
      [](const auto& value) {
        using T = std::decay_t<decltype(value)>;
        if constexpr (std::is_same_v<T, TrackAlias>) {
          return value.value;
        } else if constexpr (std::is_same_v<T, SubscribeID>) {
          return value.value;
        }
        return std::numeric_limits<uint64_t>::max();
      },
      trackIdentifier);
}
struct ObjectHeader {
  TrackIdentifier trackIdentifier;
  uint64_t group;
  uint64_t subgroup{0}; // meaningless for Track and Datagram
  uint64_t id;
  uint8_t priority;
  ObjectStatus status{ObjectStatus::NORMAL};
  folly::Optional<uint64_t> length{folly::none};
};

std::ostream& operator<<(std::ostream& os, const ObjectHeader& type);

// datagram only
folly::Expected<ObjectHeader, ErrorCode> parseObjectHeader(
    folly::io::Cursor& cursor,
    size_t length) noexcept;

folly::Expected<uint64_t, ErrorCode> parseFetchHeader(
    folly::io::Cursor& cursor) noexcept;

folly::Expected<ObjectHeader, ErrorCode> parseSubgroupHeader(
    folly::io::Cursor& cursor) noexcept;

folly::Expected<ObjectHeader, ErrorCode> parseMultiObjectHeader(
    folly::io::Cursor& cursor,
    StreamType streamType,
    const ObjectHeader& headerTemplate) noexcept;

enum class TrackRequestParamKey : uint64_t {
  AUTHORIZATION = 2,
  DELIVERY_TIMEOUT = 3,
  MAX_CACHE_DURATION = 4,
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

struct TrackNamespace {
  std::vector<std::string> trackNamespace;

  TrackNamespace() = default;
  explicit TrackNamespace(std::vector<std::string> tns) {
    trackNamespace = std::move(tns);
  }
  explicit TrackNamespace(std::string tns, std::string delimiter) {
    folly::split(delimiter, tns, trackNamespace);
  }

  bool operator==(const TrackNamespace& other) const {
    return trackNamespace == other.trackNamespace;
  }
  bool operator<(const TrackNamespace& other) const {
    return trackNamespace < other.trackNamespace;
  }
  const std::string& operator[](size_t i) const {
    return trackNamespace[i];
  }
  struct hash {
    size_t operator()(const TrackNamespace& tn) const {
      return folly::hash::hash_range(
          tn.trackNamespace.begin(), tn.trackNamespace.end());
    }
  };
  friend std::ostream& operator<<(
      std::ostream& os,
      const TrackNamespace& trackNs) {
    if (trackNs.trackNamespace.empty()) {
      return os;
    }

    // Iterate through all elements except the last one
    for (size_t i = 0; i < trackNs.trackNamespace.size() - 1; ++i) {
      os << trackNs.trackNamespace[i] << '/';
    }

    // Add the last element without a trailing slash
    os << trackNs.trackNamespace.back();
    return os;
  }
  bool empty() const {
    return trackNamespace.empty() ||
        (trackNamespace.size() == 1 && trackNamespace[0].empty());
  }
  size_t size() const {
    return trackNamespace.size();
  }
  void append(std::string token) {
    trackNamespace.emplace_back(std::move(token));
  }
  bool startsWith(const TrackNamespace& other) const {
    if (other.trackNamespace.size() > trackNamespace.size()) {
      return false;
    }
    for (size_t i = 0; i < other.trackNamespace.size(); ++i) {
      if (other.trackNamespace[i] != trackNamespace[i]) {
        return false;
      }
    }
    return true;
  }
  void trimEnd() {
    CHECK_GT(size(), 0);
    trackNamespace.pop_back();
  }
};

struct FullTrackName {
  TrackNamespace trackNamespace;
  std::string trackName;
  bool operator==(const FullTrackName& other) const {
    return trackNamespace == other.trackNamespace &&
        trackName == other.trackName;
  }
  bool operator<(const FullTrackName& other) const {
    return trackNamespace < other.trackNamespace ||
        (trackNamespace == other.trackNamespace && trackName < other.trackName);
  }
  friend std::ostream& operator<<(std::ostream& os, const FullTrackName& ftn) {
    if (ftn.trackNamespace.empty()) {
      return os << ftn.trackName;
    }
    return os << ftn.trackNamespace << '/' << ftn.trackName;
  }
  struct hash {
    size_t operator()(const FullTrackName& ftn) const {
      return folly::hash::hash_combine(
          TrackNamespace::hash()(ftn.trackNamespace), ftn.trackName);
    }
  };
};

enum class GroupOrder : uint8_t {
  Default = 0x0,
  OldestFirst = 0x1,
  NewestFirst = 0x2
};

struct SubscribeRequest {
  SubscribeID subscribeID;
  TrackAlias trackAlias;
  FullTrackName fullTrackName;
  uint8_t priority;
  GroupOrder groupOrder;
  LocationType locType;
  folly::Optional<AbsoluteLocation> start;
  folly::Optional<AbsoluteLocation> end;
  std::vector<TrackRequestParameter> params;
};

folly::Expected<SubscribeRequest, ErrorCode> parseSubscribeRequest(
    folly::io::Cursor& cursor,
    size_t length) noexcept;

struct SubscribeUpdate {
  SubscribeID subscribeID;
  AbsoluteLocation start;
  AbsoluteLocation end;
  uint8_t priority;
  std::vector<TrackRequestParameter> params;
};

folly::Expected<SubscribeUpdate, ErrorCode> parseSubscribeUpdate(
    folly::io::Cursor& cursor,
    size_t length) noexcept;

struct SubscribeOk {
  SubscribeID subscribeID;
  std::chrono::milliseconds expires;
  GroupOrder groupOrder;
  // context exists is inferred from presence of latest
  folly::Optional<AbsoluteLocation> latest;
  std::vector<TrackRequestParameter> params;
};

folly::Expected<SubscribeOk, ErrorCode> parseSubscribeOk(
    folly::io::Cursor& cursor,
    size_t length) noexcept;

struct SubscribeError {
  SubscribeID subscribeID;
  uint64_t errorCode;
  std::string reasonPhrase;
  folly::Optional<uint64_t> retryAlias{folly::none};
};

folly::Expected<SubscribeError, ErrorCode> parseSubscribeError(
    folly::io::Cursor& cursor,
    size_t length) noexcept;

struct Unsubscribe {
  SubscribeID subscribeID;
};

folly::Expected<Unsubscribe, ErrorCode> parseUnsubscribe(
    folly::io::Cursor& cursor,
    size_t length) noexcept;

struct SubscribeDone {
  SubscribeID subscribeID;
  SubscribeDoneStatusCode statusCode;
  uint64_t streamCount;
  std::string reasonPhrase;
  folly::Optional<AbsoluteLocation> finalObject;
};

folly::Expected<SubscribeDone, ErrorCode> parseSubscribeDone(
    folly::io::Cursor& cursor,
    size_t length) noexcept;

struct Announce {
  TrackNamespace trackNamespace;
  std::vector<TrackRequestParameter> params;
};

folly::Expected<Announce, ErrorCode> parseAnnounce(
    folly::io::Cursor& cursor,
    size_t length) noexcept;

struct AnnounceOk {
  TrackNamespace trackNamespace;
};

folly::Expected<AnnounceOk, ErrorCode> parseAnnounceOk(
    folly::io::Cursor& cursor,
    size_t length) noexcept;

struct AnnounceError {
  TrackNamespace trackNamespace;
  uint64_t errorCode;
  std::string reasonPhrase;
};

folly::Expected<AnnounceError, ErrorCode> parseAnnounceError(
    folly::io::Cursor& cursor,
    size_t length) noexcept;

struct Unannounce {
  TrackNamespace trackNamespace;
};

folly::Expected<Unannounce, ErrorCode> parseUnannounce(
    folly::io::Cursor& cursor,
    size_t length) noexcept;

struct AnnounceCancel {
  TrackNamespace trackNamespace;
  uint64_t errorCode;
  std::string reasonPhrase;
};

folly::Expected<AnnounceCancel, ErrorCode> parseAnnounceCancel(
    folly::io::Cursor& cursor,
    size_t length) noexcept;

struct TrackStatusRequest {
  FullTrackName fullTrackName;
};

folly::Expected<TrackStatusRequest, ErrorCode> parseTrackStatusRequest(
    folly::io::Cursor& cursor,
    size_t length) noexcept;

struct TrackStatus {
  FullTrackName fullTrackName;
  TrackStatusCode statusCode;
  folly::Optional<AbsoluteLocation> latestGroupAndObject;
};

folly::Expected<TrackStatus, ErrorCode> parseTrackStatus(
    folly::io::Cursor& cursor,
    size_t length) noexcept;

struct Goaway {
  std::string newSessionUri;
};

folly::Expected<Goaway, ErrorCode> parseGoaway(
    folly::io::Cursor& cursor,
    size_t length) noexcept;

struct MaxSubscribeId {
  SubscribeID subscribeID;
};

folly::Expected<MaxSubscribeId, ErrorCode> parseMaxSubscribeId(
    folly::io::Cursor& cursor,
    size_t length) noexcept;

struct SubscribesBlocked {
  SubscribeID maxSubscribeID;
};

folly::Expected<SubscribesBlocked, ErrorCode> parseSubscribesBlocked(
    folly::io::Cursor& cursor,
    size_t length) noexcept;

struct Fetch {
  Fetch() = default;
  Fetch(
      SubscribeID su,
      FullTrackName n,
      uint8_t p,
      GroupOrder g,
      AbsoluteLocation st,
      AbsoluteLocation e,
      std::vector<TrackRequestParameter> pa = {})
      : subscribeID(su),
        fullTrackName(std::move(n)),
        priority(p),
        groupOrder(g),
        start(st),
        end(e),
        params(std::move(pa)) {}
  SubscribeID subscribeID;
  FullTrackName fullTrackName;
  uint8_t priority;
  GroupOrder groupOrder;
  AbsoluteLocation start;
  AbsoluteLocation end;
  std::vector<TrackRequestParameter> params;
};

folly::Expected<Fetch, ErrorCode> parseFetch(
    folly::io::Cursor& cursor,
    size_t length) noexcept;

struct FetchCancel {
  SubscribeID subscribeID;
};

folly::Expected<FetchCancel, ErrorCode> parseFetchCancel(
    folly::io::Cursor& cursor,
    size_t length) noexcept;

struct FetchOk {
  SubscribeID subscribeID;
  GroupOrder groupOrder;
  uint8_t endOfTrack;
  AbsoluteLocation latestGroupAndObject;
  std::vector<TrackRequestParameter> params;
};

folly::Expected<FetchOk, ErrorCode> parseFetchOk(
    folly::io::Cursor& cursor,
    size_t length) noexcept;

struct FetchError {
  SubscribeID subscribeID;
  uint64_t errorCode;
  std::string reasonPhrase;
};

folly::Expected<FetchError, ErrorCode> parseFetchError(
    folly::io::Cursor& cursor,
    size_t length) noexcept;

struct SubscribeAnnounces {
  TrackNamespace trackNamespacePrefix;
  std::vector<TrackRequestParameter> params;
};

folly::Expected<SubscribeAnnounces, ErrorCode> parseSubscribeAnnounces(
    folly::io::Cursor& cursor,
    size_t length) noexcept;

struct SubscribeAnnouncesOk {
  TrackNamespace trackNamespacePrefix;
};

folly::Expected<SubscribeAnnouncesOk, ErrorCode> parseSubscribeAnnouncesOk(
    folly::io::Cursor& cursor,
    size_t length) noexcept;

struct SubscribeAnnouncesError {
  TrackNamespace trackNamespacePrefix;
  uint64_t errorCode;
  std::string reasonPhrase;
};

folly::Expected<SubscribeAnnouncesError, ErrorCode>
parseSubscribeAnnouncesError(folly::io::Cursor& cursor, size_t length) noexcept;

struct UnsubscribeAnnounces {
  TrackNamespace trackNamespacePrefix;
};

folly::Expected<UnsubscribeAnnounces, ErrorCode> parseUnsubscribeAnnounces(
    folly::io::Cursor& cursor,
    size_t length) noexcept;

//// Egress ////

WriteResult writeClientSetup(
    folly::IOBufQueue& writeBuf,
    const ClientSetup& clientSetup) noexcept;

WriteResult writeServerSetup(
    folly::IOBufQueue& writeBuf,
    const ServerSetup& serverSetup) noexcept;

WriteResult writeSubgroupHeader(
    folly::IOBufQueue& writeBuf,
    const ObjectHeader& objectHeader) noexcept;

WriteResult writeFetchHeader(
    folly::IOBufQueue& writeBuf,
    SubscribeID subscribeID) noexcept;

WriteResult writeStreamHeader(
    folly::IOBufQueue& writeBuf,
    StreamType streamType,
    const ObjectHeader& objectHeader) noexcept;

WriteResult writeObject(
    folly::IOBufQueue& writeBuf,
    StreamType streamType,
    const ObjectHeader& objectHeader,
    std::unique_ptr<folly::IOBuf> objectPayload) noexcept;

WriteResult writeSingleObjectStream(
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

WriteResult writeMaxSubscribeId(
    folly::IOBufQueue& writeBuf,
    const MaxSubscribeId& maxSubscribeId) noexcept;

WriteResult writeSubscribesBlocked(
    folly::IOBufQueue& writeBuf,
    const SubscribesBlocked& subscribesBlocked) noexcept;

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

WriteResult writeSubscribeAnnounces(
    folly::IOBufQueue& writeBuf,
    const SubscribeAnnounces& subscribeAnnounces) noexcept;

WriteResult writeSubscribeAnnouncesOk(
    folly::IOBufQueue& writeBuf,
    const SubscribeAnnouncesOk& subscribeAnnouncesOk) noexcept;

WriteResult writeSubscribeAnnouncesError(
    folly::IOBufQueue& writeBuf,
    const SubscribeAnnouncesError& subscribeAnnouncesError) noexcept;

WriteResult writeUnsubscribeAnnounces(
    folly::IOBufQueue& writeBuf,
    const UnsubscribeAnnounces& unsubscribeAnnounces) noexcept;

WriteResult writeFetch(
    folly::IOBufQueue& writeBuf,
    const Fetch& fetch) noexcept;

WriteResult writeFetchCancel(
    folly::IOBufQueue& writeBuf,
    const FetchCancel& fetchCancel) noexcept;

WriteResult writeFetchOk(
    folly::IOBufQueue& writeBuf,
    const FetchOk& fetchOk) noexcept;

WriteResult writeFetchError(
    folly::IOBufQueue& writeBuf,
    const FetchError& fetchError) noexcept;

} // namespace moxygen

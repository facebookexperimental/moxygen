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
const uint8_t kDefaultPriority = 128;

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
  GOAWAY_TIMEOUT = 0x10,
  CONTROL_MESSAGE_TIMEOUT = 0x11,
  DATA_STREAM_TIMEOUT = 0x12,
};

enum class SubscribeErrorCode : uint32_t {
  INTERNAL_ERROR = 0,
  UNAUTHORIZED = 1,
  TIMEOUT = 2,
  NOT_SUPPORTED = 3,
  TRACK_NOT_EXIST = 4,
  INVALID_RANGE = 5,
  RETRY_TRACK_ALIAS = 6,
};

enum class SubscribeDoneStatusCode : uint32_t {
  INTERNAL_ERROR = 0x0,
  UNAUTHORIZED = 0x1,
  TRACK_ENDED = 0x2,
  SUBSCRIPTION_ENDED = 0x3,
  GOING_AWAY = 0x4,
  EXPIRED = 0x5,
  TOO_FAR_BEHIND = 0x6,
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
  UNAUTHORIZED = 1,
  TIMEOUT = 2,
  NOT_SUPPORTED = 3,
  TRACK_NOT_EXIST = 4,
  INVALID_RANGE = 5,
  //
  CANCELLED = std::numeric_limits<uint32_t>::max()
};

enum class SubscribeAnnouncesErrorCode : uint32_t {
  INTERNAL_ERROR = 0,
  UNAUTHORIZED = 1,
  TIMEOUT = 2,
  NOT_SUPPORTED = 3,
  NAMESPACE_PREFIX_UNKNOWN = 4,
};

enum class AnnounceErrorCode : uint32_t {
  INTERNAL_ERROR = 0,
  UNAUTHORIZED = 1,
  TIMEOUT = 2,
  NOT_SUPPORTED = 3,
  UNINTERESTED = 4,
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
  OBJECT_DATAGRAM_STATUS = 02,
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
constexpr uint64_t kVersionDraft08_exp3 = 0xff080003; // Draft 8 datagram status
constexpr uint64_t kVersionDraft08_exp4 = 0xff080004; // Draft 8 END_OF_TRACK
constexpr uint64_t kVersionDraft08_exp5 = 0xff080005; // Draft 8 Joining FETCH
constexpr uint64_t kVersionDraft08_exp6 = 0xff080006; // Draft 8 End Group
constexpr uint64_t kVersionDraft08_exp7 = 0xff080007; // Draft 8 Error Codes
constexpr uint64_t kVersionDraft08_exp8 = 0xff080008; // Draft 8 Sub Done codes
constexpr uint64_t kVersionDraft08_exp9 = 0xff080009; // Draft 8 Extensions

// kVersionDraftCurrent is only used in test cases. Elsewhere, we use the more
// specific constants.
constexpr uint64_t kVersionDraftCurrent = kVersionDraft08;
constexpr uint64_t kVersionDraft09 = 0xff000009;
constexpr uint64_t kVersionDraft10 = 0xff00000A;
constexpr uint64_t kVersionDraft11 = 0xff00000B;

// In the terminology I'm using for this function, each draft has a "major"
// and a "minor" version. For example, kVersionDraft08_exp2 has the major
// version 8 and minor version 2.
uint64_t getDraftMajorVersion(uint64_t version);

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
  END_OF_TRACK = 5,
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

struct Extension {
  // Even type => holds value in intValue
  // Odd type => holds value in arrayValue
  uint64_t type;
  uint64_t intValue;
  std::unique_ptr<folly::IOBuf> arrayValue;

  Extension() noexcept : Extension(0, 0) {}
  Extension(uint64_t type, uint64_t intValue) noexcept
      : type(type), intValue(intValue), arrayValue(nullptr) {}
  Extension(uint64_t type, std::unique_ptr<folly::IOBuf> arrayValue) noexcept
      : type(type), intValue(0), arrayValue(std::move(arrayValue)) {}

  Extension(const Extension& other) noexcept {
    copyFrom(other);
  }

  Extension& operator=(const Extension& other) noexcept {
    copyFrom(other);
    return *this;
  }

  Extension(Extension&& other) noexcept {
    moveFrom(std::move(other));
  }

  Extension& operator=(Extension&& other) noexcept {
    moveFrom(std::move(other));
    return *this;
  }

  bool operator==(const Extension& other) const noexcept {
    if (type == other.type) {
      if (isOddType()) {
        folly::IOBufEqualTo eq;
        return eq(arrayValue, other.arrayValue);
      } else {
        return intValue == other.intValue;
      }
    }
    return false;
  }

  bool isOddType() const {
    return type & 0x1;
  }

 private:
  void copyFrom(const Extension& other) {
    type = other.type;
    intValue = other.intValue;
    arrayValue = other.arrayValue ? other.arrayValue->clone() : nullptr;
  };
  void moveFrom(Extension&& other) {
    type = other.type;
    intValue = other.intValue;
    arrayValue = std::move(other.arrayValue);
  }
};

using Extensions = std::vector<Extension>;
inline Extensions noExtensions() {
  return Extensions();
}
struct ObjectHeader {
  ObjectHeader() = default;
  ObjectHeader(
      TrackIdentifier trackIdentifierIn,
      uint64_t groupIn,
      uint64_t subgroupIn,
      uint64_t idIn,
      uint8_t priorityIn = 128,
      ObjectStatus statusIn = ObjectStatus::NORMAL,
      std::vector<Extension> extensionsIn = noExtensions(),
      folly::Optional<uint64_t> lengthIn = folly::none)
      : trackIdentifier(trackIdentifierIn),
        group(groupIn),
        subgroup(subgroupIn),
        id(idIn),
        priority(priorityIn),
        status(statusIn),
        extensions(std::move(extensionsIn)),
        length(std::move(lengthIn)) {}
  ObjectHeader(
      TrackIdentifier trackIdentifierIn,
      uint64_t groupIn,
      uint64_t subgroupIn,
      uint64_t idIn,
      uint8_t priorityIn,
      uint64_t lengthIn,
      std::vector<Extension> extensionsIn = noExtensions())
      : trackIdentifier(trackIdentifierIn),
        group(groupIn),
        subgroup(subgroupIn),
        id(idIn),
        priority(priorityIn),
        status(ObjectStatus::NORMAL),
        extensions(std::move(extensionsIn)),
        length(lengthIn) {}
  TrackIdentifier trackIdentifier;
  uint64_t group;
  uint64_t subgroup{0}; // meaningless for Datagram
  uint64_t id;
  uint8_t priority{kDefaultPriority};
  ObjectStatus status{ObjectStatus::NORMAL};
  std::vector<Extension> extensions;
  folly::Optional<uint64_t> length{folly::none};
};

std::ostream& operator<<(std::ostream& os, const ObjectHeader& type);

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
    quic::kEightByteLimit,
    quic::kEightByteLimit};

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
    os << trackNs.describe();
    return os;
  }

  std::string describe() const {
    std::string result;
    if (trackNamespace.empty()) {
      return result;
    }

    // Iterate through all elements except the last one
    for (size_t i = 0; i < trackNamespace.size() - 1; ++i) {
      result += trackNamespace[i];
      result += '/';
    }

    // Add the last element without a trailing slash
    result += trackNamespace.back();
    return result;
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
    os << ftn.describe();
    return os;
  }
  std::string describe() const {
    if (trackNamespace.empty()) {
      return trackName;
    }
    return folly::to<std::string>(trackNamespace.describe(), '/', trackName);
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
  uint8_t priority{kDefaultPriority};
  GroupOrder groupOrder;
  LocationType locType;
  folly::Optional<AbsoluteLocation> start;
  uint64_t endGroup;
  std::vector<TrackRequestParameter> params;
};

struct SubscribeUpdate {
  SubscribeID subscribeID;
  AbsoluteLocation start;
  uint64_t endGroup;
  uint8_t priority{kDefaultPriority};
  std::vector<TrackRequestParameter> params;
};

struct SubscribeOk {
  SubscribeID subscribeID;
  std::chrono::milliseconds expires;
  GroupOrder groupOrder;
  // context exists is inferred from presence of latest
  folly::Optional<AbsoluteLocation> latest;
  std::vector<TrackRequestParameter> params;
};

struct SubscribeError {
  SubscribeID subscribeID;
  SubscribeErrorCode errorCode;
  std::string reasonPhrase;
  folly::Optional<uint64_t> retryAlias{folly::none};
};

struct Unsubscribe {
  SubscribeID subscribeID;
};

struct SubscribeDone {
  SubscribeID subscribeID;
  SubscribeDoneStatusCode statusCode;
  uint64_t streamCount;
  std::string reasonPhrase;
};

struct Announce {
  TrackNamespace trackNamespace;
  std::vector<TrackRequestParameter> params;
};

struct AnnounceOk {
  TrackNamespace trackNamespace;
};

struct AnnounceError {
  TrackNamespace trackNamespace;
  AnnounceErrorCode errorCode;
  std::string reasonPhrase;
};

struct Unannounce {
  TrackNamespace trackNamespace;
};

struct AnnounceCancel {
  TrackNamespace trackNamespace;
  AnnounceErrorCode errorCode;
  std::string reasonPhrase;
};

struct TrackStatusRequest {
  FullTrackName fullTrackName;
  std::vector<TrackRequestParameter> params; // draft-11 and later
};

struct TrackStatus {
  FullTrackName fullTrackName;
  TrackStatusCode statusCode;
  folly::Optional<AbsoluteLocation> latestGroupAndObject;
  std::vector<TrackRequestParameter> params; // draft-11 and later
};

struct Goaway {
  std::string newSessionUri;
};

struct MaxSubscribeId {
  SubscribeID subscribeID;
};

struct SubscribesBlocked {
  SubscribeID maxSubscribeID;
};

enum class FetchType : uint8_t {
  STANDALONE = 0x1,
  RELATIVE_JOINING = 0x2,
  ABSOLUTE_JOINING = 0x3,
};

struct StandaloneFetch {
  StandaloneFetch() = default;
  StandaloneFetch(AbsoluteLocation s, AbsoluteLocation e) : start(s), end(e) {}
  AbsoluteLocation start;
  AbsoluteLocation end;
};

struct JoiningFetch {
  JoiningFetch(SubscribeID jsid, uint64_t joiningStartIn, FetchType fetchTypeIn)
      : joiningSubscribeID(jsid),
        joiningStart(joiningStartIn),
        fetchType(fetchTypeIn) {
    CHECK(
        fetchType == FetchType::RELATIVE_JOINING ||
        fetchType == FetchType::ABSOLUTE_JOINING);
  }
  SubscribeID joiningSubscribeID;
  // For absolute joining, this is the starting group id. For relative joining,
  // this is the group offset prior and relative to the current group of the
  // corresponding subscribe.
  uint64_t joiningStart;
  FetchType fetchType; // Can be either RELATIVE_JOINING or ABSOLUTE_JOINING
};

struct Fetch {
  Fetch() : args(StandaloneFetch()) {}

  // Used for standalone fetches
  Fetch(
      SubscribeID su,
      FullTrackName ftn,
      AbsoluteLocation st,
      AbsoluteLocation e,
      uint8_t p = kDefaultPriority,
      GroupOrder g = GroupOrder::Default,
      std::vector<TrackRequestParameter> pa = {})
      : subscribeID(su),
        fullTrackName(std::move(ftn)),
        priority(p),
        groupOrder(g),
        params(std::move(pa)),
        args(StandaloneFetch(st, e)) {}

  // Used for absolute or relative joining fetches
  Fetch(
      SubscribeID su,
      SubscribeID jsid,
      uint64_t joiningStart,
      FetchType fetchType,
      uint8_t p = kDefaultPriority,
      GroupOrder g = GroupOrder::Default,
      std::vector<TrackRequestParameter> pa = {})
      : subscribeID(su),
        priority(p),
        groupOrder(g),
        params(std::move(pa)),
        args(JoiningFetch(jsid, joiningStart, fetchType)) {
    CHECK(
        fetchType == FetchType::RELATIVE_JOINING ||
        fetchType == FetchType::ABSOLUTE_JOINING);
  }
  SubscribeID subscribeID;
  FullTrackName fullTrackName;
  uint8_t priority{kDefaultPriority};
  GroupOrder groupOrder;
  std::vector<TrackRequestParameter> params;
  std::variant<StandaloneFetch, JoiningFetch> args;
};

inline std::pair<StandaloneFetch*, JoiningFetch*> fetchType(Fetch& fetch) {
  auto standalone = std::get_if<StandaloneFetch>(&fetch.args);
  auto joining = std::get_if<JoiningFetch>(&fetch.args);
  return {standalone, joining};
}

inline std::pair<const StandaloneFetch*, const JoiningFetch*> fetchType(
    const Fetch& fetch) {
  auto standalone = std::get_if<StandaloneFetch>(&fetch.args);
  auto joining = std::get_if<JoiningFetch>(&fetch.args);
  return {standalone, joining};
}

struct FetchCancel {
  SubscribeID subscribeID;
};

struct FetchOk {
  SubscribeID subscribeID;
  GroupOrder groupOrder;
  uint8_t endOfTrack;
  AbsoluteLocation latestGroupAndObject;
  std::vector<TrackRequestParameter> params;
};

struct FetchError {
  SubscribeID subscribeID;
  FetchErrorCode errorCode;
  std::string reasonPhrase;
};

struct SubscribeAnnounces {
  TrackNamespace trackNamespacePrefix;
  std::vector<TrackRequestParameter> params;
};

struct SubscribeAnnouncesOk {
  TrackNamespace trackNamespacePrefix;
};

struct SubscribeAnnouncesError {
  TrackNamespace trackNamespacePrefix;
  SubscribeAnnouncesErrorCode errorCode;
  std::string reasonPhrase;
};

struct UnsubscribeAnnounces {
  TrackNamespace trackNamespacePrefix;
};

// parseClientSetup and parseServerSetup are version-agnostic, so we're
// leaving them out of the MoQFrameParser.
class MoQFrameParser {
 public:
  // datagram only
  folly::Expected<ObjectHeader, ErrorCode> parseDatagramObjectHeader(
      folly::io::Cursor& cursor,
      StreamType streamType,
      size_t& length) const noexcept;

  folly::Expected<SubscribeID, ErrorCode> parseFetchHeader(
      folly::io::Cursor& cursor) const noexcept;

  folly::Expected<ObjectHeader, ErrorCode> parseSubgroupHeader(
      folly::io::Cursor& cursor) const noexcept;

  folly::Expected<ObjectHeader, ErrorCode> parseFetchObjectHeader(
      folly::io::Cursor& cursor,
      const ObjectHeader& headerTemplate) const noexcept;

  folly::Expected<ObjectHeader, ErrorCode> parseSubgroupObjectHeader(
      folly::io::Cursor& cursor,
      const ObjectHeader& headerTemplate) const noexcept;

  folly::Expected<SubscribeRequest, ErrorCode> parseSubscribeRequest(
      folly::io::Cursor& cursor,
      size_t length) const noexcept;

  folly::Expected<SubscribeUpdate, ErrorCode> parseSubscribeUpdate(
      folly::io::Cursor& cursor,
      size_t length) const noexcept;

  folly::Expected<SubscribeOk, ErrorCode> parseSubscribeOk(
      folly::io::Cursor& cursor,
      size_t length) const noexcept;

  folly::Expected<SubscribeError, ErrorCode> parseSubscribeError(
      folly::io::Cursor& cursor,
      size_t length) const noexcept;

  folly::Expected<Unsubscribe, ErrorCode> parseUnsubscribe(
      folly::io::Cursor& cursor,
      size_t length) const noexcept;

  folly::Expected<SubscribeDone, ErrorCode> parseSubscribeDone(
      folly::io::Cursor& cursor,
      size_t length) const noexcept;

  folly::Expected<Announce, ErrorCode> parseAnnounce(
      folly::io::Cursor& cursor,
      size_t length) const noexcept;

  folly::Expected<AnnounceOk, ErrorCode> parseAnnounceOk(
      folly::io::Cursor& cursor,
      size_t length) const noexcept;

  folly::Expected<AnnounceError, ErrorCode> parseAnnounceError(
      folly::io::Cursor& cursor,
      size_t length) const noexcept;

  folly::Expected<Unannounce, ErrorCode> parseUnannounce(
      folly::io::Cursor& cursor,
      size_t length) const noexcept;

  folly::Expected<AnnounceCancel, ErrorCode> parseAnnounceCancel(
      folly::io::Cursor& cursor,
      size_t length) const noexcept;

  folly::Expected<TrackStatusRequest, ErrorCode> parseTrackStatusRequest(
      folly::io::Cursor& cursor,
      size_t length) const noexcept;

  folly::Expected<TrackStatus, ErrorCode> parseTrackStatus(
      folly::io::Cursor& cursor,
      size_t length) const noexcept;

  folly::Expected<Goaway, ErrorCode> parseGoaway(
      folly::io::Cursor& cursor,
      size_t length) const noexcept;

  folly::Expected<MaxSubscribeId, ErrorCode> parseMaxSubscribeId(
      folly::io::Cursor& cursor,
      size_t length) const noexcept;

  folly::Expected<SubscribesBlocked, ErrorCode> parseSubscribesBlocked(
      folly::io::Cursor& cursor,
      size_t length) const noexcept;

  folly::Expected<Fetch, ErrorCode> parseFetch(
      folly::io::Cursor& cursor,
      size_t length) const noexcept;

  folly::Expected<FetchCancel, ErrorCode> parseFetchCancel(
      folly::io::Cursor& cursor,
      size_t length) const noexcept;

  folly::Expected<FetchOk, ErrorCode> parseFetchOk(
      folly::io::Cursor& cursor,
      size_t length) const noexcept;

  folly::Expected<FetchError, ErrorCode> parseFetchError(
      folly::io::Cursor& cursor,
      size_t length) const noexcept;

  folly::Expected<SubscribeAnnounces, ErrorCode> parseSubscribeAnnounces(
      folly::io::Cursor& cursor,
      size_t length) const noexcept;

  folly::Expected<SubscribeAnnouncesOk, ErrorCode> parseSubscribeAnnouncesOk(
      folly::io::Cursor& cursor,
      size_t length) const noexcept;

  folly::Expected<SubscribeAnnouncesError, ErrorCode>
  parseSubscribeAnnouncesError(folly::io::Cursor& cursor, size_t length)
      const noexcept;

  folly::Expected<UnsubscribeAnnounces, ErrorCode> parseUnsubscribeAnnounces(
      folly::io::Cursor& cursor,
      size_t length) const noexcept;

  void initializeVersion(uint64_t versionIn) {
    CHECK(!version_) << "Version already initialized";
    version_ = versionIn;
  }

 private:
  folly::Expected<folly::Unit, ErrorCode> parseObjectStatusAndLength(
      folly::io::Cursor& cursor,
      size_t length,
      ObjectHeader& objectHeader) const noexcept;

  folly::Expected<folly::Unit, ErrorCode> parseTrackRequestParams(
      folly::io::Cursor& cursor,
      size_t length,
      size_t numParams,
      std::vector<TrackRequestParameter>& params) const noexcept;

  folly::Expected<std::vector<std::string>, ErrorCode> parseFixedTuple(
      folly::io::Cursor& cursor,
      size_t& length) const noexcept;

  folly::Expected<FullTrackName, ErrorCode> parseFullTrackName(
      folly::io::Cursor& cursor,
      size_t& length) const noexcept;

  folly::Expected<AbsoluteLocation, ErrorCode> parseAbsoluteLocation(
      folly::io::Cursor& cursor,
      size_t& length) const noexcept;

  folly::Expected<folly::Unit, ErrorCode> parseExtensions(
      folly::io::Cursor& cursor,
      size_t& length,
      ObjectHeader& objectHeader) const noexcept;

  folly::Expected<Extension, ErrorCode> parseExtension(
      folly::io::Cursor& cursor,
      size_t& length) const noexcept;

  folly::Optional<uint64_t> version_;
};

//// Egress ////

WriteResult writeClientSetup(
    folly::IOBufQueue& writeBuf,
    const ClientSetup& clientSetup) noexcept;

WriteResult writeServerSetup(
    folly::IOBufQueue& writeBuf,
    const ServerSetup& serverSetup) noexcept;

// writeClientSetup and writeServerSetup are the only two functions that
// are version-agnostic, so we are leaving them out of the MoQFrameWriter.
class MoQFrameWriter {
 public:
  WriteResult writeSubgroupHeader(
      folly::IOBufQueue& writeBuf,
      const ObjectHeader& objectHeader) const noexcept;

  WriteResult writeFetchHeader(
      folly::IOBufQueue& writeBuf,
      SubscribeID subscribeID) const noexcept;

  WriteResult writeStreamHeader(
      folly::IOBufQueue& writeBuf,
      StreamType streamType,
      const ObjectHeader& objectHeader) const noexcept;

  WriteResult writeDatagramObject(
      folly::IOBufQueue& writeBuf,
      const ObjectHeader& objectHeader,
      std::unique_ptr<folly::IOBuf> objectPayload) const noexcept;

  WriteResult writeStreamObject(
      folly::IOBufQueue& writeBuf,
      StreamType streamType,
      const ObjectHeader& objectHeader,
      std::unique_ptr<folly::IOBuf> objectPayload) const noexcept;

  WriteResult writeSingleObjectStream(
      folly::IOBufQueue& writeBuf,
      const ObjectHeader& objectHeader,
      std::unique_ptr<folly::IOBuf> objectPayload) const noexcept;

  WriteResult writeSubscribeRequest(
      folly::IOBufQueue& writeBuf,
      const SubscribeRequest& subscribeRequest) const noexcept;

  WriteResult writeSubscribeUpdate(
      folly::IOBufQueue& writeBuf,
      const SubscribeUpdate& update) const noexcept;

  WriteResult writeSubscribeOk(
      folly::IOBufQueue& writeBuf,
      const SubscribeOk& subscribeOk) const noexcept;

  WriteResult writeSubscribeError(
      folly::IOBufQueue& writeBuf,
      const SubscribeError& subscribeError) const noexcept;

  WriteResult writeSubscribeDone(
      folly::IOBufQueue& writeBuf,
      const SubscribeDone& subscribeDone) const noexcept;

  WriteResult writeUnsubscribe(
      folly::IOBufQueue& writeBuf,
      const Unsubscribe& unsubscribe) const noexcept;

  WriteResult writeMaxSubscribeId(
      folly::IOBufQueue& writeBuf,
      const MaxSubscribeId& maxSubscribeId) const noexcept;

  WriteResult writeSubscribesBlocked(
      folly::IOBufQueue& writeBuf,
      const SubscribesBlocked& subscribesBlocked) const noexcept;

  WriteResult writeAnnounce(
      folly::IOBufQueue& writeBuf,
      const Announce& announce) const noexcept;

  WriteResult writeAnnounceOk(
      folly::IOBufQueue& writeBuf,
      const AnnounceOk& announceOk) const noexcept;

  WriteResult writeAnnounceError(
      folly::IOBufQueue& writeBuf,
      const AnnounceError& announceError) const noexcept;

  WriteResult writeUnannounce(
      folly::IOBufQueue& writeBuf,
      const Unannounce& unannounce) const noexcept;

  WriteResult writeAnnounceCancel(
      folly::IOBufQueue& writeBuf,
      const AnnounceCancel& announceCancel) const noexcept;

  WriteResult writeTrackStatusRequest(
      folly::IOBufQueue& writeBuf,
      const TrackStatusRequest& trackStatusRequest) const noexcept;

  WriteResult writeTrackStatus(
      folly::IOBufQueue& writeBuf,
      const TrackStatus& trackStatus) const noexcept;

  WriteResult writeGoaway(folly::IOBufQueue& writeBuf, const Goaway& goaway)
      const noexcept;

  WriteResult writeSubscribeAnnounces(
      folly::IOBufQueue& writeBuf,
      const SubscribeAnnounces& subscribeAnnounces) const noexcept;

  WriteResult writeSubscribeAnnouncesOk(
      folly::IOBufQueue& writeBuf,
      const SubscribeAnnouncesOk& subscribeAnnouncesOk) const noexcept;

  WriteResult writeSubscribeAnnouncesError(
      folly::IOBufQueue& writeBuf,
      const SubscribeAnnouncesError& subscribeAnnouncesError) const noexcept;

  WriteResult writeUnsubscribeAnnounces(
      folly::IOBufQueue& writeBuf,
      const UnsubscribeAnnounces& unsubscribeAnnounces) const noexcept;

  WriteResult writeFetch(folly::IOBufQueue& writeBuf, const Fetch& fetch)
      const noexcept;

  WriteResult writeFetchCancel(
      folly::IOBufQueue& writeBuf,
      const FetchCancel& fetchCancel) const noexcept;

  WriteResult writeFetchOk(folly::IOBufQueue& writeBuf, const FetchOk& fetchOk)
      const noexcept;

  WriteResult writeFetchError(
      folly::IOBufQueue& writeBuf,
      const FetchError& fetchError) const noexcept;

  void initializeVersion(uint64_t versionIn) {
    CHECK(!version_) << "Version already initialized";
    version_ = versionIn;
  }

 private:
  void writeExtensions(
      folly::IOBufQueue& writeBuf,
      const std::vector<Extension>& extensions,
      size_t& size,
      bool& error) const noexcept;

  size_t getExtensionSize(const std::vector<Extension>& extensions, bool& error)
      const noexcept;

  folly::Optional<uint64_t> version_;
};

} // namespace moxygen

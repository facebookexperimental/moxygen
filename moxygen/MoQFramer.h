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
#include <moxygen/MoQTokenCache.h>

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

// Undefine Windows macros that conflict with our enum values
#ifdef NO_ERROR
#undef NO_ERROR
#endif
#ifdef DELETE
#undef DELETE
#endif

enum class SessionCloseErrorCode : uint32_t {
  NO_ERROR = 0,
  INTERNAL_ERROR = 1,
  UNAUTHORIZED = 2,
  PROTOCOL_VIOLATION = 3,
  INVALID_REQUEST_ID = 4,
  DUPLICATE_TRACK_ALIAS = 5,
  KEY_VALUE_FORMATTING_ERROR = 6,
  TOO_MANY_REQUESTS = 7,
  INVALID_PATH = 8,
  MALFORMED_PATH = 9,
  GOAWAY_TIMEOUT = 0x10,
  CONTROL_MESSAGE_TIMEOUT = 0x11,
  DATA_STREAM_TIMEOUT = 0x12,
  AUTH_TOKEN_CACHE_OVERFLOW = 0x13,
  DUPLICATE_AUTH_TOKEN_ALIAS = 0x14,
  VERSION_NEGOTIATION_FAILED = 0x15,

  UNKNOWN_AUTH_TOKEN_ALIAS = std::numeric_limits<uint32_t>::max() - 1,
  PARSE_UNDERFLOW = std::numeric_limits<uint32_t>::max(),
};

using ErrorCode = SessionCloseErrorCode;

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
  NO_OBJECTS = 6,
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
  MAX_REQUEST_ID = 0x15,
  FETCH = 0x16,
  FETCH_CANCEL = 0x17,
  FETCH_OK = 0x18,
  FETCH_ERROR = 0x19,
  REQUESTS_BLOCKED = 0x1A,
  CLIENT_SETUP = 0x20,
  SERVER_SETUP = 0x21,
  LEGACY_CLIENT_SETUP = 0x40,
  LEGACY_SERVER_SETUP = 0x41,
};

enum class StreamType : uint64_t {
  OBJECT_DATAGRAM_NO_EXT = 0x0,
  OBJECT_DATAGRAM_EXT = 0x1,
  OBJECT_DATAGRAM_STATUS = 0x2,
  OBJECT_DATAGRAM_STATUS_EXT = 0x3,
  SUBGROUP_HEADER = 0x4, // draft-10 and earlier
  FETCH_HEADER = 0x5,
  SUBGROUP_HEADER_MASK = 0x8,
  SUBGROUP_HEADER_SG_ZERO = 0x8,
  SUBGROUP_HEADER_SG_ZERO_EXT = 0x9,
  SUBGROUP_HEADER_SG_FIRST = 0xA,
  SUBGROUP_HEADER_SG_FIRST_EXT = 0xB,
  SUBGROUP_HEADER_SG = 0xC,
  SUBGROUP_HEADER_SG_EXT = 0xD,
};

// Subgroup Bit Fields
constexpr uint8_t SG_HAS_EXTENSIONS = 0x1;
constexpr uint8_t SG_SUBGROUP_VALUE = 0x2;
constexpr uint8_t SG_HAS_SUBGROUP_ID = 0x4;

std::ostream& operator<<(std::ostream& os, FrameType type);

std::ostream& operator<<(std::ostream& os, StreamType type);

enum class SetupKey : uint64_t {
  PATH = 1,
  MAX_REQUEST_ID = 2,
  MAX_AUTH_TOKEN_CACHE_SIZE = 4,
};

enum class AliasType : uint8_t {
  DELETE = 0x0,
  REGISTER = 0x1,
  USE_ALIAS = 0x2,
  USE_VALUE = 0x3
};

struct AuthToken {
  uint64_t tokenType;
  std::string tokenValue;
  folly::Optional<uint64_t> alias;
  // Set alias to one of these constants when sending an AuthToken parameter
  // Register: Will attempt to save the token in the cache, there is space
  // DontRegister: Will not attempt to save the token in the cache
  static constexpr uint64_t Register = 1;
  static constexpr folly::Optional<uint64_t> DontRegister = folly::none;
};

struct Parameter {
  uint64_t key;
  std::string asString;
  uint64_t asUint64;
  AuthToken asAuthToken;
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
constexpr uint64_t kVersionDraft12 = 0xff00000C;

// In the terminology I'm using for this function, each draft has a "major"
// and a "minor" version. For example, kVersionDraft08_exp2 has the major
// version 8 and minor version 2.
uint64_t getDraftMajorVersion(uint64_t version);

void writeVarint(
    folly::IOBufQueue& buf,
    uint64_t value,
    size_t& size,
    bool& error) noexcept;

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
  END_OF_TRACK = 4,
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

struct RequestID {
  /* implicit */ RequestID(uint64_t v) : value(v) {}
  RequestID() = default;
  uint64_t value{0};
  bool operator==(const RequestID& s) const {
    return value == s.value;
  }
  bool operator!=(const RequestID& s) const {
    return value != s.value;
  }
  bool operator<(const RequestID& other) const {
    return value < other.value;
  }
  bool operator<=(const RequestID& other) const {
    return value <= other.value;
  }
  bool operator>(const RequestID& other) const {
    return value > other.value;
  }
  bool operator>=(const RequestID& other) const {
    return value >= other.value;
  }
  struct hash {
    size_t operator()(const RequestID& s) const {
      return std::hash<uint64_t>{}(s.value);
    }
  };
};
std::ostream& operator<<(std::ostream& os, RequestID id);

struct UnitializedIdentifier {
  bool operator==(const UnitializedIdentifier&) const {
    return false;
  }
};
using TrackIdentifier =
    std::variant<UnitializedIdentifier, TrackAlias, RequestID>;
struct TrackIdentifierHash {
  size_t operator()(const TrackIdentifier& trackIdentifier) const {
    XCHECK_GT(trackIdentifier.index(), 0llu);
    auto trackAlias = std::get_if<TrackAlias>(&trackIdentifier);
    if (trackAlias) {
      return folly::hash::hash_combine(
          trackIdentifier.index(), trackAlias->value);
    } else {
      return folly::hash::hash_combine(
          trackIdentifier.index(), std::get<RequestID>(trackIdentifier).value);
    }
  }
};

inline uint64_t value(const TrackIdentifier& trackIdentifier) {
  return std::visit(
      [](const auto& value) {
        using T = std::decay_t<decltype(value)>;
        if constexpr (std::is_same_v<T, TrackAlias>) {
          return value.value;
        } else if constexpr (std::is_same_v<T, RequestID>) {
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

  // == Operator For Datagram Testing
  bool operator==(const ObjectHeader& other) const {
    return trackIdentifier == other.trackIdentifier && group == other.group &&
        subgroup == other.subgroup && id == other.id &&
        priority == other.priority && status == other.status &&
        extensions == other.extensions && length == other.length;
  }
};

std::ostream& operator<<(std::ostream& os, const ObjectHeader& type);

uint64_t getAuthorizationParamKey(uint64_t version);

uint64_t getDeliveryTimeoutParamKey(uint64_t version);

uint64_t getMaxCacheDurationParamKey(uint64_t version);

enum class LocationType : uint8_t {
  NextGroupStart = 1,
  LatestObject = 2,
  AbsoluteStart = 3,
  AbsoluteRange = 4,
  LatestGroup = 250,
};

std::string toString(LocationType locType);

struct AbsoluteLocation {
  uint64_t group{0};
  uint64_t object{0};

  AbsoluteLocation() = default;
  constexpr AbsoluteLocation(uint64_t g, uint64_t o) : group(g), object(o) {}

  bool operator==(const AbsoluteLocation& other) const {
    return group == other.group && object == other.object;
  }

  bool operator!=(const AbsoluteLocation& other) const {
    return !(*this == other);
  }

  bool operator<(const AbsoluteLocation& other) const {
    if (group < other.group) {
      return true;
    } else if (group == other.group) {
      return object < other.object;
    }
    return false;
  }

  bool operator<=(const AbsoluteLocation& other) const {
    return *this < other || *this == other;
  }

  bool operator>(const AbsoluteLocation& other) const {
    return !(*this <= other);
  }

  bool operator>=(const AbsoluteLocation& other) const {
    return !(*this < other);
  }

  friend std::ostream& operator<<(
      std::ostream& os,
      const AbsoluteLocation& loc) {
    os << loc.describe();
    return os;
  }

  std::string describe() const {
    return folly::to<std::string>("{", group, ",", object, "}");
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
  bool operator!=(const FullTrackName& other) const {
    return !(*this == other);
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
  static SubscribeRequest make(
      const FullTrackName& fullTrackName,
      uint8_t priority = kDefaultPriority,
      GroupOrder groupOrder = GroupOrder::Default,
      bool forward = true,
      LocationType locType = LocationType::LatestGroup,
      folly::Optional<AbsoluteLocation> start = folly::none,
      uint64_t endGroup = 0,
      std::vector<TrackRequestParameter> params = {}) {
    return SubscribeRequest{
        RequestID(), // Default constructed RequestID
        folly::none, // Default constructed TrackAlias (folly::none)
        fullTrackName,
        priority,
        groupOrder,
        forward,
        locType,
        std::move(start),
        endGroup,
        std::move(params)};
  }

  RequestID requestID;
  folly::Optional<TrackAlias> trackAlias; // < v12
  FullTrackName fullTrackName;
  uint8_t priority{kDefaultPriority};
  GroupOrder groupOrder;
  bool forward{true}; // Only used in draft-11 and above
  LocationType locType;
  folly::Optional<AbsoluteLocation> start;
  uint64_t endGroup;
  std::vector<TrackRequestParameter> params;
};

struct SubscribeUpdate {
  RequestID requestID;
  AbsoluteLocation start;
  uint64_t endGroup;
  uint8_t priority{kDefaultPriority};
  bool forward{true}; // Only used in draft-11 and above
  std::vector<TrackRequestParameter> params;
};

struct SubscribeOk {
  RequestID requestID;
  TrackAlias trackAlias; // >= v12
  std::chrono::milliseconds expires;
  GroupOrder groupOrder;
  // context exists is inferred from presence of latest
  folly::Optional<AbsoluteLocation> latest;
  std::vector<TrackRequestParameter> params;
};

struct SubscribeError {
  RequestID requestID;
  SubscribeErrorCode errorCode;
  std::string reasonPhrase;
  folly::Optional<uint64_t> retryAlias{folly::none};
};

struct Unsubscribe {
  RequestID requestID;
};

struct SubscribeDone {
  RequestID requestID;
  SubscribeDoneStatusCode statusCode;
  uint64_t streamCount;
  std::string reasonPhrase;
};

struct Announce {
  RequestID requestID;
  TrackNamespace trackNamespace;
  std::vector<TrackRequestParameter> params;
};

struct AnnounceOk {
  RequestID requestID;
  TrackNamespace trackNamespace;
};

struct AnnounceError {
  RequestID requestID;
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
  RequestID requestID;
  FullTrackName fullTrackName;
  std::vector<TrackRequestParameter> params; // draft-11 and later
};

struct TrackStatus {
  RequestID requestID;
  FullTrackName fullTrackName;
  TrackStatusCode statusCode;
  folly::Optional<AbsoluteLocation> latestGroupAndObject;
  std::vector<TrackRequestParameter> params; // draft-11 and later
};

struct Goaway {
  std::string newSessionUri;
};

struct MaxRequestID {
  RequestID requestID;
};

struct RequestsBlocked {
  RequestID maxRequestID;
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
  JoiningFetch(RequestID jsid, uint64_t joiningStartIn, FetchType fetchTypeIn)
      : joiningRequestID(jsid),
        joiningStart(joiningStartIn),
        fetchType(fetchTypeIn) {
    CHECK(
        fetchType == FetchType::RELATIVE_JOINING ||
        fetchType == FetchType::ABSOLUTE_JOINING);
  }
  RequestID joiningRequestID;
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
      RequestID su,
      FullTrackName ftn,
      AbsoluteLocation st,
      AbsoluteLocation e,
      uint8_t p = kDefaultPriority,
      GroupOrder g = GroupOrder::Default,
      std::vector<TrackRequestParameter> pa = {})
      : requestID(su),
        fullTrackName(std::move(ftn)),
        priority(p),
        groupOrder(g),
        params(std::move(pa)),
        args(StandaloneFetch(st, e)) {}

  // Used for absolute or relative joining fetches
  Fetch(
      RequestID su,
      RequestID jsid,
      uint64_t joiningStart,
      FetchType fetchType,
      uint8_t p = kDefaultPriority,
      GroupOrder g = GroupOrder::Default,
      std::vector<TrackRequestParameter> pa = {})
      : requestID(su),
        priority(p),
        groupOrder(g),
        params(std::move(pa)),
        args(JoiningFetch(jsid, joiningStart, fetchType)) {
    CHECK(
        fetchType == FetchType::RELATIVE_JOINING ||
        fetchType == FetchType::ABSOLUTE_JOINING);
  }
  RequestID requestID;
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
  RequestID requestID;
};

struct FetchOk {
  RequestID requestID;
  GroupOrder groupOrder;
  uint8_t endOfTrack;
  AbsoluteLocation endLocation;
  std::vector<TrackRequestParameter> params;
};

struct FetchError {
  RequestID requestID;
  FetchErrorCode errorCode;
  std::string reasonPhrase;
};

struct SubscribeAnnounces {
  RequestID requestID;
  TrackNamespace trackNamespacePrefix;
  std::vector<TrackRequestParameter> params;
};

struct SubscribeAnnouncesOk {
  RequestID requestID;
  TrackNamespace trackNamespacePrefix;
};

struct SubscribeAnnouncesError {
  RequestID requestID;
  TrackNamespace trackNamespacePrefix;
  SubscribeAnnouncesErrorCode errorCode;
  std::string reasonPhrase;
};

struct UnsubscribeAnnounces {
  TrackNamespace trackNamespacePrefix;
};

enum class SubgroupIDFormat : uint8_t { Present, Zero, FirstObject };
inline StreamType getSubgroupStreamType(
    uint64_t version,
    SubgroupIDFormat format,
    bool includeExtensions) {
  if (getDraftMajorVersion(version) < 11) {
    return StreamType::SUBGROUP_HEADER;
  }
  return StreamType(
      folly::to_underlying(StreamType::SUBGROUP_HEADER_MASK) |
      (format == SubgroupIDFormat::Present ? SG_HAS_SUBGROUP_ID : 0) |
      (format == SubgroupIDFormat::FirstObject ? SG_SUBGROUP_VALUE : 0) |
      (includeExtensions ? SG_HAS_EXTENSIONS : 0));
}

inline StreamType
getDatagramType(uint64_t version, bool status, bool includeExtensions) {
  return getDraftMajorVersion(version) < 11
      ? (status ? StreamType::OBJECT_DATAGRAM_STATUS
                : StreamType::OBJECT_DATAGRAM_EXT)
      : (StreamType((status ? 0x2 : 0) | (includeExtensions ? 0x1 : 0)));
}

folly::Expected<std::string, ErrorCode> parseFixedString(
    folly::io::Cursor& cursor,
    size_t& length);

// parseClientSetup and parseServerSetup are version-agnostic, so we're
// leaving them out of the MoQFrameParser.
class MoQFrameParser {
 public:
  // datagram only
  folly::Expected<ObjectHeader, ErrorCode> parseDatagramObjectHeader(
      folly::io::Cursor& cursor,
      StreamType streamType,
      size_t& length) const noexcept;

  folly::Expected<RequestID, ErrorCode> parseFetchHeader(
      folly::io::Cursor& cursor) const noexcept;

  folly::Expected<ObjectHeader, ErrorCode> parseSubgroupHeader(
      folly::io::Cursor& cursor,
      SubgroupIDFormat format,
      bool includeExtensions) const noexcept;

  folly::Expected<ObjectHeader, ErrorCode> parseFetchObjectHeader(
      folly::io::Cursor& cursor,
      const ObjectHeader& headerTemplate) const noexcept;

  folly::Expected<ObjectHeader, ErrorCode> parseSubgroupObjectHeader(
      folly::io::Cursor& cursor,
      const ObjectHeader& headerTemplate,
      SubgroupIDFormat format,
      bool includeExtensions) const noexcept;

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

  folly::Expected<MaxRequestID, ErrorCode> parseMaxRequestID(
      folly::io::Cursor& cursor,
      size_t length) const noexcept;

  folly::Expected<RequestsBlocked, ErrorCode> parseRequestsBlocked(
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

  folly::Optional<uint64_t> getVersion() const {
    return version_;
  }

  void setTokenCacheMaxSize(size_t size) {
    tokenCache_.setMaxSize(size);
  }

 private:
  folly::Expected<folly::Unit, ErrorCode> parseObjectStatusAndLength(
      folly::io::Cursor& cursor,
      size_t length,
      ObjectHeader& objectHeader) const noexcept;

  folly::Expected<folly::Unit, ErrorCode> parseTrackRequestParams(
      folly::io::Cursor& cursor,
      size_t& length,
      size_t numParams,
      std::vector<TrackRequestParameter>& params) const noexcept;

  folly::Expected<folly::Optional<AuthToken>, ErrorCode> parseToken(
      folly::io::Cursor& cursor,
      size_t length) const noexcept;

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
  mutable MoQTokenCache tokenCache_;
};

//// Egress ////
TrackRequestParameter getAuthParam(
    uint64_t version,
    std::string token,
    uint64_t tokenType = 0,
    folly::Optional<uint64_t> registerToken = AuthToken::Register);

WriteResult writeClientSetup(
    folly::IOBufQueue& writeBuf,
    const ClientSetup& clientSetup,
    uint64_t version) noexcept;

WriteResult writeServerSetup(
    folly::IOBufQueue& writeBuf,
    const ServerSetup& serverSetup,
    uint64_t version) noexcept;

// writeClientSetup and writeServerSetup are the only two functions that
// are version-agnostic, so we are leaving them out of the MoQFrameWriter.
class MoQFrameWriter {
 public:
  WriteResult writeSubgroupHeader(
      folly::IOBufQueue& writeBuf,
      const ObjectHeader& objectHeader,
      SubgroupIDFormat format = SubgroupIDFormat::Present,
      bool includeExtensions = true) const noexcept;

  WriteResult writeFetchHeader(folly::IOBufQueue& writeBuf, RequestID requestID)
      const noexcept;

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

  WriteResult writeMaxRequestID(
      folly::IOBufQueue& writeBuf,
      const MaxRequestID& maxRequestID) const noexcept;

  WriteResult writeRequestsBlocked(
      folly::IOBufQueue& writeBuf,
      const RequestsBlocked& subscribesBlocked) const noexcept;

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

  std::string encodeUseAlias(uint64_t alias) const;

  std::string encodeDeleteTokenAlias(uint64_t alias) const;

  std::string encodeRegisterToken(
      uint64_t alias,
      uint64_t tokenType,
      const std::string& tokenValue) const;

  std::string encodeTokenValue(
      uint64_t tokenType,
      const std::string& tokenValue) const;

  void initializeVersion(uint64_t versionIn) {
    CHECK(!version_) << "Version already initialized";
    version_ = versionIn;
  }

  folly::Optional<uint64_t> getVersion() const {
    return version_;
  }

 private:
  void writeExtensions(
      folly::IOBufQueue& writeBuf,
      const std::vector<Extension>& extensions,
      size_t& size,
      bool& error) const noexcept;

  size_t getExtensionSize(const std::vector<Extension>& extensions, bool& error)
      const noexcept;

  void writeTrackRequestParams(
      folly::IOBufQueue& writeBuf,
      const std::vector<TrackRequestParameter>& params,
      size_t& size,
      bool& error) const noexcept;

  folly::Optional<uint64_t> version_;
};

} // namespace moxygen

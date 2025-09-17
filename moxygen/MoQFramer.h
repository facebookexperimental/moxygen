/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/Optional.h>
#include <folly/Range.h>
#include <folly/String.h>
#include <folly/hash/Hash.h>
#include <folly/io/Cursor.h>
#include <folly/io/IOBufQueue.h>
#include <folly/logging/xlog.h>
#include <moxygen/MoQTokenCache.h>

#include <quic/QuicException.h>
#include <quic/codec/QuicInteger.h>
#include <quic/folly_utils/Utils.h>
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
  MALFORMED_AUTH_TOKEN = 0x16,
  UNKNOWN_AUTH_TOKEN_ALIAS = 0x17,
  EXPIRED_AUTH_TOKEN = 0x18,

  PARSE_UNDERFLOW = std::numeric_limits<uint32_t>::max(),
};

using ErrorCode = SessionCloseErrorCode;

// SubscribeErrorCode is now an alias for RequestErrorCode - see below

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

// FetchErrorCode is now an alias for RequestErrorCode - see below

// SubscribeAnnouncesErrorCode is now an alias for RequestErrorCode - see below

// AnnounceErrorCode is now an alias for RequestErrorCode - see below

// PublishError is now an alias for RequestError - see below

// Consolidated error code enum for all request types
enum class RequestErrorCode : uint32_t {
  // Shared error codes (same semantic meaning across request types)
  INTERNAL_ERROR = 0,
  UNAUTHORIZED = 1,
  TIMEOUT = 2,
  NOT_SUPPORTED = 3,
  TRACK_NOT_EXIST = 4,
  INVALID_RANGE = 5,

  // Fetch-specific codes
  NO_OBJECTS = 6,

  // SubscribeAnnounces-specific codes
  NAMESPACE_PREFIX_UNKNOWN = 4, // Same value as TRACK_NOT_EXIST

  // Announce-specific codes
  UNINTERESTED = 4, // Same value as TRACK_NOT_EXIST

  // Special values
  CANCELLED = std::numeric_limits<uint32_t>::max(),
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
  ANNOUNCE = 0x6,
  ANNOUNCE_OK = 0x7,
  ANNOUNCE_ERROR = 0x8,
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
  PUBLISH = 0x1D,
  PUBLISH_OK = 0x1E,
  PUBLISH_ERROR = 0x1F,
  CLIENT_SETUP = 0x20,
  SERVER_SETUP = 0x21,
  LEGACY_CLIENT_SETUP = 0x40,
  LEGACY_SERVER_SETUP = 0x41,
};

enum class DatagramType : uint64_t {
  OBJECT_DATAGRAM_NO_EXT_V11 = 0x0,
  OBJECT_DATAGRAM_EXT_V11 = 0x1,
  OBJECT_DATAGRAM_STATUS_V11 = 0x2,
  OBJECT_DATAGRAM_STATUS_EXT_V11 = 0x3,

  OBJECT_DATAGRAM_NO_EXT = 0x0,
  OBJECT_DATAGRAM_EXT = 0x1,
  OBJECT_DATAGRAM_NO_EXT_EOG = 0x2,
  OBJECT_DATAGRAM_EXT_EOG = 0x3,
  OBJECT_DATAGRAM_STATUS = 0x20,
  OBJECT_DATAGRAM_STATUS_EXT = 0x21,
};

enum class StreamType : uint64_t {
  FETCH_HEADER = 0x5,
  SUBGROUP_HEADER_MASK_V11 = 0x8,
  SUBGROUP_HEADER_SG_ZERO_V11 = 0x8,
  SUBGROUP_HEADER_SG_ZERO_EXT_V11 = 0x9,
  SUBGROUP_HEADER_SG_FIRST_V11 = 0xA,
  SUBGROUP_HEADER_SG_FIRST_EXT_V11 = 0xB,
  SUBGROUP_HEADER_SG_V11 = 0xC,
  SUBGROUP_HEADER_SG_EXT_V11 = 0xD,

  SUBGROUP_HEADER_MASK = 0x10,
  SUBGROUP_HEADER_SG_ZERO = 0x10,
  SUBGROUP_HEADER_SG_ZERO_EXT = 0x11,
  SUBGROUP_HEADER_SG_FIRST = 0x12,
  SUBGROUP_HEADER_SG_FIRST_EXT = 0x13,
  SUBGROUP_HEADER_SG = 0x14,
  SUBGROUP_HEADER_SG_EXT = 0x15,
  SUBGROUP_HEADER_SG_ZERO_EOG = 0x18,
  SUBGROUP_HEADER_SG_ZERO_EXT_EOG = 0x19,
  SUBGROUP_HEADER_SG_FIRST_EOG = 0x1A,
  SUBGROUP_HEADER_SG_FIRST_EXT_EOG = 0x1B,
  SUBGROUP_HEADER_SG_EOG = 0x1C,
  SUBGROUP_HEADER_SG_EXT_EOG = 0x1D,
};

// Subgroup Bit Fields
constexpr uint8_t SG_HAS_EXTENSIONS = 0x1;
constexpr uint8_t SG_SUBGROUP_VALUE = 0x2;
constexpr uint8_t SG_HAS_SUBGROUP_ID = 0x4;
constexpr uint8_t SG_HAS_END_OF_GROUP = 0x8;

enum class SubgroupIDFormat : uint8_t { Present, Zero, FirstObject };

struct SubgroupOptions {
  bool hasExtensions{false};
  SubgroupIDFormat subgroupIDFormat{SubgroupIDFormat::Present};
  bool hasEndOfGroup{false};
};

std::ostream& operator<<(std::ostream& os, FrameType type);

std::ostream& operator<<(std::ostream& os, StreamType type);

enum class SetupKey : uint64_t {
  PATH = 1,
  MAX_REQUEST_ID = 2,
  AUTHORIZATION_TOKEN = 3,
  MAX_AUTH_TOKEN_CACHE_SIZE = 4,
  MOQT_IMPLEMENTATION = 7
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

using SetupParameter = Parameter;
using TrackRequestParameter = Parameter;

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

constexpr uint64_t kVersionDraft09 = 0xff000009;
constexpr uint64_t kVersionDraft10 = 0xff00000A;
constexpr uint64_t kVersionDraft11 = 0xff00000B;
constexpr uint64_t kVersionDraft12 = 0xff00000C;
constexpr uint64_t kVersionDraft13 = 0xff00000D;
constexpr uint64_t kVersionDraft14 = 0xff00000E;

constexpr uint64_t kVersionDraftCurrent = kVersionDraft11;

// In the terminology I'm using for this function, each draft has a "major"
// and a "minor" version. For example, kVersionDraft08_exp2 has the major
// version 8 and minor version 2.
uint64_t getDraftMajorVersion(uint64_t version);
constexpr std::array<uint64_t, 2> kSupportedVersions{
    kVersionDraft11,
    kVersionDraft12};

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
  bool operator!=(const TrackAlias& a) const {
    return !(*this == a);
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

// TrackIdentifier variant removed - ObjectHeader now uses TrackAlias directly

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
Extensions noExtensions();

struct ObjectHeader {
  ObjectHeader() = default;
  ObjectHeader(
      uint64_t groupIn,
      uint64_t subgroupIn,
      uint64_t idIn,
      uint8_t priorityIn = 128,
      ObjectStatus statusIn = ObjectStatus::NORMAL,
      std::vector<Extension> extensionsIn = noExtensions(),
      folly::Optional<uint64_t> lengthIn = folly::none)
      : group(groupIn),
        subgroup(subgroupIn),
        id(idIn),
        priority(priorityIn),
        status(statusIn),
        extensions(std::move(extensionsIn)),
        length(std::move(lengthIn)) {}
  ObjectHeader(
      uint64_t groupIn,
      uint64_t subgroupIn,
      uint64_t idIn,
      uint8_t priorityIn,
      uint64_t lengthIn,
      std::vector<Extension> extensionsIn = noExtensions())
      : group(groupIn),
        subgroup(subgroupIn),
        id(idIn),
        priority(priorityIn),
        status(ObjectStatus::NORMAL),
        extensions(std::move(extensionsIn)),
        length(lengthIn) {}
  uint64_t group;
  uint64_t subgroup{0}; // meaningless for Datagram
  uint64_t id;
  uint8_t priority{kDefaultPriority};
  ObjectStatus status{ObjectStatus::NORMAL};
  std::vector<Extension> extensions;
  folly::Optional<uint64_t> length{folly::none};

  // == Operator For Datagram Testing
  bool operator==(const ObjectHeader& other) const {
    return group == other.group && subgroup == other.subgroup &&
        id == other.id && priority == other.priority &&
        status == other.status && extensions == other.extensions &&
        length == other.length;
  }
};

// Struct to hold both TrackAlias and ObjectHeader for datagram parsing
struct DatagramObjectHeader {
  TrackAlias trackAlias;
  ObjectHeader objectHeader;

  DatagramObjectHeader(TrackAlias alias, ObjectHeader header)
      : trackAlias(alias), objectHeader(std::move(header)) {}
};

std::ostream& operator<<(std::ostream& os, const ObjectHeader& type);

uint64_t getAuthorizationParamKey(uint64_t version);

uint64_t getDeliveryTimeoutParamKey(uint64_t version);

uint64_t getMaxCacheDurationParamKey(uint64_t version);

enum class LocationType : uint8_t {
  NextGroupStart = 1,
  LargestObject = 2,
  AbsoluteStart = 3,
  AbsoluteRange = 4,
  LargestGroup = 250,
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
      LocationType locType = LocationType::LargestGroup,
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
  RequestID subscriptionRequestID;
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
  // context exists is inferred from presence of largest
  folly::Optional<AbsoluteLocation> largest;
  std::vector<TrackRequestParameter> params;
};

// SubscribeError is now an alias for RequestError - see below

struct Unsubscribe {
  RequestID requestID;
};

struct SubscribeDone {
  RequestID requestID;
  SubscribeDoneStatusCode statusCode;
  uint64_t streamCount;
  std::string reasonPhrase;
};

struct PublishRequest {
  RequestID requestID{0};
  FullTrackName fullTrackName;
  TrackAlias trackAlias{0};
  GroupOrder groupOrder{GroupOrder::Default};
  folly::Optional<AbsoluteLocation> largest;
  bool forward{true};
  std::vector<TrackRequestParameter> params;
};

struct PublishOk {
  RequestID requestID;
  bool forward;
  uint8_t subscriberPriority;
  GroupOrder groupOrder;
  LocationType locType;
  folly::Optional<AbsoluteLocation> start;
  folly::Optional<uint64_t> endGroup;
  std::vector<TrackRequestParameter> params;
};

// PublishError is now an alias for RequestError - see below

struct Announce {
  RequestID requestID;
  TrackNamespace trackNamespace;
  std::vector<TrackRequestParameter> params;
};

struct AnnounceOk {
  RequestID requestID;
  TrackNamespace trackNamespace;
};

// AnnounceError is now an alias for RequestError - see below

struct Unannounce {
  TrackNamespace trackNamespace;
};

struct AnnounceCancel {
  TrackNamespace trackNamespace;
  RequestErrorCode errorCode;
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
  folly::Optional<AbsoluteLocation> largestGroupAndObject;
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

std::pair<StandaloneFetch*, JoiningFetch*> fetchType(Fetch& fetch);

std::pair<const StandaloneFetch*, const JoiningFetch*> fetchType(
    const Fetch& fetch);

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

// FetchError is now an alias for RequestError - see below

struct SubscribeAnnounces {
  RequestID requestID;
  TrackNamespace trackNamespacePrefix;
  std::vector<TrackRequestParameter> params;
};

struct SubscribeAnnouncesOk {
  RequestID requestID;
  TrackNamespace trackNamespacePrefix;
};

// SubscribeAnnouncesError is now an alias for RequestError - see below

struct UnsubscribeAnnounces {
  TrackNamespace trackNamespacePrefix;
};

// Consolidated request error structure
struct RequestError {
  RequestID requestID;
  RequestErrorCode errorCode;
  std::string reasonPhrase;
};

// Type aliases for backward compatibility
using SubscribeError = RequestError;
using FetchError = RequestError;
using SubscribeAnnouncesError = RequestError;
using AnnounceError = RequestError;
using PublishError = RequestError;

// Error code aliases
using SubscribeErrorCode = RequestErrorCode;
using FetchErrorCode = RequestErrorCode;
using SubscribeAnnouncesErrorCode = RequestErrorCode;
using AnnounceErrorCode = RequestErrorCode;
using PublishErrorCode = RequestErrorCode;

inline StreamType getSubgroupStreamType(
    uint64_t version,
    SubgroupIDFormat format,
    bool includeExtensions,
    bool endOfGroup) {
  auto majorVersion = getDraftMajorVersion(version);
  if (majorVersion == 11) {
    return StreamType(
        folly::to_underlying(StreamType::SUBGROUP_HEADER_MASK_V11) |
        (format == SubgroupIDFormat::Present ? SG_HAS_SUBGROUP_ID : 0) |
        (format == SubgroupIDFormat::FirstObject ? SG_SUBGROUP_VALUE : 0) |
        (includeExtensions ? SG_HAS_EXTENSIONS : 0));
  } else {
    return StreamType(
        folly::to_underlying(StreamType::SUBGROUP_HEADER_MASK) |
        (format == SubgroupIDFormat::Present ? SG_HAS_SUBGROUP_ID : 0) |
        (format == SubgroupIDFormat::FirstObject ? SG_SUBGROUP_VALUE : 0) |
        (includeExtensions ? SG_HAS_EXTENSIONS : 0) |
        (endOfGroup ? SG_HAS_END_OF_GROUP : 0));
  }
}
inline folly::Optional<SubgroupOptions> getSubgroupOptions(
    uint64_t version,
    StreamType streamType) {
  SubgroupOptions options;
  auto streamTypeInt = folly::to_underlying(streamType);
  auto majorVersion = getDraftMajorVersion(version);
  if (majorVersion == 11) {
    if ((streamTypeInt &
         folly::to_underlying(StreamType::SUBGROUP_HEADER_MASK_V11)) == 0) {
      return folly::none;
    }
    streamTypeInt &=
        ~folly::to_underlying(StreamType::SUBGROUP_HEADER_MASK_V11);
  } else {
    if ((streamTypeInt &
         folly::to_underlying(StreamType::SUBGROUP_HEADER_MASK)) == 0) {
      return folly::none;
    }
    streamTypeInt &= ~folly::to_underlying(StreamType::SUBGROUP_HEADER_MASK);
    options.hasEndOfGroup =
        folly::to_underlying(streamType) & SG_HAS_END_OF_GROUP;
  }

  options.hasExtensions = streamTypeInt & SG_HAS_EXTENSIONS;
  options.subgroupIDFormat = streamTypeInt & SG_HAS_SUBGROUP_ID
      ? SubgroupIDFormat::Present
      : (streamTypeInt & SG_SUBGROUP_VALUE) ? SubgroupIDFormat::FirstObject
                                            : SubgroupIDFormat::Zero;
  options.hasEndOfGroup = false;
  return options;
}

bool isValidDatagramType(uint64_t version, uint64_t datagramType);

inline DatagramType getDatagramType(
    uint64_t version,
    bool status,
    bool includeExtensions,
    bool endOfGroup) {
  auto majorVersion = getDraftMajorVersion(version);
  if (majorVersion == 11) {
    return DatagramType((status ? 0x2 : 0) | (includeExtensions ? 0x1 : 0));
  } else if (status) {
    return DatagramType(
        folly::to_underlying(DatagramType::OBJECT_DATAGRAM_STATUS) |
        (includeExtensions ? 0x1 : 0));
  } else {
    return DatagramType((includeExtensions ? 0x1 : 0) | (endOfGroup ? 0x4 : 0));
  }
}

folly::Expected<std::string, ErrorCode> parseFixedString(
    folly::io::Cursor& cursor,
    size_t& length);

class MoQFrameParser {
 public:
  folly::Expected<ClientSetup, ErrorCode> parseClientSetup(
      folly::io::Cursor& cursor,
      size_t length) noexcept;

  folly::Expected<ServerSetup, ErrorCode> parseServerSetup(
      folly::io::Cursor& cursor,
      size_t length) noexcept;

  // datagram only
  folly::Expected<DatagramObjectHeader, ErrorCode> parseDatagramObjectHeader(
      folly::io::Cursor& cursor,
      DatagramType datagramType,
      size_t& length) const noexcept;

  folly::Expected<RequestID, ErrorCode> parseFetchHeader(
      folly::io::Cursor& cursor) const noexcept;

  struct SubgroupHeaderResult {
    TrackAlias trackAlias;
    ObjectHeader objectHeader;
  };

  folly::Expected<SubgroupHeaderResult, ErrorCode> parseSubgroupHeader(
      folly::io::Cursor& cursor,
      SubgroupIDFormat format,
      bool includeExtensions) const noexcept;

  // Parses the stream header and if it's a subgroup type,
  // parses and returns the Track Alias.  For non-subgroups,
  // returns folly::none.
  folly::Expected<folly::Optional<TrackAlias>, ErrorCode>
  parseSubgroupTypeAndAlias(folly::io::Cursor& cursor, size_t length)
      const noexcept;

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

  folly::Expected<PublishRequest, ErrorCode> parsePublish(
      folly::io::Cursor& cursor,
      size_t length) const noexcept;

  folly::Expected<PublishOk, ErrorCode> parsePublishOk(
      folly::io::Cursor& cursor,
      size_t length) const noexcept;

  folly::Expected<PublishError, ErrorCode> parsePublishError(
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

  // Unified request error parsing function
  folly::Expected<RequestError, ErrorCode> parseRequestError(
      folly::io::Cursor& cursor,
      size_t length,
      FrameType frameType) const noexcept;

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
    tokenCache_.setMaxSize(size, /*evict=*/true);
  }

  // Test only
  void reset() {
    previousObjectID_ = folly::none;
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
  mutable folly::Optional<uint64_t> previousObjectID_;
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
      TrackAlias trackAlias,
      const ObjectHeader& objectHeader,
      SubgroupIDFormat format = SubgroupIDFormat::Present,
      bool includeExtensions = true) const noexcept;

  WriteResult writeFetchHeader(folly::IOBufQueue& writeBuf, RequestID requestID)
      const noexcept;

  WriteResult writeStreamHeader(
      folly::IOBufQueue& writeBuf,
      StreamType streamType,
      TrackAlias trackAlias,
      const ObjectHeader& objectHeader) const noexcept;

  WriteResult writeDatagramObject(
      folly::IOBufQueue& writeBuf,
      TrackAlias trackAlias,
      const ObjectHeader& objectHeader,
      std::unique_ptr<folly::IOBuf> objectPayload) const noexcept;

  WriteResult writeStreamObject(
      folly::IOBufQueue& writeBuf,
      StreamType streamType,
      const ObjectHeader& objectHeader,
      std::unique_ptr<folly::IOBuf> objectPayload) const noexcept;

  WriteResult writeSingleObjectStream(
      folly::IOBufQueue& writeBuf,
      TrackAlias trackAlias,
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

  WriteResult writePublish(
      folly::IOBufQueue& writeBuf,
      const PublishRequest& publish) const noexcept;

  WriteResult writePublishOk(
      folly::IOBufQueue& writeBuf,
      const PublishOk& publishOk) const noexcept;

  WriteResult writePublishError(
      folly::IOBufQueue& writeBuf,
      const PublishError& publishError) const noexcept;

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

  // Unified request error writing function
  WriteResult writeRequestError(
      folly::IOBufQueue& writeBuf,
      const RequestError& requestError,
      FrameType frameType) const noexcept;

  std::string encodeUseAlias(uint64_t alias) const;

  std::string encodeDeleteTokenAlias(uint64_t alias) const;

  std::string encodeRegisterToken(
      uint64_t alias,
      uint64_t tokenType,
      const std::string& tokenValue) const;

  std::string encodeTokenValue(
      uint64_t tokenType,
      const std::string& tokenValue,
      const folly::Optional<uint64_t>& forceVersion = folly::none) const;

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
  mutable folly::Optional<uint64_t> previousObjectID_;
};

} // namespace moxygen

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
#include <algorithm>
#include <vector>

namespace moxygen {

//////// Constants ////////
const size_t kMaxFrameHeaderSize = 32;
const size_t kMaxNamespaceLength = 32;
const uint8_t kDefaultPriority = 128;

// Extension types
const uint64_t kImmutableExtensionType = 0xB;

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
  INVALID_AUTHORITY = 0x19,
  MALFORMED_AUTHORITY = 0x1A,

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
  UPDATE_FAILED = 0x8,
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
  CANCELLED = 3,        // received UNSUBSCRIBE / FETCH_CANCEL / STOP_SENDING
  MALFORMED_TRACK = 12, // track violated protocol ordering constraints
};

using WriteResult = folly::Expected<size_t, quic::TransportErrorCode>;

enum class FrameType : uint64_t {
  SUBSCRIBE_UPDATE = 2,
  SUBSCRIBE = 3,
  SUBSCRIBE_OK = 4,
  SUBSCRIBE_ERROR = 5,
  REQUEST_ERROR = 5,
  ANNOUNCE = 0x6,
  ANNOUNCE_OK = 0x7,
  REQUEST_OK = 0x7,
  ANNOUNCE_ERROR = 0x8,
  UNANNOUNCE = 9,
  UNSUBSCRIBE = 0xA,
  SUBSCRIBE_DONE = 0xB,
  ANNOUNCE_CANCEL = 0xC,
  TRACK_STATUS = 0xD,
  TRACK_STATUS_OK = 0xE,
  TRACK_STATUS_ERROR = 0xF,
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
  OBJECT_DATAGRAM_NO_EXT = 0x0,
  OBJECT_DATAGRAM_EXT = 0x1,
  OBJECT_DATAGRAM_NO_EXT_EOG = 0x2,
  OBJECT_DATAGRAM_EXT_EOG = 0x3,
  OBJECT_DATAGRAM_NO_EXT_ID_ZERO = 0x4,
  OBJECT_DATAGRAM_EXT_ID_ZERO = 0x5,
  OBJECT_DATAGRAM_NO_EXT_EOG_ID_ZERO = 0x6,
  OBJECT_DATAGRAM_EXT_EOG_ID_ZERO = 0x7,
  OBJECT_DATAGRAM_STATUS = 0x20,
  OBJECT_DATAGRAM_STATUS_EXT = 0x21,
  OBJECT_DATAGRAM_STATUS_ID_ZERO = 0x24,
  OBJECT_DATAGRAM_STATUS_EXT_ID_ZERO = 0x25,

  // Version 15+ datagram types without priority
  OBJECT_DATAGRAM_NO_EXT_NO_PRI = 0x8,
  OBJECT_DATAGRAM_EXT_NO_PRI = 0x9,
  OBJECT_DATAGRAM_NO_EXT_EOG_NO_PRI = 0xA,
  OBJECT_DATAGRAM_EXT_EOG_NO_PRI = 0xB,
  OBJECT_DATAGRAM_NO_EXT_ID_ZERO_NO_PRI = 0xC,
  OBJECT_DATAGRAM_EXT_ID_ZERO_NO_PRI = 0xD,
  OBJECT_DATAGRAM_NO_EXT_EOG_ID_ZERO_NO_PRI = 0xE,
  OBJECT_DATAGRAM_EXT_EOG_ID_ZERO_NO_PRI = 0xF,
  OBJECT_DATAGRAM_STATUS_NO_PRI = 0x28,
  OBJECT_DATAGRAM_STATUS_EXT_NO_PRI = 0x29,
  OBJECT_DATAGRAM_STATUS_ID_ZERO_NO_PRI = 0x2C,
  OBJECT_DATAGRAM_STATUS_EXT_ID_ZERO_NO_PRI = 0x2D,
};

enum class StreamType : uint64_t {
  FETCH_HEADER = 0x5,

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

  // Version 15+ subgroup types without priority
  SUBGROUP_HEADER_SG_ZERO_NO_PRI = 0x30,
  SUBGROUP_HEADER_SG_ZERO_EXT_NO_PRI = 0x31,
  SUBGROUP_HEADER_SG_FIRST_NO_PRI = 0x32,
  SUBGROUP_HEADER_SG_FIRST_EXT_NO_PRI = 0x33,
  SUBGROUP_HEADER_SG_NO_PRI = 0x34,
  SUBGROUP_HEADER_SG_EXT_NO_PRI = 0x35,
  SUBGROUP_HEADER_SG_ZERO_EOG_NO_PRI = 0x38,
  SUBGROUP_HEADER_SG_ZERO_EXT_EOG_NO_PRI = 0x39,
  SUBGROUP_HEADER_SG_FIRST_EOG_NO_PRI = 0x3A,
  SUBGROUP_HEADER_SG_FIRST_EXT_EOG_NO_PRI = 0x3B,
  SUBGROUP_HEADER_SG_EOG_NO_PRI = 0x3C,
  SUBGROUP_HEADER_SG_EXT_EOG_NO_PRI = 0x3D,
};

// Subgroup Bit Fields
constexpr uint8_t SG_HAS_EXTENSIONS = 0x1;
constexpr uint8_t SG_SUBGROUP_VALUE = 0x2;
constexpr uint8_t SG_HAS_SUBGROUP_ID = 0x4;
constexpr uint8_t SG_HAS_END_OF_GROUP = 0x8;
constexpr uint8_t SG_PRIORITY_NOT_PRESENT = 0x20;

// Datagram Type Bit Fields
constexpr uint8_t DG_HAS_EXTENSIONS = 0x1;
constexpr uint8_t DG_HAS_STATUS_V11 = 0x2;
constexpr uint8_t DG_HAS_END_OF_GROUP = 0x2;
constexpr uint8_t DG_OBJECT_ID_ZERO = 0x4;
constexpr uint8_t DG_PRIORITY_NOT_PRESENT = 0x8;
constexpr uint8_t DG_IS_STATUS = 0x20;

enum class SubgroupIDFormat : uint8_t { Present, Zero, FirstObject };

struct SubgroupOptions {
  bool hasExtensions{false};
  SubgroupIDFormat subgroupIDFormat{SubgroupIDFormat::Present};
  bool hasEndOfGroup{false};
  bool priorityPresent{true};
};

std::ostream& operator<<(std::ostream& os, FrameType type);

std::ostream& operator<<(std::ostream& os, StreamType type);

enum class SetupKey : uint64_t {
  PATH = 1,
  MAX_REQUEST_ID = 2,
  AUTHORIZATION_TOKEN = 3,
  MAX_AUTH_TOKEN_CACHE_SIZE = 4,
  AUTHORITY = 5,
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

enum class LocationType : uint8_t {
  NextGroupStart = 1,
  LargestObject = 2,
  AbsoluteStart = 3,
  AbsoluteRange = 4,
  LargestGroup = 250,
};

std::string toString(LocationType locType);

struct SubscriptionFilter {
  LocationType filterType{LocationType::AbsoluteStart};
  folly::Optional<AbsoluteLocation> location;
  folly::Optional<uint64_t> endGroup;

  SubscriptionFilter() = default;
  SubscriptionFilter(
      LocationType ft,
      folly::Optional<AbsoluteLocation> loc,
      folly::Optional<uint64_t> eg)
      : filterType(ft), location(loc), endGroup(std::move(eg)) {}
};

struct Parameter {
  uint64_t key = 0;
  std::string asString;
  uint64_t asUint64 = 0;
  AuthToken asAuthToken;
  SubscriptionFilter asSubscriptionFilter;
  folly::Optional<AbsoluteLocation> largestObject;

  // Constructors for Parameter for each type, provided the key and the type

  // String parameter
  Parameter(uint64_t keyIn, const std::string& str)
      : key(keyIn),
        asString(str),
        asUint64(0),
        asAuthToken(),
        asSubscriptionFilter(),
        largestObject() {}

  // uint64_t parameter
  Parameter(uint64_t keyIn, uint64_t uint64)
      : key(keyIn),
        asString(),
        asUint64(uint64),
        asAuthToken(),
        asSubscriptionFilter(),
        largestObject() {}

  // AuthToken parameter
  Parameter(uint64_t keyIn, const AuthToken& token)
      : key(keyIn),
        asString(),
        asUint64(0),
        asAuthToken(token),
        asSubscriptionFilter(),
        largestObject() {}

  // SubscriptionFilter parameter
  Parameter(uint64_t keyIn, const SubscriptionFilter& filter)
      : key(keyIn),
        asString(),
        asUint64(0),
        asAuthToken(),
        asSubscriptionFilter(filter),
        largestObject() {}

  // LargestObject parameter (folly::Optional<AbsoluteLocation>)
  Parameter(uint64_t keyIn, const folly::Optional<AbsoluteLocation>& loc)
      : key(keyIn),
        asString(),
        asUint64(0),
        asAuthToken(),
        asSubscriptionFilter(),
        largestObject(loc) {}

  // Default constructor
  Parameter() = default;
};

using SetupParameter = Parameter;
using TrackRequestParameter = Parameter;

enum class TrackRequestParamKey : uint64_t {
  AUTHORIZATION_TOKEN = 3,
  DELIVERY_TIMEOUT = 2,
  MAX_CACHE_DURATION = 4,
  PUBLISHER_PRIORITY = 0x0E,
  SUBSCRIBER_PRIORITY = 0x20,
  SUBSCRIPTION_FILTER = 0x21,
  EXPIRES = 8,
  GROUP_ORDER = 0x22,
  LARGEST_OBJECT = 0x9,
  FORWARD = 0x10,
};

class Parameters {
 public:
  using const_iterator = std::vector<Parameter>::const_iterator;

  Parameters() = default;

  /* implicit */ Parameters(std::initializer_list<Parameter> params)
      : params_(params) {}

  const Parameter& getParam(size_t position) const {
    return params_.at(position);
  }

  const Parameter& at(size_t position) const {
    return params_.at(position);
  }

  void insertParam(Parameter&& param) {
    params_.emplace_back(std::move(param));
  }

  void insertParam(const Parameter& param) {
    params_.emplace_back(param);
  }

  void insertParam(size_t position, Parameter&& param) {
    CHECK_LE(position, params_.size());
    params_.insert(params_.begin() + position, std::move(param));
  }

  void eraseParam(size_t position) {
    CHECK_LT(position, params_.size());
    params_.erase(params_.begin() + position);
  }

  void modifyString(size_t position, const std::string& newValue) {
    params_.at(position).asString = newValue;
  }

  void eraseAllParamsOfType(TrackRequestParamKey key) {
    const auto targetKey = static_cast<uint64_t>(key);
    params_.erase(
        std::remove_if(
            params_.begin(),
            params_.end(),
            [targetKey](const Parameter& param) {
              return param.key == targetKey;
            }),
        params_.end());
  }

  void modifyParam(
      size_t position,
      const std::string& newString,
      uint64_t newInt64,
      AuthToken newAuthToken) {
    params_.at(position).asString = newString;
    params_.at(position).asUint64 = newInt64;
    params_.at(position).asAuthToken = std::move(newAuthToken);
  }

  const_iterator begin() const {
    return params_.begin();
  }

  const_iterator end() const {
    return params_.end();
  }

  size_t size() const {
    return params_.size();
  }

  bool empty() const {
    return params_.empty();
  }

 private:
  std::vector<Parameter> params_;
};

using SetupParameters = Parameters;
using TrackRequestParameters = Parameters;

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
constexpr uint64_t kVersionDraft12 = 0xff00000C;
constexpr uint64_t kVersionDraft13 = 0xff00000D;
constexpr uint64_t kVersionDraft14 = 0xff00000E;
constexpr uint64_t kVersionDraft15 = 0xff00000F;

constexpr uint64_t kVersionDraftCurrent = kVersionDraft14;

// ALPN constants for version negotiation
constexpr std::string_view kAlpnMoqtLegacy = "moq-00";
constexpr std::string_view kAlpnMoqtDraft15Meta00 = "moqt-15-meta-00";
constexpr std::string_view kAlpnMoqtDraft15Meta01 = "moqt-15-meta-01";
constexpr std::string_view kAlpnMoqtDraft15Meta02 = "moqt-15-meta-02";
constexpr std::string_view kAlpnMoqtDraft15Meta03 = "moqt-15-meta-03";
constexpr std::string_view kAlpnMoqtDraft15Meta04 = "moqt-15-meta-04";
constexpr std::string_view kAlpnMoqtDraft15Meta05 = "moqt-15-meta-05";
constexpr std::string_view kAlpnMoqtDraft15Latest = kAlpnMoqtDraft15Meta05;

// In the terminology I'm using for this function, each draft has a "major"
// and a "minor" version. For example, kVersionDraft08_exp2 has the major
// version 8 and minor version 2.
uint64_t getDraftMajorVersion(uint64_t version);

// ALPN utility functions
bool isLegacyAlpn(folly::StringPiece alpn);
std::vector<uint64_t> getSupportedLegacyVersions();
folly::Optional<uint64_t> getVersionFromAlpn(folly::StringPiece alpn);
folly::Optional<std::string> getAlpnFromVersion(uint64_t version);

// Returns the default list of supported MoQT protocols
// includeExperimental: if true, includes experimental/draft protocols
std::vector<std::string> getDefaultMoqtProtocols(
    bool includeExperimental = false);

constexpr std::array<uint64_t, 2> kSupportedVersions{
    kVersionDraft14,
    kVersionDraft15};

bool isSupportedVersion(uint64_t version);

// Returns a comma-separated list of supported versions, useful for logging.
std::string getSupportedVersionsString();

// Helper function to extract an integer parameter by key from a parameter list
template <class T>
folly::Optional<uint64_t> getFirstIntParam(
    const T& params,
    TrackRequestParamKey key) {
  auto keyValue = folly::to_underlying(key);
  for (const auto& param : params) {
    if (param.key == keyValue) {
      return param.asUint64;
    }
  }
  return folly::none;
}

void writeVarint(
    folly::IOBufQueue& buf,
    uint64_t value,
    size_t& size,
    bool& error) noexcept;

struct ClientSetup {
  std::vector<uint64_t> supportedVersions;
  SetupParameters params;
};

struct ServerSetup {
  uint64_t selectedVersion;
  SetupParameters params;
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

class Extensions {
 private:
  std::vector<Extension> mutableExtensions;
  std::vector<Extension> immutableExtensions;

 public:
  Extensions() = default;

  // Constructor that takes both mutable and immutable extensions
  Extensions(
      const std::vector<Extension>& mutableExts,
      const std::vector<Extension>& immutableExts)
      : mutableExtensions(mutableExts), immutableExtensions(immutableExts) {}

  // Getter for mutableExtensions (returns reference)
  std::vector<Extension>& getMutableExtensions() {
    return mutableExtensions;
  }

  // Getter for mutableExtensions (returns const reference)
  const std::vector<Extension>& getMutableExtensions() const {
    return mutableExtensions;
  }

  // Getter for immutableExtensions (returns const reference)
  const std::vector<Extension>& getImmutableExtensions() const {
    return immutableExtensions;
  }

  // Member functions to insert extensions
  void insertMutableExtension(Extension&& ext) {
    mutableExtensions.emplace_back(std::move(ext));
  }

  void insertImmutableExtension(Extension&& ext) {
    immutableExtensions.emplace_back(std::move(ext));
  }

  void insertMutableExtensions(const std::vector<Extension>& extensions) {
    mutableExtensions.insert(
        mutableExtensions.end(), extensions.begin(), extensions.end());
  }

  void insertImmutableExtensions(const std::vector<Extension>& extensions) {
    immutableExtensions.insert(
        immutableExtensions.end(), extensions.begin(), extensions.end());
  }

  size_t size() const {
    return mutableExtensions.size() + immutableExtensions.size();
  }

  bool empty() const {
    return mutableExtensions.empty() && immutableExtensions.empty();
  }

  // For backward compatibility and testing
  bool operator==(const Extensions& other) const {
    return mutableExtensions == other.mutableExtensions &&
        immutableExtensions == other.immutableExtensions;
  }
};

Extensions noExtensions();

struct ObjectHeader {
  ObjectHeader() = default;
  ObjectHeader(
      uint64_t groupIn,
      uint64_t subgroupIn,
      uint64_t idIn,
      folly::Optional<uint8_t> priorityIn = kDefaultPriority,
      ObjectStatus statusIn = ObjectStatus::NORMAL,
      Extensions extensionsIn = noExtensions(),
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
      folly::Optional<uint8_t> priorityIn,
      uint64_t lengthIn,
      Extensions extensionsIn = noExtensions())
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
  folly::Optional<uint8_t> priority{kDefaultPriority};
  ObjectStatus status{ObjectStatus::NORMAL};
  Extensions extensions;
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
      TrackRequestParameters params = {}) {
    return SubscribeRequest{
        RequestID(), // Default constructed RequestID
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
  FullTrackName fullTrackName;
  uint8_t priority{kDefaultPriority};
  GroupOrder groupOrder;
  bool forward{true};
  LocationType locType;
  folly::Optional<AbsoluteLocation> start;
  uint64_t endGroup;
  TrackRequestParameters params;
};

struct SubscribeUpdate {
  RequestID requestID;
  RequestID subscriptionRequestID;
  folly::Optional<AbsoluteLocation> start;
  folly::Optional<uint64_t> endGroup;
  uint8_t priority{kDefaultPriority};
  // Draft 15+: Optional forward field. When absent, existing forward state is
  // preserved. For earlier drafts, this is always set during parsing.
  folly::Optional<bool> forward;
  TrackRequestParameters params;
};

struct SubscribeOk {
  RequestID requestID;
  TrackAlias trackAlias;
  std::chrono::milliseconds expires;
  GroupOrder groupOrder;
  // context exists is inferred from presence of largest
  folly::Optional<AbsoluteLocation> largest;
  TrackRequestParameters params;
};

// SubscribeError is now an alias for RequestError - see below

struct Unsubscribe {
  RequestID requestID;
};

struct SubscribeDone {
  RequestID requestID;
  SubscribeDoneStatusCode statusCode;
  uint64_t streamCount{0};
  std::string reasonPhrase;
};

struct PublishRequest {
  RequestID requestID{0};
  FullTrackName fullTrackName;
  TrackAlias trackAlias{0};
  GroupOrder groupOrder{GroupOrder::Default};
  folly::Optional<AbsoluteLocation> largest;
  bool forward{true};
  TrackRequestParameters params;
};

struct PublishOk {
  RequestID requestID;
  bool forward;
  uint8_t subscriberPriority;
  GroupOrder groupOrder;
  LocationType locType;
  folly::Optional<AbsoluteLocation> start;
  folly::Optional<uint64_t> endGroup;
  TrackRequestParameters params;
};

// PublishError is now an alias for RequestError - see below

struct Announce {
  RequestID requestID;
  TrackNamespace trackNamespace;
  TrackRequestParameters params;
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

using TrackStatus = SubscribeRequest;

// There are fields in the pre v14 TrackStatusOk (previously named TrackStatus)
// which are not present in SubscribeOk, so we cannot alias directly yet.
struct TrackStatusOk {
  RequestID requestID;
  TrackAlias trackAlias;
  std::chrono::milliseconds expires{};
  GroupOrder groupOrder{};
  // context exists is inferred from presence of largest
  folly::Optional<AbsoluteLocation> largest;
  TrackRequestParameters params;
  // < v14 parameters maintained for compatibility
  FullTrackName fullTrackName;
  TrackStatusCode statusCode{};
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
      TrackRequestParameters pa = {})
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
      TrackRequestParameters pa = {})
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
  TrackRequestParameters params;
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
  TrackRequestParameters params;
};

// FetchError is now an alias for RequestError - see below

struct SubscribeAnnounces {
  RequestID requestID;
  TrackNamespace trackNamespacePrefix;
  bool forward{true}; // Only used in draft-15 and above
  TrackRequestParameters params;
};

// SubscribeAnnouncesError is now an alias for RequestError - see below

struct UnsubscribeAnnounces {
  // Keeping both to maintain compatibility between v15 and v15-
  folly::Optional<RequestID> requestID;
  folly::Optional<TrackNamespace> trackNamespacePrefix;
};

struct RequestOk {
  RequestID requestID;
  TrackRequestParameters params;
  std::vector<Parameter> requestSpecificParams;

  TrackStatusOk toTrackStatusOk() const;
  static RequestOk fromTrackStatusOk(const TrackStatusOk& trackStatusOk);
};

using SubscribeAnnouncesOk = RequestOk;
using AnnounceOk = RequestOk;
using SubscribeUpdateOk = RequestOk;

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
using TrackStatusError = RequestError;
using SubscribeUpdateError = RequestError;

// Error code aliases
using SubscribeErrorCode = RequestErrorCode;
using FetchErrorCode = RequestErrorCode;
using SubscribeAnnouncesErrorCode = RequestErrorCode;
using AnnounceErrorCode = RequestErrorCode;
using PublishErrorCode = RequestErrorCode;
using TrackStatusErrorCode = RequestErrorCode;
using SubscribeUpdateErrorCode = RequestErrorCode;

inline StreamType getSubgroupStreamType(
    uint64_t version,
    SubgroupIDFormat format,
    bool includeExtensions,
    bool endOfGroup,
    bool priorityPresent = true) {
  auto majorVersion = getDraftMajorVersion(version);
  return StreamType(
      folly::to_underlying(StreamType::SUBGROUP_HEADER_MASK) |
      (format == SubgroupIDFormat::Present ? SG_HAS_SUBGROUP_ID : 0) |
      (format == SubgroupIDFormat::FirstObject ? SG_SUBGROUP_VALUE : 0) |
      (includeExtensions ? SG_HAS_EXTENSIONS : 0) |
      (endOfGroup ? SG_HAS_END_OF_GROUP : 0) |
      (majorVersion >= 15 && !priorityPresent ? SG_PRIORITY_NOT_PRESENT : 0));
}

bool isValidSubgroupType(uint64_t version, uint64_t streamType);

inline SubgroupOptions getSubgroupOptions(
    uint64_t version,
    StreamType streamType) {
  SubgroupOptions options;
  auto streamTypeInt = folly::to_underlying(streamType);
  auto majorVersion = getDraftMajorVersion(version);

  options.hasExtensions = streamTypeInt & SG_HAS_EXTENSIONS;
  options.subgroupIDFormat = streamTypeInt & SG_HAS_SUBGROUP_ID
      ? SubgroupIDFormat::Present
      : (streamTypeInt & SG_SUBGROUP_VALUE) ? SubgroupIDFormat::FirstObject
                                            : SubgroupIDFormat::Zero;
  options.hasEndOfGroup =
      folly::to_underlying(streamType) & SG_HAS_END_OF_GROUP;
  // In Draft 15+, check if priority is not present
  if (majorVersion >= 15) {
    options.priorityPresent = !(streamTypeInt & SG_PRIORITY_NOT_PRESENT);
  }
  return options;
}

bool isValidDatagramType(uint64_t version, uint64_t datagramType);
bool datagramPriorityPresent(uint64_t version, DatagramType datagramType);
bool subgroupPriorityPresent(uint64_t version, StreamType streamType);

inline DatagramType getDatagramType(
    uint64_t version,
    bool status,
    bool includeExtensions,
    bool endOfGroup,
    bool isObjectIdZero,
    bool priorityPresent = true) {
  auto majorVersion = getDraftMajorVersion(version);
  if (majorVersion == 11) {
    return DatagramType(
        (status ? DG_HAS_STATUS_V11 : 0) |
        (includeExtensions ? DG_HAS_EXTENSIONS : 0));
  } else if (status) {
    return DatagramType(
        DG_IS_STATUS | (includeExtensions ? DG_HAS_EXTENSIONS : 0) |
        (isObjectIdZero ? DG_OBJECT_ID_ZERO : 0) |
        (majorVersion >= 15 && !priorityPresent ? DG_PRIORITY_NOT_PRESENT : 0));
  } else {
    return DatagramType(
        (includeExtensions ? DG_HAS_EXTENSIONS : 0) |
        (endOfGroup ? DG_HAS_END_OF_GROUP : 0) |
        (isObjectIdZero ? DG_OBJECT_ID_ZERO : 0) |
        (majorVersion >= 15 && !priorityPresent ? DG_PRIORITY_NOT_PRESENT : 0));
  }
}

folly::Expected<std::string, ErrorCode> parseFixedString(
    folly::io::Cursor& cursor,
    size_t& length);

class MoQFrameParser {
 public:
  template <typename T>
  struct ParseResultAndLength {
    T value;
    size_t bytesConsumed;
  };
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

  folly::Expected<ParseResultAndLength<RequestID>, ErrorCode> parseFetchHeader(
      folly::io::Cursor& cursor,
      size_t length) const noexcept;

  struct SubgroupHeaderResult {
    TrackAlias trackAlias;
    ObjectHeader objectHeader;
  };

  folly::Expected<ParseResultAndLength<SubgroupHeaderResult>, ErrorCode>
  parseSubgroupHeader(
      folly::io::Cursor& cursor,
      size_t length,
      const SubgroupOptions& options) const noexcept;

  folly::Expected<ParseResultAndLength<ObjectHeader>, ErrorCode>
  parseFetchObjectHeader(
      folly::io::Cursor& cursor,
      size_t length,
      const ObjectHeader& headerTemplate) const noexcept;

  folly::Expected<ParseResultAndLength<ObjectHeader>, ErrorCode>
  parseSubgroupObjectHeader(
      folly::io::Cursor& cursor,
      size_t length,
      const ObjectHeader& headerTemplate,
      const SubgroupOptions& options) const noexcept;

  folly::Expected<SubscribeRequest, ErrorCode> parseSubscribeRequest(
      folly::io::Cursor& cursor,
      size_t length) const noexcept;

  folly::Expected<SubscribeUpdate, ErrorCode> parseSubscribeUpdate(
      folly::io::Cursor& cursor,
      size_t length) const noexcept;

  folly::Expected<SubscribeOk, ErrorCode> parseSubscribeOk(
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

  folly::Expected<Announce, ErrorCode> parseAnnounce(
      folly::io::Cursor& cursor,
      size_t length) const noexcept;

  folly::Expected<AnnounceOk, ErrorCode> parseAnnounceOk(
      folly::io::Cursor& cursor,
      size_t length) const noexcept;

  folly::Expected<RequestOk, ErrorCode> parseRequestOk(
      folly::io::Cursor& cursor,
      size_t length,
      FrameType frameType) const noexcept;

  folly::Expected<Unannounce, ErrorCode> parseUnannounce(
      folly::io::Cursor& cursor,
      size_t length) const noexcept;

  folly::Expected<AnnounceCancel, ErrorCode> parseAnnounceCancel(
      folly::io::Cursor& cursor,
      size_t length) const noexcept;

  folly::Expected<TrackStatus, ErrorCode> parseTrackStatus(
      folly::io::Cursor& cursor,
      size_t length) const noexcept;

  folly::Expected<TrackStatusOk, ErrorCode> parseTrackStatusOk(
      folly::io::Cursor& cursor,
      size_t length) const noexcept;

  folly::Expected<TrackStatusError, ErrorCode> parseTrackStatusError(
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

  folly::Expected<SubscribeAnnounces, ErrorCode> parseSubscribeAnnounces(
      folly::io::Cursor& cursor,
      size_t length) const noexcept;

  folly::Expected<SubscribeAnnouncesOk, ErrorCode> parseSubscribeAnnouncesOk(
      folly::io::Cursor& cursor,
      size_t length) const noexcept;

  // Unified request error parsing function
  folly::Expected<RequestError, ErrorCode> parseRequestError(
      folly::io::Cursor& cursor,
      size_t length,
      FrameType frameType) const noexcept;

  folly::Expected<UnsubscribeAnnounces, ErrorCode> parseUnsubscribeAnnounces(
      folly::io::Cursor& cursor,
      size_t length) const noexcept;

  folly::Expected<folly::Unit, ErrorCode> parseExtensions(
      folly::io::Cursor& cursor,
      size_t& length,
      ObjectHeader& objectHeader) const noexcept;

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
    previousFetchGroup_ = folly::none;
    previousFetchSubgroup_ = folly::none;
    previousFetchPriority_ = folly::none;
  }

 private:
  // Legacy FETCH object parser (draft <= 14)
  folly::Expected<ObjectHeader, ErrorCode> parseFetchObjectHeaderLegacy(
      folly::io::Cursor& cursor,
      size_t& length,
      const ObjectHeader& headerTemplate) const noexcept;

  // Draft-15+ FETCH object parser with Serialization Flags
  folly::Expected<ObjectHeader, ErrorCode> parseFetchObjectDraft15(
      folly::io::Cursor& cursor,
      size_t& length,
      const ObjectHeader& headerTemplate) const noexcept;

  // Reset fetch context at start of new FETCH stream
  void resetFetchContext() const noexcept;

  folly::Expected<folly::Unit, ErrorCode> parseObjectStatusAndLength(
      folly::io::Cursor& cursor,
      size_t& length,
      ObjectHeader& objectHeader) const noexcept;

  bool isValidStatusForExtensions(
      const ObjectHeader& objectHeader) const noexcept;

  folly::Expected<folly::Unit, ErrorCode> parseTrackRequestParams(
      folly::io::Cursor& cursor,
      size_t& length,
      size_t numParams,
      TrackRequestParameters& params,
      std::vector<Parameter>& requestSpecificParams) const noexcept;

  folly::Expected<folly::Optional<AuthToken>, ErrorCode> parseToken(
      folly::io::Cursor& cursor,
      size_t length) const noexcept;

  folly::Expected<std::vector<std::string>, ErrorCode> parseFixedTuple(
      folly::io::Cursor& cursor,
      size_t& length) const noexcept;

  folly::Expected<FullTrackName, ErrorCode> parseFullTrackName(
      folly::io::Cursor& cursor,
      size_t& length) const noexcept;

  folly::Expected<folly::Unit, ErrorCode> parseExtensionKvPairs(
      folly::io::Cursor& cursor,
      ObjectHeader& objectHeader,
      size_t extensionBlockLength,
      bool allowImmutable = true) const noexcept;

  folly::Expected<folly::Unit, ErrorCode> parseExtension(
      folly::io::Cursor& cursor,
      size_t& length,
      ObjectHeader& objectHeader,
      bool allowImmutable = true) const noexcept;

  folly::Optional<SubscriptionFilter> extractSubscriptionFilter(
      const std::vector<Parameter>& requestSpecificParams) const noexcept;

  void handleRequestSpecificParams(
      SubscribeRequest& subscribeRequest,
      const std::vector<Parameter>& requestSpecificParams) const noexcept;

  void handleRequestSpecificParams(
      SubscribeOk& subscribeOk,
      const std::vector<Parameter>& requestSpecificParams) const noexcept;

  void handleRequestSpecificParams(
      SubscribeUpdate& subscribeUpdate,
      const std::vector<Parameter>& requestSpecificParams) const noexcept;

  void handleRequestSpecificParams(
      PublishRequest& publishRequest,
      const std::vector<Parameter>& requestSpecificParams) const noexcept;

  void handleRequestSpecificParams(
      PublishOk& publishOk,
      const std::vector<Parameter>& requestSpecificParams) const noexcept;

  void handleRequestSpecificParams(
      Fetch& fetchRequest,
      const std::vector<Parameter>& requestSpecificParams) const noexcept;

  void handleGroupOrderParam(
      GroupOrder& groupOrderField,
      const std::vector<Parameter>& requestSpecificParams,
      GroupOrder defaultGroupOrder) const noexcept;

  void handleSubscriberPriorityParam(
      uint8_t& priorityField,
      const std::vector<Parameter>& requestSpecificParams) const noexcept;

  void handleForwardParam(
      bool& forwardField,
      const std::vector<Parameter>& requestSpecificParams) const noexcept;

  // Overload for Optional<bool> - used by SubscribeUpdate
  void handleForwardParam(
      folly::Optional<bool>& forwardField,
      const std::vector<Parameter>& requestSpecificParams) const noexcept;

  folly::Optional<uint64_t> version_;
  mutable MoQTokenCache tokenCache_;
  mutable folly::Optional<uint64_t> previousObjectID_;
  // Context for FETCH object delta encoding (draft-15+)
  mutable folly::Optional<uint64_t> previousFetchGroup_;
  mutable folly::Optional<uint64_t> previousFetchSubgroup_;
  mutable folly::Optional<uint8_t> previousFetchPriority_;
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

  WriteResult writeRequestOk(
      folly::IOBufQueue& writeBuf,
      const RequestOk& requestOk,
      FrameType frameType) const noexcept;

  WriteResult writeUnannounce(
      folly::IOBufQueue& writeBuf,
      const Unannounce& unannounce) const noexcept;

  WriteResult writeAnnounceCancel(
      folly::IOBufQueue& writeBuf,
      const AnnounceCancel& announceCancel) const noexcept;

  WriteResult writeTrackStatus(
      folly::IOBufQueue& writeBuf,
      const TrackStatus& trackStatus) const noexcept;

  WriteResult writeTrackStatusOk(
      folly::IOBufQueue& writeBuf,
      const TrackStatusOk& trackStatusOk) const noexcept;

  WriteResult writeTrackStatusError(
      folly::IOBufQueue& writeBuf,
      const TrackStatusError& trackStatusError) const noexcept;

  WriteResult writeGoaway(folly::IOBufQueue& writeBuf, const Goaway& goaway)
      const noexcept;

  WriteResult writeSubscribeAnnounces(
      folly::IOBufQueue& writeBuf,
      const SubscribeAnnounces& subscribeAnnounces) const noexcept;

  WriteResult writeSubscribeAnnouncesOk(
      folly::IOBufQueue& writeBuf,
      const SubscribeAnnouncesOk& subscribeAnnouncesOk) const noexcept;

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

  void writeExtensions(
      folly::IOBufQueue& writeBuf,
      const Extensions& extensions,
      size_t& size,
      bool& error) const noexcept;

 private:
  void writeKeyValuePairs(
      folly::IOBufQueue& writeBuf,
      const std::vector<Extension>& extensions,
      size_t& size,
      bool& error) const noexcept;

  size_t calculateExtensionVectorSize(
      const std::vector<Extension>& extensions,
      bool& error) const noexcept;

  void writeTrackRequestParams(
      folly::IOBufQueue& writeBuf,
      const TrackRequestParameters& params,
      const std::vector<Parameter>& requestSpecificParams,
      size_t& size,
      bool& error) const noexcept;

  void writeSubscriptionFilter(
      folly::IOBufQueue& writeBuf,
      const SubscriptionFilter& filter,
      size_t& size,
      bool& error) const noexcept;

  WriteResult writeSubscribeOkHelper(
      folly::IOBufQueue& writeBuf,
      const SubscribeOk& subscribeOk) const noexcept;

  WriteResult writeSubscribeRequestHelper(
      folly::IOBufQueue& writeBuf,
      const SubscribeRequest& subscribeRequest) const noexcept;

  // Legacy FETCH object writer (draft <= 14)
  void writeFetchObjectHeaderLegacy(
      folly::IOBufQueue& writeBuf,
      const ObjectHeader& objectHeader,
      size_t& size,
      bool& error) const noexcept;

  // Draft-15+ FETCH object writer with Serialization Flags
  void writeFetchObjectDraft15(
      folly::IOBufQueue& writeBuf,
      const ObjectHeader& objectHeader,
      size_t& size,
      bool& error) const noexcept;

  void resetWriterFetchContext() const noexcept;

  folly::Optional<uint64_t> version_;
  mutable folly::Optional<uint64_t> previousObjectID_;
  // Context for FETCH object delta encoding (draft-15+)
  mutable folly::Optional<uint64_t> previousFetchGroup_;
  mutable folly::Optional<uint64_t> previousFetchSubgroup_;
  mutable folly::Optional<uint8_t> previousFetchPriority_;
};

} // namespace moxygen

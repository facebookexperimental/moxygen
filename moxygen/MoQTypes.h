/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/Range.h>
#include <folly/String.h>
#include <folly/hash/Hash.h>
#include <folly/io/IOBuf.h>
#include <folly/logging/xlog.h>
#include <quic/codec/QuicInteger.h>
#include <algorithm>
#include <array>
#include <optional>
#include <vector>

namespace moxygen {

//////// Constants ////////
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

  // Note: Pending draft update
  GOING_AWAY = 6,

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

enum class FrameType : uint64_t {
  SUBSCRIBE_UPDATE = 2,
  SUBSCRIBE = 3,
  SUBSCRIBE_OK = 4,
  SUBSCRIBE_ERROR = 5,
  REQUEST_ERROR = 5,
  ANNOUNCE = 0x6,
  ANNOUNCE_OK = 0x7,
  REQUEST_OK = 0x7,
  ANNOUNCE_ERROR = 0x8, // Draft 15 and below
  NAMESPACE = 0x8,      // Draft 16 and above
  UNANNOUNCE = 9,
  UNSUBSCRIBE = 0xA,
  SUBSCRIBE_DONE = 0xB,
  ANNOUNCE_CANCEL = 0xC,
  TRACK_STATUS = 0xD,
  TRACK_STATUS_OK = 0xE, // Draft 15 and below
  NAMESPACE_DONE = 0xE,  // Draft 16 and above
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
  std::optional<uint64_t> alias;
  // Set alias to one of these constants when sending an AuthToken parameter
  // Register: Will attempt to save the token in the cache, there is space
  // DontRegister: Will not attempt to save the token in the cache
  static constexpr uint64_t Register = 1;
  static constexpr std::optional<uint64_t> DontRegister = std::nullopt;
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
  std::optional<AbsoluteLocation> location;
  std::optional<uint64_t> endGroup;

  SubscriptionFilter() = default;
  SubscriptionFilter(
      LocationType ft,
      std::optional<AbsoluteLocation> loc,
      std::optional<uint64_t> eg)
      : filterType(ft), location(loc), endGroup(std::move(eg)) {}
};

struct Parameter {
  uint64_t key = 0;
  std::string asString;
  uint64_t asUint64 = 0;
  AuthToken asAuthToken;
  SubscriptionFilter asSubscriptionFilter;
  std::optional<AbsoluteLocation> largestObject;

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

  // LargestObject parameter (std::optional<AbsoluteLocation>)
  Parameter(uint64_t keyIn, const std::optional<AbsoluteLocation>& loc)
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

  explicit Parameters(FrameType frameType) : frameType_(frameType) {}

  FrameType getFrameType() const {
    return frameType_;
  }

  // Validates if a parameter is allowed for frameType_
  bool isParamAllowed(TrackRequestParamKey key) const;

  const Parameter& getParam(size_t position) const {
    return params_.at(position);
  }

  const Parameter& at(size_t position) const {
    return params_.at(position);
  }

  folly::Expected<folly::Unit, ErrorCode> insertParam(Parameter&& param) {
    auto key = static_cast<TrackRequestParamKey>(param.key);
    if (!isParamAllowed(key)) {
      return folly::makeUnexpected(ErrorCode::INVALID_REQUEST_ID);
    }
    params_.emplace_back(std::move(param));
    return folly::unit;
  }

  folly::Expected<folly::Unit, ErrorCode> insertParam(const Parameter& param) {
    auto key = static_cast<TrackRequestParamKey>(param.key);
    if (!isParamAllowed(key)) {
      return folly::makeUnexpected(ErrorCode::INVALID_REQUEST_ID);
    }
    params_.emplace_back(param);
    return folly::unit;
  }

  folly::Expected<folly::Unit, ErrorCode> insertParam(
      size_t position,
      Parameter&& param) {
    auto key = static_cast<TrackRequestParamKey>(param.key);
    if (!isParamAllowed(key)) {
      return folly::makeUnexpected(ErrorCode::INVALID_REQUEST_ID);
    }
    CHECK_LE(position, params_.size());
    params_.insert(params_.begin() + position, std::move(param));
    return folly::unit;
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
  FrameType frameType_{};
  std::vector<Parameter> params_;
};

using SetupParameters = Parameters;
using TrackRequestParameters = Parameters;

// Helper function to extract an integer parameter by key from a parameter list
template <class T>
std::optional<uint64_t> getFirstIntParam(
    const T& params,
    TrackRequestParamKey key) {
  auto keyValue = folly::to_underlying(key);
  for (const auto& param : params) {
    if (param.key == keyValue) {
      return param.asUint64;
    }
  }
  return std::nullopt;
}

struct ClientSetup {
  std::vector<uint64_t> supportedVersions;
  SetupParameters params{FrameType::CLIENT_SETUP};
};

struct ServerSetup {
  uint64_t selectedVersion;
  SetupParameters params{FrameType::SERVER_SETUP};
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
      std::optional<uint8_t> priorityIn = kDefaultPriority,
      ObjectStatus statusIn = ObjectStatus::NORMAL,
      Extensions extensionsIn = noExtensions(),
      std::optional<uint64_t> lengthIn = std::nullopt)
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
      std::optional<uint8_t> priorityIn,
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
  std::optional<uint8_t> priority{kDefaultPriority};
  ObjectStatus status{ObjectStatus::NORMAL};
  Extensions extensions;
  std::optional<uint64_t> length{std::nullopt};

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
      std::optional<AbsoluteLocation> start = std::nullopt,
      uint64_t endGroup = 0,
      const std::vector<Parameter>& inputParams = {}) {
    SubscribeRequest req = SubscribeRequest{
        RequestID(), // Default constructed RequestID
        fullTrackName,
        priority,
        groupOrder,
        forward,
        locType,
        std::move(start),
        endGroup,
        TrackRequestParameters{FrameType::SUBSCRIBE}};

    for (const auto& param : inputParams) {
      auto result = req.params.insertParam(param);
      if (result.hasError()) {
        XLOG(ERR) << "SubscribeRequest::make: param not allowed, key="
                  << param.key;
      }
    }
    return req;
  }

  RequestID requestID;
  FullTrackName fullTrackName;
  uint8_t priority{kDefaultPriority};
  GroupOrder groupOrder;
  bool forward{true};
  LocationType locType;
  std::optional<AbsoluteLocation> start;
  uint64_t endGroup;
  TrackRequestParameters params{FrameType::SUBSCRIBE};
};

struct SubscribeUpdate {
  RequestID requestID;
  RequestID subscriptionRequestID;
  std::optional<AbsoluteLocation> start;
  std::optional<uint64_t> endGroup;
  uint8_t priority{kDefaultPriority};
  // Draft 15+: Optional forward field. When absent, existing forward state is
  // preserved. For earlier drafts, this is always set during parsing.
  std::optional<bool> forward;
  TrackRequestParameters params{FrameType::SUBSCRIBE_UPDATE};
};

struct SubscribeOk {
  RequestID requestID;
  TrackAlias trackAlias;
  std::chrono::milliseconds expires;
  GroupOrder groupOrder;
  // context exists is inferred from presence of largest
  std::optional<AbsoluteLocation> largest;
  TrackRequestParameters params{FrameType::SUBSCRIBE_OK};
};

// SubscribeError is now an alias for RequestError

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
  std::optional<AbsoluteLocation> largest;
  bool forward{true};
  TrackRequestParameters params{FrameType::PUBLISH};
};

struct PublishOk {
  RequestID requestID;
  bool forward;
  uint8_t subscriberPriority;
  GroupOrder groupOrder;
  LocationType locType;
  std::optional<AbsoluteLocation> start;
  std::optional<uint64_t> endGroup;
  TrackRequestParameters params{FrameType::PUBLISH_OK};
};

// PublishError is now an alias for RequestError

struct Announce {
  RequestID requestID;
  TrackNamespace trackNamespace;
  TrackRequestParameters params{FrameType::ANNOUNCE};
};

// AnnounceError is now an alias for RequestError - see below

struct Unannounce {
  TrackNamespace trackNamespace;      // Used in v15 and below
  std::optional<RequestID> requestID; // Used in v16+
};

struct AnnounceCancel {
  TrackNamespace trackNamespace;      // Used in v15 and below
  std::optional<RequestID> requestID; // Used in v16+
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
  std::optional<AbsoluteLocation> largest;
  TrackRequestParameters params{FrameType::REQUEST_OK};
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
      const std::vector<Parameter>& pa = {})
      : requestID(su),
        fullTrackName(std::move(ftn)),
        priority(p),
        groupOrder(g),
        args(StandaloneFetch(st, e)) {
    for (const auto& param : pa) {
      auto result = params.insertParam(param);
      if (result.hasError()) {
        XLOG(ERR) << "Fetch: param not allowed, key=" << param.key;
      }
    }
  }

  // Used for absolute or relative joining fetches
  Fetch(
      RequestID su,
      RequestID jsid,
      uint64_t joiningStart,
      FetchType fetchType,
      uint8_t p = kDefaultPriority,
      GroupOrder g = GroupOrder::Default,
      const std::vector<Parameter>& pa = {})
      : requestID(su),
        priority(p),
        groupOrder(g),
        args(JoiningFetch(jsid, joiningStart, fetchType)) {
    CHECK(
        fetchType == FetchType::RELATIVE_JOINING ||
        fetchType == FetchType::ABSOLUTE_JOINING);
    for (const auto& param : pa) {
      auto result = params.insertParam(param);
      if (result.hasError()) {
        XLOG(ERR) << "Fetch: param not allowed, key=" << param.key;
      }
    }
  }

  RequestID requestID;
  FullTrackName fullTrackName;
  uint8_t priority{kDefaultPriority};
  GroupOrder groupOrder;
  TrackRequestParameters params{FrameType::FETCH};
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
  TrackRequestParameters params{FrameType::FETCH_OK};
};

// FetchError is now an alias for RequestError - see below
enum SubscribeAnnouncesOptions {
  PUBLISH = 0,
  NAMESPACE = 1,
  BOTH = 2,
};

struct SubscribeAnnounces {
  RequestID requestID;
  TrackNamespace trackNamespacePrefix;
  bool forward{true}; // Only used in draft-15 and above
  TrackRequestParameters params{FrameType::SUBSCRIBE_ANNOUNCES};
  SubscribeAnnouncesOptions options; // Only used in draft-16 and above
};

// Only used in draft-16 and above
struct NamespaceDone {
  TrackNamespace trackNamespaceSuffix;
};

// Only used in draft-16 and above
struct Namespace {
  TrackNamespace trackNamespaceSuffix;
};

// SubscribeAnnouncesError is now an alias for RequestError - see below

struct UnsubscribeAnnounces {
  // Keeping both to maintain compatibility between v15 and v15-
  std::optional<RequestID> requestID;
  std::optional<TrackNamespace> trackNamespacePrefix;
};

struct RequestOk {
  RequestID requestID;
  TrackRequestParameters params{FrameType::REQUEST_OK};
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

} // namespace moxygen

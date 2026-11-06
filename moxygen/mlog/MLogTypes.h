/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <variant>
#include <vector>
#include "folly/io/IOBuf.h"
#include "folly/json/dynamic.h"

namespace moxygen {

// Constants + Helper Structs

enum VantagePoint : int {
  CLIENT = 0,
  SERVER = 1,
};

enum ControlMessageType : int {
  CREATED = 0,
  PARSED = 1,
};

enum class MOQTFetchRangeEndType : uint8_t {
  NONEXISTENT,
  UNKNOWN,
  TIMED_OUT,
};

// Data Type Structs

enum MOQTStreamType { CONTROL, SUBGROUP_HEADER, FETCH_HEADER, PADDING };

enum Owner { LOCAL, REMOTE };

struct MOQTRawInfo {
  std::optional<uint64_t> length;
  std::optional<uint64_t> payloadLength;
  std::optional<std::string> data;

  folly::dynamic toDynamic() const;
};

struct MOQTProperty {
  uint64_t type{0};
  std::optional<uint64_t> value;
  std::optional<uint64_t> length;
  std::optional<MOQTRawInfo> valueBytes;

  folly::dynamic toDynamic() const;
};

// Parameters are extensible maps in the mlog schema. Keeping their encoded
// representation here lets MLogger support both current and unknown types.
struct MOQTParameter {
  folly::dynamic fields = folly::dynamic::object;

  MOQTParameter() = default;
  explicit MOQTParameter(folly::dynamic value) : fields(std::move(value)) {}
};

folly::dynamic parameterToDynamic(const MOQTParameter& param);

struct MOQTSetupOption {
  folly::dynamic fields = folly::dynamic::object;

  MOQTSetupOption() = default;
  explicit MOQTSetupOption(folly::dynamic value) : fields(std::move(value)) {}
};

using MOQTSetupParameter = MOQTSetupOption;

// Helper to convert variant to dynamic
folly::dynamic setupParameterToDynamic(const MOQTSetupParameter& param);

enum MOQTByteStringType { STRING_VALUE, VALUE_BYTES, UNKNOWN_VALUE };
struct MOQTByteString {
  std::string value;
  std::unique_ptr<folly::IOBuf> valueBytes;
  MOQTByteStringType type = MOQTByteStringType::UNKNOWN_VALUE;

  MOQTByteString() {}
  MOQTByteString(const MOQTByteString& other) = delete;
  MOQTByteString& operator=(const MOQTByteString& other) = delete;
  MOQTByteString(MOQTByteString&& other) noexcept = default;
  MOQTByteString& operator=(MOQTByteString&& other) noexcept = default;

  ~MOQTByteString() {}

  folly::dynamic toDynamic() const;
};

class MOQTBaseControlMessage {
 public:
  MOQTBaseControlMessage() = default;
  MOQTBaseControlMessage(const MOQTBaseControlMessage&) = default;
  MOQTBaseControlMessage& operator=(const MOQTBaseControlMessage&) = default;
  MOQTBaseControlMessage(MOQTBaseControlMessage&&) noexcept = default;
  MOQTBaseControlMessage& operator=(MOQTBaseControlMessage&&) noexcept =
      default;
  std::string type;
  virtual folly::dynamic toDynamic() const = 0;
  virtual ~MOQTBaseControlMessage() {}

  // Helper Methods Extended to BaseControlMessage Classes
  std::vector<folly::dynamic> parseTrackNamespace(
      const std::vector<MOQTByteString>& trackNamespace) const;
  folly::dynamic parseTrackName(const MOQTByteString& trackName) const;
};

class MOQTSetupMessage : public MOQTBaseControlMessage {
 public:
  explicit MOQTSetupMessage(const std::string& /*setupType*/ = "setup") {
    type = "setup";
  }
  folly::dynamic toDynamic() const override;
  std::vector<MOQTSetupOption> setupOptions;
};

// Keep old names as aliases for backward compatibility
using MOQTClientSetupMessage = MOQTSetupMessage;
using MOQTServerSetupMessage = MOQTSetupMessage;

class MOQTGoaway : public MOQTBaseControlMessage {
 public:
  MOQTGoaway() {
    type = "goaway";
  }
  folly::dynamic toDynamic() const override;
  MOQTRawInfo newSessionUri;
  uint64_t timeout{0};
};

struct MOQTLocation {
  uint64_t group{0};
  uint64_t object{0};

  folly::dynamic toDynamic() const;
};

struct MOQTRedirect {
  MOQTRawInfo connectUri;
  std::vector<MOQTByteString> trackNamespace;
  MOQTByteString trackName;

  folly::dynamic toDynamic() const;
};

class MOQTRequestOk : public MOQTBaseControlMessage {
 public:
  MOQTRequestOk() {
    type = "request_ok";
  }
  folly::dynamic toDynamic() const override;
  uint64_t numberOfParameters{0};
  std::vector<MOQTParameter> parameters;
  std::vector<MOQTProperty> trackProperties;
};

class MOQTRequestError : public MOQTBaseControlMessage {
 public:
  MOQTRequestError() {
    type = "request_error";
  }
  folly::dynamic toDynamic() const override;
  uint64_t errorCode{0};
  uint64_t retryInterval{0};
  std::optional<std::string> reasonPhrase;
  std::optional<MOQTRedirect> redirect;
};

class MOQTSubscribe : public MOQTBaseControlMessage {
 public:
  MOQTSubscribe() {
    type = "subscribe";
  }
  folly::dynamic toDynamic() const override;
  uint64_t requestId{0};
  std::vector<MOQTByteString> trackNamespace;
  MOQTByteString trackName;
  uint8_t subscriberPriority{};
  uint8_t groupOrder{};
  uint8_t forward{};
  uint64_t filterType{};
  std::optional<MOQTLocation> startLocation;
  std::optional<uint64_t> endGroup;
  uint64_t numberOfParameters{};
  std::vector<MOQTParameter> subscribeParameters;
};

class MOQTSubscribeUpdate : public MOQTBaseControlMessage {
 public:
  MOQTSubscribeUpdate() {
    type = "request_update";
  }
  folly::dynamic toDynamic() const override;
  uint64_t requestId{0};
  uint64_t subscriptionRequestId{0};
  MOQTLocation startLocation;
  uint64_t endGroup{};
  uint8_t subscriberPriority{};
  uint8_t forward{};
  uint64_t numberOfParameters{};
  std::vector<MOQTParameter> parameters;
};

class MOQTPublishStateNotify : public MOQTBaseControlMessage {
 public:
  MOQTPublishStateNotify() {
    type = "publish_state_notify";
  }
  folly::dynamic toDynamic() const override;
  uint64_t numberOfParameters{0};
  std::vector<MOQTParameter> parameters;
};

class MOQTUnsubscribe : public MOQTBaseControlMessage {
 public:
  MOQTUnsubscribe() {
    type = "unsubscribe";
  }
  folly::dynamic toDynamic() const override;
  uint64_t subscribeId{0};
};

struct MOQTStandaloneFetch {
  std::vector<MOQTByteString> trackNamespace;
  MOQTByteString trackName;
  MOQTLocation startLocation;
  MOQTLocation endLocation;

  folly::dynamic toDynamic() const;
};

struct MOQTJoiningFetch {
  uint64_t joiningRequestId{0};
  uint64_t joiningStart{0};

  folly::dynamic toDynamic() const;
};

class MOQTFetch : public MOQTBaseControlMessage {
 public:
  MOQTFetch() {
    type = "fetch";
  }
  folly::dynamic toDynamic() const override;
  uint64_t requestId{0};
  std::vector<MOQTByteString> trackNamespace;
  MOQTByteString trackName;
  uint8_t subscriberPriority{};
  uint8_t groupOrder{};
  std::string fetchType;
  std::optional<MOQTStandaloneFetch> standaloneFetch;
  std::optional<MOQTJoiningFetch> joiningFetch;
  uint64_t numberOfParameters{0};
  std::vector<MOQTParameter> parameters;
};

class MOQTFetchCancel : public MOQTBaseControlMessage {
 public:
  MOQTFetchCancel() {
    type = "fetch_cancel";
  }
  folly::dynamic toDynamic() const override;
  uint64_t requestId{0};
};

class MOQTPublishNamespaceOk : public MOQTBaseControlMessage {
 public:
  MOQTPublishNamespaceOk() {
    type = "publishNamespace_ok";
  }
  folly::dynamic toDynamic() const override;
  uint64_t requestId{0};
};

class MOQTPublishNamespaceError : public MOQTBaseControlMessage {
 public:
  MOQTPublishNamespaceError() {
    type = "publishNamespace_error";
  }
  folly::dynamic toDynamic() const override;
  uint64_t requestId{0};
  uint64_t errorCode{};
  std::optional<std::string> reason;
  std::optional<std::string> reasonBytes;
};

class MOQTPublishNamespaceCancel : public MOQTBaseControlMessage {
 public:
  MOQTPublishNamespaceCancel() {
    type = "publishNamespace_cancel";
  }
  folly::dynamic toDynamic() const override;
  std::optional<uint64_t> requestId;
  uint64_t errorCode{};
  std::optional<std::string> reason;
  std::optional<std::string> reasonBytes;
};

class MOQTTrackStatus : public MOQTBaseControlMessage {
 public:
  MOQTTrackStatus() {
    type = "track_status";
  }
  folly::dynamic toDynamic() const override;
  uint64_t requestId{0};
  std::vector<MOQTByteString> trackNamespace;
  MOQTByteString trackName;
  uint8_t subscriberPriority{};
  uint8_t groupOrder{};
  uint8_t forward{};
  uint64_t filterType{};
  std::optional<MOQTLocation> startLocation;
  std::optional<uint64_t> endGroup;
  uint64_t numberOfParameters{};
  std::vector<MOQTParameter> parameters;
};

class MOQTSubscribeNamespace : public MOQTBaseControlMessage {
 public:
  MOQTSubscribeNamespace() {
    type = "subscribe_namespace";
  }
  folly::dynamic toDynamic() const override;
  uint64_t requestId{0};
  std::vector<MOQTByteString> trackNamespacePrefix;
  uint64_t numberOfParameters{};
  std::vector<MOQTParameter> parameters;
};

class MOQTSubscribeTracks : public MOQTBaseControlMessage {
 public:
  MOQTSubscribeTracks() {
    type = "subscribe_tracks";
  }
  folly::dynamic toDynamic() const override;
  uint64_t requestId{0};
  std::vector<MOQTByteString> trackNamespacePrefix;
  uint64_t numberOfParameters{0};
  std::vector<MOQTParameter> parameters;
};

class MOQTUnsubscribeNamespace : public MOQTBaseControlMessage {
 public:
  MOQTUnsubscribeNamespace() {
    type = "unsubscribe_namespace";
  }
  folly::dynamic toDynamic() const override;
  std::optional<uint64_t> requestId;
};

class MOQTSubscribeOk : public MOQTBaseControlMessage {
 public:
  MOQTSubscribeOk() {
    type = "subscribe_ok";
  }
  folly::dynamic toDynamic() const override;
  uint64_t requestId{0};
  uint64_t trackAlias{0};
  uint64_t expires{};
  uint8_t groupOrder{};
  uint8_t contentExists{};
  std::optional<MOQTLocation> largestLocation;
  uint64_t numberOfParameters{};
  std::vector<MOQTParameter> parameters;
  std::vector<MOQTProperty> trackProperties;
};

class MOQTSubscribeError : public MOQTBaseControlMessage {
 public:
  MOQTSubscribeError() {
    type = "subscribe_error";
  }
  folly::dynamic toDynamic() const override;
  uint64_t requestId{0};
  uint64_t errorCode{};
  std::optional<std::string> reason;
  std::optional<std::string> reasonBytes;
};

class MOQTFetchOk : public MOQTBaseControlMessage {
 public:
  MOQTFetchOk() {
    type = "fetch_ok";
  }
  folly::dynamic toDynamic() const override;
  uint64_t requestId{0};
  uint8_t groupOrder{};
  uint8_t endOfTrack{};
  MOQTLocation endLocation;
  uint64_t numberOfParameters{};
  std::vector<MOQTParameter> parameters;
  std::vector<MOQTProperty> trackProperties;
};

class MOQTFetchError : public MOQTBaseControlMessage {
 public:
  MOQTFetchError() {
    type = "fetch_error";
  }
  folly::dynamic toDynamic() const override;
  uint64_t requestId{0};
  uint64_t errorCode{};
  std::optional<std::string> reason;
  std::optional<std::string> reasonBytes;
};

class MOQTPublishDone : public MOQTBaseControlMessage {
 public:
  MOQTPublishDone() {
    type = "publish_done";
  }
  folly::dynamic toDynamic() const override;
  uint64_t requestId{0};
  uint64_t statusCode{};
  uint64_t streamCount{};
  std::optional<std::string> reason;
  std::optional<std::string> reasonBytes;
};

class MOQTMaxRequestId : public MOQTBaseControlMessage {
 public:
  MOQTMaxRequestId() {
    type = "max_request_id";
  }
  folly::dynamic toDynamic() const override;
  uint64_t requestId{0};
};

class MOQTRequestsBlocked : public MOQTBaseControlMessage {
 public:
  MOQTRequestsBlocked() {
    type = "requests_blocked";
  }
  folly::dynamic toDynamic() const override;
  uint64_t maximumRequestId{0};
};

class MOQTPublishNamespace : public MOQTBaseControlMessage {
 public:
  MOQTPublishNamespace() {
    type = "publish_namespace";
  }
  folly::dynamic toDynamic() const override;
  uint64_t requestId{0};
  std::vector<MOQTByteString> trackNamespace;
  uint64_t numberOfParameters{};
  std::vector<MOQTParameter> parameters;
};

class MOQTNamespace : public MOQTBaseControlMessage {
 public:
  MOQTNamespace() {
    type = "namespace";
  }
  folly::dynamic toDynamic() const override;
  std::vector<MOQTByteString> trackNamespaceSuffix;
};

class MOQTNamespaceDone : public MOQTBaseControlMessage {
 public:
  MOQTNamespaceDone() {
    type = "namespace_done";
  }
  folly::dynamic toDynamic() const override;
  std::vector<MOQTByteString> trackNamespaceSuffix;
};

class MOQTPublishSkipped : public MOQTBaseControlMessage {
 public:
  MOQTPublishSkipped() {
    type = "publish_skipped";
  }
  folly::dynamic toDynamic() const override;
  std::vector<MOQTByteString> trackNamespaceSuffix;
  MOQTByteString trackName;
};

class MOQTPublishNamespaceDone : public MOQTBaseControlMessage {
 public:
  MOQTPublishNamespaceDone() {
    type = "publishNamespaceDone";
  }
  folly::dynamic toDynamic() const override;
  std::optional<uint64_t> requestId;
};

class MOQTTrackStatusOk : public MOQTBaseControlMessage {
 public:
  MOQTTrackStatusOk() {
    type = "track_status_ok";
  }
  folly::dynamic toDynamic() const override;
  uint64_t requestId{0};
  uint64_t trackAlias{0};
  uint64_t expires{};
  uint8_t groupOrder{};
  uint8_t contentExists{};
  std::optional<MOQTLocation> largestLocation;
  uint64_t numberOfParameters{};
  std::vector<MOQTParameter> parameters;
};

class MOQTTrackStatusError : public MOQTBaseControlMessage {
 public:
  MOQTTrackStatusError() {
    type = "track_status_error";
  }
  folly::dynamic toDynamic() const override;
  uint64_t requestId{0};
  uint64_t errorCode{};
  std::optional<std::string> reason;
  std::optional<std::string> reasonBytes;
};

class MOQTSubscribeNamespaceOk : public MOQTBaseControlMessage {
 public:
  MOQTSubscribeNamespaceOk() {
    type = "subscribe_namespace_ok";
  }
  folly::dynamic toDynamic() const override;
  uint64_t requestId{0};
};

class MOQTSubscribeNamespaceError : public MOQTBaseControlMessage {
 public:
  MOQTSubscribeNamespaceError() {
    type = "subscribe_namespace_error";
  }
  folly::dynamic toDynamic() const override;
  uint64_t requestId{0};
  uint64_t errorCode{};
  std::optional<std::string> reason;
  std::optional<std::string> reasonBytes;
};

class MOQTPublish : public MOQTBaseControlMessage {
 public:
  MOQTPublish() {
    type = "publish";
  }
  folly::dynamic toDynamic() const override;
  uint64_t requestId{0};
  std::vector<MOQTByteString> trackNamespace;
  MOQTByteString trackName;
  uint64_t trackAlias{0};
  uint8_t groupOrder{};
  uint8_t contentExists{};
  std::optional<MOQTLocation> largest;
  uint8_t forward{};
  uint64_t numberOfParameters{};
  std::vector<MOQTParameter> parameters;
  std::vector<MOQTProperty> trackProperties;
};

class MOQTPublishOk : public MOQTBaseControlMessage {
 public:
  MOQTPublishOk() {
    type = "publish_ok";
  }
  folly::dynamic toDynamic() const override;
  uint64_t requestId{0};
  uint8_t forward{};
  uint8_t subscriberPriority{};
  uint8_t groupOrder{};
  uint64_t filterType{};
  std::optional<MOQTLocation> start;
  std::optional<uint64_t> endGroup;
  uint64_t numberOfParameters{};
  std::vector<MOQTParameter> parameters;
};

class MOQTPublishError : public MOQTBaseControlMessage {
 public:
  MOQTPublishError() {
    type = "publish_error";
  }
  folly::dynamic toDynamic() const override;
  uint64_t requestId{0};
  uint64_t errorCode{};
  std::optional<std::string> reason;
  std::optional<std::string> reasonBytes;
};

// MOQTEvents Structs
struct MOQTControlMessageCreated {
  uint64_t streamId{0};
  std::optional<uint64_t> length;
  std::unique_ptr<MOQTBaseControlMessage> message;

  folly::dynamic toDynamic() const;
};

struct MOQTControlMessageParsed {
  uint64_t streamId{0};
  std::optional<uint64_t> length;
  std::unique_ptr<MOQTBaseControlMessage> message;

  folly::dynamic toDynamic() const;
};

struct MOQTStreamTypeSet {
  std::optional<Owner> owner;
  uint64_t streamId{0};
  MOQTStreamType streamType;

  folly::dynamic toDynamic() const;
};

struct MOQTObjectDatagramCreated {
  uint64_t trackAlias{0};
  uint64_t groupId{0};
  std::optional<uint64_t> objectId;
  std::optional<uint8_t> publisherPriority;
  std::optional<uint64_t> propertiesLength;
  std::vector<MOQTProperty> properties;
  std::optional<uint64_t> objectStatus;
  std::optional<MOQTRawInfo> objectPayload;
  bool endOfGroup{false};

  folly::dynamic toDynamic() const;
};

struct MOQTObjectDatagramParsed {
  uint64_t trackAlias{0};
  uint64_t groupId{0};
  std::optional<uint64_t> objectId;
  std::optional<uint8_t> publisherPriority;
  std::optional<uint64_t> propertiesLength;
  std::vector<MOQTProperty> properties;
  std::optional<uint64_t> objectStatus;
  std::optional<MOQTRawInfo> objectPayload;
  bool endOfGroup{false};

  folly::dynamic toDynamic() const;
};

struct MOQTSubgroupHeaderCreated {
  uint64_t streamId{0};
  uint64_t trackAlias{0};
  uint64_t groupId{0};
  uint8_t subgroupIdMode{0};
  std::optional<uint64_t> subgroupId;
  std::optional<uint8_t> publisherPriority;
  bool containsEndOfGroup{false};
  bool propertiesPresent{false};
  bool firstObject{false};

  folly::dynamic toDynamic() const;
};

struct MOQTSubgroupHeaderParsed {
  uint64_t streamId{0};
  uint64_t trackAlias{0};
  uint64_t groupId{0};
  uint8_t subgroupIdMode{0};
  std::optional<uint64_t> subgroupId;
  std::optional<uint8_t> publisherPriority;
  bool containsEndOfGroup{false};
  bool propertiesPresent{false};
  bool firstObject{false};

  folly::dynamic toDynamic() const;
};

struct MOQTSubgroupObjectCreated {
  uint64_t streamId{0};
  uint64_t objectIdDelta{0};
  std::vector<MOQTProperty> properties;
  uint64_t objectPayloadLength{0};
  std::optional<uint64_t> objectStatus;

  folly::dynamic toDynamic() const;
};

struct MOQTSubgroupObjectParsed {
  uint64_t streamId{0};
  uint64_t objectIdDelta{0};
  std::vector<MOQTProperty> properties;
  uint64_t objectPayloadLength{0};
  std::optional<uint64_t> objectStatus;

  folly::dynamic toDynamic() const;
};

struct MOQTFetchHeaderCreated {
  uint64_t streamId{0};
  uint64_t requestId{0};

  folly::dynamic toDynamic() const;
};

struct MOQTFetchHeaderParsed {
  uint64_t streamId{0};
  uint64_t requestId{0};

  folly::dynamic toDynamic() const;
};

struct MOQTFetchObjectCreated {
  uint64_t streamId{0};
  bool datagram{false};
  bool endOfNonexistentRange{false};
  bool endOfUnknownRange{false};
  bool endOfTimedOutRange{false};
  std::optional<uint8_t> subgroupIdBits;
  std::optional<uint64_t> groupIdDelta;
  std::optional<uint64_t> subgroupId;
  std::optional<uint64_t> objectIdDelta;
  std::optional<uint8_t> publisherPriority;
  std::optional<uint64_t> propertiesLength;
  std::vector<MOQTProperty> properties;
  uint64_t objectPayloadLength{0};

  folly::dynamic toDynamic() const;
};

struct MOQTFetchObjectParsed {
  uint64_t streamId{0};
  bool datagram{false};
  bool endOfNonexistentRange{false};
  bool endOfUnknownRange{false};
  bool endOfTimedOutRange{false};
  std::optional<uint8_t> subgroupIdBits;
  std::optional<uint64_t> groupIdDelta;
  std::optional<uint64_t> subgroupId;
  std::optional<uint64_t> objectIdDelta;
  std::optional<uint8_t> publisherPriority;
  std::optional<uint64_t> propertiesLength;
  std::vector<MOQTProperty> properties;
  uint64_t objectPayloadLength{0};

  folly::dynamic toDynamic() const;
};

struct MOQTPaddingDatagramCreated {
  uint64_t paddingLength{0};

  folly::dynamic toDynamic() const;
};

struct MOQTPaddingDatagramParsed {
  uint64_t paddingLength{0};

  folly::dynamic toDynamic() const;
};

} // namespace moxygen

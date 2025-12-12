/*
 *  Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 *  This source code is licensed under the MIT license found in the LICENSE
 *  file in the root directory of this source tree.
 *
 */

#pragma once

#include <cstdint>
#include <string>
#include <variant>
#include <vector>
#include "folly/Optional.h"
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

// Data Type Structs

enum MOQTStreamType { CONTROL, SUBGROUP_HEADER, FETCH_HEADER };

enum Owner { LOCAL, REMOTE };

struct MOQTExtensionHeader {
  uint64_t headerType{0};
  folly::Optional<uint64_t> headerValue;
  folly::Optional<uint64_t> headerLength;
  std::unique_ptr<folly::IOBuf> payload;

  folly::dynamic toDynamic() const;
};

// Parameter Types (Section 5.3 of the mLog spec)

struct MOQTAuthorizationTokenParameter {
  std::string name = "authorization_token";
  uint64_t aliasType{0};
  folly::Optional<uint64_t> tokenAlias;
  folly::Optional<uint64_t> tokenType;
  std::unique_ptr<folly::IOBuf> tokenValue;

  folly::dynamic toDynamic() const;
};

struct MOQTDeliveryTimeoutParameter {
  std::string name = "delivery_timeout";
  uint64_t value{0};

  folly::dynamic toDynamic() const;
};

struct MOQTMaxCacheDurationParameter {
  std::string name = "max_cache_duration";
  uint64_t value{0};

  folly::dynamic toDynamic() const;
};

struct MOQTUnknownParameter {
  std::string name = "unknown";
  uint64_t nameBytes{0};
  folly::Optional<uint64_t> length;
  folly::Optional<uint64_t> value;
  std::unique_ptr<folly::IOBuf> valueBytes;

  folly::dynamic toDynamic() const;
};

using MOQTParameter = std::variant<
    MOQTAuthorizationTokenParameter,
    MOQTDeliveryTimeoutParameter,
    MOQTMaxCacheDurationParameter,
    MOQTUnknownParameter>;

// Helper to convert variant to dynamic
folly::dynamic parameterToDynamic(const MOQTParameter& param);

// Setup Parameter Types (Section 5.2 of the mLog spec)
enum MOQTAliasType { DELETE, REGISTER, USE_ALIAS, USE_VALUE };

struct MOQTAuthoritySetupParameter {
  std::string name = "authority";
  std::string value;

  folly::dynamic toDynamic() const;
};

struct MOQTAuthorizationTokenSetupParameter {
  std::string name = "authorization_token";
  MOQTAliasType aliasType;
  folly::Optional<uint64_t> tokenAlias;
  folly::Optional<uint64_t> tokenType;
  std::unique_ptr<folly::IOBuf> tokenValue;

  folly::dynamic toDynamic() const;
};

struct MOQTPathSetupParameter {
  std::string name = "path";
  std::string value;

  folly::dynamic toDynamic() const;
};

struct MOQTMaxRequestIdSetupParameter {
  std::string name = "max_request_id";
  uint64_t value{0};

  folly::dynamic toDynamic() const;
};

struct MOQTMaxAuthTokenCacheSizeSetupParameter {
  std::string name = "max_auth_token_cache_size";
  uint64_t value{0};

  folly::dynamic toDynamic() const;
};

struct MOQTImplementationSetupParameter {
  std::string name = "implementation";
  std::string value;

  folly::dynamic toDynamic() const;
};

struct MOQTUnknownSetupParameter {
  std::string name = "unknown";
  uint64_t nameBytes{0};
  folly::Optional<uint64_t> length;
  folly::Optional<uint64_t> value;
  std::unique_ptr<folly::IOBuf> valueBytes;

  folly::dynamic toDynamic() const;
};

using MOQTSetupParameter = std::variant<
    MOQTAuthoritySetupParameter,
    MOQTAuthorizationTokenSetupParameter,
    MOQTPathSetupParameter,
    MOQTMaxRequestIdSetupParameter,
    MOQTMaxAuthTokenCacheSizeSetupParameter,
    MOQTImplementationSetupParameter,
    MOQTUnknownSetupParameter>;

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
  std::vector<std::string> parseTrackNamespace(
      const std::vector<MOQTByteString>& trackNamespace) const;
  std::string parseTrackName(const MOQTByteString& trackName) const;
};

class MOQTClientSetupMessage : public MOQTBaseControlMessage {
 public:
  MOQTClientSetupMessage() {
    type = "client_setup";
  }
  folly::dynamic toDynamic() const override;
  uint64_t numberOfSupportedVersions{0};
  std::vector<uint64_t> supportedVersions;
  uint64_t numberOfParameters{0};
  std::vector<MOQTSetupParameter> setupParameters;
};

class MOQTServerSetupMessage : public MOQTBaseControlMessage {
 public:
  MOQTServerSetupMessage() {
    type = "server_setup";
  }
  folly::dynamic toDynamic() const override;
  uint64_t selectedVersion{0};
  uint64_t numberOfParameters{0};
  std::vector<MOQTSetupParameter> setupParameters;
};

class MOQTGoaway : public MOQTBaseControlMessage {
 public:
  MOQTGoaway() {
    type = "goaway";
  }
  folly::dynamic toDynamic() const override;
  folly::Optional<uint64_t> length;
  std::unique_ptr<folly::IOBuf> newSessionUri;
};

struct MOQTLocation {
  uint64_t group{0};
  uint64_t object{0};

  folly::dynamic toDynamic() const;
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
  folly::Optional<MOQTLocation> startLocation;
  folly::Optional<uint64_t> endGroup;
  uint64_t numberOfParameters{};
  std::vector<MOQTParameter> subscribeParameters;
};

class MOQTSubscribeUpdate : public MOQTBaseControlMessage {
 public:
  MOQTSubscribeUpdate() {
    type = "subscribe_update";
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
  uint8_t subscriberPriority{};
  uint8_t groupOrder{};
  std::string fetchType;
  folly::Optional<MOQTStandaloneFetch> standaloneFetch;
  folly::Optional<MOQTJoiningFetch> joiningFetch;
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

class MOQTAnnounceOk : public MOQTBaseControlMessage {
 public:
  MOQTAnnounceOk() {
    type = "announce_ok";
  }
  folly::dynamic toDynamic() const override;
  uint64_t requestId{0};
};

class MOQTAnnounceError : public MOQTBaseControlMessage {
 public:
  MOQTAnnounceError() {
    type = "announce_error";
  }
  folly::dynamic toDynamic() const override;
  uint64_t requestId{0};
  uint64_t errorCode{};
  folly::Optional<std::string> reason;
  folly::Optional<std::string> reasonBytes;
};

class MOQTAnnounceCancel : public MOQTBaseControlMessage {
 public:
  MOQTAnnounceCancel() {
    type = "announce_cancel";
  }
  folly::dynamic toDynamic() const override;
  std::vector<MOQTByteString> trackNamespace;
  uint64_t errorCode{};
  folly::Optional<std::string> reason;
  folly::Optional<std::string> reasonBytes;
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
  folly::Optional<MOQTLocation> startLocation;
  folly::Optional<uint64_t> endGroup;
  uint64_t numberOfParameters{};
  std::vector<MOQTParameter> parameters;
};

class MOQTSubscribeAnnounces : public MOQTBaseControlMessage {
 public:
  MOQTSubscribeAnnounces() {
    type = "subscribe_announces";
  }
  folly::dynamic toDynamic() const override;
  uint64_t requestId{0};
  std::vector<MOQTByteString> trackNamespacePrefix;
  uint64_t numberOfParameters{};
  std::vector<MOQTParameter> parameters;
};

class MOQTUnsubscribeAnnounces : public MOQTBaseControlMessage {
 public:
  MOQTUnsubscribeAnnounces() {
    type = "unsubscribe_announces";
  }
  folly::dynamic toDynamic() const override;
  // Keeping both to maintain compatibility between v15 and v15-
  folly::Optional<uint64_t> requestID;
  std::vector<MOQTByteString> trackNamespace;
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
  folly::Optional<MOQTLocation> largestLocation;
  uint64_t numberOfParameters{};
  std::vector<MOQTParameter> parameters;
};

class MOQTSubscribeError : public MOQTBaseControlMessage {
 public:
  MOQTSubscribeError() {
    type = "subscribe_error";
  }
  folly::dynamic toDynamic() const override;
  uint64_t requestId{0};
  uint64_t errorCode{};
  folly::Optional<std::string> reason;
  folly::Optional<std::string> reasonBytes;
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
};

class MOQTFetchError : public MOQTBaseControlMessage {
 public:
  MOQTFetchError() {
    type = "fetch_error";
  }
  folly::dynamic toDynamic() const override;
  uint64_t requestId{0};
  uint64_t errorCode{};
  folly::Optional<std::string> reason;
  folly::Optional<std::string> reasonBytes;
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
  folly::Optional<std::string> reason;
  folly::Optional<std::string> reasonBytes;
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

class MOQTAnnounce : public MOQTBaseControlMessage {
 public:
  MOQTAnnounce() {
    type = "announce";
  }
  folly::dynamic toDynamic() const override;
  uint64_t requestId{0};
  std::vector<MOQTByteString> trackNamespace;
  uint64_t numberOfParameters{};
  std::vector<MOQTParameter> parameters;
};

class MOQTUnannounce : public MOQTBaseControlMessage {
 public:
  MOQTUnannounce() {
    type = "unannounce";
  }
  folly::dynamic toDynamic() const override;
  std::vector<MOQTByteString> trackNamespace;
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
  folly::Optional<MOQTLocation> largestLocation;
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
  folly::Optional<std::string> reason;
  folly::Optional<std::string> reasonBytes;
};

class MOQTSubscribeAnnouncesOk : public MOQTBaseControlMessage {
 public:
  MOQTSubscribeAnnouncesOk() {
    type = "subscribe_announces_ok";
  }
  folly::dynamic toDynamic() const override;
  uint64_t requestId{0};
};

class MOQTSubscribeAnnouncesError : public MOQTBaseControlMessage {
 public:
  MOQTSubscribeAnnouncesError() {
    type = "subscribe_announces_error";
  }
  folly::dynamic toDynamic() const override;
  uint64_t requestId{0};
  uint64_t errorCode{};
  folly::Optional<std::string> reason;
  folly::Optional<std::string> reasonBytes;
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
  folly::Optional<MOQTLocation> largest;
  uint8_t forward{};
  uint64_t numberOfParameters{};
  std::vector<MOQTParameter> parameters;
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
  folly::Optional<MOQTLocation> start;
  folly::Optional<uint64_t> endGroup;
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
  folly::Optional<std::string> reason;
  folly::Optional<std::string> reasonBytes;
};

// MOQTEvents Structs
struct MOQTControlMessageCreated {
  uint64_t streamId{0};
  folly::Optional<uint64_t> length;
  std::unique_ptr<MOQTBaseControlMessage> message;
  std::unique_ptr<folly::IOBuf> raw;

  folly::dynamic toDynamic() const;
};

struct MOQTControlMessageParsed {
  uint64_t streamId{0};
  folly::Optional<uint64_t> length;
  std::unique_ptr<MOQTBaseControlMessage> message;
  std::unique_ptr<folly::IOBuf> raw;

  folly::dynamic toDynamic() const;
};

struct MOQTStreamTypeSet {
  folly::Optional<Owner> owner;
  uint64_t streamId{0};
  MOQTStreamType streamType;

  folly::dynamic toDynamic() const;
};

struct MOQTObjectDatagramCreated {
  uint64_t trackAlias{0};
  uint64_t groupId{0};
  folly::Optional<uint64_t> objectId;
  uint8_t publisherPriority{0};
  folly::Optional<uint64_t> extensionHeadersLength;
  std::vector<MOQTExtensionHeader> extensionHeaders;
  folly::Optional<uint64_t> objectStatus;
  std::unique_ptr<folly::IOBuf> objectPayload;
  bool endOfGroup{false};

  folly::dynamic toDynamic() const;
};

struct MOQTObjectDatagramParsed {
  uint64_t trackAlias{0};
  uint64_t groupId{0};
  folly::Optional<uint64_t> objectId;
  uint8_t publisherPriority{0};
  folly::Optional<uint64_t> extensionHeadersLength;
  std::vector<MOQTExtensionHeader> extensionHeaders;
  folly::Optional<uint64_t> objectStatus;
  std::unique_ptr<folly::IOBuf> objectPayload;
  bool endOfGroup{false};

  folly::dynamic toDynamic() const;
};

struct MOQTSubgroupHeaderCreated {
  uint64_t streamId{0};
  uint64_t trackAlias{0};
  uint64_t groupId{0};
  folly::Optional<uint64_t> subgroupId;
  uint8_t publisherPriority{0};
  bool containsEndOfGroup{false};
  bool extensionsPresent{false};

  folly::dynamic toDynamic() const;
};

struct MOQTSubgroupHeaderParsed {
  uint64_t streamId{0};
  uint64_t trackAlias{0};
  uint64_t groupId{0};
  folly::Optional<uint64_t> subgroupId;
  uint8_t publisherPriority{0};
  bool containsEndOfGroup{false};
  bool extensionsPresent{false};

  folly::dynamic toDynamic() const;
};

struct MOQTSubgroupObjectCreated {
  uint64_t streamId{0};
  folly::Optional<uint64_t> groupId;
  folly::Optional<uint64_t> subgroupId;
  uint64_t objectId{0};
  uint64_t extensionHeadersLength{0};
  std::vector<MOQTExtensionHeader> extensionHeaders;
  uint64_t objectPayloadLength{0};
  folly::Optional<uint64_t> objectStatus;
  std::unique_ptr<folly::IOBuf> objectPayload;

  folly::dynamic toDynamic() const;
};

struct MOQTSubgroupObjectParsed {
  uint64_t streamId{0};
  folly::Optional<uint64_t> groupId;
  folly::Optional<uint64_t> subgroupId;
  uint64_t objectId{0};
  uint64_t extensionHeadersLength{0};
  std::vector<MOQTExtensionHeader> extensionHeaders;
  uint64_t objectPayloadLength{0};
  folly::Optional<uint64_t> objectStatus;
  std::unique_ptr<folly::IOBuf> objectPayload;

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
  uint64_t groupId{};
  uint64_t subgroupId{};
  uint64_t objectId{0};
  uint8_t publisherPriority{0};
  uint64_t extensionHeadersLength{0};
  std::vector<MOQTExtensionHeader> extensionHeaders;
  uint64_t objectPayloadLength{0};
  folly::Optional<uint64_t> objectStatus;
  std::unique_ptr<folly::IOBuf> objectPayload;

  folly::dynamic toDynamic() const;
};

struct MOQTFetchObjectParsed {
  uint64_t streamId{0};
  uint64_t groupId{};
  uint64_t subgroupId{};
  uint64_t objectId{0};
  uint8_t publisherPriority{0};
  uint64_t extensionHeadersLength{0};
  std::vector<MOQTExtensionHeader> extensionHeaders;
  uint64_t objectPayloadLength{0};
  folly::Optional<uint64_t> objectStatus;
  std::unique_ptr<folly::IOBuf> objectPayload;

  folly::dynamic toDynamic() const;
};

} // namespace moxygen

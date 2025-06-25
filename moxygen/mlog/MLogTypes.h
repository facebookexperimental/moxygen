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

// No RawInfo Value Type For Now
enum MOQTParameterType { STRING, INT, UNKNOWN };

class MOQTParameter {
 public:
  MOQTParameter() {}
  MOQTParameter(const MOQTParameter& other) = default;
  MOQTParameter& operator=(const MOQTParameter& other) = default;
  MOQTParameter(MOQTParameter&& other) noexcept = default;
  MOQTParameter& operator=(MOQTParameter&& other) noexcept = default;
  MOQTParameterType type = MOQTParameterType::UNKNOWN;
  std::string name;
  std::string stringValue;
  uint64_t intValue{};
  ~MOQTParameter() {}

  folly::dynamic toDynamic() const;
};

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
  std::vector<MOQTParameter> setupParameters;
};

class MOQTServerSetupMessage : public MOQTBaseControlMessage {
 public:
  MOQTServerSetupMessage() {
    type = "server_setup";
  }
  folly::dynamic toDynamic() const override;
  uint64_t selectedVersion{0};
  uint64_t numberOfParameters{0};
  std::vector<MOQTParameter> setupParameters;
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

class MOQTSubscribe : public MOQTBaseControlMessage {
 public:
  MOQTSubscribe() {
    type = "subscribe";
  }
  folly::dynamic toDynamic() const override;
  uint64_t subscribeId{0};
  uint64_t trackAlias{0};
  std::vector<MOQTByteString> trackNamespace;
  MOQTByteString trackName;
  uint8_t subscriberPriority{};
  uint8_t groupOrder{};
  uint64_t filterType{};
  folly::Optional<uint64_t> startGroup;
  folly::Optional<uint64_t> startObject;
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
  uint64_t subscribeId{0};
  uint64_t startGroup{};
  uint64_t startObject{};
  uint64_t endGroup{};
  uint8_t subscriberPriority{};
  uint64_t numberOfParameters{};
  std::vector<MOQTParameter> subscribeParameters;
};

class MOQTUnsubscribe : public MOQTBaseControlMessage {
 public:
  MOQTUnsubscribe() {
    type = "unsubscribe";
  }
  folly::dynamic toDynamic() const override;
  uint64_t subscribeId{0};
};

class MOQTFetch : public MOQTBaseControlMessage {
 public:
  MOQTFetch() {
    type = "fetch";
  }
  folly::dynamic toDynamic() const override;
  uint64_t subscribeId{0};
  uint8_t subscriberPriority{};
  uint8_t groupOrder{};
  uint64_t fetchType{};
  std::vector<MOQTByteString> trackNamespace;
  folly::Optional<MOQTByteString> trackName;
  folly::Optional<uint64_t> startGroup;
  folly::Optional<uint64_t> startObject;
  folly::Optional<uint64_t> endGroup;
  folly::Optional<uint64_t> endObject;
  folly::Optional<uint64_t> joiningSubscribeId;
  folly::Optional<uint64_t> precedingGroupOffset;
  uint64_t numberOfParameters{};
  std::vector<MOQTParameter> parameters;
};

class MOQTFetchCancel : public MOQTBaseControlMessage {
 public:
  MOQTFetchCancel() {
    type = "fetch_cancel";
  }
  folly::dynamic toDynamic() const override;
  uint64_t subscribeId{0};
};

class MOQTAnnounceOk : public MOQTBaseControlMessage {
 public:
  MOQTAnnounceOk() {
    type = "announce_ok";
  }
  folly::dynamic toDynamic() const override;
  std::vector<MOQTByteString> trackNamespace;
};

class MOQTAnnounceError : public MOQTBaseControlMessage {
 public:
  MOQTAnnounceError() {
    type = "announce_error";
  }
  folly::dynamic toDynamic() const override;
  std::vector<MOQTByteString> trackNamespace;
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

class MOQTTrackStatusRequest : public MOQTBaseControlMessage {
 public:
  MOQTTrackStatusRequest() {
    type = "track_status_request";
  }
  folly::dynamic toDynamic() const override;
  std::vector<MOQTByteString> trackNamespace;
  MOQTByteString trackName;
};

class MOQTSubscribeAnnounces : public MOQTBaseControlMessage {
 public:
  MOQTSubscribeAnnounces() {
    type = "subscribe_announces";
  }
  folly::dynamic toDynamic() const override;
  std::vector<MOQTByteString> trackNamespace;
  uint64_t numberOfParameters{};
  std::vector<MOQTParameter> parameters;
};

class MOQTUnsubscribeAnnounces : public MOQTBaseControlMessage {
 public:
  MOQTUnsubscribeAnnounces() {
    type = "unsubscribe_announces";
  }
  folly::dynamic toDynamic() const override;
  std::vector<MOQTByteString> trackNamespace;
};

class MOQTSubscribeOk : public MOQTBaseControlMessage {
 public:
  MOQTSubscribeOk() {
    type = "subscribe_ok";
  }
  folly::dynamic toDynamic() const override;
  uint64_t subscribeId{0};
  uint64_t expires{};
  uint8_t groupOrder{};
  uint8_t contentExists{};
  folly::Optional<uint64_t> largestGroupId;
  folly::Optional<uint64_t> largestObjectId;
  uint64_t numberOfParameters{};
  std::vector<MOQTParameter> subscribeParameters;
};

class MOQTSubscribeError : public MOQTBaseControlMessage {
 public:
  MOQTSubscribeError() {
    type = "subscribe_error";
  }
  folly::dynamic toDynamic() const override;
  uint64_t subscribeId{0};
  uint64_t errorCode{};
  folly::Optional<std::string> reason;
  folly::Optional<std::string> reasonBytes;
  folly::Optional<uint64_t> trackAlias{};
};

class MOQTFetchOk : public MOQTBaseControlMessage {
 public:
  MOQTFetchOk() {
    type = "fetch_ok";
  }
  folly::dynamic toDynamic() const override;
  uint64_t subscribeId{0};
  uint8_t groupOrder{};
  uint8_t endOfTrack{};
  uint64_t largestGroupId{};
  uint64_t largestObjectId{};
  uint64_t numberOfParameters{};
  std::vector<MOQTParameter> subscribeParameters;
};

class MOQTFetchError : public MOQTBaseControlMessage {
 public:
  MOQTFetchError() {
    type = "fetch_error";
  }
  folly::dynamic toDynamic() const override;
  uint64_t subscribeId{0};
  uint64_t errorCode{};
  folly::Optional<std::string> reason;
  folly::Optional<std::string> reasonBytes;
};

class MOQTSubscribeDone : public MOQTBaseControlMessage {
 public:
  MOQTSubscribeDone() {
    type = "subscribe_done";
  }
  folly::dynamic toDynamic() const override;
  uint64_t subscribeId{0};
  uint64_t statusCode{};
  uint64_t streamCount{};
  folly::Optional<std::string> reason;
  folly::Optional<std::string> reasonBytes;
};

class MOQTMaxSubscribeId : public MOQTBaseControlMessage {
 public:
  MOQTMaxSubscribeId() {
    type = "max_subscribe_id";
  }
  folly::dynamic toDynamic() const override;
  uint64_t subscribeId{0};
};

class MOQTSubscribesBlocked : public MOQTBaseControlMessage {
 public:
  MOQTSubscribesBlocked() {
    type = "subscribes_blocked";
  }
  folly::dynamic toDynamic() const override;
  uint64_t maximumSubscribeId{0};
};

class MOQTAnnounce : public MOQTBaseControlMessage {
 public:
  MOQTAnnounce() {
    type = "announce";
  }
  folly::dynamic toDynamic() const override;
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

class MOQTTrackStatus : public MOQTBaseControlMessage {
 public:
  MOQTTrackStatus() {
    type = "track_status";
  }
  folly::dynamic toDynamic() const override;
  std::vector<MOQTByteString> trackNamespace;
  MOQTByteString trackName;
  uint64_t statusCode{};
  folly::Optional<uint64_t> lastGroupId;
  folly::Optional<uint64_t> lastObjectId;
};

class MOQTSubscribeAnnouncesOk : public MOQTBaseControlMessage {
 public:
  MOQTSubscribeAnnouncesOk() {
    type = "subscribe_announces_ok";
  }
  folly::dynamic toDynamic() const override;
  std::vector<MOQTByteString> trackNamespace;
};

class MOQTSubscribeAnnouncesError : public MOQTBaseControlMessage {
 public:
  MOQTSubscribeAnnouncesError() {
    type = "subscribe_announces_error";
  }
  folly::dynamic toDynamic() const override;
  std::vector<MOQTByteString> trackNamespace;
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
  uint64_t objectId{0};
  uint8_t publisherPriority{0};
  uint64_t extensionHeadersLength{0};
  std::vector<MOQTExtensionHeader> extensionHeaders;
  std::unique_ptr<folly::IOBuf> objectPayload;

  folly::dynamic toDynamic() const;
};

struct MOQTObjectDatagramParsed {
  uint64_t trackAlias{0};
  uint64_t groupId{0};
  uint64_t objectId{0};
  uint8_t publisherPriority{0};
  uint64_t extensionHeadersLength{0};
  std::vector<MOQTExtensionHeader> extensionHeaders;
  std::unique_ptr<folly::IOBuf> objectPayload;

  folly::dynamic toDynamic() const;
};

struct MOQTObjectDatagramStatusCreated {
  uint64_t trackAlias{0};
  uint64_t groupId{0};
  uint64_t objectId{0};
  uint8_t publisherPriority{0};
  uint64_t extensionHeadersLength{0};
  std::vector<MOQTExtensionHeader> extensionHeaders;
  uint64_t objectStatus{0};

  folly::dynamic toDynamic() const;
};

struct MOQTObjectDatagramStatusParsed {
  uint64_t trackAlias{0};
  uint64_t groupId{0};
  uint64_t objectId{0};
  uint8_t publisherPriority{0};
  uint64_t extensionHeadersLength{0};
  std::vector<MOQTExtensionHeader> extensionHeaders;
  uint64_t objectStatus{0};
};

struct MOQTSubgroupHeaderCreated {
  uint64_t streamId{0};
  uint64_t trackAlias{0};
  uint64_t groupId{0};
  uint64_t subgroupId{0};
  uint8_t publisherPriority{0};
};

struct MOQTSubgroupHeaderParsed {
  uint64_t streamId{0};
  uint64_t trackAlias{0};
  uint64_t groupId{0};
  uint64_t subgroupId{0};
  uint8_t publisherPriority{0};
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
};

struct MOQTFetchHeaderCreated {
  uint64_t streamId{0};
  uint64_t subscribeId{0};
};

struct MOQTFetchHeaderParsed {
  uint64_t streamId{0};
  uint64_t subscribeId{0};
};

struct MOQTFetchObjectCreated {
  uint64_t streamId{0};
  uint64_t groupId;
  uint64_t subgroupId;
  uint64_t objectId{0};
  uint8_t publisherPriority{0};
  uint64_t extensionHeadersLength{0};
  std::vector<MOQTExtensionHeader> extensionHeaders;
  uint64_t objectPayloadLength{0};
  folly::Optional<uint64_t> objectStatus;
  std::unique_ptr<folly::IOBuf> objectPayload;
};

struct MOQTFetchObjectParsed {
  uint64_t streamId{0};
  uint64_t groupId;
  uint64_t subgroupId;
  uint64_t objectId{0};
  uint8_t publisherPriority{0};
  uint64_t extensionHeadersLength{0};
  std::vector<MOQTExtensionHeader> extensionHeaders;
  uint64_t objectPayloadLength{0};
  folly::Optional<uint64_t> objectStatus;
  std::unique_ptr<folly::IOBuf> objectPayload;
};

} // namespace moxygen

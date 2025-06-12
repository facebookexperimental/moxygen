// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#pragma once

#include <folly/Optional.h>
#include <folly/io/IOBuf.h>
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <vector>

namespace moxygen {

// Data Type Structs

enum MOQTStreamType { CONTROL, SUBGROUP_HEADER, FETCH_HEADER };

enum Owner { LOCAL, REMOTE };

struct MOQTExtensionHeader {
  uint64_t headerType{0};
  folly::Optional<uint64_t> headerValue;
  folly::Optional<uint64_t> headerLength;
  std::unique_ptr<folly::IOBuf> payload;
};

struct MOQTPathSetupParameter {
  std::string name = std::string("path");
  std::string value = std::string("");
};

struct MOQTMaxSubscribeIdSetupParameter {
  std::string name = std::string("max_subscribe_id");
  uint64_t value{0};
};

struct MOQTUnknownSetupParameter {
  std::string name = std::string("unknown");
  uint64_t nameBytes{0};
  folly::Optional<uint64_t> length;
  folly::Optional<uint64_t> value;
  std::unique_ptr<folly::IOBuf> valueBytes;
};

union MOQTBaseSetupParameters {
  MOQTPathSetupParameter path;
  MOQTMaxSubscribeIdSetupParameter maxSubscribeId;
  MOQTUnknownSetupParameter unknown;
};

struct MOQTSetupParameter {
  // Variant so Types being mapped to can be extended
  std::map<std::string, std::variant<MOQTBaseSetupParameters>> map;
};

struct MOQTAuthorizationInfoParameter {
  std::string name = std::string("authorization_info");
  folly::Optional<std::string> value;
};

struct MOQTDeliveryTimeoutParameter {
  std::string name = std::string("delivery_timeout");
  uint64_t value{0};
};

struct MOQTMaxCacheDurationParameter {
  std::string name = std::string("max_cache_duration");
  uint64_t value{0};
};

struct MOQTUnknownParameter {
  std::string name = std::string("unknown");
  uint64_t nameBytes{0};
  folly::Optional<uint64_t> length;
  folly::Optional<uint64_t> value;
  std::unique_ptr<folly::IOBuf> valueBytes;
};

union MOQTBaseParameters {
  MOQTAuthorizationInfoParameter authorizationInfo;
  MOQTDeliveryTimeoutParameter deliveryTimeout;
  MOQTMaxCacheDurationParameter maxCacheDuration;
  MOQTUnknownParameter unknown;
};

struct MOQTParameter {
  // Variant so Types being mapped to can be extended
  std::map<std::string, std::variant<MOQTBaseParameters>> map;
};

union MOQTByteString {
  std::string value;
  std::unique_ptr<folly::IOBuf> valueBytes;
};

struct MOQTClientSetupMessage {
  std::string type = std::string("client_setup");
  uint64_t numberOfSupportedVersions{0};
  std::vector<uint64_t> supportedVersions;
  uint64_t numberOfParameters{0};
  std::vector<MOQTSetupParameter> setupParameters;
};

struct MOQTServerSetupMessage {
  std::string type = std::string("server_setup");
  uint64_t selectedVersion{0};
  uint64_t numberOfParameters{0};
  std::vector<MOQTSetupParameter> setupParameters;
};

struct MOQTGoaway {
  std::string type = std::string("goaway");
  folly::Optional<uint64_t> length;
  std::unique_ptr<folly::IOBuf> newSessionUri;
};

struct MOQTSubscribe {
  std::string type = std::string("subscribe");
  uint64_t subscribeId{0};
  uint64_t trackAlias{0};
  std::vector<MOQTByteString> trackNamespace;
  MOQTByteString trackName;
  uint8_t subscriberPriority;
  uint8_t groupOrder;
  uint64_t filterType;
  folly::Optional<uint64_t> startGroup;
  folly::Optional<uint64_t> startObject;
  folly::Optional<uint64_t> endGroup;
  uint64_t numberOfParameters;
  std::vector<MOQTParameter> subscribeParameters;
};

struct MOQTSubscribeUpdate {
  std::string type = std::string("subscribe_update");
  uint64_t subscribeId{0};
  uint64_t startGroup;
  uint64_t startObject;
  uint64_t endGroup;
  uint8_t subscriberPriority;
  uint64_t numberOfParameters;
  std::vector<MOQTParameter> subscribeParameters;
};

struct MOQTUnsubscribe {
  std::string type = std::string("unsubscribe");
  uint64_t subscribeId{0};
};

struct MOQTFetch {
  std::string type = std::string("fetch");
  uint64_t subscribeId{0};
  uint8_t subscriberPriority;
  uint8_t groupOrder;
  uint64_t fetchType;

  std::vector<MOQTByteString> trackNamespace;
  folly::Optional<MOQTByteString> trackName;
  folly::Optional<uint64_t> startGroup;
  folly::Optional<uint64_t> startObject;
  folly::Optional<uint64_t> endGroup;
  folly::Optional<uint64_t> endObject;
  folly::Optional<uint64_t> joiningSubscribeId;
  folly::Optional<uint64_t> precedingGroupOffset;

  uint64_t numberOfParameters;
  std::vector<MOQTParameter> parameters;
};

struct MOQTFetchCancel {
  std::string type = std::string("fetch_cancel");
  uint64_t subscribeId{0};
};

struct MOQTAnnounceOk {
  std::string type = std::string("announce_ok");
  std::vector<MOQTByteString> trackNamespace;
};

struct MOQTAnnounceError {
  std::string type = std::string("announce_error");
  std::vector<MOQTByteString> trackNamespace;
  uint64_t errorCode;
  folly::Optional<std::string> reason;
  folly::Optional<std::string> reasonBytes;
};

struct MOQTAnnounceCancel {
  std::string type = std::string("announce_cancel");
  std::vector<MOQTByteString> trackNamespace;
  uint64_t errorCode;
  folly::Optional<std::string> reason;
  folly::Optional<std::string> reasonBytes;
};

struct MOQTTrackStatusRequest {
  std::string type = std::string("track_status_request");
  std::vector<MOQTByteString> trackNamespace;
  MOQTByteString trackName;
};

struct MOQTSubscribeAnnounces {
  std::string type = std::string("subscribe_announces");
  std::vector<MOQTByteString> trackNamespace;
  uint64_t numberOfParameters;
  std::vector<MOQTParameter> parameters;
};

struct MOQTUnsubscribeAnnounces {
  std::string type = std::string("unsubscribe_announces");
  std::vector<MOQTByteString> trackNamespace;
};

struct MOQTSubscribeOk {
  std::string type = std::string("subscribe_ok");
  uint64_t subscribeId{0};
  uint64_t expires;
  uint8_t groupOrder;
  uint8_t contentExists;
  folly::Optional<uint64_t> largestGroupId;
  folly::Optional<uint64_t> largestObjectId;
  uint64_t numberOfParameters;
  std::vector<MOQTParameter> subscribeParameters;
};

struct MOQTSubscribeError {
  std::string type = std::string("subscribe_error");
  uint64_t subscribeId{0};
  uint64_t errorCode;
  folly::Optional<std::string> reason;
  folly::Optional<std::string> reasonBytes;
  uint64_t trackAlias;
};

struct MOQTFetchOk {
  std::string type = std::string("fetch_ok");
  uint64_t subscribeId{0};
  uint8_t groupOrder;
  uint8_t endOfTrack;
  uint64_t largestGroupId;
  uint64_t largestObjectId;
  uint64_t numberOfParameters;
  std::vector<MOQTParameter> subscribeParameters;
};

struct MOQTFetchError {
  std::string type = std::string("fetch_error");
  uint64_t subscribeId{0};
  uint64_t errorCode;
  folly::Optional<std::string> reason;
  folly::Optional<std::string> reasonBytes;
};

struct MOQTSubscribeDone {
  std::string type = std::string("subscribe_done");
  uint64_t subscribeId{0};
  uint64_t statusCode;
  uint64_t streamCount;
  folly::Optional<std::string> reason;
  folly::Optional<std::string> reasonBytes;
};

struct MOQTMaxSubscribeId {
  std::string type = std::string("max_subscribe_id");
  uint64_t subscribeId{0};
};

struct MOQTSubscribesBlocked {
  std::string type = std::string("subscribes_blocked");
  uint64_t maximumSubscribeId{0};
};

struct MOQTAnnounce {
  std::string type = std::string("announce");
  std::vector<MOQTByteString> trackNamespace;
  uint64_t numberOfParameters;
  std::vector<MOQTParameter> parameters;
};

struct MOQTUnannounce {
  std::string type = std::string("unannounce");
  std::vector<MOQTByteString> trackNamespace;
};

struct MOQTTrackStatus {
  std::string type = std::string("track_status");
  std::vector<MOQTByteString> trackNamespace;
  MOQTByteString trackName;
  uint64_t statusCode;
  uint64_t lastGroupId;
  uint64_t lastObjectId;
};

struct MOQTSubscribeAnnouncesOk {
  std::string type = std::string("subscribe_announces_ok");
  std::vector<MOQTByteString> trackNamespace;
};

struct MOQTSubscribeAnnouncesError {
  std::string type = std::string("subscribe_announces_error");
  std::vector<MOQTByteString> trackNamespace;
  uint64_t errorCode;
  folly::Optional<std::string> reason;
  folly::Optional<std::string> reasonBytes;
};

union MOQTBaseControlMessages {
  MOQTClientSetupMessage clientSetup;
  MOQTServerSetupMessage serverSetup;
  MOQTGoaway goaway;
  MOQTSubscribe subscribe;
  MOQTSubscribeUpdate subscribeUpdate;
  MOQTUnsubscribe unsubscribe;
  MOQTFetch fetch;
  MOQTFetchCancel fetchCancel;
  MOQTAnnounceOk announceOk;
  MOQTAnnounceError announceError;
  MOQTAnnounceCancel announceCancel;
  MOQTTrackStatusRequest trackStatusRequest;
  MOQTSubscribeAnnounces subscribeAnnounces;
  MOQTUnsubscribeAnnounces unsubscribeAnnounces;
  MOQTSubscribeOk subscribeOk;
  MOQTSubscribeError subscribeError;
  MOQTFetchOk fetchOk;
  MOQTFetchError fetchError;
  MOQTSubscribeDone subscribeDone;
  MOQTMaxSubscribeId maxSubscribeId;
  MOQTSubscribesBlocked subscribesBlocked;
  MOQTAnnounce announce;
  MOQTUnannounce unannounce;
  MOQTTrackStatus trackStatus;
  MOQTSubscribeAnnouncesOk subscribeAnnouncesOk;
  MOQTSubscribeAnnouncesError subscribeAnnouncesError;
};

struct MOQTControlMessage {
  // Variant so Types being mapped to can be extended
  std::map<std::string, std::variant<MOQTBaseControlMessages>> map;
};

// MOQTEvents Structs
struct MOQTControlMessageCreated {
  uint64_t streamId{0};
  folly::Optional<uint64_t> length;
  MOQTControlMessage message;
  std::unique_ptr<folly::IOBuf> raw;
};

struct MOQTControlMessageParsed {
  uint64_t streamId{0};
  folly::Optional<uint64_t> length;
  MOQTControlMessage message;
  std::unique_ptr<folly::IOBuf> raw;
};

struct MOQTStreamTypeSet {
  folly::Optional<Owner> owner;
  uint64_t streamId{0};
  MOQTStreamType streamType;
};

struct MOQTObjectDatagramCreated {
  uint64_t trackAlias{0};
  uint64_t groupId{0};
  uint64_t objectId{0};
  uint8_t publisherPriority{0};
  uint64_t extensionHeadersLength{0};
  std::vector<MOQTExtensionHeader> extensionHeaders;
  std::unique_ptr<folly::IOBuf> objectPayload;
};

struct MOQTObjectDatagramParsed {
  uint64_t trackAlias{0};
  uint64_t groupId{0};
  uint64_t objectId{0};
  uint8_t publisherPriority{0};
  uint64_t extensionHeadersLength{0};
  std::vector<MOQTExtensionHeader> extensionHeaders;
  std::unique_ptr<folly::IOBuf> objectPayload;
};

struct MOQTObjectDatagramStatusCreated {
  uint64_t trackAlias{0};
  uint64_t groupId{0};
  uint64_t objectId{0};
  uint8_t publisherPriority{0};
  uint64_t extensionHeadersLength{0};
  std::vector<MOQTExtensionHeader> extensionHeaders;
  uint64_t objectStatus{0};
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

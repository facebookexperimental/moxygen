// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#pragma once

#include <folly/Optional.h>
#include <folly/io/IOBuf.h>
#include <cstdint>
#include <map>
#include <memory>
#include <optional>
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

struct MOQTControlMessage {
  // Implement Once MOQTBaseControlStructs Are Written
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

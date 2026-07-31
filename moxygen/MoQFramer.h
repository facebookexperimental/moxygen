/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/io/Cursor.h>
#include <folly/io/IOBufQueue.h>
#include <folly/logging/xlog.h>
#include <moxygen/MoQTokenCache.h>
#include <moxygen/MoQTypes.h>
#include <moxygen/MoQVarint.h>
#include <moxygen/MoQVersions.h>

#include <quic/QuicException.h>
#include <quic/codec/QuicInteger.h>
#include <quic/folly_utils/Utils.h>

#include <variant>

namespace moxygen {

//////// Constants ////////
const size_t kMaxFrameHeaderSize = 32;

using WriteResult = folly::Expected<size_t, quic::TransportErrorCode>;

// Test-only free helpers that always operate on QUIC varints (drafts <17
// behavior). Production code should use MoQFrameWriter::writeVarint /
// MoQFrameParser::parseFixedString which dispatch on the negotiated version.
void writeVarint(
    folly::IOBufQueue& buf,
    uint64_t value,
    size_t& size,
    bool& error) noexcept;

folly::Expected<std::string, ErrorCode> parseFixedString(
    folly::io::Cursor& cursor,
    size_t& length);

inline StreamType getSubgroupStreamType(
    uint64_t version,
    SubgroupIDFormat format,
    bool includeExtensions,
    bool endOfGroup,
    bool priorityPresent = true,
    bool beginsWithFirstObject = false) {
  auto majorVersion = getDraftMajorVersion(version);
  return StreamType(
      folly::to_underlying(StreamType::SUBGROUP_HEADER_MASK) |
      (format == SubgroupIDFormat::Present ? SG_HAS_SUBGROUP_ID : 0) |
      (format == SubgroupIDFormat::FirstObject ? SG_SUBGROUP_VALUE : 0) |
      (includeExtensions ? SG_HAS_EXTENSIONS : 0) |
      (endOfGroup ? SG_HAS_END_OF_GROUP : 0) |
      (majorVersion >= 15 && !priorityPresent ? SG_PRIORITY_NOT_PRESENT : 0) |
      (majorVersion >= 18 && beginsWithFirstObject ? SG_FIRST_OBJECT : 0));
}

bool isValidSubgroupType(uint64_t version, uint64_t streamType);
bool isPaddingStreamType(uint64_t version, uint64_t streamType);
bool isPaddingDatagramType(uint64_t version, uint64_t datagramType);
folly::Expected<folly::Unit, ErrorCode> parsePaddingData(
    folly::io::Cursor& cursor,
    size_t& length) noexcept;

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
  options.beginsWithFirstObject =
      majorVersion >= 18 && (streamTypeInt & SG_FIRST_OBJECT);
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

class MoQFrameParser {
 public:
  template <typename T>
  struct ParseResultAndLength {
    T value;
    size_t bytesConsumed;
  };
  folly::Expected<Setup, ErrorCode> parseClientSetup(
      folly::io::Cursor& cursor,
      size_t length) noexcept;

  folly::Expected<Setup, ErrorCode> parseServerSetup(
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

  // Marker for End of Range results from FETCH parsing (MOQT spec)
  // Indicates a range of objects that either don't exist or have unknown status
  struct EndOfRangeMarker {
    uint64_t groupId;
    uint64_t objectId;
    bool isUnknownOrNonexistent; // true = 0x10C (unknown), false = 0x8C
                                 // (non-existent)
  };

  // Result from parsing FETCH objects - can be either a normal object or an
  // End of Range marker
  using FetchObjectParseResult = std::variant<ObjectHeader, EndOfRangeMarker>;

  folly::Expected<ParseResultAndLength<SubgroupHeaderResult>, ErrorCode>
  parseSubgroupHeader(
      folly::io::Cursor& cursor,
      size_t length,
      const SubgroupOptions& options) const noexcept;

  folly::Expected<ParseResultAndLength<FetchObjectParseResult>, ErrorCode>
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

  folly::Expected<RequestUpdate, ErrorCode> parseRequestUpdate(
      folly::io::Cursor& cursor,
      size_t length) const noexcept;

  folly::Expected<SubscribeOk, ErrorCode> parseSubscribeOk(
      folly::io::Cursor& cursor,
      size_t length) const noexcept;

  folly::Expected<Unsubscribe, ErrorCode> parseUnsubscribe(
      folly::io::Cursor& cursor,
      size_t length) const noexcept;

  folly::Expected<PublishDone, ErrorCode> parsePublishDone(
      folly::io::Cursor& cursor,
      size_t length) const noexcept;

  folly::Expected<PublishRequest, ErrorCode> parsePublish(
      folly::io::Cursor& cursor,
      size_t length) const noexcept;

  folly::Expected<PublishOk, ErrorCode> parsePublishOk(
      folly::io::Cursor& cursor,
      size_t length) const noexcept;

  folly::Expected<PublishNamespace, ErrorCode> parsePublishNamespace(
      folly::io::Cursor& cursor,
      size_t length) const noexcept;

  folly::Expected<PublishNamespaceOk, ErrorCode> parsePublishNamespaceOk(
      folly::io::Cursor& cursor,
      size_t length) const noexcept;

  folly::Expected<RequestOk, ErrorCode> parseRequestOk(
      folly::io::Cursor& cursor,
      size_t length,
      FrameType frameType) const noexcept;

  folly::Expected<PublishNamespaceDone, ErrorCode> parsePublishNamespaceDone(
      folly::io::Cursor& cursor,
      size_t length) const noexcept;

  folly::Expected<PublishNamespaceCancel, ErrorCode>
  parsePublishNamespaceCancel(folly::io::Cursor& cursor, size_t length)
      const noexcept;

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

  folly::Expected<SubscribeNamespace, ErrorCode> parseSubscribeNamespace(
      folly::io::Cursor& cursor,
      size_t length) const noexcept;

  // Draft 18+ only.
  folly::Expected<SubscribeTracks, ErrorCode> parseSubscribeTracks(
      folly::io::Cursor& cursor,
      size_t length) const noexcept;

  // Draft 18+ only.
  folly::Expected<PublishBlocked, ErrorCode> parsePublishBlocked(
      folly::io::Cursor& cursor,
      size_t length) const noexcept;

  folly::Expected<SubscribeNamespaceOk, ErrorCode> parseSubscribeNamespaceOk(
      folly::io::Cursor& cursor,
      size_t length) const noexcept;

  // Unified request error parsing function
  folly::Expected<RequestError, ErrorCode> parseRequestError(
      folly::io::Cursor& cursor,
      size_t length,
      FrameType frameType) const noexcept;

  folly::Expected<UnsubscribeNamespace, ErrorCode> parseUnsubscribeNamespace(
      folly::io::Cursor& cursor,
      size_t length) const noexcept;

  // v16+ messages for SUBSCRIBE_NAMESPACE response stream
  folly::Expected<Namespace, ErrorCode> parseNamespace(
      folly::io::Cursor& cursor,
      size_t length) const noexcept;

  folly::Expected<NamespaceDone, ErrorCode> parseNamespaceDone(
      folly::io::Cursor& cursor,
      size_t length) const noexcept;

  folly::Expected<folly::Unit, ErrorCode> parseExtensions(
      folly::io::Cursor& cursor,
      size_t& length,
      ObjectHeader& objectHeader) const noexcept;

  void initializeVersion(uint64_t versionIn) {
    XCHECK(!version_) << "Version already initialized";
    version_ = versionIn;
    useMoQVarint_ = getDraftMajorVersion(versionIn) >= 17;
  }

  std::optional<uint64_t> getVersion() const {
    return version_;
  }

  // Decode a TRACK_NAMESPACE_PREFIX parameter value (a Track Namespace tuple,
  // §2.4.1) into a TrackNamespace for the given negotiated version. Used by
  // consumers of REQUEST_UPDATE (e.g. the relay) that receive the prefix as a
  // generic parameter in the message's parameter list.
  static folly::Expected<TrackNamespace, ErrorCode>
  parseTrackNamespacePrefixParam(const std::string& value, uint64_t version);

  // Version-aware varint decode. Dispatches to QUIC varint on drafts <17 and
  // MoQ varint on drafts >=17. The dispatch flag is cached in
  // `initializeVersion` so this stays a single load + branch. The default cap
  // branches on the dispatch flag: MoQ varints can be 9 bytes, QUIC varints 8.
  quic::Optional<std::pair<uint64_t, size_t>> decodeVarint(
      folly::io::Cursor& cursor) const {
    if (useMoQVarint_) {
      return decodeMoQVarint(cursor, 9);
    }
    return quic::follyutils::decodeQuicInteger(cursor, sizeof(uint64_t));
  }

  quic::Optional<std::pair<uint64_t, size_t>> decodeVarint(
      folly::io::Cursor& cursor,
      uint64_t atMost) const {
    if (useMoQVarint_) {
      return decodeMoQVarint(cursor, atMost);
    }
    return quic::follyutils::decodeQuicInteger(cursor, atMost);
  }

  void setFetchGroupOrder(GroupOrder groupOrder) noexcept;

  void setTokenCacheMaxSize(size_t size) {
    tokenCache_->setMaxSize(size, /*evict=*/true);
  }

  // Use an external token cache instead of the internal one.
  // The caller must ensure the external cache outlives this parser.
  void setTokenCache(MoQTokenCache* cache) {
    tokenCache_ = cache;
  }

  // Test only
  void reset() {
    previousObjectID_ = std::nullopt;
    previousFetchGroup_ = std::nullopt;
    previousFetchSubgroup_ = std::nullopt;
    previousFetchPriority_ = std::nullopt;
    fetchGroupOrder_ = GroupOrder::OldestFirst;
  }

  std::optional<TrackFilter> extractTrackFilter(
      const std::vector<Parameter>& requestSpecificParams) const noexcept;

 private:
  // Legacy FETCH object parser (draft <= 14)
  folly::Expected<ObjectHeader, ErrorCode> parseFetchObjectHeaderLegacy(
      folly::io::Cursor& cursor,
      size_t& length,
      const ObjectHeader& headerTemplate) const noexcept;

  // Draft-15+ FETCH object parser with Serialization Flags
  folly::Expected<FetchObjectParseResult, ErrorCode> parseFetchObjectDraft15(
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

  folly::Expected<std::optional<AuthToken>, ErrorCode> parseToken(
      folly::io::Cursor& cursor,
      size_t length) const noexcept;

  folly::Expected<std::vector<std::string>, ErrorCode> parseNamespaceTuple(
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

  std::optional<SubscriptionFilter> extractSubscriptionFilter(
      const std::vector<Parameter>& requestSpecificParams) const noexcept;

  void handleRequestSpecificParams(
      SubscribeRequest& subscribeRequest,
      const std::vector<Parameter>& requestSpecificParams) const noexcept;

  void handleRequestSpecificParams(
      SubscribeOk& subscribeOk,
      const std::vector<Parameter>& requestSpecificParams) const noexcept;

  void handleRequestSpecificParams(
      RequestUpdate& requestUpdate,
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
      std::optional<bool>& forwardField,
      const std::vector<Parameter>& requestSpecificParams) const noexcept;

  // Version translation: convert track property params to extensions for < v16
  void convertTrackPropertyParamsToExtensions(
      const TrackRequestParameters& params,
      Extensions& extensions) const noexcept;

  // Promoted from free helpers so member-to-member calls can use the cached
  // useMoQVarint_/version_/tokenCache_ state without threading them through.
  enum class ParamsType { ClientSetup, ServerSetup, Request };

  folly::Expected<std::string, ErrorCode> parseFixedString(
      folly::io::Cursor& cursor,
      size_t& length) const noexcept;

  folly::Expected<AbsoluteLocation, ErrorCode> parseAbsoluteLocation(
      folly::io::Cursor& cursor,
      size_t& length) const noexcept;

  folly::Expected<SubscriptionFilter, ErrorCode> parseSubscriptionFilter(
      folly::io::Cursor& cursor,
      size_t& length) const noexcept;

  folly::Expected<std::optional<AuthToken>, ErrorCode> parseAuthToken(
      folly::io::Cursor& cursor,
      size_t length,
      ParamsType paramsType) const noexcept;

  folly::Expected<std::optional<Parameter>, ErrorCode> parseVariableParam(
      folly::io::Cursor& cursor,
      size_t& length,
      uint64_t version,
      uint64_t key,
      ParamsType paramsType) const noexcept;

  folly::Expected<std::optional<Parameter>, ErrorCode> parseIntParam(
      folly::io::Cursor& cursor,
      size_t& length,
      uint64_t version,
      uint64_t key,
      ParamsType paramsType) const noexcept;

  folly::Expected<std::optional<Parameter>, ErrorCode> parseV18ParamValue(
      folly::io::Cursor& cursor,
      size_t& length,
      uint64_t version,
      uint64_t key,
      ParamsType paramsType) const noexcept;

  // numParams == std::nullopt means "consume options until the declared
  // message length is exhausted" (draft-17+ SETUP has no Number-of-Options
  // field on the wire).
  folly::Expected<folly::Unit, ErrorCode> parseParams(
      folly::io::Cursor& cursor,
      size_t& length,
      uint64_t version,
      std::optional<size_t> numParams,
      Parameters& params,
      std::vector<Parameter>& requestSpecificParams,
      ParamsType paramsType) const noexcept;

  std::optional<uint64_t> version_;
  bool useMoQVarint_{false};
  MoQTokenCache* tokenCache_{nullptr};
  mutable std::optional<uint64_t> previousObjectID_;
  // Context for FETCH object delta encoding (draft-15+)
  mutable std::optional<uint64_t> previousFetchGroup_;
  mutable std::optional<uint64_t> previousFetchSubgroup_;
  mutable std::optional<uint8_t> previousFetchPriority_;
  mutable GroupOrder fetchGroupOrder_{GroupOrder::OldestFirst};
  // Context for extension delta decoding (draft-16+)
  mutable uint64_t previousExtensionType_ = 0;
};

//// Egress ////
TrackRequestParameter getAuthParam(
    uint64_t version,
    std::string token,
    uint64_t tokenType = 0,
    std::optional<uint64_t> registerToken = AuthToken::Register);

WriteResult writeSetup(
    folly::IOBufQueue& writeBuf,
    const Setup& setup,
    uint64_t version,
    bool isClient) noexcept;

WriteResult writeClientSetup(
    folly::IOBufQueue& writeBuf,
    const Setup& clientSetup,
    uint64_t version) noexcept;

WriteResult writeServerSetup(
    folly::IOBufQueue& writeBuf,
    const Setup& serverSetup,
    uint64_t version) noexcept;

// writeSetup, writeClientSetup and writeServerSetup are the only functions that
// are version-agnostic, so we are leaving them out of the MoQFrameWriter.
class MoQFrameWriter {
 public:
  WriteResult writeSubgroupHeader(
      folly::IOBufQueue& writeBuf,
      TrackAlias trackAlias,
      const ObjectHeader& objectHeader,
      SubgroupIDFormat format = SubgroupIDFormat::Present,
      bool includeExtensions = true,
      bool beginsWithFirstObject = false) const noexcept;

  WriteResult writeFetchHeader(folly::IOBufQueue& writeBuf, RequestID requestID)
      const noexcept;

  WriteResult writePaddingStream(
      folly::IOBufQueue& writeBuf,
      uint64_t paddingLength) const noexcept;

  WriteResult writePaddingDatagram(
      folly::IOBufQueue& writeBuf,
      uint64_t paddingLength) const noexcept;

  WriteResult writeStreamHeader(
      folly::IOBufQueue& writeBuf,
      StreamType streamType,
      TrackAlias trackAlias,
      const ObjectHeader& objectHeader) const noexcept;

  WriteResult writeDatagramObject(
      folly::IOBufQueue& writeBuf,
      TrackAlias trackAlias,
      const ObjectHeader& objectHeader,
      std::unique_ptr<folly::IOBuf> objectPayload,
      bool endOfGroup = false) const noexcept;

  WriteResult writeStreamObject(
      folly::IOBufQueue& writeBuf,
      StreamType streamType,
      const ObjectHeader& objectHeader,
      std::unique_ptr<folly::IOBuf> objectPayload,
      bool forwardingPreferenceIsDatagram = false) const noexcept;

  WriteResult writeSingleObjectStream(
      folly::IOBufQueue& writeBuf,
      TrackAlias trackAlias,
      const ObjectHeader& objectHeader,
      std::unique_ptr<folly::IOBuf> objectPayload) const noexcept;

  WriteResult writeSubscribeRequest(
      folly::IOBufQueue& writeBuf,
      const SubscribeRequest& subscribeRequest) const noexcept;

  WriteResult writeRequestUpdate(
      folly::IOBufQueue& writeBuf,
      const RequestUpdate& update) const noexcept;

  // Backward compatibility forwarder
  WriteResult writeSubscribeUpdate(
      folly::IOBufQueue& writeBuf,
      const SubscribeUpdate& update) const noexcept {
    return writeRequestUpdate(writeBuf, update);
  }

  WriteResult writeSubscribeOk(
      folly::IOBufQueue& writeBuf,
      const SubscribeOk& subscribeOk) const noexcept;

  WriteResult writePublishDone(
      folly::IOBufQueue& writeBuf,
      const PublishDone& publishDone) const noexcept;

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

  WriteResult writePublishNamespace(
      folly::IOBufQueue& writeBuf,
      const PublishNamespace& publishNamespace) const noexcept;

  WriteResult writePublishNamespaceOk(
      folly::IOBufQueue& writeBuf,
      const PublishNamespaceOk& publishNamespaceOk) const noexcept;

  WriteResult writeRequestOk(
      folly::IOBufQueue& writeBuf,
      const RequestOk& requestOk,
      FrameType frameType) const noexcept;

  WriteResult writePublishNamespaceDone(
      folly::IOBufQueue& writeBuf,
      const PublishNamespaceDone& publishNamespaceDone) const noexcept;

  WriteResult writePublishNamespaceCancel(
      folly::IOBufQueue& writeBuf,
      const PublishNamespaceCancel& publishNamespaceCancel) const noexcept;

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

  WriteResult writeSubscribeNamespace(
      folly::IOBufQueue& writeBuf,
      const SubscribeNamespace& subscribeNamespace) const noexcept;

  // Draft 18+ only.
  WriteResult writeSubscribeTracks(
      folly::IOBufQueue& writeBuf,
      const SubscribeTracks& subscribeTracks) const noexcept;

  // Draft 18+ only.
  WriteResult writePublishBlocked(
      folly::IOBufQueue& writeBuf,
      const PublishBlocked& publishBlocked) const noexcept;

  WriteResult writeSubscribeNamespaceOk(
      folly::IOBufQueue& writeBuf,
      const SubscribeNamespaceOk& subscribeNamespaceOk) const noexcept;

  WriteResult writeUnsubscribeNamespace(
      folly::IOBufQueue& writeBuf,
      const UnsubscribeNamespace& unsubscribeNamespace) const noexcept;

  // v16+ messages for SUBSCRIBE_NAMESPACE response stream
  WriteResult writeNamespace(folly::IOBufQueue& writeBuf, const Namespace& ns)
      const noexcept;

  WriteResult writeNamespaceDone(
      folly::IOBufQueue& writeBuf,
      const NamespaceDone& namespaceDone) const noexcept;

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
      const std::optional<uint64_t>& forceVersion = std::nullopt) const;

  void initializeVersion(uint64_t versionIn) {
    XCHECK(!version_) << "Version already initialized";
    version_ = versionIn;
    useMoQVarint_ = getDraftMajorVersion(versionIn) >= 17;
  }

  std::optional<uint64_t> getVersion() const {
    return version_;
  }

  // Encode a TrackNamespace as a TRACK_NAMESPACE_PREFIX parameter (a Track
  // Namespace tuple value, §2.4.1) for the given negotiated version. The
  // resulting parameter can be added to a REQUEST_UPDATE's parameter list to
  // update an established SUBSCRIBE_NAMESPACE / SUBSCRIBE_TRACKS prefix.
  static Parameter encodeTrackNamespacePrefixParam(
      const TrackNamespace& trackNamespacePrefix,
      uint64_t version);

  // Version-aware varint encoded-size query. Returns the number of bytes the
  // minimal encoding of `value` would consume, or sets `error` and returns 0
  // if the value is inexpressible (only possible with QUIC varint, which
  // tops out at 2^62-1; MoQ varint covers the full uint64_t range).
  size_t getVarintSize(uint64_t value, bool& error) const noexcept {
    if (useMoQVarint_) {
      return getMoQVarintSize(value);
    }
    auto res = quic::getQuicIntegerSize(value);
    if (res.hasError()) {
      error = true;
      return 0;
    }
    return *res;
  }

  // Version-aware varint encode. Dispatches to QUIC varint on drafts <17 and
  // MoQ varint on drafts >=17.
  void writeVarint(
      folly::IOBufQueue& buf,
      uint64_t value,
      size_t& size,
      bool& error) const noexcept {
    if (error) {
      return;
    }
    folly::io::QueueAppender appender(&buf, kMaxFrameHeaderSize);
    if (useMoQVarint_) {
      auto res = encodeMoQVarint(value, appender);
      if (res.hasError()) {
        error = true;
      } else {
        size += *res;
      }
      return;
    }
    auto appenderOp = [&appender](auto val) mutable {
      appender.writeBE(folly::tag<decltype(val)>, val);
    };
    auto res = quic::encodeQuicInteger(value, appenderOp);
    if (res.hasError()) {
      error = true;
    } else {
      size += *res;
    }
  }

  void setFetchGroupOrder(GroupOrder groupOrder) noexcept;

  void writeExtensions(
      folly::IOBufQueue& writeBuf,
      const Extensions& extensions,
      size_t& size,
      bool& error,
      bool withLengthPrefix = true) const noexcept;

 private:
  // Promoted from free helpers so they pick up useMoQVarint_/version_.
  void writeFixedString(
      folly::IOBufQueue& writeBuf,
      const std::string& str,
      size_t& size,
      bool& error) const noexcept;

  void writeFixedTuple(
      folly::IOBufQueue& writeBuf,
      const std::vector<std::string>& tup,
      size_t& size,
      bool& error) const noexcept;

  void writeTrackNamespace(
      folly::IOBufQueue& writeBuf,
      const TrackNamespace& tn,
      size_t& size,
      bool& error) const noexcept;

  uint16_t* writeFrameHeader(
      folly::IOBufQueue& writeBuf,
      FrameType frameType,
      bool& error) const noexcept;

  void writeFullTrackName(
      folly::IOBufQueue& writeBuf,
      const FullTrackName& fullTrackName,
      size_t& size,
      bool error) const noexcept;

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

  void writeParamValue(
      folly::IOBufQueue& writeBuf,
      const Parameter& param,
      size_t& size,
      bool& error) const noexcept;

  void writeV18ParamValue(
      folly::IOBufQueue& writeBuf,
      const Parameter& param,
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
      bool& error,
      bool forwardingPreferenceIsDatagram = false) const noexcept;

  void resetWriterFetchContext() const noexcept;

  // Version translation: convert track property extensions to params for < v16
  void convertTrackPropertyExtensionsToParams(
      const Extensions& extensions,
      TrackRequestParameters& params) const noexcept;

  std::optional<uint64_t> version_;
  bool useMoQVarint_{false};
  mutable std::optional<uint64_t> previousObjectID_;
  // Context for FETCH object delta encoding (draft-15+)
  mutable std::optional<uint64_t> previousFetchGroup_;
  mutable std::optional<uint64_t> previousFetchSubgroup_;
  mutable std::optional<uint8_t> previousFetchPriority_;
  mutable GroupOrder fetchGroupOrder_{GroupOrder::OldestFirst};

  // writeSetup is a free function but needs access to writer member helpers
  // to dispatch on the negotiated version.
  friend WriteResult writeSetup(
      folly::IOBufQueue& writeBuf,
      const Setup& setup,
      uint64_t version,
      bool isClient) noexcept;
};

// Parses the frame type from the beginning of the buffer without consuming it.
// Returns std::nullopt if there isn't enough data to parse the frame type.
std::optional<FrameType> getFrameType(
    const folly::IOBufQueue& readBuf,
    std::optional<uint64_t> version = std::nullopt);

} // namespace moxygen

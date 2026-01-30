/*
 *  Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 *  This source code is licensed under the MIT license found in the LICENSE
 *  file in the root directory of this source tree.
 *
 */

#pragma once

#include "folly/SocketAddress.h"
#include "folly/json/dynamic.h"
#include "folly/json/json.h"
#include "quic/codec/QuicConnectionId.h"
#include "moxygen/MoQTypes.h"
#include "moxygen/mlog/MLogEvents.h"
#include "moxygen/mlog/MLogTypes.h"

namespace moxygen {

// Client-Initiated Bidirectional Stream ID
const uint64_t kFirstBidiStreamId = 0;
const std::string kDefaultLoggerFilePath = "./mlog.txt";

// Abstract base class for MoQ logging.
// Subclasses implement outputLogs() to emit logs to a specific backend
// (e.g., FileMLogger for file output, ScubaMLogger for Scuba).
class MLogger {
 public:
  explicit MLogger(VantagePoint vantagePoint) : vantagePoint_(vantagePoint) {}
  virtual ~MLogger() = default;

  MOQTClientSetupMessage createClientSetupControlMessage(
      uint64_t numberOfSupportedVersions,
      std::vector<uint64_t> supportedVersions,
      uint64_t numberOfParameters,
      std::vector<MOQTSetupParameter> params);
  MOQTServerSetupMessage createServerSetupControlMessage(
      uint64_t selectedVersion,
      uint64_t number_of_parameters,
      std::vector<MOQTSetupParameter> params);

  void addControlMessageCreatedLog(MOQTControlMessageCreated req);
  void addControlMessageParsedLog(MOQTControlMessageParsed req);
  void addStreamTypeSetLog(MOQTStreamTypeSet req);
  void addObjectDatagramCreatedLog(MOQTObjectDatagramCreated req);
  void addObjectDatagramParsedLog(MOQTObjectDatagramParsed req);
  void addSubgroupHeaderCreatedLog(MOQTSubgroupHeaderCreated req);
  void addSubgroupHeaderParsedLog(MOQTSubgroupHeaderParsed req);
  void addSubgroupObjectCreatedLog(MOQTSubgroupObjectCreated req);
  void addSubgroupObjectParsedLog(MOQTSubgroupObjectParsed req);
  void addFetchHeaderCreatedLog(MOQTFetchHeaderCreated req);
  void addFetchHeaderParsedLog(MOQTFetchHeaderParsed req);
  void addFetchObjectCreatedLog(MOQTFetchObjectCreated req);
  void addFetchObjectParsedLog(MOQTFetchObjectParsed req);

  // Pure virtual method - subclasses must implement to emit logs to their
  // backend
  virtual void outputLogs() = 0;

  void logClientSetup(
      const ClientSetup& setup,
      ControlMessageType controlType = ControlMessageType::CREATED);
  void logServerSetup(
      const ServerSetup& setup,
      ControlMessageType controlType = ControlMessageType::CREATED);
  void logGoaway(
      const Goaway& goaway,
      ControlMessageType controlType = ControlMessageType::CREATED);
  void logSubscribe(
      const SubscribeRequest& req,
      const MOQTByteStringType& type = MOQTByteStringType::STRING_VALUE,
      ControlMessageType controlType = ControlMessageType::CREATED);
  void logSubscribeUpdate(
      const SubscribeUpdate& req,
      ControlMessageType controlType = ControlMessageType::CREATED);
  void logUnsubscribe(
      const Unsubscribe& req,
      ControlMessageType controlType = ControlMessageType::CREATED);
  void logFetch(
      const Fetch& req,
      const MOQTByteStringType& type = MOQTByteStringType::STRING_VALUE,
      ControlMessageType controlType = ControlMessageType::CREATED);
  void logFetchCancel(
      const FetchCancel& req,
      ControlMessageType controlType = ControlMessageType::CREATED);
  void logAnnounceOk(
      const AnnounceOk& req,
      ControlMessageType controlType = ControlMessageType::CREATED);
  void logAnnounceError(
      const AnnounceError& req,
      ControlMessageType controlType = ControlMessageType::CREATED);
  void logAnnounceCancel(
      const AnnounceCancel& req,
      const MOQTByteStringType& type = MOQTByteStringType::STRING_VALUE,
      ControlMessageType controlType = ControlMessageType::CREATED);
  void logTrackStatus(
      const TrackStatus& req,
      const MOQTByteStringType& type = MOQTByteStringType::STRING_VALUE,
      ControlMessageType controlType = ControlMessageType::CREATED);
  void logSubscribeAnnounces(
      const SubscribeAnnounces& req,
      const MOQTByteStringType& type = MOQTByteStringType::STRING_VALUE,
      ControlMessageType controlType = ControlMessageType::CREATED);
  void logUnsubscribeAnnounces(
      const UnsubscribeAnnounces& req,
      const MOQTByteStringType& type = MOQTByteStringType::STRING_VALUE,
      ControlMessageType controlType = ControlMessageType::CREATED);
  void logSubscribeOk(
      const SubscribeOk& req,
      ControlMessageType controlType = ControlMessageType::CREATED);
  void logSubscribeError(
      const SubscribeError& req,
      ControlMessageType controlType = ControlMessageType::CREATED);
  void logFetchOk(
      const FetchOk& req,
      ControlMessageType controlType = ControlMessageType::CREATED);
  void logFetchError(
      const FetchError& req,
      ControlMessageType controlType = ControlMessageType::CREATED);
  void logPublishDone(
      const SubscribeDone& req,
      ControlMessageType controlType = ControlMessageType::CREATED);
  void logMaxRequestId(
      const uint64_t requestId,
      ControlMessageType controlType = ControlMessageType::CREATED);
  void logRequestsBlocked(
      const uint64_t maximumRequestId,
      ControlMessageType controlType = ControlMessageType::CREATED);
  void logAnnounce(
      const Announce& req,
      const MOQTByteStringType& type = MOQTByteStringType::STRING_VALUE,
      ControlMessageType controlType = ControlMessageType::CREATED);
  void logUnannounce(
      const Unannounce& req,
      const MOQTByteStringType& type = MOQTByteStringType::STRING_VALUE,
      ControlMessageType controlType = ControlMessageType::CREATED);
  void logTrackStatusOk(
      const TrackStatusOk& req,
      const MOQTByteStringType& type = MOQTByteStringType::STRING_VALUE,
      ControlMessageType controlType = ControlMessageType::CREATED);
  void logTrackStatusError(
      const TrackStatusError& req,
      const MOQTByteStringType& type = MOQTByteStringType::STRING_VALUE,
      ControlMessageType controlType = ControlMessageType::CREATED);
  void logSubscribeAnnouncesOk(
      const SubscribeAnnouncesOk& req,
      ControlMessageType controlType = ControlMessageType::CREATED);
  void logSubscribeAnnouncesError(
      const SubscribeAnnouncesError& req,
      ControlMessageType controlType = ControlMessageType::CREATED);
  void logPublish(
      const PublishRequest& req,
      const MOQTByteStringType& type = MOQTByteStringType::STRING_VALUE,
      ControlMessageType controlType = ControlMessageType::CREATED);
  void logPublishOk(
      const PublishOk& req,
      ControlMessageType controlType = ControlMessageType::CREATED);
  void logPublishError(
      const PublishError& req,
      ControlMessageType controlType = ControlMessageType::CREATED);

  void logStreamTypeSet(
      uint64_t streamId,
      MOQTStreamType type,
      std::optional<Owner> owner);
  void logObjectDatagramCreated(
      TrackAlias trackAlias,
      const ObjectHeader& header,
      const Payload& payload);
  void logObjectDatagramParsed(
      TrackAlias trackAlias,
      const ObjectHeader& header,
      const Payload& payload);
  void logSubgroupHeaderCreated(
      uint64_t streamId,
      TrackAlias trackAlias,
      uint64_t groupId,
      uint64_t sugroupId,
      uint8_t publisherPriority,
      SubgroupIDFormat format,
      bool includeExtensions,
      bool endOfGroup);
  void logSubgroupHeaderParsed(
      uint64_t streamId,
      TrackAlias trackAlias,
      uint64_t groupId,
      uint64_t sugroupId,
      uint8_t publisherPriority,
      const SubgroupOptions& options);
  void logSubgroupObjectCreated(
      uint64_t streamId,
      TrackAlias trackAlias,
      const ObjectHeader& objHeader,
      Payload payload);
  void logSubgroupObjectParsed(
      uint64_t streamId,
      TrackAlias trackAlias,
      const ObjectHeader& objHeader,
      Payload payload);
  void logFetchHeaderCreated(const uint64_t streamId, const uint64_t requestId);
  void logFetchHeaderParsed(const uint64_t streamId, const uint64_t requestId);
  void logFetchObjectCreated(
      const uint64_t streamId,
      const ObjectHeader& header,
      Payload payload);
  void logFetchObjectParsed(
      const uint64_t streamId,
      const ObjectHeader& header,
      Payload payload);

  void setPath(const std::string& path);

  // Setter APIs for connection metadata (used by ScubaMLogger)
  void setDcid(const quic::ConnectionId& dcid);
  void setSrcCid(const quic::ConnectionId& srcCid);
  void setPeerAddress(const folly::SocketAddress& peerAddress);
  void setLocalAddress(const folly::SocketAddress& localAddress);
  void setNegotiatedMoQVersion(uint64_t version);
  void setExperiments(const std::vector<std::string>& experiments);

 protected:
  // Core members accessible to subclasses
  VantagePoint vantagePoint_;
  std::vector<MLogEvent> logs_;
  std::string path_ = kDefaultLoggerFilePath;
  MLogEventCreator eventCreator_ = MLogEventCreator();

  // Connection metadata (populated via setters, consumed by ScubaMLogger)
  std::optional<quic::ConnectionId> dcid_;
  std::optional<quic::ConnectionId> srcCid_;
  std::optional<folly::SocketAddress> peerAddress_;
  std::optional<folly::SocketAddress> localAddress_;
  std::optional<uint64_t> negotiatedMoQVersion_;
  std::vector<std::string> experiments_;

  // Log Formatting (protected for subclass access)
  folly::dynamic formatLog(const MLogEvent& log);

 private:
  MOQTByteString convertTrackNameToByteStringFormat(
      const std::string& t,
      const MOQTByteStringType& type = MOQTByteStringType::STRING_VALUE);
  std::vector<MOQTParameter> convertTrackParamsToMoQTParams(
      const TrackRequestParameters& params);
  std::vector<MOQTByteString> convertTrackNamespaceToByteStringFormat(
      const std::vector<std::string>& ns,
      const MOQTByteStringType& type = MOQTByteStringType::STRING_VALUE);
  std::vector<MOQTSetupParameter> convertSetupParamsToMoQTSetupParams(
      const SetupParameters& params);
  bool isHexstring(const std::string& s);
  void logControlMessage(
      ControlMessageType controlType,
      uint64_t streamId,
      const std::optional<uint64_t>& length,
      std::unique_ptr<MOQTBaseControlMessage> message,
      std::unique_ptr<folly::IOBuf> raw = nullptr);
  std::vector<MOQTExtensionHeader> convertExtensionToMoQTExtensionHeaders(
      std::vector<Extension> extensions);
};

} // namespace moxygen

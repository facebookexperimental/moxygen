/*
 *  Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 *  This source code is licensed under the MIT license found in the LICENSE
 *  file in the root directory of this source tree.
 *
 */

#pragma once

#include "folly/json/dynamic.h"
#include "folly/json/json.h"
#include "moxygen/MoQFramer.h"
#include "moxygen/mlog/MLogEvents.h"
#include "moxygen/mlog/MLogTypes.h"

namespace moxygen {

// Client-Initiated Bidirectional Stream ID
const uint64_t kFirstBidiStreamId = 0;
const std::string kDefaultLoggerFilePath = "./mlog.txt";

// Main Logger Class -> results in json output in mlog.txt file
class MLogger {
 public:
  explicit MLogger(VantagePoint vantagePoint) : vantagePoint_(vantagePoint) {}

  MOQTClientSetupMessage createClientSetupControlMessage(
      uint64_t numberOfSupportedVersions,
      std::vector<uint64_t> supportedVersions,
      uint64_t numberOfParameters,
      std::vector<MOQTParameter> params);
  MOQTServerSetupMessage createServerSetupControlMessage(
      uint64_t selectedVersion,
      uint64_t number_of_parameters,
      std::vector<MOQTParameter> params);

  void addControlMessageCreatedLog(MOQTControlMessageCreated req);
  void addControlMessageParsedLog(MOQTControlMessageParsed req);
  void addStreamTypeSetLog(MOQTStreamTypeSet req);
  void addObjectDatagramCreatedLog(MOQTObjectDatagramCreated req);
  void addObjectDatagramParsedLog(MOQTObjectDatagramParsed req);
  void addObjectDatagramStatusCreatedLog(MOQTObjectDatagramStatusCreated req);
  void addObjectDatagramStatusParsedLog(MOQTObjectDatagramStatusParsed req);
  void addSubgroupHeaderCreatedLog(MOQTSubgroupHeaderCreated req);
  void addSubgroupHeaderParsedLog(MOQTSubgroupHeaderParsed req);
  void addSubgroupObjectCreatedLog(MOQTSubgroupObjectCreated req);

  void outputLogsToFile();

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
      const MOQTByteStringType& type = MOQTByteStringType::STRING_VALUE,
      ControlMessageType controlType = ControlMessageType::CREATED);
  void logAnnounceError(
      const AnnounceError& req,
      const MOQTByteStringType& type = MOQTByteStringType::STRING_VALUE,
      ControlMessageType controlType = ControlMessageType::CREATED);
  void logAnnounceCancel(
      const AnnounceCancel& req,
      const MOQTByteStringType& type = MOQTByteStringType::STRING_VALUE,
      ControlMessageType controlType = ControlMessageType::CREATED);
  void logTrackStatusRequest(
      const TrackStatusRequest& req,
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
  void logSubscribeDone(
      const SubscribeDone& req,
      ControlMessageType controlType = ControlMessageType::CREATED);
  void logMaxSubscribeId(
      const uint64_t maxRequestID,
      ControlMessageType controlType = ControlMessageType::CREATED);
  void logSubscribesBlocked(
      const uint64_t maxRequestID,
      ControlMessageType controlType = ControlMessageType::CREATED);
  void logAnnounce(
      const Announce& req,
      const MOQTByteStringType& type = MOQTByteStringType::STRING_VALUE,
      ControlMessageType controlType = ControlMessageType::CREATED);
  void logUnannounce(
      const Unannounce& req,
      const MOQTByteStringType& type = MOQTByteStringType::STRING_VALUE,
      ControlMessageType controlType = ControlMessageType::CREATED);
  void logTrackStatus(
      const TrackStatus& req,
      const MOQTByteStringType& type = MOQTByteStringType::STRING_VALUE,
      ControlMessageType controlType = ControlMessageType::CREATED);
  void logSubscribeAnnouncesOk(
      const SubscribeAnnouncesOk& req,
      const MOQTByteStringType& type = MOQTByteStringType::STRING_VALUE,
      ControlMessageType controlType = ControlMessageType::CREATED);
  void logSubscribeAnnouncesError(
      const SubscribeAnnouncesError& req,
      const MOQTByteStringType& type = MOQTByteStringType::STRING_VALUE,
      ControlMessageType controlType = ControlMessageType::CREATED);

  void logStreamTypeSet(
      uint64_t streamId,
      MOQTStreamType type,
      folly::Optional<Owner> owner);
  void logObjectDatagramCreated(
      const ObjectHeader& header,
      const Payload& payload);
  void logObjectDatagramParsed(
      const ObjectHeader& header,
      const Payload& payload);
  void logObjectDatagramStatusCreated(const ObjectHeader& header);
  void logObjectDatagramStatusParsed(const ObjectHeader& header);
  void logSubgroupHeaderCreated(
      uint64_t streamId,
      TrackAlias trackAlias,
      uint64_t groupId,
      uint64_t sugroupId,
      uint8_t publisherPriority);
  void logSubgroupHeaderParsed(
      uint64_t streamId,
      TrackAlias trackAlias,
      uint64_t groupId,
      uint64_t sugroupId,
      uint8_t publisherPriority);
  void logSubgroupObjectCreated(
      uint64_t streamId,
      const ObjectHeader& objHeader,
      Payload payload);

  void setPath(const std::string& path);

 private:
  VantagePoint vantagePoint_;
  std::vector<MLogEvent> logs_;
  std::string path_ = kDefaultLoggerFilePath;
  MLogEventCreator eventCreator_ = MLogEventCreator();

  // Log Formatting
  folly::dynamic formatLog(const MLogEvent& log);
  MOQTByteString convertTrackNameToByteStringFormat(
      const std::string& t,
      const MOQTByteStringType& type = MOQTByteStringType::STRING_VALUE);
  std::vector<MOQTParameter> convertTrackParamsToMoQTParams(
      const std::vector<TrackRequestParameter>& params);
  std::vector<MOQTByteString> convertTrackNamespaceToByteStringFormat(
      const std::vector<std::string>& ns,
      const MOQTByteStringType& type = MOQTByteStringType::STRING_VALUE);
  std::vector<MOQTParameter> convertSetupParamsToMoQTParams(
      const std::vector<SetupParameter>& params);
  bool isHexstring(const std::string& s);
  void logControlMessage(
      ControlMessageType controlType,
      uint64_t streamId,
      const folly::Optional<uint64_t>& length,
      std::unique_ptr<MOQTBaseControlMessage> message,
      std::unique_ptr<folly::IOBuf> raw = nullptr);
  std::vector<MOQTExtensionHeader> convertExtensionToMoQTExtensionHeaders(
      std::vector<Extension> extensions);
};

} // namespace moxygen

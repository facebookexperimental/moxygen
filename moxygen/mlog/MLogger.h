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

  void outputLogsToFile();

  void logClientSetup(const ClientSetup& setup);
  void logServerSetup(const ServerSetup& setup);
  void logGoaway(const Goaway& goaway);
  void logSubscribe(
      const SubscribeRequest& req,
      const MOQTByteStringType& type = MOQTByteStringType::STRING_VALUE);
  void logSubscribeUpdate(const SubscribeUpdate& req);
  void logUnsubscribe(const Unsubscribe& req);
  void logFetch(
      const Fetch& req,
      const MOQTByteStringType& type = MOQTByteStringType::STRING_VALUE);
  void logFetchCancel(const FetchCancel& req);
  void logAnnounceOk(
      const AnnounceOk& req,
      const MOQTByteStringType& type = MOQTByteStringType::STRING_VALUE);
  void logAnnounceError(
      const AnnounceError& req,
      const MOQTByteStringType& type = MOQTByteStringType::STRING_VALUE);
  void logAnnounceCancel(
      const AnnounceCancel& req,
      const MOQTByteStringType& type = MOQTByteStringType::STRING_VALUE);
  void logTrackStatusRequest(
      const TrackStatusRequest& req,
      const MOQTByteStringType& type = MOQTByteStringType::STRING_VALUE);
  void logSubscribeAnnounces(
      const SubscribeAnnounces& req,
      const MOQTByteStringType& type = MOQTByteStringType::STRING_VALUE);
  void logUnsubscribeAnnounces(
      const UnsubscribeAnnounces& req,
      const MOQTByteStringType& type = MOQTByteStringType::STRING_VALUE);
  void logSubscribeOk(const SubscribeOk& req);
  void logSubscribeError(const SubscribeError& req);
  void logFetchOk(const FetchOk& req);
  void logFetchError(const FetchError& req);
  void logSubscribeDone(const SubscribeDone& req);
  void logMaxSubscribeId(const uint64_t maxRequestID);
  void logSubscribesBlocked(const uint64_t maxRequestID);
  void logAnnounce(
      const Announce& req,
      const MOQTByteStringType& type = MOQTByteStringType::STRING_VALUE);
  void logUnannounce(
      const Unannounce& req,
      const MOQTByteStringType& type = MOQTByteStringType::STRING_VALUE);
  void logTrackStatus(
      const TrackStatus& req,
      const MOQTByteStringType& type = MOQTByteStringType::STRING_VALUE);
  void logSubscribeAnnouncesOk(
      const SubscribeAnnouncesOk& req,
      const MOQTByteStringType& type = MOQTByteStringType::STRING_VALUE);

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
};

} // namespace moxygen

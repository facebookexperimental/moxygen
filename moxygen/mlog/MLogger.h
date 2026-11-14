/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <unordered_map>
#include <vector>
#include "folly/Function.h"
#include "folly/SocketAddress.h"
#include "folly/container/F14Map.h"
#include "folly/json/dynamic.h"
#include "folly/json/json.h"
#include "quic/codec/QuicConnectionId.h"
#include "moxygen/MoQFrameObserver.h"
#include "moxygen/MoQTypes.h"
#include "moxygen/mlog/MLogEvents.h"
#include "moxygen/mlog/MLogTypes.h"

namespace moxygen {

const std::string kDefaultLoggerFilePath = "./mlog.txt";

// Abstract base class for MoQ logging.
// Subclasses implement outputLogs() to emit logs to a specific backend
// (e.g., FileMLogger for file output, ScubaMLogger for Scuba).
class MLogger : public MoQFrameObserver {
 public:
  explicit MLogger(VantagePoint vantagePoint) : vantagePoint_(vantagePoint) {}
  ~MLogger() override = default;

  MOQTSetupMessage createSetupControlMessage(
      const std::string& setupType,
      uint64_t numberOfParameters,
      std::vector<MOQTSetupParameter> params);

  // Keep old names for backward compatibility
  MOQTSetupMessage createClientSetupControlMessage(
      uint64_t numberOfParameters,
      std::vector<MOQTSetupParameter> params) {
    return createSetupControlMessage(
        "client_setup", numberOfParameters, std::move(params));
  }
  MOQTSetupMessage createServerSetupControlMessage(
      uint64_t numberOfParameters,
      std::vector<MOQTSetupParameter> params) {
    return createSetupControlMessage(
        "server_setup", numberOfParameters, std::move(params));
  }

  void addLog(std::string_view name, MLogEventData data);

  // Pure virtual method - subclasses must implement to emit logs to their
  // backend
  virtual void outputLogs() = 0;

  void onClientSetup(Direction, uint64_t, const ClientSetup&) override;
  void onServerSetup(Direction, uint64_t, const ServerSetup&) override;
  void onSubscribe(Direction, uint64_t, const SubscribeRequest&) override;
  void onSubscribeOk(Direction, uint64_t, const SubscribeOk&) override;
  void onRequestUpdate(Direction, uint64_t, const RequestUpdate&) override;
  void onUnsubscribe(Direction, uint64_t, const Unsubscribe&) override;
  void onPublishDone(Direction, uint64_t, const PublishDone&) override;
  void onPublish(Direction, uint64_t, const PublishRequest&) override;
  void onPublishOk(Direction, uint64_t, const PublishOk&) override;
  void onFetch(Direction, uint64_t, const Fetch&) override;
  void onFetchOk(Direction, uint64_t, const FetchOk&) override;
  void onFetchCancel(Direction, uint64_t, const FetchCancel&) override;
  void onRequestOk(Direction, uint64_t, const RequestOk&) override;
  void onRequestError(Direction, uint64_t, const RequestError&) override;
  void onTrackStatus(Direction, uint64_t, const TrackStatus&) override;
  void onPublishNamespace(Direction, uint64_t, const PublishNamespace&)
      override;
  void onPublishNamespaceDone(Direction, uint64_t, const PublishNamespaceDone&)
      override;
  void onPublishNamespaceCancel(
      Direction,
      uint64_t,
      const PublishNamespaceCancel&) override;
  void onSubscribeNamespace(Direction, uint64_t, const SubscribeNamespace&)
      override;
  void onNamespace(Direction, uint64_t, const Namespace&) override;
  void onNamespaceDone(Direction, uint64_t, const NamespaceDone&) override;
  void onSubscribeTracks(Direction, uint64_t, const SubscribeTracks&) override;
  void onPublishBlocked(Direction, uint64_t, const PublishBlocked&) override;
  void onGoaway(Direction, uint64_t, const Goaway&) override;
  void onMaxRequestID(Direction, uint64_t, const MaxRequestID&) override;
  void onRequestsBlocked(Direction, uint64_t, const RequestsBlocked&) override;
  void onPaddingStream(Direction, DataStream) override;
  void onSubgroupHeader(
      Direction,
      DataStream,
      TrackAlias,
      uint64_t,
      uint64_t,
      std::optional<uint8_t>,
      const SubgroupOptions&) override;
  void onSubgroupObject(Direction, DataStream, TrackAlias, const ObjectHeader&)
      override;
  void onFetchHeader(Direction, DataStream, uint64_t) override;
  void onFetchObject(Direction, DataStream, const ObjectHeader&) override;
  void onFetchEndOfRange(
      Direction,
      DataStream,
      uint64_t,
      uint64_t,
      FetchRangeEnd) override;
  void onPaddingDatagram(Direction, uint64_t paddingLength) override;
  void onDatagramObject(Direction, TrackAlias, const ObjectHeader&, bool)
      override;
  void onDataStreamBound(uint64_t pendingKey, uint64_t streamID) override;
  void onDataStreamAbandoned(uint64_t pendingKey) override;

  void logClientSetup(
      uint64_t streamId,
      const Setup& setup,
      ControlMessageType controlType = ControlMessageType::CREATED);
  void logServerSetup(
      uint64_t streamId,
      const Setup& setup,
      ControlMessageType controlType = ControlMessageType::CREATED);
  void logGoaway(
      uint64_t streamId,
      const Goaway& goaway,
      ControlMessageType controlType = ControlMessageType::CREATED);
  void logSubscribe(
      uint64_t streamId,
      const SubscribeRequest& req,
      const MOQTByteStringType& type = MOQTByteStringType::STRING_VALUE,
      ControlMessageType controlType = ControlMessageType::CREATED);
  // `fillRangeFilters` contains encoded 0x25-0x28 parameter values. It is a
  // sidecar until moxygen's FillParameters model carries these draft-20
  // fields itself.
  void logSubscribe(
      uint64_t streamId,
      const SubscribeRequest& req,
      const std::vector<Parameter>& fillRangeFilters,
      const MOQTByteStringType& type = MOQTByteStringType::STRING_VALUE,
      ControlMessageType controlType = ControlMessageType::CREATED);
  void logSubscribeUpdate(
      uint64_t streamId,
      const SubscribeUpdate& req,
      ControlMessageType controlType = ControlMessageType::CREATED);
  void logSubscribeUpdate(
      uint64_t streamId,
      const SubscribeUpdate& req,
      const std::vector<Parameter>& fillRangeFilters,
      ControlMessageType controlType = ControlMessageType::CREATED);
  void logRequestOk(
      uint64_t streamId,
      const RequestOk& req,
      ControlMessageType controlType = ControlMessageType::CREATED);
  void logRequestError(
      uint64_t streamId,
      const RequestError& req,
      ControlMessageType controlType = ControlMessageType::CREATED);
  void logPublishStateNotify(
      uint64_t streamId,
      const TrackRequestParameters& params,
      ControlMessageType controlType = ControlMessageType::CREATED);
  void logUnsubscribe(
      uint64_t streamId,
      const Unsubscribe& req,
      ControlMessageType controlType = ControlMessageType::CREATED);
  void logFetch(
      uint64_t streamId,
      const Fetch& req,
      const MOQTByteStringType& type = MOQTByteStringType::STRING_VALUE,
      ControlMessageType controlType = ControlMessageType::CREATED);
  void logFetchCancel(
      uint64_t streamId,
      const FetchCancel& req,
      ControlMessageType controlType = ControlMessageType::CREATED);
  void logPublishNamespaceOk(
      uint64_t streamId,
      const PublishNamespaceOk& req,
      ControlMessageType controlType = ControlMessageType::CREATED);
  void logPublishNamespaceError(
      uint64_t streamId,
      const PublishNamespaceError& req,
      ControlMessageType controlType = ControlMessageType::CREATED);
  void logPublishNamespaceCancel(
      uint64_t streamId,
      const PublishNamespaceCancel& req,
      const MOQTByteStringType& type = MOQTByteStringType::STRING_VALUE,
      ControlMessageType controlType = ControlMessageType::CREATED);
  void logTrackStatus(
      uint64_t streamId,
      const TrackStatus& req,
      const MOQTByteStringType& type = MOQTByteStringType::STRING_VALUE,
      ControlMessageType controlType = ControlMessageType::CREATED);
  void logSubscribeNamespace(
      uint64_t streamId,
      const SubscribeNamespace& req,
      const MOQTByteStringType& type = MOQTByteStringType::STRING_VALUE,
      ControlMessageType controlType = ControlMessageType::CREATED);
  void logSubscribeTracks(
      uint64_t streamId,
      const SubscribeTracks& req,
      const MOQTByteStringType& type = MOQTByteStringType::STRING_VALUE,
      ControlMessageType controlType = ControlMessageType::CREATED);
  void logNamespace(
      uint64_t streamId,
      const Namespace& req,
      const MOQTByteStringType& type = MOQTByteStringType::STRING_VALUE,
      ControlMessageType controlType = ControlMessageType::CREATED);
  void logNamespaceDone(
      uint64_t streamId,
      const NamespaceDone& req,
      const MOQTByteStringType& type = MOQTByteStringType::STRING_VALUE,
      ControlMessageType controlType = ControlMessageType::CREATED);
  void logPublishSkipped(
      uint64_t streamId,
      const PublishBlocked& req,
      const MOQTByteStringType& type = MOQTByteStringType::STRING_VALUE,
      ControlMessageType controlType = ControlMessageType::CREATED);
  void logUnsubscribeNamespace(
      uint64_t streamId,
      const UnsubscribeNamespace& req,
      const MOQTByteStringType& type = MOQTByteStringType::STRING_VALUE,
      ControlMessageType controlType = ControlMessageType::CREATED);
  void logSubscribeOk(
      uint64_t streamId,
      const SubscribeOk& req,
      ControlMessageType controlType = ControlMessageType::CREATED);
  void logSubscribeError(
      uint64_t streamId,
      const SubscribeError& req,
      ControlMessageType controlType = ControlMessageType::CREATED);
  void logFetchOk(
      uint64_t streamId,
      const FetchOk& req,
      ControlMessageType controlType = ControlMessageType::CREATED);
  void logFetchError(
      uint64_t streamId,
      const FetchError& req,
      ControlMessageType controlType = ControlMessageType::CREATED);
  void logPublishDone(
      uint64_t streamId,
      const PublishDone& req,
      ControlMessageType controlType = ControlMessageType::CREATED);
  void logMaxRequestId(
      uint64_t streamId,
      const uint64_t requestId,
      ControlMessageType controlType = ControlMessageType::CREATED);
  void logRequestsBlocked(
      uint64_t streamId,
      const uint64_t maximumRequestId,
      ControlMessageType controlType = ControlMessageType::CREATED);
  void logPublishNamespace(
      uint64_t streamId,
      const PublishNamespace& req,
      const MOQTByteStringType& type = MOQTByteStringType::STRING_VALUE,
      ControlMessageType controlType = ControlMessageType::CREATED);
  void logPublishNamespaceDone(
      uint64_t streamId,
      const PublishNamespaceDone& req,
      const MOQTByteStringType& type = MOQTByteStringType::STRING_VALUE,
      ControlMessageType controlType = ControlMessageType::CREATED);
  void logTrackStatusOk(
      uint64_t streamId,
      const TrackStatusOk& req,
      const MOQTByteStringType& type = MOQTByteStringType::STRING_VALUE,
      ControlMessageType controlType = ControlMessageType::CREATED);
  void logTrackStatusError(
      uint64_t streamId,
      const TrackStatusError& req,
      const MOQTByteStringType& type = MOQTByteStringType::STRING_VALUE,
      ControlMessageType controlType = ControlMessageType::CREATED);
  void logSubscribeNamespaceOk(
      uint64_t streamId,
      const SubscribeNamespaceOk& req,
      ControlMessageType controlType = ControlMessageType::CREATED);
  void logSubscribeNamespaceError(
      uint64_t streamId,
      const SubscribeNamespaceError& req,
      ControlMessageType controlType = ControlMessageType::CREATED);
  void logPublish(
      uint64_t streamId,
      const PublishRequest& req,
      const MOQTByteStringType& type = MOQTByteStringType::STRING_VALUE,
      ControlMessageType controlType = ControlMessageType::CREATED);
  void logPublishOk(
      uint64_t streamId,
      const PublishOk& req,
      ControlMessageType controlType = ControlMessageType::CREATED);
  void logPublishError(
      uint64_t streamId,
      const PublishError& req,
      ControlMessageType controlType = ControlMessageType::CREATED);

  void logStreamTypeSet(
      uint64_t streamId,
      MOQTStreamType type,
      std::optional<Owner> owner);
  // Setter APIs for connection metadata (used by ScubaMLogger)
  void setDcid(const quic::ConnectionId& dcid);
  void setSrcCid(const quic::ConnectionId& srcCid);
  void setPeerAddress(const folly::SocketAddress& peerAddress);
  void setLocalAddress(const folly::SocketAddress& localAddress);
  void setNegotiatedMoQVersion(uint64_t version);
  void setExperiments(const std::vector<std::string>& experiments);

 protected:
  size_t pendingDataStreamCount() const {
    return pendingDataStreams_.size();
  }

  // Core members accessible to subclasses
  VantagePoint vantagePoint_;
  std::vector<MLogEvent> logs_;
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
      std::unique_ptr<MOQTBaseControlMessage> message);
  // Logs stream_type_set for a frame that opens its control stream.
  void logStreamOpen(Direction dir, uint64_t streamID, bool opens);
  bool bidiRequests() const;
  std::vector<MOQTProperty> convertExtensionsToProperties(
      const Extensions& extensions);

  struct ObjectStreamState {
    std::optional<uint64_t> group;
    std::optional<uint64_t> subgroup;
    std::optional<uint64_t> object;
    std::optional<uint8_t> priority;
  };

  std::unordered_map<uint64_t, ObjectStreamState> createdObjectStreamState_;
  std::unordered_map<uint64_t, ObjectStreamState> parsedObjectStreamState_;
  std::unordered_map<uint64_t, ObjectStreamState>& objectStreamState(
      Direction dir);

  // Queues hook(dir, stream, args...) until the key is bound. Returns false
  // when stream is a stream ID, so the caller logs it now.
  template <typename Hook, typename... Args>
  bool holdIfPending(
      DataStream stream,
      Hook hook,
      Direction dir,
      const Args&... args) {
    if (stream.kind != DataStream::Kind::PendingKey) {
      return false;
    }
    pendingDataStreams_[stream.value].emplace_back(
        [this, hook, dir, args...](uint64_t streamID) {
          (this->*hook)(dir, DataStream{streamID}, args...);
        });
    return true;
  }

  // Holds records for frames serialized before their stream existed, keyed by
  // pending key. Each one is logged with the stream ID once the key is bound.
  folly::F14FastMap<uint64_t, std::vector<folly::Function<void(uint64_t)>>>
      pendingDataStreams_;
};

} // namespace moxygen

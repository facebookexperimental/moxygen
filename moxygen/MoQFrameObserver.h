/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <moxygen/MoQTypes.h>

namespace moxygen {

// Sent fires once MoQFrameWriter serializes a frame. Received fires once a
// codec parses a frame, before the session's callback runs.
class MoQFrameObserver {
 public:
  enum class Direction : uint8_t { Sent, Received };

  virtual ~MoQFrameObserver() = default;

  virtual void
  onClientSetup(Direction, uint64_t /*streamID*/, const ClientSetup&) {}
  virtual void
  onServerSetup(Direction, uint64_t /*streamID*/, const ServerSetup&) {}

  virtual void
  onSubscribe(Direction, uint64_t /*streamID*/, const SubscribeRequest&) {}
  virtual void
  onSubscribeOk(Direction, uint64_t /*streamID*/, const SubscribeOk&) {}
  virtual void
  onRequestUpdate(Direction, uint64_t /*streamID*/, const RequestUpdate&) {}
  virtual void
  onUnsubscribe(Direction, uint64_t /*streamID*/, const Unsubscribe&) {}
  virtual void
  onPublishDone(Direction, uint64_t /*streamID*/, const PublishDone&) {}

  virtual void
  onPublish(Direction, uint64_t /*streamID*/, const PublishRequest&) {}
  virtual void onPublishOk(Direction, uint64_t /*streamID*/, const PublishOk&) {
  }

  virtual void onFetch(Direction, uint64_t /*streamID*/, const Fetch&) {}
  virtual void onFetchOk(Direction, uint64_t /*streamID*/, const FetchOk&) {}
  virtual void
  onFetchCancel(Direction, uint64_t /*streamID*/, const FetchCancel&) {}

  // These cover REQUEST_OK and REQUEST_ERROR for every request type.
  virtual void onRequestOk(Direction, uint64_t /*streamID*/, const RequestOk&) {
  }
  virtual void
  onRequestError(Direction, uint64_t /*streamID*/, const RequestError&) {}

  virtual void
  onTrackStatus(Direction, uint64_t /*streamID*/, const TrackStatus&) {}

  virtual void onPublishNamespace(
      Direction,
      uint64_t /*streamID*/,
      const PublishNamespace&) {}
  virtual void onPublishNamespaceDone(
      Direction,
      uint64_t /*streamID*/,
      const PublishNamespaceDone&) {}
  virtual void onPublishNamespaceCancel(
      Direction,
      uint64_t /*streamID*/,
      const PublishNamespaceCancel&) {}
  virtual void onSubscribeNamespace(
      Direction,
      uint64_t /*streamID*/,
      const SubscribeNamespace&) {}
  virtual void onNamespace(Direction, uint64_t /*streamID*/, const Namespace&) {
  }
  virtual void
  onNamespaceDone(Direction, uint64_t /*streamID*/, const NamespaceDone&) {}
  virtual void
  onSubscribeTracks(Direction, uint64_t /*streamID*/, const SubscribeTracks&) {}
  virtual void
  onPublishBlocked(Direction, uint64_t /*streamID*/, const PublishBlocked&) {}

  virtual void onGoaway(Direction, uint64_t /*streamID*/, const Goaway&) {}
  virtual void
  onMaxRequestID(Direction, uint64_t /*streamID*/, const MaxRequestID&) {}
  virtual void
  onRequestsBlocked(Direction, uint64_t /*streamID*/, const RequestsBlocked&) {}

  // A data-stream frame serialized before its stream exists carries a pending
  // key instead of a stream ID. onDataStreamBound later gives the key's
  // stream. onDataStreamAbandoned means the stream will not exist.
  struct DataStream {
    enum class Kind : uint8_t { StreamID, PendingKey };
    uint64_t value{0};
    Kind kind{Kind::StreamID};
  };
  virtual void onPaddingStream(Direction, DataStream) {}
  // priority is unset when the header omits it. options.priorityPresent is
  // not used.
  virtual void onSubgroupHeader(
      Direction,
      DataStream,
      TrackAlias,
      uint64_t /*group*/,
      uint64_t /*subgroup*/,
      std::optional<uint8_t> /*priority*/,
      const SubgroupOptions&) {}
  // An object's header.length is its declared payload length.
  virtual void
  onSubgroupObject(Direction, DataStream, TrackAlias, const ObjectHeader&) {}
  virtual void onFetchHeader(Direction, DataStream, uint64_t /*requestID*/) {}
  virtual void onFetchObject(Direction, DataStream, const ObjectHeader&) {}
  virtual void onFetchEndOfRange(
      Direction,
      DataStream,
      uint64_t /*group*/,
      uint64_t /*object*/,
      FetchRangeEnd) {}
  virtual void onDataStreamBound(
      uint64_t /*pendingKey*/,
      uint64_t /*streamID*/) {}
  virtual void onDataStreamAbandoned(uint64_t /*pendingKey*/) {}
};

} // namespace moxygen

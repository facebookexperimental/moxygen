/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <moxygen/MoQConsumers.h>

namespace moxygen {

struct StreamingObjectContext {
  std::optional<TrackAlias> trackAlias;
  ObjectHeader header;
  std::optional<BeginSubgroupOptions> subgroupOptions;
  std::optional<bool> lastInGroup;
};

struct StreamingObjectPayloadMetadata {
  // Set on exactly the call that completes the object's declared length.
  bool endOfObject{false};
  // True when a subgroup stream's FIN arrives with the object's final bytes;
  // onEndOfStream follows either way.  MoQSession reports FIN only with an
  // object delivered whole, so chunked objects always see false.  Always false
  // for FETCH, objectStream and datagram deliveries, none of which produce
  // onEndOfStream.
  bool endOfSubgroup{false};
  // nullopt when unknown: FETCH objects, and subgroup objects unless the
  // subgroup declares the group's last object and FIN arrives with this one
  // (see endOfSubgroup).
  // An END_OF_GROUP status can end any subgroup, so no subgroup object is known
  // false.  Datagram and objectStream deliveries carry the sender's flag.
  std::optional<bool> lastInGroup;
};

struct StreamingSubgroupContext {
  std::optional<TrackAlias> trackAlias;
  uint64_t groupID{0};
  uint64_t subgroupID{0};
  Priority priority{kDefaultPriority};
  std::optional<BeginSubgroupOptions> subgroupOptions;
};

struct StreamingUnknownRange {
  std::optional<TrackAlias> trackAlias;
  uint64_t groupID{0};
  uint64_t objectID{0};
};

class StreamingObjectPayloadConsumer {
 public:
  virtual ~StreamingObjectPayloadConsumer() = default;

  // Any error is terminal for the object.  This prototype does not expose
  // application-level pause/resume flow control.
  virtual folly::Expected<folly::Unit, MoQPublishError> onPayload(
      Payload payload,
      StreamingObjectPayloadMetadata metadata) = 0;

  // Delivered if the object stream is reset or ends before the object's
  // declared length is received.  A reset raised from within onPayload is
  // delivered before that call returns, even for the endOfObject payload.
  virtual void onError(ResetStreamErrorCode error) = 0;
};

// Neither this interface nor StreamingObjectPayloadConsumer may throw: a
// subgroup receiver's destructor delivers their terminal callbacks.
class StreamingObjectReceiverCallback {
 public:
  virtual ~StreamingObjectReceiverCallback() = default;

  // Called before any payload for this object.  The returned consumer scopes
  // all later payload and error callbacks to this exact object.  An error
  // rejects the object before any payload is accepted and resets the stream
  // carrying it: later objects on a subgroup or FETCH stream are lost too.  A
  // rejected datagram is dropped without any further callback.
  virtual folly::
      Expected<std::shared_ptr<StreamingObjectPayloadConsumer>, MoQPublishError>
      onObjectBegin(StreamingObjectContext context) = 0;
  virtual void onObjectStatus(StreamingObjectContext context) = 0;
  // Reports a subgroup's clean FIN, after the final object's payload when the
  // FIN arrives with it.  A subgroup ended by an END_OF_GROUP or END_OF_TRACK
  // status and a FETCH never produce one; onAllDataReceived is the request's
  // completion signal.
  virtual void onEndOfStream(StreamingSubgroupContext context) = 0;
  virtual void onError(
      StreamingSubgroupContext context,
      ResetStreamErrorCode error) = 0;
  virtual void onPublishDone(PublishDone done) = 0;
  virtual void onUnknownRange(StreamingUnknownRange /*range*/) {}

  // For SUBSCRIBEs, called after PUBLISH_DONE and all subgroup streams close.
  // For FETCHes, called after a clean FETCH terminal.  A failed FETCH never
  // invokes this callback.
  virtual void onAllDataReceived() {}

  // NOLINTNEXTLINE(performance-unnecessary-value-param)
  virtual void onGoaway(Goaway /*goaway*/) {}
};

class StreamingObjectReceiver;

class StreamingObjectSubgroupReceiver : public SubgroupConsumer {
 public:
  explicit StreamingObjectSubgroupReceiver(
      std::shared_ptr<StreamingObjectReceiverCallback> callback,
      std::optional<TrackAlias> trackAlias = std::nullopt,
      uint64_t groupID = 0,
      uint64_t subgroupID = 0,
      Priority priority = kDefaultPriority,
      std::optional<BeginSubgroupOptions> subgroupOptions = std::nullopt)
      : callback_(std::move(callback)),
        streamType_(StreamType::SUBGROUP_HEADER_SG),
        header_(groupID, subgroupID, 0, priority),
        trackAlias_(trackAlias),
        subgroupOptions_(subgroupOptions) {}

  // MoQSession can drop a consumer without a terminal callback (a local
  // cancel, a stream that FINs mid-object).  An unfinished subscription
  // subgroup is terminated so the subscription still completes; a FETCH
  // receiver has no parent and only fails its in-flight object.
  ~StreamingObjectSubgroupReceiver() override {
    if (parent_) {
      terminateWithError(ResetStreamErrorCode::CANCELLED);
    } else {
      failCurrentObject(ResetStreamErrorCode::CANCELLED);
    }
  }

  void setParent(std::shared_ptr<StreamingObjectReceiver> parent) {
    parent_ = std::move(parent);
  }

  void setTrackAlias(TrackAlias trackAlias) {
    trackAlias_ = trackAlias;
  }

  void markFetchStream() {
    streamType_ = StreamType::FETCH_HEADER;
    subgroupOptions_.reset();
  }

  void setFetchGroupAndSubgroup(uint64_t groupID, uint64_t subgroupID) {
    header_.group = groupID;
    header_.subgroup = subgroupID;
  }

  void setForwardingPreferenceIsDatagram(bool forwardingPreferenceIsDatagram) {
    forwardingPreferenceIsDatagram_ = forwardingPreferenceIsDatagram;
  }

  void markFinished() {
    notifyParentFinished();
  }

  bool hasActiveObject() const {
    return currentObject_ != nullptr;
  }

  folly::Expected<folly::Unit, MoQPublishError> object(
      uint64_t objectID,
      Payload payload,
      Extensions extensions,
      bool finSubgroup) override {
    auto preparation = prepareForObject(objectID, "Object");
    if (preparation.hasError()) {
      return preparation;
    }

    const auto length = payloadSize(payload);
    header_.id = objectID;
    header_.status = ObjectStatus::NORMAL;
    header_.extensions = std::move(extensions);
    header_.length = length;
    header_.forwardingPreferenceIsDatagram = forwardingPreferenceIsDatagram_;

    auto admission = admitObject(length, lastInGroup(finSubgroup));
    if (admission.hasError()) {
      return admission;
    }
    return deliverAdmittedPayload(std::move(payload), finSubgroup);
  }

  folly::Expected<folly::Unit, MoQPublishError> beginObject(
      uint64_t objectID,
      uint64_t length,
      Payload initialPayload,
      Extensions extensions) override {
    auto preparation = prepareForObject(objectID, "Object");
    if (preparation.hasError()) {
      return preparation;
    }

    const auto initialLength = payloadSize(initialPayload);
    if (initialLength > length) {
      terminateWithError(ResetStreamErrorCode::MALFORMED_TRACK);
      return apiError("Initial payload exceeds declared object length");
    }

    header_.id = objectID;
    header_.status = ObjectStatus::NORMAL;
    header_.extensions = std::move(extensions);
    header_.length = length;
    header_.forwardingPreferenceIsDatagram = false;

    auto admission = admitObject(length, lastInGroup(false));
    if (admission.hasError()) {
      return admission;
    }
    return deliverAdmittedPayload(std::move(initialPayload), false);
  }

  folly::Expected<ObjectPublishStatus, MoQPublishError> objectPayload(
      Payload payload,
      bool finSubgroup) override {
    if (finished_) {
      return statusApiError("Payload delivered after subgroup terminal");
    }
    if (!currentObject_) {
      terminateWithError(ResetStreamErrorCode::MALFORMED_TRACK);
      return statusApiError("Payload delivered without an active object");
    }
    return deliverPayload(std::move(payload), finSubgroup);
  }

  folly::Expected<folly::Unit, MoQPublishError> endOfGroup(
      uint64_t endOfGroupObjectID) override {
    return deliverStatus(
        endOfGroupObjectID,
        ObjectStatus::END_OF_GROUP,
        /*finishSubgroup=*/true);
  }

  folly::Expected<folly::Unit, MoQPublishError> endOfTrackAndGroup(
      uint64_t endOfTrackObjectID) override {
    return deliverStatus(
        endOfTrackObjectID,
        ObjectStatus::END_OF_TRACK,
        /*finishSubgroup=*/true);
  }

  folly::Expected<folly::Unit, MoQPublishError> endOfSubgroup() override {
    if (finished_) {
      return folly::unit;
    }
    if (currentObject_) {
      terminateWithError(ResetStreamErrorCode::MALFORMED_TRACK);
      return apiError("Subgroup ended before the current object completed");
    }
    finishAtEndOfStream();
    return folly::unit;
  }

  void reset(ResetStreamErrorCode error) override {
    terminateWithError(error);
  }

 private:
  friend class StreamingObjectReceiver;

  static uint64_t payloadSize(const Payload& payload) {
    return payload ? payload->computeChainDataLength() : 0;
  }

  folly::Expected<folly::Unit, MoQPublishError> apiError(
      std::string message) const {
    return folly::makeUnexpected(
        MoQPublishError(MoQPublishError::API_ERROR, std::move(message)));
  }

  folly::Expected<ObjectPublishStatus, MoQPublishError> statusApiError(
      std::string message) const {
    return folly::makeUnexpected(
        MoQPublishError(MoQPublishError::API_ERROR, std::move(message)));
  }

  folly::Expected<folly::Unit, MoQPublishError> prepareForObject(
      uint64_t objectID,
      const char* description) {
    if (finished_) {
      return apiError(
          std::string(description) + " delivered after subgroup terminal");
    }
    if (currentObject_) {
      terminateWithError(ResetStreamErrorCode::MALFORMED_TRACK);
      return apiError(
          std::string(description) +
          " delivered while another object is active");
    }
    if (streamType_ != StreamType::FETCH_HEADER && lastObjectID_ &&
        objectID <= *lastObjectID_) {
      terminateWithError(ResetStreamErrorCode::MALFORMED_TRACK);
      return apiError("Object ID is not strictly increasing");
    }
    lastObjectID_ = objectID;
    return folly::unit;
  }

  // Announces the object described by header_ and makes its consumer current.
  folly::Expected<folly::Unit, MoQPublishError> admitObject(
      uint64_t length,
      std::optional<bool> lastInGroupValue) {
    remainingPayload_ = length;
    auto consumerResult = callback_->onObjectBegin(
        StreamingObjectContext{
            trackAlias_, header_, subgroupOptions_, lastInGroupValue});
    if (finished_) {
      if (consumerResult.hasValue() && *consumerResult) {
        (*consumerResult)
            ->onError(
                terminalError_.value_or(ResetStreamErrorCode::MALFORMED_TRACK));
      }
      return apiError("Subgroup terminated during onObjectBegin");
    }
    if (consumerResult.hasError()) {
      auto error = std::move(consumerResult.error());
      terminateWithError(resetErrorFor(error.code));
      return folly::makeUnexpected(std::move(error));
    }
    auto consumer = std::move(*consumerResult);
    if (!consumer) {
      terminateWithError(ResetStreamErrorCode::MALFORMED_TRACK);
      return apiError("onObjectBegin returned no payload consumer");
    }
    currentObject_ = std::move(consumer);
    return folly::unit;
  }

  folly::Expected<folly::Unit, MoQPublishError> deliverAdmittedPayload(
      Payload payload,
      bool finSubgroup) {
    auto result = deliverPayload(std::move(payload), finSubgroup);
    if (result.hasError()) {
      return folly::makeUnexpected(std::move(result.error()));
    }
    return folly::unit;
  }

  std::optional<bool> lastInGroup(bool finSubgroup) const {
    if (streamType_ != StreamType::FETCH_HEADER && finSubgroup &&
        subgroupOptions_ && subgroupOptions_->containsLastInGroup) {
      return true;
    }
    return std::nullopt;
  }

  StreamingObjectPayloadMetadata completionMetadata(bool finSubgroup) const {
    return StreamingObjectPayloadMetadata{
        true,
        streamType_ != StreamType::FETCH_HEADER && finSubgroup,
        lastInGroup(finSubgroup)};
  }

  folly::Expected<ObjectPublishStatus, MoQPublishError> deliverPayload(
      Payload payload,
      bool finSubgroup) {
    const auto chunkLength = payloadSize(payload);
    if (chunkLength > remainingPayload_) {
      terminateWithError(ResetStreamErrorCode::MALFORMED_TRACK);
      return statusApiError("Payload exceeds declared object length");
    }

    if (chunkLength == 0 && remainingPayload_ > 0) {
      if (finSubgroup) {
        terminateWithError(ResetStreamErrorCode::MALFORMED_TRACK);
        return statusApiError(
            "Subgroup ended before the declared object length was received");
      }
      return ObjectPublishStatus::IN_PROGRESS;
    }

    remainingPayload_ -= chunkLength;
    const bool complete = remainingPayload_ == 0;
    auto consumer = currentObject_;
    const auto metadata = complete ? completionMetadata(finSubgroup)
                                   : StreamingObjectPayloadMetadata{};
    auto result = consumer->onPayload(std::move(payload), metadata);
    if (finished_) {
      return statusApiError("Subgroup terminated during onPayload");
    }
    if (result.hasError()) {
      auto error = std::move(result.error());
      terminateWithError(resetErrorFor(error.code));
      return folly::makeUnexpected(std::move(error));
    }

    if (complete) {
      currentObject_.reset();
      if (finSubgroup) {
        finishAtEndOfStream();
      }
    } else if (finSubgroup) {
      terminateWithError(ResetStreamErrorCode::MALFORMED_TRACK);
      return statusApiError(
          "Subgroup ended before the declared object length was received");
    }

    return complete ? ObjectPublishStatus::DONE
                    : ObjectPublishStatus::IN_PROGRESS;
  }

  folly::Expected<folly::Unit, MoQPublishError>
  deliverStatus(uint64_t objectID, ObjectStatus status, bool finishSubgroup) {
    auto preparation = prepareForObject(objectID, "Object status");
    if (preparation.hasError()) {
      return preparation;
    }
    header_.id = objectID;
    header_.status = status;
    header_.extensions = noExtensions();
    header_.length = std::nullopt;
    header_.forwardingPreferenceIsDatagram = false;
    std::shared_ptr<StreamingObjectReceiver> parent;
    if (finishSubgroup) {
      finished_ = true;
      parent = std::move(parent_);
    }
    callback_->onObjectStatus(
        StreamingObjectContext{
            trackAlias_, header_, subgroupOptions_, /*lastInGroup=*/true});
    if (finishSubgroup) {
      notifyParentFinished(std::move(parent));
    }
    return folly::unit;
  }

  // A FETCH has no subgroups; its terminal is onAllDataReceived alone.
  void finishAtEndOfStream() {
    auto context = subgroupContext();
    finished_ = true;
    auto parent = std::move(parent_);
    if (streamType_ != StreamType::FETCH_HEADER) {
      callback_->onEndOfStream(context);
    }
    notifyParentFinished(std::move(parent));
  }

  void failCurrentObject(ResetStreamErrorCode error) {
    auto consumer = std::move(currentObject_);
    remainingPayload_ = 0;
    if (consumer) {
      consumer->onError(error);
    }
  }

  static ResetStreamErrorCode resetErrorFor(MoQPublishError::Code error) {
    if (error == MoQPublishError::CANCELLED) {
      return ResetStreamErrorCode::CANCELLED;
    }
    if (error == MoQPublishError::TOO_FAR_BEHIND) {
      return ResetStreamErrorCode::TOO_FAR_BEHIND;
    }
    if (error == MoQPublishError::MALFORMED_TRACK) {
      return ResetStreamErrorCode::MALFORMED_TRACK;
    }
    return ResetStreamErrorCode::INTERNAL_ERROR;
  }

  void terminateWithError(ResetStreamErrorCode error) {
    if (finished_) {
      return;
    }
    finished_ = true;
    terminalError_ = error;
    failCurrentObject(error);
    callback_->onError(subgroupContext(), error);
    notifyParentFinished();
  }

  StreamingSubgroupContext subgroupContext() const {
    return StreamingSubgroupContext{
        trackAlias_,
        header_.group,
        header_.subgroup,
        header_.priority.value_or(kDefaultPriority),
        subgroupOptions_};
  }

  void notifyParentFinished();
  void notifyParentFinished(std::shared_ptr<StreamingObjectReceiver> parent);

  std::shared_ptr<StreamingObjectReceiverCallback> callback_;
  std::shared_ptr<StreamingObjectReceiver> parent_;
  StreamType streamType_;
  ObjectHeader header_;
  std::optional<TrackAlias> trackAlias_;
  std::optional<BeginSubgroupOptions> subgroupOptions_;
  std::shared_ptr<StreamingObjectPayloadConsumer> currentObject_;
  std::optional<uint64_t> lastObjectID_;
  std::optional<ResetStreamErrorCode> terminalError_;
  uint64_t remainingPayload_{0};
  bool finished_{false};
  bool forwardingPreferenceIsDatagram_{false};
};

class StreamingObjectReceiver
    : public TrackConsumer,
      public FetchConsumer,
      public std::enable_shared_from_this<StreamingObjectReceiver> {
 public:
  enum Type { SUBSCRIBE, FETCH };

  // fetchGroupOrder is the FETCH request's group order; it decides which
  // direction FETCH groups must advance.  Ignored for SUBSCRIBE.
  explicit StreamingObjectReceiver(
      Type type,
      std::shared_ptr<StreamingObjectReceiverCallback> callback,
      GroupOrder fetchGroupOrder = GroupOrder::OldestFirst)
      : callback_(std::move(callback)),
        fetchGroupOrder_(
            fetchGroupOrder == GroupOrder::Default ? GroupOrder::OldestFirst
                                                   : fetchGroupOrder) {
    if (type == FETCH) {
      // FetchConsumer does not carry the wire priority, so FETCH objects
      // report kDefaultPriority.
      fetchReceiver_ = std::make_shared<StreamingObjectSubgroupReceiver>(
          callback_, trackAlias_);
      fetchReceiver_->markFetchStream();
    }
  }

  void goaway(Goaway goaway) override {
    callback_->onGoaway(std::move(goaway));
  }

  folly::Expected<folly::Unit, MoQPublishError> setTrackAlias(
      TrackAlias alias) override {
    trackAlias_ = alias;
    if (fetchReceiver_) {
      fetchReceiver_->setTrackAlias(alias);
    }
    return folly::unit;
  }

  folly::Expected<std::shared_ptr<SubgroupConsumer>, MoQPublishError>
  beginSubgroup(
      uint64_t groupID,
      uint64_t subgroupID,
      Priority priority,
      BeginSubgroupOptions options = {}) override {
    if (publishDoneDelivered_) {
      return folly::makeUnexpected(MoQPublishError(
          MoQPublishError::API_ERROR,
          "Subgroup delivered after subscription terminal"));
    }
    auto receiver = std::make_shared<StreamingObjectSubgroupReceiver>(
        callback_, trackAlias_, groupID, subgroupID, priority, options);
    receiver->setParent(shared_from_this());
    ++openSubgroups_;
    return receiver;
  }

  folly::Expected<folly::SemiFuture<folly::Unit>, MoQPublishError>
  awaitStreamCredit() override {
    return folly::makeSemiFuture();
  }

  folly::Expected<folly::Unit, MoQPublishError> objectStream(
      const ObjectHeader& header,
      Payload payload,
      bool lastInGroup = false) override {
    if (publishDoneDelivered_) {
      return apiError("Object stream delivered after subscription terminal");
    }
    return deliverCompleteObject(
        header, std::move(payload), lastInGroup, /*datagram=*/false);
  }

  folly::Expected<folly::Unit, MoQPublishError> datagram(
      const ObjectHeader& header,
      Payload payload,
      bool lastInGroup = false) override {
    if (publishDoneDelivered_) {
      return apiError("Datagram delivered after subscription terminal");
    }
    auto deliveredHeader = header;
    deliveredHeader.forwardingPreferenceIsDatagram = true;
    return deliverCompleteObject(
        std::move(deliveredHeader),
        std::move(payload),
        lastInGroup,
        /*datagram=*/true);
  }

  folly::Expected<folly::Unit, MoQPublishError> publishDone(
      PublishDone done) override {
    if (publishDoneDelivered_) {
      return apiError("PUBLISH_DONE delivered more than once");
    }
    publishDoneDelivered_ = true;
    callback_->onPublishDone(std::move(done));
    maybeFireAllDataReceived();
    return folly::unit;
  }

  folly::Expected<folly::Unit, MoQPublishError> object(
      uint64_t groupID,
      uint64_t subgroupID,
      uint64_t objectID,
      Payload payload,
      Extensions extensions,
      bool finFetch,
      bool forwardingPreferenceIsDatagram = false) override {
    auto ordering = prepareFetchLocation(groupID, subgroupID, objectID);
    if (ordering.hasError()) {
      return ordering;
    }
    fetchReceiver_->setForwardingPreferenceIsDatagram(
        forwardingPreferenceIsDatagram);
    auto result = fetchReceiver_->object(
        objectID, std::move(payload), std::move(extensions), finFetch);
    return finishFetchIfRequested(std::move(result), finFetch);
  }

  folly::Expected<folly::Unit, MoQPublishError> beginObject(
      uint64_t groupID,
      uint64_t subgroupID,
      uint64_t objectID,
      uint64_t length,
      Payload initialPayload,
      Extensions extensions) override {
    auto ordering = prepareFetchLocation(groupID, subgroupID, objectID);
    if (ordering.hasError()) {
      return ordering;
    }
    auto result = fetchReceiver_->beginObject(
        objectID, length, std::move(initialPayload), std::move(extensions));
    if (result.hasError()) {
      markFetchFailed();
    }
    return result;
  }

  folly::Expected<ObjectPublishStatus, MoQPublishError> objectPayload(
      Payload payload,
      bool finFetch) override {
    if (!fetchReceiver_) {
      return fetchStatusApiError(
          "FETCH payload delivered to SUBSCRIBE receiver");
    }
    if (fetchTerminal_) {
      return fetchStatusApiError(
          fetchFailed_ ? "FETCH already failed"
                       : "Payload delivered after FETCH terminal");
    }
    auto result = fetchReceiver_->objectPayload(std::move(payload), finFetch);
    if (result.hasError()) {
      markFetchFailed();
    } else if (finFetch && result.value() == ObjectPublishStatus::DONE) {
      finishFetch();
    }
    return result;
  }

  folly::Expected<folly::Unit, MoQPublishError> endOfGroup(
      uint64_t groupID,
      uint64_t subgroupID,
      uint64_t objectID,
      bool finFetch) override {
    return deliverFetchStatus(
        groupID, subgroupID, objectID, ObjectStatus::END_OF_GROUP, finFetch);
  }

  folly::Expected<folly::Unit, MoQPublishError> endOfTrackAndGroup(
      uint64_t groupID,
      uint64_t subgroupID,
      uint64_t objectID) override {
    return deliverFetchStatus(
        groupID,
        subgroupID,
        objectID,
        ObjectStatus::END_OF_TRACK,
        /*finFetch=*/true);
  }

  folly::Expected<folly::Unit, MoQPublishError> endOfFetch() override {
    if (!fetchReceiver_) {
      return apiError("FETCH terminal delivered to SUBSCRIBE receiver");
    }
    if (fetchTerminal_) {
      return fetchFailed_
          ? apiError("FETCH already failed")
          : folly::Expected<folly::Unit, MoQPublishError>(folly::unit);
    }
    fetchTerminal_ = true;
    auto result = fetchReceiver_->endOfSubgroup();
    if (result.hasError()) {
      fetchFailed_ = true;
      return result;
    }
    fireAllDataReceived();
    return result;
  }

  folly::Expected<folly::Unit, MoQPublishError> endOfUnknownRange(
      uint64_t groupID,
      uint64_t objectID,
      bool finFetch) override {
    if (!fetchReceiver_) {
      return apiError("FETCH unknown range delivered to SUBSCRIBE receiver");
    }
    auto ordering = prepareFetchLocation(
        groupID, fetchReceiver_->subgroupContext().subgroupID, objectID);
    if (ordering.hasError()) {
      return ordering;
    }
    if (fetchReceiver_->hasActiveObject()) {
      return failFetch("Unknown range delivered while an object is active");
    }
    callback_->onUnknownRange(
        StreamingUnknownRange{trackAlias_, groupID, objectID});
    if (finFetch) {
      finishFetch();
    }
    return folly::unit;
  }

  void reset(ResetStreamErrorCode error) override {
    // MoQSession resets only FETCH consumers.
    if (!fetchReceiver_) {
      return;
    }
    if (fetchTerminal_) {
      return;
    }
    fetchFailed_ = true;
    fetchTerminal_ = true;
    fetchReceiver_->reset(error);
  }

  folly::Expected<folly::SemiFuture<uint64_t>, MoQPublishError>
  awaitReadyToConsume() override {
    return folly::makeSemiFuture<uint64_t>(0);
  }

  void onSubgroupFinished() {
    if (openSubgroups_ > 0) {
      --openSubgroups_;
    }
    maybeFireAllDataReceived();
  }

 private:
  static uint64_t payloadSize(const Payload& payload) {
    return payload ? payload->computeChainDataLength() : 0;
  }

  folly::Expected<folly::Unit, MoQPublishError> apiError(
      std::string message) const {
    return folly::makeUnexpected(
        MoQPublishError(MoQPublishError::API_ERROR, std::move(message)));
  }

  folly::Expected<ObjectPublishStatus, MoQPublishError> fetchStatusApiError(
      std::string message) const {
    return folly::makeUnexpected(
        MoQPublishError(MoQPublishError::API_ERROR, std::move(message)));
  }

  folly::Expected<folly::Unit, MoQPublishError> prepareFetchLocation(
      uint64_t groupID,
      uint64_t subgroupID,
      uint64_t objectID) {
    if (!fetchReceiver_) {
      return apiError("FETCH object delivered to SUBSCRIBE receiver");
    }
    if (fetchTerminal_) {
      return apiError(
          fetchFailed_ ? "FETCH already failed"
                       : "Object delivered after FETCH terminal");
    }
    fetchReceiver_->setFetchGroupAndSubgroup(groupID, subgroupID);
    if (lastFetchGroup_ && !fetchLocationAdvances(groupID, objectID)) {
      return failFetch("FETCH locations are out of order");
    }
    lastFetchGroup_ = groupID;
    lastFetchObject_ = objectID;
    return folly::unit;
  }

  bool fetchLocationAdvances(uint64_t groupID, uint64_t objectID) const {
    if (groupID == *lastFetchGroup_) {
      return objectID > *lastFetchObject_;
    }
    return fetchGroupOrder_ == GroupOrder::NewestFirst
        ? groupID < *lastFetchGroup_
        : groupID > *lastFetchGroup_;
  }

  folly::Expected<folly::Unit, MoQPublishError> failFetch(std::string message) {
    if (!fetchTerminal_) {
      fetchFailed_ = true;
      fetchTerminal_ = true;
      fetchReceiver_->reset(ResetStreamErrorCode::MALFORMED_TRACK);
    }
    return apiError(std::move(message));
  }

  void markFetchFailed() {
    fetchFailed_ = true;
    fetchTerminal_ = true;
  }

  folly::Expected<folly::Unit, MoQPublishError> deliverFetchStatus(
      uint64_t groupID,
      uint64_t subgroupID,
      uint64_t objectID,
      ObjectStatus status,
      bool finFetch) {
    auto ordering = prepareFetchLocation(groupID, subgroupID, objectID);
    if (ordering.hasError()) {
      return ordering;
    }
    if (finFetch) {
      fetchTerminal_ = true;
    }
    auto result = fetchReceiver_->deliverStatus(objectID, status, finFetch);
    if (result.hasError()) {
      markFetchFailed();
    } else if (finFetch) {
      fireAllDataReceived();
    }
    return result;
  }

  folly::Expected<folly::Unit, MoQPublishError> finishFetchIfRequested(
      folly::Expected<folly::Unit, MoQPublishError> result,
      bool finFetch) {
    if (result.hasError()) {
      markFetchFailed();
      return result;
    }
    if (finFetch) {
      finishFetch();
    }
    return result;
  }

  void finishFetch() {
    if (fetchTerminal_) {
      return;
    }
    fetchTerminal_ = true;
    fetchReceiver_->markFinished();
    fireAllDataReceived();
  }

  folly::Expected<folly::Unit, MoQPublishError> deliverCompleteObject(
      ObjectHeader header,
      Payload payload,
      bool lastInGroup,
      bool datagram) {
    if (header.status != ObjectStatus::NORMAL) {
      if (payloadSize(payload) != 0 || (header.length && *header.length != 0)) {
        return apiError("Status object must not contain payload");
      }
      callback_->onObjectStatus(
          StreamingObjectContext{
              trackAlias_, std::move(header), std::nullopt, lastInGroup});
      return folly::unit;
    }

    const auto length = payloadSize(payload);
    if (!header.length || *header.length != length) {
      return apiError("Payload does not match declared object length");
    }

    auto consumerResult = callback_->onObjectBegin(
        StreamingObjectContext{trackAlias_, header, std::nullopt, lastInGroup});
    if (consumerResult.hasError()) {
      auto error = std::move(consumerResult.error());
      failCompleteObject(header, nullptr, error.code, datagram);
      return folly::makeUnexpected(std::move(error));
    }
    auto consumer = std::move(*consumerResult);
    if (!consumer) {
      failCompleteObject(
          header, nullptr, MoQPublishError::MALFORMED_TRACK, datagram);
      return apiError("onObjectBegin returned no payload consumer");
    }
    if (publishDoneDelivered_) {
      consumer->onError(ResetStreamErrorCode::CANCELLED);
      return apiError("Subscription terminated during onObjectBegin");
    }
    auto result = consumer->onPayload(
        std::move(payload),
        StreamingObjectPayloadMetadata{
            true, /*endOfSubgroup=*/false, lastInGroup});
    if (result.hasError()) {
      auto error = std::move(result.error());
      failCompleteObject(header, std::move(consumer), error.code, datagram);
      return folly::makeUnexpected(std::move(error));
    }
    return folly::unit;
  }

  void failCompleteObject(
      const ObjectHeader& header,
      std::shared_ptr<StreamingObjectPayloadConsumer> consumer,
      MoQPublishError::Code error,
      bool datagram) {
    const auto resetError =
        StreamingObjectSubgroupReceiver::resetErrorFor(error);
    if (consumer) {
      consumer->onError(resetError);
    }
    if (datagram || publishDoneDelivered_) {
      return;
    }
    callback_->onError(
        StreamingSubgroupContext{
            trackAlias_,
            header.group,
            header.subgroup,
            header.priority.value_or(kDefaultPriority),
            std::nullopt},
        resetError);
  }

  void maybeFireAllDataReceived() {
    if (publishDoneDelivered_ && openSubgroups_ == 0) {
      fireAllDataReceived();
    }
  }

  void fireAllDataReceived() {
    if (!allDataCallbackSent_) {
      allDataCallbackSent_ = true;
      callback_->onAllDataReceived();
    }
  }

  std::shared_ptr<StreamingObjectReceiverCallback> callback_;
  GroupOrder fetchGroupOrder_;
  std::shared_ptr<StreamingObjectSubgroupReceiver> fetchReceiver_;
  std::optional<TrackAlias> trackAlias_;
  std::optional<uint64_t> lastFetchGroup_;
  std::optional<uint64_t> lastFetchObject_;
  size_t openSubgroups_{0};
  bool publishDoneDelivered_{false};
  bool allDataCallbackSent_{false};
  bool fetchTerminal_{false};
  bool fetchFailed_{false};
};

inline void StreamingObjectSubgroupReceiver::notifyParentFinished() {
  finished_ = true;
  notifyParentFinished(std::move(parent_));
}

inline void StreamingObjectSubgroupReceiver::notifyParentFinished(
    std::shared_ptr<StreamingObjectReceiver> parent) {
  if (parent) {
    parent->onSubgroupFinished();
  }
}

} // namespace moxygen

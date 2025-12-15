/*
 *  Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 *  This source code is licensed under the MIT license found in the LICENSE
 *  file in the root directory of this source tree.
 *
 */

#pragma once

#include <moxygen/MoQConsumers.h>

namespace moxygen {

class ObjectReceiverCallback {
 public:
  virtual ~ObjectReceiverCallback() = default;
  enum class FlowControlState { BLOCKED, UNBLOCKED };
  virtual FlowControlState onObject(
      folly::Optional<TrackAlias> trackAlias,
      const ObjectHeader& objHeader,
      Payload payload) = 0;
  virtual void onObjectStatus(
      folly::Optional<TrackAlias> trackAlias,
      const ObjectHeader& objHeader) = 0;
  virtual void onEndOfStream() = 0;
  virtual void onError(ResetStreamErrorCode) = 0;
  virtual void onSubscribeDone(SubscribeDone done) = 0;
  // Called when SUBSCRIBE_DONE has arrived AND all outstanding subgroup
  // streams have closed. Only fires for subscriptions, not fetches.
  virtual void onAllDataReceived() {}
};

class ObjectReceiver;

class ObjectSubgroupReceiver : public SubgroupConsumer {
  std::shared_ptr<ObjectReceiverCallback> callback_{nullptr};
  std::shared_ptr<ObjectReceiver> parent_{nullptr};
  StreamType streamType_;
  ObjectHeader header_;
  folly::IOBufQueue payload_{folly::IOBufQueue::cacheChainLength()};
  folly::Optional<TrackAlias> trackAlias_;
  bool finished_{false};

 public:
  explicit ObjectSubgroupReceiver(
      std::shared_ptr<ObjectReceiverCallback> callback,
      folly::Optional<TrackAlias> trackAlias = folly::none,
      uint64_t groupID = 0,
      uint64_t subgroupID = 0,
      uint8_t priority = 0)
      : callback_(callback),
        streamType_(StreamType::SUBGROUP_HEADER_SG),
        header_(groupID, subgroupID, 0, priority),
        trackAlias_(trackAlias) {}

  void setParent(std::shared_ptr<ObjectReceiver> parent) {
    parent_ = std::move(parent);
  }

  void setFetchGroupAndSubgroup(uint64_t groupID, uint64_t subgroupID) {
    streamType_ = StreamType::FETCH_HEADER;
    header_.group = groupID;
    header_.subgroup = subgroupID;
  }

  folly::Expected<folly::Unit, MoQPublishError> object(
      uint64_t objectID,
      Payload payload,
      Extensions ext,
      bool finSubgroup) override {
    header_.id = objectID;
    header_.status = ObjectStatus::NORMAL;
    header_.extensions = std::move(ext);
    auto fcState =
        callback_->onObject(trackAlias_, header_, std::move(payload));
    if (finSubgroup) {
      notifyParentFinished();
    }
    if (fcState == ObjectReceiverCallback::FlowControlState::BLOCKED) {
      if (streamType_ == StreamType::FETCH_HEADER) {
        return folly::makeUnexpected(
            MoQPublishError(MoQPublishError::BLOCKED, "blocked"));
      } else {
        XLOG(WARN) << "ObjectReceiverCallback returned BLOCKED for Subscribe";
      }
    }
    return folly::unit;
  }

  folly::Expected<folly::Unit, MoQPublishError> objectNotExists(
      uint64_t objectID,
      bool /*finSubgroup*/) override {
    header_.id = objectID;
    header_.status = ObjectStatus::OBJECT_NOT_EXIST;
    header_.extensions = noExtensions();
    callback_->onObjectStatus(trackAlias_, header_);
    return folly::unit;
  }

  folly::Expected<folly::Unit, MoQPublishError> beginObject(
      uint64_t objectID,
      uint64_t length,
      Payload initialPayload,
      Extensions ext) override {
    header_.id = objectID;
    header_.length = length;
    header_.status = ObjectStatus::NORMAL;
    header_.extensions = std::move(ext);
    objectPayload(std::move(initialPayload), false);
    return folly::unit;
  }

  folly::Expected<ObjectPublishStatus, MoQPublishError> objectPayload(
      Payload payload,
      bool /*finSubgroup*/) override {
    // TODO: add common component for state verification
    payload_.append(std::move(payload));
    if (payload_.chainLength() == header_.length) {
      auto fcState = callback_->onObject(trackAlias_, header_, payload_.move());
      if (fcState == ObjectReceiverCallback::FlowControlState::BLOCKED) {
        // Is it bad that we can't return DONE here?
        if (streamType_ == StreamType::FETCH_HEADER) {
          return folly::makeUnexpected(
              MoQPublishError(MoQPublishError::BLOCKED, "blocked"));
        } else {
          XLOG(WARN) << "ObjectReceiverCallback returned BLOCKED for Subscribe";
        }
      }
      return ObjectPublishStatus::DONE;
    }
    return ObjectPublishStatus::IN_PROGRESS;
  }

  folly::Expected<folly::Unit, MoQPublishError> endOfGroup(
      uint64_t endOfGroupObjectID) override {
    header_.id = endOfGroupObjectID;
    header_.status = ObjectStatus::END_OF_GROUP;
    header_.extensions = noExtensions();
    callback_->onObjectStatus(trackAlias_, header_);
    notifyParentFinished();
    return folly::unit;
  }

  folly::Expected<folly::Unit, MoQPublishError> endOfTrackAndGroup(
      uint64_t endOfTrackObjectID) override {
    header_.id = endOfTrackObjectID;
    header_.status = ObjectStatus::END_OF_TRACK;
    header_.extensions = noExtensions();
    callback_->onObjectStatus(trackAlias_, header_);
    notifyParentFinished();
    return folly::unit;
  }

  folly::Expected<folly::Unit, MoQPublishError> endOfSubgroup() override {
    callback_->onEndOfStream();
    notifyParentFinished();
    return folly::unit;
  }

  void reset(ResetStreamErrorCode error) override {
    callback_->onError(error);
    notifyParentFinished();
  }

 private:
  void notifyParentFinished();
};

class ObjectReceiver : public TrackConsumer,
                       public FetchConsumer,
                       public std::enable_shared_from_this<ObjectReceiver> {
  std::shared_ptr<ObjectReceiverCallback> callback_{nullptr};
  folly::Optional<ObjectSubgroupReceiver> fetchPublisher_;
  folly::Optional<TrackAlias> trackAlias_;
  // Tracking for onAllDataReceived callback (subscription mode only)
  size_t openSubgroups_{0};
  bool subscribeDoneDelivered_{false};
  bool allDataCallbackSent_{false};

 public:
  enum Type { SUBSCRIBE, FETCH };
  explicit ObjectReceiver(
      Type t,
      std::shared_ptr<ObjectReceiverCallback> callback)
      : callback_(callback) {
    if (t == FETCH) {
      fetchPublisher_.emplace(callback, trackAlias_);
    }
  }

  folly::Expected<folly::Unit, MoQPublishError> setTrackAlias(
      TrackAlias alias) override {
    trackAlias_ = alias;
    return folly::unit;
  }

  folly::Expected<std::shared_ptr<SubgroupConsumer>, MoQPublishError>
  beginSubgroup(uint64_t groupID, uint64_t subgroupID, Priority priority)
      override {
    ++openSubgroups_;
    auto receiver = std::make_shared<ObjectSubgroupReceiver>(
        callback_, trackAlias_, groupID, subgroupID, priority);
    receiver->setParent(shared_from_this());
    return receiver;
  }

  // Called when a subgroup stream finishes (via endOfSubgroup or reset)
  void onSubgroupFinished() {
    if (openSubgroups_ > 0) {
      --openSubgroups_;
    }
    maybeFireAllDataReceived();
  }

  // Fire onAllDataReceived callback once when both conditions are met:
  // 1. SUBSCRIBE_DONE has been received
  // 2. All subgroup streams have closed
  void maybeFireAllDataReceived() {
    if (!allDataCallbackSent_ && subscribeDoneDelivered_ &&
        openSubgroups_ == 0) {
      allDataCallbackSent_ = true;
      callback_->onAllDataReceived();
    }
  }

  folly::Expected<folly::SemiFuture<folly::Unit>, MoQPublishError>
  awaitStreamCredit() override {
    return folly::makeSemiFuture();
  }

  folly::Expected<folly::Unit, MoQPublishError> objectStream(
      const ObjectHeader& header,
      Payload payload) override {
    auto fcState = callback_->onObject(trackAlias_, header, std::move(payload));
    if (fcState == ObjectReceiverCallback::FlowControlState::BLOCKED) {
      if (fetchPublisher_) {
        return folly::makeUnexpected(
            MoQPublishError(MoQPublishError::BLOCKED, "blocked"));
      } else {
        XLOG(WARN) << "ObjectReceiverCallback returned BLOCKED for Subscribe";
      }
    }
    return folly::unit;
  }

  folly::Expected<folly::Unit, MoQPublishError>
  groupNotExists(uint64_t groupID, uint64_t subgroup, Priority pri) override {
    callback_->onObjectStatus(
        trackAlias_,
        ObjectHeader(
            groupID,
            subgroup,
            0,
            pri,
            ObjectStatus::END_OF_GROUP,
            noExtensions()));
    return folly::unit;
  }

  folly::Expected<folly::Unit, MoQPublishError> datagram(
      const ObjectHeader& header,
      Payload payload) override {
    (void)callback_->onObject(trackAlias_, header, std::move(payload));
    return folly::unit;
  }

  folly::Expected<folly::Unit, MoQPublishError> subscribeDone(
      SubscribeDone subDone) override {
    callback_->onSubscribeDone(std::move(subDone));
    subscribeDoneDelivered_ = true;
    maybeFireAllDataReceived();
    return folly::unit;
  }

  folly::Expected<folly::Unit, MoQPublishError> object(
      uint64_t groupID,
      uint64_t subgroupID,
      uint64_t objectID,
      Payload payload,
      Extensions extensions,
      bool finFetch) override {
    fetchPublisher_->setFetchGroupAndSubgroup(groupID, subgroupID);
    return fetchPublisher_->object(
        objectID, std::move(payload), std::move(extensions), finFetch);
  }

  folly::Expected<folly::Unit, MoQPublishError> objectNotExists(
      uint64_t groupID,
      uint64_t subgroupID,
      uint64_t objectID,
      bool finFetch) override {
    fetchPublisher_->setFetchGroupAndSubgroup(groupID, subgroupID);
    return fetchPublisher_->objectNotExists(objectID, finFetch);
  }

  folly::Expected<folly::Unit, MoQPublishError> groupNotExists(
      uint64_t groupID,
      uint64_t subgroupID,
      bool /*finFetch*/) override {
    return groupNotExists(groupID, subgroupID, Priority(0));
  }

  folly::Expected<folly::Unit, MoQPublishError> beginObject(
      uint64_t groupID,
      uint64_t subgroupID,
      uint64_t objectID,
      uint64_t length,
      Payload initialPayload,
      Extensions extensions) override {
    fetchPublisher_->setFetchGroupAndSubgroup(groupID, subgroupID);
    return fetchPublisher_->beginObject(
        objectID, length, std::move(initialPayload), std::move(extensions));
  }

  folly::Expected<ObjectPublishStatus, MoQPublishError> objectPayload(
      Payload payload,
      bool finSubgroup) override {
    return fetchPublisher_->objectPayload(std::move(payload), finSubgroup);
  }

  folly::Expected<folly::Unit, MoQPublishError> endOfGroup(
      uint64_t groupID,
      uint64_t subgroupID,
      uint64_t objectID,
      bool /*finFetch*/) override {
    fetchPublisher_->setFetchGroupAndSubgroup(groupID, subgroupID);
    return fetchPublisher_->endOfGroup(objectID);
  }

  folly::Expected<folly::Unit, MoQPublishError> endOfTrackAndGroup(
      uint64_t groupID,
      uint64_t subgroupID,
      uint64_t objectID) override {
    fetchPublisher_->setFetchGroupAndSubgroup(groupID, subgroupID);
    return fetchPublisher_->endOfTrackAndGroup(objectID);
  }

  folly::Expected<folly::Unit, MoQPublishError> endOfFetch() override {
    return fetchPublisher_->endOfSubgroup();
  }

  void reset(ResetStreamErrorCode error) override {
    return fetchPublisher_->reset(error);
  }

  folly::Expected<folly::SemiFuture<uint64_t>, MoQPublishError>
  awaitReadyToConsume() override {
    // TODO: Consider extending ObjectReceiverCallback with a mechanism
    // to trigger backpressure here.  For now, FETCH consumers that want
    // actual backpressure need to implement FetchConsumer directly.
    return folly::makeSemiFuture<uint64_t>(0);
  }
};

// Definition of ObjectSubgroupReceiver::notifyParentFinished() - needs full
// ObjectReceiver definition
inline void ObjectSubgroupReceiver::notifyParentFinished() {
  if (parent_ && !finished_) {
    finished_ = true;
    parent_->onSubgroupFinished();
  }
}

} // namespace moxygen

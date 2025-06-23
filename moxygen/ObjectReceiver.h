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
      const ObjectHeader& objHeader,
      Payload payload) = 0;
  virtual void onObjectStatus(const ObjectHeader& objHeader) = 0;
  virtual void onEndOfStream() = 0;
  virtual void onError(ResetStreamErrorCode) = 0;
  virtual void onSubscribeDone(SubscribeDone done) = 0;
};

class ObjectSubgroupReceiver : public SubgroupConsumer {
  std::shared_ptr<ObjectReceiverCallback> callback_{nullptr};
  StreamType streamType_;
  ObjectHeader header_;
  folly::IOBufQueue payload_{folly::IOBufQueue::cacheChainLength()};

 public:
  explicit ObjectSubgroupReceiver(
      std::shared_ptr<ObjectReceiverCallback> callback,
      uint64_t groupID = 0,
      uint64_t subgroupID = 0,
      uint8_t priority = 0)
      : callback_(callback),
        streamType_(StreamType::SUBGROUP_HEADER),
        header_(TrackAlias(0), groupID, subgroupID, 0, priority) {}

  void setFetchGroupAndSubgroup(uint64_t groupID, uint64_t subgroupID) {
    streamType_ = StreamType::FETCH_HEADER;
    header_.group = groupID;
    header_.subgroup = subgroupID;
  }

  folly::Expected<folly::Unit, MoQPublishError>
  object(uint64_t objectID, Payload payload, Extensions ext, bool) override {
    header_.id = objectID;
    header_.status = ObjectStatus::NORMAL;
    header_.extensions = std::move(ext);
    auto fcState = callback_->onObject(header_, std::move(payload));
    if (fcState == ObjectReceiverCallback::FlowControlState::BLOCKED) {
      if (streamType_ == StreamType::FETCH_HEADER) {
        return folly::makeUnexpected(MoQPublishError(MoQPublishError::BLOCKED));
      } else {
        XLOG(WARN) << "ObjectReceiverCallback returned BLOCKED for Subscribe";
      }
    }
    return folly::unit;
  }

  folly::Expected<folly::Unit, MoQPublishError> objectNotExists(
      uint64_t objectID,
      Extensions ext,
      bool /*finSubgroup*/) override {
    header_.id = objectID;
    header_.status = ObjectStatus::OBJECT_NOT_EXIST;
    header_.extensions = std::move(ext);
    callback_->onObjectStatus(header_);
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
      auto fcState = callback_->onObject(header_, payload_.move());
      if (fcState == ObjectReceiverCallback::FlowControlState::BLOCKED) {
        // Is it bad that we can't return DONE here?
        return folly::makeUnexpected(MoQPublishError(MoQPublishError::BLOCKED));
      }
      return ObjectPublishStatus::DONE;
    }
    return ObjectPublishStatus::IN_PROGRESS;
  }

  folly::Expected<folly::Unit, MoQPublishError> endOfGroup(
      uint64_t endOfGroupObjectID,
      Extensions ext) override {
    header_.id = endOfGroupObjectID;
    header_.status = ObjectStatus::END_OF_GROUP;
    header_.extensions = std::move(ext);
    callback_->onObjectStatus(header_);
    return folly::unit;
  }

  folly::Expected<folly::Unit, MoQPublishError> endOfTrackAndGroup(
      uint64_t endOfTrackObjectID,
      Extensions ext) override {
    header_.id = endOfTrackObjectID;
    header_.status = ObjectStatus::END_OF_TRACK;
    header_.extensions = std::move(ext);
    callback_->onObjectStatus(header_);
    return folly::unit;
  }

  folly::Expected<folly::Unit, MoQPublishError> endOfSubgroup() override {
    callback_->onEndOfStream();
    return folly::unit;
  }

  void reset(ResetStreamErrorCode error) override {
    callback_->onError(error);
  }
};

class ObjectReceiver : public TrackConsumer, public FetchConsumer {
  std::shared_ptr<ObjectReceiverCallback> callback_{nullptr};
  folly::Optional<ObjectSubgroupReceiver> fetchPublisher_;

 public:
  enum Type { SUBSCRIBE, FETCH };
  explicit ObjectReceiver(
      Type t,
      std::shared_ptr<ObjectReceiverCallback> callback)
      : callback_(callback) {
    if (t == FETCH) {
      fetchPublisher_.emplace(callback);
    }
  }

  folly::Expected<std::shared_ptr<SubgroupConsumer>, MoQPublishError>
  beginSubgroup(uint64_t groupID, uint64_t subgroupID, Priority priority)
      override {
    return std::make_shared<ObjectSubgroupReceiver>(
        callback_, groupID, subgroupID, priority);
  }

  folly::Expected<folly::SemiFuture<folly::Unit>, MoQPublishError>
  awaitStreamCredit() override {
    return folly::makeSemiFuture();
  }

  folly::Expected<folly::Unit, MoQPublishError> objectStream(
      const ObjectHeader& header,
      Payload payload) override {
    auto fcState = callback_->onObject(header, std::move(payload));
    if (fcState == ObjectReceiverCallback::FlowControlState::BLOCKED) {
      return folly::makeUnexpected(MoQPublishError(MoQPublishError::BLOCKED));
    }
    return folly::unit;
  }

  folly::Expected<folly::Unit, MoQPublishError> groupNotExists(
      uint64_t groupID,
      uint64_t subgroup,
      Priority pri,
      Extensions ext) override {
    callback_->onObjectStatus(ObjectHeader(
        TrackAlias(0),
        groupID,
        subgroup,
        0,
        pri,
        ObjectStatus::END_OF_GROUP,
        std::move(ext)));
    return folly::unit;
  }

  folly::Expected<folly::Unit, MoQPublishError> datagram(
      const ObjectHeader& header,
      Payload payload) override {
    (void)callback_->onObject(header, std::move(payload));
    return folly::unit;
  }

  folly::Expected<folly::Unit, MoQPublishError> subscribeDone(
      SubscribeDone subDone) override {
    callback_->onSubscribeDone(std::move(subDone));
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
      Extensions extensions,
      bool finFetch) override {
    fetchPublisher_->setFetchGroupAndSubgroup(groupID, subgroupID);
    return fetchPublisher_->objectNotExists(
        objectID, std::move(extensions), finFetch);
  }

  folly::Expected<folly::Unit, MoQPublishError> groupNotExists(
      uint64_t groupID,
      uint64_t subgroupID,
      Extensions extensions,
      bool /*finFetch*/) override {
    return groupNotExists(
        groupID, subgroupID, Priority(0), std::move(extensions));
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
      Extensions extensions,
      bool /*finFetch*/) override {
    fetchPublisher_->setFetchGroupAndSubgroup(groupID, subgroupID);
    return fetchPublisher_->endOfGroup(objectID, std::move(extensions));
  }

  folly::Expected<folly::Unit, MoQPublishError> endOfTrackAndGroup(
      uint64_t groupID,
      uint64_t subgroupID,
      uint64_t objectID,
      Extensions extensions) override {
    fetchPublisher_->setFetchGroupAndSubgroup(groupID, subgroupID);
    return fetchPublisher_->endOfTrackAndGroup(objectID, std::move(extensions));
  }

  folly::Expected<folly::Unit, MoQPublishError> endOfFetch() override {
    return fetchPublisher_->endOfSubgroup();
  }

  void reset(ResetStreamErrorCode error) override {
    return fetchPublisher_->reset(error);
  }

  folly::Expected<folly::SemiFuture<folly::Unit>, MoQPublishError>
  awaitReadyToConsume() override {
    // TODO: Consider extending ObjectReceiverCallback with a mechanism
    // to trigger backpressure here.  For now, FETCH consumers that want
    // actual backpressure need to implement FetchConsumer directly.
    return folly::makeSemiFuture();
  }
};

} // namespace moxygen

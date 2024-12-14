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
  ObjectReceiverCallback* callback_{nullptr};
  ObjectHeader header_;
  folly::IOBufQueue payload_{folly::IOBufQueue::cacheChainLength()};

 public:
  explicit ObjectSubgroupReceiver(
      ObjectReceiverCallback* callback,
      uint64_t groupID = 0,
      uint64_t subgroupID = 0,
      uint8_t priority = 0)
      : callback_(callback),
        header_{
            TrackAlias(0),
            groupID,
            subgroupID,
            0,
            priority,
            ForwardPreference::Subgroup,
            ObjectStatus::NORMAL,
            folly::none} {}

  void setFetchGroupAndSubgroup(uint64_t groupID, uint64_t subgroupID) {
    header_.group = groupID;
    header_.subgroup = subgroupID;
    header_.forwardPreference = ForwardPreference::Fetch;
  }

  folly::Expected<folly::Unit, MoQPublishError>
  object(uint64_t objectID, Payload payload, bool) override {
    header_.id = objectID;
    header_.status = ObjectStatus::NORMAL;
    auto fcState = callback_->onObject(header_, std::move(payload));
    if (fcState == ObjectReceiverCallback::FlowControlState::BLOCKED) {
      if (header_.forwardPreference == ForwardPreference::Fetch) {
        return folly::makeUnexpected(MoQPublishError(MoQPublishError::BLOCKED));
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
    callback_->onObjectStatus(header_);
    return folly::unit;
  }

  folly::Expected<folly::Unit, MoQPublishError> beginObject(
      uint64_t objectID,
      uint64_t length,
      Payload initialPayload) override {
    header_.id = objectID;
    header_.length = length;
    header_.status = ObjectStatus::NORMAL;
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
      uint64_t endOfGroupObjectID) override {
    header_.id = endOfGroupObjectID;
    header_.status = ObjectStatus::END_OF_GROUP;
    callback_->onObjectStatus(header_);
    return folly::unit;
  }

  folly::Expected<folly::Unit, MoQPublishError> endOfTrackAndGroup(
      uint64_t endOfTrackObjectID) override {
    header_.id = endOfTrackObjectID;
    header_.status = ObjectStatus::END_OF_TRACK_AND_GROUP;
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
  ObjectReceiverCallback* callback_{nullptr};
  folly::Optional<ObjectSubgroupReceiver> fetchPublisher_;

 public:
  enum Type { SUBSCRIBE, FETCH };
  explicit ObjectReceiver(Type t, ObjectReceiverCallback* callback)
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

  folly::Expected<folly::Unit, MoQPublishError>
  groupNotExists(uint64_t groupID, uint64_t subgroup, Priority pri) override {
    callback_->onObjectStatus(
        {TrackAlias(0),
         groupID,
         subgroup,
         0,
         pri,
         ForwardPreference::Subgroup,
         ObjectStatus::END_OF_TRACK_AND_GROUP,
         0});
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
      bool finFetch) override {
    fetchPublisher_->setFetchGroupAndSubgroup(groupID, subgroupID);
    return fetchPublisher_->object(objectID, std::move(payload), finFetch);
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
      Payload initialPayload) override {
    fetchPublisher_->setFetchGroupAndSubgroup(groupID, subgroupID);
    return fetchPublisher_->beginObject(
        objectID, length, std::move(initialPayload));
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

  folly::Expected<folly::SemiFuture<folly::Unit>, MoQPublishError>
  awaitReadyToConsume() override {
    // TODO: Consider extending ObjectReceiverCallback with a mechanism
    // to trigger backpressure here.  For now, FETCH consumers that want
    // actual backpressure need to implement FetchConsumer directly.
    return folly::makeSemiFuture();
  }
};

} // namespace moxygen

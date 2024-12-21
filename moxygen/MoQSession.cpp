/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/MoQSession.h"
#include <folly/coro/Collect.h>
#include <folly/futures/ThreadWheelTimekeeper.h>
#include <folly/io/async/EventBase.h>

#include <folly/logging/xlog.h>

namespace {
using namespace moxygen;
constexpr std::chrono::seconds kSetupTimeout(5);

constexpr uint32_t IdMask = 0x1FFFFF;
uint64_t groupOrder(GroupOrder groupOrder, uint64_t group) {
  uint32_t truncGroup = static_cast<uint32_t>(group) & IdMask;
  return groupOrder == GroupOrder::OldestFirst ? truncGroup
                                               : (IdMask - truncGroup);
}

uint32_t objOrder(uint64_t objId) {
  return static_cast<uint32_t>(objId) & IdMask;
}

uint64_t order(
    uint64_t group,
    uint64_t object,
    uint8_t priority,
    uint8_t pubPri,
    GroupOrder pubGroupOrder) {
  // 6 reserved bits | 58 bit order
  // 6 reserved | 8 sub pri | 8 pub pri | 21 group order | 21 obj order
  return (
      (uint64_t(pubPri) << 50) | (uint64_t(priority) << 42) |
      (groupOrder(pubGroupOrder, group) << 21) | objOrder(object));
}

// Helper classes for publishing

// StreamPublisherImpl is for publishing to a single stream, either a Subgroup
// or a Fetch response.  It's of course illegal to mix-and-match the APIs, but
// the object is only handed to the application as either a SubgroupConsumer
// or a FetchConsumer
class StreamPublisherImpl : public SubgroupConsumer,
                            public FetchConsumer,
                            public folly::CancellationCallback {
 public:
  StreamPublisherImpl() = delete;

  // Fetch constructor
  StreamPublisherImpl(
      MoQSession::PublisherImpl* publisher,
      proxygen::WebTransport::StreamWriteHandle* writeHandle);

  // Subscribe constructor
  StreamPublisherImpl(
      MoQSession::PublisherImpl* publisher,
      proxygen::WebTransport::StreamWriteHandle* writeHandle,
      TrackAlias alias,
      uint64_t groupID,
      uint64_t subgroupID);

  // SubgroupConsumer overrides
  // Note where the interface uses finSubgroup, this class uses finStream,
  // since it is used for fetch and subgroups
  folly::Expected<folly::Unit, MoQPublishError>
  object(uint64_t objectID, Payload payload, bool finStream) override;
  folly::Expected<folly::Unit, MoQPublishError> objectNotExists(
      uint64_t objectID,
      bool finStream) override;
  folly::Expected<folly::Unit, MoQPublishError> beginObject(
      uint64_t objectId,
      uint64_t length,
      Payload initialPayload) override;
  folly::Expected<ObjectPublishStatus, MoQPublishError> objectPayload(
      Payload payload,
      bool finStream) override;
  folly::Expected<folly::Unit, MoQPublishError> endOfGroup(
      uint64_t endOfGroupObjectId) override;
  folly::Expected<folly::Unit, MoQPublishError> endOfTrackAndGroup(
      uint64_t endOfTrackObjectId) override;
  folly::Expected<folly::Unit, MoQPublishError> endOfSubgroup() override;
  void reset(ResetStreamErrorCode error) override;

  // FetchConsumer overrides
  folly::Expected<folly::Unit, MoQPublishError> object(
      uint64_t groupID,
      uint64_t subgroupID,
      uint64_t objectID,
      Payload payload,
      bool finFetch) override {
    if (!setGroupAndSubgroup(groupID, subgroupID)) {
      return folly::makeUnexpected(
          MoQPublishError(MoQPublishError::API_ERROR, "Group moved back"));
    }
    header_.status = ObjectStatus::NORMAL;
    return object(objectID, std::move(payload), finFetch);
  }

  folly::Expected<folly::Unit, MoQPublishError> objectNotExists(
      uint64_t groupID,
      uint64_t subgroupID,
      uint64_t objectID,
      bool finFetch) override {
    if (!setGroupAndSubgroup(groupID, subgroupID)) {
      return folly::makeUnexpected(
          MoQPublishError(MoQPublishError::API_ERROR, "Group moved back"));
    }
    return objectNotExists(objectID, finFetch);
  }

  folly::Expected<folly::Unit, MoQPublishError> groupNotExists(
      uint64_t groupID,
      uint64_t subgroupID,
      bool finFetch) override {
    if (!setGroupAndSubgroup(groupID, subgroupID)) {
      return folly::makeUnexpected(
          MoQPublishError(MoQPublishError::API_ERROR, "Group moved back"));
    }
    return publishStatus(0, ObjectStatus::GROUP_NOT_EXIST, finFetch);
  }

  folly::Expected<folly::Unit, MoQPublishError> beginObject(
      uint64_t groupID,
      uint64_t subgroupID,
      uint64_t objectID,
      uint64_t length,
      Payload initialPayload) override {
    if (!setGroupAndSubgroup(groupID, subgroupID)) {
      return folly::makeUnexpected(
          MoQPublishError(MoQPublishError::API_ERROR, "Group moved back"));
    }
    header_.status = ObjectStatus::NORMAL;
    return beginObject(objectID, length, std::move(initialPayload));
  }

  folly::Expected<folly::Unit, MoQPublishError> endOfGroup(
      uint64_t groupID,
      uint64_t subgroupID,
      uint64_t objectID,
      bool finFetch) override {
    if (!setGroupAndSubgroup(groupID, subgroupID)) {
      return folly::makeUnexpected(
          MoQPublishError(MoQPublishError::API_ERROR, "Group moved back"));
    }
    return publishStatus(objectID, ObjectStatus::END_OF_GROUP, finFetch);
  }
  folly::Expected<folly::Unit, MoQPublishError> endOfTrackAndGroup(
      uint64_t groupID,
      uint64_t subgroupID,
      uint64_t objectID) override {
    if (!setGroupAndSubgroup(groupID, subgroupID)) {
      return folly::makeUnexpected(
          MoQPublishError(MoQPublishError::API_ERROR, "Group moved back"));
    }
    return endOfTrackAndGroup(objectID);
  }
  folly::Expected<folly::Unit, MoQPublishError> endOfFetch() override {
    if (!writeHandle_) {
      return folly::makeUnexpected(
          MoQPublishError(MoQPublishError::CANCELLED, "Fetch cancelled"));
    }
    return endOfSubgroup();
  }

  folly::Expected<folly::SemiFuture<folly::Unit>, MoQPublishError>
  awaitReadyToConsume() override;

  folly::Expected<folly::Unit, MoQPublishError>
  publishStatus(uint64_t objectID, ObjectStatus status, bool finStream);

 private:
  bool setGroupAndSubgroup(uint64_t groupID, uint64_t subgroupID) {
    if (groupID < header_.group) {
      return false;
    } else if (groupID > header_.group) {
      // TODO(T211026595): reverse this check with group order
      // Fetch group advanced, reset expected object
      header_.id = std::numeric_limits<uint64_t>::max();
    }
    header_.group = groupID;
    header_.subgroup = subgroupID;
    return true;
  }

  folly::Expected<folly::Unit, MoQPublishError> validatePublish(
      uint64_t objectID);
  folly::Expected<ObjectPublishStatus, MoQPublishError>
  validateObjectPublishAndUpdateState(folly::IOBuf* payload, bool finStream);
  folly::Expected<folly::Unit, MoQPublishError> writeCurrentObject(
      uint64_t objectID,
      uint64_t length,
      Payload payload,
      bool finStream);
  folly::Expected<folly::Unit, MoQPublishError> writeToStream(bool finStream);

  void onStreamComplete();

  MoQSession::PublisherImpl* publisher_{nullptr};
  proxygen::WebTransport::StreamWriteHandle* writeHandle_{nullptr};
  ObjectHeader header_;
  folly::Optional<uint64_t> currentLengthRemaining_;
  folly::IOBufQueue writeBuf_{folly::IOBufQueue::cacheChainLength()};
};

class TrackPublisherImpl : public MoQSession::PublisherImpl,
                           public TrackConsumer {
 public:
  TrackPublisherImpl() = delete;
  TrackPublisherImpl(
      MoQSession* session,
      SubscribeID subscribeID,
      TrackAlias trackAlias,
      Priority priority,
      GroupOrder groupOrder)
      : PublisherImpl(session, subscribeID, priority, groupOrder),
        trackAlias_(trackAlias) {}

  // PublisherImpl overrides
  void onStreamComplete(const ObjectHeader& finalHeader) override;

  void reset(ResetStreamErrorCode) override {
    // TBD: reset all subgroups_?  Currently called from cleanup()
  }

  // TrackConsumer overrides
  folly::Expected<std::shared_ptr<SubgroupConsumer>, MoQPublishError>
  beginSubgroup(uint64_t groupID, uint64_t subgroupID, Priority priority)
      override;

  folly::Expected<folly::SemiFuture<folly::Unit>, MoQPublishError>
  awaitStreamCredit() override;

  folly::Expected<folly::Unit, MoQPublishError> objectStream(
      const ObjectHeader& header,
      Payload payload) override;

  folly::Expected<folly::Unit, MoQPublishError>
  groupNotExists(uint64_t groupID, uint64_t subgroup, Priority pri) override;

  folly::Expected<folly::Unit, MoQPublishError> datagram(
      const ObjectHeader& header,
      Payload payload) override;

  folly::Expected<folly::Unit, MoQPublishError> subscribeDone(
      SubscribeDone subDone) override;

 private:
  TrackAlias trackAlias_;
  folly::F14FastMap<
      std::pair<uint64_t, uint64_t>,
      std::shared_ptr<StreamPublisherImpl>>
      subgroups_;
};

class FetchPublisherImpl : public MoQSession::PublisherImpl {
 public:
  FetchPublisherImpl(
      MoQSession* session,
      SubscribeID subscribeID,
      Priority priority,
      GroupOrder groupOrder)
      : PublisherImpl(session, subscribeID, priority, groupOrder) {}

  folly::Expected<std::shared_ptr<FetchConsumer>, MoQPublishError> beginFetch(
      GroupOrder groupOrder);

  void reset(ResetStreamErrorCode error) override {
    if (streamPublisher_) {
      streamPublisher_->reset(error);
    }
  }

  void onStreamComplete(const ObjectHeader&) override {
    streamPublisher_.reset();
    PublisherImpl::fetchComplete();
  }

 private:
  std::shared_ptr<StreamPublisherImpl> streamPublisher_;
};

// TrackPublisherImpl

folly::Expected<std::shared_ptr<SubgroupConsumer>, MoQPublishError>
TrackPublisherImpl::beginSubgroup(
    uint64_t groupID,
    uint64_t subgroupID,
    Priority priority) {
  auto wt = getWebTransport();
  if (!wt) {
    XLOG(ERR) << "Trying to publish after subscribeDone";
    return folly::makeUnexpected(MoQPublishError(
        MoQPublishError::API_ERROR, "Publish after subscribeDone"));
  }
  auto stream = wt->createUniStream();
  if (!stream) {
    // failed to create a stream
    // TODO: can it fail for non-stream credit reasons? Session closing should
    // be handled above.
    XLOG(ERR) << "Failed to create uni stream tp=" << this;
    return folly::makeUnexpected(MoQPublishError(
        MoQPublishError::BLOCKED, "Failed to create uni stream."));
  }
  XLOG(DBG4) << "New stream created, id: " << stream.value()->getID()
             << " tp=" << this;
  stream.value()->setPriority(
      1, order(groupID, subgroupID, priority, priority_, groupOrder_), false);
  auto subgroupPublisher = std::make_shared<StreamPublisherImpl>(
      this, *stream, trackAlias_, groupID, subgroupID);
  // TODO: these are currently unused, but the intent might be to reset
  // open subgroups automatically from some path?
  subgroups_[{groupID, subgroupID}] = subgroupPublisher;
  return subgroupPublisher;
}

folly::Expected<folly::SemiFuture<folly::Unit>, MoQPublishError>
TrackPublisherImpl::awaitStreamCredit() {
  auto wt = getWebTransport();
  if (!wt) {
    return folly::makeUnexpected(MoQPublishError(
        MoQPublishError::API_ERROR, "awaitStreamCredit after subscribeDone"));
  }
  return wt->awaitUniStreamCredit();
}

void TrackPublisherImpl::onStreamComplete(const ObjectHeader& finalHeader) {
  subgroups_.erase({finalHeader.group, finalHeader.subgroup});
}

folly::Expected<folly::Unit, MoQPublishError> TrackPublisherImpl::objectStream(
    const ObjectHeader& objHeader,
    Payload payload) {
  XCHECK(objHeader.status == ObjectStatus::NORMAL || !payload);
  auto subgroup =
      beginSubgroup(objHeader.group, objHeader.subgroup, objHeader.priority);
  if (subgroup.hasError()) {
    return folly::makeUnexpected(std::move(subgroup.error()));
  }
  switch (objHeader.status) {
    case ObjectStatus::NORMAL:
      return subgroup.value()->object(
          objHeader.id, std::move(payload), /*finStream=*/true);
    case ObjectStatus::OBJECT_NOT_EXIST:
      return subgroup.value()->objectNotExists(
          objHeader.id, /*finStream=*/true);
    case ObjectStatus::GROUP_NOT_EXIST: {
      auto& subgroupPublisherImpl =
          static_cast<StreamPublisherImpl&>(*subgroup.value());
      return subgroupPublisherImpl.publishStatus(
          objHeader.id, objHeader.status, /*finStream=*/true);
    }
    case ObjectStatus::END_OF_GROUP:
      return subgroup.value()->endOfGroup(objHeader.id);
    case ObjectStatus::END_OF_TRACK_AND_GROUP:
      return subgroup.value()->endOfTrackAndGroup(objHeader.id);
    case ObjectStatus::END_OF_SUBGROUP:
      return subgroup.value()->endOfSubgroup();
  }
}

folly::Expected<folly::Unit, MoQPublishError>
TrackPublisherImpl::groupNotExists(
    uint64_t groupID,
    uint64_t subgroupID,
    Priority priority) {
  return objectStream(
      {trackAlias_,
       groupID,
       subgroupID,
       0,
       priority,
       ForwardPreference::Subgroup,
       ObjectStatus::GROUP_NOT_EXIST,
       0},
      nullptr);
}

folly::Expected<folly::Unit, MoQPublishError> TrackPublisherImpl::datagram(
    const ObjectHeader& header,
    Payload payload) {
  XCHECK(header.forwardPreference == ForwardPreference::Datagram);
  auto wt = getWebTransport();
  if (!wt) {
    XLOG(ERR) << "Trying to publish after subscribeDone";
    return folly::makeUnexpected(MoQPublishError(
        MoQPublishError::API_ERROR, "Publish after subscribeDone"));
  }
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  XCHECK(header.length);
  (void)writeObject(
      writeBuf,
      ObjectHeader{
          trackAlias_,
          header.group,
          header.id,
          header.id,
          header.priority,
          header.forwardPreference,
          header.status,
          *header.length},
      std::move(payload));
  // TODO: set priority when WT has an API for that
  auto res = wt->sendDatagram(writeBuf.move());
  if (res.hasError()) {
    return folly::makeUnexpected(
        MoQPublishError(MoQPublishError::WRITE_ERROR, "sendDatagram failed"));
  }
  return folly::unit;
}

folly::Expected<folly::Unit, MoQPublishError> TrackPublisherImpl::subscribeDone(
    SubscribeDone subDone) {
  subDone.subscribeID = subscribeID_;
  return PublisherImpl::subscribeDone(std::move(subDone));
}

// FetchPublisherImpl

folly::Expected<std::shared_ptr<FetchConsumer>, MoQPublishError>
FetchPublisherImpl::beginFetch(GroupOrder groupOrder) {
  auto wt = getWebTransport();
  if (!wt) {
    XLOG(ERR) << "Trying to publish after fetchCancel";
    return folly::makeUnexpected(MoQPublishError(
        MoQPublishError::API_ERROR, "Publish after fetchCancel"));
  }

  auto stream = wt->createUniStream();
  if (!stream) {
    // failed to create a stream
    XLOG(ERR) << "Failed to create uni stream tp=" << this;
    return folly::makeUnexpected(MoQPublishError(
        MoQPublishError::BLOCKED, "Failed to create uni stream."));
  }
  XLOG(DBG4) << "New stream created, id: " << stream.value()->getID()
             << " tp=" << this;
  setGroupOrder(groupOrder);
  stream.value()->setPriority(1, order(0, 0, 0, priority_, groupOrder_), false);
  streamPublisher_ = std::make_shared<StreamPublisherImpl>(this, *stream);
  return streamPublisher_;
}

// StreamPublisherImpl

StreamPublisherImpl::StreamPublisherImpl(
    MoQSession::PublisherImpl* publisher,
    proxygen::WebTransport::StreamWriteHandle* writeHandle)
    : CancellationCallback(
          writeHandle->getCancelToken(),
          [this] {
            if (writeHandle_) {
              auto code = writeHandle_->stopSendingErrorCode();
              XLOG(DBG1) << "Peer requested write termination code="
                         << (code ? folly::to<std::string>(*code)
                                  : std::string("none"));
              reset(ResetStreamErrorCode::CANCELLED);
            }
          }),
      publisher_(publisher),
      writeHandle_(writeHandle),
      header_{
          publisher->subscribeID(),
          0,
          0,
          std::numeric_limits<uint64_t>::max(),
          0,
          ForwardPreference::Fetch,
          ObjectStatus::NORMAL,
          folly::none} {
  (void)writeStreamHeader(writeBuf_, header_);
}

StreamPublisherImpl::StreamPublisherImpl(
    MoQSession::PublisherImpl* publisher,
    proxygen::WebTransport::StreamWriteHandle* writeHandle,
    TrackAlias alias,
    uint64_t groupID,
    uint64_t subgroupID)
    : StreamPublisherImpl(publisher, writeHandle) {
  writeBuf_.move();
  header_.trackIdentifier = alias;
  header_.forwardPreference = ForwardPreference::Subgroup;
  setGroupAndSubgroup(groupID, subgroupID);
  (void)writeStreamHeader(writeBuf_, header_);
}

// Private methods

void StreamPublisherImpl::onStreamComplete() {
  XCHECK_EQ(writeHandle_, nullptr);
  publisher_->onStreamComplete(header_);
}

folly::Expected<folly::Unit, MoQPublishError>
StreamPublisherImpl::validatePublish(uint64_t objectID) {
  if (currentLengthRemaining_) {
    XLOG(ERR) << "Still publishing previous object sgp=" << this;
    reset(ResetStreamErrorCode::INTERNAL_ERROR);
    return folly::makeUnexpected(MoQPublishError(
        MoQPublishError::API_ERROR, "Previous object incomplete"));
  }
  if (header_.id != std::numeric_limits<uint64_t>::max() &&
      objectID <= header_.id) {
    XLOG(ERR) << "Object ID not advancing header_.id=" << header_.id
              << " objectID=" << objectID << " sgp=" << this;
    reset(ResetStreamErrorCode::INTERNAL_ERROR);
    return folly::makeUnexpected(MoQPublishError(
        MoQPublishError::API_ERROR, "Object ID not advancing in subgroup"));
  }
  if (!writeHandle_) {
    XLOG(ERR) << "Write after subgroup complete sgp=" << this;
    return folly::makeUnexpected(
        MoQPublishError(MoQPublishError::API_ERROR, "Subgroup reset"));
  }
  return folly::unit;
}

folly::Expected<folly::Unit, MoQPublishError>
StreamPublisherImpl::writeCurrentObject(
    uint64_t objectID,
    uint64_t length,
    Payload payload,
    bool finStream) {
  header_.id = objectID;
  header_.length = length;
  (void)writeObject(writeBuf_, header_, std::move(payload));
  return writeToStream(finStream);
}

folly::Expected<folly::Unit, MoQPublishError>
StreamPublisherImpl::writeToStream(bool finStream) {
  auto writeHandle = writeHandle_;
  if (finStream) {
    writeHandle_ = nullptr;
  }
  auto writeRes = writeHandle->writeStreamData(writeBuf_.move(), finStream);
  if (writeRes.hasValue()) {
    if (finStream) {
      onStreamComplete();
    }
    return folly::unit;
  }
  XLOG(ERR) << "write error=" << uint64_t(writeRes.error());
  reset(ResetStreamErrorCode::INTERNAL_ERROR);
  return folly::makeUnexpected(
      MoQPublishError(MoQPublishError::WRITE_ERROR, "write error"));
}

folly::Expected<ObjectPublishStatus, MoQPublishError>
StreamPublisherImpl::validateObjectPublishAndUpdateState(
    folly::IOBuf* payload,
    bool finStream) {
  auto length = payload ? payload->computeChainDataLength() : 0;
  if (!currentLengthRemaining_) {
    XLOG(ERR) << "Not publishing object sgp=" << this;
    return folly::makeUnexpected(
        MoQPublishError(MoQPublishError::API_ERROR, "Not publishing object"));
  }
  if (length > *currentLengthRemaining_) {
    XLOG(ERR) << "Length=" << length
              << " exceeds remaining=" << *currentLengthRemaining_
              << " sgp=" << this;
    reset(ResetStreamErrorCode::INTERNAL_ERROR);
    return folly::makeUnexpected(MoQPublishError(
        MoQPublishError::API_ERROR, "Length exceeds remaining in object"));
  }
  *currentLengthRemaining_ -= length;
  if (*currentLengthRemaining_ == 0) {
    currentLengthRemaining_.reset();
    return ObjectPublishStatus::DONE;
  } else if (finStream) {
    XLOG(ERR) << "finStream with length remaining=" << *currentLengthRemaining_
              << " sgp=" << this;
    reset(ResetStreamErrorCode::INTERNAL_ERROR);
    return folly::makeUnexpected(MoQPublishError(
        MoQPublishError::API_ERROR, "finStream with open object"));
  }
  return ObjectPublishStatus::IN_PROGRESS;
}

// Interface Methods

folly::Expected<folly::Unit, MoQPublishError> StreamPublisherImpl::object(
    uint64_t objectID,
    Payload payload,
    bool finStream) {
  auto validateRes = validatePublish(objectID);
  if (!validateRes) {
    return validateRes;
  }
  auto length = payload ? payload->computeChainDataLength() : 0;
  return writeCurrentObject(objectID, length, std::move(payload), finStream);
}

folly::Expected<folly::Unit, MoQPublishError>
StreamPublisherImpl::objectNotExists(uint64_t objectID, bool finStream) {
  return publishStatus(objectID, ObjectStatus::OBJECT_NOT_EXIST, finStream);
}

folly::Expected<folly::Unit, MoQPublishError> StreamPublisherImpl::beginObject(
    uint64_t objectID,
    uint64_t length,
    Payload initialPayload) {
  auto validateRes = validatePublish(objectID);
  if (!validateRes) {
    return validateRes;
  }
  currentLengthRemaining_ = length;
  auto validateObjectPublishRes = validateObjectPublishAndUpdateState(
      initialPayload.get(),
      /*finStream=*/false);
  if (!validateObjectPublishRes) {
    return folly::makeUnexpected(validateObjectPublishRes.error());
  }
  return writeCurrentObject(
      objectID, length, std::move(initialPayload), /*finStream=*/false);
}

folly::Expected<ObjectPublishStatus, MoQPublishError>
StreamPublisherImpl::objectPayload(Payload payload, bool finStream) {
  auto validateObjectPublishRes =
      validateObjectPublishAndUpdateState(payload.get(), finStream);
  if (!validateObjectPublishRes) {
    return validateObjectPublishRes;
  }
  writeBuf_.append(std::move(payload));
  auto writeRes = writeToStream(finStream);
  if (writeRes.hasValue()) {
    return validateObjectPublishRes.value();
  } else {
    return folly::makeUnexpected(writeRes.error());
  }
}

folly::Expected<folly::Unit, MoQPublishError> StreamPublisherImpl::endOfGroup(
    uint64_t endOfGroupObjectId) {
  return publishStatus(
      endOfGroupObjectId, ObjectStatus::END_OF_GROUP, /*finStream=*/true);
}

folly::Expected<folly::Unit, MoQPublishError>
StreamPublisherImpl::endOfTrackAndGroup(uint64_t endOfTrackObjectId) {
  return publishStatus(
      endOfTrackObjectId,
      ObjectStatus::END_OF_TRACK_AND_GROUP,
      /*finStream=*/true);
}

folly::Expected<folly::Unit, MoQPublishError>
StreamPublisherImpl::publishStatus(
    uint64_t objectID,
    ObjectStatus status,
    bool finStream) {
  auto validateRes = validatePublish(objectID);
  if (!validateRes) {
    return validateRes;
  }
  header_.status = status;
  return writeCurrentObject(
      objectID, /*length=*/0, /*payload=*/nullptr, finStream);
}

folly::Expected<folly::Unit, MoQPublishError>
StreamPublisherImpl::endOfSubgroup() {
  if (currentLengthRemaining_) {
    XLOG(ERR) << "Still publishing previous object sgp=" << this;
    reset(ResetStreamErrorCode::INTERNAL_ERROR);
    return folly::makeUnexpected(MoQPublishError(
        MoQPublishError::API_ERROR, "Previous object incomplete"));
  }
  if (!writeBuf_.empty()) {
    XLOG(WARN) << "No objects published on subgroup=" << header_;
  }
  return writeToStream(/*finStream=*/true);
}

void StreamPublisherImpl::reset(ResetStreamErrorCode error) {
  if (!writeBuf_.empty()) {
    // TODO: stream header is pending, reliable reset?
    XLOG(WARN) << "Stream header pending on subgroup=" << header_;
  }
  if (writeHandle_) {
    auto writeHandle = writeHandle_;
    writeHandle_ = nullptr;
    writeHandle->resetStream(uint32_t(error));
  } else {
    // Can happen on STOP_SENDING
    XLOG(ERR) << "reset with no write handle: sgp=" << this;
  }
  onStreamComplete();
}

folly::Expected<folly::SemiFuture<folly::Unit>, MoQPublishError>
StreamPublisherImpl::awaitReadyToConsume() {
  if (!writeHandle_) {
    return folly::makeUnexpected(
        MoQPublishError(MoQPublishError::CANCELLED, "Fetch cancelled"));
  }
  auto writableFuture = writeHandle_->awaitWritable();
  if (!writableFuture) {
    return folly::makeUnexpected(
        MoQPublishError(MoQPublishError::WRITE_ERROR, "awaitWritable failed"));
  }
  return std::move(writableFuture.value());
}

} // namespace

namespace moxygen {

using folly::coro::co_awaitTry;
using folly::coro::co_error;

MoQSession::~MoQSession() {
  cleanup();
}

void MoQSession::cleanup() {
  // TODO: Are these loops safe since they may (should?) delete elements
  for (auto& pubTrack : pubTracks_) {
    pubTrack.second->reset(ResetStreamErrorCode::SESSION_CLOSED);
  }
  pubTracks_.clear();
  for (auto& subTrack : subTracks_) {
    subTrack.second->subscribeError(
        {/*TrackHandle fills in subId*/ 0, 500, "session closed", folly::none});
  }
  subTracks_.clear();
  for (auto& fetch : fetches_) {
    // TODO: there needs to be a way to queue an error in TrackHandle, both
    // from here, when close races the FETCH stream, and from readLoop
    // where we get a reset.
    fetch.second->fetchError(
        {/*TrackHandle fills in subId*/ 0, 500, "session closed"});
  }
  fetches_.clear();
  for (auto& [trackNamespace, pendingAnn] : pendingAnnounce_) {
    pendingAnn.setValue(folly::makeUnexpected(
        AnnounceError({trackNamespace, 500, "session closed"})));
  }
  pendingAnnounce_.clear();
  for (auto& [trackNamespace, pendingSn] : pendingSubscribeAnnounces_) {
    pendingSn.setValue(folly::makeUnexpected(
        SubscribeAnnouncesError({trackNamespace, 500, "session closed"})));
  }
  pendingSubscribeAnnounces_.clear();
  if (!cancellationSource_.isCancellationRequested()) {
    XLOG(DBG1) << "requestCancellation from cleanup sess=" << this;
    cancellationSource_.requestCancellation();
  }
}

void MoQSession::start() {
  XLOG(DBG1) << __func__ << " sess=" << this;
  if (dir_ == MoQControlCodec::Direction::CLIENT) {
    auto cs = wt_->createBidiStream();
    if (!cs) {
      XLOG(ERR) << "Failed to get control stream sess=" << this;
      wt_->closeSession();
      return;
    }
    auto controlStream = cs.value();
    controlStream.writeHandle->setPriority(0, 0, false);

    auto mergeToken = folly::CancellationToken::merge(
        cancellationSource_.getToken(),
        controlStream.writeHandle->getCancelToken());
    folly::coro::co_withCancellation(
        std::move(mergeToken), controlWriteLoop(controlStream.writeHandle))
        .scheduleOn(evb_)
        .start();
    co_withCancellation(
        cancellationSource_.getToken(),
        readLoop(StreamType::CONTROL, controlStream.readHandle))
        .scheduleOn(evb_)
        .start();
  }
}

void MoQSession::drain() {
  XLOG(DBG1) << __func__ << " sess=" << this;
  draining_ = true;
  checkForCloseOnDrain();
}

void MoQSession::checkForCloseOnDrain() {
  if (draining_ && fetches_.empty() && subTracks_.empty()) {
    close();
  }
}

void MoQSession::close(folly::Optional<SessionCloseErrorCode> error) {
  XLOG(DBG1) << __func__ << " sess=" << this;
  if (wt_) {
    // TODO: The error code should be propagated to
    // whatever implemented proxygen::WebTransport.
    // TxnWebTransport current just ignores the errorCode
    auto wt = wt_;
    wt_ = nullptr;

    cleanup();

    wt->closeSession(
        error.has_value()
            ? folly::make_optional(folly::to_underlying(error.value()))
            : folly::none);
    XLOG(DBG1) << "requestCancellation from close sess=" << this;
    cancellationSource_.requestCancellation();
  }
}

folly::coro::Task<void> MoQSession::controlWriteLoop(
    proxygen::WebTransport::StreamWriteHandle* controlStream) {
  XLOG(DBG1) << __func__ << " sess=" << this;
  auto g = folly::makeGuard([func = __func__, this] {
    XLOG(DBG1) << "exit " << func << " sess=" << this;
  });
  while (true) {
    co_await folly::coro::co_safe_point;
    if (controlWriteBuf_.empty()) {
      controlWriteEvent_.reset();
      auto res = co_await co_awaitTry(controlWriteEvent_.wait());
      if (res.tryGetExceptionObject<folly::FutureTimeout>()) {
      } else if (res.tryGetExceptionObject<folly::OperationCancelled>()) {
        co_return;
      } else if (res.hasException()) {
        XLOG(ERR) << "Unexpected exception: " << res.exception().what();
        co_return;
      }
    }
    co_await folly::coro::co_safe_point;
    auto writeRes =
        controlStream->writeStreamData(controlWriteBuf_.move(), false);
    if (!writeRes) {
      XLOG(ERR) << "Write error: " << uint64_t(writeRes.error());
      break;
    }
    auto awaitRes = controlStream->awaitWritable();
    if (!awaitRes) {
      XLOG(ERR) << "Control stream write error";
      break;
    }
    co_await std::move(*awaitRes);
  }
}

folly::coro::Task<ServerSetup> MoQSession::setup(ClientSetup setup) {
  XCHECK(dir_ == MoQControlCodec::Direction::CLIENT);
  XLOG(DBG1) << __func__ << " sess=" << this;
  std::tie(setupPromise_, setupFuture_) =
      folly::coro::makePromiseContract<ServerSetup>();

  auto maxSubscribeId = getMaxSubscribeIdIfPresent(setup.params);
  auto res = writeClientSetup(controlWriteBuf_, std::move(setup));
  if (!res) {
    XLOG(ERR) << "writeClientSetup failed sess=" << this;
    co_yield folly::coro::co_error(std::runtime_error("Failed to write setup"));
  }
  maxSubscribeID_ = maxConcurrentSubscribes_ = maxSubscribeId;
  controlWriteEvent_.signal();

  auto deletedToken = cancellationSource_.getToken();
  auto token = co_await folly::coro::co_current_cancellation_token;
  auto mergeToken = folly::CancellationToken::merge(deletedToken, token);
  folly::EventBaseThreadTimekeeper tk(*evb_);
  auto serverSetup = co_await co_awaitTry(folly::coro::co_withCancellation(
      mergeToken,
      folly::coro::timeout(std::move(setupFuture_), kSetupTimeout, &tk)));
  if (mergeToken.isCancellationRequested()) {
    co_yield folly::coro::co_error(folly::OperationCancelled());
  }
  if (serverSetup.hasException()) {
    close();
    XLOG(ERR) << "Setup Failed: " << serverSetup.exception().what();
    co_yield folly::coro::co_error(serverSetup.exception());
  }
  setupComplete_ = true;
  co_return *serverSetup;
}

void MoQSession::onServerSetup(ServerSetup serverSetup) {
  XCHECK(dir_ == MoQControlCodec::Direction::CLIENT);
  XLOG(DBG1) << __func__ << " sess=" << this;
  if (serverSetup.selectedVersion != kVersionDraftCurrent) {
    XLOG(ERR) << "Invalid version = " << serverSetup.selectedVersion
              << " sess=" << this;
    close();
    setupPromise_.setException(std::runtime_error("Invalid version"));
    return;
  }
  peerMaxSubscribeID_ = getMaxSubscribeIdIfPresent(serverSetup.params);
  setupPromise_.setValue(std::move(serverSetup));
}

void MoQSession::onClientSetup(ClientSetup clientSetup) {
  XCHECK(dir_ == MoQControlCodec::Direction::SERVER);
  XLOG(DBG1) << __func__ << " sess=" << this;
  if (std::find(
          clientSetup.supportedVersions.begin(),
          clientSetup.supportedVersions.end(),
          kVersionDraftCurrent) == clientSetup.supportedVersions.end()) {
    XLOG(ERR) << "No matching versions sess=" << this;
    for (auto v : clientSetup.supportedVersions) {
      XLOG(ERR) << "client sent=" << v << " sess=" << this;
    }
    close();
    return;
  }
  peerMaxSubscribeID_ = getMaxSubscribeIdIfPresent(clientSetup.params);
  auto serverSetup =
      serverSetupCallback_->onClientSetup(std::move(clientSetup));
  if (!serverSetup.hasValue()) {
    XLOG(ERR) << "Server setup callback failed sess=" << this;
    close();
    return;
  }

  auto maxSubscribeId = getMaxSubscribeIdIfPresent(serverSetup->params);
  auto res = writeServerSetup(controlWriteBuf_, std::move(*serverSetup));
  if (!res) {
    XLOG(ERR) << "writeServerSetup failed sess=" << this;
    return;
  }
  maxSubscribeID_ = maxConcurrentSubscribes_ = maxSubscribeId;
  setupComplete_ = true;
  setupPromise_.setValue(ServerSetup());
  controlWriteEvent_.signal();
}

folly::coro::AsyncGenerator<MoQSession::MoQMessage>
MoQSession::controlMessages() {
  XLOG(DBG1) << __func__ << " sess=" << this;
  while (true) {
    auto message =
        co_await folly::coro::co_awaitTry(folly::coro::co_withCancellation(
            cancellationSource_.getToken(), controlMessages_.dequeue()));
    if (message.hasException()) {
      XLOG(ERR) << message.exception().what() << " sess=" << this;
      break;
    }
    co_yield *message;
  }
}

folly::coro::Task<void> MoQSession::readLoop(
    StreamType streamType,
    proxygen::WebTransport::StreamReadHandle* readHandle) {
  XLOG(DBG1) << __func__ << " sess=" << this;
  auto g = folly::makeGuard([func = __func__, this] {
    XLOG(DBG1) << "exit " << func << " sess=" << this;
  });
  co_await folly::coro::co_safe_point;
  std::unique_ptr<MoQCodec> codec;
  MoQObjectStreamCodec* objCodec = nullptr;
  if (streamType == StreamType::CONTROL) {
    codec = std::make_unique<MoQControlCodec>(dir_, this);
  } else {
    auto res = std::make_unique<MoQObjectStreamCodec>(this);
    objCodec = res.get();
    codec = std::move(res);
  }
  auto streamId = readHandle->getID();
  codec->setStreamId(streamId);

  // TODO: disallow OBJECT on control streams and non-object on non-control
  bool fin = false;
  auto token = co_await folly::coro::co_current_cancellation_token;
  std::shared_ptr<TrackHandle> track;
  folly::CancellationSource cs;
  while (!fin && !token.isCancellationRequested()) {
    auto streamData = co_await folly::coro::co_awaitTry(
        readHandle->readStreamData().via(evb_));
    if (streamData.hasException()) {
      XLOG(ERR) << streamData.exception().what() << " id=" << streamId
                << " sess=" << this;
      // TODO: possibly erase fetch
      cs.requestCancellation();
      break;
    } else {
      if (streamData->data || streamData->fin) {
        codec->onIngress(std::move(streamData->data), streamData->fin);
      }
      if (!track && objCodec) {
        // TODO: this might not be set
        auto trackId = objCodec->getTrackIdentifier();
        if (auto subscribeID = std::get_if<SubscribeID>(&trackId)) {
          // it's fetch
          track = getTrack(trackId);
          track->mergeReadCancelToken(
              folly::CancellationToken::merge(cs.getToken(), token));
        }
      }
      fin = streamData->fin;
      XLOG_IF(DBG3, fin) << "End of stream id=" << streamId << " sess=" << this;
    }
  }
  if (track) {
    track->fin();
    track->setAllDataReceived();
    if (track->fetchOkReceived()) {
      fetches_.erase(track->subscribeID());
      checkForCloseOnDrain();
    }
  }
}

std::shared_ptr<MoQSession::TrackHandle> MoQSession::getTrack(
    TrackIdentifier trackIdentifier) {
  // This can be cached in the handling stream
  std::shared_ptr<TrackHandle> track;
  auto alias = std::get_if<TrackAlias>(&trackIdentifier);
  if (alias) {
    auto trackIt = subTracks_.find(*alias);
    if (trackIt == subTracks_.end()) {
      // received an object for unknown track alias
      XLOG(ERR) << "unknown track alias=" << alias->value << " sess=" << this;
      return nullptr;
    }
    track = trackIt->second;
  } else {
    auto subscribeID = std::get<SubscribeID>(trackIdentifier);
    XLOG(DBG3) << "getTrack subID=" << subscribeID;
    auto trackIt = fetches_.find(subscribeID);
    if (trackIt == fetches_.end()) {
      // received an object for unknown subscribe ID
      XLOG(ERR) << "unknown subscribe ID=" << subscribeID << " sess=" << this;
      return nullptr;
    }
    track = trackIt->second;
  }
  return track;
}

void MoQSession::onObjectHeader(ObjectHeader objHeader) {
  XLOG(DBG1) << "MoQSession::" << __func__ << " " << objHeader
             << " sess=" << this;
  auto track = getTrack(objHeader.trackIdentifier);
  if (track) {
    track->onObjectHeader(std::move(objHeader));
  }
}

void MoQSession::onObjectPayload(
    TrackIdentifier trackIdentifier,
    uint64_t groupID,
    uint64_t id,
    std::unique_ptr<folly::IOBuf> payload,
    bool eom) {
  XLOG(DBG1) << __func__ << " sess=" << this;
  auto track = getTrack(trackIdentifier);
  if (track) {
    track->onObjectPayload(groupID, id, std::move(payload), eom);
  }
}

void MoQSession::TrackHandle::onObjectHeader(ObjectHeader objHeader) {
  XLOG(DBG1) << __func__ << " objHeader=" << objHeader
             << " trackHandle=" << this;
  uint64_t objectIdKey = objHeader.id;
  auto status = objHeader.status;
  if (status != ObjectStatus::NORMAL) {
    objectIdKey |= (1ull << 63);
  }
  auto res = objects_.emplace(
      std::piecewise_construct,
      std::forward_as_tuple(std::make_pair(objHeader.group, objectIdKey)),
      std::forward_as_tuple(std::make_shared<ObjectSource>()));
  res.first->second->header = std::move(objHeader);
  res.first->second->fullTrackName = fullTrackName_;
  res.first->second->cancelToken = cancelToken_;
  if (status != ObjectStatus::NORMAL) {
    res.first->second->payloadQueue.enqueue(nullptr);
  }
  // TODO: objects_ accumulates the headers of all objects for the life of the
  // track.  Remove an entry from objects when returning the end of the payload,
  // or the object itself for non-normal.
  newObjects_.enqueue(res.first->second);
}

void MoQSession::TrackHandle::fin() {
  newObjects_.enqueue(nullptr);
}

void MoQSession::TrackHandle::onObjectPayload(
    uint64_t groupId,
    uint64_t id,
    std::unique_ptr<folly::IOBuf> payload,
    bool eom) {
  XLOG(DBG1) << __func__ << " g=" << groupId << " o=" << id
             << " len=" << (payload ? payload->computeChainDataLength() : 0)
             << " eom=" << uint64_t(eom) << " trackHandle=" << this;
  auto objIt = objects_.find(std::make_pair(groupId, id));
  if (objIt == objects_.end()) {
    // error;
    XLOG(ERR) << "unknown object gid=" << groupId << " seq=" << id
              << " trackHandle=" << this;
    return;
  }
  if (payload) {
    XLOG(DBG1) << "payload enqueued trackHandle=" << this;
    objIt->second->payloadQueue.enqueue(std::move(payload));
  }
  if (eom) {
    XLOG(DBG1) << "eom enqueued trackHandle=" << this;
    objIt->second->payloadQueue.enqueue(nullptr);
  }
}

void MoQSession::onSubscribe(SubscribeRequest subscribeRequest) {
  XLOG(DBG1) << __func__ << " sess=" << this;
  const auto subscribeID = subscribeRequest.subscribeID;
  if (closeSessionIfSubscribeIdInvalid(subscribeID)) {
    return;
  }

  // TODO: The publisher should maintain some state like
  //   Subscribe ID -> Track Name, Locations [currently held in
  //   MoQForwarder] Track Alias -> Track Name
  // If ths session holds this state, it can check for duplicate
  // subscriptions
  auto it = pubTracks_.find(subscribeRequest.subscribeID);
  if (it != pubTracks_.end()) {
    XLOG(ERR) << "Duplicate subscribe ID=" << subscribeRequest.subscribeID
              << " sess=" << this;
    subscribeError({subscribeRequest.subscribeID, 400, "dup sub ID"});
    return;
  }
  // TODO: Check for duplicate alias
  auto trackPublisher = std::make_shared<TrackPublisherImpl>(
      this,
      subscribeRequest.subscribeID,
      subscribeRequest.trackAlias,
      subscribeRequest.priority,
      subscribeRequest.groupOrder);
  pubTracks_.emplace(subscribeID, std::move(trackPublisher));
  // TODO: there should be a timeout for the application to call
  // subscribeOK/Error
  controlMessages_.enqueue(std::move(subscribeRequest));
}

void MoQSession::onSubscribeUpdate(SubscribeUpdate subscribeUpdate) {
  XLOG(DBG1) << __func__ << " sess=" << this;
  const auto subscribeID = subscribeUpdate.subscribeID;
  auto it = pubTracks_.find(subscribeID);
  if (it == pubTracks_.end()) {
    XLOG(ERR) << "No matching subscribe ID=" << subscribeID << " sess=" << this;
    return;
  }
  if (closeSessionIfSubscribeIdInvalid(subscribeID)) {
    return;
  }

  it->second->setPriority(subscribeUpdate.priority);
  // TODO: update priority of tracks in flight
  controlMessages_.enqueue(std::move(subscribeUpdate));
}

void MoQSession::onUnsubscribe(Unsubscribe unsubscribe) {
  XLOG(DBG1) << __func__ << " sess=" << this;
  // How does this impact pending subscribes?
  // and open TrackHandles
  controlMessages_.enqueue(std::move(unsubscribe));
}

void MoQSession::onSubscribeOk(SubscribeOk subOk) {
  XLOG(DBG1) << __func__ << " sess=" << this;
  auto trackAliasIt = subIdToTrackAlias_.find(subOk.subscribeID);
  if (trackAliasIt == subIdToTrackAlias_.end()) {
    // unknown
    XLOG(ERR) << "No matching subscribe ID=" << subOk.subscribeID
              << " sess=" << this;
    return;
  }
  subTracks_[trackAliasIt->second]->subscribeOK(
      subTracks_[trackAliasIt->second], subOk.groupOrder, subOk.latest);
}

void MoQSession::onSubscribeError(SubscribeError subErr) {
  XLOG(DBG1) << __func__ << " sess=" << this;
  auto trackAliasIt = subIdToTrackAlias_.find(subErr.subscribeID);
  if (trackAliasIt == subIdToTrackAlias_.end()) {
    // unknown
    XLOG(ERR) << "No matching subscribe ID=" << subErr.subscribeID
              << " sess=" << this;
    return;
  }
  subTracks_[trackAliasIt->second]->subscribeError(std::move(subErr));
  subTracks_.erase(trackAliasIt->second);
  subIdToTrackAlias_.erase(trackAliasIt);
  checkForCloseOnDrain();
}

void MoQSession::onSubscribeDone(SubscribeDone subscribeDone) {
  XLOG(DBG1) << "SubscribeDone id=" << subscribeDone.subscribeID
             << " code=" << folly::to_underlying(subscribeDone.statusCode)
             << " reason=" << subscribeDone.reasonPhrase;
  auto trackAliasIt = subIdToTrackAlias_.find(subscribeDone.subscribeID);
  if (trackAliasIt == subIdToTrackAlias_.end()) {
    // unknown
    XLOG(ERR) << "No matching subscribe ID=" << subscribeDone.subscribeID
              << " sess=" << this;
    return;
  }

  // TODO: handle final object and status code
  // TODO: there could still be objects in flight.  Removing from maps now
  // will prevent their delivery.  I think the only way to handle this is with
  // timeouts.
  auto trackHandleIt = subTracks_.find(trackAliasIt->second);
  if (trackHandleIt != subTracks_.end()) {
    auto trackHandle = trackHandleIt->second;
    subTracks_.erase(trackHandleIt);
    trackHandle->fin();
  } else {
    XLOG(DFATAL) << "trackAliasIt but no trackHandleIt for id="
                 << subscribeDone.subscribeID << " sess=" << this;
  }
  subIdToTrackAlias_.erase(trackAliasIt);
  controlMessages_.enqueue(std::move(subscribeDone));
  checkForCloseOnDrain();
}

void MoQSession::onMaxSubscribeId(MaxSubscribeId maxSubscribeId) {
  XLOG(DBG1) << __func__ << " sess=" << this;
  if (maxSubscribeId.subscribeID.value > peerMaxSubscribeID_) {
    XLOG(DBG1) << fmt::format(
        "Bumping the maxSubscribeId to: {} from: {}",
        maxSubscribeId.subscribeID.value,
        peerMaxSubscribeID_);
    peerMaxSubscribeID_ = maxSubscribeId.subscribeID.value;
    return;
  }

  XLOG(ERR) << fmt::format(
      "Invalid MaxSubscribeId: {}. Current maxSubscribeId:{}",
      maxSubscribeId.subscribeID.value,
      maxSubscribeID_);
  close(SessionCloseErrorCode::PROTOCOL_VIOLATION);
}

void MoQSession::onFetch(Fetch fetch) {
  XLOG(DBG1) << __func__ << " sess=" << this;
  const auto subscribeID = fetch.subscribeID;
  if (closeSessionIfSubscribeIdInvalid(subscribeID)) {
    return;
  }
  if (fetch.end < fetch.start) {
    fetchError(
        {fetch.subscribeID,
         folly::to_underlying(FetchErrorCode::INVALID_RANGE),
         "End must be after start"});
    return;
  }
  auto it = pubTracks_.find(fetch.subscribeID);
  if (it != pubTracks_.end()) {
    XLOG(ERR) << "Duplicate subscribe ID=" << fetch.subscribeID
              << " sess=" << this;
    fetchError({fetch.subscribeID, 400, "dup sub ID"});
    return;
  }
  auto fetchPublisher = std::make_shared<FetchPublisherImpl>(
      this, fetch.subscribeID, fetch.priority, fetch.groupOrder);
  pubTracks_.emplace(fetch.subscribeID, std::move(fetchPublisher));
  controlMessages_.enqueue(std::move(fetch));
}

void MoQSession::onFetchCancel(FetchCancel fetchCancel) {
  XLOG(DBG1) << __func__ << " sess=" << this;
  auto pubTrackIt = pubTracks_.find(fetchCancel.subscribeID);
  if (pubTrackIt == pubTracks_.end()) {
    XLOG(DBG4) << "No publish key for fetch id=" << fetchCancel.subscribeID
               << " sess=" << this;
    // The Fetch stream has already closed, or never existed
    // If it's already closed, a no-op is fine.
    // See: https://github.com/moq-wg/moq-transport/issues/630
  } else {
    // It's possible the fetch stream hasn't opened yet if the application
    // hasn't made it to fetchOK.
    pubTrackIt->second->reset(ResetStreamErrorCode::CANCELLED);
    retireSubscribeId(/*signalWriteLoop=*/true);
  }
}

void MoQSession::onFetchOk(FetchOk fetchOk) {
  XLOG(DBG1) << __func__ << " sess=" << this;
  auto fetchIt = fetches_.find(fetchOk.subscribeID);
  if (fetchIt == fetches_.end()) {
    XLOG(ERR) << "No matching subscribe ID=" << fetchOk.subscribeID
              << " sess=" << this;
    return;
  }
  auto trackHandle = fetchIt->second;
  trackHandle->fetchOK(trackHandle);
  if (trackHandle->allDataReceived()) {
    fetches_.erase(trackHandle->subscribeID());
  }
}

void MoQSession::onFetchError(FetchError fetchError) {
  XLOG(DBG1) << __func__ << " sess=" << this;
  auto fetchIt = fetches_.find(fetchError.subscribeID);
  if (fetchIt == fetches_.end()) {
    XLOG(ERR) << "No matching subscribe ID=" << fetchError.subscribeID
              << " sess=" << this;
    return;
  }
  fetchIt->second->fetchError(fetchError);
  fetches_.erase(fetchIt);
  checkForCloseOnDrain();
}

void MoQSession::onAnnounce(Announce ann) {
  XLOG(DBG1) << __func__ << " sess=" << this << " ns=" << ann.trackNamespace;
  controlMessages_.enqueue(std::move(ann));
}

void MoQSession::onAnnounceOk(AnnounceOk annOk) {
  XLOG(DBG1) << __func__ << " sess=" << this;
  auto annIt = pendingAnnounce_.find(annOk.trackNamespace);
  if (annIt == pendingAnnounce_.end()) {
    // unknown
    XLOG(ERR) << "No matching announce trackNamespace=" << annOk.trackNamespace
              << " sess=" << this;
    return;
  }
  annIt->second.setValue(std::move(annOk));
  pendingAnnounce_.erase(annIt);
}

void MoQSession::onAnnounceError(AnnounceError announceError) {
  XLOG(DBG1) << __func__ << " sess=" << this;
  auto annIt = pendingAnnounce_.find(announceError.trackNamespace);
  if (annIt == pendingAnnounce_.end()) {
    // unknown
    XLOG(ERR) << "No matching announce trackNamespace="
              << announceError.trackNamespace << " sess=" << this;
    return;
  }
  annIt->second.setValue(folly::makeUnexpected(std::move(announceError)));
  pendingAnnounce_.erase(annIt);
}

void MoQSession::onUnannounce(Unannounce unAnn) {
  XLOG(DBG1) << __func__ << " sess=" << this;
  controlMessages_.enqueue(std::move(unAnn));
}

void MoQSession::onAnnounceCancel(AnnounceCancel announceCancel) {
  XLOG(DBG1) << __func__ << " sess=" << this;
  controlMessages_.enqueue(std::move(announceCancel));
}

void MoQSession::onSubscribeAnnounces(SubscribeAnnounces sa) {
  XLOG(DBG1) << __func__ << " sess=" << this;
  controlMessages_.enqueue(std::move(sa));
}

void MoQSession::onSubscribeAnnouncesOk(SubscribeAnnouncesOk saOk) {
  XLOG(DBG1) << __func__ << " sess=" << this;
  auto saIt = pendingSubscribeAnnounces_.find(saOk.trackNamespacePrefix);
  if (saIt == pendingSubscribeAnnounces_.end()) {
    // unknown
    XLOG(ERR) << "No matching subscribeAnnounces trackNamespace="
              << saOk.trackNamespacePrefix << " sess=" << this;
    return;
  }
  saIt->second.setValue(std::move(saOk));
  pendingSubscribeAnnounces_.erase(saIt);
}

void MoQSession::onSubscribeAnnouncesError(
    SubscribeAnnouncesError subscribeAnnouncesError) {
  XLOG(DBG1) << __func__ << " sess=" << this;
  auto saIt = pendingSubscribeAnnounces_.find(
      subscribeAnnouncesError.trackNamespacePrefix);
  if (saIt == pendingSubscribeAnnounces_.end()) {
    // unknown
    XLOG(ERR) << "No matching subscribeAnnounces trackNamespace="
              << subscribeAnnouncesError.trackNamespacePrefix
              << " sess=" << this;
    return;
  }
  saIt->second.setValue(
      folly::makeUnexpected(std::move(subscribeAnnouncesError)));
  pendingSubscribeAnnounces_.erase(saIt);
}

void MoQSession::onUnsubscribeAnnounces(UnsubscribeAnnounces unsub) {
  XLOG(DBG1) << __func__ << " sess=" << this;
  controlMessages_.enqueue(std::move(unsub));
}

void MoQSession::onTrackStatusRequest(TrackStatusRequest trackStatusRequest) {
  XLOG(DBG1) << __func__ << " sess=" << this;
  controlMessages_.enqueue(std::move(trackStatusRequest));
}

void MoQSession::onTrackStatus(TrackStatus trackStatus) {
  XLOG(DBG1) << __func__ << " sess=" << this;
  controlMessages_.enqueue(std::move(trackStatus));
}

void MoQSession::onGoaway(Goaway goaway) {
  XLOG(DBG1) << __func__ << " sess=" << this;
  controlMessages_.enqueue(std::move(goaway));
}

void MoQSession::onConnectionError(ErrorCode error) {
  XLOG(DBG1) << __func__ << " sess=" << this;
  XLOG(ERR) << "err=" << folly::to_underlying(error);
  // TODO
}

folly::coro::Task<folly::Expected<AnnounceOk, AnnounceError>>
MoQSession::announce(Announce ann) {
  XLOG(DBG1) << __func__ << " sess=" << this;
  auto trackNamespace = ann.trackNamespace;
  auto res = writeAnnounce(controlWriteBuf_, std::move(ann));
  if (!res) {
    XLOG(ERR) << "writeAnnounce failed sess=" << this;
    co_return folly::makeUnexpected(
        AnnounceError({std::move(trackNamespace), 500, "local write failed"}));
  }
  controlWriteEvent_.signal();
  auto contract = folly::coro::makePromiseContract<
      folly::Expected<AnnounceOk, AnnounceError>>();
  pendingAnnounce_.emplace(
      std::move(trackNamespace), std::move(contract.first));
  co_return co_await std::move(contract.second);
}

void MoQSession::announceOk(AnnounceOk annOk) {
  XLOG(DBG1) << __func__ << " sess=" << this;
  auto res = writeAnnounceOk(controlWriteBuf_, std::move(annOk));
  if (!res) {
    XLOG(ERR) << "writeAnnounceOk failed sess=" << this;
    return;
  }
  controlWriteEvent_.signal();
}

void MoQSession::announceError(AnnounceError announceError) {
  XLOG(DBG1) << __func__ << " sess=" << this;
  auto res = writeAnnounceError(controlWriteBuf_, std::move(announceError));
  if (!res) {
    XLOG(ERR) << "writeAnnounceError failed sess=" << this;
    return;
  }
  controlWriteEvent_.signal();
}

void MoQSession::unannounce(Unannounce unann) {
  XLOG(DBG1) << __func__ << " sess=" << this;
  auto trackNamespace = unann.trackNamespace;
  auto res = writeUnannounce(controlWriteBuf_, std::move(unann));
  if (!res) {
    XLOG(ERR) << "writeUnannounce failed sess=" << this;
  }
  controlWriteEvent_.signal();
}

folly::coro::Task<
    folly::Expected<SubscribeAnnouncesOk, SubscribeAnnouncesError>>
MoQSession::subscribeAnnounces(SubscribeAnnounces sa) {
  XLOG(DBG1) << __func__ << " sess=" << this;
  auto trackNamespace = sa.trackNamespacePrefix;
  auto res = writeSubscribeAnnounces(controlWriteBuf_, std::move(sa));
  if (!res) {
    XLOG(ERR) << "writeSubscribeAnnounces failed sess=" << this;
    co_return folly::makeUnexpected(SubscribeAnnouncesError(
        {std::move(trackNamespace), 500, "local write failed"}));
  }
  controlWriteEvent_.signal();
  auto contract = folly::coro::makePromiseContract<
      folly::Expected<SubscribeAnnouncesOk, SubscribeAnnouncesError>>();
  pendingSubscribeAnnounces_.emplace(
      std::move(trackNamespace), std::move(contract.first));
  co_return co_await std::move(contract.second);
}

void MoQSession::subscribeAnnouncesOk(SubscribeAnnouncesOk saOk) {
  XLOG(DBG1) << __func__ << " sess=" << this;
  auto res = writeSubscribeAnnouncesOk(controlWriteBuf_, std::move(saOk));
  if (!res) {
    XLOG(ERR) << "writeSubscribeAnnouncesOk failed sess=" << this;
    return;
  }
  controlWriteEvent_.signal();
}

void MoQSession::subscribeAnnouncesError(
    SubscribeAnnouncesError subscribeAnnouncesError) {
  XLOG(DBG1) << __func__ << " sess=" << this;
  auto res = writeSubscribeAnnouncesError(
      controlWriteBuf_, std::move(subscribeAnnouncesError));
  if (!res) {
    XLOG(ERR) << "writeSubscribeAnnouncesError failed sess=" << this;
    return;
  }
  controlWriteEvent_.signal();
}

folly::coro::AsyncGenerator<
    std::shared_ptr<MoQSession::TrackHandle::ObjectSource>>
MoQSession::TrackHandle::objects() {
  XLOG(DBG1) << __func__ << " trackHandle=" << this;
  auto g =
      folly::makeGuard([func = __func__] { XLOG(DBG1) << "exit " << func; });
  auto cancelToken = co_await folly::coro::co_current_cancellation_token;
  auto mergeToken = folly::CancellationToken::merge(cancelToken, cancelToken_);
  folly::EventBaseThreadTimekeeper tk(*evb_);
  while (!cancelToken.isCancellationRequested()) {
    auto optionalObj = newObjects_.try_dequeue();
    std::shared_ptr<ObjectSource> obj;
    if (optionalObj) {
      obj = *optionalObj;
    } else {
      obj = co_await folly::coro::co_withCancellation(
          mergeToken,
          folly::coro::timeout(newObjects_.dequeue(), objectTimeout_, &tk));
    }
    if (!obj) {
      XLOG(DBG3) << "Out of objects for trackHandle=" << this
                 << " id=" << subscribeID_;
      break;
    }
    co_yield obj;
  }
}

folly::coro::Task<
    folly::Expected<std::shared_ptr<MoQSession::TrackHandle>, SubscribeError>>
MoQSession::subscribe(SubscribeRequest sub) {
  XLOG(DBG1) << __func__ << " sess=" << this;
  auto fullTrackName = sub.fullTrackName;
  if (nextSubscribeID_ >= peerMaxSubscribeID_) {
    XLOG(WARN) << "Issuing subscribe that will fail; nextSubscribeID_="
               << nextSubscribeID_
               << " peerMaxSubscribeID_=" << peerMaxSubscribeID_
               << " sess=" << this;
  }
  SubscribeID subID = nextSubscribeID_++;
  sub.subscribeID = subID;
  sub.trackAlias = TrackAlias(subID.value);
  TrackAlias trackAlias = sub.trackAlias;
  auto wres = writeSubscribeRequest(controlWriteBuf_, std::move(sub));
  if (!wres) {
    XLOG(ERR) << "writeSubscribeRequest failed sess=" << this;
    co_return folly::makeUnexpected(
        SubscribeError({subID, 500, "local write failed", folly::none}));
  }
  controlWriteEvent_.signal();
  auto res = subIdToTrackAlias_.emplace(subID, trackAlias);
  XCHECK(res.second) << "Duplicate subscribe ID";
  auto subTrack = subTracks_.emplace(
      std::piecewise_construct,
      std::forward_as_tuple(trackAlias),
      std::forward_as_tuple(std::make_shared<TrackHandle>(
          fullTrackName, subID, evb_, cancellationSource_.getToken())));

  auto trackHandle = subTrack.first->second;
  auto res2 = co_await trackHandle->ready();
  XLOG(DBG1) << "Subscribe ready trackHandle=" << trackHandle
             << " subscribeID=" << subID;
  co_return res2;
}

std::shared_ptr<TrackConsumer> MoQSession::subscribeOk(SubscribeOk subOk) {
  XLOG(DBG1) << __func__ << " sess=" << this;
  auto it = pubTracks_.find(subOk.subscribeID);
  if (it == pubTracks_.end()) {
    XLOG(ERR) << "Invalid Subscribe OK, id=" << subOk.subscribeID;
    return nullptr;
  }
  auto trackPublisher =
      std::dynamic_pointer_cast<TrackPublisherImpl>(it->second);
  if (!trackPublisher) {
    XLOG(ERR) << "subscribe ID maps to a fetch, not a subscribe, id="
              << subOk.subscribeID;
    subscribeError(
        {subOk.subscribeID,
         folly::to_underlying(FetchErrorCode::INTERNAL_ERROR),
         ""});
    return nullptr;
  }
  trackPublisher->setGroupOrder(subOk.groupOrder);
  auto res = writeSubscribeOk(controlWriteBuf_, subOk);
  if (!res) {
    XLOG(ERR) << "writeSubscribeOk failed sess=" << this;
    return nullptr;
  }
  controlWriteEvent_.signal();
  return std::static_pointer_cast<TrackConsumer>(trackPublisher);
}

void MoQSession::subscribeError(SubscribeError subErr) {
  XLOG(DBG1) << __func__ << " sess=" << this;
  auto it = pubTracks_.find(subErr.subscribeID);
  if (it == pubTracks_.end()) {
    XLOG(ERR) << "Invalid Subscribe OK, id=" << subErr.subscribeID;
    return;
  }
  pubTracks_.erase(it);
  auto res = writeSubscribeError(controlWriteBuf_, std::move(subErr));
  retireSubscribeId(/*signalWriteLoop=*/false);
  if (!res) {
    XLOG(ERR) << "writeSubscribeError failed sess=" << this;
    return;
  }
  controlWriteEvent_.signal();
}

void MoQSession::unsubscribe(Unsubscribe unsubscribe) {
  XLOG(DBG1) << __func__ << " sess=" << this;
  auto trackAliasIt = subIdToTrackAlias_.find(unsubscribe.subscribeID);
  if (trackAliasIt == subIdToTrackAlias_.end()) {
    // unknown
    XLOG(ERR) << "No matching subscribe ID=" << unsubscribe.subscribeID
              << " sess=" << this;
    return;
  }
  auto trackIt = subTracks_.find(trackAliasIt->second);
  if (trackIt == subTracks_.end()) {
    // unknown
    XLOG(ERR) << "No matching subscribe ID=" << unsubscribe.subscribeID
              << " sess=" << this;
    return;
  }
  // no more callbacks after unsubscribe
  XLOG(DBG1) << "unsubscribing from ftn=" << trackIt->second->fullTrackName()
             << " sess=" << this;
  // if there are open streams for this subscription, we should STOP_SENDING
  // them?
  auto res = writeUnsubscribe(controlWriteBuf_, std::move(unsubscribe));
  if (!res) {
    XLOG(ERR) << "writeUnsubscribe failed sess=" << this;
    return;
  }
  // we rely on receiving subscribeDone after unsubscribe to remove from
  // subTracks_
  controlWriteEvent_.signal();
}

folly::Expected<folly::Unit, MoQPublishError>
MoQSession::PublisherImpl::subscribeDone(SubscribeDone subscribeDone) {
  auto session = session_;
  session_ = nullptr;
  session->subscribeDone(std::move(subscribeDone));
  return folly::unit;
}

void MoQSession::subscribeDone(SubscribeDone subDone) {
  XLOG(DBG1) << __func__ << " sess=" << this;
  auto it = pubTracks_.find(subDone.subscribeID);
  if (it == pubTracks_.end()) {
    XLOG(ERR) << "subscribeDone for invalid id=" << subDone.subscribeID
              << " sess=" << this;
    return;
  }
  pubTracks_.erase(it);
  auto res = writeSubscribeDone(controlWriteBuf_, std::move(subDone));
  if (!res) {
    XLOG(ERR) << "writeSubscribeDone failed sess=" << this;
    // TODO: any control write failure should probably result in close()
    return;
  }

  retireSubscribeId(/*signalWriteLoop=*/false);
  controlWriteEvent_.signal();
}

void MoQSession::retireSubscribeId(bool signalWriteLoop) {
  // If # of closed subscribes is greater than 1/2 of max subscribes, then
  // let's bump the maxSubscribeID by the number of closed subscribes.
  if (++closedSubscribes_ >= maxConcurrentSubscribes_ / 2) {
    maxSubscribeID_ += closedSubscribes_;
    closedSubscribes_ = 0;
    sendMaxSubscribeID(signalWriteLoop);
  }
}

void MoQSession::sendMaxSubscribeID(bool signal) {
  XLOG(DBG1) << "Issuing new maxSubscribeID=" << maxSubscribeID_
             << " sess=" << this;
  auto res =
      writeMaxSubscribeId(controlWriteBuf_, {.subscribeID = maxSubscribeID_});
  if (!res) {
    XLOG(ERR) << "writeMaxSubscribeId failed sess=" << this;
    return;
  }
  if (signal) {
    controlWriteEvent_.signal();
  }
}

void MoQSession::PublisherImpl::fetchComplete() {
  auto session = session_;
  session_ = nullptr;
  session->fetchComplete(subscribeID_);
}

void MoQSession::fetchComplete(SubscribeID subscribeID) {
  XLOG(DBG1) << __func__ << " sess=" << this;
  auto it = pubTracks_.find(subscribeID);
  if (it == pubTracks_.end()) {
    XLOG(ERR) << "fetchComplete for invalid id=" << subscribeID
              << " sess=" << this;
    return;
  }
  pubTracks_.erase(it);
  retireSubscribeId(/*signalWriteLoop=*/true);
}

void MoQSession::subscribeUpdate(SubscribeUpdate subUpdate) {
  XLOG(DBG1) << __func__ << " sess=" << this;
  auto trackAliasIt = subIdToTrackAlias_.find(subUpdate.subscribeID);
  if (trackAliasIt == subIdToTrackAlias_.end()) {
    // unknown
    XLOG(ERR) << "No matching subscribe ID=" << subUpdate.subscribeID
              << " sess=" << this;
    return;
  }
  auto trackIt = subTracks_.find(trackAliasIt->second);
  if (trackIt == subTracks_.end()) {
    // unknown
    XLOG(ERR) << "No matching subscribe ID=" << subUpdate.subscribeID
              << " sess=" << this;
    return;
  }
  auto res = writeSubscribeUpdate(controlWriteBuf_, std::move(subUpdate));
  if (!res) {
    XLOG(ERR) << "writeSubscribeUpdate failed sess=" << this;
    return;
  }
  controlWriteEvent_.signal();
}

folly::coro::Task<
    folly::Expected<std::shared_ptr<MoQSession::TrackHandle>, FetchError>>
MoQSession::fetch(Fetch fetch) {
  XLOG(DBG1) << __func__ << " sess=" << this;
  auto g =
      folly::makeGuard([func = __func__] { XLOG(DBG1) << "exit " << func; });
  auto fullTrackName = fetch.fullTrackName;
  if (nextSubscribeID_ >= peerMaxSubscribeID_) {
    XLOG(WARN) << "Issuing fetch that will fail; nextSubscribeID_="
               << nextSubscribeID_
               << " peerMaxSubscribeid_=" << peerMaxSubscribeID_
               << " sess=" << this;
  }
  auto subID = nextSubscribeID_++;
  fetch.subscribeID = subID;
  auto wres = writeFetch(controlWriteBuf_, std::move(fetch));
  if (!wres) {
    XLOG(ERR) << "writeFetch failed sess=" << this;
    co_return folly::makeUnexpected(
        FetchError({subID, 500, "local write failed"}));
  }
  controlWriteEvent_.signal();
  auto subTrack = fetches_.emplace(
      std::piecewise_construct,
      std::forward_as_tuple(subID),
      std::forward_as_tuple(std::make_shared<TrackHandle>(
          fullTrackName, subID, evb_, cancellationSource_.getToken())));

  auto trackHandle = subTrack.first->second;
  trackHandle->setNewObjectTimeout(std::chrono::seconds(2));
  auto res = co_await trackHandle->fetchReady();
  XLOG(DBG1) << __func__ << " fetchReady trackHandle=" << trackHandle;
  co_return res;
}

std::shared_ptr<FetchConsumer> MoQSession::fetchOk(FetchOk fetchOk) {
  XLOG(DBG1) << __func__ << " sess=" << this;
  auto it = pubTracks_.find(fetchOk.subscribeID);
  if (it == pubTracks_.end()) {
    XLOG(ERR) << "Invalid Fetch OK, id=" << fetchOk.subscribeID;
    return nullptr;
  }
  auto fetchPublisher = dynamic_cast<FetchPublisherImpl*>(it->second.get());
  if (!fetchPublisher) {
    XLOG(ERR) << "subscribe ID maps to a subscribe, not a fetch, id="
              << fetchOk.subscribeID;
    fetchError(
        {fetchOk.subscribeID,
         folly::to_underlying(FetchErrorCode::INTERNAL_ERROR),
         ""});
    return nullptr;
  }
  auto fetchConsumer = fetchPublisher->beginFetch(fetchOk.groupOrder);
  if (!fetchConsumer) {
    XLOG(ERR) << "beginFetch Failed, id=" << fetchOk.subscribeID;
    fetchError(
        {fetchOk.subscribeID,
         folly::to_underlying(FetchErrorCode::INTERNAL_ERROR),
         ""});
    return nullptr;
  }

  auto res = writeFetchOk(controlWriteBuf_, fetchOk);
  if (!res) {
    XLOG(ERR) << "writeFetchOk failed sess=" << this;
    return nullptr;
  }
  controlWriteEvent_.signal();
  return *fetchConsumer;
}

void MoQSession::fetchError(FetchError fetchErr) {
  XLOG(DBG1) << __func__ << " sess=" << this;
  if (pubTracks_.erase(fetchErr.subscribeID) == 0) {
    // fetchError is called sometimes before adding publisher state, so this
    // is not an error
    XLOG(DBG1) << "fetchErr for invalid id=" << fetchErr.subscribeID
               << " sess=" << this;
  }
  auto res = writeFetchError(controlWriteBuf_, std::move(fetchErr));
  if (!res) {
    XLOG(ERR) << "writeFetchError failed sess=" << this;
    return;
  }
  controlWriteEvent_.signal();
}

void MoQSession::fetchCancel(FetchCancel fetchCan) {
  XLOG(DBG1) << __func__ << " sess=" << this;
  auto trackIt = fetches_.find(fetchCan.subscribeID);
  if (trackIt == fetches_.end()) {
    XLOG(ERR) << "unknown subscribe ID=" << fetchCan.subscribeID
              << " sess=" << this;
    return;
  }
  auto res = writeFetchCancel(controlWriteBuf_, std::move(fetchCan));
  if (!res) {
    XLOG(ERR) << "writeFetchCancel failed sess=" << this;
    return;
  }
  controlWriteEvent_.signal();
}

void MoQSession::onNewUniStream(proxygen::WebTransport::StreamReadHandle* rh) {
  XLOG(DBG1) << __func__ << " sess=" << this;
  if (!setupComplete_) {
    XLOG(ERR) << "Uni stream before setup complete sess=" << this;
    close();
    return;
  }
  // maybe not STREAM_HEADER_SUBGROUP, but at least not control
  co_withCancellation(
      cancellationSource_.getToken(),
      readLoop(StreamType::STREAM_HEADER_SUBGROUP, rh))
      .scheduleOn(evb_)
      .start();
}

void MoQSession::onNewBidiStream(proxygen::WebTransport::BidiStreamHandle bh) {
  XLOG(DBG1) << __func__ << " sess=" << this;
  // TODO: prevent second control stream?
  if (dir_ == MoQControlCodec::Direction::CLIENT) {
    XLOG(ERR) << "Received bidi stream on client, kill it sess=" << this;
    bh.writeHandle->resetStream(/*error=*/0);
    bh.readHandle->stopSending(/*error=*/0);
  } else {
    bh.writeHandle->setPriority(0, 0, false);
    co_withCancellation(
        cancellationSource_.getToken(),
        readLoop(StreamType::CONTROL, bh.readHandle))
        .scheduleOn(evb_)
        .start();
    auto mergeToken = folly::CancellationToken::merge(
        cancellationSource_.getToken(), bh.writeHandle->getCancelToken());
    folly::coro::co_withCancellation(
        std::move(mergeToken), controlWriteLoop(bh.writeHandle))
        .scheduleOn(evb_)
        .start();
  }
}

void MoQSession::onDatagram(std::unique_ptr<folly::IOBuf> datagram) {
  XLOG(DBG1) << __func__ << " sess=" << this;
  folly::IOBufQueue readBuf{folly::IOBufQueue::cacheChainLength()};
  readBuf.append(std::move(datagram));
  folly::io::Cursor cursor(readBuf.front());
  auto type = quic::decodeQuicInteger(cursor);
  if (!type || StreamType(type->first) != StreamType::OBJECT_DATAGRAM) {
    XLOG(ERR) << __func__ << " Bad datagram header";
    close(SessionCloseErrorCode::PROTOCOL_VIOLATION);
    return;
  }
  auto dgLength = readBuf.chainLength();
  auto res = parseObjectHeader(cursor, dgLength);
  if (res.hasError()) {
    XLOG(ERR) << __func__ << " Bad Datagram: Failed to parse object header";
    close(SessionCloseErrorCode::PROTOCOL_VIOLATION);
    return;
  }
  auto remainingLength = cursor.totalLength();
  if (remainingLength != *res->length) {
    XLOG(ERR) << __func__ << " Bad datagram: Length mismatch";
    close(SessionCloseErrorCode::PROTOCOL_VIOLATION);
    return;
  }
  readBuf.trimStart(dgLength - remainingLength);
  auto alias = std::get_if<TrackAlias>(&res->trackIdentifier);
  XCHECK(alias);
  auto track = getTrack(*alias);
  if (track) {
    auto groupID = res->group;
    auto objID = res->id;
    track->onObjectHeader(std::move(*res));
    track->onObjectPayload(groupID, objID, readBuf.move(), true);
  }
}

bool MoQSession::closeSessionIfSubscribeIdInvalid(SubscribeID subscribeID) {
  if (maxSubscribeID_ <= subscribeID.value) {
    XLOG(ERR) << "Invalid subscribeID: " << subscribeID << " sess=" << this;
    close(SessionCloseErrorCode::TOO_MANY_SUBSCRIBES);
    return true;
  }
  return false;
}

/*static*/
uint64_t MoQSession::getMaxSubscribeIdIfPresent(
    const std::vector<SetupParameter>& params) {
  for (const auto& param : params) {
    if (param.key == folly::to_underlying(SetupKey::MAX_SUBSCRIBE_ID)) {
      return param.asUint64;
    }
  }
  return 0;
}
} // namespace moxygen

/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/MoQSession.h"
#include <folly/coro/Collect.h>
#include <folly/coro/FutureUtil.h>
#include <folly/io/async/EventBase.h>
#include <quic/common/CircularDeque.h>

#include <folly/logging/xlog.h>

#include <utility>

namespace {
using namespace moxygen;
constexpr std::chrono::seconds kSetupTimeout(5);
constexpr uint64_t kMaxSendTokenCacheSize(1024);

constexpr uint32_t IdMask = 0x1FFFFF;
uint32_t groupPriorityBits(GroupOrder groupOrder, uint64_t group) {
  // If the group order is oldest first, we want to give lower group
  // ids a higher precedence. Otherwise, if it is newest first, we want
  // to give higher group ids a higher precedence.
  uint32_t truncGroup = static_cast<uint32_t>(group) & IdMask;
  return groupOrder == GroupOrder::OldestFirst ? truncGroup
                                               : (IdMask - truncGroup);
}

uint32_t subgroupPriorityBits(uint32_t subgroupID) {
  return static_cast<uint32_t>(subgroupID) & IdMask;
}

/*
 * The spec mentions that scheduling should go as per
 * the following precedence list:
 * (1) Higher subscriber priority
 * (2) Higher publisher priority
 * (3) Group order, if the objects belong to different groups
 * (4) Lowest subgroup id
 *
 * This function takes in the relevant parameters and encodes them into a stream
 * priority so that we respect the aforementioned precedence order when we are
 * sending objects.
 */
uint64_t getStreamPriority(
    uint64_t groupID,
    uint64_t subgroupID,
    uint8_t subPri,
    uint8_t pubPri,
    GroupOrder pubGroupOrder) {
  // 6 reserved bits | 58 bit order
  // 6 reserved | 8 sub pri | 8 pub pri | 21 group order | 21 obj order
  uint32_t groupBits = groupPriorityBits(pubGroupOrder, groupID);
  uint32_t subgroupBits = subgroupPriorityBits(subgroupID);
  return (
      (uint64_t(subPri) << 50) | (uint64_t(pubPri) << 42) | (groupBits << 21) |
      subgroupBits);
}

// Helper classes for publishing

// StreamPublisherImpl is for publishing to a single stream, either a Subgroup
// or a Fetch response.  It's of course illegal to mix-and-match the APIs, but
// the object is only handed to the application as either a SubgroupConsumer
// or a FetchConsumer
class StreamPublisherImpl
    : public SubgroupConsumer,
      public FetchConsumer,
      public std::enable_shared_from_this<StreamPublisherImpl>,
      public proxygen::WebTransport::ByteEventCallback {
 public:
  StreamPublisherImpl() = delete;

  // Fetch constructor - we defer creating the stream/writeHandle until the
  // first published object.
  explicit StreamPublisherImpl(
      std::shared_ptr<MoQSession::PublisherImpl> publisher,
      std::shared_ptr<MLogger> logger = nullptr,
      std::shared_ptr<DeliveryCallback> deliveryCallback = nullptr);

  // Subscribe constructor
  StreamPublisherImpl(
      std::shared_ptr<MoQSession::PublisherImpl> publisher,
      proxygen::WebTransport::StreamWriteHandle* writeHandle,
      TrackAlias alias,
      uint64_t groupID,
      uint64_t subgroupID,
      SubgroupIDFormat format,
      bool includeExtensions,
      std::shared_ptr<MLogger> logger = nullptr,
      std::shared_ptr<DeliveryCallback> deliveryCallback = nullptr);

  // SubgroupConsumer overrides
  // Note where the interface uses finSubgroup, this class uses finStream,
  // since it is used for fetch and subgroups
  folly::Expected<folly::Unit, MoQPublishError> object(
      uint64_t objectID,
      Payload payload,
      Extensions extensions,
      bool finStream) override;
  folly::Expected<folly::Unit, MoQPublishError> objectNotExists(
      uint64_t objectID,
      Extensions extensions,
      bool finStream) override;
  folly::Expected<folly::Unit, MoQPublishError> beginObject(
      uint64_t objectId,
      uint64_t length,
      Payload initialPayload,
      Extensions extensions) override;
  folly::Expected<ObjectPublishStatus, MoQPublishError> objectPayload(
      Payload payload,
      bool finStream) override;
  folly::Expected<folly::Unit, MoQPublishError> endOfGroup(
      uint64_t endOfGroupObjectId,
      Extensions extensions) override;
  folly::Expected<folly::Unit, MoQPublishError> endOfTrackAndGroup(
      uint64_t endOfTrackObjectId,
      Extensions extensions) override;
  folly::Expected<folly::Unit, MoQPublishError> endOfSubgroup() override;
  void reset(ResetStreamErrorCode error) override;

  // FetchConsumer overrides
  folly::Expected<folly::Unit, MoQPublishError> object(
      uint64_t groupID,
      uint64_t subgroupID,
      uint64_t objectID,
      Payload payload,
      Extensions extensions,
      bool finFetch) override {
    if (!setGroupAndSubgroup(groupID, subgroupID)) {
      return folly::makeUnexpected(
          MoQPublishError(MoQPublishError::API_ERROR, "Group moved back"));
    }
    header_.status = ObjectStatus::NORMAL;
    return object(
        objectID, std::move(payload), std::move(extensions), finFetch);
  }

  folly::Expected<folly::Unit, MoQPublishError> objectNotExists(
      uint64_t groupID,
      uint64_t subgroupID,
      uint64_t objectID,
      Extensions extensions,
      bool finFetch) override {
    if (!setGroupAndSubgroup(groupID, subgroupID)) {
      return folly::makeUnexpected(
          MoQPublishError(MoQPublishError::API_ERROR, "Group moved back"));
    }
    return objectNotExists(objectID, std::move(extensions), finFetch);
  }

  folly::Expected<folly::Unit, MoQPublishError> groupNotExists(
      uint64_t groupID,
      uint64_t subgroupID,
      Extensions extensions,
      bool finFetch) override {
    if (!setGroupAndSubgroup(groupID, subgroupID)) {
      return folly::makeUnexpected(
          MoQPublishError(MoQPublishError::API_ERROR, "Group moved back"));
    }
    return publishStatus(
        0, ObjectStatus::GROUP_NOT_EXIST, extensions, finFetch);
  }

  folly::Expected<folly::Unit, MoQPublishError> beginObject(
      uint64_t groupID,
      uint64_t subgroupID,
      uint64_t objectID,
      uint64_t length,
      Payload initialPayload,
      Extensions extensions) override {
    if (!setGroupAndSubgroup(groupID, subgroupID)) {
      return folly::makeUnexpected(
          MoQPublishError(MoQPublishError::API_ERROR, "Group moved back"));
    }
    header_.status = ObjectStatus::NORMAL;
    return beginObject(
        objectID, length, std::move(initialPayload), std::move(extensions));
  }

  folly::Expected<folly::Unit, MoQPublishError> endOfGroup(
      uint64_t groupID,
      uint64_t subgroupID,
      uint64_t objectID,
      Extensions extensions,
      bool finFetch) override {
    if (!setGroupAndSubgroup(groupID, subgroupID)) {
      return folly::makeUnexpected(
          MoQPublishError(MoQPublishError::API_ERROR, "Group moved back"));
    }
    return publishStatus(
        objectID, ObjectStatus::END_OF_GROUP, extensions, finFetch);
  }
  folly::Expected<folly::Unit, MoQPublishError> endOfTrackAndGroup(
      uint64_t groupID,
      uint64_t subgroupID,
      uint64_t objectID,
      Extensions extensions) override {
    if (!setGroupAndSubgroup(groupID, subgroupID)) {
      return folly::makeUnexpected(
          MoQPublishError(MoQPublishError::API_ERROR, "Group moved back"));
    }
    return endOfTrackAndGroup(objectID, std::move(extensions));
  }
  folly::Expected<folly::Unit, MoQPublishError> endOfFetch() override {
    if (!writeHandle_) {
      return folly::makeUnexpected(
          MoQPublishError(MoQPublishError::CANCELLED, "Fetch cancelled"));
    }
    return endOfSubgroup();
  }

  folly::Expected<folly::SemiFuture<uint64_t>, MoQPublishError>
  awaitReadyToConsume() override;

  folly::Expected<folly::Unit, MoQPublishError> publishStatus(
      uint64_t objectID,
      ObjectStatus status,
      const Extensions& extensions,
      bool finStream);

  void onByteEvent(quic::StreamId id, uint64_t offset) noexcept override {
    // Notify delivery callback for all objects delivered up to this offset
    if (deliveryCallback_) {
      while (!pendingDeliveries_.empty()) {
        auto& pendingDelivery = pendingDeliveries_.front();

        if (pendingDelivery.endOffset <= offset) {
          folly::Optional<TrackAlias> maybeTrackAlias = folly::none;
          if (streamType_ != StreamType::FETCH_HEADER) {
            maybeTrackAlias = trackAlias_;
          }
          deliveryCallback_->onDelivered(
              maybeTrackAlias,
              pendingDelivery.groupId,
              pendingDelivery.subgroupId,
              pendingDelivery.objectId);
          pendingDeliveries_.pop_front();
        } else {
          break;
        }
      }
    }
    onByteEventCommon(id, offset);
  }

  void onByteEventCanceled(quic::StreamId id, uint64_t offset) noexcept
      override {
    // Notify delivery cancelled for all objects delivered after this offset.
    // If delivery has been cancelled for an offset (e.g. by a reset), then
    // delivery must be cancelled for all offsets higher than the cancelled
    // offset as well;
    if (deliveryCallback_) {
      while (!pendingDeliveries_.empty()) {
        auto& pendingDelivery = pendingDeliveries_.back();

        if (pendingDelivery.endOffset >= offset) {
          folly::Optional<TrackAlias> maybeTrackAlias = folly::none;
          if (streamType_ != StreamType::FETCH_HEADER) {
            maybeTrackAlias = trackAlias_;
          }
          deliveryCallback_->onDeliveryCancelled(
              maybeTrackAlias,
              pendingDelivery.groupId,
              pendingDelivery.subgroupId,
              pendingDelivery.objectId);
          pendingDeliveries_.pop_back();
        } else {
          break;
        }
      }
    }
    onByteEventCommon(id, offset);
  }

  void setForward(bool forwardIn) {
    forward_ = forwardIn;
  }

 private:
  std::shared_ptr<MLogger> logger_ = nullptr;
  std::shared_ptr<DeliveryCallback> deliveryCallback_ = nullptr;

  // Track objects and their end offsets for delivery callbacks
  struct ObjectDeliveryInfo {
    uint64_t groupId;
    uint64_t subgroupId;
    uint64_t objectId;
    uint64_t endOffset;

    ObjectDeliveryInfo(uint64_t g, uint64_t sg, uint64_t o, uint64_t offset)
        : groupId(g), subgroupId(sg), objectId(o), endOffset(offset) {}
  };
  quic::CircularDeque<ObjectDeliveryInfo> pendingDeliveries_;

  void onByteEventCommon(quic::StreamId id, uint64_t offset) {
    uint64_t bytesDeliveredOrCanceled = offset + 1;
    if (bytesDeliveredOrCanceled > bytesDeliveredOrCanceled_) {
      publisher_->onBytesUnbuffered(
          bytesDeliveredOrCanceled - bytesDeliveredOrCanceled_);
      bytesDeliveredOrCanceled_ = bytesDeliveredOrCanceled;
    }

    refCountForCallbacks_--;
    if (refCountForCallbacks_ == 0) {
      keepaliveForDeliveryCallbacks_.reset();
    }
  }

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

  folly::Expected<folly::Unit, MoQPublishError> ensureWriteHandle();

  void setWriteHandle(proxygen::WebTransport::StreamWriteHandle* writeHandle);

  folly::Expected<folly::Unit, MoQPublishError> validatePublish(
      uint64_t objectID);
  folly::Expected<ObjectPublishStatus, MoQPublishError>
  validateObjectPublishAndUpdateState(folly::IOBuf* payload, bool finStream);
  folly::Expected<folly::Unit, MoQPublishError> writeCurrentObject(
      uint64_t objectID,
      uint64_t length,
      Payload payload,
      const Extensions& extensions,
      bool finStream);
  folly::Expected<folly::Unit, MoQPublishError> writeToStream(
      bool finStream,
      bool endObject = false);

  void onStreamComplete();

  std::shared_ptr<MoQSession::PublisherImpl> publisher_{nullptr};
  bool streamComplete_{false};
  folly::Optional<folly::CancellationCallback> cancelCallback_;
  proxygen::WebTransport::StreamWriteHandle* writeHandle_{nullptr};
  StreamType streamType_;
  TrackAlias trackAlias_{0}; // Store track alias separately from ObjectHeader
  ObjectHeader header_;
  folly::Optional<uint64_t> currentLengthRemaining_;
  folly::IOBufQueue writeBuf_{folly::IOBufQueue::cacheChainLength()};
  MoQFrameWriter moqFrameWriter_;

  std::shared_ptr<StreamPublisherImpl> keepaliveForDeliveryCallbacks_{nullptr};
  uint32_t refCountForCallbacks_{0};

  uint32_t bytesWritten_{0};
  uint32_t bytesDeliveredOrCanceled_{0};

  bool forward_{true};
};

// StreamPublisherImpl

StreamPublisherImpl::StreamPublisherImpl(
    std::shared_ptr<MoQSession::PublisherImpl> publisher,
    std::shared_ptr<MLogger> logger,
    std::shared_ptr<DeliveryCallback> deliveryCallback)
    : publisher_(publisher),
      streamType_(StreamType::FETCH_HEADER),
      header_(
          0,
          0,
          std::numeric_limits<uint64_t>::max(),
          0,
          ObjectStatus::NORMAL) {
  logger_ = logger;
  deliveryCallback_ = deliveryCallback;
  moqFrameWriter_.initializeVersion(publisher->getVersion());
  (void)moqFrameWriter_.writeFetchHeader(writeBuf_, publisher->requestID());
}

StreamPublisherImpl::StreamPublisherImpl(
    std::shared_ptr<MoQSession::PublisherImpl> publisher,
    proxygen::WebTransport::StreamWriteHandle* writeHandle,
    TrackAlias alias,
    uint64_t groupID,
    uint64_t subgroupID,
    SubgroupIDFormat format,
    bool includeExtensions,
    std::shared_ptr<MLogger> logger,
    std::shared_ptr<DeliveryCallback> deliveryCallback)
    : StreamPublisherImpl(publisher, nullptr, deliveryCallback) {
  CHECK(writeHandle)
      << "For a SUBSCRIBE, you need to pass in a non-null writeHandle";
  streamType_ = getSubgroupStreamType(
      publisher->getVersion(), format, includeExtensions, false);
  trackAlias_ = alias;
  setWriteHandle(writeHandle);
  setGroupAndSubgroup(groupID, subgroupID);
  logger_ = logger;
  if (logger_) {
    logger_->logStreamTypeSet(
        writeHandle->getID(), MOQTStreamType::SUBGROUP_HEADER, Owner::LOCAL);
    logger_->logSubgroupHeaderCreated(
        writeHandle->getID(),
        alias,
        groupID,
        subgroupID,
        publisher->subPriority());
  }

  writeBuf_.move(); // clear FETCH_HEADER
  (void)moqFrameWriter_.writeSubgroupHeader(
      writeBuf_, trackAlias_, header_, format, includeExtensions);
}

// Private methods

void StreamPublisherImpl::setWriteHandle(
    proxygen::WebTransport::StreamWriteHandle* writeHandle) {
  XCHECK(publisher_);
  XCHECK(!streamComplete_);
  XCHECK(!writeHandle_);
  writeHandle_ = writeHandle;
  if (streamType_ == StreamType::FETCH_HEADER && logger_) {
    logger_->logStreamTypeSet(
        writeHandle_->getID(), MOQTStreamType::FETCH_HEADER, Owner::LOCAL);
    RequestID req = publisher_->requestID();
    logger_->logFetchHeaderCreated(writeHandle_->getID(), req.value);
  }

  cancelCallback_.emplace(writeHandle_->getCancelToken(), [this] {
    if (writeHandle_) {
      auto code = writeHandle_->stopSendingErrorCode();
      XLOG(DBG1) << "Peer requested write termination code="
                 << (code ? folly::to<std::string>(*code)
                          : std::string("none"));
      reset(ResetStreamErrorCode::CANCELLED);
    }
  });
  if (publisher_) {
    publisher_->onStreamCreated();
  }
}

void StreamPublisherImpl::onStreamComplete() {
  XCHECK_EQ(writeHandle_, nullptr);
  streamComplete_ = true;

  if (publisher_) {
    publisher_->onStreamComplete(header_);
  }
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
  return ensureWriteHandle();
}

folly::Expected<folly::Unit, MoQPublishError>
StreamPublisherImpl::writeCurrentObject(
    uint64_t objectID,
    uint64_t length,
    Payload payload,
    const Extensions& extensions,
    bool finStream) {
  header_.id = objectID;
  header_.length = length;
  // copy is gratuitous
  header_.extensions = extensions;
  XLOG(DBG6) << "writeCurrentObject sgp=" << this << " objectID=" << objectID;
  bool entireObjectWritten = (!currentLengthRemaining_.hasValue());
  (void)moqFrameWriter_.writeStreamObject(
      writeBuf_, streamType_, header_, std::move(payload));
  return writeToStream(finStream, entireObjectWritten);
}

folly::Expected<folly::Unit, MoQPublishError>
StreamPublisherImpl::writeToStream(bool finStream, bool endObject) {
  if (streamType_ != StreamType::FETCH_HEADER &&
      !publisher_->canBufferBytes(writeBuf_.chainLength())) {
    publisher_->onTooManyBytesBuffered();
    return folly::makeUnexpected(
        MoQPublishError(MoQPublishError::TOO_FAR_BEHIND));
  }

  if (!writeHandle_) {
    return folly::makeUnexpected(MoQPublishError(MoQPublishError::CANCELLED));
  }

  auto writeHandle = writeHandle_;
  if (finStream) {
    writeHandle_ = nullptr;
  }

  proxygen::WebTransport::ByteEventCallback* deliveryCallback = nullptr;
  if (!writeBuf_.empty() || finStream) {
    deliveryCallback = this;

    if (deliveryCallback_ && endObject) {
      uint64_t endOffset = bytesWritten_ + writeBuf_.chainLength() - 1;
      pendingDeliveries_.emplace_back(
          header_.group, header_.subgroup, header_.id, endOffset);
    }

    bytesWritten_ += writeBuf_.chainLength();
    publisher_->onBytesBuffered(writeBuf_.chainLength());
    if (refCountForCallbacks_ == 0) {
      keepaliveForDeliveryCallbacks_ = shared_from_this();
    }
    refCountForCallbacks_++;
  }
  auto writeRes = writeHandle->writeStreamData(
      writeBuf_.move(), finStream, deliveryCallback);
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
    Extensions extensions,
    bool finStream) {
  if (!forward_) {
    reset(ResetStreamErrorCode::INTERNAL_ERROR);
    return folly::makeUnexpected(
        MoQPublishError(MoQPublishError::API_ERROR, "shouldForward is false"));
  }
  auto validateRes = validatePublish(objectID);
  if (!validateRes) {
    return validateRes;
  }
  auto length = payload ? payload->computeChainDataLength() : 0;
  header_.status = ObjectStatus::NORMAL;

  if (logger_) {
    if (streamType_ != StreamType::FETCH_HEADER) {
      logger_->logSubgroupObjectCreated(
          writeHandle_->getID(), trackAlias_, header_, payload->clone());
    } else {
      logger_->logFetchObjectCreated(
          writeHandle_->getID(), header_, payload->clone());
    }
  }

  return writeCurrentObject(
      objectID, length, std::move(payload), extensions, finStream);
}

folly::Expected<folly::Unit, MoQPublishError>
StreamPublisherImpl::objectNotExists(
    uint64_t objectID,
    Extensions extensions,
    bool finStream) {
  if (!forward_) {
    reset(ResetStreamErrorCode::INTERNAL_ERROR);
    return folly::makeUnexpected(
        MoQPublishError(MoQPublishError::API_ERROR, "shouldForward is false"));
  }
  return publishStatus(
      objectID, ObjectStatus::OBJECT_NOT_EXIST, extensions, finStream);
}

folly::Expected<folly::Unit, MoQPublishError> StreamPublisherImpl::beginObject(
    uint64_t objectID,
    uint64_t length,
    Payload initialPayload,
    Extensions extensions) {
  if (!forward_) {
    reset(ResetStreamErrorCode::INTERNAL_ERROR);
    return folly::makeUnexpected(
        MoQPublishError(MoQPublishError::API_ERROR, "shouldForward is false"));
  }
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
  header_.status = ObjectStatus::NORMAL;

  return writeCurrentObject(
      objectID,
      length,
      std::move(initialPayload),
      extensions,
      /*finStream=*/false);
}

folly::Expected<ObjectPublishStatus, MoQPublishError>
StreamPublisherImpl::objectPayload(Payload payload, bool finStream) {
  if (!forward_) {
    reset(ResetStreamErrorCode::INTERNAL_ERROR);
    return folly::makeUnexpected(
        MoQPublishError(MoQPublishError::API_ERROR, "shouldForward is false"));
  }
  if (!writeHandle_) {
    return folly::makeUnexpected(
        MoQPublishError(MoQPublishError::CANCELLED, "Cancelled"));
  }
  auto validateObjectPublishRes =
      validateObjectPublishAndUpdateState(payload.get(), finStream);
  if (!validateObjectPublishRes) {
    return validateObjectPublishRes;
  }
  writeBuf_.append(std::move(payload));
  bool entireObjectWritten = (!currentLengthRemaining_.hasValue());
  auto writeRes = writeToStream(finStream, entireObjectWritten);
  if (writeRes.hasValue()) {
    return validateObjectPublishRes.value();
  } else {
    return folly::makeUnexpected(writeRes.error());
  }
}

folly::Expected<folly::Unit, MoQPublishError> StreamPublisherImpl::endOfGroup(
    uint64_t endOfGroupObjectId,
    Extensions extensions) {
  if (!forward_) {
    reset(ResetStreamErrorCode::INTERNAL_ERROR);
    return folly::makeUnexpected(
        MoQPublishError(MoQPublishError::API_ERROR, "shouldForward is false"));
  }
  return publishStatus(
      endOfGroupObjectId,
      ObjectStatus::END_OF_GROUP,
      extensions,
      /*finStream=*/true);
}

folly::Expected<folly::Unit, MoQPublishError>
StreamPublisherImpl::endOfTrackAndGroup(
    uint64_t endOfTrackObjectId,
    Extensions extensions) {
  if (!forward_) {
    reset(ResetStreamErrorCode::INTERNAL_ERROR);
    return folly::makeUnexpected(
        MoQPublishError(MoQPublishError::API_ERROR, "shouldForward is false"));
  }
  return publishStatus(
      endOfTrackObjectId,
      ObjectStatus::END_OF_TRACK,
      extensions,
      /*finStream=*/true);
}

folly::Expected<folly::Unit, MoQPublishError>
StreamPublisherImpl::publishStatus(
    uint64_t objectID,
    ObjectStatus status,
    const Extensions& extensions,
    bool finStream) {
  if (!forward_) {
    reset(ResetStreamErrorCode::INTERNAL_ERROR);
    return folly::makeUnexpected(
        MoQPublishError(MoQPublishError::API_ERROR, "shouldForward is false"));
  }
  auto validateRes = validatePublish(objectID);
  if (!validateRes) {
    return validateRes;
  }
  header_.status = status;
  header_.length = folly::none;
  return writeCurrentObject(
      objectID,
      /*length=*/0,
      /*payload=*/nullptr,
      extensions,
      finStream);
}

folly::Expected<folly::Unit, MoQPublishError>
StreamPublisherImpl::endOfSubgroup() {
  if (!forward_) {
    reset(ResetStreamErrorCode::INTERNAL_ERROR);
    return folly::makeUnexpected(
        MoQPublishError(MoQPublishError::API_ERROR, "shouldForward is false"));
  }
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
  if (auto* wh = std::exchange(writeHandle_, nullptr)) {
    wh->resetStream(uint32_t(error));
  } else {
    // Can happen on STOP_SENDING or prior to first fetch write
    XLOG(ERR) << "reset with no write handle: sgp=" << this;
  }
  onStreamComplete();
}

folly::Expected<folly::SemiFuture<uint64_t>, MoQPublishError>
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

folly::Expected<folly::Unit, MoQPublishError>
StreamPublisherImpl::ensureWriteHandle() {
  if (writeHandle_) {
    return folly::unit;
  }
  if (streamComplete_) {
    return folly::makeUnexpected(MoQPublishError(
        MoQPublishError::API_ERROR, "Write after stream complete"));
  }
  XCHECK(publisher_) << "publisher_ has not been set";
  // This has to be FETCH, subscribe is created with a writeHandle_ and
  // publisher_ is cleared when the stream FIN's or resets.
  auto wt = publisher_->getWebTransport();
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
  // publisher group order is not known here, but it shouldn't matter
  // Currently sets group=0 for FETCH priority bits
  stream.value()->setPriority(
      1,
      getStreamPriority(
          0, 0, publisher_->subPriority(), 0, GroupOrder::OldestFirst),
      false);
  setWriteHandle(*stream);
  return folly::unit;
}

} // namespace

namespace moxygen {

class MoQSession::TrackPublisherImpl : public MoQSession::PublisherImpl,
                                       public TrackConsumer {
 public:
  TrackPublisherImpl() = delete;
  TrackPublisherImpl(
      MoQSession* session,
      FullTrackName fullTrackName,
      RequestID requestID,
      folly::Optional<TrackAlias> trackAlias,
      Priority subPriority,
      GroupOrder groupOrder,
      uint64_t version,
      uint64_t bytesBufferedThreshold,
      bool forward)
      : PublisherImpl(
            session,
            std::move(fullTrackName),
            requestID,
            subPriority,
            groupOrder,
            version,
            bytesBufferedThreshold),
        trackAlias_(trackAlias),
        forward_(forward) {}

  folly::Expected<folly::Unit, MoQPublishError> setTrackAlias(
      TrackAlias trackAlias) override {
    if (trackAlias_ && trackAlias != *trackAlias_) {
      return folly::makeUnexpected(MoQPublishError(
          MoQPublishError::API_ERROR,
          "Track Alias already set to different value"));
    }
    // TODO: there's now way to verify which aliases are already in use from the
    // publisher side
    trackAlias_ = trackAlias;
    return folly::unit;
  }

  void setSubscriptionHandle(
      std::shared_ptr<Publisher::SubscriptionHandle> handle) {
    if (!trackAlias_) {
      trackAlias_ = handle->subscribeOk().trackAlias;
    }
    subscriptionHandle_ = std::move(handle);
    if (pendingSubscribeDone_) {
      // If subscribeDone is called before publishHandler_->subscribe() returns,
      // catch the DONE here and defer it until after we send subscribe OK.
      auto subDone = std::move(*pendingSubscribeDone_);
      subDone.streamCount = streamCount_;
      pendingSubscribeDone_.reset();
      PublisherImpl::subscribeDone(std::move(subDone));
    }
  }

  // PublisherImpl overrides
  void onStreamCreated() override {
    streamCount_++;
  }

  void onStreamComplete(const ObjectHeader& finalHeader) override;

  void onTooManyBytesBuffered() override;

  void subscribeUpdate(SubscribeUpdate subscribeUpdate) {
    if (!subscriptionHandle_) {
      XLOG(ERR) << "Received SubscribeUpdate before sending SUBSCRIBE_OK id="
                << requestID_ << " trackPub=" << this;
      // TODO: I think we need to buffer it?
    } else {
      setForward(subscribeUpdate.forward);
      subscriptionHandle_->subscribeUpdate(std::move(subscribeUpdate));
    }
  }

  void setForward(bool forward) {
    forward_ = forward;
    for (const auto& [_, subgroupPublisher] : subgroups_) {
      subgroupPublisher->setForward(forward_);
    }
  }

  void unsubscribe() {
    if (!subscriptionHandle_) {
      XLOG(ERR) << "Received Unsubscribe before sending SUBSCRIBE_OK id="
                << requestID_ << " trackPub=" << this;
      // TODO: cancel handleSubscribe?
    } else {
      subscriptionHandle_->unsubscribe();
    }
    resetAllSubgroups(ResetStreamErrorCode::CANCELLED);
  }

  void terminatePublish(SubscribeDone subDone, ResetStreamErrorCode code)
      override {
    resetAllSubgroups(code);
    if (!subscriptionHandle_) {
      session_->subscribeError(
          {subDone.requestID, SubscribeErrorCode::INTERNAL_ERROR, "terminate"});
    } else {
      subscribeDone(std::move(subDone));
    }
  }

  void resetAllSubgroups(ResetStreamErrorCode code) {
    while (!subgroups_.empty()) {
      auto it = subgroups_.begin();
      // reset will invoke onStreamComplete, which erases from subgroups_
      it->second->reset(code);
    }
  }

  // TrackConsumer overrides
  folly::Expected<std::shared_ptr<SubgroupConsumer>, MoQPublishError>
  beginSubgroup(uint64_t groupID, uint64_t subgroupID, Priority priority)
      override;

  folly::Expected<std::shared_ptr<SubgroupConsumer>, MoQPublishError>
  beginSubgroup(
      uint64_t groupID,
      uint64_t subgroupID,
      Priority priority,
      SubgroupIDFormat format,
      bool includeExtensions);

  folly::Expected<folly::SemiFuture<folly::Unit>, MoQPublishError>
  awaitStreamCredit() override;

  folly::Expected<folly::Unit, MoQPublishError> objectStream(
      const ObjectHeader& header,
      Payload payload) override;

  folly::Expected<folly::Unit, MoQPublishError> groupNotExists(
      uint64_t groupID,
      uint64_t subgroup,
      Priority pri,
      Extensions extensions) override;

  folly::Expected<folly::Unit, MoQPublishError> datagram(
      const ObjectHeader& header,
      Payload payload) override;

  folly::Expected<folly::Unit, MoQPublishError> subscribeDone(
      SubscribeDone subDone) override;

  void setDeliveryCallback(std::shared_ptr<DeliveryCallback> callback) override;

  void setLogger(std::shared_ptr<MLogger> logger) {
    logger_ = logger;
  }

 private:
  std::shared_ptr<MLogger> logger_ = nullptr;
  std::shared_ptr<Subscriber::SubscriptionHandle> subscriptionHandle_;
  folly::Optional<TrackAlias> trackAlias_;
  folly::Optional<SubscribeDone> pendingSubscribeDone_;
  folly::F14FastMap<
      std::pair<uint64_t, uint64_t>,
      std::shared_ptr<StreamPublisherImpl>>
      subgroups_;
  uint64_t streamCount_{0};
  enum class State { OPEN, DONE };
  State state_{State::OPEN};
  bool forward_;
  std::shared_ptr<DeliveryCallback> deliveryCallback_;
};

class MoQSession::FetchPublisherImpl : public MoQSession::PublisherImpl {
 public:
  FetchPublisherImpl(
      MoQSession* session,
      FullTrackName fullTrackName,
      RequestID requestID,
      Priority subPriority,
      GroupOrder groupOrder,
      uint64_t version,
      uint64_t bytesBufferedThreshold)
      : PublisherImpl(
            session,
            std::move(fullTrackName),
            requestID,
            subPriority,
            groupOrder,
            version,
            bytesBufferedThreshold) {}

  void initialize() {
    streamPublisher_ = std::make_shared<StreamPublisherImpl>(
        shared_from_this(), logger_, nullptr);
  }

  std::shared_ptr<StreamPublisherImpl> getStreamPublisher() const {
    return streamPublisher_;
  }

  void setFetchHandle(std::shared_ptr<Publisher::FetchHandle> handle) {
    handle_ = std::move(handle);
  }

  bool isCancelled() const {
    return cancelled_;
  }

  void cancel() {
    cancelled_ = true;
    // reset -> onStreamComplete -> fetchComplete: handles pubTracks_.erase
    // and retireRequestID
    reset(ResetStreamErrorCode::CANCELLED);
    if (auto handle = std::exchange(handle_, nullptr)) {
      handle->fetchCancel();
    }
  }

  void terminatePublish(SubscribeDone, ResetStreamErrorCode error) override {
    reset(error);
  }

  void reset(ResetStreamErrorCode error) {
    if (streamPublisher_) {
      streamPublisher_->reset(error);
    }
  }

  void onStreamComplete(const ObjectHeader&) override {
    streamPublisher_.reset();
    PublisherImpl::fetchComplete();
  }

  void onTooManyBytesBuffered() override {
    // Right now, we don't do anything when we buffer too many bytes for a
    // FETCH.
  }

  void setLogger(std::shared_ptr<MLogger> logger) {
    logger_ = logger;
  }

 private:
  std::shared_ptr<MLogger> logger_;
  std::shared_ptr<Publisher::FetchHandle> handle_;
  std::shared_ptr<StreamPublisherImpl> streamPublisher_;
  bool cancelled_{false};
};

// TrackPublisherImpl

folly::Expected<std::shared_ptr<SubgroupConsumer>, MoQPublishError>
MoQSession::TrackPublisherImpl::beginSubgroup(
    uint64_t groupID,
    uint64_t subgroupID,
    Priority pubPriority) {
  return beginSubgroup(
      groupID, subgroupID, pubPriority, SubgroupIDFormat::Present, true);
}

folly::Expected<std::shared_ptr<SubgroupConsumer>, MoQPublishError>
MoQSession::TrackPublisherImpl::beginSubgroup(
    uint64_t groupID,
    uint64_t subgroupID,
    Priority pubPriority,
    SubgroupIDFormat format,
    bool includeExtensions) {
  if (!trackAlias_) {
    return folly::makeUnexpected(MoQPublishError(
        MoQPublishError::API_ERROR, "Must set track alias first"));
  }
  if (!forward_) {
    return folly::makeUnexpected(MoQPublishError(
        MoQPublishError::API_ERROR,
        "Cannot create subgroups for subscriptions with forward flag set to false"));
  }

  auto wt = getWebTransport();
  if (!wt || state_ != State::OPEN) {
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
  session_->onSubscriptionStreamOpened();
  stream.value()->setPriority(
      1,
      getStreamPriority(
          groupID, subgroupID, subPriority_, pubPriority, groupOrder_),
      false);
  auto subgroupPublisher = std::make_shared<StreamPublisherImpl>(
      shared_from_this(),
      *stream,
      *trackAlias_,
      groupID,
      subgroupID,
      format,
      includeExtensions,
      logger_,
      deliveryCallback_);
  // TODO: these are currently unused, but the intent might be to reset
  // open subgroups automatically from some path?
  subgroups_[{groupID, subgroupID}] = subgroupPublisher;

  return subgroupPublisher;
}

folly::Expected<folly::SemiFuture<folly::Unit>, MoQPublishError>
MoQSession::TrackPublisherImpl::awaitStreamCredit() {
  auto wt = getWebTransport();
  if (!wt || state_ != State::OPEN) {
    return folly::makeUnexpected(MoQPublishError(
        MoQPublishError::API_ERROR, "awaitStreamCredit after subscribeDone"));
  }
  return wt->awaitUniStreamCredit();
}

void MoQSession::TrackPublisherImpl::onStreamComplete(
    const ObjectHeader& finalHeader) {
  if (session_) {
    session_->onSubscriptionStreamClosed();
  }
  // The reason we need the keepalive is that when we call subgroups_.erase(),
  // we might end up erasing the last SubgroupPublisherImpl that has a
  // shared_ptr reference to this TrackPublisherImpl, causing the destruction of
  // the map while erasing from it.
  auto keepalive = shared_from_this();
  subgroups_.erase({finalHeader.group, finalHeader.subgroup});
}

void MoQSession::TrackPublisherImpl::onTooManyBytesBuffered() {
  // Note: There is one case in which this reset can be problematic. If some
  // streams have been created, but have been reset before the subgroup header
  // is sent out, then there can be a stream count discrepancy. We could, some
  // time in the future, change this to a reliable reset so that we're ensured
  // that the stream counts are consistent. We can also add in a timeout for the
  // stream count discrepancy so that the peer doesn't hang indefinitely waiting
  // for the stream counts to equalize.
  terminatePublish(
      SubscribeDone(
          {requestID_,
           SubscribeDoneStatusCode::TOO_FAR_BEHIND,
           streamCount_,
           "peer is too far behind"}),
      ResetStreamErrorCode::INTERNAL_ERROR);
}

folly::Expected<folly::Unit, MoQPublishError>
MoQSession::TrackPublisherImpl::objectStream(
    const ObjectHeader& objHeader,
    Payload payload) {
  if (!trackAlias_) {
    return folly::makeUnexpected(MoQPublishError(
        MoQPublishError::API_ERROR, "Must set track alias first"));
  }
  XCHECK(objHeader.status == ObjectStatus::NORMAL || !payload);
  Extensions extensions = objHeader.extensions;
  auto subgroup = beginSubgroup(
      objHeader.group,
      objHeader.subgroup,
      objHeader.priority,
      objHeader.subgroup == objHeader.id ? SubgroupIDFormat::FirstObject
                                         : SubgroupIDFormat::Present,
      !extensions.empty());
  if (subgroup.hasError()) {
    return folly::makeUnexpected(std::move(subgroup.error()));
  }

  switch (objHeader.status) {
    case ObjectStatus::NORMAL:
      return subgroup.value()->object(
          objHeader.id,
          std::move(payload),
          extensions,
          /*finSubgroup=*/true);
    case ObjectStatus::OBJECT_NOT_EXIST:
      return subgroup.value()->objectNotExists(
          objHeader.id, objHeader.extensions, /*finSubgroup=*/true);
    case ObjectStatus::GROUP_NOT_EXIST: {
      auto& subgroupPublisherImpl =
          static_cast<StreamPublisherImpl&>(*subgroup.value());
      return subgroupPublisherImpl.publishStatus(
          objHeader.id,
          objHeader.status,
          objHeader.extensions,
          /*finStream=*/true);
    }
    case ObjectStatus::END_OF_GROUP:
      return subgroup.value()->endOfGroup(objHeader.id);
    case ObjectStatus::END_OF_TRACK:
      return subgroup.value()->endOfTrackAndGroup(objHeader.id);
  }
  return folly::makeUnexpected(
      MoQPublishError(MoQPublishError::WRITE_ERROR, "unreachable"));
}

folly::Expected<folly::Unit, MoQPublishError>
MoQSession::TrackPublisherImpl::groupNotExists(
    uint64_t groupID,
    uint64_t subgroupID,
    Priority priority,
    Extensions extensions) {
  if (!forward_) {
    return folly::makeUnexpected(MoQPublishError(
        MoQPublishError::API_ERROR,
        "Cannot send status for subscriptions with forward flag set to false"));
  }
  return objectStream(
      ObjectHeader(
          groupID,
          subgroupID,
          0,
          priority,
          ObjectStatus::GROUP_NOT_EXIST,
          std::move(extensions)),
      nullptr);
}

folly::Expected<folly::Unit, MoQPublishError>
MoQSession::TrackPublisherImpl::datagram(
    const ObjectHeader& header,
    Payload payload) {
  if (!trackAlias_) {
    return folly::makeUnexpected(MoQPublishError(
        MoQPublishError::API_ERROR, "Must set track alias first"));
  }
  if (!forward_) {
    return folly::makeUnexpected(MoQPublishError(
        MoQPublishError::API_ERROR,
        "Cannot send datagrams for subscriptions with forward flag set to false"));
  }
  auto wt = getWebTransport();
  if (!wt || state_ != State::OPEN) {
    XLOG(ERR) << "Trying to publish after subscribeDone";
    return folly::makeUnexpected(MoQPublishError(
        MoQPublishError::API_ERROR, "Publish after subscribeDone"));
  }

  if (logger_) {
    if (header.status == ObjectStatus::NORMAL) {
      logger_->logObjectDatagramCreated(*trackAlias_, header, payload);
    } else {
      logger_->logObjectDatagramStatusCreated(*trackAlias_, header);
    }
  }

  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  uint64_t headerLength = 0;
  if (header.length) {
    headerLength = *header.length;
  } else if (header.status == ObjectStatus::NORMAL && payload) {
    headerLength = payload->computeChainDataLength();
  } // else 0 is fine
  DCHECK_EQ(headerLength, payload ? payload->computeChainDataLength() : 0);
  (void)moqFrameWriter_.writeDatagramObject(
      writeBuf,
      *trackAlias_,
      ObjectHeader(
          header.group,
          header.id,
          header.id,
          header.priority,
          header.status,
          header.extensions,
          headerLength),
      std::move(payload));
  // TODO: set priority when WT has an API for that
  auto res = wt->sendDatagram(writeBuf.move());
  if (res.hasError()) {
    return folly::makeUnexpected(
        MoQPublishError(MoQPublishError::WRITE_ERROR, "sendDatagram failed"));
  }
  return folly::unit;
}

folly::Expected<folly::Unit, MoQPublishError>
MoQSession::TrackPublisherImpl::subscribeDone(SubscribeDone subDone) {
  if (state_ != State::OPEN) {
    return folly::makeUnexpected(
        MoQPublishError(MoQPublishError::API_ERROR, "subscribeDone twice"));
  }
  state_ = State::DONE;
  subDone.requestID = requestID_;
  if (!subscriptionHandle_) {
    // subscribeDone called from inside the subscribe handler,
    // before subscribeOk.
    pendingSubscribeDone_ = std::move(subDone);
    return folly::unit;
  }
  subDone.streamCount = streamCount_;
  return PublisherImpl::subscribeDone(std::move(subDone));
}

void MoQSession::TrackPublisherImpl::setDeliveryCallback(
    std::shared_ptr<DeliveryCallback> callback) {
  deliveryCallback_ = std::move(callback);
}

// Receive State
class MoQSession::TrackReceiveStateBase {
 public:
  TrackReceiveStateBase(FullTrackName fullTrackName, RequestID requestID)
      : fullTrackName_(std::move(fullTrackName)), requestID_(requestID) {}

  ~TrackReceiveStateBase() = default;

  [[nodiscard]] const FullTrackName& fullTrackName() const {
    return fullTrackName_;
  }

  [[nodiscard]] RequestID getRequestID() const {
    return requestID_;
  }

  folly::CancellationToken getCancelToken() const {
    return cancelSource_.getToken();
  }

 protected:
  FullTrackName fullTrackName_;
  RequestID requestID_;
  folly::CancellationSource cancelSource_;
};

class MoQSession::SubscribeTrackReceiveState
    : public MoQSession::TrackReceiveStateBase {
 public:
  using SubscribeResult = folly::Expected<SubscribeOk, SubscribeError>;
  SubscribeTrackReceiveState(
      FullTrackName fullTrackName,
      RequestID requestID,
      std::shared_ptr<TrackConsumer> callback,
      std::shared_ptr<MLogger> logger = nullptr,
      bool publish = false)
      : TrackReceiveStateBase(std::move(fullTrackName), requestID),
        callback_(std::move(callback)),
        publish_(publish) {
    logger_ = logger;
  }

  bool isPublish() {
    return publish_;
  }

  folly::coro::Future<SubscribeResult> subscribeFuture() {
    auto contract = folly::coro::makePromiseContract<SubscribeResult>();
    subscribePromise_ = std::move(contract.first);
    return std::move(contract.second);
  }

  TrackAlias getTrackAlias() const {
    // This is only called when version < 12
    return TrackAlias(requestID_.value);
  }

  [[nodiscard]] const FullTrackName& fullTrackName() const {
    return fullTrackName_;
  }

  std::shared_ptr<TrackConsumer> getSubscribeCallback() const {
    return callback_;
  }

  void resetSubscribeCallback() {
    callback_.reset();
  }

  void cancel() {
    callback_.reset();
    cancelSource_.requestCancellation();
  }

  void subscribeOK(SubscribeOk subscribeOK) {
    subscribePromise_.setValue(std::move(subscribeOK));
  }

  void subscribeError(SubscribeError subErr) {
    XLOG(DBG1) << __func__ << " trackReceiveState=" << this;
    if (!subscribePromise_.isFulfilled()) {
      subErr.requestID = requestID_;
      subscribePromise_.setValue(folly::makeUnexpected(std::move(subErr)));
    } else {
      subscribeDone(
          {requestID_,
           SubscribeDoneStatusCode::SESSION_CLOSED,
           0, // forces immediately invoking the callback
           "closed locally"});
    }
  }

  // returns true if subscription can be removed from state
  bool onSubgroup(
      const std::shared_ptr<MoQSession>& session,
      TrackAlias alias) {
    if (logger_) {
      logger_->logStreamTypeSet(
          currentStreamId_, MOQTStreamType::SUBGROUP_HEADER, Owner::REMOTE);
    }
    streamCount_++;
    if (pendingSubscribeDone_ &&
        streamCount_ >= pendingSubscribeDone_->streamCount) {
      if (callback_) {
        callback_->subscribeDone(std::move(*pendingSubscribeDone_));
        pendingSubscribeDone_.reset();
      }
      session->removeSubscriptionState(alias, requestID_);
      return true;
    }
    return false;
  }

  // return true if subscription can be removed from state
  bool subscribeDone(SubscribeDone subDone) {
    XLOG(DBG1) << __func__ << " trackReceiveState=" << this;
    if (callback_) {
      if (subDone.streamCount > streamCount_) {
        XLOG(DBG1) << "Waiting for streams in flight, have=" << streamCount_
                   << " need=" << subDone.streamCount
                   << " trackReceiveState=" << this;
        pendingSubscribeDone_ = std::move(subDone);
        // TODO: timeout
        return false;
      } else {
        callback_->subscribeDone(std::move(subDone));
      }
    } // else, unsubscribe raced with subscribeDone and callback was removed
    return true;
  }

  void setCurrentStreamId(uint64_t id) {
    currentStreamId_ = id;
  }

 private:
  std::shared_ptr<MLogger> logger_ = nullptr;
  std::shared_ptr<TrackConsumer> callback_;
  folly::coro::Promise<SubscribeResult> subscribePromise_;
  folly::Optional<SubscribeDone> pendingSubscribeDone_;
  uint64_t streamCount_{0};
  uint64_t currentStreamId_{0};

  // Indicates whether receiveState is for Publish
  bool publish_;
};

class MoQSession::FetchTrackReceiveState
    : public MoQSession::TrackReceiveStateBase {
 public:
  using FetchResult = folly::Expected<FetchOk, FetchError>;
  FetchTrackReceiveState(
      FullTrackName fullTrackName,
      RequestID requestID,
      std::shared_ptr<FetchConsumer> fetchCallback,
      std::shared_ptr<MLogger> logger = nullptr)
      : TrackReceiveStateBase(std::move(fullTrackName), requestID),
        callback_(std::move(fetchCallback)) {
    logger_ = logger;
  }

  folly::coro::Future<FetchResult> fetchFuture() {
    auto contract = folly::coro::makePromiseContract<FetchResult>();
    promise_ = std::move(contract.first);
    return std::move(contract.second);
  }

  std::shared_ptr<FetchConsumer> getFetchCallback() const {
    return callback_;
  }

  void resetFetchCallback(const std::shared_ptr<MoQSession>& session) {
    callback_.reset();
    if (fetchOkAndAllDataReceived()) {
      session->fetches_.erase(requestID_);
      session->checkForCloseOnDrain();
    }
  }

  void cancel(const std::shared_ptr<MoQSession>& session) {
    cancelSource_.requestCancellation();
    fetchError({requestID_, FetchErrorCode::CANCELLED, "cancelled"});
    resetFetchCallback(session);
  }

  void fetchOK(FetchOk ok) {
    XLOG(DBG1) << __func__ << " trackReceiveState=" << this;
    promise_.setValue(std::move(ok));
  }

  void fetchError(FetchError fetchErr) {
    if (!promise_.isFulfilled()) {
      fetchErr.requestID = requestID_;
      promise_.setValue(folly::makeUnexpected(std::move(fetchErr)));
    } // there's likely a missing case here from shutdown
  }

  void onFetchHeader(RequestID requestID) {
    if (logger_) {
      logger_->logStreamTypeSet(
          currentStreamId_, MOQTStreamType::FETCH_HEADER, Owner::REMOTE);
      logger_->logFetchHeaderParsed(currentStreamId_, requestID.value);
    }
  }

  bool fetchOkAndAllDataReceived() const {
    return promise_.isFulfilled() && !callback_;
  }

  void setCurrentStreamId(uint64_t id) {
    currentStreamId_ = id;
  }

 private:
  std::shared_ptr<MLogger> logger_ = nullptr;
  std::shared_ptr<FetchConsumer> callback_;
  folly::coro::Promise<FetchResult> promise_;
  uint64_t currentStreamId_{0};
};

void MoQSession::PendingRequestState::deliverError(RequestID reqID) {
  switch (type_) {
    case Type::SUBSCRIBE_TRACK: {
      if (!storage_.subscribeTrack_->isPublish()) {
        storage_.subscribeTrack_->subscribeError(
            {reqID, SubscribeErrorCode::INTERNAL_ERROR, "session closed"});
      }
      break;
    }
    case Type::ANNOUNCE: {
      storage_.announce_.promise.setValue(folly::makeUnexpected(AnnounceError(
          {reqID, AnnounceErrorCode::INTERNAL_ERROR, "session closed"})));
      break;
    }
    case Type::SUBSCRIBE_ANNOUNCES: {
      storage_.subscribeAnnounces_.setValue(
          folly::makeUnexpected(SubscribeAnnouncesError(
              {reqID,
               SubscribeAnnouncesErrorCode::INTERNAL_ERROR,
               "session closed"})));
      break;
    }
    case Type::PUBLISH: {
      storage_.publish_.setValue(folly::makeUnexpected(PublishError(
          {reqID, PublishErrorCode::INTERNAL_ERROR, "session closed"})));
      break;
    }
    case Type::TRACK_STATUS: {
      storage_.trackStatus_.setValue(TrackStatus{
          .requestID = reqID,
          .fullTrackName = FullTrackName(),
          .statusCode = TrackStatusCode::TRACK_NOT_EXIST,
          .largestGroupAndObject = folly::none,
          .params = {}});
      break;
    }
  }
}

using folly::coro::co_awaitTry;
using folly::coro::co_error;

class MoQSession::SubscriberAnnounceCallback
    : public Subscriber::AnnounceCallback {
 public:
  SubscriberAnnounceCallback(MoQSession& session, const TrackNamespace& ns)
      : session_(session), trackNamespace_(ns) {}

  void announceCancel(AnnounceErrorCode errorCode, std::string reasonPhrase)
      override {
    session_.announceCancel(
        {trackNamespace_, errorCode, std::move(reasonPhrase)});
  }

 private:
  MoQSession& session_;
  TrackNamespace trackNamespace_;
};

class MoQSession::PublisherAnnounceHandle : public Subscriber::AnnounceHandle {
 public:
  PublisherAnnounceHandle(std::shared_ptr<MoQSession> session, AnnounceOk annOk)
      : Subscriber::AnnounceHandle(std::move(annOk)),
        session_(std::move(session)) {}
  PublisherAnnounceHandle(const PublisherAnnounceHandle&) = delete;
  PublisherAnnounceHandle& operator=(const PublisherAnnounceHandle&) = delete;
  PublisherAnnounceHandle(PublisherAnnounceHandle&&) = delete;
  PublisherAnnounceHandle& operator=(PublisherAnnounceHandle&&) = delete;
  ~PublisherAnnounceHandle() override {
    unannounce();
  }

  void unannounce() override {
    if (session_) {
      session_->unannounce({announceOk().trackNamespace});
      session_.reset();
    }
  }

 private:
  std::shared_ptr<MoQSession> session_;
};

// Constructors
MoQSession::MoQSession(
    folly::MaybeManagedPtr<proxygen::WebTransport> wt,
    std::shared_ptr<MoQExecutor> exec)
    : dir_(MoQControlCodec::Direction::CLIENT),
      wt_(wt),
      exec_(std::move(exec)),
      nextRequestID_(0),
      nextExpectedPeerRequestID_(1),
      controlCodec_(dir_, this) {}

MoQSession::MoQSession(
    folly::MaybeManagedPtr<proxygen::WebTransport> wt,
    ServerSetupCallback& serverSetupCallback,
    std::shared_ptr<MoQExecutor> exec)
    : dir_(MoQControlCodec::Direction::SERVER),
      wt_(wt),
      exec_(std::move(exec)),
      nextRequestID_(1),
      nextExpectedPeerRequestID_(0),
      serverSetupCallback_(&serverSetupCallback),
      controlCodec_(dir_, this) {}

MoQSession::~MoQSession() {
  cleanup();
  XLOG(DBG1) << __func__ << " sess=" << this;
}

void MoQSession::cleanup() {
  // TODO: Are these loops safe since they may (should?) delete elements
  for (auto& subAnn : subscribeAnnounces_) {
    subAnn.second->unsubscribeAnnounces();
  }
  subscribeAnnounces_.clear();
  for (auto& ann : subscriberAnnounces_) {
    ann.second->unannounce();
  }
  subscriberAnnounces_.clear();
  for (auto& ann : publisherAnnounces_) {
    if (ann.second) {
      ann.second->announceCancel(
          AnnounceErrorCode::INTERNAL_ERROR, "Session ended");
    }
  }
  publisherAnnounces_.clear();
  while (!pubTracks_.empty()) {
    auto pubTrack = pubTracks_.begin();
    pubTrack->second->terminatePublish(
        SubscribeDone(
            {pubTrack->first,
             SubscribeDoneStatusCode::SESSION_CLOSED,
             0,
             "Session Closed"}),
        ResetStreamErrorCode::SESSION_CLOSED);
  }
  for (auto& subTrack : subTracks_) {
    if (!subTrack.second->isPublish()) {
      subTrack.second->subscribeError(
          {/*TrackReceiveState fills in subId*/ 0,
           SubscribeErrorCode::INTERNAL_ERROR,
           "session closed"});
    }
  }
  subTracks_.clear();
  // We parse a subscribeDone after cleanup
  reqIdToTrackAlias_.clear();
  for (auto& fetch : fetches_) {
    // TODO: there needs to be a way to queue an error in TrackReceiveState,
    // both from here, when close races the FETCH stream, and from readLoop
    // where we get a reset.
    fetch.second->fetchError(
        {/*TrackReceiveState fills in subId*/ 0,
         FetchErrorCode::INTERNAL_ERROR,
         "session closed"});
  }
  fetches_.clear();
  // Handle all pending requests in consolidated map
  for (auto& [reqID, pendingState] : pendingRequests_) {
    pendingState.deliverError(reqID);
  }
  pendingRequests_.clear();
  if (!cancellationSource_.isCancellationRequested()) {
    XLOG(DBG1) << "requestCancellation from cleanup sess=" << this;
    cancellationSource_.requestCancellation();
  }
  bufferedDatagrams_.clear();
}

const folly::RequestToken& MoQSession::sessionRequestToken() {
  static folly::RequestToken token("moq_session");
  return token;
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

    if (logger_) {
      logger_->logStreamTypeSet(
          controlStream.readHandle->getID(),
          MOQTStreamType::CONTROL,
          Owner::LOCAL);
    }

    auto mergeToken = folly::cancellation_token_merge(
        cancellationSource_.getToken(),
        controlStream.writeHandle->getCancelToken());
    co_withExecutor(
        exec_.get(),
        co_withCancellation(
            std::move(mergeToken), controlWriteLoop(controlStream.writeHandle)))
        .start();
    co_withExecutor(
        exec_.get(),
        co_withCancellation(
            cancellationSource_.getToken(),
            controlReadLoop(controlStream.readHandle)))
        .start();
  }
}

void MoQSession::setLogger(const std::shared_ptr<MLogger>& logger) {
  logger_ = logger;
}

void MoQSession::drain() {
  XLOG(DBG1) << __func__ << " sess=" << this;
  draining_ = true;
  checkForCloseOnDrain();
}

void MoQSession::goaway(Goaway goaway) {
  if (!draining_) {
    if (logger_) {
      logger_->logGoaway(goaway);
    }
    moqFrameWriter_.writeGoaway(controlWriteBuf_, goaway);
    controlWriteEvent_.signal();
    drain();
  }
}

void MoQSession::checkForCloseOnDrain() {
  if (draining_ && fetches_.empty() && subTracks_.empty()) {
    close(SessionCloseErrorCode::NO_ERROR);
  }
}

void MoQSession::close(SessionCloseErrorCode error) {
  XLOG(DBG1) << __func__ << " sess=" << this;
  if (auto wt = std::exchange(wt_, nullptr)) {
    // TODO: The error code should be propagated to
    // whatever implemented proxygen::WebTransport.
    // TxnWebTransport current just ignores the errorCode
    cleanup();

    wt->closeSession(folly::to_underlying(error));
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
        XLOG(ERR) << "Unexpected exception: "
                  << folly::exceptionStr(res.exception());
        co_return;
      }
    }
    co_await folly::coro::co_safe_point;
    auto writeRes =
        controlStream->writeStreamData(controlWriteBuf_.move(), false, nullptr);
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
  folly::coro::Future<ServerSetup> setupFuture;
  std::tie(setupPromise_, setupFuture) =
      folly::coro::makePromiseContract<ServerSetup>();

  auto maxRequestID = getMaxRequestIDIfPresent(setup.params);

  if (shouldIncludeMoqtImplementationParam(setup.supportedVersions)) {
    setup.params.emplace_back(SetupParameter(
        {folly::to_underlying(SetupKey::MOQT_IMPLEMENTATION),
         getMoQTImplementationString(),
         0,
         {}}));
  }

  // Choose serialization version based on supported v11 vs v12
  uint64_t setupSerializationVersion = kVersionDraft12;
  for (auto supportedVersion : setup.supportedVersions) {
    auto majorVersion = getDraftMajorVersion(supportedVersion);
    if (majorVersion < 12) {
      setupSerializationVersion = kVersionDraft11;
      break;
    }
  }
  // This sets the receive token cache size, but it's necesarily empty
  controlCodec_.setMaxAuthTokenCacheSize(
      getMaxAuthTokenCacheSizeIfPresent(setup.params));
  // Optimistically registers params without knowing peer's capabilities
  if (getDraftMajorVersion(setupSerializationVersion) >= 12) {
    aliasifyAuthTokens(setup.params, setupSerializationVersion);
  }
  auto res =
      writeClientSetup(controlWriteBuf_, setup, setupSerializationVersion);
  if (!res) {
    XLOG(ERR) << "writeClientSetup failed sess=" << this;
    co_yield folly::coro::co_error(std::runtime_error("Failed to write setup"));
  }
  maxRequestID_ = maxRequestID;
  maxConcurrentRequests_ = maxRequestID_ / getRequestIDMultiplier();
  controlWriteEvent_.signal();

  auto deletedToken = cancellationSource_.getToken();
  auto token = co_await folly::coro::co_current_cancellation_token;
  auto mergeToken = folly::cancellation_token_merge(deletedToken, token);
  auto serverSetup = co_await co_awaitTry(co_withCancellation(
      mergeToken, folly::coro::timeout(std::move(setupFuture), kSetupTimeout)));
  if (mergeToken.isCancellationRequested()) {
    co_yield folly::coro::co_error(folly::OperationCancelled());
  }
  if (serverSetup.hasException()) {
    close(SessionCloseErrorCode::VERSION_NEGOTIATION_FAILED);
    XLOG(ERR) << "Setup Failed: "
              << folly::exceptionStr(serverSetup.exception());
    co_yield folly::coro::co_error(serverSetup.exception());
  }
  if (std::find(
          setup.supportedVersions.begin(),
          setup.supportedVersions.end(),
          serverSetup->selectedVersion) == setup.supportedVersions.end()) {
    close(SessionCloseErrorCode::VERSION_NEGOTIATION_FAILED);
    XLOG(ERR) << "Server chose a version that the client doesn't support";
    co_yield folly::coro::co_error(serverSetup.exception());
  }

  setupComplete_ = true;
  XLOG(DBG1) << "Negotiated Version=" << *getNegotiatedVersion();

  co_return *serverSetup;
}

void MoQSession::onServerSetup(ServerSetup serverSetup) {
  XCHECK(dir_ == MoQControlCodec::Direction::CLIENT);
  XLOG(DBG1) << __func__ << " sess=" << this;

  if (logger_) {
    logger_->logServerSetup(serverSetup, ControlMessageType::PARSED);
  }

  // Validate that server MUST NOT send AUTHORITY parameter
  auto authorityParam = std::find_if(
      serverSetup.params.begin(),
      serverSetup.params.end(),
      [](const SetupParameter& p) {
        return p.key == folly::to_underlying(SetupKey::AUTHORITY);
      });

  if (authorityParam != serverSetup.params.end()) {
    XLOG(ERR)
        << "Parameter AUTHORITY not allowed in server setup, closing session";
    close(SessionCloseErrorCode::INVALID_AUTHORITY);
    return;
  }

  initializeNegotiatedVersion(serverSetup.selectedVersion);
  peerMaxRequestID_ = getMaxRequestIDIfPresent(serverSetup.params);
  tokenCache_.setMaxSize(
      std::min(
          kMaxSendTokenCacheSize,
          getMaxAuthTokenCacheSizeIfPresent(serverSetup.params)),
      /*evict=*/true);
  setupPromise_.setValue(std::move(serverSetup));
}

void MoQSession::onClientSetup(ClientSetup clientSetup) {
  XCHECK(dir_ == MoQControlCodec::Direction::SERVER);
  XLOG(DBG1) << __func__ << " sess=" << this;

  if (logger_) {
    logger_->logClientSetup(clientSetup, ControlMessageType::PARSED);
  }

  auto moqtImplementation = getMoQTImplementationIfPresent(clientSetup.params);
  if (moqtImplementation) {
    XLOG(DBG1) << "Client MOQT_IMPLEMENTATION=" << *moqtImplementation
               << " sess=" << this;
  }

  peerMaxRequestID_ = getMaxRequestIDIfPresent(clientSetup.params);
  tokenCache_.setMaxSize(
      std::min(
          kMaxSendTokenCacheSize,
          getMaxAuthTokenCacheSizeIfPresent(clientSetup.params)),
      /*evict=*/true);

  auto serverSetup =
      serverSetupCallback_->onClientSetup(clientSetup, shared_from_this());
  if (!serverSetup.hasValue()) {
    auto errorMsg = serverSetup.exception().what();
    XLOG(ERR) << "Server setup callback failed sess=" << this
              << " err=" << errorMsg;
    close(SessionCloseErrorCode::VERSION_NEGOTIATION_FAILED);
    return;
  }

  // Validate authority with the negotiated version
  auto authorityValidation = serverSetupCallback_->validateAuthority(
      clientSetup, serverSetup->selectedVersion, shared_from_this());
  if (!authorityValidation.hasValue()) {
    SessionCloseErrorCode errorCode = authorityValidation.error();
    XLOG(ERR) << "Authority validation failed sess=" << this
              << " errorCode=" << static_cast<uint64_t>(errorCode);
    close(errorCode);
    return;
  }

  if (getDraftMajorVersion(serverSetup->selectedVersion) >= 14) {
    serverSetup->params.emplace_back(SetupParameter(
        {folly::to_underlying(SetupKey::MOQT_IMPLEMENTATION),
         getMoQTImplementationString(),
         0,
         {}}));
  }

  // Check if selected version is < 11 and handle appropriately
  auto majorVersion = getDraftMajorVersion(serverSetup->selectedVersion);
  if (majorVersion < 11) {
    XLOG(ERR) << "Selected version " << serverSetup->selectedVersion
              << " (major=" << majorVersion
              << ") is not supported. Minimum version is 11. sess=" << this;
    close(SessionCloseErrorCode::VERSION_NEGOTIATION_FAILED);
    return;
  }

  initializeNegotiatedVersion(serverSetup->selectedVersion);
  XLOG(DBG1) << "Negotiated Version=" << *getNegotiatedVersion();
  auto maxRequestID = getMaxRequestIDIfPresent(serverSetup->params);

  // This sets the receive cache size and may evict received tokens
  controlCodec_.setMaxAuthTokenCacheSize(
      getMaxAuthTokenCacheSizeIfPresent(serverSetup->params));
  aliasifyAuthTokens(serverSetup->params);
  auto res = writeServerSetup(
      controlWriteBuf_, *serverSetup, serverSetup->selectedVersion);
  if (!res) {
    XLOG(ERR) << "writeServerSetup failed sess=" << this;
    close(SessionCloseErrorCode::VERSION_NEGOTIATION_FAILED);
    return;
  }
  maxRequestID_ = maxRequestID;
  maxConcurrentRequests_ = maxRequestID_ / getRequestIDMultiplier();
  setupComplete_ = true;
  controlWriteEvent_.signal();
}

folly::coro::Task<void> MoQSession::controlReadLoop(
    proxygen::WebTransport::StreamReadHandle* readHandle) {
  XLOG(DBG1) << __func__ << " sess=" << this;
  auto g = folly::makeGuard([func = __func__, this] {
    XLOG(DBG1) << "exit " << func << " sess=" << this;
  });
  co_await folly::coro::co_safe_point;
  auto streamId = readHandle->getID();
  controlCodec_.setStreamId(streamId);

  bool fin = false;
  auto token = co_await folly::coro::co_current_cancellation_token;
  while (!fin && !token.isCancellationRequested()) {
    auto streamData =
        co_await co_awaitTry(readHandle->readStreamData().via(exec_.get()));
    if (streamData.hasException()) {
      XLOG(ERR) << folly::exceptionStr(streamData.exception())
                << " id=" << streamId << " sess=" << this;
      break;
    }
    if (streamData->data || streamData->fin) {
      try {
        controlCodec_.onIngress(std::move(streamData->data), streamData->fin);
      } catch (const std::exception& ex) {
        XLOG(FATAL) << "exception thrown from onIngress ex="
                    << folly::exceptionStr(ex);
      }
    }
    fin = streamData->fin;
    XLOG_IF(DBG3, fin) << "End of stream id=" << streamId << " sess=" << this;
  }
  // TODO: close session on control exit
}

std::shared_ptr<MoQSession::SubscribeTrackReceiveState>
MoQSession::getSubscribeTrackReceiveState(TrackAlias trackAlias) {
  auto trackIt = subTracks_.find(trackAlias);
  if (trackIt == subTracks_.end()) {
    // received an object for unknown track alias
    XLOG(ERR) << "unknown track alias=" << trackAlias << " sess=" << this;
    return nullptr;
  }
  return trackIt->second;
}

std::shared_ptr<MoQSession::FetchTrackReceiveState>
MoQSession::getFetchTrackReceiveState(RequestID requestID) {
  XLOG(DBG3) << "getTrack reqID=" << requestID;
  auto trackIt = fetches_.find(requestID);
  if (trackIt == fetches_.end()) {
    // received an object for unknown subscribe ID
    XLOG(ERR) << "unknown subscribe ID=" << requestID << " sess=" << this;
    return nullptr;
  }
  return trackIt->second;
}

namespace detail {
class ObjectStreamCallback : public MoQObjectStreamCodec::ObjectCallback {
  // TODO: MoQConsumers should have a "StreamConsumer" that both
  // SubgroupConsumer and FetchConsumer can inherit.  In that case we can
  // get rid of these templates.  It will also be easier for publishers.

  template <typename SubscribeMethod, typename FetchMethod, typename... Args>
  auto invokeCallback(
      SubscribeMethod smethod,
      FetchMethod fmethod,
      uint64_t groupID,
      uint64_t subgroupID,
      Args... args) {
    if (fetchState_) {
      return (fetchState_->getFetchCallback().get()->*fmethod)(
          groupID, subgroupID, std::forward<Args>(args)...);
    } else {
      return (subgroupCallback_.get()->*smethod)(std::forward<Args>(args)...);
    }
  }

  template <typename SubscribeMethod, typename FetchMethod, typename... Args>
  auto invokeCallbackNoGroup(
      SubscribeMethod smethod,
      FetchMethod fmethod,
      Args... args) {
    if (fetchState_) {
      return (fetchState_->getFetchCallback().get()->*fmethod)(
          std::forward<Args>(args)...);
    } else {
      return (subgroupCallback_.get()->*smethod)(std::forward<Args>(args)...);
    }
  }

 public:
  ObjectStreamCallback(
      std::shared_ptr<MoQSession> session,
      folly::CancellationToken& token)
      : session_(session), token_(token) {}

  void setCurrentStreamId(uint64_t id) {
    currentStreamId_ = id;
  }

  void setSubscribeTrackReceiveState(
      std::shared_ptr<MoQSession::SubscribeTrackReceiveState> state) {
    subscribeState_ = std::move(state);
  }

  void onSubgroup(
      TrackAlias alias,
      uint64_t group,
      uint64_t subgroup,
      Priority priority) override {
    XCHECK(subscribeState_);
    trackAlias_ = alias; // Store for use in onObjectBegin logging
    session_->onSubscriptionStreamOpenedByPeer();
    if (!subscribeState_) {
      error_ = MoQPublishError(
          MoQPublishError::CANCELLED, "Subgroup for unknown track");
      return;
    }

    token_ = folly::cancellation_token_merge(
        token_, subscribeState_->getCancelToken());
    auto callback = subscribeState_->getSubscribeCallback();
    if (!callback) {
      XLOG(DBG2) << "No callback for subgroup";
      return;
    }
    auto res = callback->beginSubgroup(group, subgroup, priority);
    if (res.hasValue()) {
      subgroupCallback_ = *res;
    } else {
      error_ = std::move(res.error());
    }
    if (logger_) {
      logger_->logSubgroupHeaderParsed(
          currentStreamId_, alias, group, subgroup, priority);
    }

    subscribeState_->setCurrentStreamId(currentStreamId_);
    subscribeState_->onSubgroup(session_, alias);
  }

  void onFetchHeader(RequestID requestID) override {
    fetchState_ = session_->getFetchTrackReceiveState(requestID);

    if (!fetchState_) {
      error_ = MoQPublishError(
          MoQPublishError::CANCELLED, "Fetch response for unknown track");
      return;
    }
    fetchState_->setCurrentStreamId(currentStreamId_);
    fetchState_->onFetchHeader(requestID);
    token_ =
        folly::cancellation_token_merge(token_, fetchState_->getCancelToken());
  }

  void onObjectBegin(
      uint64_t group,
      uint64_t subgroup,
      uint64_t objectID,
      Extensions extensions,
      uint64_t length,
      Payload initialPayload,
      bool objectComplete,
      bool streamComplete) override {
    if (isCancelled()) {
      return;
    }

    if (logger_) {
      ObjectHeader obj = ObjectHeader();
      obj.id = objectID;
      obj.group = group;
      obj.subgroup = subgroup;
      obj.status = ObjectStatus::NORMAL;
      obj.length = length;
      obj.extensions = extensions;
      if (objectComplete && subscribeState_) {
        logger_->logSubgroupObjectParsed(
            currentStreamId_, trackAlias_, obj, initialPayload->clone());
      } else if (objectComplete && fetchState_) {
        logger_->logFetchObjectParsed(
            currentStreamId_, obj, initialPayload->clone());
      } else {
        currentObj_ = obj;
      }
    }

    folly::Expected<folly::Unit, MoQPublishError> res{folly::unit};
    if (objectComplete) {
      res = invokeCallback(
          &SubgroupConsumer::object,
          &FetchConsumer::object,
          group,
          subgroup,
          objectID,
          std::move(initialPayload),
          std::move(extensions),
          streamComplete);
      if (streamComplete) {
        endOfSubgroup();
      }
    } else {
      res = invokeCallback(
          &SubgroupConsumer::beginObject,
          &FetchConsumer::beginObject,
          group,
          subgroup,
          objectID,
          length,
          std::move(initialPayload),
          std::move(extensions));
    }
    if (!res) {
      error_ = std::move(res.error());
    }
  }

  void onObjectPayload(Payload payload, bool objectComplete) override {
    if (isCancelled()) {
      return;
    }

    if (logger_ && objectComplete) {
      if (subscribeState_) {
        logger_->logSubgroupObjectParsed(
            currentStreamId_, trackAlias_, currentObj_, payload->clone());
      } else if (fetchState_) {
        logger_->logFetchObjectParsed(
            currentStreamId_, currentObj_, payload->clone());
      }
    }

    bool finStream = false;
    // Handle subscription/fetch consumers
    auto res = invokeCallbackNoGroup(
        &SubgroupConsumer::objectPayload,
        &FetchConsumer::objectPayload,
        std::move(payload),
        finStream);
    if (!res) {
      error_ = std::move(res.error());
    } else {
      XCHECK_EQ(objectComplete, res.value() == ObjectPublishStatus::DONE);
    }
  }

  void onObjectStatus(
      uint64_t group,
      uint64_t subgroup,
      uint64_t objectID,
      Priority pri,
      ObjectStatus status,
      Extensions extensions) override {
    if (isCancelled()) {
      return;
    }
    folly::Expected<folly::Unit, MoQPublishError> res{folly::unit};
    // Handle subscription/fetch consumers
    switch (status) {
      case ObjectStatus::NORMAL:
        break;
      case ObjectStatus::OBJECT_NOT_EXIST:
        res = invokeCallback(
            &SubgroupConsumer::objectNotExists,
            &FetchConsumer::objectNotExists,
            group,
            subgroup,
            objectID,
            std::move(extensions),
            false);
        break;
      case ObjectStatus::GROUP_NOT_EXIST:
        // groupNotExists is on the TrackConsumer not SubgroupConsumer
        if (fetchState_) {
          res = fetchState_->getFetchCallback()->groupNotExists(
              group, subgroup, std::move(extensions), false);
        } else {
          res = subscribeState_->getSubscribeCallback()->groupNotExists(
              group, subgroup, pri, std::move(extensions));
          endOfSubgroup();
        }
        break;
      case ObjectStatus::END_OF_GROUP:
        // FetchConsumer::endOfGroup has an optional param
        if (fetchState_) {
          res = fetchState_->getFetchCallback()->endOfGroup(
              group,
              subgroup,
              objectID,
              std::move(extensions),
              /*finFetch=*/false);
        } else {
          res = subgroupCallback_->endOfGroup(objectID, std::move(extensions));
          endOfSubgroup();
        }
        break;
      case ObjectStatus::END_OF_TRACK:
        res = invokeCallback(
            &SubgroupConsumer::endOfTrackAndGroup,
            &FetchConsumer::endOfTrackAndGroup,
            group,
            subgroup,
            objectID,
            std::move(extensions));
        endOfSubgroup();
        break;
    }
    if (!res) {
      error_ = std::move(res.error());
    }
  }

  void onEndOfStream() override {
    if (!isCancelled()) {
      endOfSubgroup(/*deliverCallback=*/true);
    }
  }

  void onConnectionError(ErrorCode error) override {
    XLOG(ERR) << "Parse error=" << folly::to_underlying(error);
    session_->close(error);
  }

  // Called by read loop on read error (eg: RESET_STREAM)
  bool reset(ResetStreamErrorCode error) {
    if (!subscribeState_ && !fetchState_) {
      return false;
    }
    if (!isCancelled()) {
      // ignoring error from reset?
      invokeCallbackNoGroup(
          &SubgroupConsumer::reset, &FetchConsumer::reset, error);
    }
    endOfSubgroup();
    return true;
  }

  folly::Optional<MoQPublishError> error() const {
    return error_;
  }

  void setLogger(std::shared_ptr<MLogger> logger) {
    logger_ = logger;
  }

 private:
  bool isCancelled() const {
    if (fetchState_) {
      return !fetchState_->getFetchCallback();
    } else if (subscribeState_) {
      return !subgroupCallback_ || !subscribeState_->getSubscribeCallback();
    }
    return true;
  }

  void endOfSubgroup(bool deliverCallback = false) {
    if (subgroupCallback_) {
      CHECK(session_) << "session_ is NULL in ObjectStreamCallback";
      // We only want to call this for SUBSCRIBEs and not FETCHes, which is
      // why we condition on subgroupCallback_
      session_->onSubscriptionStreamClosedByPeer();
    }
    if (deliverCallback && !isCancelled()) {
      if (fetchState_) {
        fetchState_->getFetchCallback()->endOfFetch();
      } else {
        subgroupCallback_->endOfSubgroup();
      }
    }
    if (fetchState_) {
      fetchState_->resetFetchCallback(session_);
    } else {
      subgroupCallback_.reset();
    }
  }
  std::shared_ptr<MLogger> logger_ = nullptr;
  std::shared_ptr<MoQSession> session_;
  folly::CancellationToken& token_;
  std::shared_ptr<MoQSession::SubscribeTrackReceiveState> subscribeState_;
  std::shared_ptr<SubgroupConsumer> subgroupCallback_;
  std::shared_ptr<MoQSession::FetchTrackReceiveState> fetchState_;
  folly::Optional<MoQPublishError> error_;
  uint64_t currentStreamId_{0};
  TrackAlias trackAlias_{0};
  ObjectHeader currentObj_;
};

} // namespace detail

folly::coro::Task<folly::Expected<bool, MoQPublishError>>
MoQSession::headerParsed(
    MoQObjectStreamCodec& codec,
    detail::ObjectStreamCallback& callback,
    proxygen::WebTransport::StreamData& streamData) {
  auto parseResult = codec.parseSubgroupTypeAndAlias(
      std::move(streamData.data), streamData.fin);
  if (parseResult.hasError()) {
    if (parseResult.error() == ErrorCode::PARSE_UNDERFLOW) {
      XLOG(DBG4) << "Underflow on uni stream header";
      co_return false;
    } else {
      MoQPublishError err(
          MoQPublishError::CANCELLED, "Error parsing subgroup type and alias");
      co_return folly::makeUnexpected(err);
    }
  }
  auto trackAlias = parseResult.value();
  if (!trackAlias) {
    // it's fetch
    co_return true;
  }
  auto state = getSubscribeTrackReceiveState(*trackAlias);
  if (!state) {
    XLOG(DBG4) << "No receive state for alias=" << *trackAlias << " waiting";
    TimedBaton baton;
    auto res =
        bufferedSubgroups_.emplace(*trackAlias, std::list<TimedBaton*>());
    res.first->second.push_back(&baton);
    constexpr std::chrono::milliseconds kUnknownAliasTimeout(5000);
    auto waitRes = co_await co_awaitTry(baton.wait(kUnknownAliasTimeout));
    if (waitRes.hasException()) {
      // TODO: Remove the baton we created from bufferedSubgroups_. The removal
      // would be a lot easier once we've implemented a TimedBaton/TimedBarrier
      // that allows multiple waiters on the same object.
      MoQPublishError err(
          MoQPublishError::CANCELLED, "Timed out waiting for unknown alias");
      co_return folly::makeUnexpected(err);
    } else {
      state = getSubscribeTrackReceiveState(*trackAlias);
      if (!state->getSubscribeCallback()) {
        co_return folly::makeUnexpected(MoQPublishError(
            MoQPublishError::CANCELLED, "Canceled or unusbscribed"));
      }
      XCHECK(state);
    }
  }
  callback.setSubscribeTrackReceiveState(std::move(state));
  co_return true;
}

folly::coro::Task<void> MoQSession::unidirectionalReadLoop(
    std::shared_ptr<MoQSession> session,
    proxygen::WebTransport::StreamReadHandle* readHandle) {
  auto id = readHandle->getID();
  XLOG(DBG1) << __func__ << " id=" << id << " sess=" << this;
  auto g = folly::makeGuard([func = __func__, this, id] {
    XLOG(DBG1) << "exit " << func << " id=" << id << " sess=" << this;
  });
  co_await folly::coro::co_safe_point;
  auto token = co_await folly::coro::co_current_cancellation_token;

  if (!negotiatedVersion_.hasValue()) {
    auto versionBaton = std::make_shared<moxygen::TimedBaton>();
    subgroupsWaitingForVersion_.push_back(versionBaton);
    constexpr std::chrono::milliseconds kVersionTimeout(5000);
    auto waitRes = co_await co_awaitTry(versionBaton->wait(kVersionTimeout));
    if (waitRes.hasException()) {
      readHandle->stopSending(0);
      co_return;
    }
  }

  MoQObjectStreamCodec codec(nullptr);
  codec.initializeVersion(*negotiatedVersion_);
  detail::ObjectStreamCallback dcb(session, /*by ref*/ token);
  if (logger_) {
    dcb.setLogger(logger_);
  }
  dcb.setCurrentStreamId(readHandle->getID());
  codec.setCallback(&dcb);
  codec.setStreamId(id);

  bool fin = false;
  bool headerParsed = false;
  while (!fin && !token.isCancellationRequested()) {
    auto streamData = co_await co_awaitTry(co_withCancellation(
        token,
        folly::coro::toTaskInterruptOnCancel(
            readHandle->readStreamData().via(exec_.get()))));
    if (streamData.hasException()) {
      XLOG(ERR) << folly::exceptionStr(streamData.exception()) << " id=" << id
                << " sess=" << this;
      ResetStreamErrorCode errorCode{ResetStreamErrorCode::INTERNAL_ERROR};
      auto wtEx =
          streamData.tryGetExceptionObject<proxygen::WebTransport::Exception>();
      if (wtEx) {
        errorCode = ResetStreamErrorCode(wtEx->error);
      } else {
        XLOG(ERR) << folly::exceptionStr(streamData.exception());
      }
      if (!dcb.reset(errorCode)) {
        XLOG(ERR) << __func__ << " terminating for unknown "
                  << "stream id=" << id << " sess=" << this;
      }
      break;
    }
    if (streamData->data || streamData->fin) {
      fin = streamData->fin;
      folly::Optional<MoQPublishError> err;
      if (!headerParsed) {
        auto res = co_await this->headerParsed(codec, dcb, streamData.value());
        if (res.hasError()) {
          err = res.error();
        } else {
          headerParsed = *res;
          if (!headerParsed) {
            continue;
          }
        }
      }
      try {
        if (!err) {
          codec.onIngress(std::move(streamData->data), streamData->fin);
          err = dcb.error();
        }
      } catch (const std::exception& ex) {
        err = MoQPublishError(
            MoQPublishError::CANCELLED, folly::exceptionStr(ex).toStdString());
      }
      XLOG_IF(DBG3, fin) << "End of stream id=" << id << " sess=" << this;
      if (err) {
        XLOG(ERR) << "Error parsing/consuming stream, " << err->describe()
                  << " id=" << id << " sess=" << this;
        if (!fin) {
          readHandle->stopSending(/*error=*/0);
          break;
        }
      }
    } // else empty read
  }
}

void MoQSession::onSubscribe(SubscribeRequest subscribeRequest) {
  XLOG(DBG1) << __func__ << " ftn=" << subscribeRequest.fullTrackName
             << " sess=" << this;
  const auto requestID = subscribeRequest.requestID;
  if (closeSessionIfRequestIDInvalid(requestID, false, true)) {
    return;
  }
  if (logger_) {
    logger_->logSubscribe(
        subscribeRequest,
        MOQTByteStringType::STRING_VALUE,
        ControlMessageType::PARSED);
  }

  // TODO: The publisher should maintain some state like
  //   Subscribe ID -> Track Name, Locations [currently held in
  //   MoQForwarder] Track Alias -> Track Name
  // If ths session holds this state, it can check for duplicate
  // subscriptions
  auto it = pubTracks_.find(subscribeRequest.requestID);
  if (it != pubTracks_.end()) {
    XLOG(ERR) << "Duplicate subscribe ID=" << subscribeRequest.requestID
              << " sess=" << this;
    // TODO: message error?
    subscribeError(
        {subscribeRequest.requestID,
         SubscribeErrorCode::INTERNAL_ERROR,
         "dup sub ID"});
    return;
  }
  // TODO: Check for duplicate alias
  bool forward = subscribeRequest.forward;
  auto trackPublisher = std::make_shared<TrackPublisherImpl>(
      this,
      subscribeRequest.fullTrackName,
      subscribeRequest.requestID,
      subscribeRequest.trackAlias,
      subscribeRequest.priority,
      subscribeRequest.groupOrder,
      *negotiatedVersion_,
      moqSettings_.bufferingThresholds.perSubscription,
      forward);

  if (logger_) {
    trackPublisher->setLogger(logger_);
  }

  pubTracks_.emplace(requestID, trackPublisher);
  // TODO: there should be a timeout for the application to call
  // subscribeOK/Error
  co_withExecutor(
      exec_.get(),
      handleSubscribe(std::move(subscribeRequest), std::move(trackPublisher)))
      .start();
}

folly::coro::Task<void> MoQSession::handleSubscribe(
    SubscribeRequest sub,
    std::shared_ptr<TrackPublisherImpl> trackPublisher) {
  folly::RequestContextScopeGuard guard;
  setRequestSession();
  auto requestID = sub.requestID;
  auto subscribeResult = co_await co_awaitTry(co_withCancellation(
      cancellationSource_.getToken(),
      publishHandler_->subscribe(
          std::move(sub),
          std::static_pointer_cast<TrackConsumer>(trackPublisher))));
  if (subscribeResult.hasException()) {
    XLOG(ERR) << "Exception in Publisher callback ex="
              << subscribeResult.exception().what().toStdString();
    subscribeError(
        {requestID,
         SubscribeErrorCode::INTERNAL_ERROR,
         subscribeResult.exception().what().toStdString()});
    co_return;
  }
  if (subscribeResult->hasError()) {
    XLOG(DBG1) << "Application subscribe error err="
               << subscribeResult->error().reasonPhrase;
    auto subErr = std::move(subscribeResult->error());
    subErr.requestID = requestID; // In case app got it wrong
    subscribeError(subErr);
  } else {
    auto subHandle = std::move(subscribeResult->value());
    auto subOk = subHandle->subscribeOk();
    subOk.requestID = requestID;
    // TODO: verify TrackAlias is unique
    subscribeOk(subOk);

    trackPublisher->setSubscriptionHandle(std::move(subHandle));
  }
}

void MoQSession::onSubscribeUpdate(SubscribeUpdate subscribeUpdate) {
  XLOG(DBG1) << __func__ << " id=" << subscribeUpdate.requestID
             << " sess=" << this;
  MOQ_PUBLISHER_STATS(publisherStatsCallback_, onSubscribeUpdate);
  auto subscriptionRequestID = subscribeUpdate.requestID;

  if (getDraftMajorVersion(*getNegotiatedVersion()) >= 14) {
    subscriptionRequestID = subscribeUpdate.subscriptionRequestID;

    // RequestID meaning has changed, check validity
    if (closeSessionIfRequestIDInvalid(
            subscribeUpdate.requestID, false, true)) {
      return;
    }
  }

  if (!publishHandler_) {
    XLOG(DBG1) << __func__ << "No publisher callback set";
    return;
  }

  if (logger_) {
    logger_->logSubscribeUpdate(subscribeUpdate, ControlMessageType::PARSED);
  }

  if (closeSessionIfRequestIDInvalid(
          subscriptionRequestID, false, false, false)) {
    return;
  }
  auto it = pubTracks_.find(subscriptionRequestID);
  if (it == pubTracks_.end()) {
    XLOG(ERR) << "No matching subscribe ID=" << subscriptionRequestID
              << " sess=" << this;
    return;
  }

  it->second->setSubPriority(subscribeUpdate.priority);
  // TODO: update priority of tracks in flight
  auto pubTrackIt = pubTracks_.find(subscriptionRequestID);
  if (pubTrackIt == pubTracks_.end()) {
    XLOG(ERR) << "SubscribeUpdate track not found id=" << subscriptionRequestID
              << " sess=" << this;
    return;
  }
  auto trackPublisher =
      dynamic_cast<TrackPublisherImpl*>(pubTrackIt->second.get());
  if (!trackPublisher) {
    XLOG(ERR) << "SubscriptionRequestID in SubscribeUpdate is for a FETCH, id="
              << subscriptionRequestID << " sess=" << this;
  } else {
    trackPublisher->subscribeUpdate(std::move(subscribeUpdate));
  }
}

void MoQSession::onUnsubscribe(Unsubscribe unsubscribe) {
  XLOG(DBG1) << __func__ << " id=" << unsubscribe.requestID << " sess=" << this;

  if (logger_) {
    logger_->logUnsubscribe(unsubscribe, ControlMessageType::PARSED);
  }

  MOQ_PUBLISHER_STATS(publisherStatsCallback_, onUnsubscribe);
  if (closeSessionIfRequestIDInvalid(
          unsubscribe.requestID, false, false, false)) {
    return;
  }
  if (!publishHandler_) {
    XLOG(DBG1) << __func__ << "No publisher callback set";
    return;
  }
  // How does this impact pending subscribes?
  // and open TrackReceiveStates
  auto pubTrackIt = pubTracks_.find(unsubscribe.requestID);
  if (pubTrackIt == pubTracks_.end()) {
    XLOG(ERR) << "Unsubscribe track not found id=" << unsubscribe.requestID
              << " sess=" << this;
    return;
  }
  auto trackPublisher =
      dynamic_cast<TrackPublisherImpl*>(pubTrackIt->second.get());
  if (!trackPublisher) {
    XLOG(ERR) << "RequestID in Unsubscribe is for a FETCH, id="
              << unsubscribe.requestID << " sess=" << this;
  } else {
    trackPublisher->unsubscribe();
    if (pubTracks_.erase(unsubscribe.requestID)) {
      retireRequestID(/*signalWriteLoop=*/true);
    } // else, the caller invoked subscribeDone, which isn't needed but fine
  }
}

void MoQSession::onPublishOk(PublishOk publishOk) {
  XLOG(DBG1) << __func__ << " reqID=" << publishOk.requestID
             << " sess=" << this;
  auto pubIt = pendingRequests_.find(publishOk.requestID);
  if (pubIt == pendingRequests_.end()) {
    XLOG(ERR) << "No matching publish reqID=" << publishOk.requestID
              << " sess=" << this;
    close(SessionCloseErrorCode::PROTOCOL_VIOLATION);
    return;
  }

  auto* publishPtr = pubIt->second.tryGetPublish();
  if (!publishPtr) {
    XLOG(ERR) << "Request ID " << publishOk.requestID
              << " is not a publish request, sess=" << this;
    close(SessionCloseErrorCode::PROTOCOL_VIOLATION);
    return;
  }

  publishPtr->setValue(std::move(publishOk));
  pendingRequests_.erase(pubIt);
}

void MoQSession::onPublishError(PublishError publishError) {
  XLOG(DBG1) << __func__ << " reqID=" << publishError.requestID
             << " sess=" << this;

  auto pendIt = pendingRequests_.find(publishError.requestID);
  if (pendIt == pendingRequests_.end()) {
    XLOG(ERR) << "No matching publish reqID=" << publishError.requestID
              << " sess=" << this;
    close(SessionCloseErrorCode::PROTOCOL_VIOLATION);
    return;
  }

  auto* publishPtr = pendIt->second.tryGetPublish();
  if (!publishPtr) {
    XLOG(ERR) << "Request ID " << publishError.requestID
              << " is not a publish request, sess=" << this;
    close(SessionCloseErrorCode::PROTOCOL_VIOLATION);
    return;
  }

  auto requestId = publishError.requestID;
  publishPtr->setValue(folly::makeUnexpected(std::move(publishError)));
  pendingRequests_.erase(pendIt);

  auto pubIt = pubTracks_.find(requestId);
  if (pubIt == pubTracks_.end()) {
    XLOG(ERR) << "Invalid Publish OK, id=" << requestId;
    return;
  }
  pubIt->second->setSession(nullptr);
  pubTracks_.erase(pubIt);
}

void MoQSession::onSubscribeOk(SubscribeOk subOk) {
  XLOG(DBG1) << __func__ << " id=" << subOk.requestID << " sess=" << this;

  if (logger_) {
    logger_->logSubscribeOk(subOk, ControlMessageType::PARSED);
  }

  auto it = pendingRequests_.find(subOk.requestID);
  if (it == pendingRequests_.end()) {
    XLOG(ERR) << "No matching subscribe ID=" << subOk.requestID
              << " sess=" << this;
    close(SessionCloseErrorCode::PROTOCOL_VIOLATION);
    return;
  }
  auto* trackPtr = it->second.tryGetSubscribeTrack();
  if (!trackPtr) {
    XLOG(ERR) << "Request ID " << subOk.requestID
              << " is not a subscribe track request, sess=" << this;
    close(SessionCloseErrorCode::PROTOCOL_VIOLATION);
    return;
  }
  auto trackReceiveState = std::move(*trackPtr);
  pendingRequests_.erase(it);
  if (getDraftMajorVersion(*getNegotiatedVersion()) < 12) {
    subOk.trackAlias = trackReceiveState->getTrackAlias();
  }

  auto res = reqIdToTrackAlias_.try_emplace(subOk.requestID, subOk.trackAlias);
  if (!res.second) {
    XLOG(ERR) << "Request ID already mapped to alias reqID=" << subOk.requestID
              << " sess=" << this;
    close(SessionCloseErrorCode::PROTOCOL_VIOLATION);
    return;
  }

  auto emplaceRes = subTracks_.try_emplace(subOk.trackAlias, trackReceiveState);
  if (!emplaceRes.second) {
    XLOG(ERR) << "TrackAlias already in use" << subOk.trackAlias
              << " sess=" << this;
    close(SessionCloseErrorCode::DUPLICATE_TRACK_ALIAS);
  }
  auto trackAlias = subOk.trackAlias;
  trackReceiveState->subscribeOK(std::move(subOk));
  if (trackReceiveState->getSubscribeCallback()) {
    trackReceiveState->getSubscribeCallback()->setTrackAlias(trackAlias);
  }
  deliverBufferedData(trackAlias);
}

void MoQSession::deliverBufferedData(TrackAlias trackAlias) {
  auto datagramsIt = bufferedDatagrams_.find(trackAlias);
  if (datagramsIt != bufferedDatagrams_.end()) {
    auto datagrams = std::move(datagramsIt->second);
    bufferedDatagrams_.erase(datagramsIt);
    for (auto& datagram : datagrams) {
      onDatagram(std::move(datagram));
    }
  }

  auto subgroupsIt = bufferedSubgroups_.find(trackAlias);
  if (subgroupsIt != bufferedSubgroups_.end()) {
    auto subgroups = std::move(subgroupsIt->second);
    bufferedSubgroups_.erase(subgroupsIt);
    for (auto* baton : subgroups) {
      baton->signal();
    }
  }
}

void MoQSession::onSubscribeError(SubscribeError subErr) {
  XLOG(DBG1) << __func__ << " id=" << subErr.requestID << " sess=" << this;

  if (logger_) {
    logger_->logSubscribeError(subErr, ControlMessageType::PARSED);
  }

  auto it = pendingRequests_.find(subErr.requestID);
  if (it == pendingRequests_.end()) {
    XLOG(ERR) << "No matching request ID=" << subErr.requestID
              << " sess=" << this;
    close(SessionCloseErrorCode::PROTOCOL_VIOLATION);
    return;
  }
  auto* trackPtr = it->second.tryGetSubscribeTrack();
  if (!trackPtr) {
    XLOG(ERR) << "Request ID " << subErr.requestID
              << " is not a subscribe track request, sess=" << this;
    close(SessionCloseErrorCode::PROTOCOL_VIOLATION);
    return;
  }
  auto trackReceiveState = std::move(*trackPtr);
  pendingRequests_.erase(it);
  trackReceiveState->subscribeError(std::move(subErr));
  checkForCloseOnDrain();
}

class MoQSession::ReceiverSubscriptionHandle
    : public Publisher::SubscriptionHandle {
 public:
  ReceiverSubscriptionHandle(
      SubscribeOk ok,
      TrackAlias alias,
      std::shared_ptr<MoQSession> session)
      : Publisher::SubscriptionHandle(std::move(ok)),
        trackAlias_(alias),
        session_(std::move(session)) {}

  void subscribeUpdate(SubscribeUpdate subscribeUpdate) override {
    if (session_) {
      subscribeUpdate.subscriptionRequestID = subscribeOk_->requestID;
      if (getDraftMajorVersion(*(session_->getNegotiatedVersion())) >= 14) {
        subscribeUpdate.requestID = session_->getNextRequestID();
      } else {
        subscribeUpdate.requestID = subscribeOk_->requestID;
      }
      session_->subscribeUpdate(subscribeUpdate);
    }
  }

  void unsubscribe() override {
    if (session_) {
      session_->unsubscribe({subscribeOk_->requestID});
      session_.reset();
    }
  }

 private:
  TrackAlias trackAlias_;
  std::shared_ptr<MoQSession> session_;
};

void MoQSession::onPublish(PublishRequest publish) {
  XLOG(DBG1) << __func__ << " reqID=" << publish.requestID << " sess=" << this;
  MOQ_SUBSCRIBER_STATS(subscriberStatsCallback_, onPublish);
  if (closeSessionIfRequestIDInvalid(publish.requestID, false, true)) {
    return;
  }

  if (!subscribeHandler_) {
    XLOG(DBG1) << __func__ << " No subscriber callback set";
    publishError(PublishError{
        publish.requestID,
        PublishErrorCode::NOT_SUPPORTED,
        "Not a subscriber"});
    return;
  }

  auto publishHandle = std::make_shared<ReceiverSubscriptionHandle>(
      SubscribeOk{publish.requestID}, publish.trackAlias, shared_from_this());

  // Use single coroutine pattern like working onAnnounce
  co_withExecutor(
      exec_.get(), handlePublish(std::move(publish), std::move(publishHandle)))
      .start();
}

folly::coro::Task<void> MoQSession::handlePublish(
    PublishRequest publish,
    std::shared_ptr<Publisher::SubscriptionHandle> publishHandle) {
  folly::RequestContextScopeGuard guard;
  setRequestSession();
  // Capture params before moving publish
  auto requestID = publish.requestID;
  auto alias = publish.trackAlias;
  auto ftn = publish.fullTrackName;

  // PubError if error occurs
  auto publishErr =
      PublishError{requestID, PublishErrorCode::INTERNAL_ERROR, ""};

  try {
    // Call publish handler synchronously (now returns PublishResult)
    auto publishResult =
        subscribeHandler_->publish(std::move(publish), publishHandle);

    if (publishResult.hasError()) {
      XLOG(DBG1) << "Application publish error err="
                 << publishResult.error().reasonPhrase;
      publishErr.errorCode = publishResult.error().errorCode;
      publishErr.reasonPhrase = publishResult.error().reasonPhrase;
    } else {
      // Extract the initiator and process reply with co_await
      auto& initiator = publishResult.value();
      // Process the async reply - this is the only async part
      auto replyResult =
          co_await folly::coro::co_awaitTry(std::move(initiator.reply));
      if (replyResult.hasException()) {
        XLOG(ERR) << "Exception in publish reply ex="
                  << replyResult.exception().what().toStdString();
        publishErr.reasonPhrase = replyResult.exception().what().toStdString();
      } else if (replyResult->hasError()) {
        publishErr.reasonPhrase = replyResult->error().reasonPhrase;
      } else {
        // Create SubscribeTrackReceiveState
        // Need in order to obtain Alias Later on
        reqIdToTrackAlias_.emplace(requestID, alias);

        // Add ReceiveState to subTracks_
        auto trackReceiveState = std::make_shared<SubscribeTrackReceiveState>(
            ftn, requestID, initiator.consumer, logger_, true);
        initiator.consumer->setTrackAlias(alias);
        subTracks_.emplace(alias, trackReceiveState);
        // Ensure the PublishOk we send back corresponds to the inbound
        // publish request (requestID), not the republish's requestID.
        auto pubOk = replyResult->value();
        pubOk.requestID = requestID;
        publishOk(pubOk);
        deliverBufferedData(alias);
        co_return;
      }
    }
  } catch (const std::exception& ex) {
    XLOG(ERR) << "Exception in Subscriber publish callback ex=" << ex.what();
    publishErr.reasonPhrase = ex.what();
  }
  publishError(publishErr);
}

void MoQSession::onSubscribeDone(SubscribeDone subscribeDone) {
  XLOG(DBG1) << "SubscribeDone id=" << subscribeDone.requestID
             << " code=" << folly::to_underlying(subscribeDone.statusCode)
             << " reason=" << subscribeDone.reasonPhrase;

  if (logger_) {
    logger_->logSubscribeDone(subscribeDone, ControlMessageType::PARSED);
  }
  MOQ_SUBSCRIBER_STATS(
      subscriberStatsCallback_, onSubscribeDone, subscribeDone.statusCode);

  // Handle regular subscription SUBSCRIBE_DONE
  auto trackAliasIt = reqIdToTrackAlias_.find(subscribeDone.requestID);
  if (trackAliasIt == reqIdToTrackAlias_.end()) {
    // unknown
    XLOG(ERR) << "No matching subscribe ID=" << subscribeDone.requestID
              << " sess=" << this;
    return;
  }

  // TODO: handle final object and status code
  // TODO: there could still be objects in flight.  Removing from maps now
  // will prevent their delivery.  I think the only way to handle this is with
  // timeouts.
  auto alias = trackAliasIt->second;
  auto reqId = subscribeDone.requestID;
  auto trackReceiveStateIt = subTracks_.find(alias);
  if (trackReceiveStateIt != subTracks_.end()) {
    auto state = trackReceiveStateIt->second;
    if (state->subscribeDone(std::move(subscribeDone))) {
      removeSubscriptionState(alias, reqId);
    }
  } else {
    XLOG(DFATAL) << "trackAliasIt but no trackReceiveStateIt for id="
                 << subscribeDone.requestID << " sess=" << this;
  }
}

void MoQSession::removeSubscriptionState(TrackAlias alias, RequestID id) {
  subTracks_.erase(alias);
  reqIdToTrackAlias_.erase(id);
  checkForCloseOnDrain();
}

void MoQSession::onMaxRequestID(MaxRequestID maxRequestID) {
  XLOG(DBG1) << __func__ << " sess=" << this;

  if (logger_) {
    logger_->logMaxSubscribeId(
        maxRequestID.requestID.value, ControlMessageType::PARSED);
  }

  if (maxRequestID.requestID.value > peerMaxRequestID_) {
    XLOG(DBG1) << fmt::format(
        "Bumping the maxRequestID to: {} from: {}",
        maxRequestID.requestID.value,
        peerMaxRequestID_);
    peerMaxRequestID_ = maxRequestID.requestID.value;
    return;
  }

  XLOG(ERR) << fmt::format(
      "Invalid MaxRequestID: {}. Current maxRequestID:{}",
      maxRequestID.requestID.value,
      maxRequestID_);
  close(SessionCloseErrorCode::PROTOCOL_VIOLATION);
}

void MoQSession::onRequestsBlocked(RequestsBlocked requestsBlocked) {
  XLOG(DBG1) << __func__ << " sess=" << this;
  // Increment the maxRequestID_ by the number of pending closed subscribes
  // and send a new MaxRequestID.

  if (logger_) {
    logger_->logSubscribesBlocked(
        requestsBlocked.maxRequestID.value, ControlMessageType::PARSED);
  }

  if (requestsBlocked.maxRequestID >= maxRequestID_ && closedRequests_ > 0) {
    maxRequestID_ += (closedRequests_ * getRequestIDMultiplier());
    closedRequests_ = 0;
    sendMaxRequestID(true);
  }
}

void MoQSession::onFetch(Fetch fetch) {
  auto [standalone, joining] = fetchType(fetch);
  auto logStr = (standalone)
      ? fetch.fullTrackName.describe()
      : folly::to<std::string>("joining=", joining->joiningRequestID.value);
  XLOG(DBG1) << __func__ << " (" << logStr << ") sess=" << this;
  const auto requestID = fetch.requestID;

  if (logger_) {
    logger_->logFetch(
        fetch, MOQTByteStringType::STRING_VALUE, ControlMessageType::PARSED);
  }

  if (closeSessionIfRequestIDInvalid(requestID, false, true)) {
    return;
  }
  if (standalone) {
    if (standalone->end <= standalone->start) {
      // If the end object is zero this indicates a fetch for the entire group,
      // which is valid as long as the start and end group are the same.
      if (!(standalone->end.group == standalone->start.group &&
            standalone->end.object == 0)) {
        fetchError(
            {fetch.requestID,
             FetchErrorCode::INVALID_RANGE,
             "End must be after start"});
        return;
      }
    }
  } else {
    auto joinIt = pubTracks_.find(joining->joiningRequestID);
    if (joinIt == pubTracks_.end()) {
      XLOG(ERR) << "Unknown joining subscribe ID=" << joining->joiningRequestID
                << " sess=" << this;
      // message error
      fetchError(
          {fetch.requestID,
           FetchErrorCode::INTERNAL_ERROR,
           "Unknown joining requestID"});
      return;
    }
    fetch.fullTrackName = joinIt->second->fullTrackName();
  }
  auto it = pubTracks_.find(fetch.requestID);
  if (it != pubTracks_.end()) {
    XLOG(ERR) << "Duplicate subscribe ID=" << fetch.requestID
              << " sess=" << this;
    // message error
    fetchError({fetch.requestID, FetchErrorCode::INTERNAL_ERROR, "dup sub ID"});
    return;
  }
  auto fetchPublisher = std::make_shared<FetchPublisherImpl>(
      this,
      fetch.fullTrackName,
      fetch.requestID,
      fetch.priority,
      fetch.groupOrder,
      *negotiatedVersion_,
      moqSettings_.bufferingThresholds.perSubscription);
  fetchPublisher->setLogger(logger_);
  fetchPublisher->initialize();
  pubTracks_.emplace(fetch.requestID, fetchPublisher);
  co_withExecutor(
      exec_.get(), handleFetch(std::move(fetch), std::move(fetchPublisher)))
      .start();
}

folly::coro::Task<void> MoQSession::handleFetch(
    Fetch fetch,
    std::shared_ptr<FetchPublisherImpl> fetchPublisher) {
  folly::RequestContextScopeGuard guard;
  setRequestSession();
  auto requestID = fetch.requestID;
  if (!fetchPublisher->getStreamPublisher()) {
    XLOG(ERR) << "Fetch Publisher killed sess=" << this;
    fetchError({requestID, FetchErrorCode::INTERNAL_ERROR, "Fetch Failed"});
    co_return;
  }
  auto fetchResult = co_await co_awaitTry(co_withCancellation(
      cancellationSource_.getToken(),
      publishHandler_->fetch(
          std::move(fetch), fetchPublisher->getStreamPublisher())));
  if (fetchResult.hasException() || fetchResult->hasError()) {
    // We need to call reset() in order to ensure that the StreamPublisherImpl
    // is destructed, otherwise there could be memory leaks, since the
    // StreamPublisherImpl and the PublisherImpl hold references to each other.
    // This doesn't actually reset the stream unless the user wrote something to
    // it within the fetch handler, because the stream creation is deferred
    // until the user actually writes something to a stream.
    fetchPublisher->reset(ResetStreamErrorCode::INTERNAL_ERROR);
  }

  if (fetchResult.hasException()) {
    XLOG(ERR) << "Exception in Publisher callback ex="
              << fetchResult.exception().what();
    fetchError(
        {requestID,
         FetchErrorCode::INTERNAL_ERROR,
         fetchResult.exception().what().toStdString()});
    co_return;
  }
  if (fetchResult->hasError()) {
    XLOG(DBG1) << "Application fetch error err="
               << fetchResult->error().reasonPhrase;
    auto fetchErr = std::move(fetchResult->error());
    fetchErr.requestID = requestID; // In case app got it wrong
    fetchError(fetchErr);
  } else if (!fetchPublisher->isCancelled()) {
    auto fetchHandle = std::move(fetchResult->value());
    auto fetchOkMsg = fetchHandle->fetchOk();
    fetchOkMsg.requestID = requestID;
    fetchOk(fetchOkMsg);
    fetchPublisher->setFetchHandle(std::move(fetchHandle));
  } // else, no need to fetchError, state has been removed on both sides already
}

void MoQSession::onFetchCancel(FetchCancel fetchCancel) {
  XLOG(DBG1) << __func__ << " id=" << fetchCancel.requestID << " sess=" << this;
  if (closeSessionIfRequestIDInvalid(fetchCancel.requestID, false, false)) {
    return;
  }

  if (logger_) {
    logger_->logFetchCancel(fetchCancel, ControlMessageType::PARSED);
  }

  auto pubTrackIt = pubTracks_.find(fetchCancel.requestID);
  if (pubTrackIt == pubTracks_.end()) {
    XLOG(DBG4) << "No publish key for fetch id=" << fetchCancel.requestID
               << " sess=" << this;
    // The Fetch stream has already closed, or never existed
    // If it's already closed, a no-op is fine.
    // See: https://github.com/moq-wg/moq-transport/issues/630
  } else {
    // It's possible the fetch stream hasn't opened yet if the application
    // hasn't made it to fetchOK.
    auto fetchPublisher =
        std::dynamic_pointer_cast<FetchPublisherImpl>(pubTrackIt->second);
    if (!fetchPublisher) {
      XLOG(ERR) << "FETCH_CANCEL on SUBSCRIBE id=" << fetchCancel.requestID;
      return;
    }
    fetchPublisher->cancel();
  }
}

void MoQSession::onFetchOk(FetchOk fetchOk) {
  XLOG(DBG1) << __func__ << " id=" << fetchOk.requestID << " sess=" << this;
  auto fetchIt = fetches_.find(fetchOk.requestID);

  if (logger_) {
    logger_->logFetchOk(fetchOk, ControlMessageType::PARSED);
  }

  if (fetchIt == fetches_.end()) {
    XLOG(ERR) << "No matching subscribe ID=" << fetchOk.requestID
              << " sess=" << this;
    return;
  }
  const auto& trackReceiveState = fetchIt->second;
  trackReceiveState->fetchOK(std::move(fetchOk));
  if (trackReceiveState->fetchOkAndAllDataReceived()) {
    fetches_.erase(fetchIt);
    checkForCloseOnDrain();
  }
}

void MoQSession::onFetchError(FetchError fetchError) {
  XLOG(DBG1) << __func__ << " id=" << fetchError.requestID << " sess=" << this;
  auto fetchIt = fetches_.find(fetchError.requestID);

  if (logger_) {
    logger_->logFetchError(fetchError, ControlMessageType::PARSED);
  }

  if (fetchIt == fetches_.end()) {
    XLOG(ERR) << "No matching subscribe ID=" << fetchError.requestID
              << " sess=" << this;
    return;
  }
  fetchIt->second->fetchError(fetchError);
  fetches_.erase(fetchIt);
  checkForCloseOnDrain();
}

void MoQSession::onAnnounce(Announce ann) {
  XLOG(DBG1) << __func__ << " ns=" << ann.trackNamespace << " sess=" << this;

  if (logger_) {
    logger_->logAnnounce(
        ann, MOQTByteStringType::STRING_VALUE, ControlMessageType::PARSED);
  }

  if (closeSessionIfRequestIDInvalid(ann.requestID, false, true)) {
    return;
  }

  if (!subscribeHandler_) {
    XLOG(DBG1) << __func__ << " No subscriber callback set";
    announceError(
        {ann.requestID, AnnounceErrorCode::NOT_SUPPORTED, "Not a subscriber"});
    return;
  }
  co_withExecutor(exec_.get(), handleAnnounce(std::move(ann))).start();
}

folly::coro::Task<void> MoQSession::handleAnnounce(Announce announce) {
  folly::RequestContextScopeGuard guard;
  setRequestSession();
  auto annCb = std::make_shared<SubscriberAnnounceCallback>(
      *this, announce.trackNamespace);
  auto announceResult = co_await co_awaitTry(co_withCancellation(
      cancellationSource_.getToken(),
      subscribeHandler_->announce(announce, std::move(annCb))));
  if (announceResult.hasException()) {
    XLOG(ERR) << "Exception in Subscriber callback ex="
              << announceResult.exception().what().toStdString();
    announceError(
        {announce.requestID,
         AnnounceErrorCode::INTERNAL_ERROR,
         announceResult.exception().what().toStdString()});
    co_return;
  }
  if (announceResult->hasError()) {
    XLOG(DBG1) << "Application announce error err="
               << announceResult->error().reasonPhrase;
    auto annErr = std::move(announceResult->error());
    annErr.requestID = announce.requestID; // In case app got it wrong
    announceError(annErr);
  } else {
    auto handle = std::move(announceResult->value());
    auto announceOkMsg = handle->announceOk();
    announceOk(announceOkMsg);
    // TODO: what about UNANNOUNCE before ANNOUNCE_OK
    subscriberAnnounces_[announce.trackNamespace] = std::move(handle);
  }
}

void MoQSession::onAnnounceOk(AnnounceOk annOk) {
  XLOG(DBG1) << __func__ << " ns=" << annOk.trackNamespace << " sess=" << this;

  if (logger_) {
    logger_->logAnnounceOk(
        annOk, MOQTByteStringType::STRING_VALUE, ControlMessageType::PARSED);
  }

  auto reqID = annOk.requestID;
  auto annIt = pendingRequests_.find(reqID);
  if (annIt == pendingRequests_.end()) {
    // unknown
    XLOG(ERR) << "No matching announce reqID=" << reqID
              << " trackNamespace=" << annOk.trackNamespace << " sess=" << this;
    close(SessionCloseErrorCode::PROTOCOL_VIOLATION);
    return;
  }

  auto* announcePtr = annIt->second.tryGetAnnounce();
  if (!announcePtr) {
    XLOG(ERR) << "Request ID " << reqID
              << " is not an announce request, sess=" << this;
    close(SessionCloseErrorCode::PROTOCOL_VIOLATION);
    return;
  }

  publisherAnnounces_[announcePtr->trackNamespace] =
      std::move(announcePtr->callback);
  annOk.trackNamespace = announcePtr->trackNamespace;
  announcePtr->promise.setValue(std::move(annOk));
  pendingRequests_.erase(annIt);
}

void MoQSession::onAnnounceError(AnnounceError announceError) {
  XLOG(DBG1) << __func__ << " reqID=" << announceError.requestID.value
             << " sess=" << this;

  if (logger_) {
    logger_->logAnnounceError(
        announceError,
        TrackNamespace(), // TODO: real namespace from pendingRequest
        MOQTByteStringType::STRING_VALUE,
        ControlMessageType::PARSED);
  }
  auto reqID = announceError.requestID.value;
  auto annIt = pendingRequests_.find(reqID);
  if (annIt == pendingRequests_.end()) {
    // unknown
    XLOG(ERR) << "No matching announce requestID=" << reqID << " sess=" << this;
    close(SessionCloseErrorCode::PROTOCOL_VIOLATION);
    return;
  }

  auto* announcePtr = annIt->second.tryGetAnnounce();
  if (!announcePtr) {
    XLOG(ERR) << "Request ID " << reqID
              << " is not an announce request, sess=" << this;
    close(SessionCloseErrorCode::PROTOCOL_VIOLATION);
    return;
  }

  announcePtr->promise.setValue(
      folly::makeUnexpected(std::move(announceError)));
  pendingRequests_.erase(annIt);
}

void MoQSession::onUnannounce(Unannounce unAnn) {
  XLOG(DBG1) << __func__ << " ns=" << unAnn.trackNamespace << " sess=" << this;
  MOQ_SUBSCRIBER_STATS(subscriberStatsCallback_, onUnannounce);

  if (logger_) {
    logger_->logUnannounce(
        unAnn, MOQTByteStringType::STRING_VALUE, ControlMessageType::PARSED);
  }

  auto annIt = subscriberAnnounces_.find(unAnn.trackNamespace);
  if (annIt == subscriberAnnounces_.end()) {
    XLOG(ERR) << "Unannounce for bad namespace ns=" << unAnn.trackNamespace;
  } else {
    annIt->second->unannounce();
    subscriberAnnounces_.erase(annIt);
    retireRequestID(/*signalWriteLoop=*/true);
  }
}

void MoQSession::announceCancel(const AnnounceCancel& annCan) {
  MOQ_SUBSCRIBER_STATS(subscriberStatsCallback_, onAnnounceCancel);
  auto res = moqFrameWriter_.writeAnnounceCancel(controlWriteBuf_, annCan);
  if (!res) {
    XLOG(ERR) << "writeAnnounceCancel failed sess=" << this;
  }
  controlWriteEvent_.signal();
  subscriberAnnounces_.erase(annCan.trackNamespace);
  retireRequestID(/*signalWriteLoop=*/false);

  if (logger_) {
    logger_->logAnnounceCancel(annCan);
  }
}

void MoQSession::onAnnounceCancel(AnnounceCancel announceCancel) {
  XLOG(DBG1) << __func__ << " ns=" << announceCancel.trackNamespace
             << " sess=" << this;
  MOQ_PUBLISHER_STATS(publisherStatsCallback_, onAnnounceCancel);

  if (logger_) {
    logger_->logAnnounceCancel(
        announceCancel,
        MOQTByteStringType::STRING_VALUE,
        ControlMessageType::PARSED);
  }

  auto it = publisherAnnounces_.find(announceCancel.trackNamespace);
  if (it == publisherAnnounces_.end()) {
    XLOG(ERR) << "Invalid announce cancel ns=" << announceCancel.trackNamespace;
  } else {
    it->second->announceCancel(
        announceCancel.errorCode, std::move(announceCancel.reasonPhrase));
    publisherAnnounces_.erase(it);
  }
}

void MoQSession::onSubscribeAnnounces(SubscribeAnnounces sa) {
  XLOG(DBG1) << __func__ << " prefix=" << sa.trackNamespacePrefix
             << " sess=" << this;
  if (logger_) {
    logger_->logSubscribeAnnounces(
        sa, MOQTByteStringType::STRING_VALUE, ControlMessageType::PARSED);
  }
  if (closeSessionIfRequestIDInvalid(sa.requestID, false, true)) {
    return;
  }
  if (!publishHandler_) {
    XLOG(DBG1) << __func__ << "No publisher callback set";
    subscribeAnnouncesError(
        {sa.requestID,
         SubscribeAnnouncesErrorCode::NOT_SUPPORTED,
         "Not a publisher"});
    return;
  }
  co_withExecutor(exec_.get(), handleSubscribeAnnounces(std::move(sa))).start();
}

folly::coro::Task<void> MoQSession::handleSubscribeAnnounces(
    SubscribeAnnounces subAnn) {
  folly::RequestContextScopeGuard guard;
  setRequestSession();
  auto subAnnResult = co_await co_awaitTry(co_withCancellation(
      cancellationSource_.getToken(),
      publishHandler_->subscribeAnnounces(subAnn)));
  if (subAnnResult.hasException()) {
    XLOG(ERR) << "Exception in Publisher callback ex="
              << subAnnResult.exception().what().toStdString();
    subscribeAnnouncesError(
        {subAnn.requestID,
         SubscribeAnnouncesErrorCode::INTERNAL_ERROR,
         subAnnResult.exception().what().toStdString()});
    co_return;
  }
  if (subAnnResult->hasError()) {
    XLOG(DBG1) << "Application subAnn error err="
               << subAnnResult->error().reasonPhrase;
    auto subAnnErr = std::move(subAnnResult->error());
    subAnnErr.requestID = subAnn.requestID; // In case app got it wrong
    subscribeAnnouncesError(subAnnErr);
  } else {
    auto handle = std::move(subAnnResult->value());
    auto subAnnOk = handle->subscribeAnnouncesOk();
    subAnnOk.trackNamespacePrefix = subAnn.trackNamespacePrefix;
    subscribeAnnouncesOk(subAnnOk);
    subscribeAnnounces_[subAnn.trackNamespacePrefix] = std::move(handle);
  }
}

void MoQSession::onSubscribeAnnouncesOk(SubscribeAnnouncesOk saOk) {
  XLOG(DBG1) << __func__ << " prefix=" << saOk.trackNamespacePrefix
             << " sess=" << this;
  if (logger_) {
    logger_->logSubscribeAnnouncesOk(
        saOk, MOQTByteStringType::STRING_VALUE, ControlMessageType::PARSED);
  }
  auto reqID = saOk.requestID;
  auto saIt = pendingRequests_.find(reqID);
  if (saIt == pendingRequests_.end()) {
    // unknown
    XLOG(ERR) << "No matching subscribeAnnounces reqID=" << reqID
              << " trackNamespace=" << saOk.trackNamespacePrefix
              << " sess=" << this;
    close(SessionCloseErrorCode::PROTOCOL_VIOLATION);
    return;
  }

  auto* subscribeAnnouncesPtr = saIt->second.tryGetSubscribeAnnounces();
  if (!subscribeAnnouncesPtr) {
    XLOG(ERR) << "Request ID " << reqID
              << " is not a subscribe announces request, sess=" << this;
    close(SessionCloseErrorCode::PROTOCOL_VIOLATION);
    return;
  }

  subscribeAnnouncesPtr->setValue(std::move(saOk));
  pendingRequests_.erase(saIt);
}

void MoQSession::onSubscribeAnnouncesError(
    SubscribeAnnouncesError subscribeAnnouncesError) {
  XLOG(DBG1) << __func__ << " reqID=" << subscribeAnnouncesError.requestID.value
             << " sess=" << this;
  if (logger_) {
    logger_->logSubscribeAnnouncesError(
        subscribeAnnouncesError,
        TrackNamespace(), // TODO: use namespace from pendingRequest
        MOQTByteStringType::STRING_VALUE,
        ControlMessageType::PARSED);
  }
  auto reqID = subscribeAnnouncesError.requestID.value;
  auto saIt = pendingRequests_.find(reqID);
  if (saIt == pendingRequests_.end()) {
    // unknown
    XLOG(ERR) << "No matching subscribeAnnounces reqID=" << reqID
              << " sess=" << this;
    close(SessionCloseErrorCode::PROTOCOL_VIOLATION);
    return;
  }

  auto* subscribeAnnouncesPtr = saIt->second.tryGetSubscribeAnnounces();
  if (!subscribeAnnouncesPtr) {
    XLOG(ERR) << "Request ID " << reqID
              << " is not a subscribe announces request, sess=" << this;
    close(SessionCloseErrorCode::PROTOCOL_VIOLATION);
    return;
  }

  subscribeAnnouncesPtr->setValue(
      folly::makeUnexpected(std::move(subscribeAnnouncesError)));
  pendingRequests_.erase(saIt);
}

void MoQSession::onUnsubscribeAnnounces(UnsubscribeAnnounces unsub) {
  XLOG(DBG1) << __func__ << " prefix=" << unsub.trackNamespacePrefix
             << " sess=" << this;
  if (logger_) {
    logger_->logUnsubscribeAnnounces(
        unsub, MOQTByteStringType::STRING_VALUE, ControlMessageType::PARSED);
  }
  MOQ_PUBLISHER_STATS(publisherStatsCallback_, onUnsubscribeAnnounces);
  if (!publishHandler_) {
    XLOG(DBG1) << __func__ << "No publisher callback set";
    return;
  }
  auto saIt = subscribeAnnounces_.find(unsub.trackNamespacePrefix);
  if (saIt == subscribeAnnounces_.end()) {
    XLOG(ERR) << "Invalid unsub announce nsp=" << unsub.trackNamespacePrefix;
  } else {
    folly::RequestContextScopeGuard guard;
    setRequestSession();
    saIt->second->unsubscribeAnnounces();
    subscribeAnnounces_.erase(saIt);
    retireRequestID(/*signalWriteLoop=*/true);
  }
}

void MoQSession::onTrackStatusRequest(TrackStatusRequest trackStatusRequest) {
  MOQ_PUBLISHER_STATS(publisherStatsCallback_, onTrackStatus);
  XLOG(DBG1) << __func__ << " ftn=" << trackStatusRequest.fullTrackName
             << " sess=" << this;
  if (logger_) {
    logger_->logTrackStatusRequest(
        trackStatusRequest,
        MOQTByteStringType::STRING_VALUE,
        ControlMessageType::PARSED);
  }
  if (closeSessionIfRequestIDInvalid(
          trackStatusRequest.requestID, false, true)) {
    return;
  }
  if (!publishHandler_) {
    XLOG(DBG1) << __func__ << "No publisher callback set";
    writeTrackStatus(
        {trackStatusRequest.requestID,
         trackStatusRequest.fullTrackName,
         TrackStatusCode::UNKNOWN,
         folly::none});
  } else {
    co_withExecutor(
        exec_.get(), handleTrackStatus(std::move(trackStatusRequest)))
        .start();
  }
}

folly::coro::Task<void> MoQSession::handleTrackStatus(
    TrackStatusRequest trackStatusReq) {
  auto trackStatusResult = co_await co_awaitTry(co_withCancellation(
      cancellationSource_.getToken(),
      publishHandler_->trackStatus(trackStatusReq)));
  if (trackStatusResult.hasException()) {
    XLOG(ERR) << "Exception in Publisher callback ex="
              << trackStatusResult.exception().what().toStdString();
    writeTrackStatus(
        {trackStatusReq.requestID,
         trackStatusReq.fullTrackName,
         TrackStatusCode::UNKNOWN,
         folly::none});
  } else {
    trackStatusResult->requestID = trackStatusReq.requestID;
    trackStatusResult->fullTrackName = trackStatusReq.fullTrackName;
    writeTrackStatus(trackStatusResult.value());
  }
  retireRequestID(/*signalWriteLoop=*/false);
}

void MoQSession::writeTrackStatus(const TrackStatus& trackStatus) {
  auto res = moqFrameWriter_.writeTrackStatus(controlWriteBuf_, trackStatus);

  if (logger_) {
    logger_->logTrackStatus(trackStatus);
  }

  if (!res) {
    XLOG(ERR) << "writeTrackStatus failed sess=" << this;
  } else {
    controlWriteEvent_.signal();
  }
}

folly::coro::Task<MoQSession::TrackStatusResult> MoQSession::trackStatus(
    TrackStatusRequest trackStatusRequest) {
  MOQ_SUBSCRIBER_STATS(subscriberStatsCallback_, onTrackStatus);
  XLOG(DBG1) << __func__ << " ftn=" << trackStatusRequest.fullTrackName
             << "sess=" << this;
  aliasifyAuthTokens(trackStatusRequest.params);
  trackStatusRequest.requestID = getNextRequestID();

  auto res = moqFrameWriter_.writeTrackStatusRequest(
      controlWriteBuf_, trackStatusRequest);
  if (!res) {
    XLOG(ERR) << "writeTrackStatusREquest failed sess=" << this;
    co_return TrackStatusResult{
        trackStatusRequest.requestID,
        trackStatusRequest.fullTrackName,
        TrackStatusCode::UNKNOWN,
        folly::none};
  }
  if (logger_) {
    logger_->logTrackStatusRequest(trackStatusRequest);
  }
  controlWriteEvent_.signal();
  auto contract = folly::coro::makePromiseContract<TrackStatus>();
  pendingRequests_.emplace(
      trackStatusRequest.requestID,
      PendingRequestState::makeTrackStatus(std::move(contract.first)));
  co_return co_await std::move(contract.second);
}

void MoQSession::onTrackStatus(TrackStatus trackStatus) {
  XLOG(DBG1) << __func__ << " ftn=" << trackStatus.fullTrackName
             << " code=" << uint64_t(trackStatus.statusCode)
             << " sess=" << this;
  auto reqID = trackStatus.requestID;
  auto trackStatusIt = pendingRequests_.find(reqID);

  if (logger_) {
    logger_->logTrackStatus(
        trackStatus,
        MOQTByteStringType::STRING_VALUE,
        ControlMessageType::PARSED);
  }
  if (trackStatusIt == pendingRequests_.end()) {
    XLOG(ERR) << __func__
              << " Couldn't find a pending TrackStatusRequest for reqID="
              << reqID << " ftn=" << trackStatus.fullTrackName;
    close(SessionCloseErrorCode::PROTOCOL_VIOLATION);
    return;
  }

  auto* trackStatusPtr = trackStatusIt->second.tryGetTrackStatus();
  if (!trackStatusPtr) {
    XLOG(ERR) << "Request ID " << reqID
              << " is not a track status request, sess=" << this;
    close(SessionCloseErrorCode::PROTOCOL_VIOLATION);
    return;
  }

  trackStatusPtr->setValue(std::move(trackStatus));
  pendingRequests_.erase(trackStatusIt);
}

void MoQSession::onGoaway(Goaway goaway) {
  XLOG(DBG1) << __func__ << " sess=" << this;

  if (logger_) {
    logger_->logGoaway(goaway, ControlMessageType::PARSED);
  }
  if (receivedGoaway_) {
    XLOG(ERR) << "Received multiple GOAWAYs sess=" << this;
    close(SessionCloseErrorCode::PROTOCOL_VIOLATION);
    return;
  }
  if (dir_ == MoQControlCodec::Direction::SERVER &&
      !goaway.newSessionUri.empty()) {
    XLOG(ERR) << "Server received GOAWAY newSessionUri sess=" << this;
    close(SessionCloseErrorCode::PROTOCOL_VIOLATION);
    return;
  }
  receivedGoaway_ = true;
  folly::RequestContextScopeGuard guard;
  setRequestSession();
  if (publishHandler_) {
    publishHandler_->goaway(goaway);
  }
  // This doesn't work if the application made a single object inherit both
  // classes but provided separate instances.  But that's unlikely.
  if (subscribeHandler_ &&
      typeid(subscribeHandler_.get()) != typeid(publishHandler_.get())) {
    subscribeHandler_->goaway(goaway);
  }
}

void MoQSession::onConnectionError(ErrorCode error) {
  XLOG(DBG1) << __func__ << " sess=" << this;
  XLOG(ERR) << "MoQCodec control stream parse error err="
            << folly::to_underlying(error);
  // TODO: This error is coming from MoQCodec -- do we need a better
  // error code?
  close(SessionCloseErrorCode::PROTOCOL_VIOLATION);
}

folly::coro::Task<Subscriber::AnnounceResult> MoQSession::announce(
    Announce ann,
    std::shared_ptr<AnnounceCallback> announceCallback) {
  XLOG(DBG1) << __func__ << " ns=" << ann.trackNamespace << " sess=" << this;

  if (logger_) {
    logger_->logAnnounce(ann);
  }

  auto announceStartTime = std::chrono::steady_clock::now();
  SCOPE_EXIT {
    auto duration = (std::chrono::steady_clock::now() - announceStartTime);
    auto durationMsec =
        std::chrono::duration_cast<std::chrono::milliseconds>(duration);
    MOQ_PUBLISHER_STATS(
        publisherStatsCallback_, recordAnnounceLatency, durationMsec.count());
  };
  aliasifyAuthTokens(ann.params);
  ann.requestID = getNextRequestID();
  auto res = moqFrameWriter_.writeAnnounce(controlWriteBuf_, ann);
  if (!res) {
    XLOG(ERR) << "writeAnnounce failed sess=" << this;
    co_return folly::makeUnexpected(AnnounceError(
        {ann.requestID,
         AnnounceErrorCode::INTERNAL_ERROR,
         "local write failed"}));
  }
  controlWriteEvent_.signal();
  auto contract = folly::coro::makePromiseContract<
      folly::Expected<AnnounceOk, AnnounceError>>();
  pendingRequests_.emplace(
      ann.requestID,
      PendingRequestState::makeAnnounce(PendingAnnounce(
          {std::move(ann.trackNamespace),
           std::move(contract.first),
           std::move(announceCallback)})));
  auto announceResult = co_await std::move(contract.second);
  if (announceResult.hasError()) {
    MOQ_PUBLISHER_STATS(
        publisherStatsCallback_,
        onAnnounceError,
        announceResult.error().errorCode);
    co_return folly::makeUnexpected(announceResult.error());
  } else {
    MOQ_PUBLISHER_STATS(publisherStatsCallback_, onAnnounceSuccess);
    co_return std::make_shared<PublisherAnnounceHandle>(
        shared_from_this(), std::move(announceResult.value()));
  }
}

Subscriber::PublishResult MoQSession::publish(
    PublishRequest pub,
    std::shared_ptr<Publisher::SubscriptionHandle> handle) {
  XLOG(DBG1) << __func__ << " reqID=" << pub.requestID
             << " track=" << pub.fullTrackName.trackName << " sess=" << this;

  // Reject new publish attempts if session is draining
  if (draining_) {
    XLOG(DBG1) << "Rejecting publish request, session draining sess=" << this;
    return folly::makeUnexpected(PublishError{
        pub.requestID, PublishErrorCode::INTERNAL_ERROR, "Session draining"});
  }

  if (!handle) {
    XLOG(DBG1) << "Rejecting publish request, no subscription handle sess="
               << this;
    return folly::makeUnexpected(PublishError{
        pub.requestID,
        PublishErrorCode::INTERNAL_ERROR,
        "No subscription handle"});
  }

  auto publishStartTime = std::chrono::steady_clock::now();
  SCOPE_EXIT {
    auto duration = (std::chrono::steady_clock::now() - publishStartTime);
    auto durationMsec =
        std::chrono::duration_cast<std::chrono::milliseconds>(duration);
    MOQ_PUBLISHER_STATS(
        publisherStatsCallback_, recordPublishLatency, durationMsec.count());
  };

  aliasifyAuthTokens(pub.params);
  pub.requestID = getNextRequestID();
  pub.trackAlias = TrackAlias(pub.requestID.value);
  XLOG(DBG1) << "publish() got requestID=" << pub.requestID
             << " nextRequestID_=" << nextRequestID_
             << " peerMaxRequestID_=" << peerMaxRequestID_ << " sess=" << this;
  auto res = moqFrameWriter_.writePublish(controlWriteBuf_, pub);
  if (!res) {
    XLOG(ERR) << "writePublish failed sess=" << this;
    return folly::makeUnexpected(PublishError{
        pub.requestID, PublishErrorCode::INTERNAL_ERROR, "local write failed"});
  }
  controlWriteEvent_.signal();

  // Create TrackConsumer for the publisher to write data
  auto trackPublisher = std::make_shared<TrackPublisherImpl>(
      this,
      pub.fullTrackName,
      pub.requestID,
      pub.trackAlias,
      0, // priority
      pub.groupOrder,
      *negotiatedVersion_,
      moqSettings_.bufferingThresholds.perSubscription,
      pub.forward);

  // Set publishHandle in trackPublisher so it can cancel on unsubscribes
  trackPublisher->setSubscriptionHandle(handle);

  // Store the track publisher for later lookup
  pubTracks_.emplace(pub.requestID, trackPublisher);

  // Create Contract and place in pending publishes
  auto contract = folly::coro::makePromiseContract<
      folly::Expected<PublishOk, PublishError>>();
  pendingRequests_.emplace(
      pub.requestID,
      PendingRequestState::makePublish(std::move(contract.first)));

  // Build replyTask that co_awaits the future and handles cleanup
  auto replyTask = folly::coro::co_invoke(
      [fut = std::move(contract.second),
       rid = pub.requestID,
       trackPublisher,
       this]() mutable
      -> folly::coro::Task<folly::Expected<PublishOk, PublishError>> {
        auto result = co_await std::move(fut);
        if (result.hasValue()) {
          auto it = pubTracks_.find(rid);
          if (it != pubTracks_.end()) {
            // If Receive PublishOk is received, update the trackPublisher
            // If Receive PublishError, the session is reset in onPublishError()
            trackPublisher->setForward(result.value().forward);
            trackPublisher->setGroupOrder(result.value().groupOrder);
            trackPublisher->setSubPriority(result.value().subscriberPriority);
          }
        }
        trackPublisher.reset();
        co_return result;
      });

  // Return PublishConsumerAndReplyTask immediately (no co_await)
  return Subscriber::PublishConsumerAndReplyTask{
      std::static_pointer_cast<TrackConsumer>(trackPublisher),
      std::move(replyTask)};
}

void MoQSession::announceOk(const AnnounceOk& annOk) {
  XLOG(DBG1) << __func__ << " ns=" << annOk.trackNamespace << " sess=" << this;

  if (logger_) {
    logger_->logAnnounceOk(annOk);
  }

  MOQ_SUBSCRIBER_STATS(subscriberStatsCallback_, onAnnounceSuccess);
  auto res = moqFrameWriter_.writeAnnounceOk(controlWriteBuf_, annOk);
  if (!res) {
    XLOG(ERR) << "writeAnnounceOk failed sess=" << this;
    return;
  }
  controlWriteEvent_.signal();
}

void MoQSession::announceError(const AnnounceError& announceError) {
  XLOG(DBG1) << __func__ << " reqID=" << announceError.requestID.value
             << " sess=" << this;
  MOQ_SUBSCRIBER_STATS(
      subscriberStatsCallback_, onAnnounceError, announceError.errorCode);
  auto res =
      moqFrameWriter_.writeAnnounceError(controlWriteBuf_, announceError);
  if (!res) {
    XLOG(ERR) << "writeAnnounceError failed sess=" << this;
    return;
  }

  // Log AnnounceError
  if (logger_) {
    logger_->logAnnounceError(
        announceError, TrackNamespace() // TODO: pass real namespace
    );
  }

  controlWriteEvent_.signal();
}

void MoQSession::publishOk(const PublishOk& pubOk) {
  XLOG(DBG1) << __func__ << " reqID=" << pubOk.requestID << " sess=" << this;
  MOQ_SUBSCRIBER_STATS(subscriberStatsCallback_, onPublishOk);

  auto res = moqFrameWriter_.writePublishOk(controlWriteBuf_, pubOk);
  if (!res) {
    XLOG(ERR) << "writePublishOk failed sess=" << this;
    return;
  }
  controlWriteEvent_.signal();
}

void MoQSession::publishError(const PublishError& publishError) {
  XLOG(DBG1) << __func__ << " reqID=" << publishError.requestID
             << " sess=" << this;
  MOQ_SUBSCRIBER_STATS(
      subscriberStatsCallback_, onPublishError, publishError.errorCode);
  auto res = moqFrameWriter_.writePublishError(controlWriteBuf_, publishError);
  if (!res) {
    XLOG(ERR) << "writePublishError failed sess=" << this;
    return;
  }
  controlWriteEvent_.signal();

  auto aliasRes = reqIdToTrackAlias_.find(publishError.requestID);
  if (aliasRes == reqIdToTrackAlias_.end()) {
    XLOG(ERR) << "No track alias found for requestId="
              << publishError.requestID;
    return;
  }
  removeSubscriptionState(aliasRes->second, publishError.requestID);
}

void MoQSession::unannounce(const Unannounce& unann) {
  XLOG(DBG1) << __func__ << " ns=" << unann.trackNamespace << " sess=" << this;

  if (logger_) {
    logger_->logUnannounce(unann);
  }

  MOQ_PUBLISHER_STATS(publisherStatsCallback_, onUnannounce);
  auto it = publisherAnnounces_.find(unann.trackNamespace);
  if (it == publisherAnnounces_.end()) {
    // Not established but could be pending
    auto pendingIt = std::find_if(
        pendingRequests_.begin(),
        pendingRequests_.end(),
        [&unann](const auto& pair) {
          if (auto* announcePtr = pair.second.tryGetAnnounce()) {
            return announcePtr->trackNamespace == unann.trackNamespace;
          }
          return false;
        });

    if (pendingIt != pendingRequests_.end()) {
      if (auto* announcePtr = pendingIt->second.tryGetAnnounce()) {
        announcePtr->promise.setValue(folly::makeUnexpected(AnnounceError(
            {pendingIt->first,
             AnnounceErrorCode::INTERNAL_ERROR,
             "Unannounce before announce"})));
      }
      pendingRequests_.erase(pendingIt);
    } else {
      XLOG(ERR) << "Unannounce (cancelled?) ns=" << unann.trackNamespace;
      return;
    }
  }
  auto trackNamespace = unann.trackNamespace;
  auto res = moqFrameWriter_.writeUnannounce(controlWriteBuf_, unann);
  if (!res) {
    XLOG(ERR) << "writeUnannounce failed sess=" << this;
  }
  controlWriteEvent_.signal();
}

class MoQSession::SubscribeAnnouncesHandle
    : public Publisher::SubscribeAnnouncesHandle {
 public:
  SubscribeAnnouncesHandle(
      std::shared_ptr<MoQSession> session,
      SubscribeAnnouncesOk subAnnOk)
      : Publisher::SubscribeAnnouncesHandle(std::move(subAnnOk)),
        session_(std::move(session)) {}
  SubscribeAnnouncesHandle(const SubscribeAnnouncesHandle&) = delete;
  SubscribeAnnouncesHandle& operator=(const SubscribeAnnouncesHandle&) = delete;
  SubscribeAnnouncesHandle(SubscribeAnnouncesHandle&&) = delete;
  SubscribeAnnouncesHandle& operator=(SubscribeAnnouncesHandle&&) = delete;
  ~SubscribeAnnouncesHandle() override {
    unsubscribeAnnounces();
  }

  void unsubscribeAnnounces() override {
    if (session_) {
      session_->unsubscribeAnnounces(
          {subscribeAnnouncesOk_->trackNamespacePrefix});
      session_.reset();
    }
  }

 private:
  std::shared_ptr<MoQSession> session_;
};

folly::coro::Task<Publisher::SubscribeAnnouncesResult>
MoQSession::subscribeAnnounces(SubscribeAnnounces sa) {
  XLOG(DBG1) << __func__ << " prefix=" << sa.trackNamespacePrefix
             << " sess=" << this;
  auto trackNamespace = sa.trackNamespacePrefix;
  aliasifyAuthTokens(sa.params);
  sa.requestID = getNextRequestID();

  auto res = moqFrameWriter_.writeSubscribeAnnounces(controlWriteBuf_, sa);
  if (!res) {
    XLOG(ERR) << "writeSubscribeAnnounces failed sess=" << this;
    co_return folly::makeUnexpected(SubscribeAnnouncesError(
        {0,
         SubscribeAnnouncesErrorCode::INTERNAL_ERROR,
         "local write failed"}));
  }
  if (logger_) {
    logger_->logSubscribeAnnounces(sa);
  }
  controlWriteEvent_.signal();
  auto contract = folly::coro::makePromiseContract<
      folly::Expected<SubscribeAnnouncesOk, SubscribeAnnouncesError>>();
  pendingRequests_.emplace(
      sa.requestID,
      PendingRequestState::makeSubscribeAnnounces(std::move(contract.first)));
  auto subAnnResult = co_await std::move(contract.second);
  if (subAnnResult.hasError()) {
    MOQ_SUBSCRIBER_STATS(
        subscriberStatsCallback_,
        onSubscribeAnnouncesError,
        subAnnResult.error().errorCode);
    co_return folly::makeUnexpected(subAnnResult.error());
  } else {
    MOQ_SUBSCRIBER_STATS(subscriberStatsCallback_, onSubscribeAnnouncesSuccess);
    co_return std::make_shared<SubscribeAnnouncesHandle>(
        shared_from_this(), std::move(subAnnResult.value()));
  }
}

void MoQSession::subscribeAnnouncesOk(const SubscribeAnnouncesOk& saOk) {
  XLOG(DBG1) << __func__ << " prefix=" << saOk.trackNamespacePrefix
             << " sess=" << this;
  MOQ_PUBLISHER_STATS(publisherStatsCallback_, onSubscribeAnnouncesSuccess);
  auto res = moqFrameWriter_.writeSubscribeAnnouncesOk(controlWriteBuf_, saOk);
  if (!res) {
    XLOG(ERR) << "writeSubscribeAnnouncesOk failed sess=" << this;
    return;
  }

  if (logger_) {
    logger_->logSubscribeAnnouncesOk(saOk);
  }

  controlWriteEvent_.signal();
}

void MoQSession::subscribeAnnouncesError(
    const SubscribeAnnouncesError& subscribeAnnouncesError) {
  XLOG(DBG1) << __func__ << " reqID=" << subscribeAnnouncesError.requestID.value
             << " sess=" << this;
  MOQ_PUBLISHER_STATS(
      publisherStatsCallback_,
      onSubscribeAnnouncesError,
      subscribeAnnouncesError.errorCode);
  auto res = moqFrameWriter_.writeSubscribeAnnouncesError(
      controlWriteBuf_, subscribeAnnouncesError);
  if (!res) {
    XLOG(ERR) << "writeSubscribeAnnouncesError failed sess=" << this;
    return;
  }

  if (logger_) {
    logger_->logSubscribeAnnouncesError(
        subscribeAnnouncesError, TrackNamespace() // TODO: pass real namespace
    );
  }

  controlWriteEvent_.signal();
}

void MoQSession::unsubscribeAnnounces(const UnsubscribeAnnounces& unsubAnn) {
  XLOG(DBG1) << __func__ << " prefix=" << unsubAnn.trackNamespacePrefix
             << " sess=" << this;
  MOQ_SUBSCRIBER_STATS(subscriberStatsCallback_, onUnsubscribeAnnounces);
  auto res =
      moqFrameWriter_.writeUnsubscribeAnnounces(controlWriteBuf_, unsubAnn);
  if (!res) {
    XLOG(ERR) << "writeUnsubscribeAnnounces failed sess=" << this;
    return;
  }

  if (logger_) {
    logger_->logUnsubscribeAnnounces(unsubAnn);
  }

  controlWriteEvent_.signal();
}

RequestID MoQSession::getNextRequestID() {
  if (nextRequestID_ >= peerMaxRequestID_) {
    XLOG(WARN) << "Issuing request that will fail; nextRequestID_="
               << nextRequestID_ << " peerMaxRequestID_=" << peerMaxRequestID_
               << " sess=" << this;
  }
  auto ret = nextRequestID_;
  nextRequestID_ += getRequestIDMultiplier();
  return ret;
}

folly::coro::Task<Publisher::SubscribeResult> MoQSession::subscribe(
    SubscribeRequest sub,
    std::shared_ptr<TrackConsumer> callback) {
  XLOG(DBG1) << __func__ << " sess=" << this;

  // Log SubscribeRequest
  if (logger_) {
    logger_->logSubscribe(sub);
  }

  auto subscribeStartTime = std::chrono::steady_clock::now();
  SCOPE_EXIT {
    auto duration = (std::chrono::steady_clock::now() - subscribeStartTime);
    auto durationMsec =
        std::chrono::duration_cast<std::chrono::milliseconds>(duration);
    MOQ_SUBSCRIBER_STATS(
        subscriberStatsCallback_, recordSubscribeLatency, durationMsec.count());
  };
  if (draining_) {
    SubscribeError subscribeError = {
        std::numeric_limits<uint64_t>::max(),
        SubscribeErrorCode::INTERNAL_ERROR,
        "Draining session"};
    MOQ_SUBSCRIBER_STATS(
        subscriberStatsCallback_, onSubscribeError, subscribeError.errorCode);
    co_return folly::makeUnexpected(subscribeError);
  }
  auto fullTrackName = sub.fullTrackName;
  RequestID reqID = getNextRequestID();
  sub.requestID = reqID;
  TrackAlias trackAlias = reqID.value;
  sub.trackAlias = trackAlias;
  aliasifyAuthTokens(sub.params);

  auto wres = moqFrameWriter_.writeSubscribeRequest(controlWriteBuf_, sub);
  if (!wres) {
    XLOG(ERR) << "writeSubscribeRequest failed sess=" << this;
    SubscribeError subscribeError = {
        reqID, SubscribeErrorCode::INTERNAL_ERROR, "local write failed"};
    MOQ_SUBSCRIBER_STATS(
        subscriberStatsCallback_, onSubscribeError, subscribeError.errorCode);
    co_return folly::makeUnexpected(subscribeError);
  }
  controlWriteEvent_.signal();
  auto trackReceiveState = std::make_shared<SubscribeTrackReceiveState>(
      fullTrackName, reqID, callback, logger_);
  pendingRequests_.emplace(
      reqID, PendingRequestState::makeSubscribeTrack(trackReceiveState));
  auto subscribeResultTry =
      co_await co_awaitTry(trackReceiveState->subscribeFuture());
  if (subscribeResultTry.hasException()) {
    // likely cancellation
    XLOG(ERR) << "subscribeFuture exception=" << subscribeResultTry.exception();
    MOQ_SUBSCRIBER_STATS(
        subscriberStatsCallback_,
        onSubscribeError,
        SubscribeErrorCode::INTERNAL_ERROR);
    trackReceiveState->cancel();
    co_yield folly::coro::co_error(subscribeResultTry.exception());
  }
  auto subscribeResult = subscribeResultTry.value();
  XLOG(DBG1) << "Subscribe ready trackReceiveState=" << trackReceiveState
             << " requestID=" << reqID;
  if (subscribeResult.hasError()) {
    MOQ_SUBSCRIBER_STATS(
        subscriberStatsCallback_,
        onSubscribeError,
        subscribeResult.error().errorCode);
    co_return folly::makeUnexpected(subscribeResult.error());
  } else {
    MOQ_SUBSCRIBER_STATS(subscriberStatsCallback_, onSubscribeSuccess);
    co_return std::make_shared<ReceiverSubscriptionHandle>(
        std::move(subscribeResult.value()), trackAlias, shared_from_this());
  }
}

std::shared_ptr<TrackConsumer> MoQSession::subscribeOk(
    const SubscribeOk& subOk) {
  XLOG(DBG1) << __func__ << " sess=" << this;
  MOQ_PUBLISHER_STATS(publisherStatsCallback_, onSubscribeSuccess);
  auto it = pubTracks_.find(subOk.requestID);
  if (it == pubTracks_.end()) {
    XLOG(ERR) << "Invalid Subscribe OK, id=" << subOk.requestID;
    return nullptr;
  }
  auto trackPublisher =
      std::dynamic_pointer_cast<TrackPublisherImpl>(it->second);
  if (!trackPublisher) {
    XLOG(ERR) << "subscribe ID maps to a fetch, not a subscribe, id="
              << subOk.requestID;
    subscribeError(
        {subOk.requestID,
         SubscribeErrorCode::INTERNAL_ERROR,
         "Invalid internal state"});
    return nullptr;
  }
  trackPublisher->setGroupOrder(subOk.groupOrder);
  auto res = moqFrameWriter_.writeSubscribeOk(controlWriteBuf_, subOk);
  if (!res) {
    XLOG(ERR) << "writeSubscribeOk failed sess=" << this;
    return nullptr;
  }

  if (logger_) {
    logger_->logSubscribeOk(subOk);
  }

  controlWriteEvent_.signal();
  return std::static_pointer_cast<TrackConsumer>(trackPublisher);
}

void MoQSession::subscribeError(const SubscribeError& subErr) {
  XLOG(DBG1) << __func__ << " sess=" << this;
  MOQ_PUBLISHER_STATS(
      publisherStatsCallback_, onSubscribeError, subErr.errorCode);
  auto it = pubTracks_.find(subErr.requestID);
  if (it == pubTracks_.end()) {
    XLOG(ERR) << "Invalid Subscribe OK, id=" << subErr.requestID;
    return;
  }
  pubTracks_.erase(it);
  auto res = moqFrameWriter_.writeSubscribeError(controlWriteBuf_, subErr);
  retireRequestID(/*signalWriteLoop=*/false);
  if (!res) {
    XLOG(ERR) << "writeSubscribeError failed sess=" << this;
    return;
  }

  if (logger_) {
    logger_->logSubscribeError(subErr);
  }

  controlWriteEvent_.signal();
}

void MoQSession::unsubscribe(const Unsubscribe& unsubscribe) {
  XLOG(DBG1) << __func__ << " sess=" << this;

  MOQ_SUBSCRIBER_STATS(subscriberStatsCallback_, onUnsubscribe);
  auto trackAliasIt = reqIdToTrackAlias_.find(unsubscribe.requestID);
  if (trackAliasIt == reqIdToTrackAlias_.end()) {
    // unknown
    XLOG(ERR) << "No matching subscribe ID=" << unsubscribe.requestID
              << " sess=" << this;
    return;
  }
  auto trackIt = subTracks_.find(trackAliasIt->second);
  if (trackIt == subTracks_.end()) {
    // unknown
    XLOG(ERR) << "No matching subscribe ID=" << unsubscribe.requestID
              << " sess=" << this;
    return;
  }
  // no more callbacks after unsubscribe
  XLOG(DBG1) << "unsubscribing from ftn=" << trackIt->second->fullTrackName()
             << " sess=" << this;
  // cancel() should send STOP_SENDING on any open streams for this subscription
  trackIt->second->cancel();
  subTracks_.erase(trackIt);
  reqIdToTrackAlias_.erase(trackAliasIt);
  auto res = moqFrameWriter_.writeUnsubscribe(controlWriteBuf_, unsubscribe);
  if (!res) {
    XLOG(ERR) << "writeUnsubscribe failed sess=" << this;
    return;
  }

  // Log Unsubscribe
  if (logger_) {
    logger_->logUnsubscribe(unsubscribe);
  }

  controlWriteEvent_.signal();
  checkForCloseOnDrain();
}

folly::Expected<folly::Unit, MoQPublishError>
MoQSession::PublisherImpl::subscribeDone(SubscribeDone subscribeDone) {
  CHECK(session_) << "session_ is NULL in subscribeDone";
  XLOG(DBG1) << __func__ << " sess=" << session_;
  session_->subscribeDone(subscribeDone);
  return folly::unit;
}

void MoQSession::subscribeDone(const SubscribeDone& subDone) {
  XLOG(DBG1) << __func__ << " sess=" << this;
  MOQ_PUBLISHER_STATS(
      publisherStatsCallback_, onSubscribeDone, subDone.statusCode);
  auto it = pubTracks_.find(subDone.requestID);
  if (it == pubTracks_.end()) {
    XLOG(ERR) << "subscribeDone for invalid id=" << subDone.requestID
              << " sess=" << this;
    return;
  }
  pubTracks_.erase(it);

  auto res = moqFrameWriter_.writeSubscribeDone(controlWriteBuf_, subDone);
  if (!res) {
    XLOG(ERR) << "writeSubscribeDone failed sess=" << this;
    return;
  }

  if (logger_) {
    logger_->logSubscribeDone(subDone);
  }

  retireRequestID(/*signalWriteLoop=*/false);
  controlWriteEvent_.signal();
}

void MoQSession::retireRequestID(bool signalWriteLoop) {
  // If # of closed requests is greater than 1/2 of max requests, then
  // let's bump the maxRequestID by the number of closed requests.
  if (++closedRequests_ >= maxConcurrentRequests_ / 2) {
    maxRequestID_ += (closedRequests_ * getRequestIDMultiplier());
    closedRequests_ = 0;
    sendMaxRequestID(signalWriteLoop);
  }
}

void MoQSession::sendMaxRequestID(bool signalWriteLoop) {
  XLOG(DBG1) << "Issuing new maxRequestID=" << maxRequestID_
             << " sess=" << this;
  auto res = moqFrameWriter_.writeMaxRequestID(
      controlWriteBuf_, {.requestID = maxRequestID_});
  if (!res) {
    XLOG(ERR) << "writeMaxRequestID failed sess=" << this;
    return;
  }

  if (logger_) {
    logger_->logMaxSubscribeId(maxRequestID_);
  }

  if (signalWriteLoop) {
    controlWriteEvent_.signal();
  }
}

void MoQSession::PublisherImpl::fetchComplete() {
  std::exchange(session_, nullptr)->fetchComplete(requestID_);
}

void MoQSession::fetchComplete(RequestID requestID) {
  XLOG(DBG1) << __func__ << " sess=" << this;
  auto it = pubTracks_.find(requestID);
  if (it == pubTracks_.end()) {
    XLOG(ERR) << "fetchComplete for invalid id=" << requestID
              << " sess=" << this;
    return;
  }
  pubTracks_.erase(it);
  retireRequestID(/*signalWriteLoop=*/true);
}

void MoQSession::subscribeUpdate(const SubscribeUpdate& subUpdate) {
  if (logger_) {
    logger_->logSubscribeUpdate(subUpdate);
  }
  XLOG(DBG1) << __func__ << " sess=" << this;
  MOQ_SUBSCRIBER_STATS(subscriberStatsCallback_, onSubscribeUpdate);
  auto trackAliasIt = reqIdToTrackAlias_.find(subUpdate.subscriptionRequestID);
  if (trackAliasIt == reqIdToTrackAlias_.end()) {
    // unknown
    XLOG(ERR) << "No matching request ID=" << subUpdate.subscriptionRequestID
              << " sess=" << this;
    return;
  }
  auto trackIt = subTracks_.find(trackAliasIt->second);
  if (trackIt == subTracks_.end()) {
    // unknown
    XLOG(ERR) << "No matching track Alias=" << trackAliasIt->second
              << " sess=" << this;
    return;
  }
  auto res = moqFrameWriter_.writeSubscribeUpdate(controlWriteBuf_, subUpdate);
  if (!res) {
    XLOG(ERR) << "writeSubscribeUpdate failed sess=" << this;
    return;
  }
  controlWriteEvent_.signal();
}

class MoQSession::ReceiverFetchHandle : public Publisher::FetchHandle {
 public:
  ReceiverFetchHandle(FetchOk ok, std::shared_ptr<MoQSession> session)
      : FetchHandle(std::move(ok)), session_(std::move(session)) {}

  void fetchCancel() override {
    if (session_) {
      session_->fetchCancel({fetchOk_->requestID});
      session_.reset();
    }
  }

 private:
  std::shared_ptr<MoQSession> session_;
};

folly::coro::Task<Publisher::FetchResult> MoQSession::fetch(
    Fetch fetch,
    std::shared_ptr<FetchConsumer> consumer) {
  XLOG(DBG1) << __func__ << " sess=" << this;

  // Log Fetch
  if (logger_) {
    logger_->logFetch(fetch);
  }

  auto fetchStartTime = std::chrono::steady_clock::now();
  SCOPE_EXIT {
    auto duration = (std::chrono::steady_clock::now() - fetchStartTime);
    auto durationMsec =
        std::chrono::duration_cast<std::chrono::milliseconds>(duration);
    MOQ_SUBSCRIBER_STATS(
        subscriberStatsCallback_, recordFetchLatency, durationMsec.count());
  };
  auto g =
      folly::makeGuard([func = __func__] { XLOG(DBG1) << "exit " << func; });
  if (draining_) {
    FetchError fetchError = {
        std::numeric_limits<uint64_t>::max(),
        FetchErrorCode::INTERNAL_ERROR,
        "Draining session"};
    MOQ_SUBSCRIBER_STATS(
        subscriberStatsCallback_, onFetchError, fetchError.errorCode);
    co_return folly::makeUnexpected(fetchError);
  }

  auto [standalone, joining] = fetchType(fetch);
  FullTrackName fullTrackName = fetch.fullTrackName;
  if (joining) {
    std::shared_ptr<SubscribeTrackReceiveState> state;
    auto pendingIt = pendingRequests_.find(joining->joiningRequestID);
    if (pendingIt != pendingRequests_.end()) {
      if (auto* trackPtr = pendingIt->second.tryGetSubscribeTrack()) {
        state = *trackPtr;
      }
    } else {
      auto subIt = reqIdToTrackAlias_.find(joining->joiningRequestID);
      if (subIt != reqIdToTrackAlias_.end()) {
        state = getSubscribeTrackReceiveState(subIt->second);
      }
    }
    if (!state) {
      XLOG(ERR) << "API error, joining FETCH for invalid requestID="
                << joining->joiningRequestID.value << " sess=" << this;
      FetchError fetchError = {
          std::numeric_limits<uint64_t>::max(),
          FetchErrorCode::INTERNAL_ERROR,
          "Invalid JSID"};
      MOQ_SUBSCRIBER_STATS(
          subscriberStatsCallback_, onFetchError, fetchError.errorCode);
      co_return folly::makeUnexpected(fetchError);
    }

    if (fullTrackName != state->fullTrackName()) {
      XLOG(ERR) << "API error, track name mismatch=" << fullTrackName << ","
                << state->fullTrackName() << " sess=" << this;
      FetchError fetchError = {
          std::numeric_limits<uint64_t>::max(),
          FetchErrorCode::INTERNAL_ERROR,
          "Track name mismatch"};
      MOQ_SUBSCRIBER_STATS(
          subscriberStatsCallback_, onFetchError, fetchError.errorCode);
      co_return folly::makeUnexpected(fetchError);
    }
  }
  auto reqID = getNextRequestID();
  fetch.requestID = reqID;
  aliasifyAuthTokens(fetch.params);
  auto wres = moqFrameWriter_.writeFetch(controlWriteBuf_, fetch);
  if (!wres) {
    XLOG(ERR) << "writeFetch failed sess=" << this;
    FetchError fetchError = {
        reqID, FetchErrorCode::INTERNAL_ERROR, "local write failed"};
    MOQ_SUBSCRIBER_STATS(
        subscriberStatsCallback_, onFetchError, fetchError.errorCode);
    co_return folly::makeUnexpected(fetchError);
  }
  controlWriteEvent_.signal();
  auto trackReceiveState = std::make_shared<FetchTrackReceiveState>(
      fullTrackName, reqID, std::move(consumer), logger_);
  auto fetchTrack = fetches_.try_emplace(reqID, trackReceiveState);
  XCHECK(fetchTrack.second)
      << "RequestID already in use id=" << reqID << " sess=" << this;
  auto fetchResult = co_await trackReceiveState->fetchFuture();
  XLOG(DBG1) << __func__
             << " fetchReady trackReceiveState=" << trackReceiveState;
  if (fetchResult.hasError()) {
    XLOG(ERR) << fetchResult.error().reasonPhrase;
    MOQ_SUBSCRIBER_STATS(
        subscriberStatsCallback_, onFetchError, fetchResult.error().errorCode);
    co_return folly::makeUnexpected(fetchResult.error());
  } else {
    MOQ_SUBSCRIBER_STATS(subscriberStatsCallback_, onFetchSuccess);
    co_return std::make_shared<ReceiverFetchHandle>(
        std::move(fetchResult.value()), shared_from_this());
  }
}

void MoQSession::fetchOk(const FetchOk& fetchOk) {
  XLOG(DBG1) << __func__ << " sess=" << this;
  MOQ_PUBLISHER_STATS(publisherStatsCallback_, onFetchSuccess);
  auto res = moqFrameWriter_.writeFetchOk(controlWriteBuf_, fetchOk);
  if (!res) {
    XLOG(ERR) << "writeFetchOk failed sess=" << this;
    return;
  }
  if (logger_) {
    logger_->logFetchOk(fetchOk);
  }
  controlWriteEvent_.signal();
}

void MoQSession::fetchError(const FetchError& fetchErr) {
  XLOG(DBG1) << __func__ << " sess=" << this;
  MOQ_PUBLISHER_STATS(
      publisherStatsCallback_, onFetchError, fetchErr.errorCode);

  if (logger_) {
    logger_->logFetchError(fetchErr);
  }

  if (pubTracks_.erase(fetchErr.requestID) == 0) {
    // fetchError is called sometimes before adding publisher state, so this
    // is not an error
    XLOG(DBG1) << "fetchErr for invalid id=" << fetchErr.requestID
               << " sess=" << this;
  }
  auto res = moqFrameWriter_.writeFetchError(controlWriteBuf_, fetchErr);
  if (!res) {
    XLOG(ERR) << "writeFetchError failed sess=" << this;
    return;
  }
  controlWriteEvent_.signal();
}

void MoQSession::fetchCancel(const FetchCancel& fetchCan) {
  XLOG(DBG1) << __func__ << " sess=" << this;

  // Log FetchCancel
  if (logger_) {
    logger_->logFetchCancel(fetchCan);
  }

  auto trackIt = fetches_.find(fetchCan.requestID);
  if (trackIt == fetches_.end()) {
    XLOG(ERR) << "unknown subscribe ID=" << fetchCan.requestID
              << " sess=" << this;
    return;
  }
  trackIt->second->cancel(shared_from_this());
  auto res = moqFrameWriter_.writeFetchCancel(controlWriteBuf_, fetchCan);
  if (!res) {
    XLOG(ERR) << "writeFetchCancel failed sess=" << this;
    return;
  }
  controlWriteEvent_.signal();
}

folly::coro::Task<MoQSession::JoinResult> MoQSession::join(
    SubscribeRequest sub,
    std::shared_ptr<TrackConsumer> subscribeCallback,
    uint64_t joiningStart,
    uint8_t fetchPri,
    GroupOrder fetchOrder,
    std::vector<TrackRequestParameter> fetchParams,
    std::shared_ptr<FetchConsumer> fetchCallback,
    FetchType fetchType) {
  Fetch fetchReq(
      0,              // will be picked by fetch()
      nextRequestID_, // this will be the ID for subscribe()
      joiningStart,
      fetchType,
      fetchPri,
      fetchOrder,
      std::move(fetchParams));
  fetchReq.fullTrackName = sub.fullTrackName;
  auto [subscribeResult, fetchResult] = co_await folly::coro::collectAll(
      subscribe(std::move(sub), std::move(subscribeCallback)),
      fetch(std::move(fetchReq), std::move(fetchCallback)));
  co_return {subscribeResult, fetchResult};
}

void MoQSession::onNewUniStream(proxygen::WebTransport::StreamReadHandle* rh) {
  XLOG(DBG1) << __func__ << " sess=" << this;
  if (!setupComplete_) {
    XLOG(ERR) << "Uni stream before setup complete sess=" << this;
    close(SessionCloseErrorCode::PROTOCOL_VIOLATION);
    return;
  }
  // maybe not SUBGROUP_HEADER, but at least not control
  co_withExecutor(
      exec_.get(),
      co_withCancellation(
          cancellationSource_.getToken(),
          unidirectionalReadLoop(shared_from_this(), rh)))
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
    if (logger_) {
      logger_->logStreamTypeSet(
          bh.readHandle->getID(), MOQTStreamType::CONTROL, Owner::REMOTE);
    }

    bh.writeHandle->setPriority(0, 0, false);
    co_withExecutor(
        exec_.get(),
        co_withCancellation(
            cancellationSource_.getToken(), controlReadLoop(bh.readHandle)))
        .start();
    auto mergeToken = folly::cancellation_token_merge(
        cancellationSource_.getToken(), bh.writeHandle->getCancelToken());
    co_withExecutor(
        exec_.get(),
        co_withCancellation(
            std::move(mergeToken), controlWriteLoop(bh.writeHandle)))
        .start();
  }
}

void MoQSession::onDatagram(std::unique_ptr<folly::IOBuf> datagram) {
  XLOG(DBG1) << __func__ << " sess=" << this;
  if (!setupComplete_) {
    XLOG(ERR) << "Datagram before setup complete sess=" << this;
    close(SessionCloseErrorCode::PROTOCOL_VIOLATION);
    return;
  }
  std::unique_ptr<folly::IOBuf> payload;
  if (logger_) {
    payload = datagram->clone();
  }

  folly::IOBufQueue readBuf{folly::IOBufQueue::cacheChainLength()};

  readBuf.append(std::move(datagram));
  size_t remainingLength = readBuf.chainLength();
  folly::io::Cursor cursor(readBuf.front());
  auto type = quic::follyutils::decodeQuicInteger(cursor);
  if (!type) {
    XLOG(ERR) << __func__ << " Bad datagram header failed to parse type l="
              << readBuf.chainLength() << " sess=" << this;
    close(SessionCloseErrorCode::PROTOCOL_VIOLATION);
    return;
  }

  if (!isValidDatagramType(*negotiatedVersion_, type->first)) {
    XLOG(ERR) << __func__ << " Bad datagram header type=" << type->first;
    close(SessionCloseErrorCode::PROTOCOL_VIOLATION);
    return;
  }
  remainingLength -= type->second;
  MoQFrameParser parser;
  parser.initializeVersion(*negotiatedVersion_);
  auto res = parser.parseDatagramObjectHeader(
      cursor, DatagramType(type->first), remainingLength);
  if (res.hasError()) {
    XLOG(ERR) << __func__ << " Bad Datagram: Failed to parse object header";
    close(SessionCloseErrorCode::PROTOCOL_VIOLATION);
    return;
  }
  auto& objHeader = res.value();
  if (remainingLength != *objHeader.objectHeader.length) {
    XLOG(ERR) << __func__ << " Bad datagram: Length mismatch";
    close(SessionCloseErrorCode::PROTOCOL_VIOLATION);
    return;
  }
  auto alias = objHeader.trackAlias;
  auto state = getSubscribeTrackReceiveState(alias).get();
  if (!state) {
    constexpr size_t kMaxBufferedTracks = 10;
    constexpr size_t kMaxBufferedDatagramsPerTrack = 30;
    if (bufferedDatagrams_.size() > kMaxBufferedTracks) {
      XLOG(DBG2) << " Too many buffered tracks, dropping datagram for alias="
                 << alias << " sess = " << this;
      return;
    }
    auto it = bufferedDatagrams_.emplace(alias, std::list<Payload>());
    if (it.first->second.size() > kMaxBufferedDatagramsPerTrack) {
      XLOG(DBG2)
          << " Too many buffered datagrams for track, dropping datagram for alias="
          << alias << " sess = " << this;
      return;
    }
    it.first->second.push_back(readBuf.move());
    return;
  }
  readBuf.trimStart(readBuf.chainLength() - remainingLength);
  if (logger_) {
    if (objHeader.objectHeader.status == ObjectStatus::NORMAL) {
      logger_->logObjectDatagramParsed(
          objHeader.trackAlias, objHeader.objectHeader, payload);
    } else {
      logger_->logObjectDatagramStatusParsed(
          objHeader.trackAlias, objHeader.objectHeader);
    }
  }
  if (state) {
    auto callback = state->getSubscribeCallback();
    if (callback) {
      callback->datagram(objHeader.objectHeader, readBuf.move());
    }
  }
}

bool MoQSession::closeSessionIfRequestIDInvalid(
    RequestID requestID,
    bool skipCheck,
    bool isNewRequest,
    bool parityMatters) {
  if (skipCheck) {
    return false;
  }

  if (parityMatters &&
      ((requestID.value % 2) == 1) !=
          (dir_ == MoQControlCodec::Direction::CLIENT)) {
    XLOG(ERR) << "Invalid requestID parity: " << requestID << " sess=" << this;
    close(SessionCloseErrorCode::INVALID_REQUEST_ID);
    return true;
  }
  if (isNewRequest) {
    if (requestID.value >= maxRequestID_) {
      XLOG(ERR) << "Too many requests requestID: " << requestID
                << " sess=" << this;
      close(SessionCloseErrorCode::TOO_MANY_REQUESTS);
      return true;
    }
    if (requestID.value != nextExpectedPeerRequestID_) {
      XLOG(ERR) << "Invalid next requestID: " << requestID << " sess=" << this;
      close(SessionCloseErrorCode::INVALID_REQUEST_ID);
      return true;
    }
    nextExpectedPeerRequestID_ += getRequestIDMultiplier();
  } else {
    if (requestID.value >= maxRequestID_) {
      XLOG(ERR) << "Invalid requestID: " << requestID << " sess=" << this;
      close(SessionCloseErrorCode::INVALID_REQUEST_ID);
      return true;
    }
  }
  return false;
}

void MoQSession::initializeNegotiatedVersion(uint64_t negotiatedVersion) {
  negotiatedVersion_ = negotiatedVersion;
  moqFrameWriter_.initializeVersion(*negotiatedVersion_);
  controlCodec_.initializeVersion(*negotiatedVersion_);
  for (auto versionBaton : subgroupsWaitingForVersion_) {
    versionBaton->signal();
  }
  subgroupsWaitingForVersion_.clear();
}

/*static*/
uint64_t MoQSession::getMaxRequestIDIfPresent(
    const std::vector<SetupParameter>& params) {
  for (const auto& param : params) {
    if (param.key == folly::to_underlying(SetupKey::MAX_REQUEST_ID)) {
      return param.asUint64;
    }
  }
  return 0;
}

/*static*/
folly::Optional<std::string> MoQSession::getMoQTImplementationIfPresent(
    const std::vector<SetupParameter>& params) {
  for (const auto& param : params) {
    if (param.key == folly::to_underlying(SetupKey::MOQT_IMPLEMENTATION)) {
      return param.asString;
    }
  }
  return folly::none;
}

/*static*/
std::string MoQSession::getMoQTImplementationString() {
  return fmt::format(
      "Meta MoQ C++ Draft-{}", getDraftMajorVersion(kVersionDraftCurrent));
}

/*static*/
bool MoQSession::shouldIncludeMoqtImplementationParam(
    const std::vector<uint64_t>& supportedVersions) {
  for (const auto& version : supportedVersions) {
    if (getDraftMajorVersion(version) >= 14) {
      return true;
    }
  }
  return false;
}

uint64_t MoQSession::getMaxAuthTokenCacheSizeIfPresent(
    const std::vector<SetupParameter>& params) {
  constexpr uint64_t kMaxAuthTokenCacheSize = 4096;
  for (const auto& param : params) {
    if (param.key ==
        folly::to_underlying(SetupKey::MAX_AUTH_TOKEN_CACHE_SIZE)) {
      return std::min(param.asUint64, kMaxAuthTokenCacheSize);
    }
  }
  return 0;
}

void MoQSession::aliasifyAuthTokens(
    std::vector<Parameter>& params,
    const folly::Optional<uint64_t>& forceVersion) {
  auto version = forceVersion ? forceVersion : getNegotiatedVersion();
  if (!version) {
    return;
  }

  auto authParamKey = getAuthorizationParamKey(*version);
  for (auto it = params.begin(); it != params.end(); ++it) {
    if (it->key == authParamKey) {
      const auto& token = it->asAuthToken;
      if (token.alias && token.tokenValue.size() < tokenCache_.maxTokenSize()) {
        auto lookupRes =
            tokenCache_.getAliasForToken(token.tokenType, token.tokenValue);
        if (lookupRes) {
          it->asString = moqFrameWriter_.encodeUseAlias(*lookupRes);
          continue;
        } // else, evict tokens and register this one
        auto tokenCopy = token;
        it = params.erase(it);
        while (!tokenCache_.canRegister(token.tokenType, token.tokenValue)) {
          auto aliasToEvict = tokenCache_.evictLRU();
          Parameter p;
          p.key = authParamKey;
          p.asString = moqFrameWriter_.encodeDeleteTokenAlias(aliasToEvict);
          it = params.insert(it, std::move(p));
          ++it;
        }
        auto alias =
            tokenCache_.registerToken(tokenCopy.tokenType, tokenCopy.tokenValue)
                .value();
        Parameter p;
        p.key = authParamKey;
        p.asString = moqFrameWriter_.encodeRegisterToken(
            alias, tokenCopy.tokenType, tokenCopy.tokenValue);
        it = params.insert(it, std::move(p));
      } else {
        it->asString = moqFrameWriter_.encodeTokenValue(
            token.tokenType, token.tokenValue, version);
      }
    }
  }
}

// Static methods
std::shared_ptr<MoQSession> MoQSession::getRequestSession() {
  auto reqData =
      folly::RequestContext::get()->getContextData(sessionRequestToken());
  XCHECK(reqData);
  auto sessionData = dynamic_cast<MoQSessionRequestData*>(reqData);
  XCHECK(sessionData);
  XCHECK(sessionData->session);
  return sessionData->session;
}

GroupOrder MoQSession::resolveGroupOrder(
    GroupOrder pubOrder,
    GroupOrder subOrder) {
  return subOrder == GroupOrder::Default ? pubOrder : subOrder;
}

void MoQSession::setServerMaxTokenCacheSizeGuess(size_t size) {
  if (!setupComplete_ && dir_ == MoQControlCodec::Direction::CLIENT) {
    tokenCache_.setMaxSize(size);
  }
}

void MoQSession::setMaxConcurrentRequests(uint64_t maxConcurrent) {
  if (maxConcurrent > maxConcurrentRequests_) {
    auto delta = maxConcurrent - maxConcurrentRequests_;
    maxRequestID_ += delta;
    sendMaxRequestID(/*signalWriteLoop=*/true);
  }
}

void MoQSession::setVersion(uint64_t version) {
  negotiatedVersion_ = version;
  setupComplete_ = true;
}

void MoQSession::setMoqSettings(MoQSettings settings) {
  moqSettings_ = settings;
}

void MoQSession::setPublishHandler(std::shared_ptr<Publisher> publishHandler) {
  publishHandler_ = std::move(publishHandler);
}

void MoQSession::setSubscribeHandler(
    std::shared_ptr<Subscriber> subscribeHandler) {
  subscribeHandler_ = std::move(subscribeHandler);
}

} // namespace moxygen

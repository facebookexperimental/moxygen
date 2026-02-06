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
#include <quic/priority/HTTPPriorityQueue.h>
#include <moxygen/MoQTrackProperties.h>
#include <moxygen/events/MoQDeliveryTimeoutManager.h>

#include <folly/logging/xlog.h>

#include <utility>

namespace {
using namespace moxygen;
constexpr uint64_t kMaxSendTokenCacheSize(1024);

// Bit allocations for 32-bit order: sub=5, pub=8, group=14, subgroup=5
constexpr uint32_t kSubPriBits = 5;
constexpr uint32_t kPubPriBits = 8;
constexpr uint32_t kGroupBits = 14;
constexpr uint32_t kSubgroupBits = 5;

constexpr uint32_t kSubPriMask = (1 << kSubPriBits) - 1;     // 0x1F
constexpr uint32_t kPubPriMask = (1 << kPubPriBits) - 1;     // 0xFF
constexpr uint32_t kGroupMask = (1 << kGroupBits) - 1;       // 0x3FFF
constexpr uint32_t kSubgroupMask = (1 << kSubgroupBits) - 1; // 0x1F

uint32_t groupPriorityBits(GroupOrder groupOrder, uint64_t group) {
  // If the group order is oldest first, we want to give lower group
  // ids a higher precedence. Otherwise, if it is newest first, we want
  // to give higher group ids a higher precedence.
  uint32_t truncGroup = static_cast<uint32_t>(group) & kGroupMask;
  return groupOrder == GroupOrder::OldestFirst ? truncGroup
                                               : (kGroupMask - truncGroup);
}

uint32_t subgroupPriorityBits(uint32_t subgroupID) {
  return static_cast<uint32_t>(subgroupID) & kSubgroupMask;
}

struct StreamPriority {
  uint8_t urgency;
  uint32_t order;
};

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
StreamPriority getStreamPriority(
    uint64_t groupID,
    uint64_t subgroupID,
    uint8_t subPri,
    uint8_t pubPri,
    GroupOrder pubGroupOrder) {
  // 32-bit order: 5 sub pri | 8 pub pri | 14 group order | 5 subgroup
  // Urgency: 3 MSB of subPri
  uint8_t urgency = subPri >> 5;
  uint32_t subPriBits = subPri & kSubPriMask;
  uint32_t pubPriBits = pubPri & kPubPriMask;
  uint32_t groupBits = groupPriorityBits(pubGroupOrder, groupID);
  uint32_t subgroupBits = subgroupPriorityBits(subgroupID);
  uint32_t order = (subPriBits << (kPubPriBits + kGroupBits + kSubgroupBits)) |
      (pubPriBits << (kGroupBits + kSubgroupBits)) |
      (groupBits << kSubgroupBits) | subgroupBits;
  // Never use urgency=0, order=0 except for control stream
  if (urgency == 0 && order == 0) {
    order = 1;
  }
  return {urgency, order};
}

// Helper function to validate priority from application is set.
// Applications must always provide a set priority value.
folly::Expected<folly::Unit, MoQPublishError> validatePrioritySet(
    const std::optional<uint8_t>& priority) {
  if (!priority.has_value()) {
    return folly::makeUnexpected(MoQPublishError(
        MoQPublishError::API_ERROR,
        "Application must provide a priority value (cannot be unset)"));
  }
  return folly::unit;
}

// Helper function to elide priority for writing if it matches publisher
// priority from the control plane. Returns std::nullopt if priority matches
// publisherPriority, otherwise returns the original priority.
std::optional<uint8_t> elidePriorityForWrite(
    uint8_t priority,
    const std::optional<uint8_t>& publisherPriority) {
  if (priority == publisherPriority) {
    return std::nullopt;
  }
  return priority;
}

// Helper function to log RequestError with the correct logger method based on
// frameType
void logRequestError(
    const std::shared_ptr<MLogger>& logger,
    const RequestError& error,
    FrameType frameType,
    ControlMessageType msgType = ControlMessageType::PARSED) {
  if (!logger) {
    return;
  }
  switch (frameType) {
    case FrameType::SUBSCRIBE_ERROR:
      logger->logSubscribeError(error, msgType);
      break;
    case FrameType::PUBLISH_ERROR:
      logger->logPublishError(static_cast<const PublishError&>(error), msgType);
      break;
    case FrameType::FETCH_ERROR:
      logger->logFetchError(error, msgType);
      break;
    case FrameType::PUBLISH_NAMESPACE_ERROR:
      logger->logPublishNamespaceError(error);
      break;
    case FrameType::SUBSCRIBE_NAMESPACE_ERROR:
      logger->logSubscribeNamespaceError(error);
      break;
    default:
      // Unknown or unsupported error type for logging
      break;
  }
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
      std::shared_ptr<DeliveryCallback> deliveryCallback = nullptr,
      std::optional<std::chrono::milliseconds> deliveryTimeout = std::nullopt);

  // Subscribe constructor
  StreamPublisherImpl(
      std::shared_ptr<MoQSession::PublisherImpl> publisher,
      proxygen::WebTransport::StreamWriteHandle* writeHandle,
      TrackAlias alias,
      uint64_t groupID,
      uint64_t subgroupID,
      const std::optional<uint8_t>& sgPriority,
      SubgroupIDFormat format,
      bool includeExtensions,
      std::shared_ptr<MLogger> logger = nullptr,
      std::shared_ptr<DeliveryCallback> deliveryCallback = nullptr,
      std::optional<std::chrono::milliseconds> deliveryTimeout = std::nullopt);

  // SubgroupConsumer overrides
  // Note where the interface uses finSubgroup, this class uses finStream,
  // since it is used for fetch and subgroups
  folly::Expected<folly::Unit, MoQPublishError> object(
      uint64_t objectID,
      Payload payload,
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
      bool finFetch) override {
    if (!setGroupAndSubgroup(groupID, subgroupID)) {
      return folly::makeUnexpected(
          MoQPublishError(MoQPublishError::API_ERROR, "Group moved back"));
    }
    return publishStatus(
        objectID, ObjectStatus::END_OF_GROUP, noExtensions(), finFetch);
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
      if (streamComplete_) {
        return folly::makeUnexpected(
            MoQPublishError(MoQPublishError::CANCELLED, "Fetch cancelled"));
      }
      auto res = ensureWriteHandle();
      if (!res) {
        return res;
      }
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
    std::optional<TrackAlias> maybeTrackAlias = std::nullopt;
    if (streamType_ != StreamType::FETCH_HEADER) {
      maybeTrackAlias = trackAlias_;
    }
    while (!pendingDeliveries_.empty() &&
           pendingDeliveries_.front().endOffset <= offset) {
      auto& pendingDelivery = pendingDeliveries_.front();

      if (deliveryCallback_) {
        deliveryCallback_->onDelivered(
            maybeTrackAlias,
            pendingDelivery.groupId,
            pendingDelivery.subgroupId,
            pendingDelivery.objectId);
      }

      // Cancel delivery timeout timer when object is successfully delivered
      if (deliveryTimer_) {
        deliveryTimer_->cancelTimer(pendingDelivery.objectId);
      }
      pendingDeliveries_.pop_front();
    }
    onByteEventCommon(id, offset);
  }

  void onByteEventCanceled(quic::StreamId id, uint64_t offset) noexcept
      override {
    // Notify delivery cancelled for all objects delivered after this offset.
    // If delivery has been cancelled for an offset (e.g. by a reset), then
    // delivery must be cancelled for all offsets higher than the cancelled
    // offset as well;

    std::optional<TrackAlias> maybeTrackAlias = std::nullopt;
    if (streamType_ != StreamType::FETCH_HEADER) {
      maybeTrackAlias = trackAlias_;
    }
    while (!pendingDeliveries_.empty() &&
           pendingDeliveries_.back().endOffset >= offset) {
      auto& pendingDelivery = pendingDeliveries_.back();

      if (deliveryCallback_) {
        deliveryCallback_->onDeliveryCancelled(
            maybeTrackAlias,
            pendingDelivery.groupId,
            pendingDelivery.subgroupId,
            pendingDelivery.objectId);
      }

      // Cancel delivery timeout timer when object delivery is cancelled
      if (deliveryTimer_) {
        deliveryTimer_->cancelTimer(pendingDelivery.objectId);
      }

      pendingDeliveries_.pop_back();
    }
    onByteEventCommon(id, offset);
  }

  void setForward(bool forwardIn) {
    forward_ = forwardIn;
  }

  void setDeliveryTimeout(std::optional<std::chrono::milliseconds> timeout) {
    if (timeout.has_value() && timeout->count() > 0) {
      if (deliveryTimer_) {
        XLOG(DBG6)
            << "MoQSession::SubgroupPublisher::setDeliveryTimeout: CALLING deliveryTimer->setDeliveryTimeout"
            << " timeout=" << timeout->count() << "ms";
        deliveryTimer_->setDeliveryTimeout(*timeout);
      } else {
        XLOG(DBG6)
            << "MoQSession::SubgroupPublisher::setDeliveryTimeout: No timer exists, ignoring timeout update"
            << " timeout=" << timeout->count() << "ms";
      }
    }
  }

 private:
  std::shared_ptr<MLogger> logger_;
  std::shared_ptr<DeliveryCallback> deliveryCallback_;
  std::unique_ptr<MoQDeliveryTimer> deliveryTimer_;

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
    // Check group direction based on the publisher's GroupOrder
    auto groupOrder =
        publisher_ ? publisher_->getGroupOrder() : GroupOrder::OldestFirst;
    bool isDescending = (groupOrder == GroupOrder::NewestFirst);
    bool groupMovedWrongDirection = isDescending
        ? (groupID > header_.group && header_.group != 0)
        : (groupID < header_.group);
    if (groupMovedWrongDirection) {
      return false;
    }
    if (groupID != header_.group) {
      // Group changed, reset expected object
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
  std::optional<folly::CancellationCallback> cancelCallback_;
  proxygen::WebTransport::StreamWriteHandle* writeHandle_{nullptr};
  StreamType streamType_;
  TrackAlias trackAlias_{0}; // Store track alias separately from ObjectHeader
  ObjectHeader header_;
  std::optional<uint64_t> currentLengthRemaining_;
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
    std::shared_ptr<DeliveryCallback> deliveryCallback,
    std::optional<std::chrono::milliseconds> deliveryTimeout)
    : publisher_(publisher),
      streamType_(StreamType::FETCH_HEADER),
      header_(
          0,
          0,
          std::numeric_limits<uint64_t>::max(),
          0,
          ObjectStatus::NORMAL) {
  logger_ = std::move(logger);
  deliveryCallback_ = std::move(deliveryCallback);

  // Create delivery timer if timeout is provided
  if (deliveryTimeout.has_value() && deliveryTimeout->count() > 0) {
    auto exec = publisher_->getExecutor();
    if (exec) {
      deliveryTimer_ = std::make_unique<MoQDeliveryTimer>(
          exec, *deliveryTimeout, [this](ResetStreamErrorCode errorCode) {
            if (writeHandle_) {
              XLOG(DBG1) << "Delivery Timeout: resetting sgp=" << this
                         << " streamID=" << writeHandle_->getID()
                         << " with errorCode="
                         << folly::to_underlying(errorCode);
              this->reset(errorCode);
            }
          });
    } else {
      XLOG(ERR) << "MoQSession::StreamPublisherImpl: No executor available. "
                   "Delivery timeout was not set.";
    }
  }

  moqFrameWriter_.initializeVersion(publisher->getVersion());
  (void)moqFrameWriter_.writeFetchHeader(writeBuf_, publisher->requestID());
}

StreamPublisherImpl::StreamPublisherImpl(
    std::shared_ptr<MoQSession::PublisherImpl> publisher,
    proxygen::WebTransport::StreamWriteHandle* writeHandle,
    TrackAlias alias,
    uint64_t groupID,
    uint64_t subgroupID,
    const std::optional<uint8_t>& sgPriority,
    SubgroupIDFormat format,
    bool includeExtensions,
    std::shared_ptr<MLogger> logger,
    std::shared_ptr<DeliveryCallback> deliveryCallback,
    std::optional<std::chrono::milliseconds> deliveryTimeout)
    : StreamPublisherImpl(
          publisher,
          logger,
          deliveryCallback,
          std::move(deliveryTimeout)) {
  CHECK(writeHandle)
      << "For a SUBSCRIBE, you need to pass in a non-null writeHandle";
  // When sgPriority is none, the receiver will use the value from
  // PUBLISHER_PRIORITY, which defaults to 128 if not sent by the publisher when
  // establishing the subscription.
  bool endOfGroup = false;
  streamType_ = getSubgroupStreamType(
      publisher->getVersion(),
      format,
      includeExtensions,
      endOfGroup,
      sgPriority.has_value());
  trackAlias_ = alias;
  setWriteHandle(writeHandle);
  setGroupAndSubgroup(groupID, subgroupID);
  // Set the priority in the header
  header_.priority = sgPriority;
  logger_ = logger;
  if (logger_) {
    logger_->logStreamTypeSet(
        writeHandle->getID(), MOQTStreamType::SUBGROUP_HEADER, Owner::LOCAL);
    logger_->logSubgroupHeaderCreated(
        writeHandle->getID(),
        alias,
        groupID,
        subgroupID,
        publisher->subPriority(),
        format,
        includeExtensions,
        endOfGroup);
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
  XCHECK(writeHandle);
  writeHandle_ = writeHandle;
  if (streamType_ == StreamType::FETCH_HEADER && logger_) {
    logger_->logStreamTypeSet(
        writeHandle_->getID(), MOQTStreamType::FETCH_HEADER, Owner::LOCAL);
    RequestID req = publisher_->requestID();
    logger_->logFetchHeaderCreated(writeHandle_->getID(), req.value);
  }

  cancelCallback_.emplace(writeHandle_->getCancelToken(), [this] {
    if (writeHandle_) {
      auto* ex = writeHandle_->exception();
      XLOG(DBG0) << "Peer requested write termination id="
                 << writeHandle_->getID() << " code=" << (ex ? ex->what() : "")
                 << " sgp=" << this;
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
  bool entireObjectWritten = (!currentLengthRemaining_.has_value());
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
        MoQPublishError(MoQPublishError::TOO_FAR_BEHIND, "Too far behind"));
  }

  if (!writeHandle_) {
    return folly::makeUnexpected(MoQPublishError(
        MoQPublishError::CANCELLED, "Cancelled: writeCurrentObject"));
  }

  auto writeHandle = writeHandle_;
  if (finStream) {
    writeHandle_ = nullptr;
  }

  proxygen::WebTransport::ByteEventCallback* deliveryCallback = nullptr;
  if (!writeBuf_.empty() || finStream) {
    deliveryCallback = this;

    if ((deliveryCallback_ || deliveryTimer_) && endObject) {
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

  // Start delivery timeout timer when object begins being sent to stream
  if (deliveryTimer_) {
    deliveryTimer_->startTimer(objectID, publisher_->getTransportInfo().srtt);
  }

  return writeCurrentObject(
      objectID, length, std::move(payload), extensions, finStream);
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

  // Start delivery timeout timer when object begins being sent to stream
  if (deliveryTimer_) {
    deliveryTimer_->startTimer(objectID, publisher_->getTransportInfo().srtt);
  }

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
        MoQPublishError(MoQPublishError::CANCELLED, "Cancelled objectPayload"));
  }
  auto validateObjectPublishRes =
      validateObjectPublishAndUpdateState(payload.get(), finStream);
  if (!validateObjectPublishRes) {
    return validateObjectPublishRes;
  }
  writeBuf_.append(std::move(payload));
  bool entireObjectWritten = (!currentLengthRemaining_.has_value());
  auto writeRes = writeToStream(finStream, entireObjectWritten);
  if (writeRes.hasValue()) {
    return validateObjectPublishRes.value();
  } else {
    return folly::makeUnexpected(writeRes.error());
  }
}

folly::Expected<folly::Unit, MoQPublishError> StreamPublisherImpl::endOfGroup(
    uint64_t endOfGroupObjectId) {
  if (!forward_) {
    reset(ResetStreamErrorCode::INTERNAL_ERROR);
    return folly::makeUnexpected(
        MoQPublishError(MoQPublishError::API_ERROR, "shouldForward is false"));
  }
  return publishStatus(
      endOfGroupObjectId,
      ObjectStatus::END_OF_GROUP,
      noExtensions(),
      /*finStream=*/true);
}

folly::Expected<folly::Unit, MoQPublishError>
StreamPublisherImpl::endOfTrackAndGroup(uint64_t endOfTrackObjectId) {
  if (!forward_) {
    reset(ResetStreamErrorCode::INTERNAL_ERROR);
    return folly::makeUnexpected(
        MoQPublishError(MoQPublishError::API_ERROR, "shouldForward is false"));
  }
  return publishStatus(
      endOfTrackObjectId,
      ObjectStatus::END_OF_TRACK,
      noExtensions(),
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
  header_.length = std::nullopt;
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
  if (streamComplete_) {
    XLOG(DBG4) << "Reset after streamComplete_ sgp=" << this;
    return;
  }
  if (!writeBuf_.empty()) {
    // TODO: stream header is pending, reliable reset?
    XLOG(WARN) << "Stream header pending on subgroup=" << header_;
  }
  // Cancel all delivery timeout timers for this stream since it's being reset
  if (deliveryTimer_) {
    deliveryTimer_->cancelAllTimers();
  }

  if (auto* wh = std::exchange(writeHandle_, nullptr)) {
    wh->resetStream(uint32_t(error));
  } else {
    // Can happen on STOP_SENDING, delivery timeout, multiple resets,
    // or prior to first fetch write
    XLOG(DBG4) << "reset with no write handle: sgp=" << this;
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
    // TODO: return CANCELLED if after stop sending, since that's not an API
    // error
    return folly::makeUnexpected(MoQPublishError(
        MoQPublishError::API_ERROR, "Write after stream complete or reset"));
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
  auto pri = getStreamPriority(
      0, 0, publisher_->subPriority(), 0, GroupOrder::OldestFirst);
  stream.value()->setPriority(
      quic::HTTPPriorityQueue::Priority(pri.urgency, false, pri.order));
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
      std::optional<TrackAlias> trackAlias,
      Priority subPriority,
      GroupOrder groupOrder,
      uint64_t version,
      uint64_t bytesBufferedThreshold,
      bool forward,
      std::optional<std::chrono::milliseconds> deliveryTimeout = std::nullopt)
      : PublisherImpl(
            session,
            std::move(fullTrackName),
            requestID,
            subPriority,
            groupOrder,
            version,
            bytesBufferedThreshold),
        trackAlias_(std::move(trackAlias)),
        forward_(forward) {
    // Set callback for delivery timeout changes
    deliveryTimeoutManager_.setOnChangeCallback(
        [this](std::optional<std::chrono::milliseconds> newTimeout) {
          onDeliveryTimeoutChanged(std::move(newTimeout));
        });

    // Initialize with downstream timeout by default
    if (deliveryTimeout.has_value()) {
      deliveryTimeoutManager_.setDownstreamTimeout(*deliveryTimeout);
    }
  }

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
    if (pendingPublishDone_) {
      // If publishDone is called before publishHandler_->subscribe() returns,
      // catch the DONE here and defer it until after we send subscribe OK.
      auto pubDone = std::move(*pendingPublishDone_);
      pubDone.streamCount = streamCount_;
      pendingPublishDone_.reset();
      XCHECK(session_);
      subscriptionHandle_.reset();
      session_->sendPublishDone(pubDone);
    }
  }

  // PublisherImpl overrides
  void onStreamCreated() override {
    streamCount_++;
  }

  void onStreamComplete(const ObjectHeader& finalHeader) override;

  void onTooManyBytesBuffered() override;

  void onRequestUpdate(RequestUpdate requestUpdate) {
    auto it = session_->pubTracks_.find(requestID_);
    if (!subscriptionHandle_ || it == session_->pubTracks_.end()) {
      XLOG(ERR) << "Received RequestUpdate before sending SUBSCRIBE_OK id="
                << requestID_ << " trackPub=" << this;

      // Only send error response for v15+
      if (getDraftMajorVersion(*session_->getNegotiatedVersion()) >= 15) {
        session_->requestUpdateError(
            SubscribeUpdateError{
                requestUpdate.requestID,
                static_cast<RequestErrorCode>(
                    folly::to_underlying(SubscribeErrorCode::INTERNAL_ERROR)),
                "No subscription handle or track publisher"},
            requestID_);
      }
      return;
    }

    auto trackPubImpl =
        std::static_pointer_cast<TrackPublisherImpl>(shared_from_this());

    // Handle asynchronously with shared ownership to prevent use-after-free
    // if session closes before completion
    co_withExecutor(
        session_->getExecutor(),
        folly::coro::co_invoke(
            [trackPubImpl, update = std::move(requestUpdate)]() mutable
                -> folly::coro::Task<void> {
              co_await trackPubImpl->handleRequestUpdate(std::move(update));
            }))
        .start();
  }

  folly::coro::Task<void> handleRequestUpdate(RequestUpdate requestUpdate) {
    folly::RequestContextScopeGuard guard;
    session_->setRequestSession();

    auto updateRequestID = requestUpdate.requestID;
    auto existingRequestID = requestID_;

    // Update delivery timeout if present
    auto timeoutValue = MoQSession::getDeliveryTimeoutIfPresent(
        requestUpdate.params, *session_->getNegotiatedVersion());
    if (timeoutValue.has_value() && *timeoutValue > 0) {
      XLOG(DBG6)
          << "MoQSession::TrackPublisherImpl::handleRequestUpdate: SETTING downstream timeout"
          << " timeout=" << *timeoutValue << "ms"
          << " requestID=" << existingRequestID;
      deliveryTimeoutManager_.setDownstreamTimeout(
          std::chrono::milliseconds(*timeoutValue));
    } else {
      XLOG(DBG6)
          << "MoQSession::TrackPublisherImpl::handleRequestUpdate: No delivery timeout in params or timeout=0"
          << " requestID=" << existingRequestID;
    }

    // Only update forward state if the parameter was explicitly provided
    // Otherwise, preserve existing forward state (per draft 15+)
    if (requestUpdate.forward.has_value()) {
      setForward(*requestUpdate.forward);
    }

    // Call application's async subscribeUpdate handler with cancellation
    auto updateResult = co_await co_awaitTry(co_withCancellation(
        session_->cancellationSource_.getToken(),
        subscriptionHandle_->subscribeUpdate(std::move(requestUpdate))));

    // Only send responses for v15+
    if (getDraftMajorVersion(*session_->getNegotiatedVersion()) >= 15) {
      if (updateResult.hasException()) {
        XLOG(ERR) << "Exception in requestUpdate ex="
                  << updateResult.exception().what().toStdString();
        session_->requestUpdateError(
            SubscribeUpdateError{
                updateRequestID,
                RequestErrorCode::INTERNAL_ERROR,
                updateResult.exception().what().toStdString()},
            existingRequestID);
        co_return;
      }

      if (updateResult->hasError()) {
        XLOG(ERR) << "requestUpdate failed: "
                  << updateResult->error().reasonPhrase
                  << " requestID=" << existingRequestID;
        auto updateErr = std::move(updateResult->error());
        updateErr.requestID = updateRequestID; // In case app got it wrong
        session_->requestUpdateError(updateErr, existingRequestID);
      } else {
        // send REQUEST_OK with LARGEST_OBJECT if available
        // TODO: Do we relay the params we got from the app?
        std::vector<Parameter> requestSpecificParams;
        if (subscriptionHandle_->subscribeOk().largest) {
          requestSpecificParams.emplace_back(
              folly::to_underlying(TrackRequestParamKey::LARGEST_OBJECT),
              subscriptionHandle_->subscribeOk().largest.value());
        }
        RequestOk requestOk{
            .requestID = updateRequestID,
            .requestSpecificParams = std::move(requestSpecificParams)};
        session_->requestUpdateOk(requestOk);
      }
    }
  }

  void onPublishOk(const PublishOk& publishOk) {
    auto timeoutValue = MoQSession::getDeliveryTimeoutIfPresent(
        publishOk.params, *session_->getNegotiatedVersion());
    if (timeoutValue.has_value() && *timeoutValue > 0) {
      deliveryTimeoutManager_.setDownstreamTimeout(
          std::chrono::milliseconds(*timeoutValue));
    }
  }

  void subscribeOkSent(const SubscribeOk& subscribeOk) {
    if (!session_) {
      return;
    }
    setTrackAlias(subscribeOk.trackAlias);
    setGroupOrder(subscribeOk.groupOrder);
    auto timeout = getPublisherDeliveryTimeout(subscribeOk);
    if (timeout.has_value() && timeout->count() > 0) {
      deliveryTimeoutManager_.setUpstreamTimeout(*timeout);
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
    subscriptionHandle_.reset();
  }

  void terminatePublish(PublishDone pubDone, ResetStreamErrorCode code)
      override {
    resetAllSubgroups(code);
    auto session = std::exchange(session_, nullptr);
    if (!subscriptionHandle_) {
      session->subscribeError(
          {pubDone.requestID, SubscribeErrorCode::INTERNAL_ERROR, "terminate"});
    } else {
      subscriptionHandle_.reset();
      session->sendPublishDone(pubDone);
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

  folly::Expected<folly::Unit, MoQPublishError> datagram(
      const ObjectHeader& header,
      Payload payload) override;

  folly::Expected<folly::Unit, MoQPublishError> publishDone(
      PublishDone pubDone) override;

  void setDeliveryCallback(std::shared_ptr<DeliveryCallback> callback) override;

  void setLogger(std::shared_ptr<MLogger> logger) {
    logger_ = logger;
  }

 private:
  void onDeliveryTimeoutChanged(
      std::optional<std::chrono::milliseconds> newTimeout);

  std::chrono::microseconds getCurrentRtt() const {
    return session_->getTransportInfo().srtt;
  }

  std::shared_ptr<MLogger> logger_ = nullptr;
  std::shared_ptr<Subscriber::SubscriptionHandle> subscriptionHandle_;
  std::optional<TrackAlias> trackAlias_;
  std::optional<PublishDone> pendingPublishDone_;
  folly::F14FastMap<
      std::pair<uint64_t, uint64_t>,
      std::shared_ptr<StreamPublisherImpl>>
      subgroups_;
  uint64_t streamCount_{0};
  enum class State { OPEN, DONE };
  State state_{State::OPEN};
  bool forward_;
  std::shared_ptr<DeliveryCallback> deliveryCallback_;
  MoQDeliveryTimeoutManager deliveryTimeoutManager_;
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

  void terminatePublish(PublishDone, ResetStreamErrorCode error) override {
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
    XLOG(ERR) << "Trying to publish after publishDone";
    return folly::makeUnexpected(MoQPublishError(
        MoQPublishError::API_ERROR, "Publish after publishDone"));
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
  auto pri = getStreamPriority(
      groupID, subgroupID, subPriority_, pubPriority, groupOrder_);
  stream.value()->setPriority(
      quic::HTTPPriorityQueue::Priority(pri.urgency, false, pri.order));

  // Get effective timeout to pass to StreamPublisherImpl
  auto effectiveTimeout = deliveryTimeoutManager_.getEffectiveTimeout();

  // Elide priority if it matches publisher priority from control plane
  auto elidedPriority = elidePriorityForWrite(pubPriority, publisherPriority_);

  auto subgroupPublisher = std::make_shared<StreamPublisherImpl>(
      shared_from_this(),
      *stream,
      *trackAlias_,
      groupID,
      subgroupID,
      elidedPriority,
      format,
      includeExtensions,
      logger_,
      deliveryCallback_,
      effectiveTimeout);
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
        MoQPublishError::API_ERROR, "awaitStreamCredit after publishDone"));
  }
  return wt->awaitUniStreamCredit();
}

void MoQSession::TrackPublisherImpl::onStreamComplete(
    const ObjectHeader& finalHeader) {
  if (session_) {
    session_->onSubscriptionStreamClosed();
  }
  // The reason we need the keepalive is that when we call
  // subgroups_.erase(), we might end up erasing the last
  // SubgroupPublisherImpl that has a shared_ptr reference to this
  // TrackPublisherImpl, causing the destruction of the map while erasing
  // from it.
  auto keepalive = shared_from_this();
  subgroups_.erase({finalHeader.group, finalHeader.subgroup});
}

void MoQSession::TrackPublisherImpl::onTooManyBytesBuffered() {
  // Note: There is one case in which this reset can be problematic. If some
  // streams have been created, but have been reset before the subgroup
  // header is sent out, then there can be a stream count discrepancy. We
  // could, some time in the future, change this to a reliable reset so that
  // we're ensured that the stream counts are consistent. We can also add in
  // a timeout for the stream count discrepancy so that the peer doesn't
  // hang indefinitely waiting for the stream counts to equalize.
  terminatePublish(
      PublishDone(
          {requestID_,
           PublishDoneStatusCode::TOO_FAR_BEHIND,
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
  // Validate that application provided a priority value
  auto priorityValidation = validatePrioritySet(objHeader.priority);
  if (priorityValidation.hasError()) {
    return folly::makeUnexpected(std::move(priorityValidation.error()));
  }
  XCHECK(objHeader.status == ObjectStatus::NORMAL || !payload);
  Extensions extensions = objHeader.extensions;
  auto subgroup = beginSubgroup(
      objHeader.group,
      objHeader.subgroup,
      *objHeader.priority,
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
    case ObjectStatus::GROUP_NOT_EXIST: {
      auto& subgroupPublisherImpl =
          static_cast<StreamPublisherImpl&>(*subgroup.value());
      return subgroupPublisherImpl.publishStatus(
          objHeader.id,
          objHeader.status,
          noExtensions(),
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
MoQSession::TrackPublisherImpl::datagram(
    const ObjectHeader& header,
    Payload payload) {
  if (!trackAlias_) {
    return folly::makeUnexpected(MoQPublishError(
        MoQPublishError::API_ERROR, "Must set track alias first"));
  }
  // Validate that application provided a priority value
  auto priorityValidation = validatePrioritySet(header.priority);
  if (priorityValidation.hasError()) {
    return folly::makeUnexpected(std::move(priorityValidation.error()));
  }
  if (!forward_) {
    return folly::makeUnexpected(MoQPublishError(
        MoQPublishError::API_ERROR,
        "Cannot send datagrams for subscriptions with forward flag set to false"));
  }
  auto wt = getWebTransport();
  if (!wt || state_ != State::OPEN) {
    XLOG(ERR) << "Trying to publish after publishDone";
    return folly::makeUnexpected(MoQPublishError(
        MoQPublishError::API_ERROR, "Publish after publishDone"));
  }

  if (logger_) {
    logger_->logObjectDatagramCreated(*trackAlias_, header, payload);
  }

  // Elide priority if it matches publisher priority
  auto elidedPriority =
      elidePriorityForWrite(*header.priority, publisherPriority_);

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
          elidedPriority,
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
MoQSession::TrackPublisherImpl::publishDone(PublishDone pubDone) {
  if (state_ != State::OPEN || !session_) {
    return folly::makeUnexpected(MoQPublishError(
        MoQPublishError::API_ERROR, "publishDone twice or after close"));
  }
  state_ = State::DONE;
  pubDone.requestID = requestID_;
  if (!subscriptionHandle_) {
    // publishDone called from inside the subscribe handler,
    // before subscribeOk.
    pendingPublishDone_ = std::move(pubDone);
    return folly::unit;
  }
  pubDone.streamCount = streamCount_;
  subscriptionHandle_.reset();
  session_->sendPublishDone(pubDone);
  return folly::unit;
}

void MoQSession::TrackPublisherImpl::setDeliveryCallback(
    std::shared_ptr<DeliveryCallback> callback) {
  deliveryCallback_ = std::move(callback);
}

void MoQSession::TrackPublisherImpl::onDeliveryTimeoutChanged(
    std::optional<std::chrono::milliseconds> newTimeout) {
  XLOG(DBG6)
      << "MoQSession::TrackPublisherImpl::onDeliveryTimeoutChanged: CALLBACK INVOKED"
      << " newTimeout="
      << (newTimeout.has_value() ? std::to_string(newTimeout->count()) + "ms"
                                 : "none")
      << " requestID=" << requestID_ << " subgroups.size=" << subgroups_.size();

  if (!newTimeout.has_value()) {
    return;
  }

  // Propagate to all existing subgroups
  for (const auto& [_, subgroupPublisher] : subgroups_) {
    subgroupPublisher->setDeliveryTimeout(*newTimeout);
  }
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

  bool isCancelled() const {
    return cancelSource_.isCancellationRequested();
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

  // Timer callback for stream count timeout
  class StreamCountTimeoutCallback : public quic::QuicTimerCallback {
   public:
    explicit StreamCountTimeoutCallback(SubscribeTrackReceiveState& state)
        : state_(state) {}

    void timeoutExpired() noexcept override {
      state_.streamCountTimeoutExpired();
    }

   private:
    SubscribeTrackReceiveState& state_;
  };

  SubscribeTrackReceiveState(
      FullTrackName fullTrackName,
      RequestID requestID,
      std::shared_ptr<TrackConsumer> callback,
      MoQSession* session,
      TrackAlias alias,
      std::shared_ptr<MLogger> logger = nullptr,
      bool publish = false)
      : TrackReceiveStateBase(std::move(fullTrackName), requestID),
        callback_(std::move(callback)),
        publish_(publish),
        session_(session),
        alias_(alias) {
    logger_ = std::move(logger);
  }

  ~SubscribeTrackReceiveState() {
    cancelStreamCountTimeout();
  }

  bool isPublish() {
    return publish_;
  }

  folly::coro::Future<SubscribeResult> subscribeFuture() {
    auto contract = folly::coro::makePromiseContract<SubscribeResult>();
    subscribePromise_ = std::move(contract.first);
    return std::move(contract.second);
  }

  [[nodiscard]] const FullTrackName& fullTrackName() const {
    return fullTrackName_;
  }

  std::shared_ptr<TrackConsumer> getSubscribeCallback() const {
    return cancelSource_.isCancellationRequested() ? nullptr : callback_;
  }

  void cancel() {
    XLOG(DBG1) << __func__ << " alias=" << alias_
               << " requestID=" << requestID_;
    callback_.reset();
    cancelSource_.requestCancellation();
    // Cancel stream count timer if waiting for pending streams
    cancelStreamCountTimeout();
    // Clear pending subscribe done since we're unsubscribing
    pendingPublishDone_.reset();
  }

  void processSubscribeOK(SubscribeOk subscribeOK) {
    alias_ = subscribeOK.trackAlias;
    subscribePromise_.setValue(std::move(subscribeOK));
  }

  void subscribeError(SubscribeError subErr) {
    XLOG(DBG1) << __func__ << " trackReceiveState=" << this;
    if (!publish_ && !subscribePromise_.isFulfilled()) {
      subErr.requestID = requestID_;
      subscribePromise_.setValue(folly::makeUnexpected(std::move(subErr)));
    } else if (callback_) {
      // Clear pending to avoid false duplicate detection - we're already
      // cleaning up this subscription
      pendingPublishDone_.reset();
      processPublishDone(
          {requestID_,
           PublishDoneStatusCode::SESSION_CLOSED,
           0, // forces immediately invoking the callback
           "closed locally"});
    } // else already unsubscribed or delivered done
  }

  void onSubgroup() {
    if (logger_) {
      logger_->logStreamTypeSet(
          currentStreamId_, MOQTStreamType::SUBGROUP_HEADER, Owner::REMOTE);
    }
    streamCount_++;
    if (pendingPublishDone_ &&
        streamCount_ >= pendingPublishDone_->streamCount) {
      deliverPublishDoneAndRemove();
    }
  }

  void processPublishDone(PublishDone pubDone) {
    XLOG(DBG1) << __func__ << " trackReceiveState=" << this;
    if (!callback_) {
      XLOG(DBG0)
          << "processPublishDone: No callback (unsubscribed); removing state alias="
          << alias_ << " requestID=" << requestID_;
      // Unsubscribe raced with publishDone - just remove state
      // TODO: I think alias_ will be wrong in SUBSCRIBE -> PUBLISH_DONE
      // But that should be a different error.
      session_->removeSubscriptionState(alias_, requestID_);
      return;
    }
    if (pendingPublishDone_) {
      XLOG(ERR) << "Duplicate PUBLISH_DONE";
      session_->close(SessionCloseErrorCode::PROTOCOL_VIOLATION);
      return;
    }

    pendingPublishDone_ = std::move(pubDone);
    if (pendingPublishDone_->streamCount > streamCount_) {
      // Still waiting for streams - schedule timeout
      XLOG(DBG0) << "Waiting for streams in flight, have=" << streamCount_
                 << " need=" << pendingPublishDone_->streamCount
                 << " trackReceiveState=" << this;
      scheduleStreamCountTimeout();
    } else {
      deliverPublishDoneAndRemove();
    }
  }

  void setCurrentStreamId(uint64_t id) {
    currentStreamId_ = id;
  }

  uint8_t getPublisherPriority() const {
    // Fallback to kDefaultPriority if not set
    return publisherPriority_.value_or(kDefaultPriority);
  }

  void setPublisherPriority(uint8_t priority) {
    publisherPriority_ = priority;
  }

 private:
  void deliverPublishDoneAndRemove() {
    cancelStreamCountTimeout();
    if (pendingPublishDone_) {
      if (callback_) {
        XLOG(DBG0)
            << "deliverPublishDoneAndRemove: Delivering PUBLISH_DONE to app; statusCode="
            << folly::to_underlying(pendingPublishDone_->statusCode)
            << " alias=" << alias_ << " requestID=" << requestID_;
        auto token = cancelSource_.getToken();
        auto cb = std::exchange(callback_, nullptr);
        cb->publishDone(std::move(*pendingPublishDone_));
        if (token.isCancellationRequested()) {
          return;
        }
      }
      pendingPublishDone_.reset();
    }
    XLOG(DBG4)
        << "deliverPublishDoneAndRemove: Removing subscription state alias="
        << alias_ << " requestID=" << requestID_;
    session_->removeSubscriptionState(alias_, requestID_);
  }

  void scheduleStreamCountTimeout() {
    if (!session_ || !session_->getExecutor()) {
      XLOG(ERR) << "Cannot schedule timeout: session or executor unavailable";
      return;
    }

    streamCountTimeout_ = std::make_unique<StreamCountTimeoutCallback>(*this);
    auto timeout = session_->getMoqSettings().publishDoneStreamCountTimeout;
    XLOG(DBG4) << "scheduleStreamCountTimeout: Scheduling timer duration="
               << timeout.count() << "ms alias=" << alias_
               << " requestID=" << requestID_;
    auto moqExec = session_->getExecutor();
    moqExec->scheduleTimeout(streamCountTimeout_.get(), timeout);
  }

  void cancelStreamCountTimeout() {
    if (streamCountTimeout_) {
      streamCountTimeout_->cancelTimerCallback();
      streamCountTimeout_.reset();
    }
  }

  void streamCountTimeoutExpired() {
    XCHECK(pendingPublishDone_) << "Why is there no pendingPublishDone_";
    XLOG(DBG0) << "Delivering PUBLISH_DONE after timeout, have=" << streamCount_
               << " expected=" << pendingPublishDone_->streamCount;
    deliverPublishDoneAndRemove();
  }

  std::shared_ptr<MLogger> logger_ = nullptr;
  std::shared_ptr<TrackConsumer> callback_;
  folly::coro::Promise<SubscribeResult> subscribePromise_;
  std::optional<PublishDone> pendingPublishDone_;
  uint64_t streamCount_{0};
  uint64_t currentStreamId_{0};

  // Indicates whether receiveState is for Publish
  bool publish_;

  // Publisher priority from SUBSCRIBE_OK or PUBLISH parameter
  // Stored as optional; if not set from SUBSCRIBE_OK, defaults to
  // kDefaultPriority
  std::optional<uint8_t> publisherPriority_;

  // Raw pointer to session (safe because SubscribeTrackReceiveState is owned
  // by session in subTracks_)
  MoQSession* session_;
  TrackAlias alias_;
  std::unique_ptr<StreamCountTimeoutCallback> streamCountTimeout_;
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
    logger_ = std::move(logger);
  }

  folly::coro::Future<FetchResult> fetchFuture() {
    auto contract = folly::coro::makePromiseContract<FetchResult>();
    promise_ = std::move(contract.first);
    return std::move(contract.second);
  }

  std::shared_ptr<FetchConsumer> getFetchCallback() const {
    return callback_;
  }

  void resetFetchCallback(MoQSession* session) {
    callback_.reset();
    if (fetchOkAndAllDataReceived()) {
      session->fetches_.erase(requestID_);
      session->checkForCloseOnDrain();
    }
  }

  void cancel(MoQSession* session) {
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

folly::Expected<MoQSession::PendingRequestState::Type, folly::Unit>
MoQSession::PendingRequestState::setError(
    RequestError error,
    FrameType frameType) {
  switch (frameType) {
    case FrameType::SUBSCRIBE_ERROR: {
      auto ptr = tryGetSubscribeTrack();
      if (!ptr || !*ptr || (*ptr)->isPublish()) {
        return folly::makeUnexpected(folly::unit);
      }
      (*ptr)->subscribeError(std::move(error));
      return type_;
    }
    case FrameType::PUBLISH_ERROR: {
      auto promise = tryGetPublish();
      if (!promise) {
        return folly::makeUnexpected(folly::unit);
      }
      promise->setValue(folly::makeUnexpected(std::move(error)));
      return type_;
    }
    case FrameType::TRACK_STATUS: {
      storage_.trackStatus_.setValue(
          folly::makeUnexpected(TrackStatusError(
              {error.requestID, error.errorCode, error.reasonPhrase})));
      return type_;
    }
    case FrameType::FETCH_ERROR: {
      auto fetchPtr = tryGetFetch();
      if (!fetchPtr) {
        return folly::makeUnexpected(folly::unit);
      }
      (*fetchPtr)->fetchError(std::move(error));
      return type_;
    }
    case FrameType::SUBSCRIBE_UPDATE: {
      if (type_ == Type::SUBSCRIBE_UPDATE) {
        storage_.subscribeUpdate_.setValue(
            folly::makeUnexpected(
                SubscribeUpdateError{
                    error.requestID, error.errorCode, error.reasonPhrase}));
        return type_;
      }
      return folly::makeUnexpected(folly::unit);
    }
    case FrameType::PUBLISH_NAMESPACE_ERROR:
    case FrameType::SUBSCRIBE_NAMESPACE_ERROR: {
      // These types are handled by MoQRelaySession subclass
      return folly::makeUnexpected(folly::unit);
    }
    default:
      // fall through
      break;
  }
  // Should not reach here
  return folly::makeUnexpected(folly::unit);
}

using folly::coro::co_awaitTry;
using folly::coro::co_error;

// Constructors
MoQSession::MoQSession(
    folly::MaybeManagedPtr<proxygen::WebTransport> wt,
    MoQExecutor::KeepAlive exec)
    : dir_(MoQControlCodec::Direction::CLIENT),
      wt_(std::move(wt)),
      exec_(std::move(exec)),
      nextRequestID_(0),
      nextExpectedPeerRequestID_(1),
      controlCodec_(dir_, this) {}

MoQSession::MoQSession(
    folly::MaybeManagedPtr<proxygen::WebTransport> wt,
    ServerSetupCallback& serverSetupCallback,
    MoQExecutor::KeepAlive exec)
    : dir_(MoQControlCodec::Direction::SERVER),
      wt_(std::move(wt)),
      exec_(std::move(exec)),
      nextRequestID_(1),
      nextExpectedPeerRequestID_(0),
      serverSetupCallback_(&serverSetupCallback),
      controlCodec_(dir_, this) {}

MoQSession::~MoQSession() {
  cleanup();
  if (logger_) {
    logger_->outputLogs();
  }
  XLOG(DBG1) << __func__ << " sess=" << this;
}

void MoQSession::cleanup() {
  // TODO: Are these loops safe since they may (should?) delete elements
  while (!pubTracks_.empty()) {
    auto pubTrack = pubTracks_.begin();
    pubTrack->second->terminatePublish(
        PublishDone(
            {pubTrack->first,
             PublishDoneStatusCode::SESSION_CLOSED,
             0,
             "Session Closed"}),
        ResetStreamErrorCode::SESSION_CLOSED);
  }
  for (auto it = subTracks_.begin(); it != subTracks_.end();) {
    auto sub = it->second;
    ++it;
    // For pending subscribe, delivers subscribeError
    // For established subscriptions or pending publish, delivers publishDone
    // which can erase from subTracks_.
    sub->subscribeError({/*TrackReceiveState fills in subId*/ 0,
                         SubscribeErrorCode::INTERNAL_ERROR,
                         "session closed"});
  }
  subTracks_.clear();
  // We parse a publishDone after cleanup
  reqIdToTrackAlias_.clear();
  // TODO: there needs to be a way to queue an error in TrackReceiveState,
  // both from here, when close races the FETCH stream, and from readLoop
  // where we get a reset.
  fetches_.clear();
  // Handle all pending requests in consolidated map
  for (auto& [reqID, pendingState] : pendingRequests_) {
    pendingState->setError(
        RequestError{reqID, RequestErrorCode::INTERNAL_ERROR, "Session closed"},
        pendingState->getErrorFrameType());
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
    controlStream.writeHandle->setPriority(
        quic::HTTPPriorityQueue::Priority(0, false, 0));

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

std::shared_ptr<MLogger> MoQSession::getLogger() const {
  return logger_;
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
  if (closed_) {
    return;
  }
  closed_ = true;
  if (closeCallback_) {
    XLOG(DBG1) << "Calling close callback";
    closeCallback_->onMoQSessionClosed();
  }
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
    setup.params.insertParam(SetupParameter(
        {folly::to_underlying(SetupKey::MOQT_IMPLEMENTATION),
         getMoQTImplementationString()}));
  }

  uint64_t setupSerializationVersion = kVersionDraft14;
  if (negotiatedVersion_) {
    setupSerializationVersion = *negotiatedVersion_;
  }

  // This sets the receive token cache size, but it's necesarily empty
  controlCodec_.setMaxAuthTokenCacheSize(
      getMaxAuthTokenCacheSizeIfPresent(setup.params));
  // Optimistically registers params without knowing peer's capabilities
  aliasifyAuthTokens(setup.params, setupSerializationVersion);
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
      mergeToken,
      folly::coro::timeout(std::move(setupFuture), moqSettings_.setupTimeout)));
  if (mergeToken.isCancellationRequested()) {
    co_yield folly::coro::co_stopped_may_throw;
  }
  if (serverSetup.hasException()) {
    close(SessionCloseErrorCode::VERSION_NEGOTIATION_FAILED);
    XLOG(ERR) << "Setup Failed: "
              << folly::exceptionStr(serverSetup.exception());
    co_yield folly::coro::co_error(serverSetup.exception());
  }
  // Only validate version selection in non-alpn mode
  if (!negotiatedVersion_) {
    if (std::find(
            setup.supportedVersions.begin(),
            setup.supportedVersions.end(),
            serverSetup->selectedVersion) == setup.supportedVersions.end()) {
      close(SessionCloseErrorCode::VERSION_NEGOTIATION_FAILED);
      XLOG(ERR) << "Server chose a version that the client doesn't support";
      co_yield folly::coro::co_error(serverSetup.exception());
    }
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

  // In LEGACY mode: initialize version from SERVER_SETUP
  if (!negotiatedVersion_) {
    initializeNegotiatedVersion(serverSetup.selectedVersion);
  }

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

  if (!getNegotiatedVersion()) {
    // Version negotiation has not happened at alpn
    // Negotiate from setup message

    auto majorVersion = getDraftMajorVersion(serverSetup->selectedVersion);

    if (majorVersion >= 15) {
      // Version >= 15 should always be negotiated using alpn
      XLOG(ERR) << "Selected version " << serverSetup->selectedVersion
                << " (major=" << majorVersion
                << ") can only be negotiated via alpn. sess=" << this;
      close(SessionCloseErrorCode::VERSION_NEGOTIATION_FAILED);
      return;
    }

    initializeNegotiatedVersion(serverSetup->selectedVersion);
  }

  // Validate authority with the negotiated version
  auto authorityValidation = serverSetupCallback_->validateAuthority(
      clientSetup, *getNegotiatedVersion(), shared_from_this());
  if (!authorityValidation.hasValue()) {
    SessionCloseErrorCode errorCode = authorityValidation.error();
    XLOG(ERR) << "Authority validation failed sess=" << this
              << " errorCode=" << static_cast<uint64_t>(errorCode);
    close(errorCode);
    return;
  }

  if (getDraftMajorVersion(*getNegotiatedVersion()) >= 14) {
    serverSetup->params.insertParam(SetupParameter(
        {folly::to_underlying(SetupKey::MOQT_IMPLEMENTATION),
         getMoQTImplementationString()}));
  }

  XLOG(DBG1) << "Negotiated Version=" << *getNegotiatedVersion();
  auto maxRequestID = getMaxRequestIDIfPresent(serverSetup->params);

  // This sets the receive cache size and may evict received tokens
  controlCodec_.setMaxAuthTokenCacheSize(
      getMaxAuthTokenCacheSizeIfPresent(serverSetup->params));
  aliasifyAuthTokens(serverSetup->params);
  auto res =
      writeServerSetup(controlWriteBuf_, *serverSetup, *getNegotiatedVersion());
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

class MoQNamespacePublishHandle : public Publisher::NamespacePublishHandle {
 public:
  MoQNamespacePublishHandle(
      proxygen::WebTransport::StreamWriteHandle* writeHandle,
      uint64_t negotiatedVersion)
      : writeHandle_(writeHandle) {
    moqFrameWriter_.initializeVersion(negotiatedVersion);
  }

  void namespaceMsg(const TrackNamespace& trackNamespaceSuffix) override {
    Namespace ns;
    ns.trackNamespaceSuffix = trackNamespaceSuffix;
    auto writeResult = moqFrameWriter_.writeNamespace(writeBuf_, ns);
    if (!writeResult) {
      XLOG(ERR) << "writeNamespace failed";
      return;
    }
    auto res = writeHandle_->writeStreamData(writeBuf_.move(), false, nullptr);
    if (!res) {
      XLOG(ERR) << "writeStreamData for NAMESPACE failed error="
                << uint64_t(res.error());
    }
  }

  void namespaceDoneMsg() override {
    NamespaceDone namespaceDone;
    auto writeResult =
        moqFrameWriter_.writeNamespaceDone(writeBuf_, namespaceDone);
    if (!writeResult) {
      XLOG(ERR) << "writeNamespaceDone failed";
      return;
    }
    auto res = writeHandle_->writeStreamData(writeBuf_.move(), true, nullptr);
    if (!res) {
      XLOG(ERR) << "writeStreamData for NAMESPACE_DONE failed error="
                << uint64_t(res.error());
    }
  }

 private:
  proxygen::WebTransport::StreamWriteHandle* writeHandle_;
  MoQFrameWriter moqFrameWriter_;
  folly::IOBufQueue writeBuf_{folly::IOBufQueue::cacheChainLength()};
};

folly::coro::Task<void> MoQSession::controlReadLoop(
    proxygen::WebTransport::StreamReadHandle* readHandle,
    std::unique_ptr<folly::IOBuf> initialData) {
  XLOG(DBG1) << __func__ << " sess=" << this;
  auto g = folly::makeGuard([func = __func__, this] {
    XLOG(DBG1) << "exit " << func << " sess=" << this;
  });
  co_await folly::coro::co_safe_point;
  auto streamId = readHandle->getID();
  controlCodec_.setStreamId(streamId);

  // Process any pre-buffered data first
  if (initialData) {
    try {
      auto guard = shared_from_this();
      controlCodec_.onIngress(std::move(initialData), false);
    } catch (const std::exception& ex) {
      XLOG(FATAL) << "exception thrown from onIngress ex="
                  << folly::exceptionStr(ex);
    }
  }

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
    if (!token.isCancellationRequested() &&
        (streamData->data || streamData->fin)) {
      try {
        auto guard = shared_from_this();
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
  using OnSubgroupResult =
      std::shared_ptr<MoQSession::SubscribeTrackReceiveState>;

  using OnSubgroupFunc = std::function<OnSubgroupResult(
      TrackAlias alias,
      uint64_t group,
      uint64_t subgroup,
      std::optional<uint8_t> priority,
      const SubgroupOptions& options)>;

  using OnFetchFunc =
      std::function<std::shared_ptr<MoQSession::FetchTrackReceiveState>(
          RequestID requestID)>;

  ObjectStreamCallback(
      MoQSession* session,
      OnSubgroupFunc onSubgroupFunc,
      OnFetchFunc onFetchFunc)
      : session_(session),
        onSubgroupFunc_(std::move(onSubgroupFunc)),
        onFetchFunc_(std::move(onFetchFunc)) {}

  void setCurrentStreamId(uint64_t id) {
    currentStreamId_ = id;
  }

  void setSubscribeTrackReceiveState(
      std::shared_ptr<MoQSession::SubscribeTrackReceiveState> state) {
    subscribeState_ = std::move(state);
  }

  MoQCodec::ParseResult onSubgroup(
      TrackAlias alias,
      uint64_t group,
      uint64_t subgroup,
      std::optional<uint8_t> priority,
      const SubgroupOptions& options) override {
    trackAlias_ = alias; // Store for use in onObjectBegin logging
    XLOG(DBG1) << "onSubgroup: alias=" << trackAlias_ << " group=" << group
               << " subgroup=" << subgroup << " priority="
               << (priority.has_value() ? std::to_string(*priority) : "none");

    // Call lambda to get state
    auto subscribeState =
        onSubgroupFunc_(alias, group, subgroup, priority, options);
    if (!subscribeState) {
      // State not ready, return BLOCKED and wait
      XLOG(DBG4) << "onSubgroup: State not ready, returning BLOCKED";
      return MoQCodec::ParseResult::BLOCKED;
    }

    subscribeState_ = std::move(subscribeState);
    session_->onSubscriptionStreamOpenedByPeer();
    auto callback = subscribeState_->getSubscribeCallback();
    if (!callback) {
      // This cannot happen in a PUBLISH_DONE flow, because
      // that also would have removed subscribeState.
      XLOG(DBG2) << "No callback for subgroup (unsubscribed)";
      return MoQCodec::ParseResult::ERROR_TERMINATE;
    }

    // Use object priority if present, else fall back to publisher priority
    uint8_t effectivePriority =
        priority.value_or(subscribeState_->getPublisherPriority());
    auto res = callback->beginSubgroup(group, subgroup, effectivePriority);
    if (res.hasValue()) {
      subgroupCallback_ = *res;
    } else {
      return MoQCodec::ParseResult::ERROR_TERMINATE;
    }
    if (logger_) {
      logger_->logSubgroupHeaderParsed(
          currentStreamId_, alias, group, subgroup, effectivePriority, options);
    }

    subscribeState_->setCurrentStreamId(currentStreamId_);
    subscribeState_->onSubgroup();
    return MoQCodec::ParseResult::CONTINUE;
  }

  MoQCodec::ParseResult onFetchHeader(RequestID requestID) override {
    // Call lambda to get state - fetch should always be ready
    fetchState_ = onFetchFunc_(requestID);

    if (!fetchState_) {
      XLOG(ERR) << "Fetch response for unknown track";
      return MoQCodec::ParseResult::ERROR_TERMINATE;
    }

    fetchState_->setCurrentStreamId(currentStreamId_);
    fetchState_->onFetchHeader(requestID);
    return MoQCodec::ParseResult::CONTINUE;
  }

  MoQCodec::ParseResult onObjectBegin(
      uint64_t group,
      uint64_t subgroup,
      uint64_t objectID,
      Extensions extensions,
      uint64_t length,
      Payload initialPayload,
      bool objectComplete,
      bool streamComplete) override {
    if (isCancelled()) {
      return MoQCodec::ParseResult::ERROR_TERMINATE;
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
        currentObj_ = std::move(obj);
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
    return res ? MoQCodec::ParseResult::CONTINUE
               : MoQCodec::ParseResult::ERROR_TERMINATE;
  }

  MoQCodec::ParseResult onObjectPayload(Payload payload, bool objectComplete)
      override {
    if (isCancelled()) {
      return MoQCodec::ParseResult::ERROR_TERMINATE;
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
      return MoQCodec::ParseResult::ERROR_TERMINATE;
    } else {
      // TODO: CHECK seems too aggressive
      XCHECK_EQ(objectComplete, res.value() == ObjectPublishStatus::DONE);
    }
    return MoQCodec::ParseResult::CONTINUE;
  }

  MoQCodec::ParseResult onObjectStatus(
      uint64_t group,
      uint64_t subgroup,
      uint64_t objectID,
      std::optional<uint8_t> priority,
      ObjectStatus status) override {
    if (isCancelled()) {
      return MoQCodec::ParseResult::ERROR_TERMINATE;
    }
    folly::Expected<folly::Unit, MoQPublishError> res{folly::unit};
    // Handle subscription/fetch consumers
    switch (status) {
      case ObjectStatus::NORMAL:
        break;
      case ObjectStatus::OBJECT_NOT_EXIST:
        // Object doesn't exist - no action needed, continue
        break;
      case ObjectStatus::GROUP_NOT_EXIST:
        // Group doesn't exist - end subgroup for subscriptions
        if (!fetchState_) {
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
              /*finFetch=*/false);
        } else {
          res = subgroupCallback_->endOfGroup(objectID);
          endOfSubgroup();
        }
        break;
      case ObjectStatus::END_OF_TRACK:
        res = invokeCallback(
            &SubgroupConsumer::endOfTrackAndGroup,
            &FetchConsumer::endOfTrackAndGroup,
            group,
            subgroup,
            objectID);
        endOfSubgroup();
        break;
    }
    return res ? MoQCodec::ParseResult::CONTINUE
               : MoQCodec::ParseResult::ERROR_TERMINATE;
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

  void setLogger(std::shared_ptr<MLogger> logger) {
    logger_ = logger;
  }

 private:
  bool isCancelled() const {
    if (fetchState_) {
      return !fetchState_->getFetchCallback();
    } else if (subscribeState_) {
      return !subgroupCallback_ || subscribeState_->isCancelled();
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
  MoQSession* session_{nullptr};
  OnSubgroupFunc onSubgroupFunc_;
  OnFetchFunc onFetchFunc_;
  std::shared_ptr<MoQSession::SubscribeTrackReceiveState> subscribeState_;
  std::shared_ptr<SubgroupConsumer> subgroupCallback_;
  std::shared_ptr<MoQSession::FetchTrackReceiveState> fetchState_;
  uint64_t currentStreamId_{0};
  TrackAlias trackAlias_{0};
  ObjectHeader currentObj_;
};

} // namespace detail

folly::coro::Task<void> MoQSession::unidirectionalReadLoop(
    std::shared_ptr<MoQSession> session,
    proxygen::WebTransport::StreamReadHandle* readHandle) {
  co_await folly::coro::co_safe_point;
  auto id = readHandle->getID();
  auto rhToken = readHandle->getCancelToken();
  XLOG(DBG1) << __func__ << " id=" << id << " sess=" << this;
  auto g = folly::makeGuard([func = __func__, this, id] {
    XLOG(DBG1) << "exit " << func << " id=" << id << " sess=" << this;
  });

  // Add cancellation callback to null out readHandle on cancellation
  folly::CancellationCallback cancelCb(
      rhToken, [&readHandle]() { readHandle = nullptr; });

  // Scope guard to unify stopSending on exit if readHandle is still valid
  auto stopSendingGuard = folly::makeGuard([&readHandle, sess = this]() {
    if (readHandle) {
      XLOG(DBG0) << "Sending STOP_SENDING id=" << readHandle->getID()
                 << " sess=" << sess;
      readHandle->stopSending(0);
      readHandle = nullptr;
    }
  });

  if (!negotiatedVersion_.has_value()) {
    auto versionBaton = std::make_shared<moxygen::TimedBaton>();
    subgroupsWaitingForVersion_.push_back(versionBaton);
    // Merged token for baton waits (session + readHandle)
    auto batonWaitToken = folly::cancellation_token_merge(
        cancellationSource_.getToken(), rhToken);
    auto waitRes = co_await co_awaitTry(co_withCancellation(
        batonWaitToken,
        versionBaton->wait(moqSettings_.versionNegotiationTimeout)));
    if (waitRes.hasException()) {
      co_return;
    }
  }

  MoQObjectStreamCodec codec(nullptr);
  codec.initializeVersion(*negotiatedVersion_);

  // Baton for waiting on unknown alias
  TimedBaton aliasBaton;

  // Lambda for onSubgroup
  TrackAlias deferredAlias{std::numeric_limits<uint64_t>::max()};
  uint64_t deferredGroup = 0;
  uint64_t deferredSubgroup = 0;
  std::optional<uint8_t> deferredPriority;
  SubgroupOptions deferredOptions;
  auto token = co_await folly::coro::co_current_cancellation_token;
  auto onSubgroupFunc = [this,
                         &token,
                         &aliasBaton,
                         &deferredAlias,
                         &deferredGroup,
                         &deferredSubgroup,
                         &deferredPriority,
                         &deferredOptions](
                            TrackAlias alias,
                            uint64_t group,
                            uint64_t subgroup,
                            const std::optional<uint8_t>& priority,
                            const SubgroupOptions& options)
      -> std::shared_ptr<SubscribeTrackReceiveState> {
    auto state = getSubscribeTrackReceiveState(alias);
    if (!state) {
      XLOG(DBG4) << "State not ready, adding baton to bufferedSubgroups_["
                 << alias << "]";
      bufferedSubgroups_[alias].push_back(&aliasBaton);
      deferredAlias = alias;
      deferredGroup = group;
      deferredSubgroup = subgroup;
      deferredPriority = priority;
      deferredOptions = options;
      // Indicates BLOCKED
      return nullptr;
    } else {
      // SubscribeTrackReceiveState lifecycle now controls read loop
      token = state->getCancelToken();
    }
    return state;
  };

  // Lambda for onFetch
  auto onFetchFunc = [this, &token](RequestID requestID) {
    auto state = getFetchTrackReceiveState(requestID);
    if (state) {
      // FetchTrackReceiveState lifecycle now controls read loop
      token = state->getCancelToken();
    }
    return state;
  };

  detail::ObjectStreamCallback dcb(this, onSubgroupFunc, onFetchFunc);
  if (logger_) {
    dcb.setLogger(logger_);
  }
  dcb.setCurrentStreamId(readHandle->getID());
  codec.setCallback(&dcb);
  codec.setStreamId(id);

  while (readHandle && !token.isCancellationRequested()) {
    // Use session or request state token for read (NOT readHandle token)
    // This prevents exception masking when WebTransport cancels readHandle
    auto streamData = co_await co_awaitTry(
        folly::coro::co_withCancellation(
            token, readHandle->readStreamData().via(exec_.get())));
    if (streamData.hasException()) {
      XLOG(ERR) << folly::exceptionStr(streamData.exception()) << " id=" << id
                << " sess=" << this;
      ResetStreamErrorCode errorCode{ResetStreamErrorCode::INTERNAL_ERROR};
      auto wtEx =
          streamData.tryGetExceptionObject<proxygen::WebTransport::Exception>();
      if (wtEx) {
        errorCode = ResetStreamErrorCode(wtEx->error);
      }
      if (!dcb.reset(errorCode)) {
        XLOG(ERR) << __func__ << " terminating for unknown "
                  << "stream id=" << id << " sess=" << this;
      }
      break;
    }
    if (streamData->data || streamData->fin) {
      MoQCodec::ParseResult result = MoQCodec::ParseResult::ERROR_TERMINATE;
      try {
        result = codec.onIngress(std::move(streamData->data), streamData->fin);

        // Handle BLOCKED state
        if (result == MoQCodec::ParseResult::BLOCKED) {
          XLOG(DBG4) << "Parser returned BLOCKED, waiting for signal id=" << id;
          // Merged token for baton waits (session + readHandle)
          auto batonWaitToken = folly::cancellation_token_merge(
              cancellationSource_.getToken(), rhToken);
          auto waitRes = co_await co_awaitTry(co_withCancellation(
              batonWaitToken,
              aliasBaton.wait(moqSettings_.unknownAliasTimeout)));
          if (waitRes.hasException()) {
            XLOG(ERR) << "Timed out waiting for subscription state id=" << id
                      << " sess=" << this;
            removeBufferedSubgroupBaton(deferredAlias, &aliasBaton);
            break;
          }
          result = dcb.onSubgroup(
              deferredAlias,
              deferredGroup,
              deferredSubgroup,
              deferredPriority,
              deferredOptions);

          if (result == MoQCodec::ParseResult::CONTINUE) {
            // codec may have buffered excess ingress while blocked
            result = codec.onIngress(nullptr, streamData->fin);
          }
          if (result == MoQCodec::ParseResult::BLOCKED) {
            // state was deleted (unsubscribe)
            result = MoQCodec::ParseResult::ERROR_TERMINATE;
          }
        }
      } catch (const std::exception& ex) {
        XLOG(ERR) << "Exception in stream processing: "
                  << folly::exceptionStr(ex) << " id=" << id
                  << " sess=" << this;
        result = MoQCodec::ParseResult::ERROR_TERMINATE;
      }

      if (streamData->fin) {
        XLOG(DBG3) << "End of stream id=" << id << " sess=" << this;
        readHandle = nullptr;
      } else if (result == MoQCodec::ParseResult::ERROR_TERMINATE) {
        XLOG(ERR) << "Error parsing/consuming stream id=" << id
                  << " sess=" << this;
        break;
      }
    } // else empty read
  }
  // stopSendingGuard will handle stopSending if needed
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
  if (receivedGoaway_) {
    XLOG(DBG1) << "Rejecting subscribe request, received GOAWAY sess=" << this;
    subscribeError(
        {subscribeRequest.requestID,
         SubscribeErrorCode::GOING_AWAY,
         "Session received GOAWAY"});
    return;
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

  // Extract delivery timeout from subscribe request params
  std::optional<std::chrono::milliseconds> deliveryTimeout;
  auto timeoutValue =
      getDeliveryTimeoutIfPresent(subscribeRequest.params, *negotiatedVersion_);
  if (timeoutValue.has_value() && *timeoutValue > 0) {
    deliveryTimeout = std::chrono::milliseconds(*timeoutValue);
  }

  auto trackPublisher = std::make_shared<TrackPublisherImpl>(
      this,
      subscribeRequest.fullTrackName,
      subscribeRequest.requestID,
      std::nullopt,
      subscribeRequest.priority,
      subscribeRequest.groupOrder,
      *negotiatedVersion_,
      moqSettings_.bufferingThresholds.perSubscription,
      forward,
      deliveryTimeout);
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
  auto fullTrackName = sub.fullTrackName;
  auto& params = sub.params;

  // TODO: Formalize after parameter refactor
  // We should only keep e2e params here and remove everything else
  //// Remove DELIVERY_TIMEOUT param from params before passing to
  //// publishHandler_
  params.eraseAllParamsOfType(TrackRequestParamKey::DELIVERY_TIMEOUT);

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

    setPublisherPriorityFromParams(subOk.params, trackPublisher);
    trackPublisher->subscribeOkSent(subOk);

    // TODO: verify TrackAlias is unique
    sendSubscribeOk(subOk);
    trackPublisher->setSubscriptionHandle(std::move(subHandle));
  }
}

void MoQSession::setPublisherPriorityFromParams(
    const TrackRequestParameters& params,
    const std::shared_ptr<TrackPublisherImpl>& trackPublisher) {
  // Extract PUBLISHER_PRIORITY parameter if present (version 15+)
  if (getDraftMajorVersion(*getNegotiatedVersion()) >= 15) {
    auto publisherPriority =
        getFirstIntParam(params, TrackRequestParamKey::PUBLISHER_PRIORITY);
    if (publisherPriority) {
      if (*publisherPriority <= 255) {
        trackPublisher->setPublisherPriority(
            static_cast<uint8_t>(*publisherPriority));
      } else {
        XLOG(WARN) << "Invalid priority value: "
                   << static_cast<uint64_t>(*publisherPriority);
      }
    } else {
      trackPublisher->setPublisherPriority(kDefaultPriority);
    }
  }
}

void MoQSession::setPublisherPriorityFromParams(
    const TrackRequestParameters& params,
    const std::shared_ptr<SubscribeTrackReceiveState>& trackReceiveState) {
  // Extract PUBLISHER_PRIORITY parameter if present (version 15+)
  if (getDraftMajorVersion(*getNegotiatedVersion()) >= 15) {
    auto publisherPriority =
        getFirstIntParam(params, TrackRequestParamKey::PUBLISHER_PRIORITY);
    if (publisherPriority) {
      if (*publisherPriority <= 255) {
        trackReceiveState->setPublisherPriority(
            static_cast<uint8_t>(*publisherPriority));
      } else {
        XLOG(ERR) << "Invalid priority value: "
                  << static_cast<uint64_t>(*publisherPriority);
      }
    }
  }
}

void MoQSession::onRequestUpdate(RequestUpdate requestUpdate) {
  XLOG(DBG1) << __func__ << " id=" << requestUpdate.requestID
             << " sess=" << this;
  MOQ_PUBLISHER_STATS(publisherStatsCallback_, onRequestUpdate);
  auto existingRequestID = requestUpdate.requestID;

  if (getDraftMajorVersion(*getNegotiatedVersion()) >= 14) {
    existingRequestID = requestUpdate.existingRequestID;

    // RequestID meaning has changed, check validity
    if (closeSessionIfRequestIDInvalid(requestUpdate.requestID, false, true)) {
      return;
    }
  }

  if (!publishHandler_) {
    XLOG(DBG1) << __func__ << "No publisher callback set";
    return;
  }

  if (logger_) {
    logger_->logSubscribeUpdate(requestUpdate, ControlMessageType::PARSED);
  }

  if (closeSessionIfRequestIDInvalid(existingRequestID, false, false, false)) {
    return;
  }
  auto it = pubTracks_.find(existingRequestID);
  if (it == pubTracks_.end()) {
    XLOG(ERR) << "No matching subscribe ID=" << existingRequestID
              << " sess=" << this;
    return;
  }

  it->second->setSubPriority(requestUpdate.priority);
  // TODO: update priority of tracks in flight
  auto pubTrackIt = pubTracks_.find(existingRequestID);
  if (pubTrackIt == pubTracks_.end()) {
    XLOG(ERR) << "RequestUpdate track not found id=" << existingRequestID
              << " sess=" << this;
    return;
  }
  auto trackPublisher =
      std::static_pointer_cast<TrackPublisherImpl>(pubTrackIt->second);
  if (!trackPublisher) {
    XLOG(ERR) << "existingRequestID in RequestUpdate is for a FETCH, id="
              << existingRequestID << " sess=" << this;
  } else {
    trackPublisher->onRequestUpdate(std::move(requestUpdate));
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
    } // else, the caller invoked publishDone, which isn't needed but fine
  }
}

void MoQSession::onPublishOk(PublishOk publishOk) {
  XLOG(DBG1) << __func__ << " reqID=" << publishOk.requestID
             << " sess=" << this;
  if (logger_) {
    logger_->logPublishOk(publishOk, ControlMessageType::PARSED);
  }
  auto pubIt = pendingRequests_.find(publishOk.requestID);
  if (pubIt == pendingRequests_.end()) {
    XLOG(ERR) << "No matching publish reqID=" << publishOk.requestID
              << " sess=" << this;
    close(SessionCloseErrorCode::PROTOCOL_VIOLATION);
    return;
  }

  auto* publishPtr = pubIt->second->tryGetPublish();
  if (!publishPtr) {
    XLOG(ERR) << "Request ID " << publishOk.requestID
              << " is not a publish request, sess=" << this;
    close(SessionCloseErrorCode::PROTOCOL_VIOLATION);
    return;
  }

  // Extract delivery timeout from PUBLISH_OK params and update
  // trackPublisher
  auto trackIt = pubTracks_.find(publishOk.requestID);
  if (trackIt != pubTracks_.end()) {
    auto trackPublisher =
        std::static_pointer_cast<TrackPublisherImpl>(trackIt->second);
    trackPublisher->onPublishOk(publishOk);
  }

  publishPtr->setValue(std::move(publishOk));
  pendingRequests_.erase(pubIt);
}

void MoQSession::onRequestError(RequestError error, FrameType frameType) {
  XLOG(DBG1) << __func__ << " reqID=" << error.requestID
             << " frameType=" << folly::to_underlying(frameType)
             << " sess=" << this;

  // Log the error using the appropriate logger method
  auto g = folly::makeGuard([&] {
    logRequestError(logger_, error, frameType, ControlMessageType::PARSED);
  });

  // Find the pending request and invoke setError
  auto it = pendingRequests_.find(error.requestID);
  if (it != pendingRequests_.end()) {
    auto pendingState = std::move(it->second);
    pendingRequests_.erase(it);
    if (getDraftMajorVersion(*getNegotiatedVersion()) > 14) {
      // determine real frame type from pendingRequest
      frameType = pendingState->getErrorFrameType();
    }

    auto setErrorRes = pendingState->setError(error, frameType);
    if (setErrorRes.hasError()) {
      XLOG(ERR) << "setError failure id=" << error.requestID
                << " sess=" << this;
      close(SessionCloseErrorCode::PROTOCOL_VIOLATION);
      return;
    }
  } else {
    XLOG(ERR) << "Request not found id=" << error.requestID << " sess=" << this;
    close(SessionCloseErrorCode::PROTOCOL_VIOLATION);
    return;
  }

  // Additional cleanup for specific error types if needed
  switch (frameType) {
    case FrameType::SUBSCRIBE_ERROR: {
      auto aliasIt = reqIdToTrackAlias_.find(error.requestID);
      if (aliasIt != reqIdToTrackAlias_.end()) {
        removeSubscriptionState(aliasIt->second, error.requestID);
      }
      // TODO: bufferedSubgroups_ cleanup not required - batons will timeout
      // but we should clear them proactively
      break;
    }
    case FrameType::PUBLISH_ERROR: {
      auto pubIt = pubTracks_.find(error.requestID);
      if (pubIt != pubTracks_.end()) {
        pubIt->second->setSession(nullptr);
        pubTracks_.erase(pubIt);
      }
      break;
    }
    case FrameType::FETCH_ERROR: {
      auto fetchIt = fetches_.find(error.requestID);
      if (fetchIt != fetches_.end()) {
        fetches_.erase(fetchIt);
      }
      break;
    }
    case FrameType::PUBLISH_NAMESPACE_ERROR:
    case FrameType::SUBSCRIBE_NAMESPACE_ERROR:
      break;
    default:
      // Unknown or unsupported error type
      break;
  }
  checkForCloseOnDrain();
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
  auto* trackPtr = it->second->tryGetSubscribeTrack();
  if (!trackPtr) {
    XLOG(ERR) << "Request ID " << subOk.requestID
              << " is not a subscribe track request, sess=" << this;
    close(SessionCloseErrorCode::PROTOCOL_VIOLATION);
    return;
  }
  auto trackReceiveState = std::move(*trackPtr);
  pendingRequests_.erase(it);

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
  setPublisherPriorityFromParams(subOk.params, trackReceiveState);
  trackReceiveState->processSubscribeOK(std::move(subOk));
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
    XLOG(DBG4) << "Signaling " << subgroups.size()
               << " batons for alias=" << trackAlias;
    for (auto* baton : subgroups) {
      baton->signal();
    }
  }
}

// Helper to remove a particular alias/baton from bufferedSubgroups_
void MoQSession::removeBufferedSubgroupBaton(
    TrackAlias alias,
    TimedBaton* baton) {
  auto it = bufferedSubgroups_.find(alias);
  if (it != bufferedSubgroups_.end()) {
    auto& batonList = it->second;
    batonList.remove(baton);
    if (batonList.empty()) {
      bufferedSubgroups_.erase(it);
    }
  }
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

  folly::coro::Task<SubscriptionHandle::SubscribeUpdateResult> subscribeUpdate(
      SubscribeUpdate subscribeUpdate) override {
    if (!session_) {
      co_return folly::makeUnexpected(
          SubscribeUpdateError{
              subscribeUpdate.requestID,
              RequestErrorCode::INTERNAL_ERROR,
              "Session closed"});
    }

    subscribeUpdate.existingRequestID = subscribeOk_->requestID;
    if (getDraftMajorVersion(*(session_->getNegotiatedVersion())) >= 14) {
      subscribeUpdate.requestID = session_->getNextRequestID();
    } else {
      subscribeUpdate.requestID = subscribeOk_->requestID;
    }

    // For v15+, create promise and wait for REQUEST_OK response
    // For v14 and below, just send the message and return immediately
    if (getDraftMajorVersion(*(session_->getNegotiatedVersion())) >= 15) {
      // Create promise/contract for tracking the response
      auto contract = folly::coro::makePromiseContract<
          folly::Expected<SubscribeUpdateOk, SubscribeUpdateError>>();

      // Register pending request
      session_->pendingRequests_.emplace(
          subscribeUpdate.requestID,
          PendingRequestState::makeSubscribeUpdate(std::move(contract.first)));

      // Send the SUBSCRIBE_UPDATE message
      session_->subscribeUpdate(subscribeUpdate);

      // Wait for REQUEST_OK or REQUEST_ERROR response
      co_return co_await std::move(contract.second);
    } else {
      session_->subscribeUpdate(subscribeUpdate);

      // Version < 15: Return a constructed response. SubscribeUpdate is fire
      // and forget
      co_return SubscribeUpdateOk{.requestID = subscribeUpdate.requestID};
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
  if (logger_) {
    logger_->logPublish(
        publish, MOQTByteStringType::STRING_VALUE, ControlMessageType::PARSED);
  }
  MOQ_SUBSCRIBER_STATS(subscriberStatsCallback_, onPublish);
  if (closeSessionIfRequestIDInvalid(publish.requestID, false, true)) {
    return;
  }
  if (receivedGoaway_) {
    XLOG(DBG1) << "Rejecting publish request, received GOAWAY sess=" << this;
    publishError(
        PublishError{
            publish.requestID,
            PublishErrorCode::GOING_AWAY,
            "Session received GOAWAY"});
    return;
  }

  if (!subscribeHandler_) {
    XLOG(DBG1) << __func__ << " No subscriber callback set";
    publishError(
        PublishError{
            publish.requestID,
            PublishErrorCode::NOT_SUPPORTED,
            "Not a subscriber"});
    return;
  }

  auto publishHandle = std::make_shared<ReceiverSubscriptionHandle>(
      SubscribeOk{publish.requestID}, publish.trackAlias, shared_from_this());

  // Use single coroutine pattern like working onPublishNamespace
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
  auto params = publish.params;

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
            ftn, requestID, initiator.consumer, this, alias, logger_, true);

        // Extract PUBLISHER_PRIORITY parameter if present (version 15+)
        setPublisherPriorityFromParams(params, trackReceiveState);

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

void MoQSession::onPublishDone(PublishDone publishDone) {
  XLOG(DBG1) << "PublishDone id=" << publishDone.requestID
             << " code=" << folly::to_underlying(publishDone.statusCode)
             << " reason=" << publishDone.reasonPhrase;

  if (logger_) {
    logger_->logPublishDone(publishDone, ControlMessageType::PARSED);
  }
  MOQ_SUBSCRIBER_STATS(
      subscriberStatsCallback_, onPublishDone, publishDone.statusCode);

  // Handle regular subscription PUBLISH_DONE
  auto trackAliasIt = reqIdToTrackAlias_.find(publishDone.requestID);
  if (trackAliasIt == reqIdToTrackAlias_.end()) {
    // unknown
    XLOG(ERR) << "No matching subscribe ID=" << publishDone.requestID
              << " sess=" << this;
    return;
  }

  auto alias = trackAliasIt->second;
  auto trackReceiveStateIt = subTracks_.find(alias);
  if (trackReceiveStateIt != subTracks_.end()) {
    auto state = trackReceiveStateIt->second;
    state->processPublishDone(std::move(publishDone));
    // Note: State removal is handled by deliverPublishDoneAndRemove called
    // from processPublishDone (when streams already arrived), onSubgroup
    // (when stream count reached), or timeout callback (when timeout expires)
  } else {
    XLOG(DFATAL) << "trackAliasIt but no trackReceiveStateIt for id="
                 << publishDone.requestID << " sess=" << this;
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
    logger_->logMaxRequestId(
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
    logger_->logRequestsBlocked(
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
  if (receivedGoaway_) {
    XLOG(DBG1) << "Rejecting fetch request, received GOAWAY sess=" << this;
    fetchError(
        {fetch.requestID,
         FetchErrorCode::GOING_AWAY,
         "Session received GOAWAY"});
    return;
  }
  if (standalone) {
    if (standalone->end <= standalone->start) {
      // If the end object is zero this indicates a fetch for the entire
      // group, which is valid as long as the start and end group are the
      // same.
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
    // We need to call reset() in order to ensure that the
    // StreamPublisherImpl is destructed, otherwise there could be memory
    // leaks, since the StreamPublisherImpl and the PublisherImpl hold
    // references to each other. This doesn't actually reset the stream
    // unless the user wrote something to it within the fetch handler,
    // because the stream creation is deferred until the user actually
    // writes something to a stream.
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
  } // else, no need to fetchError, state has been removed on both sides
    // already
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

void MoQSession::onTrackStatus(TrackStatus trackStatus) {
  MOQ_PUBLISHER_STATS(publisherStatsCallback_, onTrackStatus);
  XLOG(DBG1) << __func__ << " ftn=" << trackStatus.fullTrackName
             << " sess=" << this;
  if (logger_) {
    logger_->logTrackStatus(
        trackStatus,
        MOQTByteStringType::STRING_VALUE,
        ControlMessageType::PARSED);
  }
  if (closeSessionIfRequestIDInvalid(trackStatus.requestID, false, true)) {
    return;
  }
  if (receivedGoaway_) {
    XLOG(DBG1) << "Rejecting track status request, received GOAWAY sess="
               << this;
    trackStatusError(
        {trackStatus.requestID,
         TrackStatusErrorCode::GOING_AWAY,
         "Session received GOAWAY"});
    return;
  }
  if (!publishHandler_) {
    XLOG(DBG1) << __func__ << "No publisher callback set";
    trackStatusError(
        {trackStatus.requestID,
         TrackStatusErrorCode::INTERNAL_ERROR,
         "No publisher callback set"});
  } else {
    co_withExecutor(exec_.get(), handleTrackStatus(std::move(trackStatus)))
        .start();
  }
}

folly::coro::Task<void> MoQSession::handleTrackStatus(TrackStatus trackStatus) {
  auto trackStatusResult = co_await co_awaitTry(co_withCancellation(
      cancellationSource_.getToken(),
      publishHandler_->trackStatus(trackStatus)));
  if (trackStatusResult.hasException()) {
    XLOG(ERR) << "Exception in Publisher callback ex="
              << trackStatusResult.exception().what().toStdString();
    trackStatusError(
        {trackStatus.requestID,
         TrackStatusErrorCode::INTERNAL_ERROR,
         trackStatusResult.exception().what().toStdString()});
  }
  if (trackStatusResult->hasError()) {
    XLOG(DBG1) << "Application track status error err="
               << trackStatusResult->error().reasonPhrase;
    auto trackStatusErr = std::move(trackStatusResult->error());
    trackStatusErr.requestID = trackStatus.requestID;
    trackStatusError(trackStatusErr);
  } else {
    auto trackStatOk = std::move(trackStatusResult->value());
    trackStatOk.requestID = trackStatus.requestID;
    trackStatOk.fullTrackName = trackStatus.fullTrackName;
    trackStatusOk(trackStatOk);
  }
  retireRequestID(/*signalWriteLoop=*/false);
}

void MoQSession::trackStatusOk(const TrackStatusOk& trackStatusOk) {
  auto res =
      moqFrameWriter_.writeTrackStatusOk(controlWriteBuf_, trackStatusOk);

  if (logger_) {
    logger_->logTrackStatusOk(trackStatusOk);
  }

  if (!res) {
    XLOG(ERR) << "trackStatusOk failed sess=" << this;
  } else {
    controlWriteEvent_.signal();
  }
}

void MoQSession::trackStatusError(const TrackStatusError& trackStatusError) {
  auto res =
      moqFrameWriter_.writeTrackStatusError(controlWriteBuf_, trackStatusError);

  if (logger_) {
    logger_->logTrackStatusError(trackStatusError);
  }

  if (!res) {
    XLOG(ERR) << "trackStatusError failed sess=" << this;
  } else {
    controlWriteEvent_.signal();
  }
}

void MoQSession::handleTrackStatusOkFromRequestOk(const RequestOk& requestOk) {
  XLOG(DBG1) << __func__ << " redId=" << requestOk.requestID
             << " sess=" << this;
  auto trackStatusOk = requestOk.toTrackStatusOk();
  onTrackStatusOk(std::move(trackStatusOk));
}

void MoQSession::handleSubscribeUpdateOkFromRequestOk(
    const RequestOk& requestOk,
    PendingRequestIterator reqIt) {
  XLOG(DBG1) << __func__ << " reqID=" << requestOk.requestID
             << " sess=" << this;

  auto pendingRequest = std::move(reqIt->second);
  pendingRequests_.erase(reqIt);

  auto* promise = pendingRequest->tryGetSubscribeUpdate();
  if (!promise) {
    XLOG(ERR) << "handleSubscribeUpdateOkFromRequestOk: invalid promise type"
              << " requestID=" << requestOk.requestID << " sess=" << this;
    return;
  }

  promise->setValue(requestOk);
}

folly::coro::Task<MoQSession::TrackStatusResult> MoQSession::trackStatus(
    TrackStatus trackStatus) {
  MOQ_SUBSCRIBER_STATS(subscriberStatsCallback_, onTrackStatus);
  XLOG(DBG1) << __func__ << " ftn=" << trackStatus.fullTrackName
             << "sess=" << this;
  if (draining_) {
    XLOG(DBG1) << "Rejecting track status request, session draining sess="
               << this;
    TrackStatusError trackStatusError{
        trackStatus.requestID,
        TrackStatusErrorCode::INTERNAL_ERROR,
        "Draining session"};
    co_return folly::makeUnexpected(trackStatusError);
  }
  aliasifyAuthTokens(trackStatus.params);
  trackStatus.requestID = getNextRequestID();

  auto res = moqFrameWriter_.writeTrackStatus(controlWriteBuf_, trackStatus);
  if (!res) {
    XLOG(ERR) << "writeTrackStatus failed sess=" << this;
    co_return folly::makeUnexpected(
        TrackStatusError{
            trackStatus.requestID,
            TrackStatusErrorCode::INTERNAL_ERROR,
            "local write failed"});
  }
  if (logger_) {
    logger_->logTrackStatus(trackStatus);
  }
  controlWriteEvent_.signal();
  auto contract = folly::coro::makePromiseContract<
      folly::Expected<TrackStatusOk, TrackStatusError>>();
  pendingRequests_.emplace(
      trackStatus.requestID,
      PendingRequestState::makeTrackStatus(std::move(contract.first)));
  co_return co_await std::move(contract.second);
}

void MoQSession::onTrackStatusOk(TrackStatusOk trackStatusOk) {
  XLOG(DBG1) << __func__ << " ftn=" << trackStatusOk.fullTrackName
             << " code=" << uint64_t(trackStatusOk.statusCode)
             << " sess=" << this;
  auto reqID = trackStatusOk.requestID;
  auto trackStatusIt = pendingRequests_.find(reqID);

  if (logger_) {
    logger_->logTrackStatusOk(
        trackStatusOk,
        MOQTByteStringType::STRING_VALUE,
        ControlMessageType::PARSED);
  }
  if (trackStatusIt == pendingRequests_.end()) {
    XLOG(ERR) << __func__
              << " Couldn't find a pending TrackStatusRequest for reqID="
              << reqID << " ftn=" << trackStatusOk.fullTrackName;
    close(SessionCloseErrorCode::PROTOCOL_VIOLATION);
    return;
  }

  auto* trackStatusPtr = trackStatusIt->second->tryGetTrackStatus();
  if (!trackStatusPtr) {
    XLOG(ERR) << "Request ID " << reqID
              << " is not a track status request, sess=" << this;
    close(SessionCloseErrorCode::PROTOCOL_VIOLATION);
    return;
  }

  trackStatusPtr->setValue(std::move(trackStatusOk));
  pendingRequests_.erase(trackStatusIt);
}

void MoQSession::onTrackStatusError(TrackStatusError trackStatusError) {
  XLOG(DBG1) << __func__ << " id=" << trackStatusError.requestID
             << " sess=" << this;
  auto reqID = trackStatusError.requestID;
  auto trackStatusIt = pendingRequests_.find(reqID);

  if (logger_) {
    logger_->logTrackStatusError(
        trackStatusError,
        MOQTByteStringType::STRING_VALUE,
        ControlMessageType::PARSED);
  }
  if (trackStatusIt == pendingRequests_.end()) {
    XLOG(ERR) << __func__
              << " Couldn't find a pending TrackStatusRequest for reqID="
              << reqID;
    close(SessionCloseErrorCode::PROTOCOL_VIOLATION);
    return;
  }

  auto* trackStatusPtr = trackStatusIt->second->tryGetTrackStatus();
  if (!trackStatusPtr) {
    XLOG(ERR) << "Request ID " << reqID
              << " is not a track status request, sess=" << this;
    close(SessionCloseErrorCode::PROTOCOL_VIOLATION);
    return;
  }

  trackStatusPtr->setValue(folly::makeUnexpected(std::move(trackStatusError)));
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

Subscriber::PublishResult MoQSession::publish(
    PublishRequest pub,
    std::shared_ptr<Publisher::SubscriptionHandle> handle) {
  XLOG(DBG1) << __func__ << " reqID=" << pub.requestID
             << " track=" << pub.fullTrackName.trackName << " sess=" << this;

  // Reject new publish attempts if session is draining
  if (draining_) {
    XLOG(DBG1) << "Rejecting publish request, session draining sess=" << this;
    return folly::makeUnexpected(
        PublishError{
            pub.requestID,
            PublishErrorCode::INTERNAL_ERROR,
            "Session draining"});
  }

  if (!handle) {
    XLOG(DBG1) << "Rejecting publish request, no subscription handle sess="
               << this;
    return folly::makeUnexpected(
        PublishError{
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
    return folly::makeUnexpected(
        PublishError{
            pub.requestID,
            PublishErrorCode::INTERNAL_ERROR,
            "local write failed"});
  }
  if (logger_) {
    logger_->logPublish(
        pub, MOQTByteStringType::STRING_VALUE, ControlMessageType::CREATED);
  }
  controlWriteEvent_.signal();

  // Extract delivery timeout from publish extensions
  auto deliveryTimeout = getPublisherDeliveryTimeout(pub);

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
      pub.forward,
      deliveryTimeout);

  // Set publishHandle in trackPublisher so it can cancel on unsubscribes
  trackPublisher->setSubscriptionHandle(handle);

  // Extract PUBLISHER_PRIORITY parameter if present (version 15+)
  setPublisherPriorityFromParams(pub.params, trackPublisher);

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
            // If Receive PublishError, the session is reset in
            // onPublishError()
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

void MoQSession::publishOk(const PublishOk& pubOk) {
  XLOG(DBG1) << __func__ << " reqID=" << pubOk.requestID << " sess=" << this;
  MOQ_SUBSCRIBER_STATS(subscriberStatsCallback_, onPublishOk);

  if (logger_) {
    logger_->logPublishOk(pubOk, ControlMessageType::CREATED);
  }

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
  if (logger_) {
    logger_->logPublishError(publishError, ControlMessageType::CREATED);
  }
  auto res = moqFrameWriter_.writeRequestError(
      controlWriteBuf_, publishError, FrameType::PUBLISH_ERROR);
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
      fullTrackName, reqID, callback, this, trackAlias, logger_);
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

void MoQSession::sendSubscribeOk(const SubscribeOk& subOk) {
  XLOG(DBG1) << __func__ << " sess=" << this;
  MOQ_PUBLISHER_STATS(publisherStatsCallback_, onSubscribeSuccess);
  auto res = moqFrameWriter_.writeSubscribeOk(controlWriteBuf_, subOk);
  if (!res) {
    XLOG(ERR) << "writeSubscribeOk failed sess=" << this;
    return;
  }

  if (logger_) {
    logger_->logSubscribeOk(subOk);
  }

  controlWriteEvent_.signal();
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
  auto res = moqFrameWriter_.writeRequestError(
      controlWriteBuf_, subErr, FrameType::SUBSCRIBE_ERROR);
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
  // cancel() should send STOP_SENDING on any open streams for this
  // subscription
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

void MoQSession::sendPublishDone(const PublishDone& pubDone) {
  XLOG(DBG1) << __func__ << " sess=" << this;
  MOQ_PUBLISHER_STATS(
      publisherStatsCallback_, onPublishDone, pubDone.statusCode);
  auto it = pubTracks_.find(pubDone.requestID);
  if (it == pubTracks_.end()) {
    XLOG(ERR) << "publishDone for invalid id=" << pubDone.requestID
              << " sess=" << this;
    return;
  }
  pubTracks_.erase(it);

  auto res = moqFrameWriter_.writePublishDone(controlWriteBuf_, pubDone);
  if (!res) {
    XLOG(ERR) << "writePublishDone failed sess=" << this;
    return;
  }

  if (logger_) {
    logger_->logPublishDone(pubDone);
  }
  controlWriteEvent_.signal();
  retireRequestID(/*signalWriteLoop=*/false);
}

void MoQSession::requestUpdateOk(const RequestOk& requestOk) {
  XLOG(DBG1) << __func__ << " reqID=" << requestOk.requestID
             << " sess=" << this;

  auto res = moqFrameWriter_.writeRequestOk(
      controlWriteBuf_, requestOk, FrameType::REQUEST_OK);
  if (!res) {
    XLOG(ERR) << "writeRequestOk for REQUEST_UPDATE failed sess=" << this;
    return;
  }

  controlWriteEvent_.signal();
}

void MoQSession::requestUpdateError(
    const SubscribeUpdateError& requestError,
    RequestID existingRequestID) {
  XLOG(DBG1) << __func__ << " reqID=" << requestError.requestID
             << " existingReqID=" << existingRequestID << " sess=" << this;

  auto res = moqFrameWriter_.writeRequestError(
      controlWriteBuf_, requestError, FrameType::REQUEST_UPDATE);
  if (!res) {
    XLOG(ERR) << "writeRequestError for REQUEST_UPDATE failed sess=" << this;
    // Proceed to cleanup state even if write failed
    // The write has errored out but this is still an update error
  } else {
    controlWriteEvent_.signal();
  }

  // Terminate subscription with PUBLISH_DONE (UPDATE_FAILED)
  // and clean up publisher state (regardless of REQUEST_ERROR write success)
  auto it = pubTracks_.find(existingRequestID);
  if (it != pubTracks_.end()) {
    PublishDone pubDone{
        existingRequestID,
        PublishDoneStatusCode::UPDATE_FAILED,
        static_cast<uint64_t>(requestError.errorCode),
        requestError.reasonPhrase};
    it->second->terminatePublish(pubDone, ResetStreamErrorCode::CANCELLED);
  } else {
    XLOG(ERR) << "requestUpdateError for invalid subscription id="
              << existingRequestID << " sess=" << this;
  }
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
    logger_->logMaxRequestId(maxRequestID_);
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

void MoQSession::requestUpdate(const RequestUpdate& reqUpdate) {
  if (logger_) {
    logger_->logSubscribeUpdate(reqUpdate);
  }
  XLOG(DBG1) << __func__ << " sess=" << this;
  MOQ_SUBSCRIBER_STATS(subscriberStatsCallback_, onRequestUpdate);
  auto trackAliasIt = reqIdToTrackAlias_.find(reqUpdate.existingRequestID);
  if (trackAliasIt == reqIdToTrackAlias_.end()) {
    // unknown
    XLOG(ERR) << "No matching request ID=" << reqUpdate.existingRequestID
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
  auto res = moqFrameWriter_.writeRequestUpdate(controlWriteBuf_, reqUpdate);
  if (!res) {
    XLOG(ERR) << "writeRequestUpdate failed sess=" << this;
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
      if (auto* trackPtr = pendingIt->second->tryGetSubscribeTrack()) {
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
  pendingRequests_.emplace(
      trackReceiveState->getRequestID(),
      PendingRequestState::makeFetch(trackReceiveState));
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
  auto res = moqFrameWriter_.writeRequestError(
      controlWriteBuf_, fetchErr, FrameType::FETCH_ERROR);
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
  trackIt->second->cancel(this);
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
    std::vector<Parameter> fetchParams,
    std::shared_ptr<FetchConsumer> fetchCallback,
    FetchType fetchType) {
  Fetch fetchReq(
      0,              // will be picked by fetch()
      nextRequestID_, // this will be the ID for subscribe()
      joiningStart,
      fetchType,
      fetchPri,
      fetchOrder,
      fetchParams);
  fetchReq.fullTrackName = sub.fullTrackName;
  auto [subscribeResult, fetchResult] = co_await folly::coro::collectAll(
      subscribe(std::move(sub), std::move(subscribeCallback)),
      fetch(std::move(fetchReq), std::move(fetchCallback)));
  co_return {subscribeResult, fetchResult};
}

void MoQSession::onNewUniStream(
    proxygen::WebTransport::StreamReadHandle* rh) noexcept {
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

void MoQSession::onNewBidiStream(
    proxygen::WebTransport::BidiStreamHandle bh) noexcept {
  XLOG(DBG1) << __func__ << " sess=" << this;

  // In draft 16 and above, the version is negotiated through the ALPN, so we
  // would know what it is by this point.
  if (negotiatedVersion_ && getDraftMajorVersion(*negotiatedVersion_) >= 16) {
    co_withExecutor(
        exec_.get(),
        co_withCancellation(
            cancellationSource_.getToken(), bidiStreamDemuxer(std::move(bh))))
        .start();
    return;
  }

  handleClientSetup(bh);
}

void MoQSession::handleClientSetup(
    proxygen::WebTransport::BidiStreamHandle bh,
    std::unique_ptr<folly::IOBuf> initialData) noexcept {
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

    bh.writeHandle->setPriority(quic::HTTPPriorityQueue::Priority(0, false, 0));
    co_withExecutor(
        exec_.get(),
        co_withCancellation(
            cancellationSource_.getToken(),
            controlReadLoop(bh.readHandle, std::move(initialData))))
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

folly::coro::Task<void> MoQSession::bidiStreamDemuxer(
    proxygen::WebTransport::BidiStreamHandle bh) noexcept {
  co_await folly::coro::co_safe_point;
  auto token = co_await folly::coro::co_current_cancellation_token;
  if (token.isCancellationRequested()) {
    co_return;
  }
  auto readHandle = bh.readHandle;
  folly::IOBufQueue readBuf{folly::IOBufQueue::cacheChainLength()};
  folly::Optional<FrameType> frameType = folly::none;

  folly::Try<proxygen::WebTransport::StreamData> streamData;
  // Keep reading the data until we can parse the frame type. Use a do/while so
  // we only issue one read per iteration and also ensure we append any final
  // data chunk that arrives with a FIN.
  do {
    streamData =
        co_await co_awaitTry(readHandle->readStreamData().via(exec_.get()));
    if (!streamData.hasValue()) {
      break;
    }
    readBuf.append(std::move(streamData->data));
    frameType = getFrameType(readBuf);
  } while (!frameType.hasValue() && !streamData->fin);

  if (frameType.hasValue()) {
    if (*frameType == FrameType::CLIENT_SETUP) {
      // Process the frame as a CLIENT_SETUP
      handleClientSetup(bh, readBuf.move());
    } else if (*frameType == FrameType::SUBSCRIBE_NAMESPACE) {
      // TODO: Handle SUBSCRIBE_ANNOUNCES on bidirectional stream
    }
  }
}

void MoQSession::onDatagram(std::unique_ptr<folly::IOBuf> datagram) noexcept {
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
    if (payload) {
      auto payloadChainLength = payload->computeChainDataLength();
      if (payloadChainLength >= remainingLength) {
        payload->trimStart(payloadChainLength - remainingLength);
      } else {
        payload.reset();
      }
    }
    logger_->logObjectDatagramParsed(
        objHeader.trackAlias, objHeader.objectHeader, payload);
  }
  if (state) {
    auto callback = state->getSubscribeCallback();
    if (callback) {
      // Populate priority from publisher priority if not set in object header
      if (!objHeader.objectHeader.priority.has_value()) {
        objHeader.objectHeader.priority = state->getPublisherPriority();
      }
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
  for (const auto& versionBaton : subgroupsWaitingForVersion_) {
    versionBaton->signal();
  }
  subgroupsWaitingForVersion_.clear();

  if (logger_) {
    logger_->setNegotiatedMoQVersion(negotiatedVersion);
  }
}

/*static*/
uint64_t MoQSession::getMaxRequestIDIfPresent(const SetupParameters& params) {
  for (const auto& param : params) {
    if (param.key == folly::to_underlying(SetupKey::MAX_REQUEST_ID)) {
      return param.asUint64;
    }
  }
  return 0;
}

/*static*/
std::optional<std::string> MoQSession::getMoQTImplementationIfPresent(
    const SetupParameters& params) {
  for (const auto& param : params) {
    if (param.key == folly::to_underlying(SetupKey::MOQT_IMPLEMENTATION)) {
      return param.asString;
    }
  }
  return std::nullopt;
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
    const SetupParameters& params) {
  constexpr uint64_t kMaxAuthTokenCacheSize = 4096;
  for (const auto& param : params) {
    if (param.key ==
        folly::to_underlying(SetupKey::MAX_AUTH_TOKEN_CACHE_SIZE)) {
      return std::min(param.asUint64, kMaxAuthTokenCacheSize);
    }
  }
  return 0;
}

/*static*/
std::optional<uint64_t> MoQSession::getDeliveryTimeoutIfPresent(
    const TrackRequestParameters& params,
    uint64_t version) {
  for (const auto& param : params) {
    if (param.key ==
        folly::to_underlying(TrackRequestParamKey::DELIVERY_TIMEOUT)) {
      return param.asUint64;
    }
  }
  return std::nullopt;
}

void MoQSession::aliasifyAuthTokens(
    Parameters& params,
    const std::optional<uint64_t>& forceVersion) {
  auto version = forceVersion ? forceVersion : getNegotiatedVersion();
  if (!version) {
    return;
  }

  auto authParamKey =
      folly::to_underlying(TrackRequestParamKey::AUTHORIZATION_TOKEN);

  size_t currentPosition = 0;

  while (currentPosition < params.size()) {
    const auto& param = params.at(currentPosition);
    if (param.key != authParamKey) {
      ++currentPosition;
      continue;
    }

    const auto& token = param.asAuthToken;
    if (token.alias && token.tokenValue.size() < tokenCache_.maxTokenSize()) {
      auto lookupRes =
          tokenCache_.getAliasForToken(token.tokenType, token.tokenValue);
      if (lookupRes) {
        params.modifyString(
            currentPosition, moqFrameWriter_.encodeUseAlias(*lookupRes));
        ++currentPosition;
        continue;
      }

      auto tokenCopy = token;
      params.eraseParam(currentPosition);
      while (!tokenCache_.canRegister(token.tokenType, token.tokenValue)) {
        auto aliasToEvict = tokenCache_.evictLRU();
        Parameter p;
        p.key = authParamKey;
        p.asString = moqFrameWriter_.encodeDeleteTokenAlias(aliasToEvict);
        params.insertParam(currentPosition, std::move(p));
        ++currentPosition;
      }

      auto alias =
          tokenCache_.registerToken(tokenCopy.tokenType, tokenCopy.tokenValue)
              .value();
      Parameter p;
      p.key = authParamKey;
      p.asString = moqFrameWriter_.encodeRegisterToken(
          alias, tokenCopy.tokenType, tokenCopy.tokenValue);
      params.insertParam(currentPosition, std::move(p));
      ++currentPosition;
      continue;
    }

    params.modifyString(
        currentPosition,
        moqFrameWriter_.encodeTokenValue(
            token.tokenType, token.tokenValue, version));
    ++currentPosition;
  }
}

// PublishNamespace callback methods - default implementations for simple
// clients
void MoQSession::onPublishNamespace(PublishNamespace publishNamespace) {
  XLOG(DBG1) << __func__ << " ns=" << publishNamespace.trackNamespace
             << " - sending NOT_SUPPORTED error, sess=" << this;
  if (receivedGoaway_) {
    XLOG(DBG1) << "Rejecting publishNamespace request, received GOAWAY sess="
               << this;
    publishNamespaceError(
        PublishNamespaceError{
            publishNamespace.requestID,
            PublishNamespaceErrorCode::GOING_AWAY,
            "Session received GOAWAY"});
    return;
  }
  publishNamespaceError(
      PublishNamespaceError{
          publishNamespace.requestID,
          PublishNamespaceErrorCode::NOT_SUPPORTED,
          "PublishNamespace not supported by simple client"});
}

void MoQSession::onRequestOk(RequestOk requestOk, FrameType frameType) {
  XLOG(DBG1) << __func__ << " ReqId=" << requestOk.requestID.value
             << " frameType=" << folly::to_underlying(frameType)
             << " sess=" << this;

  auto reqId = requestOk.requestID;
  auto reqIt = pendingRequests_.find(reqId);

  if (reqIt == pendingRequests_.end()) {
    XLOG(ERR) << "No matching request for reqID=" << reqId << " sess=" << this;
    close(SessionCloseErrorCode::PROTOCOL_VIOLATION);
    return;
  }

  // In v15+, convert REQUEST_OK back to specific frame type
  if (*getNegotiatedVersion() > 14) {
    frameType = reqIt->second->getOkFrameType();
  }

  switch (frameType) {
    case FrameType::TRACK_STATUS_OK: {
      handleTrackStatusOkFromRequestOk(requestOk);
      break;
    }
    case FrameType::REQUEST_OK: {
      switch (reqIt->second->getType()) {
        case PendingRequestState::Type::SUBSCRIBE_UPDATE:
          handleSubscribeUpdateOkFromRequestOk(requestOk, reqIt);
          break;
        default:
          XLOG(ERR) << "Unexpected REQUEST_OK type"
                    << folly::to_underlying(reqIt->second->getType())
                    << ", sess=" << this;
          close(SessionCloseErrorCode::PROTOCOL_VIOLATION);
          break;
      }
      break;
    }
    default: {
      XLOG(ERR) << "Unexpected REQUEST_OK type "
                << folly::to_underlying(frameType)
                << " in simple client, sess=" << this;
      close(SessionCloseErrorCode::PROTOCOL_VIOLATION);
      break;
    }
  }
}

void MoQSession::onPublishNamespaceDone(
    PublishNamespaceDone publishNamespaceDone) {
  if (logger_) {
    logger_->logPublishNamespaceDone(
        publishNamespaceDone,
        MOQTByteStringType::STRING_VALUE,
        ControlMessageType::PARSED);
  }

  XLOG(DBG1)
      << "Received PublishNamespaceDone on base session - ignoring, sess="
      << this;
}

void MoQSession::onPublishNamespaceCancel(
    PublishNamespaceCancel publishNamespaceCancel) {
  XLOG(DBG1) << __func__ << " ns=" << publishNamespaceCancel.trackNamespace
             << " - ignored by simple client, sess=" << this;
  MOQ_PUBLISHER_STATS(publisherStatsCallback_, onPublishNamespaceCancel);
}

void MoQSession::onSubscribeNamespace(SubscribeNamespace subscribeNamespace) {
  XLOG(DBG1) << __func__
             << " prefix=" << subscribeNamespace.trackNamespacePrefix
             << " - sending NOT_SUPPORTED error, sess=" << this;
  if (receivedGoaway_) {
    XLOG(DBG1) << "Rejecting subscribeNamespace request, received GOAWAY sess="
               << this;
    subscribeNamespaceError(
        SubscribeNamespaceError{
            subscribeNamespace.requestID,
            SubscribeNamespaceErrorCode::GOING_AWAY,
            "Session received GOAWAY"});
    return;
  }
  subscribeNamespaceError(
      SubscribeNamespaceError{
          subscribeNamespace.requestID,
          SubscribeNamespaceErrorCode::NOT_SUPPORTED,
          "SubscribeNamespace not supported by simple client"});
}

void MoQSession::onUnsubscribeNamespace(
    UnsubscribeNamespace unsubscribeNamespace) {
  // v15+: Use Request ID
  if (getDraftMajorVersion(*getNegotiatedVersion()) >= 15) {
    if (!unsubscribeNamespace.requestID.has_value()) {
      XLOG(ERR) << __func__ << " sess=" << this
                << " - missing requestID for v15+";
      return;
    }
    XLOG(DBG1) << __func__
               << " requestID=" << unsubscribeNamespace.requestID.value()
               << " sess=" << this;
    // TODO: Implement actual unsubscribe logic when SUBSCRIBE_NAMESPACE
    // tracking is added For now, just log and update stats
  } else {
    // <v15: Use Track Namespace Prefix
    if (!unsubscribeNamespace.trackNamespacePrefix.has_value()) {
      XLOG(ERR) << __func__ << " sess=" << this
                << " - missing trackNamespacePrefix for <v15";
      return;
    }
    XLOG(DBG1) << __func__ << " prefix="
               << unsubscribeNamespace.trackNamespacePrefix.value()
               << " - ignored by simple client, sess=" << this;
  }

  if (logger_) {
    logger_->logUnsubscribeNamespace(
        unsubscribeNamespace,
        MOQTByteStringType::STRING_VALUE,
        ControlMessageType::PARSED);
  }
  MOQ_PUBLISHER_STATS(publisherStatsCallback_, onUnsubscribeNamespace);
}

// PublishNamespace response methods
void MoQSession::publishNamespaceError(
    const PublishNamespaceError& publishNamespaceError) {
  XLOG(DBG1) << __func__ << " reqID=" << publishNamespaceError.requestID.value
             << " sess=" << this;
  MOQ_SUBSCRIBER_STATS(
      subscriberStatsCallback_,
      onPublishNamespaceError,
      publishNamespaceError.errorCode);
  auto res = moqFrameWriter_.writeRequestError(
      controlWriteBuf_,
      publishNamespaceError,
      FrameType::PUBLISH_NAMESPACE_ERROR);
  if (!res) {
    XLOG(ERR) << "writePublishNamespaceError failed sess=" << this;
    return;
  }
  if (logger_) {
    logger_->logPublishNamespaceError(publishNamespaceError);
  }
  controlWriteEvent_.signal();
}

void MoQSession::subscribeNamespaceError(
    const SubscribeNamespaceError& subscribeNamespaceError) {
  XLOG(DBG1) << __func__ << " reqID=" << subscribeNamespaceError.requestID.value
             << " sess=" << this;
  MOQ_PUBLISHER_STATS(
      publisherStatsCallback_,
      onSubscribeNamespaceError,
      subscribeNamespaceError.errorCode);
  auto res = moqFrameWriter_.writeRequestError(
      controlWriteBuf_,
      subscribeNamespaceError,
      FrameType::SUBSCRIBE_NAMESPACE_ERROR);
  if (!res) {
    XLOG(ERR) << "writeSubscribeNamespaceError failed sess=" << this;
    return;
  }
  if (logger_) {
    logger_->logSubscribeNamespaceError(subscribeNamespaceError);
  }
  controlWriteEvent_.signal();
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

void MoQSession::validateAndSetVersionFromAlpn(const std::string& alpn) {
  auto version = getVersionFromAlpn(std::string_view(alpn));
  if (version) {
    initializeNegotiatedVersion(*version);
  }
}

} // namespace moxygen

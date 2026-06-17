/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/MoQSession.h"
#include <folly/coro/Collect.h>
#include <folly/coro/FutureUtil.h>
#include <quic/common/CircularDeque.h>
#include <quic/priority/HTTPPriorityQueue.h>
#include <moxygen/BidiStreamControl.h>
#include <moxygen/MoQTrackProperties.h>
#include <moxygen/ReplyContext.h>
#include <moxygen/events/MoQDeliveryTimeoutManager.h>

#include <folly/logging/xlog.h>

#include <algorithm>
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
      bool endOfGroup,
      bool beginsWithFirstObject,
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
      bool finFetch,
      bool forwardingPreferenceIsDatagram = false) override {
    if (!setGroupAndSubgroup(groupID, subgroupID)) {
      return folly::makeUnexpected(
          MoQPublishError(MoQPublishError::API_ERROR, "Group moved back"));
    }
    header_.status = ObjectStatus::NORMAL;
    return objectImpl(
        objectID,
        std::move(payload),
        std::move(extensions),
        finFetch,
        forwardingPreferenceIsDatagram);
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
  folly::Expected<folly::Unit, MoQPublishError> objectImpl(
      uint64_t objectID,
      Payload payload,
      const Extensions& extensions,
      bool finStream,
      bool forwardingPreferenceIsDatagram);
  folly::Expected<folly::Unit, MoQPublishError> writeCurrentObject(
      uint64_t objectID,
      uint64_t length,
      Payload payload,
      const Extensions& extensions,
      bool finStream,
      bool forwardingPreferenceIsDatagram = false);
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
  moqFrameWriter_.setFetchGroupOrder(publisher->getGroupOrder());
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
    bool endOfGroup,
    bool beginsWithFirstObject,
    std::shared_ptr<MLogger> logger,
    std::shared_ptr<DeliveryCallback> deliveryCallback,
    std::optional<std::chrono::milliseconds> deliveryTimeout)
    : StreamPublisherImpl(
          publisher,
          logger,
          deliveryCallback,
          std::move(deliveryTimeout)) {
  XCHECK(writeHandle)
      << "For a SUBSCRIBE, you need to pass in a non-null writeHandle";
  // When sgPriority is none, the receiver will use the value from
  // PUBLISHER_PRIORITY, which defaults to 128 if not sent by the publisher when
  // establishing the subscription.
  streamType_ = getSubgroupStreamType(
      publisher->getVersion(),
      format,
      includeExtensions,
      endOfGroup,
      sgPriority.has_value(),
      beginsWithFirstObject);
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
      writeBuf_,
      trackAlias_,
      header_,
      format,
      includeExtensions,
      beginsWithFirstObject);
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
    bool finStream,
    bool forwardingPreferenceIsDatagram) {
  header_.id = objectID;
  header_.length = length;
  // copy is gratuitous
  header_.extensions = extensions;
  XLOG(DBG6) << "writeCurrentObject sgp=" << this << " objectID=" << objectID;
  bool entireObjectWritten = (!currentLengthRemaining_.has_value());
  (void)moqFrameWriter_.writeStreamObject(
      writeBuf_,
      streamType_,
      header_,
      std::move(payload),
      forwardingPreferenceIsDatagram);
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
  return objectImpl(
      objectID,
      std::move(payload),
      std::move(extensions),
      finStream,
      false /* forwardingPreferenceIsDatagram */);
}

folly::Expected<folly::Unit, MoQPublishError> StreamPublisherImpl::objectImpl(
    uint64_t objectID,
    Payload payload,
    const Extensions& extensions,
    bool finStream,
    bool forwardingPreferenceIsDatagram) {
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
    auto payloadClone = payload ? payload->clone() : nullptr;
    if (streamType_ != StreamType::FETCH_HEADER) {
      logger_->logSubgroupObjectCreated(
          writeHandle_->getID(), trackAlias_, header_, std::move(payloadClone));
    } else {
      logger_->logFetchObjectCreated(
          writeHandle_->getID(), header_, std::move(payloadClone));
    }
  }

  // Start delivery timeout timer when object begins being sent to stream
  if (deliveryTimer_) {
    deliveryTimer_->startTimer(objectID, publisher_->getTransportInfo().srtt);
  }

  return writeCurrentObject(
      objectID,
      length,
      std::move(payload),
      extensions,
      finStream,
      forwardingPreferenceIsDatagram);
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

namespace {

class ControlStreamReplyContext : public ReplyContext {
 public:
  ControlStreamReplyContext(
      folly::IOBufQueue& controlWriteBuf,
      moxygen::TimedBaton& controlWriteEvent,
      folly::CancellationToken token)
      : ReplyContext(std::move(token)),
        controlWriteBuf_(controlWriteBuf),
        controlWriteEvent_(controlWriteEvent) {}

  folly::IOBufQueue& writeBuf() override {
    return controlWriteBuf_;
  }
  void flush(bool /*fin*/ = false) override {
    if (!cancelled()) {
      controlWriteEvent_.signal();
    }
  }

 private:
  folly::IOBufQueue& controlWriteBuf_;
  moxygen::TimedBaton& controlWriteEvent_;
};

} // namespace

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
        co_withCancellation(
            session_->cancellationSource_.getToken(),
            folly::coro::co_invoke(
                [trackPubImpl = std::move(trackPubImpl),
                 update = std::move(
                     requestUpdate)]() mutable -> folly::coro::Task<void> {
                  co_await trackPubImpl->handleRequestUpdate(std::move(update));
                })))
        .start();
  }

  folly::coro::Task<void> handleRequestUpdate(RequestUpdate requestUpdate) {
    co_await folly::coro::co_safe_point;

    // subscriptionHandle_ may have been reset by publishDone(),
    // unsubscribe(), or terminatePublish() while this coroutine was queued.
    if (!subscriptionHandle_) {
      co_return;
    }

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

    // Call application's async requestUpdate handler with cancellation
    auto updateResult = co_await co_awaitTry(co_withCancellation(
        session_->cancellationSource_.getToken(),
        subscriptionHandle_->requestUpdate(std::move(requestUpdate))));

    // Re-check after the await — handle or session may have been reset
    // by terminatePublish while this coroutine was suspended.
    if (!subscriptionHandle_ || !session_) {
      co_return;
    }

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
        session_->requestUpdateOk(requestOk, existingRequestID);
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
      XCHECK(replyContext_);
      session->subscribeError(
          {pubDone.requestID, SubscribeErrorCode::INTERNAL_ERROR, "terminate"},
          *replyContext_);
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
  beginSubgroup(
      uint64_t groupID,
      uint64_t subgroupID,
      Priority priority,
      BeginSubgroupOptions options = {}) override;

  folly::Expected<std::shared_ptr<SubgroupConsumer>, MoQPublishError>
  beginSubgroup(
      uint64_t groupID,
      uint64_t subgroupID,
      Priority priority,
      SubgroupIDFormat format,
      bool includeExtensions,
      BeginSubgroupOptions options);

  folly::Expected<folly::SemiFuture<folly::Unit>, MoQPublishError>
  awaitStreamCredit() override;

  folly::Expected<folly::Unit, MoQPublishError> objectStream(
      const ObjectHeader& header,
      Payload payload,
      bool lastInGroup = false) override;

  folly::Expected<folly::Unit, MoQPublishError> datagram(
      const ObjectHeader& header,
      Payload payload,
      bool lastInGroup = false) override;

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
  folly::coro::Task<void> onRequestUpdate(RequestUpdate requestUpdate) {
    if (!handle_) {
      XLOG(ERR) << "Received RequestUpdate before sending FETCH_OK id="
                << requestID_ << " fetchPub=" << this;
      if (getDraftMajorVersion(*session_->getNegotiatedVersion()) >= 15) {
        session_->requestUpdateError(
            RequestError{
                requestUpdate.requestID,
                RequestErrorCode::INTERNAL_ERROR,
                "FETCH not yet initialized"},
            requestID_);
      }
      co_return;
    }

    auto existingRequestID = requestID_;
    auto updateRequestID = requestUpdate.requestID;

    // Call the handle's requestUpdate
    auto updateResult = co_await co_awaitTry(co_withCancellation(
        session_->cancellationSource_.getToken(),
        handle_->requestUpdate(std::move(requestUpdate))));

    if (!session_) {
      co_return;
    }

    // Only send responses for v15+
    if (getDraftMajorVersion(*session_->getNegotiatedVersion()) >= 15) {
      if (updateResult.hasException()) {
        XLOG(ERR) << "Exception in requestUpdate ex="
                  << updateResult.exception().what() << " fetchPub=" << this;
        session_->requestUpdateError(
            RequestError{
                updateRequestID,
                RequestErrorCode::INTERNAL_ERROR,
                "Exception in requestUpdate"},
            existingRequestID);
      } else if (updateResult->hasError()) {
        auto updateErr = updateResult->error();
        updateErr.requestID = updateRequestID; // In case app got it wrong
        session_->requestUpdateError(updateErr, existingRequestID);
      } else {
        RequestOk requestOk{.requestID = updateRequestID};
        session_->requestUpdateOk(requestOk, existingRequestID);
      }
    }
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
    Priority pubPriority,
    BeginSubgroupOptions options) {
  return beginSubgroup(
      groupID,
      subgroupID,
      pubPriority,
      options.subgroupIDFormat,
      options.includeExtensions,
      options);
}

folly::Expected<std::shared_ptr<SubgroupConsumer>, MoQPublishError>
MoQSession::TrackPublisherImpl::beginSubgroup(
    uint64_t groupID,
    uint64_t subgroupID,
    Priority pubPriority,
    SubgroupIDFormat format,
    bool includeExtensions,
    TrackConsumer::BeginSubgroupOptions options) {
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
      options.containsLastInGroup,
      options.beginsWithFirstObject,
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
           tooFarBehindCode(
               session_->getNegotiatedVersion().value_or(kVersionDraft14)),
           streamCount_,
           "peer is too far behind"}),
      ResetStreamErrorCode::TOO_FAR_BEHIND);
}

folly::Expected<folly::Unit, MoQPublishError>
MoQSession::TrackPublisherImpl::objectStream(
    const ObjectHeader& objHeader,
    Payload payload,
    bool lastInGroup) {
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
  TrackConsumer::BeginSubgroupOptions options;
  options.containsLastInGroup = lastInGroup;
  options.beginsWithFirstObject = true;
  auto subgroup = beginSubgroup(
      objHeader.group,
      objHeader.subgroup,
      *objHeader.priority,
      objHeader.subgroup == objHeader.id ? SubgroupIDFormat::FirstObject
                                         : SubgroupIDFormat::Present,
      !extensions.empty(),
      options);
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
    Payload payload,
    bool lastInGroup) {
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
  XDCHECK_EQ(headerLength, payload ? payload->computeChainDataLength() : 0);
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
      std::move(payload),
      lastInGroup);
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

  // Lets terminal-reply handlers disarm the sender close callback.
  void setBidiControl(std::shared_ptr<BidiStreamControl> control) {
    bidiControl_ = std::move(control);
  }
  const std::shared_ptr<BidiStreamControl>& bidiControl() const {
    return bidiControl_;
  }

 protected:
  FullTrackName fullTrackName_;
  RequestID requestID_;
  folly::CancellationSource cancelSource_;
  std::shared_ptr<BidiStreamControl> bidiControl_;
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
        XLOG(DBG4)
            << "deliverPublishDoneAndRemove: Delivering PUBLISH_DONE to app; statusCode="
            << folly::to_underlying(pendingPublishDone_->statusCode)
            << " alias=" << alias_ << " requestID=" << requestID_;
        MOQ_SUBSCRIBER_STATS(
            session_->subscriberStatsCallback_, onSubscriptionEnd);
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
      GroupOrder fetchGroupOrder = GroupOrder::OldestFirst,
      std::shared_ptr<MLogger> logger = nullptr)
      : TrackReceiveStateBase(std::move(fullTrackName), requestID),
        callback_(std::move(fetchCallback)),
        fetchGroupOrder_(
            fetchGroupOrder == GroupOrder::Default ? GroupOrder::OldestFirst
                                                   : fetchGroupOrder) {
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

  GroupOrder getFetchGroupOrder() const {
    return fetchGroupOrder_;
  }

  void resetFetchCallback(MoQSession* session) {
    callback_.reset();
    if (fetchOkAndAllDataReceived()) {
      // Fetch data stream closed: FIN our bidi to release it cleanly.
      if (bidiControl_) {
        bidiControl_->writeFin();
      }
      session->fetches_.erase(requestID_);
      session->checkForCloseOnDrain();
    }
  }

  void cancel(MoQSession* session) {
    cancelSource_.requestCancellation();
    // RST the bidi (e.g. fetch-stream-open timeout); peer interprets as cancel.
    if (bidiControl_) {
      bidiControl_->cancel(ResetStreamErrorCode::CANCELLED);
    }
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
  GroupOrder fetchGroupOrder_;
  folly::coro::Promise<FetchResult> promise_;
  uint64_t currentStreamId_{0};
};

const std::shared_ptr<BidiStreamControl>&
MoQSession::PendingRequestState::bidiControl() const {
  switch (type_) {
    case Type::SUBSCRIBE_TRACK:
      return storage_.subscribeTrack_->bidiControl();
    case Type::FETCH:
      return storage_.fetchTrack_->bidiControl();
    default:
      return bidiControl_;
  }
}

folly::Expected<MoQSession::PendingRequestState::Type, folly::Unit>
MoQSession::PendingRequestState::setError(
    RequestError error,
    FrameType frameType) {
  switch (frameType) {
    case FrameType::REQUEST_ERROR: {
      // REQUEST_ERROR == SUBSCRIBE_ERROR (both are 5), so check type
      if (type_ == Type::REQUEST_UPDATE) {
        // This is a REQUEST_ERROR for a REQUEST_UPDATE
        storage_.requestUpdate_.setValue(
            folly::makeUnexpected(
                RequestError{
                    error.requestID, error.errorCode, error.reasonPhrase}));
        return type_;
      }
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
      storage_.trackStatus_.setValue(folly::makeUnexpected(std::move(error)));
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

class MoQSession::GoawayTimeoutCallback : public quic::QuicTimerCallback {
 public:
  explicit GoawayTimeoutCallback(MoQSession& session) : session_(session) {}

  void timeoutExpired() noexcept override {
    auto weakSession = session_.weak_from_this();
    session_.exec_->add([weakSession = std::move(weakSession)]() mutable {
      if (auto session = weakSession.lock()) {
        session->onGoawayTimeoutExpired();
      }
    });
  }

  void callbackCanceled() noexcept override {
    XLOG(DBG4) << "GOAWAY timeout canceled sess=" << &session_;
  }

 private:
  MoQSession& session_;
};

// Constructors
MoQSession::MoQSession(
    folly::MaybeManagedPtr<proxygen::WebTransport> wt,
    std::shared_ptr<MoQExecutor> exec)
    : dir_(MoQControlCodec::Direction::CLIENT),
      wt_(std::move(wt)),
      exec_(std::move(exec)),
      controlCodec_(std::make_unique<MoQControlCodec>(dir_, this)),
      nextRequestID_(0),

      nextExpectedPeerRequestID_(1),
      nextPeerRequestIDForGoaway_(1) {}

MoQSession::MoQSession(
    folly::MaybeManagedPtr<proxygen::WebTransport> wt,
    ServerSetupCallback& serverSetupCallback,
    std::shared_ptr<MoQExecutor> exec)
    : dir_(MoQControlCodec::Direction::SERVER),
      wt_(std::move(wt)),
      exec_(std::move(exec)),
      controlCodec_(std::make_unique<MoQControlCodec>(dir_, this)),
      nextRequestID_(1),
      nextExpectedPeerRequestID_(0),
      nextPeerRequestIDForGoaway_(0),
      serverSetupCallback_(&serverSetupCallback) {}

MoQSession::~MoQSession() {
  cleanup();
  if (logger_) {
    logger_->outputLogs();
  }
  XLOG(DBG1) << __func__ << " sess=" << this;
}

void MoQSession::cleanup() {
  cancelGoawayTimeout();
  // Each loop disarms its entry's bidi control before tearing it down so a
  // peer close mid-shutdown can't race the canonical error delivery.
  // fetches_ has no per-entry destroy loop, so it needs an upfront pass.
  for (auto& [reqID, fetch] : fetches_) {
    if (const auto& control = fetch->bidiControl()) {
      control->disarmOnPeerTermination();
    }
  }
  while (!pubTracks_.empty()) {
    auto it = pubTracks_.begin();
    auto requestID = it->first;
    auto pubTrack = std::move(it->second);
    pubTracks_.erase(it);
    if (const auto& control = pubTrack->bidiControl()) {
      control->disarmOnPeerTermination();
    }
    MOQ_PUBLISHER_STATS(publisherStatsCallback_, onSubscriptionEnd);
    pubTrack->terminatePublish(
        PublishDone(
            {requestID,
             PublishDoneStatusCode::SESSION_CLOSED,
             0,
             "Session Closed"}),
        ResetStreamErrorCode::SESSION_CLOSED);
  }
  for (auto it = subTracks_.begin(); it != subTracks_.end();) {
    auto sub = it->second;
    ++it;
    if (const auto& control = sub->bidiControl()) {
      control->disarmOnPeerTermination();
    }
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
  for (auto& [reqID, pendingState] : pendingRequests_) {
    if (const auto& control = pendingState->bidiControl()) {
      control->disarmOnPeerTermination();
    }
    pendingState->setError(
        RequestError{reqID, RequestErrorCode::INTERNAL_ERROR, "Session closed"},
        pendingState->getErrorFrameType());
  }
  pendingRequests_.clear();
  pendingPublishTracks_.clear();
  pendingSubscribeTracks_.clear();
  if (!cancellationSource_.isCancellationRequested()) {
    XLOG(DBG1) << "requestCancellation from cleanup sess=" << this;
    cancellationSource_.requestCancellation();
  }
  bufferedDatagrams_.clear();
  // Break any shared_ptr cycle between the session and its handlers.  A
  // handler that holds a shared_ptr<MoQSession> back to this session would
  // otherwise keep both objects alive indefinitely once all external
  // references are released.
  publishHandler_.reset();
  subscribeHandler_.reset();
}

const folly::RequestToken& MoQSession::sessionRequestToken() {
  static folly::RequestToken token("moq_session");
  return token;
}

void MoQSession::startControlWriteLoop(
    proxygen::WebTransport::StreamWriteHandle* writeHandle) {
  writeHandle->setPriority(quic::HTTPPriorityQueue::Priority(0, false, 0));
  if (logger_) {
    logger_->logStreamTypeSet(
        writeHandle->getID(), MOQTStreamType::CONTROL, Owner::LOCAL);
  }
  auto mergeToken = folly::cancellation_token_merge(
      cancellationSource_.getToken(), writeHandle->getCancelToken());
  co_withExecutor(
      exec_.get(),
      co_withCancellation(std::move(mergeToken), controlWriteLoop(writeHandle)))
      .start();
}

void MoQSession::start() {
  XLOG(DBG1) << __func__ << " sess=" << this;
  if (negotiatedVersion_ && useUniControlStreams(*negotiatedVersion_)) {
    // Uni control mode: both client and server open a uni stream for outgoing
    // control messages
    auto us = wt_->createUniStream();
    if (!us) {
      XLOG(ERR) << "Failed to get uni control stream sess=" << this;
      wt_->closeSession();
      return;
    }
    startControlWriteLoop(us.value());
    // controlReadLoop starts when the peer's uni stream arrives via
    // onNewUniStream -> handlePreSetupUniStream
  } else if (dir_ == MoQControlCodec::Direction::CLIENT) {
    auto cs = wt_->createBidiStream();
    if (!cs) {
      XLOG(ERR) << "Failed to get control stream sess=" << this;
      wt_->closeSession();
      return;
    }
    auto controlStream = cs.value();
    startControlWriteLoop(controlStream.writeHandle);

    proxygen::WebTransport::StreamData streamData{nullptr, false};
    co_withExecutor(
        exec_.get(),
        co_withCancellation(
            cancellationSource_.getToken(),
            controlReadLoop(controlStream.readHandle, std::move(streamData))))
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
    if (!moqFrameWriter_.getVersion().has_value()) {
      close(SessionCloseErrorCode::NO_ERROR);
      return;
    }
    const bool draft18OrLater =
        getDraftMajorVersion(*moqFrameWriter_.getVersion()) >= 18;
    if (draft18OrLater) {
      goaway.requestID = RequestID(nextPeerRequestIDForGoaway_);
    }
    if (logger_) {
      logger_->logGoaway(goaway);
    }
    auto res = moqFrameWriter_.writeGoaway(controlWriteBuf_, goaway);
    if (!res) {
      XLOG(ERR) << "writeGoaway failed sess=" << this;
      return;
    }
    if (draft18OrLater && goaway.timeout > 0) {
      scheduleGoawayTimeout(goaway.timeout);
    }
    controlWriteEvent_.signal();
    drain();
  }
}

void MoQSession::checkForCloseOnDrain() {
  if (draining_ && !hasOpenRequestsForDrain()) {
    close(SessionCloseErrorCode::NO_ERROR);
  }
}

void MoQSession::scheduleGoawayTimeout(uint64_t timeoutMs) {
  cancelGoawayTimeout();
  goawayTimeout_ = std::make_unique<GoawayTimeoutCallback>(*this);
  auto* callback = goawayTimeout_.get();
  const auto maxTimeout = std::chrono::milliseconds::max();
  const auto timeout = timeoutMs > static_cast<uint64_t>(maxTimeout.count())
      ? maxTimeout
      : std::chrono::milliseconds(timeoutMs);
  XLOG(DBG1) << "Scheduling GOAWAY timeout timeoutMs=" << timeout.count()
             << " sess=" << this;
  exec_->scheduleTimeout(callback, timeout);
}

void MoQSession::cancelGoawayTimeout() {
  if (goawayTimeout_) {
    goawayTimeout_->cancelTimerCallback();
    goawayTimeout_.reset();
  }
}

void MoQSession::onGoawayTimeoutExpired() {
  if (closed_ || !draining_) {
    return;
  }
  if (!hasOpenRequestsForGoaway()) {
    checkForCloseOnDrain();
    return;
  }
  XLOG(DBG1) << "GOAWAY timeout expired with open requests sess=" << this;
  close(SessionCloseErrorCode::GOAWAY_TIMEOUT);
}

bool MoQSession::hasOpenRequestsForDrain() const {
  if (negotiatedVersion_ && getDraftMajorVersion(*negotiatedVersion_) >= 18) {
    return hasOpenRequestsForGoaway();
  }
  return !fetches_.empty() || !subTracks_.empty();
}

bool MoQSession::hasOpenRequestsForGoaway() const {
  return !pubTracks_.empty() || !fetches_.empty() || !subTracks_.empty();
}

void MoQSession::close(
    SessionCloseErrorCode error,
    folly::Optional<uint32_t> wtError) {
  XLOG(DBG1) << __func__ << " sess=" << this;
  if (closed_) {
    return;
  }
  closed_ = true;
  if (closeCallback_) {
    XLOG(DBG1) << "Calling close callback";
    closeCallback_->onMoQSessionClosed(error, wtError);
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

folly::coro::Task<Setup> MoQSession::setup(Setup setup) {
  auto res = sendSetup(std::move(setup));
  if (res.hasError()) {
    throw std::runtime_error("Failed to write setup");
  }
  return awaitPeerSetup();
}

folly::Expected<folly::Unit, quic::TransportErrorCode> MoQSession::sendSetup(
    Setup setup) {
  XLOG(DBG1) << __func__ << " sess=" << this;

  // Server may also call awaitPeerSetup() after sending SERVER_SETUP
  // proactively, to negotiate setup parameters with the client.
  std::tie(setupPromise_, setupFuture_) =
      folly::coro::makePromiseContract<Setup>();

  auto maxRequestID = getMaxRequestIDIfPresent(setup.params);

  setup.params.insertParam(SetupParameter(
      {folly::to_underlying(SetupKey::MOQT_IMPLEMENTATION),
       getMoQTImplementationString()}));

  uint64_t setupSerializationVersion = kVersionDraft14;
  if (negotiatedVersion_) {
    setupSerializationVersion = *negotiatedVersion_;
  }

  bool isClient = (dir_ == MoQControlCodec::Direction::CLIENT);
  if (isClient) {
    // Set authority/path from CLIENT_SETUP params if present
    auto setupAuthority = getFirstStringParam(
        setup.params, folly::to_underlying(SetupKey::AUTHORITY));
    if (!setupAuthority.empty()) {
      authority_ = std::move(setupAuthority);
    }
    auto setupPath =
        getFirstStringParam(setup.params, folly::to_underlying(SetupKey::PATH));
    if (!setupPath.empty()) {
      path_ = std::move(setupPath);
    }
  }
  // Set up the shared receive-side token cache and point the control codec
  // at it. The cache is necessarily empty at this point.
  receiveTokenCache_.setMaxSize(
      getMaxAuthTokenCacheSizeIfPresent(
          setup.params, setupSerializationVersion),
      /*evict=*/!isClient);
  controlCodec_->setTokenCache(&receiveTokenCache_);
  // Optimistically registers params without knowing peer's capabilities
  aliasifyAuthTokens(setup.params, setupSerializationVersion);
  auto res =
      writeSetup(controlWriteBuf_, setup, setupSerializationVersion, isClient);
  if (res.hasError()) {
    XLOG(ERR) << "writeSetup failed sess=" << this;
    return folly::makeUnexpected(res.error());
  }
  initLocalMaxRequestID(maxRequestID);
  controlWriteEvent_.signal();
  return folly::unit;
}

void MoQSession::initLocalMaxRequestID(uint64_t fromParam) {
  if (negotiatedVersion_ && useBidiRequestStreams(*negotiatedVersion_)) {
    maxRequestID_ = std::numeric_limits<uint64_t>::max();
    maxConcurrentRequests_ = std::numeric_limits<uint64_t>::max();
  } else {
    maxRequestID_ = fromParam;
    maxConcurrentRequests_ = maxRequestID_ / getRequestIDMultiplier();
  }
}

void MoQSession::initPeerMaxRequestID(const Parameters& peerParams) {
  if (negotiatedVersion_ && useBidiRequestStreams(*negotiatedVersion_)) {
    peerMaxRequestID_ = std::numeric_limits<uint64_t>::max();
  } else {
    peerMaxRequestID_ = getMaxRequestIDIfPresent(peerParams);
  }
}

folly::coro::Task<Setup> MoQSession::awaitPeerSetup() {
  XLOG(DBG1) << __func__ << " sess=" << this;
  auto deletedToken = cancellationSource_.getToken();
  auto token = co_await folly::coro::co_current_cancellation_token;
  auto mergeToken = folly::cancellation_token_merge(deletedToken, token);
  auto serverSetup = co_await co_awaitTry(co_withCancellation(
      mergeToken,
      folly::coro::timeout(
          std::move(setupFuture_), moqSettings_.setupTimeout)));
  if (mergeToken.isCancellationRequested()) {
    co_yield folly::coro::co_stopped_may_throw;
  }
  if (serverSetup.hasException()) {
    close(SessionCloseErrorCode::VERSION_NEGOTIATION_FAILED);
    XLOG(ERR) << "Setup Failed: "
              << folly::exceptionStr(serverSetup.exception());
    co_yield folly::coro::co_error(serverSetup.exception());
  }
  // The framer/codec only invokes onServerSetup in legacy mode after
  // validating the server selected draft-14.

  setupComplete_ = true;
  auto negotiatedVersion = *getNegotiatedVersion();
  XLOG(DBG1) << "Negotiated Version=0x" << std::hex << negotiatedVersion
             << std::dec << " (draft-"
             << getDraftMajorVersion(negotiatedVersion) << ")";

  replayBufferedUniStreams();

  co_return *serverSetup;
}

void MoQSession::onServerSetup(Setup serverSetup) {
  XCHECK(dir_ == MoQControlCodec::Direction::CLIENT);
  XLOG(DBG1) << __func__ << " sess=" << this;

  if (logger_) {
    logger_->logServerSetup(
        serverSetup,
        negotiatedVersion_.value_or(kVersionDraft14),
        ControlMessageType::PARSED);
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
    initializeNegotiatedVersion(kVersionDraft14);
  }

  initPeerMaxRequestID(serverSetup.params);
  auto peerAuthCacheSize = getMaxAuthTokenCacheSizeIfPresent(
      serverSetup.params, *getNegotiatedVersion());
  tokenCache_.setMaxSize(
      std::min(kMaxSendTokenCacheSize, peerAuthCacheSize),
      /*evict=*/true);
  setupPromise_.setValue(std::move(serverSetup));
}

void MoQSession::onClientSetup(Setup clientSetup) {
  XCHECK(dir_ == MoQControlCodec::Direction::SERVER);
  XLOG(DBG1) << __func__ << " sess=" << this;

  if (logger_) {
    logger_->logClientSetup(
        clientSetup,
        negotiatedVersion_.value_or(kVersionDraft14),
        ControlMessageType::PARSED);
  }

  auto moqtImplementation = getMoQTImplementationIfPresent(clientSetup.params);
  if (moqtImplementation) {
    XLOG(DBG1) << "Client MOQT_IMPLEMENTATION=" << *moqtImplementation
               << " sess=" << this;
  }

  initPeerMaxRequestID(clientSetup.params);
  auto peerAuthCacheSize = getMaxAuthTokenCacheSizeIfPresent(
      clientSetup.params, negotiatedVersion_.value_or(kVersionDraft14));
  tokenCache_.setMaxSize(
      std::min(kMaxSendTokenCacheSize, peerAuthCacheSize),
      /*evict=*/true);

  auto clientAuthority = getFirstStringParam(
      clientSetup.params, folly::to_underlying(SetupKey::AUTHORITY));
  if (!clientAuthority.empty()) {
    if (!authority_.empty()) {
      XLOG(ERR) << "AUTHORITY in CLIENT_SETUP conflicts with pre-set authority"
                << " sess=" << this;
      close(SessionCloseErrorCode::PROTOCOL_VIOLATION);
      return;
    }
    authority_ = std::move(clientAuthority);
  }

  auto clientPath = getFirstStringParam(
      clientSetup.params, folly::to_underlying(SetupKey::PATH));
  if (!clientPath.empty()) {
    if (!path_.empty()) {
      XLOG(ERR) << "PATH in CLIENT_SETUP conflicts with pre-set path"
                << " sess=" << this;
      close(SessionCloseErrorCode::PROTOCOL_VIOLATION);
      return;
    }
    path_ = std::move(clientPath);
  }

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
    // In legacy mode, always use draft-14. If the client didn't offer
    // draft-14, the framer would have returned VERSION_NEGOTIATION_FAILED.
    initializeNegotiatedVersion(kVersionDraft14);
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

  {
    auto negotiatedVersion = *getNegotiatedVersion();
    XLOG(DBG1) << "Negotiated Version=0x" << std::hex << negotiatedVersion
               << std::dec << " (draft-"
               << getDraftMajorVersion(negotiatedVersion) << ")";
  }

  bool isUniControl = useUniControlStreams(*getNegotiatedVersion());

  if (isUniControl) {
    // SERVER_SETUP was already sent proactively in start(). Fulfill the
    // setup promise so a server-side awaitPeerSetup() can return.
    // MAX_REQUEST_ID is no longer derived from setup params in draft-18+; the
    // QUIC bidi stream limit governs request flow control instead.
    setupPromise_.setValue(clientSetup);
    setupComplete_ = true;
    replayBufferedUniStreams();
  } else {
    auto res = sendSetup(std::move(*serverSetup));
    if (res.hasError()) {
      close(SessionCloseErrorCode::VERSION_NEGOTIATION_FAILED);
      return;
    }
    setupComplete_ = true;
  }
}

std::unique_ptr<MoQControlCodec> MoQSession::makeBidiCodec(
    MoQControlCodec::ControlCallback* callback,
    std::vector<FrameType> allowedFrames,
    std::optional<RequestID> requestID,
    std::optional<FrameType> okType,
    std::deque<RequestID>* responseIDQueue) {
  auto codec = std::make_unique<MoQBidiStreamCodec>(
      callback, std::move(allowedFrames), requestID, okType);
  codec->initializeVersion(*negotiatedVersion_);
  codec->setTokenCache(&receiveTokenCache_);
  codec->setResponseIDQueue(responseIDQueue);
  return codec;
}

void MoQSession::BidiRequestCallback::onConnectionError(ErrorCode error) {
  XLOG(ERR) << "Parse error=" << folly::to_underlying(error);
  session_->close(error);
}

void MoQSession::BidiRequestCallback::onRequestUpdate(
    RequestUpdate requestUpdate) {
  session_->onRequestUpdate(std::move(requestUpdate));
}

void MoQSession::BidiRequestCallback::onRequestOk(
    RequestOk requestOk,
    FrameType frameType) {
  session_->onRequestOk(std::move(requestOk), frameType);
}

void MoQSession::BidiRequestCallback::onRequestError(
    RequestError requestError,
    FrameType frameType) {
  session_->onRequestError(std::move(requestError), frameType);
}

bool MoQSession::BidiRequestCallback::handleFirstFrame(RequestID reqId) {
  if (requestID_) {
    XLOG(ERR) << "Received duplicate request on bidi stream";
    session_->close(ErrorCode::PROTOCOL_VIOLATION);
    return false;
  }
  requestID_ = reqId;
  control_->setRequestID(reqId);
  if (onPeerTerminationFn_) {
    control_->setOnPeerTermination(std::move(onPeerTerminationFn_));
  }
  replyContext_ = std::make_shared<BidiStreamReplyContext>(
      control_, session_->cancellationSource_.getToken());
  return true;
}

void MoQSession::BidiRequestCallback::onSubscribe(SubscribeRequest sub) {
  if (handleFirstFrame(sub.requestID)) {
    session_->onSubscribeImpl(std::move(sub), replyContext_);
  }
}

void MoQSession::BidiRequestCallback::onFetch(Fetch fetch) {
  if (handleFirstFrame(fetch.requestID)) {
    session_->onFetchImpl(std::move(fetch), replyContext_);
  }
}

void MoQSession::BidiRequestCallback::onPublish(PublishRequest pub) {
  if (handleFirstFrame(pub.requestID)) {
    session_->onPublishImpl(std::move(pub), replyContext_, control_);
  }
}

void MoQSession::BidiRequestCallback::onPublishDone(PublishDone publishDone) {
  session_->onPublishDone(std::move(publishDone));
}

void MoQSession::BidiRequestCallback::onPublishNamespace(
    PublishNamespace pubNs) {
  if (handleFirstFrame(pubNs.requestID)) {
    session_->onPublishNamespaceImpl(std::move(pubNs), replyContext_);
  }
}

void MoQSession::BidiRequestCallback::onTrackStatus(TrackStatus ts) {
  if (handleFirstFrame(ts.requestID)) {
    session_->onTrackStatusImpl(std::move(ts), replyContext_);
  }
}

void MoQSession::BidiRequestCallback::onSubscribeNamespace(
    SubscribeNamespace subNs) {
  if (handleFirstFrame(subNs.requestID)) {
    auto subNsReply = session_->getSubNsReply(replyContext_);
    session_->onSubscribeNamespaceImpl(subNs, std::move(subNsReply));
  }
}

void MoQSession::BidiRequestCallback::onSubscribeTracks(
    SubscribeTracks subTracks) {
  if (handleFirstFrame(subTracks.requestID)) {
    auto subTracksReply = session_->getSubTracksReply(replyContext_);
    session_->onSubscribeTracksImpl(subTracks, std::move(subTracksReply));
  }
}

folly::coro::Task<void> MoQSession::controlReadLoop(
    proxygen::WebTransport::StreamReadHandle* readHandle,
    proxygen::WebTransport::StreamData initialData,
    std::unique_ptr<MoQControlCodec> codec,
    std::unique_ptr<BidiRequestCallback> bidiCallback,
    std::shared_ptr<BidiStreamControl> control,
    std::unique_ptr<MoQControlCodec::ControlCallback> senderCallback) {
  XLOG(DBG1) << __func__ << " sess=" << this;
  auto g = folly::makeGuard([func = __func__, this] {
    XLOG(DBG1) << "exit " << func << " sess=" << this;
  });
  co_await folly::coro::co_safe_point;
  auto* controlCodec = codec ? codec.get() : controlCodec_.get();
  auto streamId = readHandle->getID();
  controlCodec->setStreamId(streamId);
  // Null readHandle on peer cancel so the exit guard skips it.
  folly::CancellationCallback rhCancelCb(
      readHandle->getCancelToken(), [&readHandle]() { readHandle = nullptr; });
  auto stopSendingGuard = folly::makeGuard([&readHandle, &control, this] {
    if (readHandle) {
      uint32_t code = control ? control->readCancelCode() : 0;
      XLOG(DBG1) << "Sending STOP_SENDING id=" << readHandle->getID()
                 << " code=" << code << " sess=" << this;
      readHandle->stopSending(code);
      readHandle = nullptr;
    }
  });

  // Process any pre-buffered data first
  if (initialData.data || initialData.fin) {
    try {
      auto guard = shared_from_this();
      controlCodec->onIngress(std::move(initialData.data), initialData.fin);
    } catch (const std::exception& ex) {
      XLOG(FATAL) << "exception thrown from onIngress ex="
                  << folly::exceptionStr(ex);
    }
  }

  bool fin = false;
  bool exceptionalExit = false;
  auto token = co_await folly::coro::co_current_cancellation_token;
  while (!fin && readHandle && !token.isCancellationRequested()) {
    auto streamData =
        co_await co_awaitTry(readHandle->readStreamData().via(exec_.get()));
    if (streamData.hasException()) {
      XLOG(DBG4) << folly::exceptionStr(streamData.exception())
                 << " id=" << streamId << " sess=" << this;
      // Defensive null in case rhCancelCb hasn't fired yet.
      readHandle = nullptr;
      exceptionalExit = true;
      break;
    }
    if (!token.isCancellationRequested() &&
        (streamData->data || streamData->fin)) {
      try {
        auto guard = shared_from_this();
        controlCodec->onIngress(std::move(streamData->data), streamData->fin);
      } catch (const std::exception& ex) {
        XLOG(FATAL) << "exception thrown from onIngress ex="
                    << folly::exceptionStr(ex);
      }
    }
    fin = streamData->fin;
    if (fin) {
      // FIN invalidates the read handle.
      readHandle = nullptr;
      XLOG(DBG3) << "End of stream id=" << streamId << " sess=" << this;
    }
  }
  // TODO: close session on control exit.
  // On read-loop exit, fire responder close (RST always; FIN when
  // finIsCancellation) and fail any still-pending sender request. Both
  // suppressed during shutdown — cleanup() delivers the canonical error.
  // STOP_SENDING is handled via BidiStreamControl::writeCancelCb_.
  if (control && !token.isCancellationRequested() &&
      !cancellationSource_.isCancellationRequested() &&
      (exceptionalExit || fin)) {
    if (exceptionalExit || control->finIsCancellation()) {
      control->firePeerTermination();
    }
    if (control->requestID().has_value()) {
      // No-op if the terminal reply already resolved + erased the pending.
      failPendingRequestOnEarlyClose(*control->requestID(), exceptionalExit);
    }
    // Sender (bidiCallback==null) mirrors peer FIN with our FIN: "done
    // updating the request". No-op if write half already closed.
    if (!bidiCallback && fin) {
      control->writeFin();
    }
  }
}

void MoQSession::failPendingRequestOnEarlyClose(
    RequestID requestID,
    bool wasReset) {
  auto it = pendingRequests_.find(requestID);
  if (it == pendingRequests_.end()) {
    return;
  }
  auto pendingState = std::move(it->second);
  pendingRequests_.erase(it);
  auto frameType = pendingState->getErrorFrameType();
  XLOG(DBG1) << "Failing pending request id=" << requestID
             << (wasReset ? " peer reset request stream"
                          : " peer FINed without terminal reply")
             << " sess=" << this;
  auto res = pendingState->setError(
      RequestError{
          requestID,
          RequestErrorCode::INTERNAL_ERROR,
          wasReset ? "peer reset request stream"
                   : "peer FINed without terminal reply"},
      frameType);
  if (res.hasError()) {
    XLOG(ERR) << "setError failure id=" << requestID << " sess=" << this;
  }
}

folly::Expected<std::shared_ptr<BidiStreamControl>, std::string>
MoQSession::sendRequest(
    folly::IOBufQueue& writeBuf,
    FrameType okType,
    std::vector<FrameType> postTerminal,
    RequestID requestID,
    uint64_t minBidiDraftVersion,
    std::unique_ptr<MoQControlCodec::ControlCallback> senderCallback,
    folly::Function<void(RequestID)> onPeerTermination) {
  if (getDraftMajorVersion(*negotiatedVersion_) >= minBidiDraftVersion) {
    auto bidiStream = wt_->createBidiStream();
    if (!bidiStream) {
      XLOG(ERR) << "Failed to create bidi stream sess=" << this;
      return folly::makeUnexpected(std::string("Failed to create bidi stream"));
    }
    bidiStream->writeHandle->writeStreamData(
        writeBuf.move(), /*fin=*/false, nullptr);
    auto* cb = senderCallback ? senderCallback.get() : this;
    // Any peer close (FIN or RST) before the terminal reply also fails the
    // pending request via failPendingRequestOnEarlyClose in controlReadLoop.
    auto control = std::make_shared<BidiStreamControl>(
        bidiStream->writeHandle,
        cancellationSource_.getToken(),
        /*finIsCancellation=*/true);
    control->setRequestID(requestID);
    auto codec = makeBidiCodec(
        cb,
        std::move(postTerminal),
        requestID,
        okType,
        &control->responseIDQueue());
    if (onPeerTermination) {
      control->setOnPeerTermination(std::move(onPeerTermination));
    }
    auto mergedToken = folly::cancellation_token_merge(
        cancellationSource_.getToken(), control->getReadCancelToken());
    co_withExecutor(
        exec_.get(),
        co_withCancellation(
            std::move(mergedToken),
            controlReadLoop(
                bidiStream->readHandle,
                proxygen::WebTransport::StreamData{nullptr, false},
                std::move(codec),
                nullptr,
                control,
                std::move(senderCallback))))
        .start();
    return control;
  }
  controlWriteBuf_.append(writeBuf.move());
  controlWriteEvent_.signal();
  return std::shared_ptr<BidiStreamControl>{};
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
    TrackConsumer::BeginSubgroupOptions beginOptions;
    beginOptions.containsLastInGroup = options.hasEndOfGroup;
    beginOptions.beginsWithFirstObject = options.beginsWithFirstObject;
    beginOptions.subgroupIDFormat = options.subgroupIDFormat;
    beginOptions.includeExtensions = options.hasExtensions;
    auto res = callback->beginSubgroup(
        group, subgroup, effectivePriority, beginOptions);
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
      bool streamComplete,
      bool forwardingPreferenceIsDatagram = false) override {
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
      obj.forwardingPreferenceIsDatagram = forwardingPreferenceIsDatagram;
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
      // Handle fetch and subscribe consumers differently due to different
      // signatures for FetchConsumer::object (has
      // forwardingPreferenceIsDatagram)
      if (fetchState_) {
        res = fetchState_->getFetchCallback()->object(
            group,
            subgroup,
            objectID,
            std::move(initialPayload),
            std::move(extensions),
            streamComplete,
            forwardingPreferenceIsDatagram);
      } else {
        res = subgroupCallback_->object(
            objectID,
            std::move(initialPayload),
            std::move(extensions),
            streamComplete);
      }
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
    auto toParseResult =
        [](const folly::Expected<folly::Unit, MoQPublishError>& r) {
          return r ? MoQCodec::ParseResult::CONTINUE
                   : MoQCodec::ParseResult::ERROR_TERMINATE;
        };
    // Handle subscription/fetch consumers
    switch (status) {
      case ObjectStatus::END_OF_GROUP:
        // FetchConsumer::endOfGroup has an optional param
        if (fetchState_) {
          return toParseResult(fetchState_->getFetchCallback()->endOfGroup(
              group,
              subgroup,
              objectID,
              /*finFetch=*/false));
        } else {
          auto r = subgroupCallback_->endOfGroup(objectID);
          endOfSubgroup();
          return toParseResult(r);
        }
      case ObjectStatus::END_OF_TRACK: {
        auto r = invokeCallback(
            &SubgroupConsumer::endOfTrackAndGroup,
            &FetchConsumer::endOfTrackAndGroup,
            group,
            subgroup,
            objectID);
        endOfSubgroup();
        return toParseResult(r);
      }
      case ObjectStatus::NORMAL:
      default:
        break;
    }
    return MoQCodec::ParseResult::CONTINUE;
  }

  MoQCodec::ParseResult onEndOfRange(
      uint64_t groupId,
      uint64_t objectId,
      bool isUnknownOrNonexistent) override {
    // For non-existent range (0x8C), just continue - the next object()
    // call will implicitly tell us where we are.
    if (!isUnknownOrNonexistent) {
      return MoQCodec::ParseResult::CONTINUE;
    }

    // For unknown range (0x10C), forward to FetchConsumer
    if (!fetchState_) {
      XLOG(ERR) << "onEndOfRange called without fetchState";
      return MoQCodec::ParseResult::ERROR_TERMINATE;
    }

    auto fetchCallback = fetchState_->getFetchCallback();
    if (!fetchCallback) {
      XLOG(ERR) << "onEndOfRange: no fetch callback";
      return MoQCodec::ParseResult::ERROR_TERMINATE;
    }

    auto res = fetchCallback->endOfUnknownRange(groupId, objectId);
    if (res.hasError()) {
      XLOG(ERR) << "onEndOfRange: callback error: " << res.error().what();
      return MoQCodec::ParseResult::ERROR_TERMINATE;
    }

    return MoQCodec::ParseResult::CONTINUE;
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

folly::coro::Task<void> MoQSession::dataStreamReadLoop(
    std::shared_ptr<MoQSession> session,
    proxygen::WebTransport::StreamReadHandle* readHandle,
    proxygen::WebTransport::StreamData initialBufferedData) {
  auto streamData = std::move(initialBufferedData);
  co_await folly::coro::co_safe_point;
  auto id = readHandle->getID();
  auto rhToken = readHandle->getCancelToken();
  XLOG(DBG1) << __func__ << " id=" << id << " sess=" << this;
  bool isSubscriptionStream = false;
  auto g = folly::makeGuard([func = __func__, this, id, &isSubscriptionStream] {
    XLOG(DBG1) << "exit " << func << " id=" << id << " sess=" << this;
    if (isSubscriptionStream) {
      onSubscriptionStreamClosedByPeer();
    }
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
                         &deferredOptions,
                         &isSubscriptionStream](
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
    if (!isSubscriptionStream && state->getSubscribeCallback()) {
      isSubscriptionStream = true;
      onSubscriptionStreamOpenedByPeer();
    }
    return state;
  };

  // Lambda for onFetch
  auto onFetchFunc = [this, &token, &codec](RequestID requestID) {
    auto state = getFetchTrackReceiveState(requestID);
    if (state) {
      // FetchTrackReceiveState lifecycle now controls read loop
      token = state->getCancelToken();
      codec.setFetchGroupOrder(state->getFetchGroupOrder());
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
    // First iteration may use initialBufferedData; subsequent iterations
    // read from the stream
    if (!(streamData.data || streamData.fin)) {
      auto streamDataTry = co_await co_awaitTry(
          folly::coro::co_withCancellation(
              token, readHandle->readStreamData().via(exec_.get())));
      if (streamDataTry.hasException()) {
        XLOG(ERR) << folly::exceptionStr(streamDataTry.exception())
                  << " id=" << id << " sess=" << this;
        ResetStreamErrorCode errorCode{ResetStreamErrorCode::INTERNAL_ERROR};
        auto wtEx =
            streamDataTry
                .tryGetExceptionObject<proxygen::WebTransport::Exception>();
        if (wtEx) {
          errorCode = ResetStreamErrorCode(wtEx->error);
        }
        if (!dcb.reset(errorCode)) {
          XLOG(ERR) << __func__ << " terminating for unknown "
                    << "stream id=" << id << " sess=" << this;
        }
        break;
      }
      streamData = std::move(*streamDataTry);
    }
    if (streamData.data || streamData.fin) {
      MoQCodec::ParseResult result = MoQCodec::ParseResult::ERROR_TERMINATE;
      try {
        result = codec.onIngress(std::move(streamData.data), streamData.fin);

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
            result = codec.onIngress(nullptr, streamData.fin);
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

      if (streamData.fin) {
        XLOG(DBG3) << "End of stream id=" << id << " sess=" << this;
        readHandle = nullptr;
      } else if (result == MoQCodec::ParseResult::ERROR_TERMINATE) {
        XLOG(ERR) << "Error parsing/consuming stream id=" << id
                  << " sess=" << this;
        dcb.reset(ResetStreamErrorCode::INTERNAL_ERROR);
        break;
      }
    } // else empty read
  }
  // stopSendingGuard will handle stopSending if needed
}

void MoQSession::onSubscribe(SubscribeRequest subscribeRequest) {
  onSubscribeImpl(std::move(subscribeRequest), controlStreamReplyContext());
}

void MoQSession::onSubscribeImpl(
    SubscribeRequest subscribeRequest,
    std::shared_ptr<ReplyContext> replyContext) {
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
  if (shouldRejectNewPeerRequestDueToGoaway()) {
    XLOG(DBG1) << "Rejecting subscribe request, GOAWAY/draining sess=" << this;
    subscribeError(
        {subscribeRequest.requestID,
         SubscribeErrorCode::GOING_AWAY,
         "Session going away"},
        *replyContext);
    return;
  }
  if (!publishHandler_) {
    XLOG(DBG1) << __func__ << " No publisher callback set";
    subscribeError(
        {subscribeRequest.requestID,
         SubscribeErrorCode::INTERNAL_ERROR,
         "No publisher callback set"},
        *replyContext);
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
         "dup sub ID"},
        *replyContext);
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
  trackPublisher->setReplyContext(replyContext);
  if (logger_) {
    trackPublisher->setLogger(logger_);
  }

  pubTracks_.emplace(requestID, trackPublisher);

  // Check for duplicate subscription: if there's already a pending outgoing
  // PUBLISH for the same track, reject the SUBSCRIBE
  if (pendingPublishTracks_.count(subscribeRequest.fullTrackName)) {
    XLOG(DBG1) << "Duplicate subscription for track with pending publish"
               << " ftn=" << subscribeRequest.fullTrackName << " sess=" << this;
    subscribeError(
        {subscribeRequest.requestID,
         SubscribeErrorCode::DUPLICATE_SUBSCRIPTION,
         "duplicate subscription"},
        *replyContext);
    return;
  }

  // TODO: there should be a timeout for the application to call
  // subscribeOK/Error
  co_withExecutor(
      exec_.get(),
      co_withCancellation(
          cancellationSource_.getToken(),
          handleSubscribe(
              std::move(subscribeRequest),
              std::move(trackPublisher),
              std::move(replyContext))))
      .start();
}

folly::coro::Task<void> MoQSession::handleSubscribe(
    SubscribeRequest sub,
    std::shared_ptr<TrackPublisherImpl> trackPublisher,
    std::shared_ptr<ReplyContext> replyContext) {
  co_await folly::coro::co_safe_point;
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
         subscribeResult.exception().what().toStdString()},
        *replyContext);
    co_return;
  }
  if (subscribeResult->hasError()) {
    XLOG(DBG1) << "Application subscribe error err="
               << subscribeResult->error().reasonPhrase;
    auto subErr = std::move(subscribeResult->error());
    subErr.requestID = requestID; // In case app got it wrong
    subscribeError(subErr, *replyContext);
  } else {
    auto subHandle = std::move(subscribeResult->value());
    auto subOk = subHandle->subscribeOk();
    subOk.requestID = requestID;

    setPublisherPriorityFromParams(subOk.params, trackPublisher);
    trackPublisher->subscribeOkSent(subOk);

    // TODO: verify TrackAlias is unique
    sendSubscribeOk(subOk, *replyContext);
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
  auto existingRequestID = requestUpdate.existingRequestID;
  auto requestID = requestUpdate.requestID;

  if (closeSessionIfRequestIDInvalid(requestID, false, true)) {
    return;
  }

  if (logger_) {
    logger_->logSubscribeUpdate(requestUpdate, ControlMessageType::PARSED);
  }

  if (shouldRejectNewPeerRequestDueToGoaway()) {
    XLOG(DBG1) << "Rejecting request update, GOAWAY/draining sess=" << this;
    requestUpdateError(
        RequestError{
            requestID, RequestErrorCode::GOING_AWAY, "Session going away"},
        existingRequestID,
        /*terminateExistingRequest=*/false);
    return;
  }

  if (closeSessionIfRequestIDInvalid(existingRequestID, false, false, false)) {
    return;
  }

  // Inline lookup - check pubTracks_ for SUBSCRIBE or FETCH
  auto pubIt = pubTracks_.find(existingRequestID);
  if (pubIt != pubTracks_.end()) {
    if (auto trackPub =
            std::dynamic_pointer_cast<TrackPublisherImpl>(pubIt->second)) {
      handleSubscribeRequestUpdate(std::move(requestUpdate), trackPub);
      return;
    }
    // v16+: FETCH supports REQUEST_UPDATE
    if (getDraftMajorVersion(*getNegotiatedVersion()) >= 16) {
      if (auto fetchPub =
              std::dynamic_pointer_cast<FetchPublisherImpl>(pubIt->second)) {
        handleFetchRequestUpdate(requestUpdate, fetchPub);
        return;
      }
    }
  }

  if (getDraftMajorVersion(*getNegotiatedVersion()) >= 15) {
    requestUpdateError(
        SubscribeUpdateError{
            requestID,
            RequestErrorCode::NOT_SUPPORTED,
            "REQUEST_UPDATE not supported for this message type"},
        existingRequestID);
  }
}

void MoQSession::handleSubscribeRequestUpdate(
    RequestUpdate requestUpdate,
    std::shared_ptr<TrackPublisherImpl> trackPublisher) {
  if (!publishHandler_) {
    XLOG(DBG1) << __func__ << " No publisher callback set";
    return;
  }

  trackPublisher->setSubPriority(requestUpdate.priority);
  trackPublisher->onRequestUpdate(std::move(requestUpdate));
}

void MoQSession::handleFetchRequestUpdate(
    const RequestUpdate& requestUpdate,
    const std::shared_ptr<FetchPublisherImpl>& fetchPublisher) {
  XLOG(DBG1) << __func__ << " requestID=" << fetchPublisher->requestID()
             << " sess=" << this;

  if (!publishHandler_) {
    XLOG(DBG1) << __func__ << " No publisher callback set";
    return;
  }

  // Simple passthrough - just deliver to application and relay response
  co_withExecutor(
      getExecutor(),
      co_withCancellation(
          cancellationSource_.getToken(),
          folly::coro::co_invoke(
              [fetchPublisher = fetchPublisher,
               update = requestUpdate]() mutable -> folly::coro::Task<void> {
                co_await folly::coro::co_safe_point;
                co_await fetchPublisher->onRequestUpdate(std::move(update));
              })))
      .start();
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
      MOQ_PUBLISHER_STATS(publisherStatsCallback_, onSubscriptionEnd);
      retireRequestID(/*signalWriteLoop=*/true);
      checkForCloseOnDrain();
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
    pendingPublishTracks_.erase(trackIt->second->fullTrackName());
    MOQ_PUBLISHER_STATS(publisherStatsCallback_, onSubscriptionBegin);
  }
  // No disarm: PUBLISH_OK is non-terminal; publishDone's flush(fin) disarms.

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
    // Terminal reply: disarm peer-termination; FIN our write half — error
    // ends the request from the sender's side.
    if (const auto& control = pendingState->bidiControl()) {
      control->disarmOnPeerTermination();
      control->writeFin();
    }
    // Remove from pending subscribe tracks if this was a subscribe
    auto* trackPtr = pendingState->tryGetSubscribeTrack();
    if (trackPtr) {
      pendingSubscribeTracks_.erase((*trackPtr)->fullTrackName());
    }
    if (getDraftMajorVersion(*getNegotiatedVersion()) > 14) {
      // determine real frame type from pendingRequest
      frameType = pendingState->getErrorFrameType();
    }

    if (error.errorCode == RequestErrorCode::REDIRECT) {
      using PRStateType = PendingRequestState::Type;
      const auto pendingType = pendingState->type();
      const bool isNamespaceScoped =
          (pendingType == PRStateType::PUBLISH_NAMESPACE ||
           pendingType == PRStateType::SUBSCRIBE_NAMESPACE);
      const bool isRedirectable =
          (pendingType == PRStateType::SUBSCRIBE_TRACK ||
           pendingType == PRStateType::FETCH ||
           pendingType == PRStateType::TRACK_STATUS || isNamespaceScoped);
      if (!isRedirectable) {
        XLOG(ERR) << "REDIRECT not permitted for pending request type="
                  << static_cast<int>(pendingType) << " id=" << error.requestID
                  << " sess=" << this;
        close(SessionCloseErrorCode::PROTOCOL_VIOLATION);
        return;
      }
      if (error.redirect) {
        if (dir_ == MoQControlCodec::Direction::SERVER &&
            !error.redirect->connectUri.empty()) {
          XLOG(ERR) << "Server received REDIRECT with non-empty Connect URI"
                    << " id=" << error.requestID << " sess=" << this;
          close(SessionCloseErrorCode::PROTOCOL_VIOLATION);
          return;
        }
        if (isNamespaceScoped &&
            !error.redirect->fullTrackName.trackName.empty()) {
          XLOG(ERR) << "Namespace-scoped REDIRECT has non-empty Track Name"
                    << " id=" << error.requestID
                    << " pendingType=" << static_cast<int>(pendingType)
                    << " sess=" << this;
          close(SessionCloseErrorCode::PROTOCOL_VIOLATION);
          return;
        }
      }
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
        pendingPublishTracks_.erase(pubIt->second->fullTrackName());
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
  pendingSubscribeTracks_.erase(trackReceiveState->fullTrackName());

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
      std::shared_ptr<MoQSession> session,
      std::shared_ptr<BidiStreamControl> control = nullptr)
      : Publisher::SubscriptionHandle(std::move(ok)),
        trackAlias_(alias),
        session_(std::move(session)),
        control_(std::move(control)) {}

  folly::coro::Task<SubscriptionHandle::RequestUpdateResult> requestUpdate(
      RequestUpdate requestUpdate) override {
    if (!session_) {
      co_return folly::makeUnexpected(
          RequestError{
              requestUpdate.requestID,
              RequestErrorCode::INTERNAL_ERROR,
              "Session closed"});
    }

    requestUpdate.existingRequestID = subscribeOk_->requestID;
    if (session_->shouldFailNewLocalRequestDueToGoaway()) {
      co_return folly::makeUnexpected(
          RequestError{
              session_->peekNextRequestID(),
              RequestErrorCode::GOING_AWAY,
              "Session received GOAWAY"});
    }
    if (getDraftMajorVersion(*(session_->getNegotiatedVersion())) >= 14) {
      requestUpdate.requestID = session_->getNextRequestID();
    } else {
      requestUpdate.requestID = subscribeOk_->requestID;
    }

    // For v15+, create promise and wait for REQUEST_OK response
    // For v14 and below, just send the message and return immediately
    if (getDraftMajorVersion(*(session_->getNegotiatedVersion())) >= 15) {
      // Create promise/contract for tracking the response
      auto contract = folly::coro::makePromiseContract<
          folly::Expected<RequestOk, RequestError>>();

      // Register pending request
      session_->pendingRequests_.emplace(
          requestUpdate.requestID,
          PendingRequestState::makeRequestUpdate(std::move(contract.first)));

      // Send the REQUEST_UPDATE message
      session_->requestUpdate(requestUpdate, control_);

      // Wait for REQUEST_OK or REQUEST_ERROR response
      co_return co_await std::move(contract.second);
    } else {
      session_->requestUpdate(requestUpdate, control_);

      // Version < 15: Return a constructed response. RequestUpdate is fire
      // and forget
      co_return RequestOk{.requestID = requestUpdate.requestID};
    }
  }

  void unsubscribe() override {
    if (session_) {
      session_->unsubscribe({subscribeOk_->requestID}, control_);
      session_.reset();
    }
  }

 private:
  TrackAlias trackAlias_;
  std::shared_ptr<MoQSession> session_;
  std::shared_ptr<BidiStreamControl> control_;
};

void MoQSession::onPublish(PublishRequest publish) {
  onPublishImpl(std::move(publish), controlStreamReplyContext());
}

void MoQSession::onPublishImpl(
    PublishRequest publish,
    std::shared_ptr<ReplyContext> replyContext,
    std::shared_ptr<BidiStreamControl> control) {
  XLOG(DBG1) << __func__ << " reqID=" << publish.requestID << " sess=" << this;
  if (logger_) {
    logger_->logPublish(
        publish, MOQTByteStringType::STRING_VALUE, ControlMessageType::PARSED);
  }
  MOQ_SUBSCRIBER_STATS(subscriberStatsCallback_, onPublish);
  if (closeSessionIfRequestIDInvalid(publish.requestID, false, true)) {
    return;
  }
  if (shouldRejectNewPeerRequestDueToGoaway()) {
    XLOG(DBG1) << "Rejecting publish request, GOAWAY/draining sess=" << this;
    publishError(
        PublishError{
            publish.requestID,
            PublishErrorCode::GOING_AWAY,
            "Session going away"},
        *replyContext);
    return;
  }

  if (!subscribeHandler_) {
    XLOG(DBG1) << __func__ << " No subscriber callback set";
    publishError(
        PublishError{
            publish.requestID,
            PublishErrorCode::NOT_SUPPORTED,
            "Not a subscriber"},
        *replyContext);
    return;
  }

  // Check for duplicate subscription: if there's already a pending outgoing
  // SUBSCRIBE for the same track, reject the PUBLISH
  if (pendingSubscribeTracks_.count(publish.fullTrackName)) {
    XLOG(DBG1) << "Duplicate subscription for track with pending subscribe"
               << " ftn=" << publish.fullTrackName << " sess=" << this;
    publishError(
        PublishError{
            publish.requestID,
            PublishErrorCode::DUPLICATE_SUBSCRIPTION,
            "duplicate subscription"},
        *replyContext);
    return;
  }

  auto publishHandle = std::make_shared<ReceiverSubscriptionHandle>(
      SubscribeOk{publish.requestID},
      publish.trackAlias,
      shared_from_this(),
      std::move(control));

  // Use single coroutine pattern like working onPublishNamespace
  co_withExecutor(
      exec_.get(),
      co_withCancellation(
          cancellationSource_.getToken(),
          handlePublish(
              std::move(publish),
              std::move(publishHandle),
              std::move(replyContext))))
      .start();
}

folly::coro::Task<void> MoQSession::handlePublish(
    PublishRequest publish,
    std::shared_ptr<Publisher::SubscriptionHandle> publishHandle,
    std::shared_ptr<ReplyContext> replyContext) {
  co_await folly::coro::co_safe_point;
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
        publishOk(pubOk, *replyContext);
        deliverBufferedData(alias);
        co_return;
      }
    }
  } catch (const std::exception& ex) {
    XLOG(ERR) << "Exception in Subscriber publish callback ex=" << ex.what();
    publishErr.reasonPhrase = ex.what();
  }
  publishError(publishErr, *replyContext);
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
    // PUBLISH_DONE is terminal for the SUBSCRIBE bidi request stream; any
    // subsequent FIN/RST is informational. FIN our write half to close the
    // stream cleanly now that no further REQUEST_UPDATE can be sent.
    if (const auto& control = state->bidiControl()) {
      control->disarmOnPeerTermination();
      control->writeFin();
    }
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
  onFetchImpl(std::move(fetch), controlStreamReplyContext());
}

void MoQSession::onFetchImpl(
    Fetch fetch,
    std::shared_ptr<ReplyContext> replyContext) {
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
  if (shouldRejectNewPeerRequestDueToGoaway()) {
    XLOG(DBG1) << "Rejecting fetch request, GOAWAY/draining sess=" << this;
    fetchError(
        {fetch.requestID, FetchErrorCode::GOING_AWAY, "Session going away"},
        *replyContext);
    return;
  }
  if (!publishHandler_) {
    XLOG(DBG1) << __func__ << " No publisher callback set";
    fetchError(
        {fetch.requestID,
         FetchErrorCode::INTERNAL_ERROR,
         "No publisher callback set"},
        *replyContext);
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
             "End must be after start"},
            *replyContext);
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
           FetchErrorCode::INVALID_JOINING_REQUEST_ID,
           "Unknown joining requestID"},
          *replyContext);
      return;
    }
    fetch.fullTrackName = joinIt->second->fullTrackName();
  }
  auto it = pubTracks_.find(fetch.requestID);
  if (it != pubTracks_.end()) {
    XLOG(ERR) << "Duplicate subscribe ID=" << fetch.requestID
              << " sess=" << this;
    // message error
    fetchError(
        {fetch.requestID, FetchErrorCode::INTERNAL_ERROR, "dup sub ID"},
        *replyContext);
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
  // Kept for draft-18+ REQUEST_UPDATE replies on the FETCH bidi.
  fetchPublisher->setReplyContext(replyContext);
  pubTracks_.emplace(fetch.requestID, fetchPublisher);
  co_withExecutor(
      exec_.get(),
      co_withCancellation(
          cancellationSource_.getToken(),
          handleFetch(
              std::move(fetch),
              std::move(fetchPublisher),
              std::move(replyContext))))
      .start();
}

folly::coro::Task<void> MoQSession::handleFetch(
    Fetch fetch,
    std::shared_ptr<FetchPublisherImpl> fetchPublisher,
    std::shared_ptr<ReplyContext> replyContext) {
  co_await folly::coro::co_safe_point;
  folly::RequestContextScopeGuard guard;
  setRequestSession();
  auto requestID = fetch.requestID;
  if (!fetchPublisher->getStreamPublisher()) {
    XLOG(ERR) << "Fetch Publisher killed sess=" << this;
    fetchError(
        {requestID, FetchErrorCode::INTERNAL_ERROR, "Fetch Failed"},
        *replyContext);
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
         fetchResult.exception().what().toStdString()},
        *replyContext);
    co_return;
  }
  if (fetchResult->hasError()) {
    XLOG(DBG1) << "Application fetch error err="
               << fetchResult->error().reasonPhrase;
    auto fetchErr = std::move(fetchResult->error());
    fetchErr.requestID = requestID; // In case app got it wrong
    fetchError(fetchErr, *replyContext);
  } else if (!fetchPublisher->isCancelled()) {
    auto fetchHandle = std::move(fetchResult->value());
    auto fetchOkMsg = fetchHandle->fetchOk();
    fetchOkMsg.requestID = requestID;
    fetchOk(fetchOkMsg, *replyContext);
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
  // After FETCH_OK the fetch data streams drive completion; a subsequent
  // FIN/RST on the bidi is informational and must not synthesize an error.
  const auto& control = trackReceiveState->bidiControl();
  if (control) {
    control->disarmOnPeerTermination();
  }
  trackReceiveState->fetchOK(std::move(fetchOk));
  if (trackReceiveState->fetchOkAndAllDataReceived()) {
    // The data path already ran resetFetchCallback() before FETCH_OK arrived,
    // so it left without FINing. Mirror that FIN here so the bidi terminates.
    if (control) {
      control->writeFin();
    }
    fetches_.erase(fetchIt);
    checkForCloseOnDrain();
  }
}

void MoQSession::onTrackStatus(TrackStatus trackStatus) {
  onTrackStatusImpl(std::move(trackStatus), controlStreamReplyContext());
}

void MoQSession::onTrackStatusImpl(
    TrackStatus trackStatus,
    std::shared_ptr<ReplyContext> replyContext) {
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
  if (shouldRejectNewPeerRequestDueToGoaway()) {
    XLOG(DBG1) << "Rejecting track status request, GOAWAY/draining sess="
               << this;
    trackStatusError(
        {trackStatus.requestID,
         TrackStatusErrorCode::GOING_AWAY,
         "Session going away"},
        *replyContext);
    return;
  }
  if (!publishHandler_) {
    XLOG(DBG1) << __func__ << "No publisher callback set";
    trackStatusError(
        {trackStatus.requestID,
         TrackStatusErrorCode::INTERNAL_ERROR,
         "No publisher callback set"},
        *replyContext);
  } else {
    co_withExecutor(
        exec_.get(),
        co_withCancellation(
            cancellationSource_.getToken(),
            handleTrackStatus(std::move(trackStatus), std::move(replyContext))))
        .start();
  }
}

folly::coro::Task<void> MoQSession::handleTrackStatus(
    TrackStatus trackStatus,
    std::shared_ptr<ReplyContext> replyContext) {
  co_await folly::coro::co_safe_point;
  auto trackStatusResult = co_await co_awaitTry(co_withCancellation(
      cancellationSource_.getToken(),
      publishHandler_->trackStatus(trackStatus)));
  if (trackStatusResult.hasException()) {
    XLOG(ERR) << "Exception in Publisher callback ex="
              << trackStatusResult.exception().what().toStdString();
    trackStatusError(
        {trackStatus.requestID,
         TrackStatusErrorCode::INTERNAL_ERROR,
         trackStatusResult.exception().what().toStdString()},
        *replyContext);
    retireRequestID(/*signalWriteLoop=*/false);
    co_return;
  }
  if (trackStatusResult->hasError()) {
    XLOG(DBG1) << "Application track status error err="
               << trackStatusResult->error().reasonPhrase;
    auto trackStatusErr = std::move(trackStatusResult->error());
    trackStatusErr.requestID = trackStatus.requestID;
    trackStatusError(trackStatusErr, *replyContext);
  } else {
    auto trackStatOk = std::move(trackStatusResult->value());
    trackStatOk.requestID = trackStatus.requestID;
    trackStatOk.fullTrackName = trackStatus.fullTrackName;
    trackStatusOk(trackStatOk, *replyContext);
  }
  retireRequestID(/*signalWriteLoop=*/false);
}

void MoQSession::trackStatusOk(
    const TrackStatusOk& trackStatusOk,
    ReplyContext& replyContext) {
  auto res = moqFrameWriter_.writeTrackStatusOk(
      replyContext.writeBuf(), trackStatusOk);

  if (logger_) {
    logger_->logTrackStatusOk(trackStatusOk);
  }

  if (!res) {
    XLOG(ERR) << "trackStatusOk failed sess=" << this;
  } else {
    replyContext.flushFinal();
  }
}

void MoQSession::trackStatusError(
    const TrackStatusError& trackStatusError,
    ReplyContext& replyContext) {
  auto res = moqFrameWriter_.writeTrackStatusError(
      replyContext.writeBuf(), trackStatusError);

  if (logger_) {
    logger_->logTrackStatusError(trackStatusError);
  }

  if (!res) {
    XLOG(ERR) << "trackStatusError failed sess=" << this;
  } else {
    replyContext.flushFinal();
  }
}

void MoQSession::handleTrackStatusOkFromRequestOk(const RequestOk& requestOk) {
  XLOG(DBG1) << __func__ << " redId=" << requestOk.requestID
             << " sess=" << this;
  auto trackStatusOk = requestOk.toTrackStatusOk();
  onTrackStatusOk(std::move(trackStatusOk));
}

void MoQSession::handlePublishOkFromRequestOk(const RequestOk& requestOk) {
  XLOG(DBG1) << __func__ << " reqID=" << requestOk.requestID
             << " sess=" << this;
  auto majorVersion = getDraftMajorVersion(*getNegotiatedVersion());
  if (majorVersion < 18) {
    XLOG(ERR) << "PUBLISH_OK received as REQUEST_OK before draft 18 reqID="
              << requestOk.requestID << " sess=" << this;
    close(SessionCloseErrorCode::PROTOCOL_VIOLATION);
    return;
  }

  auto publishOk = requestOk.toPublishOk(majorVersion);
  if (publishOk.hasError()) {
    XLOG(ERR) << "Invalid PUBLISH_OK params in REQUEST_OK reqID="
              << requestOk.requestID << " sess=" << this;
    close(SessionCloseErrorCode::PROTOCOL_VIOLATION);
    return;
  }
  onPublishOk(std::move(publishOk.value()));
}

bool MoQSession::validateRequestOkTrackProperties(
    const RequestOk& requestOk,
    FrameType resolvedFrameType) {
  if (getDraftMajorVersion(*getNegotiatedVersion()) < 18) {
    return true;
  }
  if (resolvedFrameType == FrameType::TRACK_STATUS_OK) {
    return true;
  }
  if (requestOk.trackProperties.empty()) {
    return true;
  }
  XLOG(ERR) << "Track Properties not allowed in REQUEST_OK for frameType="
            << folly::to_underlying(resolvedFrameType) << ", sess=" << this;
  close(SessionCloseErrorCode::PROTOCOL_VIOLATION);
  return false;
}

bool MoQSession::validateRequestOkParams(
    const RequestOk& requestOk,
    FrameType resolvedFrameType) {
  if (getDraftMajorVersion(*getNegotiatedVersion()) < 18) {
    return true;
  }
  // OBJECT_DELIVERY_TIMEOUT and SUBGROUP_DELIVERY_TIMEOUT list REQUEST_OK in
  // their parse-time allowlist so a PUBLISH_OK (encoded on the wire as
  // REQUEST_OK) is accepted. Among REQUEST_OK responses they are valid only for
  // PUBLISH_OK, so once the shorthand resolves reject them for anything else.
  // This is keyed on the param (not isParamAllowed) because shorthands that
  // share the REQUEST_OK wire type -- e.g. PUBLISH_NAMESPACE_OK (also 0x7) --
  // are indistinguishable by frame type and would otherwise pass the superset.
  for (const auto& param : requestOk.params) {
    auto key = static_cast<TrackRequestParamKey>(param.key);
    if ((key == TrackRequestParamKey::OBJECT_DELIVERY_TIMEOUT ||
         key == TrackRequestParamKey::SUBGROUP_DELIVERY_TIMEOUT) &&
        resolvedFrameType != FrameType::PUBLISH_OK) {
      XLOG(ERR) << "Delivery timeout param key=" << param.key
                << " only valid for PUBLISH_OK among REQUEST_OK responses, got "
                << "resolved frameType="
                << folly::to_underlying(resolvedFrameType) << ", sess=" << this;
      close(SessionCloseErrorCode::PROTOCOL_VIOLATION);
      return false;
    }
  }
  return true;
}

void MoQSession::handleSubscribeUpdateOkFromRequestOk(
    const RequestOk& requestOk,
    PendingRequestIterator reqIt) {
  XLOG(DBG1) << __func__ << " reqID=" << requestOk.requestID
             << " sess=" << this;

  auto pendingRequest = std::move(reqIt->second);
  pendingRequests_.erase(reqIt);

  auto* promise = pendingRequest->tryGetRequestUpdate();
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
  if (draining_ || closed_) {
    XLOG(DBG1)
        << "Rejecting track status request, session draining/closed sess="
        << this;
    TrackStatusError trackStatusError{
        trackStatus.requestID,
        TrackStatusErrorCode::INTERNAL_ERROR,
        "draining/closed session"};
    co_return folly::makeUnexpected(trackStatusError);
  }
  if (shouldFailNewLocalRequestDueToGoaway()) {
    XLOG(DBG1) << "Rejecting track status request, received GOAWAY sess="
               << this;
    TrackStatusError trackStatusError{
        peekNextRequestID(),
        TrackStatusErrorCode::GOING_AWAY,
        "Session received GOAWAY"};
    co_return folly::makeUnexpected(trackStatusError);
  }
  aliasifyAuthTokens(trackStatus.params);
  trackStatus.requestID = getNextRequestID();

  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  moqFrameWriter_.writeTrackStatus(writeBuf, trackStatus);
  auto reqID = trackStatus.requestID;
  auto sendResult = sendRequest(
      writeBuf,
      FrameType::REQUEST_OK,
      /*postTerminal=*/{},
      reqID,
      /*minBidiDraftVersion=*/18,
      /*senderCallback=*/nullptr,
      // Peer close (FIN or RST) before sending a reply: synthesize
      // TRACK_STATUS_ERROR. (sender control finIsCancellation=true.)
      [this](RequestID id) {
        onTrackStatusError(
            TrackStatusError{
                id,
                TrackStatusErrorCode::CANCELLED,
                "peer closed stream before reply"});
      });
  if (sendResult.hasError()) {
    co_return folly::makeUnexpected(
        TrackStatusError{
            reqID,
            TrackStatusErrorCode::INTERNAL_ERROR,
            std::move(sendResult.error())});
  }
  auto control = std::move(sendResult.value());
  // No REQUEST_UPDATE channel for TRACK_STATUS — FIN now.
  if (control) {
    control->writeFin();
  }
  if (logger_) {
    logger_->logTrackStatus(trackStatus);
  }
  auto contract = folly::coro::makePromiseContract<
      folly::Expected<TrackStatusOk, TrackStatusError>>();
  auto pending =
      PendingRequestState::makeTrackStatus(std::move(contract.first));
  pending->setBidiControl(std::move(control));
  pendingRequests_.emplace(reqID, std::move(pending));
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

  if (const auto& control = trackStatusIt->second->bidiControl()) {
    control->disarmOnPeerTermination();
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

  if (const auto& control = trackStatusIt->second->bidiControl()) {
    control->disarmOnPeerTermination();
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
  auto negotiatedVersion = getNegotiatedVersion();
  if (negotiatedVersion && getDraftMajorVersion(*negotiatedVersion) >= 18) {
    // Wire input is rejected by parseGoaway(); keep this for synthesized
    // values.
    if (!goaway.requestID.has_value()) {
      XLOG(ERR) << "Received GOAWAY without requestID sess=" << this;
      close(SessionCloseErrorCode::PROTOCOL_VIOLATION);
      return;
    }
    const bool requestIDOdd = (goaway.requestID->value % 2) == 1;
    // Our locally initiated request IDs are odd when we are the server.
    const bool localRequestIDParityOdd =
        dir_ == MoQControlCodec::Direction::SERVER;
    if (requestIDOdd != localRequestIDParityOdd) {
      XLOG(ERR) << "Received GOAWAY with invalid requestID parity id="
                << *goaway.requestID << " sess=" << this;
      close(SessionCloseErrorCode::INVALID_REQUEST_ID);
      return;
    }
    receivedGoawayRequestID_ = goaway.requestID;
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

  // Reject new publish attempts if session is draining or closed
  if (draining_ || closed_) {
    XLOG(DBG1) << "Rejecting publish request, session draining/closed sess="
               << this;
    return folly::makeUnexpected(
        PublishError{
            pub.requestID,
            PublishErrorCode::INTERNAL_ERROR,
            "draining/closed session"});
  }
  if (shouldFailNewLocalRequestDueToGoaway()) {
    XLOG(DBG1) << "Rejecting publish request, received GOAWAY sess=" << this;
    return folly::makeUnexpected(
        PublishError{
            peekNextRequestID(),
            PublishErrorCode::GOING_AWAY,
            "Session received GOAWAY"});
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

  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  moqFrameWriter_.writePublish(writeBuf, pub);
  // Draft 18+ encodes PUBLISH_OK on the wire as REQUEST_OK (see
  // MoQFrameWriter::writePublishOk), so the codec's terminal-first-frame
  // check must look for REQUEST_OK.
  auto sendResult = sendRequest(
      writeBuf,
      FrameType::REQUEST_OK,
      /*postTerminal=*/
      {FrameType::REQUEST_UPDATE,
       FrameType::REQUEST_OK,
       FrameType::REQUEST_ERROR},
      pub.requestID,
      /*minBidiDraftVersion=*/18,
      /*senderCallback=*/nullptr,
      // Peer cancelled the PUBLISH bidi: tear down the local publisher.
      [this](RequestID id) {
        auto it = pubTracks_.find(id);
        if (it == pubTracks_.end()) {
          return;
        }
        auto trackPublisher =
            std::static_pointer_cast<TrackPublisherImpl>(it->second);
        trackPublisher->terminatePublish(
            PublishDone{
                id,
                PublishDoneStatusCode::SUBSCRIPTION_ENDED,
                0,
                "peer closed stream before reply"},
            ResetStreamErrorCode::CANCELLED);
      });
  if (sendResult.hasError()) {
    return folly::makeUnexpected(
        PublishError{
            pub.requestID,
            PublishErrorCode::INTERNAL_ERROR,
            std::move(sendResult.error())});
  }
  if (logger_) {
    logger_->logPublish(
        pub, MOQTByteStringType::STRING_VALUE, ControlMessageType::CREATED);
  }

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
  trackPublisher->setSubscriptionHandle(std::move(handle));

  // Extract PUBLISHER_PRIORITY parameter if present (version 15+)
  setPublisherPriorityFromParams(pub.params, trackPublisher);

  // Set reply context so sendPublishDone can write on the correct stream
  auto& control = sendResult.value();
  trackPublisher->setReplyContext(makeReplyContext(control));
  trackPublisher->setBidiControl(control);

  // Store the track publisher for later lookup
  pubTracks_.emplace(pub.requestID, trackPublisher);
  pendingPublishTracks_.insert(pub.fullTrackName);

  // Create Contract and place in pending publishes
  auto contract = folly::coro::makePromiseContract<
      folly::Expected<PublishOk, PublishError>>();
  auto pending = PendingRequestState::makePublish(std::move(contract.first));
  pending->setBidiControl(control);
  pendingRequests_.emplace(pub.requestID, std::move(pending));

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

void MoQSession::publishOk(const PublishOk& pubOk, ReplyContext& replyContext) {
  XLOG(DBG1) << __func__ << " reqID=" << pubOk.requestID << " sess=" << this;
  MOQ_SUBSCRIBER_STATS(subscriberStatsCallback_, onPublishOk);
  MOQ_SUBSCRIBER_STATS(subscriberStatsCallback_, onSubscriptionBegin);

  if (logger_) {
    logger_->logPublishOk(pubOk, ControlMessageType::CREATED);
  }

  auto res = moqFrameWriter_.writePublishOk(replyContext.writeBuf(), pubOk);
  if (!res) {
    XLOG(ERR) << "writePublishOk failed sess=" << this;
    return;
  }
  replyContext.flush();
}

void MoQSession::publishError(
    const PublishError& publishError,
    ReplyContext& replyContext) {
  XLOG(DBG1) << __func__ << " reqID=" << publishError.requestID
             << " sess=" << this;
  MOQ_SUBSCRIBER_STATS(
      subscriberStatsCallback_, onPublishError, publishError.errorCode);
  if (logger_) {
    logger_->logPublishError(publishError, ControlMessageType::CREATED);
  }
  auto res = moqFrameWriter_.writeRequestError(
      replyContext.writeBuf(), publishError, FrameType::PUBLISH_ERROR);
  if (!res) {
    XLOG(ERR) << "writePublishError failed sess=" << this;
    return;
  }
  replyContext.flushFinal();

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
  if (draining_ || closed_) {
    SubscribeError subscribeError = {
        std::numeric_limits<uint64_t>::max(),
        SubscribeErrorCode::INTERNAL_ERROR,
        "draining/closed session"};
    MOQ_SUBSCRIBER_STATS(
        subscriberStatsCallback_, onSubscribeError, subscribeError.errorCode);
    co_return folly::makeUnexpected(subscribeError);
  }
  if (shouldFailNewLocalRequestDueToGoaway()) {
    SubscribeError subscribeError = {
        peekNextRequestID(),
        SubscribeErrorCode::GOING_AWAY,
        "Session received GOAWAY"};
    MOQ_SUBSCRIBER_STATS(
        subscriberStatsCallback_, onSubscribeError, subscribeError.errorCode);
    co_return folly::makeUnexpected(subscribeError);
  }
  auto fullTrackName = sub.fullTrackName;
  RequestID reqID = getNextRequestID();
  sub.requestID = reqID;
  TrackAlias trackAlias = reqID.value;
  aliasifyAuthTokens(sub.params);
  folly::IOBufQueue buf{folly::IOBufQueue::cacheChainLength()};
  auto wres = moqFrameWriter_.writeSubscribeRequest(buf, sub);
  if (!wres) {
    XLOG(ERR) << "writeSubscribeRequest failed sess=" << this;
    SubscribeError subscribeError = {
        reqID, SubscribeErrorCode::INTERNAL_ERROR, "local write failed"};
    MOQ_SUBSCRIBER_STATS(
        subscriberStatsCallback_, onSubscribeError, subscribeError.errorCode);
    co_return folly::makeUnexpected(subscribeError);
  }
  auto sendResult = sendRequest(
      buf,
      FrameType::SUBSCRIBE_OK,
      /*postTerminal=*/
      {FrameType::PUBLISH_DONE,
       FrameType::REQUEST_OK,
       FrameType::REQUEST_ERROR},
      reqID,
      /*minBidiDraftVersion=*/18,
      /*senderCallback=*/nullptr,
      // streamCount=max so in-flight subgroups flush; timeout delivers done.
      [this](RequestID id) {
        PublishDone pd;
        pd.requestID = id;
        pd.statusCode = PublishDoneStatusCode::SUBSCRIPTION_ENDED;
        pd.streamCount = std::numeric_limits<uint64_t>::max();
        pd.reasonPhrase = "peer closed stream before reply";
        onPublishDone(std::move(pd));
      });
  if (sendResult.hasError()) {
    SubscribeError subscribeError = {
        reqID,
        SubscribeErrorCode::INTERNAL_ERROR,
        std::move(sendResult.error())};
    MOQ_SUBSCRIBER_STATS(
        subscriberStatsCallback_, onSubscribeError, subscribeError.errorCode);
    co_return folly::makeUnexpected(subscribeError);
  }
  auto control = std::move(sendResult.value());
  auto trackReceiveState = std::make_shared<SubscribeTrackReceiveState>(
      fullTrackName, reqID, callback, this, trackAlias, logger_);
  trackReceiveState->setBidiControl(control);
  pendingRequests_.emplace(
      reqID, PendingRequestState::makeSubscribeTrack(trackReceiveState));
  pendingSubscribeTracks_.insert(fullTrackName);
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
    MOQ_SUBSCRIBER_STATS(subscriberStatsCallback_, onSubscriptionBegin);
    co_return std::make_shared<ReceiverSubscriptionHandle>(
        std::move(subscribeResult.value()),
        trackAlias,
        shared_from_this(),
        std::move(control));
  }
}

void MoQSession::sendSubscribeOk(const SubscribeOk& subOk, ReplyContext& ctx) {
  XLOG(DBG1) << __func__ << " sess=" << this;
  MOQ_PUBLISHER_STATS(publisherStatsCallback_, onSubscribeSuccess);
  MOQ_PUBLISHER_STATS(publisherStatsCallback_, onSubscriptionBegin);
  auto res = moqFrameWriter_.writeSubscribeOk(ctx.writeBuf(), subOk);
  if (!res) {
    XLOG(ERR) << "writeSubscribeOk failed sess=" << this;
    return;
  }

  if (logger_) {
    logger_->logSubscribeOk(subOk);
  }

  ctx.flush();
}

void MoQSession::subscribeError(
    const SubscribeError& subErr,
    ReplyContext& ctx) {
  XLOG(DBG1) << __func__ << " sess=" << this;
  MOQ_PUBLISHER_STATS(
      publisherStatsCallback_, onSubscribeError, subErr.errorCode);
  pubTracks_.erase(subErr.requestID);
  SCOPE_EXIT {
    checkForCloseOnDrain();
  };
  auto res = moqFrameWriter_.writeRequestError(
      ctx.writeBuf(), subErr, FrameType::SUBSCRIBE_ERROR);
  retireRequestID(/*signalWriteLoop=*/false);
  if (!res) {
    XLOG(ERR) << "writeSubscribeError failed sess=" << this;
    return;
  }

  if (logger_) {
    logger_->logSubscribeError(subErr);
  }

  ctx.flushFinal();
}

void MoQSession::unsubscribe(
    const Unsubscribe& unsubscribe,
    const std::shared_ptr<BidiStreamControl>& control) {
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
  if (trackIt->second->getSubscribeCallback()) {
    MOQ_SUBSCRIBER_STATS(subscriberStatsCallback_, onSubscriptionEnd);
  }
  trackIt->second->cancel();
  subTracks_.erase(trackIt);
  reqIdToTrackAlias_.erase(trackAliasIt);
  // Draft 18+: RESET the write half and cancel the read source on the bidi
  // request stream. Older drafts send Unsubscribe on the control stream.
  if (control && getDraftMajorVersion(*negotiatedVersion_) >= 18) {
    control->cancel(ResetStreamErrorCode::CANCELLED);
  } else {
    auto res = moqFrameWriter_.writeUnsubscribe(controlWriteBuf_, unsubscribe);
    if (!res) {
      XLOG(ERR) << "writeUnsubscribe failed sess=" << this;
      return;
    }
    controlWriteEvent_.signal();
  }

  // Log Unsubscribe
  if (logger_) {
    logger_->logUnsubscribe(unsubscribe);
  }

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
  MOQ_PUBLISHER_STATS(publisherStatsCallback_, onSubscriptionEnd);
  auto* ctx = it->second->replyContext();
  SCOPE_EXIT {
    pubTracks_.erase(it);
    checkForCloseOnDrain();
  };
  if (!ctx) {
    return;
  }
  auto res = moqFrameWriter_.writePublishDone(ctx->writeBuf(), pubDone);
  if (!res) {
    XLOG(ERR) << "writePublishDone failed sess=" << this;
    return;
  }

  if (logger_) {
    logger_->logPublishDone(pubDone);
  }
  ctx->flushFinal();
  retireRequestID(/*signalWriteLoop=*/false);
}

ReplyContext* MoQSession::getRequestUpdateReplyContext(
    RequestID existingRequestID) {
  // 18+: route on the request's bidi (null if gone). Pre-18: control stream.
  if (getDraftMajorVersion(*negotiatedVersion_) < 18) {
    return controlStreamReplyContext().get();
  }
  auto it = pubTracks_.find(existingRequestID);
  return it != pubTracks_.end() ? it->second->replyContext() : nullptr;
}

void MoQSession::requestUpdateOk(
    const RequestOk& requestOk,
    RequestID existingRequestID) {
  XLOG(DBG1) << __func__ << " reqID=" << requestOk.requestID
             << " existingReqID=" << existingRequestID << " sess=" << this;

  auto* ctx = getRequestUpdateReplyContext(existingRequestID);
  if (!ctx) {
    XLOG(ERR) << "requestUpdateOk: no reply context for id="
              << existingRequestID << " sess=" << this;
    return;
  }
  auto res = moqFrameWriter_.writeRequestOk(
      ctx->writeBuf(), requestOk, FrameType::REQUEST_OK);
  if (!res) {
    XLOG(ERR) << "writeRequestOk for REQUEST_UPDATE failed sess=" << this;
    return;
  }
  ctx->flush();
}

void MoQSession::requestUpdateError(
    const SubscribeUpdateError& requestError,
    RequestID existingRequestID,
    bool terminateExistingRequest) {
  XLOG(DBG1) << __func__ << " reqID=" << requestError.requestID
             << " existingReqID=" << existingRequestID << " sess=" << this;

  if (auto* ctx = getRequestUpdateReplyContext(existingRequestID)) {
    auto res = moqFrameWriter_.writeRequestError(
        ctx->writeBuf(), requestError, FrameType::REQUEST_UPDATE);
    if (!res) {
      XLOG(ERR) << "writeRequestError for REQUEST_UPDATE failed sess=" << this;
    } else {
      ctx->flush();
    }
  } else {
    XLOG(ERR) << "requestUpdateError: no reply context for id="
              << existingRequestID << " sess=" << this;
  }

  if (!terminateExistingRequest) {
    return;
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
  if (negotiatedVersion_ && useBidiRequestStreams(*negotiatedVersion_)) {
    return;
  }
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
  checkForCloseOnDrain();
}

void MoQSession::requestUpdate(
    const RequestUpdate& reqUpdate,
    const std::shared_ptr<BidiStreamControl>& control) {
  if (logger_) {
    logger_->logSubscribeUpdate(reqUpdate);
  }
  XLOG(DBG1) << __func__ << " sess=" << this;
  MOQ_SUBSCRIBER_STATS(subscriberStatsCallback_, onRequestUpdate);

  // First check if this is for a subscription
  auto trackAliasIt = reqIdToTrackAlias_.find(reqUpdate.existingRequestID);
  if (trackAliasIt != reqIdToTrackAlias_.end()) {
    auto trackIt = subTracks_.find(trackAliasIt->second);
    if (trackIt == subTracks_.end()) {
      XLOG(ERR) << "No matching track Alias=" << trackAliasIt->second
                << " sess=" << this;
      return;
    }
  } else {
    // Check if this is for a fetch
    auto fetchIt = fetches_.find(reqUpdate.existingRequestID);
    if (fetchIt == fetches_.end()) {
      XLOG(ERR) << "No matching request ID=" << reqUpdate.existingRequestID
                << " sess=" << this;
      return;
    }
  }

  auto* wh = control ? control->writeHandle() : nullptr;
  if (wh) {
    folly::IOBufQueue buf{folly::IOBufQueue::cacheChainLength()};
    auto res = moqFrameWriter_.writeRequestUpdate(buf, reqUpdate);
    if (!res) {
      XLOG(ERR) << "writeRequestUpdate failed sess=" << this;
      return;
    }
    // Draft 18+: response REQUEST_OK/ERROR has no wire requestID; record
    // this update's id so the response codec can correlate FIFO-order.
    if (getDraftMajorVersion(*negotiatedVersion_) >= 18) {
      control->responseIDQueue().push_back(reqUpdate.requestID);
    }
    wh->writeStreamData(buf.move(), /*fin=*/false, nullptr);
  } else {
    auto res = moqFrameWriter_.writeRequestUpdate(controlWriteBuf_, reqUpdate);
    if (!res) {
      XLOG(ERR) << "writeRequestUpdate failed sess=" << this;
      return;
    }
    controlWriteEvent_.signal();
  }
}

class MoQSession::ReceiverFetchHandle : public Publisher::FetchHandle {
 public:
  ReceiverFetchHandle(
      FetchOk ok,
      std::shared_ptr<MoQSession> session,
      std::shared_ptr<BidiStreamControl> control = nullptr)
      : FetchHandle(std::move(ok)),
        session_(std::move(session)),
        control_(std::move(control)) {}

  folly::coro::Task<FetchHandle::RequestUpdateResult> requestUpdate(
      RequestUpdate reqUpdate) override {
    co_return folly::makeUnexpected(
        RequestError{
            reqUpdate.requestID,
            RequestErrorCode::NOT_SUPPORTED,
            "REQUEST_UPDATE not supported for FETCH"});
  }

  void fetchCancel() override {
    if (session_) {
      session_->fetchCancel({fetchOk_->requestID}, control_);
      session_.reset();
    }
  }

 private:
  std::shared_ptr<MoQSession> session_;
  std::shared_ptr<BidiStreamControl> control_;
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
  if (draining_ || closed_) {
    FetchError fetchError = {
        std::numeric_limits<uint64_t>::max(),
        FetchErrorCode::INTERNAL_ERROR,
        "draining/closed session"};
    MOQ_SUBSCRIBER_STATS(
        subscriberStatsCallback_, onFetchError, fetchError.errorCode);
    co_return folly::makeUnexpected(fetchError);
  }
  if (shouldFailNewLocalRequestDueToGoaway()) {
    FetchError fetchError = {
        peekNextRequestID(),
        FetchErrorCode::GOING_AWAY,
        "Session received GOAWAY"};
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

  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  moqFrameWriter_.writeFetch(writeBuf, fetch);
  auto sendResult = sendRequest(
      writeBuf,
      FrameType::FETCH_OK,
      /*postTerminal=*/{FrameType::REQUEST_OK, FrameType::REQUEST_ERROR},
      reqID,
      /*minBidiDraftVersion=*/18,
      /*senderCallback=*/nullptr,
      // After FETCH_OK we disarm in onFetchOk so the data streams own
      // completion.
      [this](RequestID id) {
        auto fetchIt = fetches_.find(id);
        if (fetchIt == fetches_.end()) {
          return;
        }
        fetchIt->second->fetchError(
            {id, FetchErrorCode::CANCELLED, "peer closed stream before reply"});
      });
  if (sendResult.hasError()) {
    FetchError fetchError = {
        reqID, FetchErrorCode::INTERNAL_ERROR, std::move(sendResult.error())};
    MOQ_SUBSCRIBER_STATS(
        subscriberStatsCallback_, onFetchError, fetchError.errorCode);
    co_return folly::makeUnexpected(fetchError);
  }
  auto control = std::move(sendResult.value());
  auto trackReceiveState = std::make_shared<FetchTrackReceiveState>(
      fullTrackName, reqID, std::move(consumer), fetch.groupOrder, logger_);
  trackReceiveState->setBidiControl(control);
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
        std::move(fetchResult.value()), shared_from_this(), std::move(control));
  }
}

void MoQSession::fetchOk(const FetchOk& fetchOk, ReplyContext& replyContext) {
  XLOG(DBG1) << __func__ << " sess=" << this;
  MOQ_PUBLISHER_STATS(publisherStatsCallback_, onFetchSuccess);
  auto res = moqFrameWriter_.writeFetchOk(replyContext.writeBuf(), fetchOk);
  if (!res) {
    XLOG(ERR) << "writeFetchOk failed sess=" << this;
    return;
  }
  if (logger_) {
    logger_->logFetchOk(fetchOk);
  }
  // Bidi stream stays open after FETCH_OK so the subscriber can send
  // REQUEST_UPDATE or signal cancellation via FIN/RST/STOP_SENDING.
  replyContext.flush();
}

void MoQSession::fetchError(const FetchError& fetchErr, ReplyContext& ctx) {
  XLOG(DBG1) << __func__ << " sess=" << this;
  MOQ_PUBLISHER_STATS(
      publisherStatsCallback_, onFetchError, fetchErr.errorCode);

  if (logger_) {
    logger_->logFetchError(fetchErr);
  }

  pubTracks_.erase(fetchErr.requestID);
  SCOPE_EXIT {
    checkForCloseOnDrain();
  };
  auto res = moqFrameWriter_.writeRequestError(
      ctx.writeBuf(), fetchErr, FrameType::FETCH_ERROR);
  if (!res) {
    XLOG(ERR) << "writeFetchError failed sess=" << this;
    return;
  }
  ctx.flushFinal();
}

void MoQSession::fetchCancel(
    const FetchCancel& fetchCan,
    const std::shared_ptr<BidiStreamControl>& control) {
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
  // Draft 18+ cancels via the bidi; older drafts send FetchCancel on control.
  if (control && getDraftMajorVersion(*negotiatedVersion_) >= 18) {
    control->cancel(ResetStreamErrorCode::CANCELLED);
  } else {
    auto res = moqFrameWriter_.writeFetchCancel(controlWriteBuf_, fetchCan);
    if (!res) {
      XLOG(ERR) << "writeFetchCancel failed sess=" << this;
      return;
    }
    controlWriteEvent_.signal();
  }
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
    if (negotiatedVersion_ && useUniControlStreams(*negotiatedVersion_)) {
      // In uni control mode, uni streams before setup may carry the peer's
      // control stream (with SETUP)
      co_withExecutor(
          exec_.get(),
          co_withCancellation(
              cancellationSource_.getToken(),
              handlePreSetupUniStream(shared_from_this(), rh)))
          .start();
      return;
    }
    XLOG(ERR) << "Uni stream before setup complete sess=" << this;
    close(SessionCloseErrorCode::PROTOCOL_VIOLATION);
    return;
  }
  // maybe not SUBGROUP_HEADER, but at least not control
  co_withExecutor(
      exec_.get(),
      co_withCancellation(
          cancellationSource_.getToken(),
          dataStreamReadLoop(shared_from_this(), rh)))
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

  handleClientSetup(bh, proxygen::WebTransport::StreamData{nullptr, false});
}

void MoQSession::replayBufferedUniStreams() {
  for (auto& buffered : bufferedPreSetupUniStreams_) {
    co_withExecutor(
        exec_.get(),
        co_withCancellation(
            cancellationSource_.getToken(),
            dataStreamReadLoop(
                shared_from_this(),
                buffered.readHandle,
                std::move(buffered.initialData))))
        .start();
  }
  bufferedPreSetupUniStreams_.clear();
}

folly::coro::Task<void> MoQSession::handlePreSetupUniStream(
    std::shared_ptr<MoQSession> session, // keeps session alive
    proxygen::WebTransport::StreamReadHandle* readHandle) {
  co_await folly::coro::co_safe_point;

  std::optional<FrameType> frameType = std::nullopt;
  folly::IOBufQueue readBuf{folly::IOBufQueue::cacheChainLength()};
  bool fin = false;

  do {
    auto streamData = co_await co_awaitTry(
        folly::coro::co_withCancellation(
            cancellationSource_.getToken(),
            readHandle->readStreamData().via(exec_.get())));
    if (!streamData.hasValue()) {
      break;
    }
    if (streamData->data) {
      readBuf.append(std::move(streamData->data));
    }
    fin = streamData->fin;
    frameType = getFrameType(readBuf, negotiatedVersion_);
  } while (!frameType.has_value() && !fin);

  if (!frameType.has_value() || readBuf.chainLength() == 0) {
    co_return;
  }

  if (*frameType == FrameType::SETUP) {
    // This is the peer's control stream
    if (peerControlStreamReceived_) {
      XLOG(ERR) << "Duplicate control stream sess=" << this;
      close(SessionCloseErrorCode::PROTOCOL_VIOLATION);
      co_return;
    }
    peerControlStreamReceived_ = true;

    if (logger_) {
      logger_->logStreamTypeSet(
          readHandle->getID(), MOQTStreamType::CONTROL, Owner::REMOTE);
    }

    proxygen::WebTransport::StreamData initialData{readBuf.move(), fin};
    co_await controlReadLoop(readHandle, std::move(initialData));
  } else if (setupComplete_) {
    // Setup already completed while we were reading — dispatch directly
    co_await dataStreamReadLoop(
        std::move(session),
        readHandle,
        proxygen::WebTransport::StreamData{readBuf.move(), fin});
  } else {
    // Data stream arrived before setup - buffer it
    static constexpr size_t kMaxBufferedPreSetupUniStreams = 100;
    if (bufferedPreSetupUniStreams_.size() >= kMaxBufferedPreSetupUniStreams) {
      XLOG(ERR) << "Too many pre-setup uni streams sess=" << this;
      close(SessionCloseErrorCode::PROTOCOL_VIOLATION);
      co_return;
    }
    bufferedPreSetupUniStreams_.push_back({readHandle, {readBuf.move(), fin}});
  }
}

void MoQSession::handleClientSetup(
    proxygen::WebTransport::BidiStreamHandle bh,
    proxygen::WebTransport::StreamData initialData) noexcept {
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

std::optional<MoQSession::BidiStreamConfig> MoQSession::getBidiStreamConfig(
    FrameType frameType) {
  const auto major = getDraftMajorVersion(*negotiatedVersion_);
  // SUBSCRIBE_NAMESPACE response stream: FIN-or-RST cancels per draft-16+.
  auto subscribeNamespaceConfig = [this](FrameType wireType) {
    return BidiStreamConfig{
        {wireType, FrameType::REQUEST_UPDATE},
        [this](RequestID id) {
          onUnsubscribeNamespace(UnsubscribeNamespace{id, std::nullopt});
        },
        /*finIsCancellation=*/true};
  };
  if (major >= 18) {
    switch (frameType) {
      case FrameType::SUBSCRIBE:
        return BidiStreamConfig{
            {FrameType::SUBSCRIBE, FrameType::REQUEST_UPDATE},
            [this](RequestID id) { onUnsubscribe(Unsubscribe{id}); }};
      case FrameType::FETCH:
        return BidiStreamConfig{
            {FrameType::FETCH, FrameType::REQUEST_UPDATE},
            [this](RequestID id) { onFetchCancel(FetchCancel{id}); }};
      case FrameType::PUBLISH:
        return BidiStreamConfig{
            {FrameType::PUBLISH,
             FrameType::REQUEST_UPDATE,
             FrameType::PUBLISH_DONE,
             FrameType::REQUEST_OK,
             FrameType::REQUEST_ERROR},
            nullptr};
      case FrameType::PUBLISH_NAMESPACE:
        // Publisher (sender) closes the stream (FIN or RST) to withdraw
        // the announce — both signal end-of-PUBLISH_NAMESPACE.
        return BidiStreamConfig{
            {FrameType::PUBLISH_NAMESPACE, FrameType::REQUEST_UPDATE},
            [this](RequestID id) {
              PublishNamespaceDone done;
              done.requestID = id;
              onPublishNamespaceDone(std::move(done));
            },
            /*finIsCancellation=*/true};
      case FrameType::TRACK_STATUS:
        return BidiStreamConfig{{FrameType::TRACK_STATUS}, nullptr};
      case FrameType::SUBSCRIBE_NAMESPACE:
        // v18 wire enumerator (0x50). The legacy 0x11 wire type is a
        // protocol violation on v18 and falls through to nullopt below.
        return subscribeNamespaceConfig(FrameType::SUBSCRIBE_NAMESPACE);
      case FrameType::SUBSCRIBE_TRACKS:
        // FIN here means "no more REQUEST_UPDATE" — cancel is RST only, which
        // surfaces via exceptionalExit regardless of finIsCancellation.
        return BidiStreamConfig{
            {FrameType::SUBSCRIBE_TRACKS, FrameType::REQUEST_UPDATE},
            [this](RequestID id) { onSubscribeTracksStreamClosed(id); }};
      default:
        return std::nullopt;
    }
  }
  // Drafts 16-17: SUBSCRIBE_NAMESPACE uses the legacy 0x11 wire enumerator.
  // The v18 wire type (0x50) is a protocol violation on pre-v18 sessions.
  // NOLINTNEXTLINE(clang-diagnostic-switch-enum)
  switch (frameType) {
    case FrameType::LEGACY_SUBSCRIBE_NAMESPACE:
      return subscribeNamespaceConfig(FrameType::LEGACY_SUBSCRIBE_NAMESPACE);
    default:
      return std::nullopt;
  }
}

folly::coro::Task<void> MoQSession::bidiStreamDemuxer(
    proxygen::WebTransport::BidiStreamHandle bh) noexcept {
  co_await folly::coro::co_safe_point;
  auto sessionToken = co_await folly::coro::co_current_cancellation_token;
  auto streamToken = folly::cancellation_token_merge(
      bh.readHandle->getCancelToken(), bh.writeHandle->getCancelToken());
  auto token =
      folly::cancellation_token_merge(sessionToken, std::move(streamToken));
  if (token.isCancellationRequested()) {
    co_return;
  }
  auto readHandle = bh.readHandle;
  std::optional<FrameType> frameType = std::nullopt;

  folly::IOBufQueue readBuf{folly::IOBufQueue::cacheChainLength()};
  bool fin = false;

  do {
    auto streamData = co_await co_awaitTry(
        folly::coro::co_withCancellation(
            token, readHandle->readStreamData().via(exec_.get())));
    if (!streamData.hasValue()) {
      break;
    }
    if (token.isCancellationRequested()) {
      co_return;
    }
    // Accumulate data in the buffer
    if (streamData->data) {
      readBuf.append(std::move(streamData->data));
    }
    fin = streamData->fin;
    // Try to parse frame type from the accumulated buffer. Each FrameType
    // enumerator is its own wire integer, so a plain cast suffices — wire
    // 0x50 maps to SUBSCRIBE_NAMESPACE (v18), 0x11 to
    // LEGACY_SUBSCRIBE_NAMESPACE (v17-), 0x51 to SUBSCRIBE_TRACKS, etc.
    // getBidiStreamConfig is version-gated so a mismatched wire type for the
    // negotiated draft returns nullopt and the session is closed.
    frameType = getFrameType(readBuf, negotiatedVersion_);
  } while (!frameType.has_value() && !fin);

  if (token.isCancellationRequested()) {
    co_return;
  }

  if (frameType.has_value() && readBuf.chainLength() > 0) {
    // Create StreamData with the accumulated buffer
    proxygen::WebTransport::StreamData accumulatedData{readBuf.move(), fin};
    if (*frameType == FrameType::CLIENT_SETUP ||
        *frameType == FrameType::SETUP) {
      if (negotiatedVersion_ && useUniControlStreams(*negotiatedVersion_)) {
        XLOG(ERR) << "Setup frame on bidi stream in uni control mode sess="
                  << this;
        close(SessionCloseErrorCode::PROTOCOL_VIOLATION);
        co_return;
      }
      // Process the frame as a CLIENT_SETUP
      handleClientSetup(bh, std::move(accumulatedData));
    } else {
      auto config = getBidiStreamConfig(*frameType);
      if (!config) {
        XLOG(ERR) << "Unexpected frame type on bidi stream: "
                  << static_cast<uint64_t>(*frameType) << " sess=" << this;
        close(SessionCloseErrorCode::PROTOCOL_VIOLATION);
        co_return;
      }
      auto control = std::make_shared<BidiStreamControl>(
          bh.writeHandle,
          cancellationSource_.getToken(),
          config->finIsCancellation);
      auto cb = std::make_unique<BidiRequestCallback>(
          this, control, std::move(config->onPeerTermination));
      auto* cbPtr = cb.get();
      auto mergedToken = folly::cancellation_token_merge(
          cancellationSource_.getToken(), control->getReadCancelToken());
      // FIFO-correlate post-terminal REQUEST_OK/ERROR for REQUEST_UPDATEs
      // this end sends (e.g. PUBLISH bidi).
      auto codec = makeBidiCodec(
          cbPtr,
          config->allowedFrames,
          /*requestID=*/std::nullopt,
          /*okType=*/std::nullopt,
          &control->responseIDQueue());
      co_withExecutor(
          exec_.get(),
          co_withCancellation(
              std::move(mergedToken),
              controlReadLoop(
                  bh.readHandle,
                  std::move(accumulatedData),
                  std::move(codec),
                  std::move(cb),
                  std::move(control))))
          .start();
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
  MoQFrameParser parser;
  parser.initializeVersion(*negotiatedVersion_);
  auto type = parser.decodeVarint(cursor);
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
  auto statePtr = getSubscribeTrackReceiveState(alias);
  auto* state = statePtr.get();
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

void MoQSession::onSessionEnd(folly::Optional<uint32_t> err) noexcept {
  if (logger_) {
    logger_->outputLogs();
  }
  XLOG(DBG1) << __func__ << "err="
             << (err ? folly::to<std::string>(*err) : std::string("none"))
             << " sess=" << this;
  // The peer closed us, but we can close with NO_ERROR
  close(SessionCloseErrorCode::NO_ERROR, err);
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
    if (getDraftMajorVersion(*getNegotiatedVersion()) < 16) {
      if (requestID.value != nextExpectedPeerRequestID_) {
        XLOG(ERR) << "Invalid next requestID: " << requestID
                  << " sess=" << this;
        close(SessionCloseErrorCode::INVALID_REQUEST_ID);
        return true;
      }
      nextExpectedPeerRequestID_ += getRequestIDMultiplier();
    } // in draft 16+, request IDs can come out of order
    if (getDraftMajorVersion(*getNegotiatedVersion()) >= 18) {
      nextPeerRequestIDForGoaway_ = std::max(
          nextPeerRequestIDForGoaway_,
          requestID.value + getRequestIDMultiplier());
    }
  } else {
    if (requestID.value >= maxRequestID_) {
      XLOG(ERR) << "Invalid requestID: " << requestID << " sess=" << this;
      close(SessionCloseErrorCode::INVALID_REQUEST_ID);
      return true;
    }
  }
  return false;
}

bool MoQSession::shouldRejectNewPeerRequestDueToGoaway() const {
  if (receivedGoaway_) {
    return true;
  }
  return draining_ && negotiatedVersion_.has_value() &&
      getDraftMajorVersion(*negotiatedVersion_) >= 18;
}

bool MoQSession::shouldFailNewLocalRequestDueToGoaway() const {
  // The received cutoff describes already-sent requests; after GOAWAY, do not
  // start new local requests on this session.
  return receivedGoawayRequestID_.has_value() &&
      negotiatedVersion_.has_value() &&
      getDraftMajorVersion(*negotiatedVersion_) >= 18;
}

void MoQSession::initializeNegotiatedVersion(uint64_t negotiatedVersion) {
  negotiatedVersion_ = negotiatedVersion;
  moqFrameWriter_.initializeVersion(*negotiatedVersion_);
  controlCodec_->initializeVersion(*negotiatedVersion_);
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

uint64_t MoQSession::getMaxAuthTokenCacheSizeIfPresent(
    const SetupParameters& params,
    uint64_t version) {
  // Draft 18+ delivers requests on independent bidi streams, breaking the
  // request-order assumption that auth token aliasing relies on.
  if (useBidiRequestStreams(version)) {
    return 0;
  }
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
    // Draft 18+ disables aliasing entirely (requests travel on independent
    // bidi streams, breaking the request-order assumption). Bypass the
    // tokenCache_ branch unconditionally — the send-side cache may still
    // hold a pre-SETUP default size before negotiation completes.
    const bool aliasingDisabled = useBidiRequestStreams(*version);
    if (!aliasingDisabled && token.alias &&
        token.tokenValue.size() < tokenCache_.maxTokenSize()) {
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
  onPublishNamespaceImpl(
      std::move(publishNamespace), controlStreamReplyContext());
}

void MoQSession::onPublishNamespaceImpl(
    PublishNamespace publishNamespace,
    std::shared_ptr<ReplyContext> replyContext) {
  XLOG(DBG1) << __func__ << " ns=" << publishNamespace.trackNamespace
             << " - sending NOT_SUPPORTED error, sess=" << this;
  if (shouldRejectNewPeerRequestDueToGoaway()) {
    XLOG(DBG1) << "Rejecting publishNamespace request, GOAWAY/draining sess="
               << this;
    publishNamespaceError(
        PublishNamespaceError{
            publishNamespace.requestID,
            PublishNamespaceErrorCode::GOING_AWAY,
            "Session going away"},
        *replyContext);
    return;
  }
  publishNamespaceError(
      PublishNamespaceError{
          publishNamespace.requestID,
          PublishNamespaceErrorCode::NOT_SUPPORTED,
          "PublishNamespace not supported by simple client"},
      *replyContext);
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

  if (!validateRequestOkTrackProperties(requestOk, frameType)) {
    return;
  }

  if (!validateRequestOkParams(requestOk, frameType)) {
    return;
  }

  switch (frameType) {
    case FrameType::TRACK_STATUS_OK: {
      handleTrackStatusOkFromRequestOk(requestOk);
      break;
    }
    case FrameType::PUBLISH_OK: {
      handlePublishOkFromRequestOk(requestOk);
      break;
    }
    case FrameType::REQUEST_OK: {
      switch (reqIt->second->getType()) {
        case PendingRequestState::Type::REQUEST_UPDATE:
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
  auto subNsReply = std::make_shared<SubNSReply>(
      moqFrameWriter_, controlStreamReplyContext());
  onSubscribeNamespaceImpl(subscribeNamespace, std::move(subNsReply));
}

void MoQSession::onSubscribeNamespaceImpl(
    const SubscribeNamespace& subscribeNamespace,
    std::shared_ptr<SubNSReply> subNsReply) {
  XLOG(DBG1) << __func__
             << " prefix=" << subscribeNamespace.trackNamespacePrefix
             << " - sending NOT_SUPPORTED error, sess=" << this;
  if (shouldRejectNewPeerRequestDueToGoaway()) {
    XLOG(DBG1) << "Rejecting subscribeNamespace request, GOAWAY/draining sess="
               << this;
    subscribeNamespaceError(
        SubscribeNamespaceError{
            subscribeNamespace.requestID,
            SubscribeNamespaceErrorCode::GOING_AWAY,
            "Session going away"},
        std::move(subNsReply));
    return;
  }
  subscribeNamespaceError(
      SubscribeNamespaceError{
          subscribeNamespace.requestID,
          SubscribeNamespaceErrorCode::NOT_SUPPORTED,
          "SubscribeNamespace not supported by simple client"},
      std::move(subNsReply));
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
    const PublishNamespaceError& publishNamespaceError,
    ReplyContext& replyContext) {
  XLOG(DBG1) << __func__ << " reqID=" << publishNamespaceError.requestID.value
             << " sess=" << this;
  MOQ_SUBSCRIBER_STATS(
      subscriberStatsCallback_,
      onPublishNamespaceError,
      publishNamespaceError.errorCode);
  auto res = moqFrameWriter_.writeRequestError(
      replyContext.writeBuf(),
      publishNamespaceError,
      FrameType::PUBLISH_NAMESPACE_ERROR);
  if (!res) {
    XLOG(ERR) << "writePublishNamespaceError failed sess=" << this;
    return;
  }
  if (logger_) {
    logger_->logPublishNamespaceError(publishNamespaceError);
  }
  replyContext.flushFinal();
}

void MoQSession::subscribeNamespaceError(
    const SubscribeNamespaceError& subscribeNamespaceError,
    std::shared_ptr<SubNSReply>&& subNsReply) {
  XLOG(DBG1) << __func__ << " reqID=" << subscribeNamespaceError.requestID.value
             << " sess=" << this;
  MOQ_PUBLISHER_STATS(
      publisherStatsCallback_,
      onSubscribeNamespaceError,
      subscribeNamespaceError.errorCode);
  auto res = subNsReply->error(subscribeNamespaceError);
  if (!res) {
    XLOG(ERR) << "writeSubscribeNamespaceError failed sess=" << this;
    return;
  }
  if (logger_) {
    logger_->logSubscribeNamespaceError(subscribeNamespaceError);
  }
}

// Draft 18+: SUBSCRIBE_TRACKS handlers and helpers
void MoQSession::onSubscribeTracksImpl(
    const SubscribeTracks& subscribeTracks,
    std::shared_ptr<SubscribeTracksReply> subTracksReply) {
  XLOG(DBG1) << __func__ << " prefix=" << subscribeTracks.trackNamespacePrefix
             << " - sending NOT_SUPPORTED error, sess=" << this;
  if (receivedGoaway_) {
    subscribeTracksError(
        SubscribeTracksError{
            subscribeTracks.requestID,
            SubscribeTracksErrorCode::GOING_AWAY,
            "Session received GOAWAY"},
        std::move(subTracksReply));
    return;
  }
  subscribeTracksError(
      SubscribeTracksError{
          subscribeTracks.requestID,
          SubscribeTracksErrorCode::NOT_SUPPORTED,
          "SubscribeTracks not supported by simple client"},
      std::move(subTracksReply));
}

void MoQSession::subscribeTracksError(
    const SubscribeTracksError& subscribeTracksError,
    std::shared_ptr<SubscribeTracksReply>&& subTracksReply) {
  XLOG(DBG1) << __func__ << " reqID=" << subscribeTracksError.requestID.value
             << " sess=" << this;
  MOQ_PUBLISHER_STATS(
      publisherStatsCallback_,
      onSubscribeTracksError,
      subscribeTracksError.errorCode);
  auto res = subTracksReply->error(subscribeTracksError);
  if (!res) {
    XLOG(ERR) << "writeSubscribeTracksError failed sess=" << this;
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

std::shared_ptr<ReplyContext> MoQSession::makeReplyContext(
    std::shared_ptr<BidiStreamControl> control) {
  if (control) {
    XCHECK(control->writeHandle())
        << "BidiStreamControl handed to makeReplyContext must have a live "
           "write handle";
    return std::make_shared<BidiStreamReplyContext>(
        std::move(control), cancellationSource_.getToken());
  }
  return controlStreamReplyContext();
}

std::shared_ptr<ReplyContext> MoQSession::controlStreamReplyContext() {
  if (!controlStreamReplyContext_) {
    controlStreamReplyContext_ = std::make_shared<ControlStreamReplyContext>(
        controlWriteBuf_, controlWriteEvent_, cancellationSource_.getToken());
  }
  return controlStreamReplyContext_;
}

void MoQSession::validateAndSetVersionFromAlpn(const std::string& alpn) {
  auto version = getVersionFromAlpn(std::string_view(alpn));
  if (version) {
    initializeNegotiatedVersion(*version);
  }
}

WriteResult SubNSReply::ok(const SubscribeNamespaceOk& subNsOk) {
  auto res = moqFrameWriter_.writeSubscribeNamespaceOk(
      replyContext_->writeBuf(), subNsOk);
  replyContext_->flush();
  return res;
}

WriteResult SubNSReply::error(const SubscribeNamespaceError& subNsError) {
  auto res = moqFrameWriter_.writeRequestError(
      replyContext_->writeBuf(),
      subNsError,
      FrameType::SUBSCRIBE_NAMESPACE_ERROR);
  replyContext_->flushFinal();
  return res;
}

WriteResult MessageReply::ok(const RequestOk& okMsg) {
  auto res = moqFrameWriter_.writeRequestOk(
      replyContext_->writeBuf(), okMsg, FrameType::REQUEST_OK);
  replyContext_->flush();
  return res;
}

WriteResult MessageReply::error(const SubscribeTracksError& errorMsg) {
  if (errorMsg.errorCode == RequestErrorCode::REDIRECT) {
    XLOG(ERR) << "REDIRECT not permitted in SUBSCRIBE_TRACKS error";
    return folly::makeUnexpected(quic::TransportErrorCode::INTERNAL_ERROR);
  }
  auto res = moqFrameWriter_.writeRequestError(
      replyContext_->writeBuf(), errorMsg, FrameType::REQUEST_ERROR);
  // After ERROR the publisher is done with this stream — FIN it.
  replyContext_->flushFinal();
  return res;
}

WriteResult SubscribeTracksReply::ok(const RequestOk& okMsg) {
  if (errorSent_ || okSent_) {
    return folly::makeUnexpected(quic::TransportErrorCode::PROTOCOL_VIOLATION);
  }
  auto res = moqFrameWriter_.writeRequestOk(
      replyContext_->writeBuf(), okMsg, FrameType::REQUEST_OK);
  replyContext_->flush();
  okSent_ = true;
  flushPendingMessages();
  return res;
}

WriteResult SubscribeTracksReply::error(const SubscribeTracksError& errorMsg) {
  if (okSent_ || errorSent_) {
    return folly::makeUnexpected(quic::TransportErrorCode::PROTOCOL_VIOLATION);
  }
  auto res = moqFrameWriter_.writeRequestError(
      replyContext_->writeBuf(), errorMsg, FrameType::REQUEST_ERROR);
  pendingBuf_.move();
  replyContext_->flush(/*fin=*/true);
  errorSent_ = true;
  return res;
}

void SubscribeTracksReply::publishBlocked(
    const TrackNamespace& trackNamespaceSuffix,
    const std::string& trackName) {
  if (errorSent_) {
    XLOG(ERR) << "Ignoring PUBLISH_BLOCKED after REQUEST_ERROR";
    return;
  }
  auto res = moqFrameWriter_.writePublishBlocked(
      replyContext_->writeBuf(),
      PublishBlocked{trackNamespaceSuffix, trackName});
  if (!res) {
    XLOG(ERR) << "writePublishBlocked failed err=" << uint64_t(res.error());
    replyContext_->writeBuf().move();
    return;
  }
  if (okSent_) {
    replyContext_->flush();
  } else {
    pendingBuf_.append(replyContext_->writeBuf().move());
  }
}

void SubscribeTracksReply::flushPendingMessages() {
  if (!pendingBuf_.empty()) {
    replyContext_->writeBuf().append(pendingBuf_.move());
    replyContext_->flush();
  }
}

} // namespace moxygen

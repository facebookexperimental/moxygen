/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/MoQSession.h"
#include <folly/Chrono.h>
#include <folly/concurrency/ProcessLocalUniqueId.h>
#include <folly/coro/Baton.h>
#include <folly/coro/Collect.h>
#include <folly/coro/DetachOnCancel.h>
#include <folly/coro/FutureUtil.h>
#include <folly/coro/SharedPromise.h>
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

// Draft-18 §10.4: GOAWAY newSessionUri MUST NOT exceed 8192 bytes.
constexpr uint64_t kMaxGoawayUriLength(8192);

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
    auto* pubStats =
        publisher_ ? publisher_->getPublisherStatsCallback() : nullptr;
    auto now = folly::chrono::coarse_steady_clock::now();
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

      auto latencyUsec = std::chrono::duration_cast<std::chrono::microseconds>(
                             now - pendingDelivery.enqueuedAt)
                             .count();
      MOQ_PUBLISHER_STATS(
          pubStats, recordObjectAckLatency, uint64_t(latencyUsec));

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
    if (!deliveryTimer_) {
      XLOG(DBG6)
          << "MoQSession::SubgroupPublisher::setDeliveryTimeout: No timer exists, ignoring timeout update";
      return;
    }
    if (!timeout.has_value()) {
      XLOG(DBG6)
          << "MoQSession::SubgroupPublisher::setDeliveryTimeout: Disabling delivery timeout";
      deliveryTimeoutActive_ = false;
      deliveryTimer_->cancelAllTimers();
      return;
    }
    CHECK_NE(timeout.value().count(), 0)
        << "MoQSession::SubgroupPublisher::setDeliveryTimeout must not be called with a value of 0";
    XLOG(DBG6)
        << "MoQSession::SubgroupPublisher::setDeliveryTimeout: CALLING deliveryTimer->setDeliveryTimeout"
        << " timeout=" << timeout->count() << "ms";
    deliveryTimeoutActive_ = true;
    deliveryTimer_->setDeliveryTimeout(*timeout);
  }

 private:
  std::shared_ptr<MLogger> logger_;
  std::shared_ptr<DeliveryCallback> deliveryCallback_;
  std::unique_ptr<MoQDeliveryTimer> deliveryTimer_;
  bool deliveryTimeoutActive_{false};

  // Track objects and their end offsets for delivery callbacks
  struct ObjectDeliveryInfo {
    uint64_t groupId;
    uint64_t subgroupId;
    uint64_t objectId;
    uint64_t endOffset;
    folly::chrono::coarse_steady_clock::time_point enqueuedAt;

    ObjectDeliveryInfo(
        uint64_t g,
        uint64_t sg,
        uint64_t o,
        uint64_t offset,
        folly::chrono::coarse_steady_clock::time_point enq)
        : groupId(g),
          subgroupId(sg),
          objectId(o),
          endOffset(offset),
          enqueuedAt(enq) {}
  };
  quic::CircularDeque<ObjectDeliveryInfo> pendingDeliveries_;

  void onByteEventCommon(quic::StreamId id, uint64_t offset) {
    // The FIN takes a stream offset of its own but was never buffered.
    uint64_t bytesDeliveredOrCanceled =
        std::min<uint64_t>(offset + 1, bytesWritten_);
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
  // Writes held for stream credit are flushed when it arrives, whether or not
  // the producer waits for readiness again.
  void waitForStreamCredit();
  void onStreamCredit(folly::Try<folly::Unit> credit);

  void setWriteHandle(proxygen::WebTransport::StreamWriteHandle* writeHandle);

  folly::Expected<folly::Unit, MoQPublishError> validatePublish(
      uint64_t objectID,
      const Extensions& extensions);
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
  bool cancelled_{false}; // Reset by the session, not the caller
  std::optional<folly::CancellationCallback> cancelCallback_;
  proxygen::WebTransport::StreamWriteHandle* writeHandle_{nullptr};
  // A FETCH out of uni stream credit accepts writes into writeBuf_ and
  // reports BLOCKED; they go out once its stream can be created.
  bool awaitingStreamCredit_{false};
  bool pendingFin_{false};
  bool pendingEndObject_{false};
  // Zero when credit was already available.
  std::optional<uint64_t> streamCreditWaitId_;
  std::vector<folly::Promise<uint64_t>> streamCreditReadiness_;
  StreamType streamType_;
  bool includeExtensions_{true}; // Subgroups only, FETCH always allows them
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
              cancelled_ = true;
              this->reset(errorCode);
            }
          });
      deliveryTimeoutActive_ = true;
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
  includeExtensions_ = includeExtensions;
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
      SubgroupOptions{
          .hasExtensions = includeExtensions,
          .subgroupIDFormat = format,
          .hasEndOfGroup = endOfGroup,
          .beginsWithFirstObject = beginsWithFirstObject});
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
    logger_->logFetchHeaderCreated(
        writeHandle_->getID(), req.value, publisher_->isFill());
  }

  cancelCallback_.emplace(writeHandle_->getCancelToken(), [this] {
    if (writeHandle_) {
      auto* ex = writeHandle_->exception();
      XLOG(DBG0) << "Peer requested write termination id="
                 << writeHandle_->getID() << " code=" << (ex ? ex->what() : "")
                 << " sgp=" << this;
      cancelled_ = true;
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
StreamPublisherImpl::validatePublish(
    uint64_t objectID,
    const Extensions& extensions) {
  if (!includeExtensions_ && !extensions.empty()) {
    XLOG(ERR) << "Extensions on a subgroup opened without them sgp=" << this;
    reset(ResetStreamErrorCode::INTERNAL_ERROR);
    return folly::makeUnexpected(MoQPublishError(
        MoQPublishError::API_ERROR,
        "Extensions on a subgroup opened with includeExtensions=false"));
  }
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
  auto writeResult = writeToStream(finStream, entireObjectWritten);
  // Only advance the largest once the full object has been written to the wire.
  if (writeResult.hasValue() && entireObjectWritten) {
    publisher_->updateLargest({header_.group, objectID});
  }
  return writeResult;
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
    if (awaitingStreamCredit_) {
      pendingFin_ = pendingFin_ || finStream;
      pendingEndObject_ = endObject;
      return folly::makeUnexpected(MoQPublishError(
          MoQPublishError::BLOCKED, "Waiting for uni stream credit"));
    }
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

    bool trackForStats =
        publisher_ && publisher_->getPublisherStatsCallback() != nullptr;
    if ((deliveryCallback_ || deliveryTimer_ || trackForStats) && endObject) {
      uint64_t endOffset = bytesWritten_ + writeBuf_.chainLength() - 1;
      pendingDeliveries_.emplace_back(
          header_.group,
          header_.subgroup,
          header_.id,
          endOffset,
          folly::chrono::coarse_steady_clock::now());
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
  auto validateRes = validatePublish(objectID, extensions);
  if (!validateRes) {
    return validateRes;
  }
  auto length = payload ? payload->computeChainDataLength() : 0;
  header_.status = ObjectStatus::NORMAL;

  if (logger_ && writeHandle_) {
    auto payloadClone = payload ? payload->clone() : nullptr;
    if (streamType_ != StreamType::FETCH_HEADER) {
      logger_->logSubgroupObjectCreated(
          writeHandle_->getID(), trackAlias_, header_, std::move(payloadClone));
    } else {
      logger_->logFetchObjectCreated(
          writeHandle_->getID(), header_, std::move(payloadClone));
    }
  }

  // Start delivery timeout timer when object begins being sent to stream. A 0
  // ("no timeout") value leaves deliveryTimeoutActive_ false, so no timer arms.
  if (deliveryTimer_ && deliveryTimeoutActive_) {
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
  auto validateRes = validatePublish(objectID, extensions);
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

  // Start delivery timeout timer when object begins being sent to stream. A 0
  // ("no timeout") value leaves deliveryTimeoutActive_ false, so no timer arms.
  if (deliveryTimer_ && deliveryTimeoutActive_) {
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
    if (!awaitingStreamCredit_ || pendingFin_) {
      return folly::makeUnexpected(MoQPublishError(
          MoQPublishError::CANCELLED, "Cancelled objectPayload"));
    }
    auto res = ensureWriteHandle();
    if (!res) {
      return folly::makeUnexpected(res.error());
    }
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
  auto validateRes = validatePublish(objectID, extensions);
  if (!validateRes) {
    return validateRes;
  }
  if (streamType_ == StreamType::FETCH_HEADER) {
    // Nothing to serialize: leaving objectID out of the response is what tells
    // the subscriber it doesn't exist.
    header_.id = objectID;
    if (finStream) {
      return writeToStream(/*finStream=*/true);
    }
    return folly::unit;
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
  if (publisher_) {
    MOQ_PUBLISHER_STATS(
        publisher_->getPublisherStatsCallback(), onSubgroupReset, error);
  }
  // Cancel all delivery timeout timers for this stream since it's being reset
  if (deliveryTimer_) {
    deliveryTimer_->cancelAllTimers();
  }
  awaitingStreamCredit_ = false;
  pendingFin_ = false;
  if (streamCreditWaitId_ && publisher_) {
    publisher_->cancelUniStreamCreditWait(*streamCreditWaitId_);
  }

  if (!writeHandle_ && publisher_ && publisher_->isFill()) {
    // A fill signals failure by resetting right after the FETCH_HEADER, so
    // the header must reach the wire.
    // TODO: checkpoint the header once proxygen's WebTransport exposes
    // updateReliableDeliveryCheckpoint.
    if (ensureWriteHandle().hasValue()) {
      auto res = writeToStream(/*finStream=*/false, /*endObject=*/false);
      XLOG_IF(ERR, res.hasError())
          << "Fill FETCH_HEADER write failed before reset: "
          << res.error().describe();
    }
  }
  if (auto* wh = std::exchange(writeHandle_, nullptr)) {
    wh->resetStream(
        toWireResetStreamErrorCode(error, publisher_->getVersion()));
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
    auto wt = (streamComplete_ || !publisher_) ? nullptr
                                               : publisher_->getWebTransport();
    if (!wt) {
      return folly::makeUnexpected(
          MoQPublishError(MoQPublishError::CANCELLED, "Fetch cancelled"));
    }
    // A FETCH stream is created by its first write, so only held writes wait.
    // Not every transport can wait for credit: HTTP/3 WebTransport aborts.
    if (!awaitingStreamCredit_) {
      return folly::makeSemiFuture<uint64_t>(0);
    }
    waitForStreamCredit();
    auto [promise, future] = folly::makePromiseContract<uint64_t>();
    streamCreditReadiness_.push_back(std::move(promise));
    return std::move(future);
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
    if (cancelled_) {
      return folly::makeUnexpected(MoQPublishError(
          MoQPublishError::CANCELLED, "Write after stream cancelled"));
    }
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
  if (pendingFin_) {
    return folly::makeUnexpected(MoQPublishError(
        MoQPublishError::API_ERROR, "Write after end of FETCH"));
  }

  auto stream = wt->createUniStream();
  if (!stream) {
    XLOG(DBG4) << "No uni stream credit, holding FETCH data tp=" << this;
    awaitingStreamCredit_ = true;
    waitForStreamCredit();
    return folly::unit;
  }
  awaitingStreamCredit_ = false;
  XLOG(DBG4) << "New stream created, id: " << stream.value()->getID()
             << " tp=" << this;
  // For FETCH, ensureWrite comes from validatePublish or endOfFetch.
  // In the former case, header_ is valid and the priority is set based
  // on the first published object.  If the stream is empty, defaults are used
  stream.value()->setPriority(publisher_->elementPriority(
      header_.group,
      header_.subgroup,
      header_.priority.value_or(kDefaultPriority)));
  setWriteHandle(*stream);
  return folly::unit;
}

void StreamPublisherImpl::waitForStreamCredit() {
  if (streamCreditWaitId_) {
    return;
  }
  auto wait = publisher_->awaitUniStreamCredit();
  streamCreditWaitId_ = wait.waiterId;
  std::move(wait.credit)
      .via(publisher_->getExecutor().get())
      .thenTry([self = shared_from_this()](folly::Try<folly::Unit>&& credit) {
        self->onStreamCredit(std::move(credit));
      });
}

void StreamPublisherImpl::onStreamCredit(folly::Try<folly::Unit> credit) {
  streamCreditWaitId_.reset();
  if (credit.hasValue() && awaitingStreamCredit_ && publisher_) {
    const bool fin = std::exchange(pendingFin_, false);
    auto res = ensureWriteHandle();
    if (res && !writeHandle_) {
      // Another stream took the credit first; ensureWriteHandle waits again.
      pendingFin_ = fin;
      return;
    }
    if (res) {
      (void)writeToStream(fin, pendingEndObject_);
    }
  }
  for (auto& waiter : std::exchange(streamCreditReadiness_, {})) {
    if (credit.hasException()) {
      waiter.setException(credit.exception());
    } else {
      waiter.setValue(0);
    }
  }
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

// Fold a newer REQUEST_UPDATE into an accumulated one for draft-18+ coalescing:
// later values win; absent optional fields keep the earlier value.
void mergeRequestUpdate(RequestUpdate& latest, const RequestUpdate& incoming) {
  latest.requestID = incoming.requestID;
  latest.existingRequestID = incoming.existingRequestID;
  if (incoming.priority) {
    latest.priority = incoming.priority;
  }
  if (incoming.filter) {
    latest.filter = incoming.filter;
  }
  if (incoming.forward) {
    latest.forward = incoming.forward;
  }
  // fill is deliberately absent: it is bound to the request ID of its own
  // message, and two fill ranges do not merge.
  // Upsert params by key: a later value replaces an earlier one of that key,
  // while params only the earlier update carried are preserved.
  for (const auto& param : incoming.params) {
    latest.params.eraseAllParamsOfType(
        static_cast<TrackRequestParamKey>(param.key));
  }
  for (const auto& param : incoming.params) {
    auto result = latest.params.insertParam(param);
    if (result.hasError()) {
      XLOG(ERR) << "mergeRequestUpdate: param not allowed, key=" << param.key;
    }
  }
}

// Answers REQUEST_UPDATE batches in arrival order, because draft-18 responses
// do not carry a request ID. Updates that arrive before the pending batch is
// dispatched coalesce into it. Several batches may be in the application at
// once.
class RequestUpdateSequencer {
 public:
  using Result = folly::Expected<RequestOk, RequestError>;

  struct Batch {
    RequestUpdate update;
    std::vector<RequestID> requestIDs;
    RequestID newestRequestID;
    // Each fill stays bound to the request ID of the update that carried it.
    std::vector<std::pair<RequestID, FillParameters>> fills;
    std::optional<Result> result;
  };

  // Returns true if the caller must schedule takeQueued().
  bool add(RequestUpdate update, bool coalesce) {
    auto fill = std::exchange(update.fill, std::nullopt);
    auto requestID = update.requestID;
    if (coalesce && !queued_.empty()) {
      auto& batch = *queued_.back();
      batch.requestIDs.push_back(update.requestID);
      batch.newestRequestID = update.requestID;
      mergeRequestUpdate(batch.update, update);
    } else {
      auto batch = std::make_shared<Batch>();
      batch->requestIDs.push_back(update.requestID);
      batch->newestRequestID = update.requestID;
      batch->update = std::move(update);
      queued_.push_back(std::move(batch));
    }
    if (fill) {
      queued_.back()->fills.emplace_back(requestID, *fill);
    }
    return !std::exchange(dispatchScheduled_, true);
  }

  std::vector<std::shared_ptr<Batch>> takeQueued() {
    dispatchScheduled_ = false;
    inFlight_.insert(inFlight_.end(), queued_.begin(), queued_.end());
    return std::exchange(queued_, {});
  }

  // Returns the batches, in order, that can be answered now.
  std::vector<std::shared_ptr<Batch>> complete(
      const std::shared_ptr<Batch>& batch,
      Result result) {
    batch->result = std::move(result);
    std::vector<std::shared_ptr<Batch>> ready;
    while (!inFlight_.empty() && inFlight_.front()->result) {
      ready.push_back(std::move(inFlight_.front()));
      inFlight_.pop_front();
    }
    return ready;
  }

  void clear() {
    queued_.clear();
    inFlight_.clear();
  }

 private:
  std::vector<std::shared_ptr<Batch>> queued_;
  std::deque<std::shared_ptr<Batch>> inFlight_;
  bool dispatchScheduled_{false};
};

// Runs a handle's requestUpdate on the session executor. An exception becomes
// INTERNAL_ERROR with exceptionPhrase, or the exception text if that is empty.
template <typename HandleT>
void startRequestUpdate(
    MoQSession& session,
    std::shared_ptr<HandleT> handle,
    RequestUpdate update,
    std::string exceptionPhrase,
    folly::Function<void(RequestUpdateSequencer::Result)> done) {
  auto requestID = update.requestID;
  auto task = handle->requestUpdate(std::move(update));
  folly::coro::co_withExecutor(
      session.getExecutor(),
      folly::coro::co_withCancellation(
          session.getCancelToken(), std::move(task)))
      .start([requestID,
              // The task may outlive the publisher's reference to the handle.
              handle = std::move(handle),
              exceptionPhrase = std::move(exceptionPhrase),
              done = std::move(done)](
                 folly::Try<RequestUpdateSequencer::Result>&& result) mutable {
        if (result.hasException()) {
          auto what = result.exception().what().toStdString();
          XLOG(ERR) << "Exception in requestUpdate ex=" << what;
          done(
              folly::makeUnexpected(
                  RequestError{
                      requestID,
                      RequestErrorCode::INTERNAL_ERROR,
                      exceptionPhrase.empty() ? what : exceptionPhrase}));
          return;
        }
        done(std::move(*result));
      });
}

bool hasLargestObject(const std::vector<Parameter>& params) {
  return std::any_of(params.begin(), params.end(), [](const auto& param) {
    return param.key ==
        folly::to_underlying(TrackRequestParamKey::LARGEST_OBJECT);
  });
}

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

    // Initialize with the subscriber timeout by default
    if (deliveryTimeout.has_value()) {
      deliveryTimeoutManager_.setSubscriberTimeout(*deliveryTimeout);
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
    // Seed the largest from SUBSCRIBE_OK when present; the PUBLISH path's
    // handle carries none.
    if (handle->hasSubscribeOk()) {
      largest_ = handle->subscribeOk().largest;
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

  void updateLargest(const AbsoluteLocation& location) override {
    if (!largest_ || location > *largest_) {
      largest_ = location;
    }
  }

  void onStreamComplete(const ObjectHeader& finalHeader) override;

  void onTooManyBytesBuffered() override;

  void onRequestUpdate(RequestUpdate requestUpdate) {
    auto it = session_->pubTracks_.find(requestID_);
    if (!subscriptionHandle_ || it == session_->pubTracks_.end()) {
      XLOG(ERR) << "Received RequestUpdate before sending SUBSCRIBE_OK id="
                << requestID_ << " trackPub=" << this;

      session_->requestUpdateError(
          SubscribeUpdateError{
              requestUpdate.requestID,
              static_cast<RequestErrorCode>(
                  folly::to_underlying(SubscribeErrorCode::INTERNAL_ERROR)),
              "No subscription handle or track publisher"},
          requestID_);
      return;
    }

    bool coalesce =
        getDraftMajorVersion(*session_->getNegotiatedVersion()) >= 18;
    if (updates_.add(std::move(requestUpdate), coalesce)) {
      session_->getExecutor()->add(
          [weakSelf = std::weak_ptr<PublisherImpl>(shared_from_this())] {
            if (auto self = weakSelf.lock()) {
              std::static_pointer_cast<TrackPublisherImpl>(self)
                  ->dispatchRequestUpdates();
            }
          });
    }
  }

  void dispatchRequestUpdates() {
    for (auto& batch : updates_.takeQueued()) {
      // publishDone(), unsubscribe() or terminatePublish() may have reset the
      // handle while the dispatch was queued or while applying a prior batch.
      if (!subscriptionHandle_ || !session_) {
        updates_.clear();
        return;
      }
      applyRequestUpdate(std::move(batch));
    }
  }

  void applyRequestUpdate(
      std::shared_ptr<RequestUpdateSequencer::Batch> batch) {
    folly::RequestContextScopeGuard guard;
    session_->setRequestSession();

    auto& requestUpdate = batch->update;
    // The merged update carries the newest update's fill. A coalesced update's
    // fill goes to the handler on its own.
    for (const auto& [fillID, fill] : batch->fills) {
      expectFill(fillID, fill);
      if (fillID == batch->newestRequestID) {
        requestUpdate.fill = fill;
      } else {
        serveCoalescedFill(fillID, fill);
      }
    }
    auto existingRequestID = requestID_;

    // Update delivery timeout if present
    auto timeoutValue = MoQSession::getDeliveryTimeoutIfPresent(
        requestUpdate.params, *session_->getNegotiatedVersion());
    if (timeoutValue.has_value() && shouldApplyDeliveryTimeout(*timeoutValue)) {
      XLOG(DBG6)
          << "MoQSession::TrackPublisherImpl::handleRequestUpdate: SETTING subscriber timeout"
          << " timeout=" << *timeoutValue << "ms"
          << " requestID=" << existingRequestID;
      deliveryTimeoutManager_.setSubscriberTimeout(
          std::chrono::milliseconds(*timeoutValue));
    } else {
      XLOG(DBG6)
          << "MoQSession::TrackPublisherImpl::handleRequestUpdate: No delivery timeout in params or ignored timeout=0"
          << " requestID=" << existingRequestID;
    }

    // Only update forward state if the parameter was explicitly provided
    // Otherwise, preserve existing forward state
    if (requestUpdate.forward.has_value()) {
      setForward(*requestUpdate.forward);
    }

    startRequestUpdate(
        *session_,
        subscriptionHandle_,
        std::move(requestUpdate),
        "",
        [weakSelf = std::weak_ptr<PublisherImpl>(shared_from_this()),
         batch](RequestUpdateSequencer::Result result) mutable {
          if (auto self = weakSelf.lock()) {
            std::static_pointer_cast<TrackPublisherImpl>(self)
                ->onRequestUpdateDone(batch, std::move(result));
          }
        });
  }

  void onRequestUpdateDone(
      const std::shared_ptr<RequestUpdateSequencer::Batch>& batch,
      RequestUpdateSequencer::Result result) {
    // terminatePublish() may have reset the handle or session meanwhile.
    if (!subscriptionHandle_ || !session_) {
      return;
    }
    for (auto& ready : updates_.complete(batch, std::move(result))) {
      // A REQUEST_ERROR can end the subscription.
      if (!session_) {
        return;
      }
      respondToRequestUpdates(*ready);
    }
  }

  void respondToRequestUpdates(RequestUpdateSequencer::Batch& batch) {
    auto existingRequestID = requestID_;
    if (batch.result->hasError()) {
      // Only the newest update in a failed batch gets an error.
      auto updateErr = std::move(batch.result->error());
      XLOG(ERR) << "requestUpdate failed: " << updateErr.reasonPhrase
                << " requestID=" << existingRequestID;
      updateErr.requestID = batch.newestRequestID;
      session_->requestUpdateError(updateErr, existingRequestID);
      return;
    }
    for (auto updateRequestID : batch.requestIDs) {
      RequestOk reqOk{
          .requestID = updateRequestID,
          .requestSpecificParams = {},
          .trackProperties = {}};
      if (updateRequestID == batch.newestRequestID) {
        reqOk.requestSpecificParams =
            std::move(batch.result->value().requestSpecificParams);
      }
      // On a resume the params already hold the track's largest, which can be
      // ahead of what this subscription delivered.
      if (largest_ && !hasLargestObject(reqOk.requestSpecificParams)) {
        reqOk.requestSpecificParams.emplace_back(
            folly::to_underlying(TrackRequestParamKey::LARGEST_OBJECT),
            largest_.value());
      }
      session_->requestUpdateOk(reqOk, existingRequestID);
    }
  }

  // The handler gets a coalesced update's fill as an update that carries only
  // the fill. Its result does not change the batch's response.
  void serveCoalescedFill(RequestID fillID, const FillParameters& fill) {
    RequestUpdate update;
    update.requestID = fillID;
    update.existingRequestID = requestID_;
    update.fill = fill;
    startRequestUpdate(
        *session_,
        subscriptionHandle_,
        std::move(update),
        "",
        [weakSelf = std::weak_ptr<PublisherImpl>(shared_from_this()),
         fillID](RequestUpdateSequencer::Result result) {
          auto self = weakSelf.lock();
          if (!self || result.hasValue()) {
            return;
          }
          XLOG(ERR) << "requestUpdate failed for the fill of coalesced update "
                    << fillID;
          // The update is still acked, so the failed fill is signalled by
          // resetting its stream, unless the handler already opened it.
          auto stream =
              std::static_pointer_cast<TrackPublisherImpl>(self)->beginFill(
                  fillID);
          if (stream.hasValue()) {
            stream.value()->reset(ResetStreamErrorCode::INTERNAL_ERROR);
          }
        });
  }

  // Draft 18+ treats a DELIVERY_TIMEOUT of 0 as an explicit "no timeout" that
  // clears any previously-negotiated value,
  bool shouldApplyDeliveryTimeout(uint64_t timeoutMs) const {
    return timeoutMs > 0 ||
        getDraftMajorVersion(*session_->getNegotiatedVersion()) >= 18;
  }

  void onPublishOk(const PublishOk& publishOk) {
    auto timeoutValue = MoQSession::getDeliveryTimeoutIfPresent(
        publishOk.params, *session_->getNegotiatedVersion());
    if (timeoutValue.has_value() && shouldApplyDeliveryTimeout(*timeoutValue)) {
      deliveryTimeoutManager_.setSubscriberTimeout(
          std::chrono::milliseconds(*timeoutValue));
    }
    session_->beginSubscriptionStat(*this);
  }

  // A deferred publishDone stops writes but still sends SUBSCRIBE_OK.
  bool publishEnded() const {
    return isDone() || pendingPublishDone_.has_value();
  }

  void publishSent(const PublishRequest& publish) {
    auto timeout = getPublisherDeliveryTimeout(publish);
    if (timeout.has_value() && shouldApplyDeliveryTimeout(timeout->count())) {
      deliveryTimeoutManager_.setPublisherTimeout(*timeout);
    }
  }

  void subscribeOkSent(const SubscribeOk& subscribeOk) {
    if (!session_) {
      return;
    }
    setTrackAlias(subscribeOk.trackAlias);
    setGroupOrder(subscribeOk.groupOrder);
    auto timeout = getPublisherDeliveryTimeout(subscribeOk);
    if (timeout.has_value() && shouldApplyDeliveryTimeout(timeout->count())) {
      deliveryTimeoutManager_.setPublisherTimeout(*timeout);
    }
    session_->beginSubscriptionStat(*this);
  }

  // The handler serves a recorded fill through beginFill().
  void expectFill(RequestID requestID, const FillParameters& fill) {
    requestedFills_.emplace(requestID, fill);
  }

  void setForward(bool forward) {
    forward_ = forward;
    for (const auto& [_, subgroupPublisher] : subgroups_) {
      subgroupPublisher->setForward(forward_);
    }
  }

  void setSubscribeLocType(LocationType locType) {
    subscribeLocType_ = locType;
  }

  // Draft 16 only allows a joining FETCH against a Largest Object
  // subscription. PUBLISH-initiated subscriptions carry no SUBSCRIBE filter.
  bool allowsPreDraft18JoiningFetch() const {
    return !subscribeLocType_ ||
        *subscribeLocType_ == LocationType::LargestObject;
  }

  void unsubscribe() {
    cancelGoawayResetTimer();
    if (!subscriptionHandle_) {
      XLOG(DBG1) << "Received Unsubscribe before sending SUBSCRIBE_OK id="
                 << requestID_ << " trackPub=" << this;
    } else {
      subscriptionHandle_->unsubscribe();
    }
    resetAllSubgroups(ResetStreamErrorCode::CANCELLED);
    subscriptionHandle_.reset();
  }

  void terminatePublish(PublishDone pubDone, ResetStreamErrorCode code)
      override {
    cancelGoawayResetTimer();
    resetAllSubgroups(code);
    auto session = std::exchange(session_, nullptr);
    // PUBLISH_DONE already went out; the publisher only lingered here to drain
    // its subgroups, so there is nothing left to tell the peer. A publisher
    // that was already retired has no session to write through either.
    if (publishDoneSent() || !session) {
      subscriptionHandle_.reset();
      return;
    }
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

  void sessionClosed(ResetStreamErrorCode code) override {
    // A subscription ends with PUBLISH_DONE or an unsubscribe, and the closed
    // session cannot send PUBLISH_DONE.
    if (auto handle = std::exchange(subscriptionHandle_, nullptr)) {
      handle->unsubscribe();
    }
    // Clear session_ last: each subgroup reports onSubscriptionStreamClosed.
    resetAllSubgroups(code);
    setSession(nullptr);
  }

  void resetForGoaway(ResetStreamErrorCode code) override {
    if (!subscriptionHandle_) {
      resetAllSubgroups(code);
      if (replyContext_) {
        replyContext_->cancel(code);
      }
      if (auto session = std::exchange(session_, nullptr)) {
        session->cleanupSubscribePublisherAfterGoawayReset(requestID_);
      }
      return;
    }
    terminatePublish(
        PublishDone{
            requestID_,
            PublishDoneStatusCode::GOING_AWAY,
            streamCount_,
            "Request stream GOAWAY timeout expired"},
        code);
  }

  bool hasOpenDataStreams() const override {
    return !subgroups_.empty() || !fills_.empty();
  }

  void resetAllSubgroups(ResetStreamErrorCode code) {
    while (!subgroups_.empty()) {
      auto it = subgroups_.begin();
      // reset will invoke onStreamComplete, which erases from subgroups_
      it->second->reset(code);
    }
    resetFills(code);
  }

  // Fills live here, not in pubTracks_, which already maps this request ID.
  void addFill(std::shared_ptr<FetchPublisherImpl> fill);
  void removeFill(const FetchPublisherImpl* fill);
  void resetFills(ResetStreamErrorCode code);

  // Retire once PUBLISH_DONE is sent and the last subgroup or fill closes.
  void maybeDrain();

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

  folly::Expected<std::shared_ptr<FetchConsumer>, MoQPublishError> beginFill(
      RequestID requestID) override;

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
  std::optional<LocationType> subscribeLocType_;
  std::optional<TrackAlias> trackAlias_;
  std::optional<PublishDone> pendingPublishDone_;
  folly::F14FastMap<
      std::pair<uint64_t, uint64_t>,
      std::shared_ptr<StreamPublisherImpl>>
      subgroups_;
  folly::F14FastMap<RequestID, FillParameters, RequestID::hash> requestedFills_;
  // Fill fetch streams this subscription has open.
  std::vector<std::shared_ptr<FetchPublisherImpl>> fills_;
  uint64_t streamCount_{0};
  bool forward_;
  std::shared_ptr<DeliveryCallback> deliveryCallback_;
  MoQDeliveryTimeoutManager deliveryTimeoutManager_;
  RequestUpdateSequencer updates_;

  // Largest object published so far; advances as objects are written and is
  // reported in REQUEST_UPDATE acks. Seeded from the SUBSCRIBE_OK largest.
  std::optional<AbsoluteLocation> largest_;
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

  void setFullTrackName(FullTrackName fullTrackName) {
    fullTrackName_ = std::move(fullTrackName);
  }

  void cancel() {
    cancelGoawayResetTimer();
    cancelled_ = true;
    // reset -> onStreamComplete -> fetchComplete: handles pubTracks_.erase
    // and retireRequestID. A fill is removed from its subscription instead.
    reset(ResetStreamErrorCode::CANCELLED);
    if (auto handle = std::exchange(handle_, nullptr)) {
      handle->fetchCancel();
    }
  }

  void terminatePublish(PublishDone, ResetStreamErrorCode error) override {
    reset(error);
  }

  void sessionClosed(ResetStreamErrorCode code) override {
    // Clear streamPublisher_ first so onStreamComplete skips fetchComplete.
    // Clear session_ last: the reset reports onSubgroupReset.
    if (auto streamPublisher = std::exchange(streamPublisher_, nullptr)) {
      streamPublisher->reset(code);
    }
    setSession(nullptr);
  }

  void resetForGoaway(ResetStreamErrorCode code) override {
    // Reset the request (bidi) stream first; reset() below resets the data
    // stream and drives fetchComplete -> pubTracks_.erase + retireRequestID.
    if (replyContext_) {
      replyContext_->cancel(code);
    }
    reset(code);
  }

  void reset(ResetStreamErrorCode error) {
    if (streamPublisher_) {
      streamPublisher_->reset(error);
    }
  }

  // Set when this serves a subscription's fill range rather than a FETCH.
  void setFillOwner(const std::shared_ptr<TrackPublisherImpl>& owner) {
    fillOwner_ = owner;
    isFill_ = true;
  }

  bool isFill() const override {
    return isFill_;
  }

  void onStreamComplete(const ObjectHeader&) override {
    cancelGoawayResetTimer();
    if (!streamPublisher_) {
      // sessionClosed() cleared streamPublisher_ and retired this publisher.
      return;
    }
    streamPublisher_.reset();
    if (isFill_) {
      // A fill shares its subscription's request ID, so fetchComplete() would
      // erase the subscription's publisher from pubTracks_.
      auto self = shared_from_this();
      session_ = nullptr;
      if (auto owner = fillOwner_.lock()) {
        owner->removeFill(this);
      }
      return;
    }
    PublisherImpl::fetchComplete();
  }

  bool hasOpenDataStreams() const override {
    return streamPublisher_ != nullptr;
  }

  void onTooManyBytesBuffered() override {
    // Right now, we don't do anything when we buffer too many bytes for a
    // FETCH.
  }

  void setLogger(std::shared_ptr<MLogger> logger) {
    logger_ = logger;
  }

  void onRequestUpdate(RequestUpdate requestUpdate) {
    bool coalesce =
        getDraftMajorVersion(*session_->getNegotiatedVersion()) >= 18;
    if (updates_.add(std::move(requestUpdate), coalesce)) {
      session_->getExecutor()->add(
          [weakSelf = std::weak_ptr<PublisherImpl>(shared_from_this())] {
            if (auto self = weakSelf.lock()) {
              std::static_pointer_cast<FetchPublisherImpl>(self)
                  ->dispatchRequestUpdates();
            }
          });
    }
  }

 private:
  void dispatchRequestUpdates() {
    for (auto& batch : updates_.takeQueued()) {
      // Rejecting a prior batch can end the fetch.
      if (!session_) {
        updates_.clear();
        return;
      }
      if (!handle_) {
        rejectUninitialized(batch);
        continue;
      }
      folly::RequestContextScopeGuard guard;
      session_->setRequestSession();
      startRequestUpdate(
          *session_,
          handle_,
          std::move(batch->update),
          "Exception in requestUpdate",
          [weakSelf = std::weak_ptr<PublisherImpl>(shared_from_this()),
           batch](RequestUpdateSequencer::Result result) mutable {
            if (auto self = weakSelf.lock()) {
              std::static_pointer_cast<FetchPublisherImpl>(self)
                  ->onRequestUpdateDone(batch, std::move(result));
            }
          });
    }
  }

  void rejectUninitialized(
      const std::shared_ptr<RequestUpdateSequencer::Batch>& batch) {
    XLOG(ERR) << "Received RequestUpdate before sending FETCH_OK id="
              << requestID_ << " fetchPub=" << this;
    onRequestUpdateDone(
        batch,
        folly::makeUnexpected(
            RequestError{
                batch->newestRequestID,
                RequestErrorCode::INTERNAL_ERROR,
                "FETCH not yet initialized"}));
  }

  void onRequestUpdateDone(
      const std::shared_ptr<RequestUpdateSequencer::Batch>& batch,
      RequestUpdateSequencer::Result result) {
    if (!session_) {
      return;
    }
    for (auto& ready : updates_.complete(batch, std::move(result))) {
      if (!session_) {
        return;
      }
      respondToRequestUpdates(*ready);
    }
  }

  void respondToRequestUpdates(RequestUpdateSequencer::Batch& batch) {
    if (batch.result->hasError()) {
      // Only the newest update in a failed batch gets an error.
      auto updateErr = std::move(batch.result->error());
      updateErr.requestID = batch.newestRequestID;
      session_->requestUpdateError(updateErr, requestID_);
      return;
    }
    for (auto updateRequestID : batch.requestIDs) {
      session_->requestUpdateOk(
          RequestOk{
              .requestID = updateRequestID,
              .requestSpecificParams = {},
              .trackProperties = {}},
          requestID_);
    }
  }

  std::shared_ptr<MLogger> logger_;
  std::shared_ptr<Publisher::FetchHandle> handle_;
  std::shared_ptr<StreamPublisherImpl> streamPublisher_;
  std::weak_ptr<TrackPublisherImpl> fillOwner_;
  bool isFill_{false};
  bool cancelled_{false};
  RequestUpdateSequencer updates_;
};

// TrackPublisherImpl

void MoQSession::TrackPublisherImpl::addFill(
    std::shared_ptr<FetchPublisherImpl> fill) {
  fills_.push_back(std::move(fill));
}

void MoQSession::TrackPublisherImpl::removeFill(
    const FetchPublisherImpl* fill) {
  auto keepalive = shared_from_this();
  fills_.erase(
      std::remove_if(
          fills_.begin(),
          fills_.end(),
          [fill](const auto& f) { return f.get() == fill; }),
      fills_.end());
  maybeDrain();
}

void MoQSession::TrackPublisherImpl::resetFills(ResetStreamErrorCode code) {
  // reset() drives removeFill(), so take the list first.
  auto fills = std::exchange(fills_, {});
  for (auto& fill : fills) {
    fill->reset(code);
  }
}

folly::Expected<std::shared_ptr<FetchConsumer>, MoQPublishError>
MoQSession::TrackPublisherImpl::beginFill(RequestID requestID) {
  auto it = requestedFills_.find(requestID);
  if (it == requestedFills_.end()) {
    return folly::makeUnexpected(
        MoQPublishError(MoQPublishError::API_ERROR, "No fill requested"));
  }
  auto fill = it->second;
  requestedFills_.erase(it);
  // A paused subscription does not open a fill stream.
  if (!session_ || !forward_ || publishEnded()) {
    return folly::makeUnexpected(
        MoQPublishError(MoQPublishError::CANCELLED, "Not delivering"));
  }
  return session_->openFillStream(
      std::static_pointer_cast<TrackPublisherImpl>(shared_from_this()),
      requestID,
      fill);
}

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
  if (!wt || publishEnded()) {
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
  stream.value()->setPriority(
      elementPriority(groupID, subgroupID, pubPriority));

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
  if (!wt || publishEnded()) {
    return folly::makeUnexpected(MoQPublishError(
        MoQPublishError::API_ERROR, "awaitStreamCredit after publishDone"));
  }
  return session_->awaitUniStreamCredit().credit;
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
  maybeDrain();
}

void MoQSession::TrackPublisherImpl::maybeDrain() {
  if (session_ && isDone() && !hasOpenDataStreams()) {
    session_->publisherDrained(requestID_);
  }
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
  if (!wt || publishEnded()) {
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
  // Only advance the largest after the datagram is actually sent.
  updateLargest({header.group, header.id});
  return folly::unit;
}

folly::Expected<folly::Unit, MoQPublishError>
MoQSession::TrackPublisherImpl::publishDone(PublishDone pubDone) {
  if (publishEnded() || !session_) {
    return folly::makeUnexpected(MoQPublishError(
        MoQPublishError::API_ERROR, "publishDone twice or after close"));
  }
  cancelGoawayResetTimer();
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

  // Propagate to all existing subgroups
  for (const auto& [_, subgroupPublisher] : subgroups_) {
    subgroupPublisher->setDeliveryTimeout(newTimeout);
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

  // Marks that a draft-18 request-stream GOAWAY has been delivered for this
  // request. Returns true the first time (newly marked); false if one was
  // already delivered (a second is a protocol violation). The flag lives with
  // the per-request state, so it is freed automatically on teardown.
  bool markRequestStreamGoawaySeen() {
    return !std::exchange(requestStreamGoawaySeen_, true);
  }

 protected:
  FullTrackName fullTrackName_;
  RequestID requestID_;
  folly::CancellationSource cancelSource_;
  std::shared_ptr<BidiStreamControl> bidiControl_;
  bool requestStreamGoawaySeen_{false};
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

  // The subscription's group order, unless FILL_PARAMETERS overrides it.
  void setFillGroupOrderOverride(std::optional<GroupOrder> order) {
    fillGroupOrderOverride_ = order;
  }

  GroupOrder getFillGroupOrder() const {
    return fillGroupOrderOverride_.value_or(groupOrder_);
  }

  // The order a REQUEST_UPDATE's fill uses. The SUBSCRIBE's override does not
  // apply to it.
  GroupOrder getUpdateFillGroupOrder(const FillParameters& fill) const {
    if (fill.groupOrder && *fill.groupOrder != GroupOrder::Default) {
      return *fill.groupOrder;
    }
    return groupOrder_;
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

  // Returns false if the subscribe was already cancelled.
  bool processSubscribeOK(SubscribeOk subscribeOK) {
    alias_ = subscribeOK.trackAlias;
    groupOrder_ = subscribeOK.groupOrder;
    return subscribePromise_.trySetValue(std::move(subscribeOK));
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
    countStream();
  }

  // Draft 20 counts fill streams in PUBLISH_DONE's Stream Count.
  void onFillStream() {
    countStream();
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
  void countStream() {
    streamCount_++;
    if (pendingPublishDone_ &&
        streamCount_ >= pendingPublishDone_->streamCount) {
      deliverPublishDoneAndRemove();
    }
  }

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
  GroupOrder groupOrder_{GroupOrder::OldestFirst};
  std::optional<GroupOrder> fillGroupOrderOverride_;

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

  class StallTimeout : public quic::QuicTimerCallback {
   public:
    StallTimeout(
        std::weak_ptr<MoQSession> session,
        std::shared_ptr<MoQExecutor> exec,
        RequestID requestID,
        std::chrono::milliseconds timeout)
        : session_(std::move(session)),
          exec_(std::move(exec)),
          requestID_(requestID),
          timeout_(timeout) {}

    void restart() {
      cancelTimerCallback();
      exec_->scheduleTimeout(this, timeout_);
    }

    void timeoutExpired() noexcept override {
      exec_->add([session = session_, requestID = requestID_] {
        if (const auto alive = session.lock()) {
          alive->onFetchStalled(requestID);
        }
      });
    }

    void callbackCanceled() noexcept override {}

   private:
    const std::weak_ptr<MoQSession> session_;
    const std::shared_ptr<MoQExecutor> exec_;
    const RequestID requestID_;
    const std::chrono::milliseconds timeout_;
  };

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

  // A fill does not get a FETCH_OK, so it is established once registered.
  // owner is the subscription the fill counts against.
  void markFill(RequestID owner) {
    isFill_ = true;
    fetchEstablished_ = true;
    owningSubscribeID_ = owner;
  }

  RequestID owningSubscribeID() const {
    return owningSubscribeID_;
  }

  bool isFill() const {
    return isFill_;
  }

  void markStreamOpened() {
    streamOpened_ = true;
  }

  bool streamOpened() const {
    return streamOpened_;
  }

  // An empty fill range never opens a stream, so this reset is the answer.
  void fillNeverOpened(MoQSession* session) {
    if (auto callback = std::exchange(callback_, nullptr)) {
      callback->reset(ResetStreamErrorCode::CANCELLED);
    }
    session->fetches_.erase(requestID_);
  }

  std::shared_ptr<FetchConsumer> getFetchCallback() const {
    return callback_;
  }

  GroupOrder getFetchGroupOrder() const {
    return fetchGroupOrder_;
  }

  ResetStreamErrorCode dataStreamCancelCode() const {
    return dataStreamCancelCode_;
  }

  void dropFetchCallback() {
    callback_.reset();
  }

  void armStallTimeout(MoQSession* session, std::chrono::milliseconds timeout) {
    stallTimeout_ = std::make_unique<StallTimeout>(
        session->weak_from_this(), session->exec_, requestID_, timeout);
    stallTimeout_->restart();
  }

  void noteDataActivity() {
    if (stallTimeout_ && stallTimeout_->isTimerCallbackScheduled()) {
      stallTimeout_->restart();
    }
  }

  void cancelStallTimeout() {
    if (stallTimeout_) {
      stallTimeout_->cancelTimerCallback();
    }
  }

  void resetFetchCallback(MoQSession* session) {
    cancelStallTimeout();
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

  void cancel(
      MoQSession* session,
      ResetStreamErrorCode consumerError = ResetStreamErrorCode::CANCELLED) {
    if (isFill_) {
      // Cancelling sends STOP_SENDING on the fill's data stream.
      dataStreamCancelCode_ = ResetStreamErrorCode::CANCELLED;
    }
    cancelSource_.requestCancellation();
    // RST the bidi (e.g. fetch-stream-open timeout); peer interprets as cancel.
    if (bidiControl_) {
      bidiControl_->cancel(ResetStreamErrorCode::CANCELLED);
    }
    fetchError({requestID_, FetchErrorCode::CANCELLED, "cancelled"});
    auto callback = callback_;
    resetFetchCallback(session);
    if (callback) {
      callback->reset(consumerError);
    }
  }

  void sessionClosed() {
    cancelStallTimeout();
    if (!fetchEstablished_) {
      return;
    }
    auto callback = std::exchange(callback_, nullptr);
    if (bidiControl_) {
      bidiControl_->cancel(ResetStreamErrorCode::SESSION_CLOSED);
    }
    dataStreamCancelCode_ = ResetStreamErrorCode::SESSION_CLOSED;
    cancelSource_.requestCancellation();
    if (callback) {
      callback->reset(ResetStreamErrorCode::SESSION_CLOSED);
    }
  }

  // Returns false if the fetch was already cancelled.
  bool fetchOK(FetchOk ok) {
    XLOG(DBG1) << __func__ << " trackReceiveState=" << this;
    if (!promise_.trySetValue(std::move(ok))) {
      return false;
    }
    fetchEstablished_ = true;
    return true;
  }

  void fetchError(FetchError fetchErr) {
    if (isFill_) {
      return;
    }
    if (!promise_.isFulfilled()) {
      fetchErr.requestID = requestID_;
      promise_.setValue(folly::makeUnexpected(std::move(fetchErr)));
    } // there's likely a missing case here from shutdown
  }

  void onFetchHeader(RequestID requestID) {
    streamOpened_ = true;
    if (logger_) {
      logger_->logStreamTypeSet(
          currentStreamId_, MOQTStreamType::FETCH_HEADER, Owner::REMOTE);
      logger_->logFetchHeaderParsed(
          currentStreamId_, requestID.value, isFill());
    }
  }

  bool fetchOkAndAllDataReceived() const {
    return (isFill_ || promise_.isFulfilled()) && !callback_;
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
  ResetStreamErrorCode dataStreamCancelCode_{
      ResetStreamErrorCode::INTERNAL_ERROR};
  bool fetchEstablished_{false};
  std::unique_ptr<StallTimeout> stallTimeout_;
  bool isFill_{false};
  bool streamOpened_{false};
  RequestID owningSubscribeID_{0};
};

// Links a held joining FETCH to the SUBSCRIBE or PUBLISH it references.
// `future` completes when that subscription is established, with its track,
// or with the error the FETCH fails with.
struct MoQSession::JoiningDependency {
  JoiningDependency(RequestID subscription, RequestID fetch)
      : subscriptionID(subscription), fetchID(fetch) {
    auto contract = folly::coro::makePromiseContract<JoiningDependencyResult>();
    promise = std::move(contract.first);
    future = std::move(contract.second);
  }

  void resolve(JoiningDependencyResult result) {
    if (std::exchange(resolved, true)) {
      return;
    }
    unseenTimer.reset();
    promise.setValue(std::move(result));
  }

  RequestID subscriptionID;
  RequestID fetchID;
  folly::coro::Promise<JoiningDependencyResult> promise;
  folly::coro::Future<JoiningDependencyResult> future;
  std::unique_ptr<JoiningDependencyTimeout> unseenTimer;
  bool resolved{false};
};

class MoQSession::JoiningDependencyTimeout : public quic::QuicTimerCallback {
 public:
  JoiningDependencyTimeout(
      std::weak_ptr<MoQSession> session,
      std::weak_ptr<JoiningDependency> dependency)
      : session_(std::move(session)), dependency_(std::move(dependency)) {}

  void timeoutExpired() noexcept override {
    auto session = session_.lock();
    auto dependency = dependency_.lock();
    if (session && dependency) {
      session->onJoiningDependencyTimeout(dependency);
    }
  }

  void callbackCanceled() noexcept override {}

 private:
  std::weak_ptr<MoQSession> session_;
  std::weak_ptr<JoiningDependency> dependency_;
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
      nextPeerRequestIDForGoaway_(1) {
  controlCodec_->setTokenCache(&receiveTokenCache_);
}

MoQSession::MoQSession(
    folly::MaybeManagedPtr<proxygen::WebTransport> wt,
    ServerSetupCallback& serverSetupCallback,
    std::shared_ptr<MoQExecutor> exec,
    bool authTokenCacheEnabled)
    : dir_(MoQControlCodec::Direction::SERVER),
      wt_(std::move(wt)),
      exec_(std::move(exec)),
      controlCodec_(std::make_unique<MoQControlCodec>(dir_, this)),
      nextRequestID_(1),
      nextPeerRequestIDForGoaway_(0),
      serverSetupCallback_(&serverSetupCallback),
      authTokenCacheEnabled_(authTokenCacheEnabled),
      tokenCache_(authTokenCacheEnabled ? kMaxSendTokenCacheSize : 0) {
  // Legacy CLIENT_SETUP is parsed before the application chooses its final
  // advertised budget, so optimistic registrations need a bounded ceiling.
  receiveTokenCache_.setMaxSize(
      authTokenCacheEnabled_ ? kMaxReceiveTokenCacheSize : 0);
  controlCodec_->setTokenCache(&receiveTokenCache_);
}

MoQSession::~MoQSession() {
  cleanup();
  // An owner may release the session without close(). Loops that exec_ has
  // not started yet check this token before they touch the session.
  cancellationSource_.requestCancellation();
  if (logger_) {
    logger_->outputLogs();
  }
  XLOG(DBG1) << __func__ << " sess=" << this;
}

void MoQSession::cleanup() {
  cancelGoawayTimeout();
  for (auto& [_, waiter] : std::exchange(uniStreamCreditWaiters_, {})) {
    waiter.setException(std::runtime_error("session closed"));
  }
  auto joiningDependencies = std::move(joiningDependencies_);
  joiningDependencies_.clear();
  numJoiningDependencies_ = 0;
  for (auto& [_, dependencies] : joiningDependencies) {
    for (auto& dependency : dependencies) {
      dependency->resolve(
          folly::makeUnexpected(
              FetchError{
                  RequestID(0),
                  FetchErrorCode::INTERNAL_ERROR,
                  "session closed"}));
    }
  }
  while (!fetches_.empty()) {
    auto it = fetches_.begin();
    auto fetch = std::move(it->second);
    fetches_.erase(it);
    if (const auto& control = fetch->bidiControl()) {
      control->disarmOnPeerTermination();
    }
    fetch->sessionClosed();
  }
  while (!pubTracks_.empty()) {
    auto it = pubTracks_.begin();
    auto pubTrack = std::move(it->second);
    pubTracks_.erase(it);
    if (const auto& control = pubTrack->bidiControl()) {
      control->disarmOnPeerTermination();
    }
    endSubscriptionStat(*pubTrack);
    pubTrack->sessionClosed(ResetStreamErrorCode::SESSION_CLOSED);
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
    // Wake read loops parked in readStreamData: the transport may cancel
    // the read handle without enqueuing an error, leaking the consumer.
    sub->cancel();
  }
  subTracks_.clear();
  // We parse a publishDone after cleanup
  reqIdToTrackAlias_.clear();
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
  for (auto& [_, source] : std::exchange(inboundRequests_, {})) {
    source.requestCancellation();
  }
  bufferedDatagrams_.clear();
  // Break any shared_ptr cycle between the session and its handlers.  A
  // handler that holds a shared_ptr<MoQSession> back to this session would
  // otherwise keep both objects alive indefinitely once all external
  // references are released.
  publishHandler_.reset();
  subscribeHandler_.reset();
}

void MoQSession::spawnInboundHandler(
    RequestID requestID,
    folly::coro::Task<void> handler) {
  // cleanup() cancels every registered source. A source registered after
  // cleanup() starts out cancelled.
  auto& source = inboundRequests_[requestID];
  if (cancellationSource_.isCancellationRequested()) {
    source.requestCancellation();
  }
  co_withExecutor(
      exec_.get(), co_withCancellation(source.getToken(), std::move(handler)))
      // Weak, so that a queued handler does not keep an abandoned session
      // alive past the destructor's cancellation.
      .start([weakSelf = weak_from_this(), requestID](folly::Try<void>&&) {
        if (auto self = weakSelf.lock()) {
          self->inboundRequests_.erase(requestID);
        }
      });
}

bool MoQSession::cancelInboundRequest(RequestID requestID) {
  auto it = inboundRequests_.find(requestID);
  if (it == inboundRequests_.end()) {
    return false;
  }
  auto source = std::move(it->second);
  inboundRequests_.erase(it);
  source.requestCancellation();
  return true;
}

const folly::RequestToken& MoQSession::sessionRequestToken() {
  static folly::RequestToken token("moq_session");
  return token;
}

quic::PriorityQueue::Priority MoQSession::PublisherImpl::elementPriority(
    uint64_t groupId,
    uint64_t subgroupId,
    uint8_t pubPri) const {
  auto pri =
      getStreamPriority(groupId, subgroupId, subPriority_, pubPri, groupOrder_);
  return quic::HTTPPriorityQueue::Priority(pri.urgency, false, pri.order);
}

quic::PriorityQueue::Priority MoQSession::controlPriority() const {
  return quic::HTTPPriorityQueue::Priority(0, false, 0);
}

void MoQSession::startControlWriteLoop(
    proxygen::WebTransport::StreamWriteHandle* writeHandle) {
  writeHandle->setPriority(controlPriority());
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
            controlReadLoop(
                ReadHandleRef(this, controlStream.readHandle),
                std::move(streamData))))
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
    const auto majorVersion =
        getDraftMajorVersion(*moqFrameWriter_.getVersion());
    const bool draft18OrLater = majorVersion >= 18;
    if (majorVersion >= 18 && majorVersion < 22) {
      goaway.requestID = RequestID(nextPeerRequestIDForGoaway_);
    } else if (majorVersion >= 22) {
      goaway.requestID.reset();
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

void MoQSession::requestStreamGoaway(
    RequestID requestID,
    std::string newSessionUri,
    std::chrono::milliseconds timeout) {
  XLOG(DBG1) << __func__ << " id=" << requestID << " sess=" << this;

  // Request-stream GOAWAY (per-request migration) is a draft-18+ feature.
  auto negotiatedVersion = getNegotiatedVersion();
  if (!negotiatedVersion || getDraftMajorVersion(*negotiatedVersion) < 18) {
    XLOG(ERR) << "request-stream GOAWAY on pre-draft-18 session sess=" << this;
    return;
  }

  XCHECK_LE(newSessionUri.size(), kMaxGoawayUriLength)
      << "request-stream GOAWAY newSessionUri must not exceed 8192 bytes";

  // Spec draft-18 §10.4: only a server may advertise a new session URI. Clear
  // it rather than emit an illegal client URI; this is a local-misuse guard, so
  // do not close the session.
  if (dir_ != MoQControlCodec::Direction::SERVER && !newSessionUri.empty()) {
    XLOG(ERR) << "client may not advertise request-stream GOAWAY newSessionUri "
              << "sess=" << this;
    newSessionUri.clear();
  }

  auto it = pubTracks_.find(requestID);
  if (it == pubTracks_.end()) {
    XLOG(ERR) << "request-stream GOAWAY for unknown request id=" << requestID
              << " sess=" << this;
    return;
  }
  auto* ctx = it->second->replyContext();
  if (!ctx) {
    XLOG(ERR) << "request-stream GOAWAY: no reply context for id=" << requestID
              << " sess=" << this;
    return;
  }
  if (!it->second->markRequestStreamGoawaySent()) {
    XLOG(ERR) << "duplicate request-stream GOAWAY for id=" << requestID
              << " sess=" << this;
    return;
  }
  // Writing after the reply stream is closed is a silent no-op in the reply
  // context, so no explicit closed-stream check is needed here.

  Goaway g;
  g.newSessionUri = std::move(newSessionUri);
  g.timeout = static_cast<uint64_t>(timeout.count());
  g.requestID = std::nullopt;
  auto res =
      moqFrameWriter_.writeGoaway(ctx->writeBuf(), g, /*onRequestStream=*/true);
  if (!res) {
    XLOG(ERR) << "writeGoaway on request stream failed sess=" << this;
    return;
  }
  ctx->flush(/*fin=*/false);

  if (logger_) {
    logger_->logGoaway(g);
  }

  // Spec draft-18 §10.4: a timeout of 0 means "no specific timeout", so the
  // sender does not schedule a reset. Arm only for a positive timeout.
  if (timeout.count() > 0) {
    it->second->armGoawayResetTimer(timeout);
  }
}

void MoQSession::checkForCloseOnDrain() {
  if (draining_ && !hasOpenRequests()) {
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

// Per-request timeout for draft-18 §10.4. Owned by the PublisherImpl and
// cancelled when the request completes. The expiry callback re-hops to the
// executor and re-checks a weak_ptr to the PublisherImpl.
class MoQSession::PublisherImpl::GoawayResetTimeoutCallback
    : public quic::QuicTimerCallback {
 public:
  explicit GoawayResetTimeoutCallback(PublisherImpl& publisher)
      : publisher_(publisher) {}

  void timeoutExpired() noexcept override {
    auto exec = publisher_.getExecutor();
    if (!exec) {
      return;
    }
    auto weakPublisher = publisher_.weak_from_this();
    exec->add([weakPublisher = std::move(weakPublisher)]() mutable {
      if (auto publisher = weakPublisher.lock()) {
        publisher->onGoawayResetTimerExpired();
      }
    });
  }

  void callbackCanceled() noexcept override {
    XLOG(DBG4) << "request-stream GOAWAY reset timeout canceled pub="
               << &publisher_;
  }

 private:
  PublisherImpl& publisher_;
};

void MoQSession::PublisherImpl::armGoawayResetTimer(
    std::chrono::milliseconds timeout) {
  cancelGoawayResetTimer();
  auto exec = getExecutor();
  if (!exec) {
    XLOG(ERR) << "request-stream GOAWAY reset timer: no executor pub=" << this;
    return;
  }
  goawayResetTimer_ = std::make_unique<GoawayResetTimeoutCallback>(*this);
  goawayResetPending_ = true;
  XLOG(DBG1) << "Scheduling request-stream GOAWAY reset timeoutMs="
             << timeout.count() << " pub=" << this;
  exec->scheduleTimeout(goawayResetTimer_.get(), timeout);
}

void MoQSession::PublisherImpl::cancelGoawayResetTimer() {
  goawayResetPending_ = false;
  if (goawayResetTimer_) {
    goawayResetTimer_->cancelTimerCallback();
    goawayResetTimer_.reset();
  }
}

void MoQSession::PublisherImpl::onGoawayResetTimerExpired() {
  if (!std::exchange(goawayResetPending_, false)) {
    return;
  }
  XLOG(DBG1) << "request-stream GOAWAY timer expired, terminating request "
             << "id=" << requestID_ << " pub=" << this;
  // End this request with GOING_AWAY. Subscriptions send PUBLISH_DONE, while
  // FETCH requests reset their request and data streams.
  resetForGoaway(ResetStreamErrorCode::GOING_AWAY);
}

MoQSession::PublisherImpl::PublisherImpl(
    MoQSession* session,
    FullTrackName ftn,
    RequestID requestID,
    Priority subPriority,
    GroupOrder groupOrder,
    uint64_t version,
    uint64_t bytesBufferedThreshold)
    : session_(session),
      fullTrackName_(std::move(ftn)),
      requestID_(requestID),
      subPriority_(subPriority),
      groupOrder_(groupOrder),
      version_(version),
      bytesBufferedThreshold_(bytesBufferedThreshold) {
  moqFrameWriter_.initializeVersion(version);
}

MoQSession::PublisherImpl::~PublisherImpl() {
  cancelGoawayResetTimer();
}

void MoQSession::onGoawayTimeoutExpired() {
  if (closed_ || !draining_) {
    return;
  }
  if (!hasOpenRequests()) {
    checkForCloseOnDrain();
    return;
  }
  XLOG(DBG1) << "GOAWAY timeout expired with open requests sess=" << this;
  close(SessionCloseErrorCode::GOAWAY_TIMEOUT);
}

bool MoQSession::hasOpenRequests() const {
  return !pubTracks_.empty() || !fetches_.empty() || !subTracks_.empty();
}

void MoQSession::close(
    SessionCloseErrorCode error,
    folly::Optional<uint32_t> wtError) {
  XLOG(DBG1) << __func__ << " sess=" << this;
  if (closed_) {
    return;
  }
  auto keepAlive = weak_from_this().lock();
  closeResult_ = CloseResult{error, wtError};
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
    exec_->add([wt = std::move(wt)]() mutable { wt = nullptr; });
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
  if (!negotiatedVersion_) {
    XLOG(ERR) << "sendSetup without a negotiated version sess=" << this;
    return folly::makeUnexpected(quic::TransportErrorCode::INTERNAL_ERROR);
  }

  // Server may also call awaitPeerSetup() after sending SERVER_SETUP
  // proactively, to negotiate setup parameters with the client.
  std::tie(setupPromise_, setupFuture_) =
      folly::coro::makePromiseContract<Setup>();

  auto maxRequestID = getMaxRequestIDIfPresent(setup.params);

  setup.params.insertParam(SetupParameter(
      {folly::to_underlying(SetupKey::MOQT_IMPLEMENTATION),
       getMoQTImplementationString()}));

  uint64_t setupSerializationVersion = *negotiatedVersion_;

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
      authTokenCacheEnabled_ ? getMaxAuthTokenCacheSizeIfPresent(
                                   setup.params, setupSerializationVersion)
                             : 0,
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
    if (getMaxRequestIDIfPresent(peerParams) != 0) {
      XLOG(WARN) << "Ignoring MAX_REQUEST_ID setup param, removed in draft "
                 << getDraftMajorVersion(*negotiatedVersion_)
                 << " sess=" << this;
    }
    peerMaxRequestID_ = std::numeric_limits<uint64_t>::max();
  } else {
    peerMaxRequestID_ = getMaxRequestIDIfPresent(peerParams);
  }
}

void MoQSession::markSetupComplete() {
  setupComplete_ = true;
  if (!setupWaiters_) {
    return;
  }
  auto waiters = std::exchange(setupWaiters_, nullptr);
  auto pending = std::exchange(*waiters, {});
  for (auto& [_, promise] : pending) {
    promise.setValue();
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
  markSetupComplete();
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
        negotiatedVersion_.value_or(kVersionDraft16),
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

  if (!negotiatedVersion_) {
    XLOG(ERR) << "SERVER_SETUP without a negotiated version sess=" << this;
    close(SessionCloseErrorCode::VERSION_NEGOTIATION_FAILED);
    return;
  }

  initPeerMaxRequestID(serverSetup.params);
  auto peerAuthCacheSize = getMaxAuthTokenCacheSizeIfPresent(
      serverSetup.params, *getNegotiatedVersion());
  tokenCache_.setMaxSize(
      authTokenCacheEnabled_
          ? std::min(kMaxSendTokenCacheSize, peerAuthCacheSize)
          : 0,
      /*evict=*/true);
  setupPromise_.setValue(std::move(serverSetup));
}

void MoQSession::onClientSetup(Setup clientSetup) {
  XCHECK(dir_ == MoQControlCodec::Direction::SERVER);
  XLOG(DBG1) << __func__ << " sess=" << this;

  if (logger_) {
    logger_->logClientSetup(
        clientSetup,
        negotiatedVersion_.value_or(kVersionDraft16),
        ControlMessageType::PARSED);
  }

  auto moqtImplementation = getMoQTImplementationIfPresent(clientSetup.params);
  if (moqtImplementation) {
    XLOG(DBG1) << "Client MOQT_IMPLEMENTATION=" << *moqtImplementation
               << " sess=" << this;
  }

  initPeerMaxRequestID(clientSetup.params);
  auto peerAuthCacheSize = getMaxAuthTokenCacheSizeIfPresent(
      clientSetup.params, negotiatedVersion_.value_or(kVersionDraft16));
  tokenCache_.setMaxSize(
      authTokenCacheEnabled_
          ? std::min(kMaxSendTokenCacheSize, peerAuthCacheSize)
          : 0,
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
    XLOG(ERR) << "CLIENT_SETUP without a negotiated version sess=" << this;
    close(SessionCloseErrorCode::VERSION_NEGOTIATION_FAILED);
    return;
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
    markSetupComplete();
    replayBufferedUniStreams();
  } else {
    auto res = sendSetup(std::move(*serverSetup));
    if (res.hasError()) {
      close(SessionCloseErrorCode::VERSION_NEGOTIATION_FAILED);
      return;
    }
    markSetupComplete();
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
  // A config with no teardown of its own still needs the close to fail the
  // REQUEST_UPDATEs this end sends on the stream.
  session_->setOnPeerTermination(control_, std::move(onPeerTerminationFn_));
  replyContext_ = std::make_shared<BidiStreamReplyContext>(
      control_,
      folly::cancellation_token_merge(
          session_->cancellationSource_.getToken(),
          control_->getPeerCancelToken()));
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
    ReadHandleRef readHandle,
    proxygen::WebTransport::StreamData initialData,
    std::unique_ptr<MoQControlCodec> codec,
    std::unique_ptr<BidiRequestCallback> bidiCallback,
    std::shared_ptr<BidiStreamControl> control,
    std::unique_ptr<MoQControlCodec::ControlCallback> senderCallback) {
  XLOG(DBG1) << __func__ << " sess=" << this;
  // The session may be gone by the time exec_ starts this loop.
  co_await folly::coro::co_safe_point;
  const auto negotiatedVersion = negotiatedVersion_;
  auto g = folly::makeGuard([func = __func__, this] {
    XLOG(DBG1) << "exit " << func << " sess=" << this;
  });
  auto* controlCodec = codec ? codec.get() : controlCodec_.get();
  auto streamId = readHandle.id();
  controlCodec->setStreamId(streamId);
  // Seeded so a handed-over FIN still reaches the close handling below.
  bool fin = initialData.fin;

  // Frame callbacks, including application handlers, run inside onIngress. An
  // exception from one closes this session, not the process.
  auto onIngress = [&](std::unique_ptr<folly::IOBuf> data, bool eom) {
    auto guard = shared_from_this();
    try {
      controlCodec->onIngress(std::move(data), eom);
    } catch (const std::exception& ex) {
      XLOG(ERR) << "exception thrown from onIngress ex="
                << folly::exceptionStr(ex) << " sess=" << this;
      close(SessionCloseErrorCode::INTERNAL_ERROR);
    }
  };

  // Process any pre-buffered data first
  if (initialData.data || fin) {
    onIngress(std::move(initialData.data), fin);
  }

  bool exceptionalExit = false;
  std::optional<ResetStreamErrorCode> peerTerminationError;
  auto token = co_await folly::coro::co_current_cancellation_token;
  while (auto* handle = readHandle.get()) {
    if (token.isCancellationRequested()) {
      break;
    }
    auto streamData =
        co_await co_awaitTry(handle->readStreamData().via(exec_.get()));
    readHandle.onReadComplete(streamData);
    if (streamData.hasException()) {
      XLOG(DBG4) << folly::exceptionStr(streamData.exception())
                 << " id=" << streamId << " sess=" << this;
      exceptionalExit = true;
      if (auto* wtEx =
              streamData
                  .tryGetExceptionObject<proxygen::WebTransport::Exception>();
          negotiatedVersion && wtEx) {
        peerTerminationError =
            fromWireResetStreamErrorCode(wtEx->error, *negotiatedVersion);
      }
      break;
    }
    if (!token.isCancellationRequested() &&
        (streamData->data || streamData->fin)) {
      onIngress(std::move(streamData->data), streamData->fin);
    }
    fin = streamData->fin;
    if (fin) {
      XLOG(DBG3) << "End of stream id=" << streamId << " sess=" << this;
    }
  }
  // Once the read loop exits, no REQUEST_OK / REQUEST_ERROR can be delivered on
  // this stream again, so a sender must not register a new pending here. Marked
  // before the fail-pending block below so both close paths agree.
  if (control) {
    control->markReadLoopExited();
  }
  // TODO: close session on control exit.
  // On read-loop exit, fire responder close (RST always; FIN when
  // finIsCancellation) and fail any still-pending sender request. Both
  // suppressed during shutdown — cleanup() delivers the canonical error.
  // STOP_SENDING is handled via BidiStreamControl::writeCancelCb_.
  if (control && !token.isCancellationRequested() &&
      !cancellationSource_.isCancellationRequested() &&
      (exceptionalExit || fin)) {
    control->setPeerClose(
        exceptionalExit ? BidiStreamControl::PeerClose::Reset
                        : BidiStreamControl::PeerClose::Fin);
    if (exceptionalExit || control->finIsCancellation()) {
      control->firePeerTermination(peerTerminationError);
    }
    // Also reached from the peer-termination callback on STOP_SENDING, where
    // this loop exits under cancellation and skips the block.
    failRequestsOnStreamClose(*control);
    // Sender (bidiCallback==null) mirrors peer FIN with our FIN: "done
    // updating the request". No-op if write half already closed.
    if (!bidiCallback && fin) {
      control->writeFin();
    }
  }
  // Anything above can still call control->cancel(), so take the code last.
  // Otherwise ~ReadHandleRef sends 0.
  if (control) {
    readHandle.stopSending(control->readCancelCode());
  }
}

static std::string_view peerCloseReason(BidiStreamControl::PeerClose type) {
  switch (type) {
    case BidiStreamControl::PeerClose::Fin:
      return "peer FINed without terminal reply";
    case BidiStreamControl::PeerClose::Reset:
      return "peer reset request stream";
    case BidiStreamControl::PeerClose::StopSending:
      return "peer stopped reading the request stream";
    case BidiStreamControl::PeerClose::None:
      break;
  }
  return "request stream closed";
}

void MoQSession::setOnPeerTermination(
    const std::shared_ptr<BidiStreamControl>& control,
    folly::Function<void(RequestID, std::optional<ResetStreamErrorCode>)>
        appTeardown) {
  // STOP_SENDING cancels the read loop, so this callback is its only path.
  control->setOnPeerTermination(
      [this,
       weakSession = weak_from_this(),
       weakControl = std::weak_ptr<BidiStreamControl>(control),
       appTeardown = std::move(appTeardown)](
          RequestID id,
          std::optional<ResetStreamErrorCode> resetError) mutable {
        // The control can outlive the session inside an application handle.
        auto sessionGuard = weakSession.lock();
        if (!sessionGuard) {
          return;
        }
        if (appTeardown) {
          appTeardown(id, resetError);
        }
        if (auto controlGuard = weakControl.lock()) {
          failRequestsOnStreamClose(*controlGuard);
        }
      });
}

void MoQSession::failRequestsOnStreamClose(BidiStreamControl& control) {
  auto reason = peerCloseReason(control.peerClose());
  if (control.requestID().has_value()) {
    // No-op if the terminal reply already resolved + erased the pending.
    failPendingRequestOnEarlyClose(*control.requestID(), reason);
  }
  // Draft 18+: each REQUEST_UPDATE sent on this stream is pending under its own
  // request ID, so the fail above does not cover it.
  auto& responseIDQueue = control.responseIDQueue();
  while (!responseIDQueue.empty()) {
    failPendingRequestOnEarlyClose(responseIDQueue.front(), reason);
    responseIDQueue.pop_front();
  }
  // These may have been the last requests holding a draining session open.
  checkForCloseOnDrain();
}

void MoQSession::failPendingRequestOnEarlyClose(
    RequestID requestID,
    std::string_view reason) {
  auto it = pendingRequests_.find(requestID);
  if (it == pendingRequests_.end()) {
    return;
  }
  auto pendingState = std::move(it->second);
  pendingRequests_.erase(it);
  auto frameType = pendingState->getErrorFrameType();
  XLOG(DBG1) << "Failing pending request id=" << requestID << " " << reason
             << " sess=" << this;
  auto res = pendingState->setError(
      RequestError{
          requestID, RequestErrorCode::INTERNAL_ERROR, std::string(reason)},
      frameType);
  if (res.hasError()) {
    XLOG(ERR) << "setError failure id=" << requestID << " sess=" << this;
  }
  cleanupUnansweredRequest(*pendingState, requestID);
}

void MoQSession::cleanupUnansweredRequest(
    PendingRequestState& pendingState,
    RequestID requestID) {
  switch (pendingState.type()) {
    case PendingRequestState::Type::SUBSCRIBE_TRACK: {
      if (auto* trackPtr = pendingState.tryGetSubscribeTrack()) {
        pendingSubscribeTracks_.erase((*trackPtr)->fullTrackName());
      }
      auto aliasIt = reqIdToTrackAlias_.find(requestID);
      if (aliasIt != reqIdToTrackAlias_.end()) {
        removeSubscriptionState(aliasIt->second, requestID);
      }
      // TODO: bufferedSubgroups_ cleanup not required - batons will timeout
      // but we should clear them proactively
      break;
    }
    case PendingRequestState::Type::PUBLISH: {
      failJoiningDependencies(requestID);
      auto pubIt = pubTracks_.find(requestID);
      if (pubIt != pubTracks_.end()) {
        pendingPublishTracks_.erase(pubIt->second->fullTrackName());
        pubIt->second->setSession(nullptr);
        pubTracks_.erase(pubIt);
      }
      break;
    }
    case PendingRequestState::Type::FETCH: {
      fetches_.erase(requestID);
      break;
    }
    default:
      break;
  }
}

MoQSession::SendRequestResult MoQSession::sendRequest(
    folly::IOBufQueue& writeBuf,
    FrameType okType,
    std::vector<FrameType> postTerminal,
    RequestID requestID,
    uint64_t minBidiDraftVersion,
    std::unique_ptr<MoQControlCodec::ControlCallback> senderCallback,
    folly::Function<void(RequestID, std::optional<ResetStreamErrorCode>)>
        onPeerTermination) {
  if (getDraftMajorVersion(*negotiatedVersion_) >= minBidiDraftVersion) {
    if (!wt_) {
      return folly::makeUnexpected(
          SendRequestError{
              proxygen::WebTransport::ErrorCode::SESSION_TERMINATED,
              "closed session"});
    }
    auto bidiStream = wt_->createBidiStream();
    if (!bidiStream) {
      XLOG(ERR) << "Failed to create bidi stream sess=" << this;
      return folly::makeUnexpected(
          SendRequestError{bidiStream.error(), "Failed to create bidi stream"});
    }
    bidiStream->writeHandle->setPriority(controlPriority());
    bidiStream->writeHandle->writeStreamData(
        writeBuf.move(), /*fin=*/false, nullptr);
    auto* cb = senderCallback ? senderCallback.get() : this;
    auto control = std::make_shared<BidiStreamControl>(
        bidiStream->writeHandle,
        cancellationSource_.getToken(),
        *negotiatedVersion_,
        /*finIsCancellation=*/true);
    control->setRequestID(requestID);
    auto codec = makeBidiCodec(
        cb,
        std::move(postTerminal),
        requestID,
        okType,
        &control->responseIDQueue());
    setOnPeerTermination(control, std::move(onPeerTermination));
    auto mergedToken = folly::cancellation_token_merge(
        cancellationSource_.getToken(), control->getReadCancelToken());
    co_withExecutor(
        exec_.get(),
        co_withCancellation(
            std::move(mergedToken),
            controlReadLoop(
                ReadHandleRef(this, bidiStream->readHandle),
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
    XLOG(DBG1) << "No subscription state for track alias=" << trackAlias
               << " sess=" << this;
    return nullptr;
  }
  return trackIt->second;
}

folly::Expected<
    std::shared_ptr<MoQSession::SubscribeTrackReceiveState>,
    FetchError>
MoQSession::resolveJoiningFetch(
    RequestID requestID,
    JoiningFetch& joining,
    const FullTrackName& fullTrackName) {
  if (!joining.joiningRequestID.has_value()) {
    for (const auto& [reqID, pending] : pendingRequests_) {
      if (const auto* trackPtr = pending->tryGetSubscribeTrack()) {
        if ((*trackPtr)->fullTrackName() == fullTrackName) {
          joining.joiningRequestID = reqID;
          return *trackPtr;
        }
      }
    }
    for (const auto& [alias, state] : subTracks_) {
      if (state->fullTrackName() == fullTrackName) {
        joining.joiningRequestID = state->getRequestID();
        return state;
      }
    }
    XLOG(ERR) << "Auto joining FETCH found no match for ftn=" << fullTrackName
              << " sess=" << this;
    return folly::makeUnexpected(
        FetchError{
            requestID,
            FetchErrorCode::INTERNAL_ERROR,
            "No matching subscribe for auto joining FETCH"});
  }

  std::shared_ptr<SubscribeTrackReceiveState> state;
  auto pendingIt = pendingRequests_.find(*joining.joiningRequestID);
  if (pendingIt != pendingRequests_.end()) {
    if (auto* trackPtr = pendingIt->second->tryGetSubscribeTrack()) {
      state = *trackPtr;
    }
  } else {
    auto subIt = reqIdToTrackAlias_.find(*joining.joiningRequestID);
    if (subIt != reqIdToTrackAlias_.end()) {
      state = getSubscribeTrackReceiveState(subIt->second);
    }
  }
  if (!state) {
    XLOG(ERR) << "API error, joining FETCH for invalid requestID="
              << joining.joiningRequestID->value << " sess=" << this;
    return folly::makeUnexpected(
        FetchError{requestID, FetchErrorCode::INTERNAL_ERROR, "Invalid JSID"});
  }
  if (fullTrackName != state->fullTrackName()) {
    XLOG(ERR) << "API error, track name mismatch=" << fullTrackName << ","
              << state->fullTrackName() << " sess=" << this;
    return folly::makeUnexpected(
        FetchError{
            requestID, FetchErrorCode::INTERNAL_ERROR, "Track name mismatch"});
  }
  return state;
}

std::shared_ptr<MoQSession::FetchTrackReceiveState>
MoQSession::openFetchTrackReceiveState(RequestID requestID) {
  XLOG(DBG3) << "getTrack reqID=" << requestID;
  auto trackIt = fetches_.find(requestID);
  if (trackIt != fetches_.end()) {
    // The count can complete the subscription and drop a waiting fill.
    auto state = trackIt->second;
    if (state->isFill() && !state->streamOpened()) {
      state->markStreamOpened();
      countFillStream(*state);
    }
    return state;
  }
  if (auto fill = beginFillReceiveState(requestID)) {
    fill->markStreamOpened();
    countFillStream(*fill);
    return fill;
  }
  // received an object for unknown subscribe ID
  XLOG(ERR) << "unknown subscribe ID=" << requestID << " sess=" << this;
  return nullptr;
}

std::shared_ptr<MoQSession::SubscribeTrackReceiveState>
MoQSession::findSubscribeStateByRequestID(RequestID requestID) {
  auto aliasIt = reqIdToTrackAlias_.find(requestID);
  if (aliasIt != reqIdToTrackAlias_.end()) {
    return getSubscribeTrackReceiveState(aliasIt->second);
  }
  // The fill can arrive before SUBSCRIBE_OK, which is on another stream.
  auto pendingIt = pendingRequests_.find(requestID);
  if (pendingIt != pendingRequests_.end()) {
    if (auto* pending = pendingIt->second->tryGetSubscribeTrack()) {
      return *pending;
    }
  }
  return nullptr;
}

bool MoQSession::registerUpdateFill(
    RequestID subscribeID,
    RequestID updateID,
    const FillParameters& fill) {
  auto subState = findSubscribeStateByRequestID(subscribeID);
  if (!subState) {
    return false;
  }
  auto callback = subState->getSubscribeCallback();
  if (!callback) {
    return false;
  }
  auto consumer = callback->beginFill(updateID);
  if (!consumer || !consumer.value()) {
    XLOG(DBG1) << "Update fill refused by consumer id=" << updateID
               << " sess=" << this;
    return false;
  }
  registerFillReceiveState(
      subState->fullTrackName(),
      updateID,
      std::move(consumer.value()),
      subState->getUpdateFillGroupOrder(fill),
      subscribeID);
  return true;
}

std::shared_ptr<MoQSession::FetchTrackReceiveState>
MoQSession::beginFillReceiveState(RequestID requestID) {
  // A fill's FETCH_HEADER carries the ID of the SUBSCRIBE or REQUEST_UPDATE
  // that initiated it.
  auto subState = findSubscribeStateByRequestID(requestID);
  if (!subState) {
    return nullptr;
  }
  auto callback = subState->getSubscribeCallback();
  if (!callback) {
    return nullptr;
  }
  auto consumer = callback->beginFill(requestID);
  if (!consumer || !consumer.value()) {
    XLOG(DBG1) << "Fill refused by consumer id=" << requestID
               << " sess=" << this;
    return nullptr;
  }
  return registerFillReceiveState(
      subState->fullTrackName(),
      requestID,
      std::move(consumer.value()),
      subState->getFillGroupOrder(),
      requestID);
}

void MoQSession::countFillStream(const FetchTrackReceiveState& fill) {
  if (auto subState = findSubscribeStateByRequestID(fill.owningSubscribeID())) {
    subState->onFillStream();
  }
}

std::shared_ptr<MoQSession::FetchTrackReceiveState>
MoQSession::registerFillReceiveState(
    const FullTrackName& fullTrackName,
    RequestID requestID,
    std::shared_ptr<FetchConsumer> consumer,
    GroupOrder groupOrder,
    RequestID owningSubscribeID) {
  auto state = std::make_shared<FetchTrackReceiveState>(
      fullTrackName, requestID, std::move(consumer), groupOrder, logger_);
  state->markFill(owningSubscribeID);
  return fetches_.emplace(requestID, std::move(state)).first->second;
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
            currentStreamId_,
            trackAlias_,
            obj,
            initialPayload ? initialPayload->clone() : nullptr);
      } else if (objectComplete && fetchState_) {
        logger_->logFetchObjectParsed(
            currentStreamId_,
            obj,
            initialPayload ? initialPayload->clone() : nullptr);
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
    return toParseResult(res);
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
      return toParseResult(res);
    }
    if (objectComplete != (res.value() == ObjectPublishStatus::DONE)) {
      XLOG(ERR) << "objectPayload status does not match the stream framing,"
                << " objectComplete=" << objectComplete
                << " stream=" << currentStreamId_;
      return MoQCodec::ParseResult::ERROR_TERMINATE;
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

  // Called by read loop on read error (eg: RESET_STREAM). Fires reset() on
  // a still-open consumer even if the subscription was cancelled — the
  // MoQConsumers contract requires exactly one terminal callback.
  bool reset(ResetStreamErrorCode error) {
    if (!subscribeState_ && !fetchState_) {
      return false;
    }
    if (hasOpenConsumer()) {
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

  bool hasFetchState() const {
    return fetchState_ != nullptr;
  }

  // Returns a SemiFuture that completes when the fetch consumer is ready
  // to accept more data. Used by the read loop to co_await after BLOCKED.
  folly::SemiFuture<uint64_t> getFetchBlockedTask() const {
    auto fetchConsumer =
        fetchState_ ? fetchState_->getFetchCallback() : nullptr;
    if (!fetchConsumer) {
      return folly::makeSemiFuture<uint64_t>(folly::OperationCancelled());
    }
    auto awaitRes = fetchConsumer->awaitReadyToConsume();
    if (awaitRes.hasError()) {
      return folly::makeSemiFuture<uint64_t>(
          std::runtime_error(awaitRes.error().describe()));
    }
    return std::move(awaitRes.value());
  }

 private:
  // BLOCKED from a fetch consumer is backpressure, not failure: the read loop
  // pauses on it and resumes when the consumer is ready.  A subgroup consumer
  // has no such signal, so BLOCKED there is still fatal.
  template <typename T>
  MoQCodec::ParseResult toParseResult(
      const folly::Expected<T, MoQPublishError>& res) const {
    if (res) {
      return MoQCodec::ParseResult::CONTINUE;
    }
    return (fetchState_ && res.error().code == MoQPublishError::BLOCKED)
        ? MoQCodec::ParseResult::BLOCKED
        : MoQCodec::ParseResult::ERROR_TERMINATE;
  }

  bool isCancelled() const {
    if (fetchState_) {
      return !fetchState_->getFetchCallback();
    } else if (subscribeState_) {
      return !subgroupCallback_ || subscribeState_->isCancelled();
    }
    return true;
  }

  // Like isCancelled() but ignores subscription cancellation: a cancelled
  // subscription still owes its open consumer the terminal callback.
  bool hasOpenConsumer() const {
    if (fetchState_) {
      return fetchState_->getFetchCallback() != nullptr;
    }
    return subgroupCallback_ != nullptr;
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
    ReadHandleRef readHandle,
    proxygen::WebTransport::StreamData initialBufferedData) {
  auto streamData = std::move(initialBufferedData);
  co_await folly::coro::co_safe_point;
  auto id = readHandle.id();
  XLOG(DBG1) << __func__ << " id=" << id << " sess=" << this;
  bool isSubscriptionStream = false;
  std::shared_ptr<FetchTrackReceiveState> fetchState;
  std::optional<folly::CancellationCallback> fetchCancelCb;
  auto g = folly::makeGuard([func = __func__, this, id, &isSubscriptionStream] {
    XLOG(DBG1) << "exit " << func << " id=" << id << " sess=" << this;
    if (isSubscriptionStream) {
      onSubscriptionStreamClosedByPeer();
    }
  });

  if (!negotiatedVersion_.has_value()) {
    auto versionBaton = std::make_shared<moxygen::TimedBaton>();
    subgroupsWaitingForVersion_.push_back(versionBaton);
    // Merged token for baton waits (session + readHandle)
    auto batonWaitToken = folly::cancellation_token_merge(
        cancellationSource_.getToken(), readHandle.cancelToken());
    auto waitRes = co_await co_awaitTry(co_withCancellation(
        batonWaitToken,
        versionBaton->wait(moqSettings_.versionNegotiationTimeout)));
    if (waitRes.hasException()) {
      co_return;
    }
  }

  MoQObjectStreamCodec codec(nullptr);
  codec.initializeVersion(*negotiatedVersion_);

  // Lambda for onSubgroup
  TrackAlias deferredAlias{std::numeric_limits<uint64_t>::max()};
  uint64_t deferredGroup = 0;
  uint64_t deferredSubgroup = 0;
  std::optional<uint8_t> deferredPriority;
  SubgroupOptions deferredOptions;
  auto token = co_await folly::coro::co_current_cancellation_token;
  auto onSubgroupFunc = [this,
                         &token,
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
      XLOG(DBG4) << "State not ready for alias=" << alias;
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
  auto onFetchFunc =
      [this, &token, &codec, &fetchState, &fetchCancelCb, &readHandle](
          RequestID requestID) {
        auto state = openFetchTrackReceiveState(requestID);
        if (state) {
          // FetchTrackReceiveState lifecycle now controls read loop
          fetchState = state;
          token = state->getCancelToken();
          fetchCancelCb.emplace(token, [this, &readHandle, &fetchState] {
            if (fetchState) {
              readHandle.stopSending(toWireResetStreamErrorCode(
                  fetchState->dataStreamCancelCode(), *negotiatedVersion_));
            }
          });
          codec.setFetchGroupOrder(state->getFetchGroupOrder());
        }
        return state;
      };

  detail::ObjectStreamCallback dcb(this, onSubgroupFunc, onFetchFunc);
  if (logger_) {
    dcb.setLogger(logger_);
  }
  dcb.setCurrentStreamId(id);
  codec.setCallback(&dcb);
  codec.setStreamId(id);

  bool codecBlocked = false;
  while (!token.isCancellationRequested()) {
    // First iteration may use initialBufferedData; subsequent iterations read
    // from the stream.  While the codec holds buffered ingress from a BLOCKED
    // parse, skip the read so the next onIngress drains it instead.
    if (!codecBlocked && !streamData.data && !streamData.fin) {
      auto* handle = readHandle.get();
      if (!handle) {
        break;
      }
      auto streamDataTry = co_await co_awaitTry(
          folly::coro::co_withCancellation(
              token, handle->readStreamData().via(exec_.get())));
      readHandle.onReadComplete(streamDataTry);
      if (streamDataTry.hasException()) {
        XLOG(ERR) << folly::exceptionStr(streamDataTry.exception())
                  << " id=" << id << " sess=" << this;
        ResetStreamErrorCode errorCode{ResetStreamErrorCode::INTERNAL_ERROR};
        auto wtEx =
            streamDataTry
                .tryGetExceptionObject<proxygen::WebTransport::Exception>();
        if (wtEx) {
          errorCode =
              fromWireResetStreamErrorCode(wtEx->error, *negotiatedVersion_);
        }
        if (!dcb.reset(errorCode)) {
          XLOG(ERR) << __func__ << " terminating for unknown "
                    << "stream id=" << id << " sess=" << this;
        } else {
          MOQ_SUBSCRIBER_STATS(
              subscriberStatsCallback_, onSubgroupReset, errorCode);
        }
        break;
      }
      streamData = std::move(*streamDataTry);
    }

    // An empty read has nothing to parse.  Only re-enter the codec with no
    // bytes when it still holds ingress buffered by a BLOCKED parse.
    const bool drainBuffered = std::exchange(codecBlocked, false);
    if (!streamData.data && !streamData.fin && !drainBuffered) {
      continue;
    }

    MoQCodec::ParseResult result = MoQCodec::ParseResult::ERROR_TERMINATE;
    try {
      result = codec.onIngress(std::move(streamData.data), streamData.fin);
      if (fetchState) {
        fetchState->noteDataActivity();
      }

      // Handle BLOCKED state
      if (result == MoQCodec::ParseResult::BLOCKED) {
        auto mergedToken = folly::cancellation_token_merge(
            cancellationSource_.getToken(), readHandle.cancelToken());
        if (dcb.hasFetchState()) {
          XLOG(DBG4) << "Fetch consumer BLOCKED, awaiting readiness id=" << id;
          auto awaitRes = co_await co_awaitTry(
              folly::coro::co_withCancellation(
                  mergedToken,
                  folly::coro::detachOnCancel(
                      dcb.getFetchBlockedTask().via(exec_.get()))));
          if (awaitRes.hasException()) {
            const bool cancelled =
                awaitRes.hasException<folly::OperationCancelled>();
            XLOG_IF(ERR, !cancelled)
                << "Fetch await failed: "
                << folly::exceptionStr(awaitRes.exception()) << " id=" << id
                << " sess=" << this;
            XLOG_IF(DBG4, cancelled)
                << "Fetch await cancelled id=" << id << " sess=" << this;
            break;
          }
          result = MoQCodec::ParseResult::CONTINUE;
        } else {
          XLOG(DBG4) << "Parser returned BLOCKED, waiting for signal id=" << id;
          // Heap allocated so that a stream that does not block only costs the
          // frame a pointer.  The destructor unlinks it however this loop
          // exits.
          auto waiter = std::make_unique<AliasWaiter>(*this, deferredAlias);
          bufferedSubgroups_[deferredAlias].push_back(*waiter);
          auto waitRes = co_await co_awaitTry(co_withCancellation(
              mergedToken,
              waiter->baton.wait(moqSettings_.unknownAliasTimeout)));
          if (waitRes.hasException()) {
            XLOG(ERR) << "Timed out waiting for subscription state id=" << id
                      << " sess=" << this;
            break;
          }
          result = dcb.onSubgroup(
              deferredAlias,
              deferredGroup,
              deferredSubgroup,
              deferredPriority,
              deferredOptions);
          if (result == MoQCodec::ParseResult::BLOCKED) {
            // state was deleted (unsubscribe)
            result = MoQCodec::ParseResult::ERROR_TERMINATE;
          }
        }
        if (result == MoQCodec::ParseResult::CONTINUE) {
          codecBlocked = true;
          continue;
        }
      }
    } catch (const std::exception& ex) {
      XLOG(ERR) << "Exception in stream processing: " << folly::exceptionStr(ex)
                << " id=" << id << " sess=" << this;
      result = MoQCodec::ParseResult::ERROR_TERMINATE;
    }

    if (streamData.fin) {
      XLOG(DBG3) << "End of stream id=" << id << " sess=" << this;
      break;
    } else if (result == MoQCodec::ParseResult::ERROR_TERMINATE) {
      XLOG(ERR) << "Error parsing/consuming stream id=" << id
                << " sess=" << this;
      dcb.reset(ResetStreamErrorCode::INTERNAL_ERROR);
      break;
    }
  }
  // A consumer left open after a cancel still gets its one terminal callback.
  dcb.reset(ResetStreamErrorCode::CANCELLED);
  // ~ReadHandleRef sends STOP_SENDING if the handle is still live.
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
  trackPublisher->setSubscribeLocType(
      toSubscriptionFilter(subscribeRequest.filter).filterType);
  if (logger_) {
    trackPublisher->setLogger(logger_);
  }

  pubTracks_.emplace(requestID, trackPublisher);
  bindJoiningDependencies(requestID);

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
  spawnInboundHandler(
      requestID,
      handleSubscribe(
          std::move(subscribeRequest),
          std::move(trackPublisher),
          std::move(replyContext)));
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
  if (sub.fill) {
    trackPublisher->expectFill(requestID, *sub.fill);
  }
  auto& params = sub.params;
  const bool sendTrackProperties =
      getDraftMajorVersion(*getNegotiatedVersion()) < 22 ||
      sub.includeProperties;

  // TODO: Formalize after parameter refactor
  // We should only keep e2e params here and remove everything else
  //// Remove DELIVERY_TIMEOUT param from params before passing to
  //// publishHandler_
  params.eraseAllParamsOfType(TrackRequestParamKey::DELIVERY_TIMEOUT);

  auto token = co_await folly::coro::co_current_cancellation_token;
  auto subscribeResult = co_await co_awaitTry(publishHandler_->subscribe(
      std::move(sub), std::static_pointer_cast<TrackConsumer>(trackPublisher)));
  // The publisher can also leave pubTracks_ without a cancel, for example when
  // publishDone() runs before the handler answers.
  if (token.isCancellationRequested() ||
      pubTracks_.find(requestID) == pubTracks_.end() ||
      pubTracks_.find(requestID)->second.get() != trackPublisher.get()) {
    // The subscriber cancelled before this handle was installed.
    if (subscribeResult.hasValue() && subscribeResult->hasValue()) {
      subscribeResult->value()->unsubscribe();
    }
    failJoiningDependencies(requestID);
    co_return;
  }
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

    setPublisherPriorityFromParams(
        subOk.params, subOk.extensions, trackPublisher);
    trackPublisher->subscribeOkSent(subOk);

    if (!sendTrackProperties) {
      subOk.extensions = Extensions{};
    }

    // TODO: verify TrackAlias is unique
    sendSubscribeOk(subOk, *replyContext);
    trackPublisher->setSubscriptionHandle(std::move(subHandle));
    // The subscription handle and its Joining Location are installed, so a
    // dependent joining FETCH can be dispatched.
    resolveJoiningDependencies(requestID, fullTrackName);
  }
}

folly::Expected<std::shared_ptr<FetchConsumer>, MoQPublishError>
MoQSession::openFillStream(
    const std::shared_ptr<TrackPublisherImpl>& trackPublisher,
    RequestID requestID,
    const FillParameters& fill) {
  auto fillPublisher = std::make_shared<FetchPublisherImpl>(
      this,
      trackPublisher->fullTrackName(),
      requestID,
      fill.subscriberPriority.value_or(trackPublisher->subPriority()),
      fill.groupOrder.value_or(trackPublisher->getGroupOrder()),
      *negotiatedVersion_,
      moqSettings_.bufferingThresholds.perSubscription);
  fillPublisher->setLogger(logger_);
  fillPublisher->initialize();
  auto streamPublisher = fillPublisher->getStreamPublisher();
  if (!streamPublisher) {
    return folly::makeUnexpected(
        MoQPublishError(MoQPublishError::API_ERROR, "Fill stream failed"));
  }
  fillPublisher->setFillOwner(trackPublisher);
  trackPublisher->addFill(fillPublisher);
  // Draft 20 counts fill streams in PUBLISH_DONE's Stream Count.
  trackPublisher->onStreamCreated();
  XLOG(DBG1) << "Opening fill fetch stream id=" << requestID
             << " sess=" << this;
  return streamPublisher;
}

// Extensions are canonical; the param fallback only fires for a locally built
// message that set the raw param.
static std::optional<uint64_t> getPublisherPriorityProperty(
    const Extensions& trackProperties,
    const TrackRequestParameters& params) {
  auto publisherPriority = getPublisherPriority(trackProperties);
  if (publisherPriority) {
    return *publisherPriority;
  }
  return getFirstIntParam(params, TrackRequestParamKey::PUBLISHER_PRIORITY);
}

// nullopt leaves the priority unset, which disables elision on the publisher.
static std::optional<uint8_t> toPublisherPriority(uint64_t advertised) {
  if (advertised > kMaxPriority) {
    XLOG(WARN) << "Invalid publisher priority value: " << advertised;
    return std::nullopt;
  }
  return static_cast<uint8_t>(advertised);
}

void MoQSession::setPublisherPriorityFromParams(
    const TrackRequestParameters& params,
    const Extensions& trackProperties,
    const std::shared_ptr<TrackPublisherImpl>& trackPublisher) {
  auto advertised = getPublisherPriorityProperty(trackProperties, params);
  if (!advertised) {
    // Naming the default lets objects published at it elide the priority byte.
    trackPublisher->setPublisherPriority(kDefaultPriority);
    return;
  }
  auto publisherPriority = toPublisherPriority(*advertised);
  if (publisherPriority) {
    trackPublisher->setPublisherPriority(*publisherPriority);
  }
}

void MoQSession::setPublisherPriorityFromParams(
    const TrackRequestParameters& params,
    const Extensions& trackProperties,
    const std::shared_ptr<SubscribeTrackReceiveState>& trackReceiveState) {
  applyResolvedPublisherPriority(
      getPublisherPriorityProperty(trackProperties, params), trackReceiveState);
}

// No default needed here; the receive state reads unset as kDefaultPriority.
void MoQSession::applyResolvedPublisherPriority(
    std::optional<uint64_t> advertised,
    const std::shared_ptr<SubscribeTrackReceiveState>& trackReceiveState) {
  if (!advertised) {
    return;
  }
  auto publisherPriority = toPublisherPriority(*advertised);
  if (publisherPriority) {
    trackReceiveState->setPublisherPriority(*publisherPriority);
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

  // The update's own ID opens no request, so nothing else retires it.
  retireRequestID(/*signalWriteLoop=*/true);

  if (logger_) {
    logger_->logSubscribeUpdate(requestUpdate, ControlMessageType::PARSED);
  }

  if (shouldRejectNewPeerRequestDueToGoaway() &&
      !(moqSettings_.acceptRequestUpdatesAfterReceivedGoaway && !draining_ &&
        pubTracks_.contains(existingRequestID))) {
    XLOG(DBG1) << "Rejecting request update, GOAWAY/draining sess=" << this;
    requestUpdateError(
        RequestError{
            requestID, RequestErrorCode::GOING_AWAY, "Session going away"},
        existingRequestID);
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
    if (auto fetchPub =
            std::dynamic_pointer_cast<FetchPublisherImpl>(pubIt->second)) {
      if (requestUpdate.fill) {
        XLOG(ERR) << "FILL_PARAMETERS in a REQUEST_UPDATE for a FETCH reqID="
                  << existingRequestID << " sess=" << this;
        close(SessionCloseErrorCode::PROTOCOL_VIOLATION);
        return;
      }
      handleFetchRequestUpdate(requestUpdate, fetchPub);
      return;
    }
  }

  requestUpdateError(
      SubscribeUpdateError{
          requestID,
          RequestErrorCode::NOT_SUPPORTED,
          "REQUEST_UPDATE not supported for this message type"},
      existingRequestID);
}

void MoQSession::handleSubscribeRequestUpdate(
    RequestUpdate requestUpdate,
    std::shared_ptr<TrackPublisherImpl> trackPublisher) {
  // An update that omits priority leaves it unchanged, so only apply an
  // explicit value; otherwise the current subscriber priority is preserved.
  if (requestUpdate.priority) {
    trackPublisher->setSubPriority(*requestUpdate.priority);
  }
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

  fetchPublisher->onRequestUpdate(requestUpdate);
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
  // Holds a ref so the erase below can't destroy it out from under the stat.
  auto trackPublisher =
      std::dynamic_pointer_cast<TrackPublisherImpl>(pubTrackIt->second);
  if (!trackPublisher) {
    XLOG(ERR) << "RequestID in Unsubscribe is for a FETCH, id="
              << unsubscribe.requestID << " sess=" << this;
  } else {
    cancelInboundRequest(unsubscribe.requestID);
    trackPublisher->unsubscribe();
    // A publisher whose subgroups are still draining is in pubTracks_ even
    // though PUBLISH_DONE already went out and retired the request, so ask the
    // publisher rather than the map.
    if (!trackPublisher->publishDoneSent()) {
      endSubscriptionStat(*trackPublisher);
      retireRequestID(/*signalWriteLoop=*/true);
      publisherDrained(unsubscribe.requestID);
    } // else, the caller invoked publishDone, which isn't needed but fine
  }
  failJoiningDependencies(unsubscribe.requestID);
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

  std::optional<FullTrackName> establishedTrack;
  auto trackIt = pubTracks_.find(publishOk.requestID);
  if (trackIt != pubTracks_.end()) {
    auto trackPublisher =
        std::static_pointer_cast<TrackPublisherImpl>(trackIt->second);
    trackPublisher->onPublishOk(publishOk);
    if (getDraftMajorVersion(*getNegotiatedVersion()) < 22) {
      trackPublisher->setForward(publishOk.forward);
      trackPublisher->setGroupOrder(publishOk.groupOrder);
      trackPublisher->setSubPriority(publishOk.subscriberPriority);
    }
    pendingPublishTracks_.erase(trackPublisher->fullTrackName());
    establishedTrack = trackPublisher->fullTrackName();
  }
  // No disarm: PUBLISH_OK is non-terminal; publishDone's flush(fin) disarms.

  const auto requestID = publishOk.requestID;
  publishPtr->setValue(std::move(publishOk));
  pendingRequests_.erase(pubIt);
  // Held joining FETCHes resume after the publisher's reply continuation, so
  // a relay has recorded the subscription before it is asked to serve them.
  if (establishedTrack) {
    resolveJoiningDependencies(requestID, *establishedTrack);
  }
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
    // determine real frame type from pendingRequest
    frameType = pendingState->getErrorFrameType();

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
    cleanupUnansweredRequest(*pendingState, error.requestID);
  } else {
    if (isLocallyIssuedRequestID(error.requestID)) {
      XLOG(DBG1) << "Error for cancelled request id=" << error.requestID
                 << " sess=" << this;
      return;
    }
    XLOG(ERR) << "Request not found id=" << error.requestID << " sess=" << this;
    close(SessionCloseErrorCode::PROTOCOL_VIOLATION);
    return;
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
    if (isLocallyIssuedRequestID(subOk.requestID)) {
      XLOG(DBG1) << "SUBSCRIBE_OK for cancelled subscribe ID="
                 << subOk.requestID << " sess=" << this;
      return;
    }
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
  // Keep the pending request registered so close() can complete it on error.
  auto trackReceiveState = *trackPtr;

  if (reqIdToTrackAlias_.contains(subOk.requestID)) {
    XLOG(ERR) << "Request ID already mapped to alias reqID=" << subOk.requestID
              << " sess=" << this;
    close(SessionCloseErrorCode::PROTOCOL_VIOLATION);
    return;
  }
  if (subTracks_.contains(subOk.trackAlias)) {
    XLOG(ERR) << "TrackAlias already in use" << subOk.trackAlias
              << " sess=" << this;
    close(SessionCloseErrorCode::DUPLICATE_TRACK_ALIAS);
    return;
  }

  auto requestID = subOk.requestID;
  auto trackAlias = subOk.trackAlias;
  setPublisherPriorityFromParams(
      subOk.params, subOk.extensions, trackReceiveState);
  // If cancellation won, the caller's guard cancels the pending request.
  if (!trackReceiveState->processSubscribeOK(std::move(subOk))) {
    XLOG(DBG1) << "SUBSCRIBE_OK for cancelled subscribe ID=" << requestID
               << " sess=" << this;
    return;
  }
  reqIdToTrackAlias_.emplace(requestID, trackAlias);
  subTracks_.emplace(trackAlias, trackReceiveState);
  pendingRequests_.erase(it);
  pendingSubscribeTracks_.erase(trackReceiveState->fullTrackName());
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
    // Detach the whole list before signaling anything.
    auto ready = std::move(subgroupsIt->second);
    bufferedSubgroups_.erase(subgroupsIt);
    XLOG(DBG4) << "Signaling " << ready.size()
               << " waiters for alias=" << trackAlias;
    while (!ready.empty()) {
      ready.front().deliver();
    }
  }
}

MoQSession::AliasWaiter::~AliasWaiter() {
  if (hook.is_linked()) {
    // Unlink first; prune erases the entry only once the list is empty.
    hook.unlink();
    session_.pruneBufferedSubgroups(alias_);
  }
}

// Leave the list before signaling; the signal can resume a waiter inline.
void MoQSession::AliasWaiter::deliver() {
  hook.unlink();
  baton.signal();
}

// Erase the alias only once its last waiter unlinks; other streams may still be
// waiting on the same alias.
void MoQSession::pruneBufferedSubgroups(TrackAlias alias) {
  auto it = bufferedSubgroups_.find(alias);
  if (it != bufferedSubgroups_.end() && it->second.empty()) {
    bufferedSubgroups_.erase(it);
  }
}

class MoQSession::ReceiverSubscriptionHandle
    : public Publisher::SubscriptionHandle {
 public:
  ReceiverSubscriptionHandle(
      SubscribeOk ok,
      TrackAlias alias,
      std::shared_ptr<MoQSession> session,
      std::shared_ptr<BidiStreamControl> control = nullptr,
      bool waitForPublishOk = false)
      : Publisher::SubscriptionHandle(std::move(ok)),
        trackAlias_(alias),
        session_(std::move(session)),
        control_(std::move(control)),
        requestUpdateState_(
            waitForPublishOk ? RequestUpdateState::PENDING
                             : RequestUpdateState::READY) {
    if (!waitForPublishOk) {
      requestUpdatesReady_.setValue();
    }
  }

  void publishOkSent() {
    if (requestUpdateState_ == RequestUpdateState::PENDING) {
      requestUpdateState_ = RequestUpdateState::READY;
      requestUpdatesReady_.setValue();
    }
  }

  void publishFailed() {
    if (requestUpdateState_ == RequestUpdateState::PENDING) {
      requestUpdateState_ = RequestUpdateState::REJECTED;
      requestUpdatesReady_.setValue();
    }
  }

  folly::coro::Task<SubscriptionHandle::RequestUpdateResult> requestUpdate(
      RequestUpdate requestUpdate) override {
    auto session = session_;
    if (!session) {
      co_return folly::makeUnexpected(
          RequestError{
              requestUpdate.requestID,
              RequestErrorCode::INTERNAL_ERROR,
              "Session closed"});
    }
    auto ready = co_await folly::coro::co_awaitTry(
        folly::coro::co_withCancellation(
            session->cancellationSource_.getToken(),
            requestUpdatesReady_.getFuture()));
    if (ready.hasException()) {
      co_return folly::makeUnexpected(
          RequestError{
              requestUpdate.requestID,
              RequestErrorCode::INTERNAL_ERROR,
              "Session closed"});
    }
    if (requestUpdateState_ == RequestUpdateState::REJECTED) {
      co_return folly::makeUnexpected(
          RequestError{
              requestUpdate.requestID,
              RequestErrorCode::CANCELLED,
              "PUBLISH was not accepted"});
    }
    if (!session_) {
      co_return folly::makeUnexpected(
          RequestError{
              requestUpdate.requestID,
              RequestErrorCode::INTERNAL_ERROR,
              "Session closed"});
    }

    requestUpdate.existingRequestID = subscribeOk_->requestID;
    if (session->shouldFailNewLocalRequestDueToGoaway()) {
      co_return folly::makeUnexpected(
          RequestError{
              MoQSession::failedLocalRequestID(),
              RequestErrorCode::GOING_AWAY,
              "Session received GOAWAY"});
    }
    requestUpdate.requestID = session->getNextRequestID();

    // The update's fill is named by the update's request ID, so file its
    // receive state before the request goes out. A fill with nowhere to go is
    // not requested.
    if (requestUpdate.fill &&
        !session_->registerUpdateFill(
            subscribeOk_->requestID,
            requestUpdate.requestID,
            *requestUpdate.fill)) {
      requestUpdate.fill.reset();
    }

    // Create promise/contract for tracking the response
    auto contract = folly::coro::makePromiseContract<
        folly::Expected<RequestOk, RequestError>>();

    // Register pending request
    session->pendingRequests_.emplace(
        requestUpdate.requestID,
        PendingRequestState::makeRequestUpdate(std::move(contract.first)));

    // Send the REQUEST_UPDATE message
    session->requestUpdate(requestUpdate, control_);

    // Wait for REQUEST_OK or REQUEST_ERROR response
    // unsubscribe() can reset session_ while this waits.
    auto cancelGuard = session->cancelOnExit(requestUpdate.requestID);
    auto result = co_await std::move(contract.second);
    cancelGuard.dismiss();
    co_return result;
  }

  void unsubscribe() override {
    if (session_) {
      session_->unsubscribe({subscribeOk_->requestID}, control_);
      session_.reset();
    }
  }

  void cancelFill(RequestID requestID) override {
    if (session_) {
      // fetchCancel() sends STOP_SENDING for a fill, not FETCH_CANCEL.
      session_->fetchCancel({requestID}, nullptr);
    }
  }

 private:
  enum class RequestUpdateState { PENDING, READY, REJECTED };

  TrackAlias trackAlias_;
  std::shared_ptr<MoQSession> session_;
  std::shared_ptr<BidiStreamControl> control_;
  RequestUpdateState requestUpdateState_;
  folly::coro::SharedPromise<void> requestUpdatesReady_;
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
      std::move(control),
      /*waitForPublishOk=*/true);

  // Use single coroutine pattern like working onPublishNamespace
  auto requestID = publish.requestID;
  spawnInboundHandler(
      requestID,
      handlePublish(
          std::move(publish),
          std::move(publishHandle),
          std::move(replyContext)));
}

bool MoQSession::installPublishReceiveState(
    const FullTrackName& fullTrackName,
    RequestID requestID,
    TrackAlias alias,
    std::optional<uint64_t> publisherPriority,
    const std::shared_ptr<TrackConsumer>& consumer) {
  auto trackReceiveState = std::make_shared<SubscribeTrackReceiveState>(
      fullTrackName, requestID, consumer, this, alias, logger_, true);
  auto emplaceRes = subTracks_.try_emplace(alias, trackReceiveState);
  if (!emplaceRes.second) {
    XLOG(ERR) << "TrackAlias already in use alias=" << alias
              << " sess=" << this;
    return false;
  }
  reqIdToTrackAlias_.emplace(requestID, alias);
  applyResolvedPublisherPriority(publisherPriority, trackReceiveState);
  consumer->setTrackAlias(alias);
  deliverBufferedData(alias);
  return true;
}

folly::coro::Task<void> MoQSession::handlePublish(
    PublishRequest publish,
    std::shared_ptr<ReceiverSubscriptionHandle> publishHandle,
    std::shared_ptr<ReplyContext> replyContext) {
  co_await folly::coro::co_safe_point;
  folly::RequestContextScopeGuard guard;
  setRequestSession();
  auto publishResponseGuard =
      folly::makeGuard([publishHandle] { publishHandle->publishFailed(); });
  // Read what we need before publish is moved; copying Extensions clones IOBufs
  auto requestID = publish.requestID;
  auto alias = publish.trackAlias;
  auto ftn = publish.fullTrackName;
  auto publisherPriority =
      getPublisherPriorityProperty(publish.extensions, publish.params);

  // PubError if error occurs
  auto publishErr =
      PublishError{requestID, PublishErrorCode::INTERNAL_ERROR, ""};
  auto token = co_await folly::coro::co_current_cancellation_token;

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
      // A ready consumer takes objects before the reply arrives.
      if (initiator.consumerReady &&
          !installPublishReceiveState(
              ftn, requestID, alias, publisherPriority, initiator.consumer)) {
        close(SessionCloseErrorCode::DUPLICATE_TRACK_ALIAS);
        co_return;
      }
      // Process the async reply - this is the only async part
      auto replyResult =
          co_await folly::coro::co_awaitTry(std::move(initiator.reply));
      if (token.isCancellationRequested()) {
        co_return;
      }
      if (replyResult.hasException()) {
        XLOG(ERR) << "Exception in publish reply ex="
                  << replyResult.exception().what().toStdString();
        publishErr.reasonPhrase = replyResult.exception().what().toStdString();
      } else if (replyResult->hasError()) {
        publishErr.reasonPhrase = replyResult->error().reasonPhrase;
      } else {
        if (!initiator.consumerReady &&
            !installPublishReceiveState(
                ftn, requestID, alias, publisherPriority, initiator.consumer)) {
          close(SessionCloseErrorCode::DUPLICATE_TRACK_ALIAS);
          co_return;
        }
        // Ensure the PublishOk we send back corresponds to the inbound
        // publish request (requestID), not the republish's requestID.
        auto pubOk = replyResult->value();
        pubOk.requestID = requestID;
        publishOk(pubOk, *replyContext);
        publishHandle->publishOkSent();
        publishResponseGuard.dismiss();
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
  endUnopenedFill(id);
  checkForCloseOnDrain();
}

void MoQSession::endUnopenedFill(RequestID id) {
  // A fill of this subscription, or of one of its REQUEST_UPDATEs, that never
  // got a stream will not get one now.
  std::vector<std::shared_ptr<FetchTrackReceiveState>> unopenedFills;
  for (const auto& [_, state] : fetches_) {
    if (state->isFill() && !state->streamOpened() &&
        state->owningSubscribeID() == id) {
      unopenedFills.push_back(state);
    }
  }
  for (const auto& fill : unopenedFills) {
    fill->fillNeverOpened(this);
  }
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

bool MoQSession::isDeferrableUnseenJoiningID(
    RequestID joiningID,
    RequestID fetchID) const {
  // Draft 18 request streams can deliver the FETCH before the SUBSCRIBE it
  // references. Only a peer-issued ID that precedes the FETCH can still be in
  // flight; this is a deferral policy, not a validity rule.
  const bool peerIssued = ((joiningID.value % 2) == 1) ==
      (dir_ == MoQControlCodec::Direction::CLIENT);
  return getDraftMajorVersion(*getNegotiatedVersion()) >= 18 && peerIssued &&
      joiningID.value < fetchID.value && joiningID.value < maxRequestID_;
}

folly::Expected<std::shared_ptr<MoQSession::JoiningDependency>, folly::Unit>
MoQSession::prepareJoiningFetch(
    Fetch& fetch,
    const JoiningFetch& joining,
    ReplyContext& replyContext) {
  const auto joiningID = *joining.joiningRequestID;
  auto reject = [&](std::string reason) {
    fetchError(
        {fetch.requestID,
         FetchErrorCode::INVALID_JOINING_REQUEST_ID,
         std::move(reason)},
        replyContext);
    return folly::makeUnexpected(folly::unit);
  };
  auto joinIt = pubTracks_.find(joiningID);
  if (joinIt == pubTracks_.end()) {
    if (!isDeferrableUnseenJoiningID(joiningID, fetch.requestID)) {
      XLOG(ERR) << "Unknown joining subscribe ID=" << joiningID.value
                << " sess=" << this;
      return reject("Unknown joining requestID");
    }
    return deferJoiningFetch(
        fetch.requestID, joiningID, /*unseen=*/true, replyContext);
  }
  auto trackPublisher =
      std::dynamic_pointer_cast<TrackPublisherImpl>(joinIt->second);
  if (!trackPublisher || trackPublisher->isDone()) {
    return reject("Joining requestID is not an active subscription");
  }
  if (getDraftMajorVersion(*getNegotiatedVersion()) < 18 &&
      !trackPublisher->allowsPreDraft18JoiningFetch()) {
    XLOG(ERR) << "Joining FETCH against a non-LargestObject subscription id="
              << joiningID.value << " sess=" << this;
    close(SessionCloseErrorCode::PROTOCOL_VIOLATION);
    return folly::makeUnexpected(folly::unit);
  }
  fetch.fullTrackName = trackPublisher->fullTrackName();
  if (trackPublisher->state() == PublisherImpl::State::ESTABLISHED) {
    return nullptr;
  }
  return deferJoiningFetch(
      fetch.requestID, joiningID, /*unseen=*/false, replyContext);
}

folly::Expected<std::shared_ptr<MoQSession::JoiningDependency>, folly::Unit>
MoQSession::deferJoiningFetch(
    RequestID fetchID,
    RequestID joiningID,
    bool unseen,
    ReplyContext& replyContext) {
  if (numJoiningDependencies_ >= moqSettings_.maxDeferredJoiningFetches) {
    fetchError(
        {fetchID,
         getDraftMajorVersion(*getNegotiatedVersion()) >= 18
             ? FetchErrorCode::EXCESSIVE_LOAD
             : FetchErrorCode::INTERNAL_ERROR,
         "Too many pending joining FETCHes"},
        replyContext);
    return folly::makeUnexpected(folly::unit);
  }
  auto dependency = std::make_shared<JoiningDependency>(joiningID, fetchID);
  if (unseen) {
    dependency->unseenTimer = std::make_unique<JoiningDependencyTimeout>(
        weak_from_this(), dependency);
    exec_->scheduleTimeout(
        dependency->unseenTimer.get(), moqSettings_.joiningDependencyTimeout);
  }
  joiningDependencies_[joiningID].push_back(dependency);
  numJoiningDependencies_++;
  return dependency;
}

void MoQSession::removeJoiningDependency(
    const std::shared_ptr<JoiningDependency>& dependency) {
  dependency->unseenTimer.reset();
  auto it = joiningDependencies_.find(dependency->subscriptionID);
  if (it == joiningDependencies_.end()) {
    return;
  }
  auto& dependencies = it->second;
  auto depIt = std::find(dependencies.begin(), dependencies.end(), dependency);
  if (depIt == dependencies.end()) {
    return;
  }
  dependencies.erase(depIt);
  numJoiningDependencies_--;
  if (dependencies.empty()) {
    joiningDependencies_.erase(it);
  }
}

void MoQSession::bindJoiningDependencies(RequestID subscriptionID) {
  auto it = joiningDependencies_.find(subscriptionID);
  if (it == joiningDependencies_.end()) {
    return;
  }
  for (auto& dependency : it->second) {
    dependency->unseenTimer.reset();
  }
}

void MoQSession::resolveJoiningDependencies(
    RequestID subscriptionID,
    const JoiningDependencyResult& result) {
  auto it = joiningDependencies_.find(subscriptionID);
  if (it == joiningDependencies_.end()) {
    return;
  }
  auto dependencies = std::move(it->second);
  joiningDependencies_.erase(it);
  numJoiningDependencies_ -= dependencies.size();
  for (auto& dependency : dependencies) {
    dependency->resolve(result);
  }
}

void MoQSession::failJoiningDependencies(RequestID subscriptionID) {
  resolveJoiningDependencies(
      subscriptionID,
      folly::makeUnexpected(
          FetchError{
              RequestID(0),
              FetchErrorCode::INVALID_JOINING_REQUEST_ID,
              "Joining subscription was not established"}));
}

void MoQSession::onJoiningDependencyTimeout(
    const std::shared_ptr<JoiningDependency>& dependency) {
  XLOG(DBG1) << "Joining FETCH id=" << dependency->fetchID
             << " gave up waiting for SUBSCRIBE id="
             << dependency->subscriptionID << " sess=" << this;
  // This runs inside the timer's own callback, so it must not be destroyed
  // here.
  exec_->add([timer = std::move(dependency->unseenTimer)]() mutable {
    timer.reset();
  });
  removeJoiningDependency(dependency);
  dependency->resolve(
      folly::makeUnexpected(
          FetchError{
              RequestID(0),
              FetchErrorCode::INVALID_JOINING_REQUEST_ID,
              "Unknown joining requestID"}));
}

MoQSession::UniStreamCreditWait MoQSession::awaitUniStreamCredit() {
  if (!waitingForUniStreamCredit_) {
    auto credit = wt_->awaitUniStreamCredit();
    if (credit.isReady()) {
      return {.credit = std::move(credit)};
    }
    waitingForUniStreamCredit_ = true;
    std::move(credit)
        .via(exec_.get())
        .thenTry(
            [weakSelf = weak_from_this()](folly::Try<folly::Unit>&& result) {
              auto self = weakSelf.lock();
              if (!self) {
                return;
              }
              self->waitingForUniStreamCredit_ = false;
              for (auto& [_, waiter] :
                   std::exchange(self->uniStreamCreditWaiters_, {})) {
                if (result.hasException()) {
                  waiter.setException(result.exception());
                } else {
                  waiter.setValue();
                }
              }
            });
  }
  auto [promise, future] = folly::makePromiseContract<folly::Unit>();
  const auto waiterId = nextUniStreamCreditWaiter_++;
  uniStreamCreditWaiters_.emplace(waiterId, std::move(promise));
  return {.waiterId = waiterId, .credit = std::move(future)};
}

void MoQSession::cancelUniStreamCreditWait(uint64_t waiterId) {
  uniStreamCreditWaiters_.erase(waiterId);
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
      : folly::to<std::string>("joining=", joining->joiningRequestID->value);
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
    if (fetch.filter.needsLargest()) {
      // Both forms depend on Largest Object. Draft 20 allows a relative start,
      // but each relay would resolve it against its own Largest.
      fetchError(
          {fetch.requestID,
           FetchErrorCode::INVALID_RANGE,
           "Fetch range is relative to Largest Object"},
          *replyContext);
      return;
    }
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
  // A FETCH under this ID means no SUBSCRIBE will ever use it.
  failJoiningDependencies(fetch.requestID);
  std::shared_ptr<JoiningDependency> dependency;
  if (joining) {
    auto prepared = prepareJoiningFetch(fetch, *joining, *replyContext);
    if (prepared.hasError()) {
      return;
    }
    dependency = std::move(prepared.value());
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
  spawnInboundHandler(
      requestID,
      handleFetch(
          std::move(fetch),
          std::move(fetchPublisher),
          std::move(replyContext),
          std::move(dependency)));
}

folly::coro::Task<void> MoQSession::handleFetch(
    Fetch fetch,
    std::shared_ptr<FetchPublisherImpl> fetchPublisher,
    std::shared_ptr<ReplyContext> replyContext,
    std::shared_ptr<JoiningDependency> dependency) {
  co_await folly::coro::co_safe_point;
  folly::RequestContextScopeGuard guard;
  setRequestSession();
  auto requestID = fetch.requestID;
  const bool sendTrackProperties =
      getDraftMajorVersion(*getNegotiatedVersion()) < 22 ||
      fetch.includeProperties;
  auto token = co_await folly::coro::co_current_cancellation_token;
  if (dependency) {
    auto dependencyResult = co_await co_awaitTry(
        co_withCancellation(token, std::move(dependency->future)));
    removeJoiningDependency(dependency);
    if (dependencyResult.hasException() || token.isCancellationRequested()) {
      // FETCH_CANCEL or session close already removed the request.
      co_return;
    }
    if (dependencyResult->hasError()) {
      fetchPublisher->reset(ResetStreamErrorCode::INTERNAL_ERROR);
      auto fetchErr = std::move(dependencyResult->error());
      fetchErr.requestID = requestID;
      fetchError(fetchErr, *replyContext);
      co_return;
    }
    fetch.fullTrackName = std::move(dependencyResult->value());
    fetchPublisher->setFullTrackName(fetch.fullTrackName);
  }
  if (!fetchPublisher->getStreamPublisher() || !publishHandler_) {
    XLOG(ERR) << "Fetch Publisher killed sess=" << this;
    fetchError(
        {requestID, FetchErrorCode::INTERNAL_ERROR, "Fetch Failed"},
        *replyContext);
    co_return;
  }
  auto fetchResult = co_await co_awaitTry(publishHandler_->fetch(
      std::move(fetch), fetchPublisher->getStreamPublisher()));
  if (token.isCancellationRequested()) {
    // The fetch was cancelled before this handle was installed.
    if (fetchResult.hasValue() && fetchResult->hasValue()) {
      fetchResult->value()->fetchCancel();
    }
    co_return;
  }
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
  } else {
    auto fetchHandle = std::move(fetchResult->value());
    auto fetchOkMsg = fetchHandle->fetchOk();
    fetchOkMsg.requestID = requestID;
    if (!sendTrackProperties) {
      fetchOkMsg.extensions = Extensions{};
    }
    fetchOk(fetchOkMsg, *replyContext);
    fetchPublisher->setFetchHandle(std::move(fetchHandle));
  }
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
    cancelInboundRequest(fetchCancel.requestID);
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
    XLOG(DBG1) << "No matching subscribe ID=" << fetchOk.requestID
               << " sess=" << this;
    return;
  }
  const auto& trackReceiveState = fetchIt->second;
  auto requestID = fetchOk.requestID;
  // If cancellation won, the caller's guard cancels the pending request.
  if (!trackReceiveState->fetchOK(std::move(fetchOk))) {
    XLOG(DBG1) << "FETCH_OK for cancelled fetch ID=" << requestID
               << " sess=" << this;
    return;
  }
  // After FETCH_OK the fetch data streams drive completion; a subsequent
  // FIN/RST on the bidi is informational and must not synthesize an error.
  const auto& control = trackReceiveState->bidiControl();
  if (control) {
    control->disarmOnPeerTermination();
  }
  // The request is answered; fetches_ owns the state from here.
  pendingRequests_.erase(requestID);
  if (trackReceiveState->fetchOkAndAllDataReceived()) {
    // The data path already ran resetFetchCallback() before FETCH_OK arrived,
    // so it left without FINing. Mirror that FIN here so the bidi terminates.
    if (control) {
      control->writeFin();
    }
    fetches_.erase(fetchIt);
    checkForCloseOnDrain();
  } else if (moqSettings_.requestTimeout > std::chrono::milliseconds::zero()) {
    trackReceiveState->armStallTimeout(this, moqSettings_.requestTimeout);
  }
}

void MoQSession::onFetchStalled(RequestID requestID) {
  auto fetchIt = fetches_.find(requestID);
  if (fetchIt == fetches_.end()) {
    return;
  }
  XLOG(DBG1) << __func__ << " id=" << requestID << " sess=" << this;
  const auto state = fetchIt->second;
  const auto control = state->bidiControl();
  fetchCancel({requestID}, control, ResetStreamErrorCode::DELIVERY_TIMEOUT);
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
    auto requestID = trackStatus.requestID;
    spawnInboundHandler(
        requestID,
        handleTrackStatus(std::move(trackStatus), std::move(replyContext)));
  }
}

folly::coro::Task<void> MoQSession::handleTrackStatus(
    TrackStatus trackStatus,
    std::shared_ptr<ReplyContext> replyContext) {
  co_await folly::coro::co_safe_point;
  folly::RequestContextScopeGuard guard;
  setRequestSession();
  const bool sendTrackProperties =
      getDraftMajorVersion(*getNegotiatedVersion()) < 22 ||
      trackStatus.includeProperties;
  auto token = co_await folly::coro::co_current_cancellation_token;
  auto trackStatusResult =
      co_await co_awaitTry(publishHandler_->trackStatus(trackStatus));
  if (token.isCancellationRequested()) {
    co_return;
  }
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
    if (!sendTrackProperties) {
      trackStatOk.trackProperties = Extensions{};
    }
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
        failedLocalRequestID(),
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
      [this](RequestID id, std::optional<ResetStreamErrorCode>) {
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
            std::move(sendResult.error().reasonPhrase)});
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
  auto cancelGuard = cancelOnExit(reqID);
  auto result = co_await std::move(contract.second);
  cancelGuard.dismiss();
  co_return result;
}

void MoQSession::onTrackStatusOk(TrackStatusOk trackStatusOk) {
  XLOG(DBG1) << __func__ << " reqID=" << trackStatusOk.requestID
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
  if (negotiatedVersion) {
    const auto majorVersion = getDraftMajorVersion(*negotiatedVersion);
    if (majorVersion >= 22) {
      if (goaway.requestID.has_value()) {
        XLOG(ERR) << "Received draft-22+ GOAWAY with requestID sess=" << this;
        close(SessionCloseErrorCode::PROTOCOL_VIOLATION);
        return;
      }
    } else if (majorVersion >= 18) {
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

void MoQSession::onRequestStreamGoaway(RequestID requestID, Goaway goaway) {
  XLOG(DBG1) << __func__ << " id=" << requestID << " sess=" << this;

  if (logger_) {
    logger_->logGoaway(goaway, ControlMessageType::PARSED);
  }

  // Request-stream GOAWAY (per-request migration) is a draft-18+ feature.
  auto negotiatedVersion = getNegotiatedVersion();
  if (!negotiatedVersion || getDraftMajorVersion(*negotiatedVersion) < 18) {
    XLOG(ERR) << "request-stream GOAWAY on pre-draft-18 session sess=" << this;
    close(SessionCloseErrorCode::PROTOCOL_VIOLATION);
    return;
  }

  // Spec draft-18 §10.4: only a server may advertise a new session URI.
  if (dir_ == MoQControlCodec::Direction::SERVER &&
      !goaway.newSessionUri.empty()) {
    XLOG(ERR) << "Server received request-stream GOAWAY newSessionUri sess="
              << this;
    close(SessionCloseErrorCode::PROTOCOL_VIOLATION);
    return;
  }
  if (goaway.newSessionUri.size() > kMaxGoawayUriLength) {
    XLOG(ERR) << "request-stream GOAWAY newSessionUri too long len="
              << goaway.newSessionUri.size() << " sess=" << this;
    close(SessionCloseErrorCode::PROTOCOL_VIOLATION);
    return;
  }

  // Resolve the consumer from the stream's RequestID (goaway.requestID is unset
  // on a request stream). A SUBSCRIBE request maps via reqIdToTrackAlias_; a
  // FETCH request lives directly in fetches_.
  std::shared_ptr<TrackConsumer> trackConsumer;
  std::shared_ptr<FetchConsumer> fetchConsumer;
  TrackReceiveStateBase* receiveState = nullptr;
  auto aliasIt = reqIdToTrackAlias_.find(requestID);
  if (aliasIt != reqIdToTrackAlias_.end()) {
    if (auto state = getSubscribeTrackReceiveState(aliasIt->second)) {
      trackConsumer = state->getSubscribeCallback();
      receiveState = state.get();
    }
  } else {
    auto fetchIt = fetches_.find(requestID);
    if (fetchIt != fetches_.end()) {
      fetchConsumer = fetchIt->second->getFetchCallback();
      receiveState = fetchIt->second.get();
    }
  }

  if (!trackConsumer && !fetchConsumer) {
    // A GOAWAY racing after the app already re-issued and tore down the request
    // is expected; do not close the session.
    XLOG(DBG1) << "request-stream GOAWAY for unknown request id=" << requestID
               << " sess=" << this;
    return;
  }

  // Spec draft-18: more than one GOAWAY on a single request stream is a
  // session-level protocol violation. The dedup flag lives on the per-request
  // receive state, so it is freed automatically when the request is torn down.
  if (receiveState && !receiveState->markRequestStreamGoawaySeen()) {
    XLOG(ERR) << "Multiple request-stream GOAWAYs for request id=" << requestID
              << " sess=" << this;
    close(SessionCloseErrorCode::PROTOCOL_VIOLATION);
    return;
  }

  // Surface the migration request to the application. GOAWAY does not impact
  // subscription state; the app drives teardown.
  folly::RequestContextScopeGuard guard;
  setRequestSession();
  if (trackConsumer) {
    trackConsumer->goaway(std::move(goaway));
  } else {
    fetchConsumer->goaway(std::move(goaway));
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
            failedLocalRequestID(),
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
      [this](RequestID id, std::optional<ResetStreamErrorCode>) {
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
    auto sendError = std::move(sendResult.error());
    return folly::makeUnexpected(
        PublishError{
            pub.requestID,
            PublishErrorCode::INTERNAL_ERROR,
            std::move(sendError.reasonPhrase),
            std::nullopt,
            std::nullopt,
            std::move(sendError.webTransportError)});
  }
  if (logger_) {
    logger_->logPublish(
        pub, MOQTByteStringType::STRING_VALUE, ControlMessageType::CREATED);
  }

  uint8_t subscriberPriority = 0;
  auto initialGroupOrder = pub.groupOrder;
  if (getDraftMajorVersion(*negotiatedVersion_) >= 22) {
    subscriberPriority = static_cast<uint8_t>(
        getFirstIntParam(pub.params, TrackRequestParamKey::SUBSCRIBER_PRIORITY)
            .value_or(kDefaultPriority));
    if (auto groupOrder =
            getFirstIntParam(pub.params, TrackRequestParamKey::GROUP_ORDER)) {
      initialGroupOrder = static_cast<GroupOrder>(*groupOrder);
    }
  }

  auto trackPublisher = std::make_shared<TrackPublisherImpl>(
      this,
      pub.fullTrackName,
      pub.requestID,
      pub.trackAlias,
      subscriberPriority,
      initialGroupOrder,
      *negotiatedVersion_,
      moqSettings_.bufferingThresholds.perSubscription,
      pub.forward);

  // Record the publisher's advertised delivery timeout as the publisher source.
  trackPublisher->publishSent(pub);

  // Set publishHandle in trackPublisher so it can cancel on unsubscribes
  trackPublisher->setSubscriptionHandle(std::move(handle));

  setPublisherPriorityFromParams(pub.params, pub.extensions, trackPublisher);

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

  auto replyTask = folly::coro::co_invoke(
      [fut = std::move(contract.second)]() mutable
          -> folly::coro::Task<folly::Expected<PublishOk, PublishError>> {
        co_return co_await std::move(fut);
      });

  // Return PublishConsumerAndReplyTask immediately (no co_await). forward, not
  // the reply, gates whether the consumer accepts writes.
  return Subscriber::PublishConsumerAndReplyTask{
      std::static_pointer_cast<TrackConsumer>(trackPublisher),
      std::move(replyTask),
      /*consumerReady=*/true};
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
    return;
  }
  auto trackIt = subTracks_.find(aliasRes->second);
  if (trackIt != subTracks_.end()) {
    // Readers already holding this state keep writing unless it's cancelled.
    trackIt->second->cancel();
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

folly::coro::Task<void> MoQSession::sendFetchForFill(
    RequestID subscribeID,
    SubscribeRequest sub,
    std::shared_ptr<FetchConsumer> consumer) {
  const auto& filter = sub.fill->locationFilter.value_or(sub.filter);
  const auto priority = sub.fill->subscriberPriority.value_or(sub.priority);
  const auto groupOrder = sub.fill->groupOrder.value_or(sub.groupOrder);
  // Draft 20 fills inherit the subscription's parameters; match that.
  const std::vector<Parameter> params(sub.params.begin(), sub.params.end());
  auto start = filter.absoluteStartLocation();
  std::optional<Fetch> fetchReq;
  if (start && !filter.isOpenEnded()) {
    // An absolute range is a standalone FETCH. The publisher clamps the end
    // to Largest Object.
    auto [rangeStart, rangeEnd] = filter.fetchRange(std::nullopt);
    fetchReq.emplace(
        0,
        sub.fullTrackName,
        rangeStart,
        rangeEnd,
        priority,
        groupOrder,
        params);
  } else if (auto groupsBack = filter.relativeStartGroupsBack()) {
    // A joining FETCH counts back from the current group, one nearer than a
    // Location Filter, which counts from the next group.
    fetchReq.emplace(
        0,
        subscribeID,
        *groupsBack > 0 ? *groupsBack - 1 : 0,
        FetchType::RELATIVE_JOINING,
        priority,
        groupOrder,
        params);
  } else if (start) {
    // A Joining FETCH starts at the beginning of a Group, so a fill that starts
    // mid-Group also gets that Group's earlier Objects.
    fetchReq.emplace(
        0,
        subscribeID,
        start->group,
        FetchType::ABSOLUTE_JOINING,
        priority,
        groupOrder,
        params);
  } else {
    // The Next Object form selects nothing a fill could carry.
    consumer->reset(ResetStreamErrorCode::CANCELLED);
    co_return;
  }
  fetchReq->fullTrackName = sub.fullTrackName;

  auto result = co_await co_awaitTry(fetch(std::move(*fetchReq), consumer));
  if (result.hasException()) {
    XLOG(ERR) << "Fill FETCH failed id=" << subscribeID
              << " err=" << result.exception().what() << " sess=" << this;
  } else if (result.value().hasError()) {
    XLOG(ERR) << "Fill FETCH failed id=" << subscribeID << " code="
              << folly::to_underlying(result.value().error().errorCode)
              << " reason=" << result.value().error().reasonPhrase
              << " sess=" << this;
  } else {
    co_return;
  }
  // At every draft, a fill reports failure only by resetting.
  consumer->reset(ResetStreamErrorCode::INTERNAL_ERROR);
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
        failedLocalRequestID(),
        SubscribeErrorCode::INTERNAL_ERROR,
        "draining/closed session"};
    MOQ_SUBSCRIBER_STATS(
        subscriberStatsCallback_, onSubscribeError, subscribeError.errorCode);
    co_return folly::makeUnexpected(subscribeError);
  }
  if (shouldFailNewLocalRequestDueToGoaway()) {
    SubscribeError subscribeError = {
        failedLocalRequestID(),
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
  if (sub.fill && sub.fill->groupOrder == GroupOrder::Default) {
    sub.fill->groupOrder.reset();
  }
  if (sub.fill && !sub.fill->groupOrder &&
      sub.groupOrder == GroupOrder::Default &&
      useFillFetch(*negotiatedVersion_)) {
    // The fill stream can arrive before the SUBSCRIBE_OK that carries the
    // publisher's order, and the stream cannot be parsed without an order.
    sub.fill->groupOrder = GroupOrder::OldestFirst;
  }
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
      // GOAWAY is admitted on the reply stream so the responder can ask us to
      // migrate this request. Responder-side admission stays deferred until
      // PUBLISH-initiated migration lands.
      {FrameType::PUBLISH_DONE,
       FrameType::REQUEST_OK,
       FrameType::REQUEST_ERROR,
       FrameType::GOAWAY},
      reqID,
      /*minBidiDraftVersion=*/18,
      /*senderCallback=*/nullptr,
      // streamCount=max so in-flight subgroups flush; timeout delivers done.
      [this](RequestID id, std::optional<ResetStreamErrorCode> resetError) {
        if (!reqIdToTrackAlias_.contains(id)) {
          // No SUBSCRIBE_OK yet, so there is no subscription to end. The
          // stream teardown fails the pending SUBSCRIBE instead.
          return;
        }
        PublishDone pd;
        pd.requestID = id;
        pd.statusCode = resetError == ResetStreamErrorCode::GOING_AWAY
            ? PublishDoneStatusCode::GOING_AWAY
            : PublishDoneStatusCode::SUBSCRIPTION_ENDED;
        pd.streamCount = std::numeric_limits<uint64_t>::max();
        pd.reasonPhrase = "peer closed stream before reply";
        onPublishDone(std::move(pd));
      });
  if (sendResult.hasError()) {
    SubscribeError subscribeError = {
        reqID,
        SubscribeErrorCode::INTERNAL_ERROR,
        std::move(sendResult.error().reasonPhrase)};
    MOQ_SUBSCRIBER_STATS(
        subscriberStatsCallback_, onSubscribeError, subscribeError.errorCode);
    co_return folly::makeUnexpected(subscribeError);
  }
  auto control = std::move(sendResult.value());
  auto trackReceiveState = std::make_shared<SubscribeTrackReceiveState>(
      fullTrackName, reqID, callback, this, trackAlias, logger_);
  trackReceiveState->setBidiControl(control);
  if (sub.fill) {
    trackReceiveState->setFillGroupOrderOverride(sub.fill->groupOrder);
  }
  pendingRequests_.emplace(
      reqID, PendingRequestState::makeSubscribeTrack(trackReceiveState));
  pendingSubscribeTracks_.insert(fullTrackName);
  auto cancelGuard = cancelOnExit(reqID);
  std::shared_ptr<FetchConsumer> fillConsumer;
  if (sub.fill) {
    auto beginFillResult = callback->beginFill(reqID);
    if (beginFillResult) {
      fillConsumer = std::move(beginFillResult.value());
    }
  }
  if (fillConsumer && useFillFetch(*negotiatedVersion_)) {
    // Register up front: a PUBLISH_DONE can remove the subscription state
    // before the fill stream is read.
    registerFillReceiveState(
        fullTrackName,
        reqID,
        std::move(fillConsumer),
        sub.fill->groupOrder.value_or(sub.groupOrder),
        reqID);
    fillConsumer.reset();
  }
  if (fillConsumer) {
    // The SUBSCRIBE reply does not wait for the fill's FETCH reply. Started
    // inline so that the FETCH follows the SUBSCRIBE before the publisher can
    // end the subscription.
    co_withExecutor(
        exec_.get(),
        co_withCancellation(
            cancellationSource_.getToken(),
            folly::coro::co_invoke(
                [self = shared_from_this(),
                 reqID,
                 sub,
                 consumer = std::move(
                     fillConsumer)]() mutable -> folly::coro::Task<void> {
                  co_await self->sendFetchForFill(
                      reqID, std::move(sub), std::move(consumer));
                })))
        .startInlineUnsafe();
  }
  const auto limit = moqSettings_.requestTimeout;
  auto reply = limit > std::chrono::milliseconds::zero()
      ? folly::coro::toTask(
            folly::coro::timeout(trackReceiveState->subscribeFuture(), limit))
      : folly::coro::toTask(trackReceiveState->subscribeFuture());
  auto subscribeResultTry = co_await co_awaitTry(std::move(reply));
  if (subscribeResultTry.tryGetExceptionObject<folly::FutureTimeout>()) {
    endUnopenedFill(reqID);
    SubscribeError subscribeError = {
        reqID, SubscribeErrorCode::TIMEOUT, "request timed out"};
    MOQ_SUBSCRIBER_STATS(
        subscriberStatsCallback_, onSubscribeError, subscribeError.errorCode);
    co_return folly::makeUnexpected(subscribeError);
  }
  if (subscribeResultTry.hasException()) {
    // likely cancellation
    XLOG(ERR) << "subscribeFuture exception=" << subscribeResultTry.exception();
    MOQ_SUBSCRIBER_STATS(
        subscriberStatsCallback_,
        onSubscribeError,
        SubscribeErrorCode::INTERNAL_ERROR);
    co_yield folly::coro::co_error(subscribeResultTry.exception());
  }
  cancelGuard.dismiss();
  auto subscribeResult = subscribeResultTry.value();
  XLOG(DBG1) << "Subscribe ready trackReceiveState=" << trackReceiveState
             << " requestID=" << reqID;
  if (subscribeResult.hasError()) {
    endUnopenedFill(reqID);
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
  failJoiningDependencies(subErr.requestID);
  MOQ_PUBLISHER_STATS(
      publisherStatsCallback_, onSubscribeError, subErr.errorCode);
  // A subscribe handler may legally open subgroups and then reject; those
  // streams must not keep delivering objects for a refused subscription.
  auto it = pubTracks_.find(subErr.requestID);
  if (it != pubTracks_.end()) {
    if (auto trackPublisher =
            std::dynamic_pointer_cast<TrackPublisherImpl>(it->second)) {
      trackPublisher->resetAllSubgroups(ResetStreamErrorCode::INTERNAL_ERROR);
    }
  }
  SCOPE_EXIT {
    publisherDrained(subErr.requestID);
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
    // An inbound PUBLISH whose reply is pending has no receive state yet.
    if (!cancelInboundRequest(unsubscribe.requestID)) {
      XLOG(ERR) << "No matching subscribe ID=" << unsubscribe.requestID
                << " sess=" << this;
      return;
    }
    // Before draft 18 an UNSUBSCRIBE is not a reply to the peer's PUBLISH.
    if (control && getDraftMajorVersion(*negotiatedVersion_) >= 18) {
      control->cancel(ResetStreamErrorCode::CANCELLED);
    } else {
      publishError(
          PublishError{
              unsubscribe.requestID,
              PublishErrorCode::UNINTERESTED,
              "Unsubscribed before reply"},
          *controlStreamReplyContext());
    }
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

void MoQSession::beginSubscriptionStat(PublisherImpl& pubTrack) {
  if (pubTrack.markEstablished()) {
    MOQ_PUBLISHER_STATS(publisherStatsCallback_, onSubscriptionBegin);
  }
}

void MoQSession::endSubscriptionStat(PublisherImpl& pubTrack) {
  if (pubTrack.markDone()) {
    MOQ_PUBLISHER_STATS(publisherStatsCallback_, onSubscriptionEnd);
  }
}

void MoQSession::sendPublishDone(const PublishDone& pubDone) {
  XLOG(DBG1) << __func__ << " sess=" << this;
  auto it = pubTracks_.find(pubDone.requestID);
  if (it == pubTracks_.end()) {
    XLOG(ERR) << "publishDone for invalid id=" << pubDone.requestID
              << " sess=" << this;
    return;
  }
  MOQ_PUBLISHER_STATS(
      publisherStatsCallback_, onPublishDone, pubDone.statusCode);
  auto pubTrack = it->second;
  pubTrack->cancelGoawayResetTimer();
  endSubscriptionStat(*pubTrack);
  // A failed write still ends the request. The publisher can outlive this call
  // draining its subgroups, and teardown must not answer the request again.
  pubTrack->markPublishDoneSent();
  auto* ctx = pubTrack->replyContext();
  // Subgroups opened before PUBLISH_DONE may still be draining. They keep
  // reporting through the session, so the write-side entry outlives the
  // protocol request; onStreamComplete retires it when the last one ends.
  SCOPE_EXIT {
    // A PUBLISH torn down before its PUBLISH_OK never cleared this. Keyed by
    // track name, so this also clears a concurrent PUBLISH's marker -- revisit
    // when the draft allows multiple subscriptions per track.
    pendingPublishTracks_.erase(pubTrack->fullTrackName());
    if (!pubTrack->hasOpenDataStreams()) {
      publisherDrained(pubDone.requestID);
    }
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
    bool terminateExisting) {
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

  if (terminateExisting) {
    // Tear down the failed request (regardless of REQUEST_ERROR write success).
    terminateExistingRequest(existingRequestID, requestError);
  }
}

void MoQSession::terminateExistingRequest(
    RequestID existingRequestID,
    const SubscribeUpdateError& requestError) {
  // Terminate subscription with PUBLISH_DONE (UPDATE_FAILED) and clean up
  // publisher state; for a FETCH terminatePublish resets the FETCH data stream.
  auto it = pubTracks_.find(existingRequestID);
  if (it != pubTracks_.end()) {
    PublishDone pubDone{
        existingRequestID,
        PublishDoneStatusCode::UPDATE_FAILED,
        static_cast<uint64_t>(requestError.errorCode),
        requestError.reasonPhrase};
    // terminatePublish can retire the publisher and drop the map's reference
    // to it before returning, so hold one for the duration of the call.
    auto pubTrack = it->second;
    cancelInboundRequest(existingRequestID);
    pubTrack->terminatePublish(pubDone, ResetStreamErrorCode::CANCELLED);
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

void MoQSession::publisherDrained(RequestID requestID) {
  auto it = pubTracks_.find(requestID);
  if (it != pubTracks_.end()) {
    XLOG(DBG1) << __func__ << " id=" << requestID << " sess=" << this;
    auto pubTrack = std::move(it->second);
    pubTracks_.erase(it);
    pubTrack->setSession(nullptr);
  }
  checkForCloseOnDrain();
}

void MoQSession::cleanupSubscribePublisherAfterGoawayReset(
    RequestID requestID) {
  XLOG(DBG1) << __func__ << " id=" << requestID << " sess=" << this;
  auto it = pubTracks_.find(requestID);
  if (it == pubTracks_.end()) {
    return;
  }
  // A publisher that sent PUBLISH_DONE was accounted for then, and is only
  // still here to drain its subgroups.
  if (!it->second->publishDoneSent()) {
    endSubscriptionStat(*it->second);
    retireRequestID(/*signalWriteLoop=*/true);
  }
  publisherDrained(requestID);
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
  if (useFillFetch(negotiatedVersion_.value_or(0)) &&
      std::holds_alternative<JoiningFetch>(fetch.args)) {
    // Draft 20 replaces Joining FETCH with a fill.
    FetchError fetchError = {
        fetch.requestID,
        FetchErrorCode::NOT_SUPPORTED,
        "Joining FETCH retired, use a fill"};
    MOQ_SUBSCRIBER_STATS(
        subscriberStatsCallback_, onFetchError, fetchError.errorCode);
    co_return folly::makeUnexpected(fetchError);
  }
  if (draining_ || closed_) {
    FetchError fetchError = {
        failedLocalRequestID(),
        FetchErrorCode::INTERNAL_ERROR,
        "draining/closed session"};
    MOQ_SUBSCRIBER_STATS(
        subscriberStatsCallback_, onFetchError, fetchError.errorCode);
    co_return folly::makeUnexpected(fetchError);
  }
  if (shouldFailNewLocalRequestDueToGoaway()) {
    FetchError fetchError = {
        failedLocalRequestID(),
        FetchErrorCode::GOING_AWAY,
        "Session received GOAWAY"};
    MOQ_SUBSCRIBER_STATS(
        subscriberStatsCallback_, onFetchError, fetchError.errorCode);
    co_return folly::makeUnexpected(fetchError);
  }

  auto [standalone, joining] = fetchType(fetch);
  FullTrackName fullTrackName = fetch.fullTrackName;
  auto reqID = getNextRequestID();
  fetch.requestID = reqID;
  if (joining) {
    // May populate joining->joiningRequestID when std::nullopt is passed.
    auto stateResult = resolveJoiningFetch(reqID, *joining, fullTrackName);
    if (stateResult.hasError()) {
      MOQ_SUBSCRIBER_STATS(
          subscriberStatsCallback_,
          onFetchError,
          stateResult.error().errorCode);
      co_return folly::makeUnexpected(stateResult.error());
    }
  }
  aliasifyAuthTokens(fetch.params);

  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  auto writeResult = moqFrameWriter_.writeFetch(writeBuf, fetch);
  if (!writeResult) {
    XLOG(ERR) << "writeFetch failed sess=" << this;
    FetchError fetchError = {
        reqID, FetchErrorCode::INTERNAL_ERROR, "local write failed"};
    MOQ_SUBSCRIBER_STATS(
        subscriberStatsCallback_, onFetchError, fetchError.errorCode);
    co_return folly::makeUnexpected(fetchError);
  }
  auto sendResult = sendRequest(
      writeBuf,
      FrameType::FETCH_OK,
      // GOAWAY is admitted here too; responder-side admission is deferred until
      // PUBLISH-initiated migration lands.
      /*postTerminal=*/
      {FrameType::REQUEST_OK, FrameType::REQUEST_ERROR, FrameType::GOAWAY},
      reqID,
      /*minBidiDraftVersion=*/18,
      /*senderCallback=*/nullptr,
      // After FETCH_OK we disarm in onFetchOk so the data streams own
      // completion.
      [this](RequestID id, std::optional<ResetStreamErrorCode>) {
        auto fetchIt = fetches_.find(id);
        if (fetchIt == fetches_.end()) {
          return;
        }
        fetchIt->second->fetchError(
            {id, FetchErrorCode::CANCELLED, "peer closed stream before reply"});
      });
  if (sendResult.hasError()) {
    FetchError fetchError = {
        reqID,
        FetchErrorCode::INTERNAL_ERROR,
        std::move(sendResult.error().reasonPhrase)};
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
  auto cancelGuard = cancelOnExit(reqID);
  auto reply = trackReceiveState->fetchFuture();
  const auto limit = moqSettings_.requestTimeout;
  folly::Try<FetchTrackReceiveState::FetchResult> fetchResultTry;
  if (limit > std::chrono::milliseconds::zero()) {
    fetchResultTry =
        co_await co_awaitTry(folly::coro::timeout(std::move(reply), limit));
  } else {
    fetchResultTry = co_await co_awaitTry(std::move(reply));
  }
  if (fetchResultTry.tryGetExceptionObject<folly::FutureTimeout>()) {
    trackReceiveState->dropFetchCallback();
    FetchError fetchError = {
        reqID, FetchErrorCode::TIMEOUT, "request timed out"};
    MOQ_SUBSCRIBER_STATS(
        subscriberStatsCallback_, onFetchError, fetchError.errorCode);
    co_return folly::makeUnexpected(fetchError);
  }
  auto fetchResult = std::move(fetchResultTry).value();
  cancelGuard.dismiss();
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

void MoQSession::cancelLocalRequest(RequestID requestID) {
  auto it = pendingRequests_.find(requestID);
  if (it == pendingRequests_.end()) {
    return;
  }
  auto pending = std::move(it->second);
  pendingRequests_.erase(it);
  const auto& control = pending->bidiControl();
  // Sending the cancel can re-enter and close the session, so local state is
  // cleared first.
  switch (pending->type()) {
    case PendingRequestState::Type::FETCH:
      fetchCancel({requestID}, control);
      return;
    case PendingRequestState::Type::SUBSCRIBE_TRACK: {
      const auto& trackReceiveState = *pending->tryGetSubscribeTrack();
      trackReceiveState->cancel();
      cleanupUnansweredRequest(*pending, requestID);
      if (!control &&
          moqFrameWriter_.writeUnsubscribe(
              controlWriteBuf_, Unsubscribe{requestID})) {
        controlWriteEvent_.signal();
      }
      break;
    }
    case PendingRequestState::Type::PUBLISH:
    case PendingRequestState::Type::REQUEST_UPDATE:
      // A REQUEST_UPDATE has no stream of its own. A PUBLISH ends through the
      // consumer returned to its caller.
      return;
    case PendingRequestState::Type::TRACK_STATUS:
    case PendingRequestState::Type::PUBLISH_NAMESPACE:
    case PendingRequestState::Type::SUBSCRIBE_NAMESPACE:
    case PendingRequestState::Type::SUBSCRIBE_TRACKS:
      break;
  }
  if (control) {
    control->cancel(ResetStreamErrorCode::CANCELLED);
  }
}

void MoQSession::fetchCancel(
    const FetchCancel& fetchCan,
    const std::shared_ptr<BidiStreamControl>& control,
    ResetStreamErrorCode consumerError) {
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
  auto isFill = trackIt->second->isFill();
  trackIt->second->cancel(this, consumerError);
  if (isFill) {
    // A FETCH_CANCEL would name the subscription's request ID. The
    // STOP_SENDING that cancel() triggers is the whole cancel.
    return;
  }
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
      0,            // will be picked by fetch()
      std::nullopt, // resolved by FullTrackName match in resolveJoiningFetch
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
  ReadHandleRef readHandle(this, rh);
  if (!setupComplete_) {
    if (negotiatedVersion_ && useUniControlStreams(*negotiatedVersion_)) {
      // In uni control mode, uni streams before setup may carry the peer's
      // control stream (with SETUP)
      co_withExecutor(
          exec_.get(),
          co_withCancellation(
              cancellationSource_.getToken(),
              handlePreSetupUniStream(
                  shared_from_this(), std::move(readHandle))))
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
          dataStreamReadLoop(shared_from_this(), std::move(readHandle))))
      .start();
}

void MoQSession::onNewBidiStream(
    proxygen::WebTransport::BidiStreamHandle bh) noexcept {
  XLOG(DBG1) << __func__ << " sess=" << this;
  ReadHandleRef readHandle(this, bh.readHandle);

  // The version comes from the ALPN or WT protocol. A transport that let the
  // peer in without one must not bring the process down.
  if (!negotiatedVersion_) {
    XLOG(ERR) << "Bidi stream without a negotiated version sess=" << this;
    bh.writeHandle->resetStream(/*error=*/0);
    readHandle.stopSending(/*error=*/0);
    close(SessionCloseErrorCode::VERSION_NEGOTIATION_FAILED);
    return;
  }
  co_withExecutor(
      exec_.get(),
      co_withCancellation(
          cancellationSource_.getToken(),
          bidiStreamDemuxer(std::move(readHandle), bh.writeHandle)))
      .start();
}

void MoQSession::replayBufferedUniStreams() {
  for (auto& buffered : bufferedPreSetupUniStreams_) {
    co_withExecutor(
        exec_.get(),
        co_withCancellation(
            cancellationSource_.getToken(),
            dataStreamReadLoop(
                shared_from_this(),
                std::move(buffered.readHandle),
                std::move(buffered.initialData))))
        .start();
  }
  bufferedPreSetupUniStreams_.clear();
}

folly::coro::Task<void> MoQSession::handlePreSetupUniStream(
    std::shared_ptr<MoQSession> session, // keeps session alive
    ReadHandleRef readHandle) {
  co_await folly::coro::co_safe_point;

  std::optional<FrameType> frameType = std::nullopt;
  folly::IOBufQueue readBuf{folly::IOBufQueue::cacheChainLength()};
  bool fin = false;

  while (auto* handle = readHandle.get()) {
    auto streamData = co_await co_awaitTry(
        folly::coro::co_withCancellation(
            cancellationSource_.getToken(),
            handle->readStreamData().via(exec_.get())));
    readHandle.onReadComplete(streamData);
    if (!streamData.hasValue()) {
      break;
    }
    if (streamData->data) {
      readBuf.append(std::move(streamData->data));
    }
    fin = streamData->fin;
    frameType = getFrameType(readBuf, negotiatedVersion_);
    if (frameType.has_value() || fin) {
      break;
    }
  }

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
          readHandle.id(), MOQTStreamType::CONTROL, Owner::REMOTE);
    }

    proxygen::WebTransport::StreamData initialData{readBuf.move(), fin};
    co_await controlReadLoop(std::move(readHandle), std::move(initialData));
  } else if (setupComplete_) {
    // Setup already completed while we were reading — dispatch directly
    co_await dataStreamReadLoop(
        std::move(session),
        std::move(readHandle),
        proxygen::WebTransport::StreamData{readBuf.move(), fin});
  } else {
    // Data stream arrived before setup - buffer it
    static constexpr size_t kMaxBufferedPreSetupUniStreams = 100;
    if (bufferedPreSetupUniStreams_.size() >= kMaxBufferedPreSetupUniStreams) {
      XLOG(ERR) << "Too many pre-setup uni streams sess=" << this;
      close(SessionCloseErrorCode::PROTOCOL_VIOLATION);
      co_return;
    }
    bufferedPreSetupUniStreams_.push_back(
        {std::move(readHandle), {readBuf.move(), fin}});
  }
}

void MoQSession::handleClientSetup(
    ReadHandleRef readHandle,
    proxygen::WebTransport::StreamWriteHandle* writeHandle,
    proxygen::WebTransport::StreamData initialData) noexcept {
  // TODO: prevent second control stream?
  if (dir_ == MoQControlCodec::Direction::CLIENT) {
    XLOG(ERR) << "Received bidi stream on client, kill it sess=" << this;
    writeHandle->resetStream(/*error=*/0);
    readHandle.stopSending(/*error=*/0);
  } else {
    if (logger_) {
      logger_->logStreamTypeSet(
          readHandle.id(), MOQTStreamType::CONTROL, Owner::REMOTE);
    }

    writeHandle->setPriority(controlPriority());
    co_withExecutor(
        exec_.get(),
        co_withCancellation(
            cancellationSource_.getToken(),
            controlReadLoop(std::move(readHandle), std::move(initialData))))
        .start();
    auto mergeToken = folly::cancellation_token_merge(
        cancellationSource_.getToken(), writeHandle->getCancelToken());
    co_withExecutor(
        exec_.get(),
        co_withCancellation(
            std::move(mergeToken), controlWriteLoop(writeHandle)))
        .start();
  }
}

std::optional<MoQSession::BidiStreamConfig> MoQSession::getBidiStreamConfig(
    FrameType frameType) {
  const auto major = getDraftMajorVersion(*negotiatedVersion_);
  auto subscribeNamespaceConfig =
      [this](FrameType wireType, bool finIsCancellation) {
        return BidiStreamConfig{
            {wireType, FrameType::REQUEST_UPDATE},
            [this](RequestID id, std::optional<ResetStreamErrorCode>) {
              onUnsubscribeNamespace(UnsubscribeNamespace{id});
            },
            finIsCancellation};
      };
  if (major >= 18) {
    switch (frameType) {
      case FrameType::SUBSCRIBE:
        return BidiStreamConfig{
            {FrameType::SUBSCRIBE, FrameType::REQUEST_UPDATE},
            [this](RequestID id, std::optional<ResetStreamErrorCode>) {
              onUnsubscribe(Unsubscribe{id});
            }};
      case FrameType::FETCH:
        return BidiStreamConfig{
            {FrameType::FETCH, FrameType::REQUEST_UPDATE},
            [this](RequestID id, std::optional<ResetStreamErrorCode>) {
              onFetchCancel(FetchCancel{id});
            }};
      case FrameType::PUBLISH:
        return BidiStreamConfig{
            {FrameType::PUBLISH,
             FrameType::REQUEST_UPDATE,
             FrameType::PUBLISH_DONE,
             FrameType::REQUEST_OK,
             FrameType::REQUEST_ERROR},
            nullptr};
      case FrameType::PUBLISH_NAMESPACE:
        // The publisher withdraws by cancelling this stream.
        return BidiStreamConfig{
            {FrameType::PUBLISH_NAMESPACE, FrameType::REQUEST_UPDATE},
            [this](RequestID id, std::optional<ResetStreamErrorCode>) {
              PublishNamespaceDone done;
              done.requestID = id;
              onPublishNamespaceDone(done);
            }};
      case FrameType::TRACK_STATUS:
        return BidiStreamConfig{{FrameType::TRACK_STATUS}, nullptr};
      case FrameType::SUBSCRIBE_NAMESPACE:
        // v18 wire enumerator (0x50). The legacy 0x11 wire type is a
        // protocol violation on v18 and falls through to nullopt below.
        return subscribeNamespaceConfig(
            FrameType::SUBSCRIBE_NAMESPACE, /*finIsCancellation=*/false);
      case FrameType::SUBSCRIBE_TRACKS:
        // FIN here means "no more REQUEST_UPDATE"; RST and STOP_SENDING cancel.
        return BidiStreamConfig{
            {FrameType::SUBSCRIBE_TRACKS, FrameType::REQUEST_UPDATE},
            [this](RequestID id, std::optional<ResetStreamErrorCode>) {
              onSubscribeTracksStreamClosed(id);
            }};
      default:
        return std::nullopt;
    }
  }
  // Drafts 16-17: SUBSCRIBE_NAMESPACE uses the legacy 0x11 wire enumerator.
  // The v18 wire type (0x50) is a protocol violation on pre-v18 sessions.
  // NOLINTNEXTLINE(clang-diagnostic-switch-enum)
  switch (frameType) {
    case FrameType::LEGACY_SUBSCRIBE_NAMESPACE:
      return subscribeNamespaceConfig(
          FrameType::LEGACY_SUBSCRIBE_NAMESPACE, /*finIsCancellation=*/true);
    default:
      return std::nullopt;
  }
}

folly::coro::Task<void> MoQSession::bidiStreamDemuxer(
    ReadHandleRef readHandle,
    proxygen::WebTransport::StreamWriteHandle* writeHandle) noexcept {
  co_await folly::coro::co_safe_point;
  auto sessionToken = co_await folly::coro::co_current_cancellation_token;
  auto streamToken = folly::cancellation_token_merge(
      readHandle.cancelToken(), writeHandle->getCancelToken());
  auto token =
      folly::cancellation_token_merge(sessionToken, std::move(streamToken));
  if (token.isCancellationRequested()) {
    co_return;
  }
  if (!setupComplete_ && useUniControlStreams(*negotiatedVersion_)) {
    if (!setupWaiters_) {
      setupWaiters_ = std::make_shared<SetupWaiters>();
    }
    auto waiters = setupWaiters_;
    const auto id = readHandle.id();
    auto [promise, future] = folly::coro::makePromiseContract<void>();
    waiters->emplace(id, std::move(promise));
    auto removeWaiter = folly::makeGuard([waiters, id] { waiters->erase(id); });
    auto ready = co_await co_awaitTry(
        folly::coro::co_withCancellation(token, std::move(future)));
    if (token.isCancellationRequested() || ready.hasException()) {
      co_return;
    }
    if (!setupComplete_ || closed_) {
      co_return;
    }
  }
  std::optional<FrameType> frameType = std::nullopt;

  folly::IOBufQueue readBuf{folly::IOBufQueue::cacheChainLength()};
  bool fin = false;

  while (auto* handle = readHandle.get()) {
    auto streamData = co_await co_awaitTry(
        folly::coro::co_withCancellation(
            token, handle->readStreamData().via(exec_.get())));
    readHandle.onReadComplete(streamData);
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
    if (frameType.has_value() || fin) {
      break;
    }
  }

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
      handleClientSetup(
          std::move(readHandle), writeHandle, std::move(accumulatedData));
    } else {
      auto config = getBidiStreamConfig(*frameType);
      if (!config) {
        XLOG(ERR) << "Unexpected frame type on bidi stream: "
                  << static_cast<uint64_t>(*frameType) << " sess=" << this;
        close(SessionCloseErrorCode::PROTOCOL_VIOLATION);
        co_return;
      }
      writeHandle->setPriority(controlPriority());
      auto control = std::make_shared<BidiStreamControl>(
          writeHandle,
          cancellationSource_.getToken(),
          *negotiatedVersion_,
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
                  std::move(readHandle),
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
  remainingLength -= type->second;

  if (isPaddingDatagramType(*negotiatedVersion_, type->first)) {
    auto paddingResult = parsePaddingData(cursor, remainingLength);
    if (paddingResult.hasError()) {
      XLOG(ERR) << __func__ << " Bad padding datagram";
      close(SessionCloseErrorCode::PROTOCOL_VIOLATION);
    }
    return;
  }

  std::unique_ptr<folly::IOBuf> payload;
  if (logger_) {
    payload = readBuf.front()->clone();
  }

  if (!isValidDatagramType(*negotiatedVersion_, type->first)) {
    XLOG(ERR) << __func__ << " Bad datagram header type=" << type->first;
    close(SessionCloseErrorCode::PROTOCOL_VIOLATION);
    return;
  }
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
      callback->datagram(
          objHeader.objectHeader, readBuf.move(), objHeader.endOfGroup);
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
    // Request IDs can arrive out of order, so there is no strict-sequence
    // check.
    const auto majorVersion = getDraftMajorVersion(*getNegotiatedVersion());
    if (majorVersion >= 18) {
      // Draft 18 dropped MAX_REQUEST_ID, so nothing else keeps the cutoff
      // from wrapping.
      if (requestID.value >
          std::numeric_limits<uint64_t>::max() - getRequestIDMultiplier()) {
        XLOG(ERR) << "requestID exhausts the ID space: " << requestID
                  << " sess=" << this;
        close(SessionCloseErrorCode::INVALID_REQUEST_ID);
        return true;
      }
      if (majorVersion < 22) {
        nextPeerRequestIDForGoaway_ = std::max(
            nextPeerRequestIDForGoaway_,
            requestID.value + getRequestIDMultiplier());
      }
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
  if (!receivedGoaway_ || !negotiatedVersion_.has_value()) {
    return false;
  }
  const auto majorVersion = getDraftMajorVersion(*negotiatedVersion_);
  // Draft 22 removed the cutoff Request ID. Drafts 18-21 still use its
  // presence to confirm receipt of a valid GOAWAY.
  return majorVersion >= 22 ||
      (majorVersion >= 18 && receivedGoawayRequestID_.has_value());
}

void MoQSession::initializeNegotiatedVersion(uint64_t negotiatedVersion) {
  negotiatedVersion_ = negotiatedVersion;
  moqFrameWriter_.initializeVersion(*negotiatedVersion_);
  controlCodec_->initializeVersion(*negotiatedVersion_);
  if (useBidiRequestStreams(*negotiatedVersion_)) {
    receiveTokenCache_.setMaxSize(0, /*evict=*/true);
  }
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
  for (const auto& param : params) {
    if (param.key ==
        folly::to_underlying(SetupKey::MAX_AUTH_TOKEN_CACHE_SIZE)) {
      return std::min(param.asUint64, kMaxReceiveTokenCacheSize);
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
    const bool aliasingDisabled =
        !authTokenCacheEnabled_ || useBidiRequestStreams(*version);
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
    if (isLocallyIssuedRequestID(reqId)) {
      XLOG(DBG1) << "REQUEST_OK for cancelled reqID=" << reqId
                 << " sess=" << this;
      return;
    }
    XLOG(ERR) << "No matching request for reqID=" << reqId << " sess=" << this;
    close(SessionCloseErrorCode::PROTOCOL_VIOLATION);
    return;
  }

  // Convert REQUEST_OK back to the specific frame type.
  frameType = reqIt->second->getOkFrameType();

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
  XLOG(DBG1) << __func__ << " requestID="
             << publishNamespaceCancel.requestID.value_or(RequestID(0))
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
  if (!unsubscribeNamespace.requestID.has_value()) {
    XLOG(ERR) << __func__ << " sess=" << this << " - missing requestID";
    return;
  }
  XLOG(DBG1) << __func__
             << " requestID=" << unsubscribeNamespace.requestID.value()
             << " sess=" << this;
  // TODO: Implement actual unsubscribe logic when SUBSCRIBE_NAMESPACE
  // tracking is added For now, just log and update stats

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

MoQSession::RequestContext MoQSession::getRequestContext() {
  auto session = getRequestSession();
  // The negotiated version is unset during teardown before SETUP completed.
  // getDraftMajorVersion(0) is 0, so a version check denies rather than
  // crashing.
  return RequestContext{
      session->sessionId(),
      session->getNegotiatedVersion().value_or(0),
      session->getExecutor()};
}

SessionId MoQSession::makeSessionId() {
  // Never returns 0, which is what kUnsetSessionId relies on.
  return SessionId(folly::processLocalUniqueId());
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
  markSetupComplete();
}

void MoQSession::setMoqSettings(MoQSettings settings) {
  moqSettings_ = settings;
}

void SessionScoped::bind(const std::shared_ptr<MoQSession>& session) {
  XCHECK(session);
  if (bound_) {
    XCHECK(sessionId_ == session->sessionId())
        << "SessionScoped handler registered on more than one session";
    return;
  }
  session_ = session;
  sessionId_ = session->sessionId();
  bound_ = true;
}

std::shared_ptr<MoQSession> SessionScoped::getSession() const {
  XCHECK(bound_) << "SessionScoped handler used before it was registered";
  return session_.lock();
}

void MoQSession::setPublishHandler(std::shared_ptr<Publisher> publishHandler) {
  if (auto* scoped = dynamic_cast<SessionScoped*>(publishHandler.get())) {
    scoped->bind(shared_from_this());
  }
  publishHandler_ = std::move(publishHandler);
}

void MoQSession::setSubscribeHandler(
    std::shared_ptr<Subscriber> subscribeHandler) {
  if (auto* scoped = dynamic_cast<SessionScoped*>(subscribeHandler.get())) {
    scoped->bind(shared_from_this());
  }
  subscribeHandler_ = std::move(subscribeHandler);
}

std::shared_ptr<ReplyContext> MoQSession::makeReplyContext(
    std::shared_ptr<BidiStreamControl> control) {
  if (control) {
    XCHECK(control->writeHandle())
        << "BidiStreamControl handed to makeReplyContext must have a live "
           "write handle";
    auto token = folly::cancellation_token_merge(
        cancellationSource_.getToken(), control->getPeerCancelToken());
    return std::make_shared<BidiStreamReplyContext>(
        std::move(control), std::move(token));
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

bool MoQSession::validateAndSetVersionFromAlpn(const std::string& alpn) {
  auto version = getVersionFromAlpn(std::string_view(alpn));
  if (!version) {
    XLOG(ERR) << "No supported MoQT version for ALPN '" << alpn
              << "' sess=" << this;
    return false;
  }
  initializeNegotiatedVersion(*version);
  return true;
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

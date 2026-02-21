/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <folly/CppAttributes.h>
#include <folly/coro/Collect.h>
#include <folly/coro/FutureUtil.h>
#include <folly/logging/xlog.h>
#include <moxygen/MoQRelaySession.h>

#include <utility>

using folly::coro::co_awaitTry;
using folly::coro::co_error;

namespace moxygen {

std::function<std::shared_ptr<MoQSession>(
    folly::MaybeManagedPtr<proxygen::WebTransport>,
    std::shared_ptr<MoQExecutor>)>
MoQRelaySession::createRelaySessionFactory() {
  static auto factory = [](folly::MaybeManagedPtr<proxygen::WebTransport> wt,
                           std::shared_ptr<MoQExecutor> exec) {
    return std::static_pointer_cast<MoQSession>(
        std::make_shared<MoQRelaySession>(wt, std::move(exec)));
  };
  return factory;
}

// Inner class implementations (moved from MoQSession.cpp)

class MoQRelaySession::SubscriberPublishNamespaceCallback
    : public Subscriber::PublishNamespaceCallback {
 public:
  SubscriberPublishNamespaceCallback(
      MoQRelaySession& session,
      const TrackNamespace& ns,
      RequestID requestID)
      : session_(session), trackNamespace_(ns), requestID_(requestID) {}

  void publishNamespaceCancel(
      PublishNamespaceErrorCode errorCode,
      std::string reasonPhrase) override {
    PublishNamespaceCancel annCan;
    annCan.requestID = requestID_;
    if (getDraftMajorVersion(*session_.getNegotiatedVersion()) < 16) {
      annCan.trackNamespace = trackNamespace_;
    }
    annCan.errorCode = errorCode;
    annCan.reasonPhrase = std::move(reasonPhrase);
    session_.publishNamespaceCancel(annCan);
  }

 private:
  MoQRelaySession& session_;
  TrackNamespace trackNamespace_;
  RequestID requestID_;
};

class MoQRelaySession::PublisherPublishNamespaceHandle
    : public Subscriber::PublishNamespaceHandle {
 public:
  PublisherPublishNamespaceHandle(
      std::shared_ptr<MoQRelaySession> session,
      TrackNamespace trackNamespace,
      PublishNamespaceOk annOk)
      : Subscriber::PublishNamespaceHandle(std::move(annOk)),
        trackNamespace_(std::move(trackNamespace)),
        session_(std::move(session)) {}
  PublisherPublishNamespaceHandle(const PublisherPublishNamespaceHandle&) =
      delete;
  PublisherPublishNamespaceHandle& operator=(
      const PublisherPublishNamespaceHandle&) = delete;
  PublisherPublishNamespaceHandle(PublisherPublishNamespaceHandle&&) = delete;
  PublisherPublishNamespaceHandle& operator=(
      PublisherPublishNamespaceHandle&&) = delete;
  ~PublisherPublishNamespaceHandle() override {
    publishNamespaceDone();
  }

  void publishNamespaceDone() override {
    if (session_) {
      PublishNamespaceDone unann;
      if (getDraftMajorVersion(*session_->getNegotiatedVersion()) >= 16) {
        unann.requestID = publishNamespaceOk().requestID;
      } else {
        unann.trackNamespace = trackNamespace_;
      }
      session_->publishNamespaceDone(unann);
      session_.reset();
    }
  }

  folly::coro::Task<RequestUpdateResult> requestUpdate(
      RequestUpdate reqUpdate) override {
    co_return folly::makeUnexpected(
        RequestError{
            reqUpdate.requestID,
            RequestErrorCode::NOT_SUPPORTED,
            "REQUEST_UPDATE not supported for PUBLISH_NAMESPACE"});
  }

 private:
  TrackNamespace trackNamespace_;
  std::shared_ptr<MoQRelaySession> session_;
};

class MoQRelaySession::SubscribeNamespaceHandle
    : public Publisher::SubscribeNamespaceHandle {
 public:
  SubscribeNamespaceHandle(
      std::shared_ptr<MoQRelaySession> session,
      TrackNamespace trackNamespacePrefix,
      SubscribeNamespaceOk subAnnOk)
      : Publisher::SubscribeNamespaceHandle(std::move(subAnnOk)),
        trackNamespacePrefix_(std::move(trackNamespacePrefix)),
        session_(std::move(session)) {}
  SubscribeNamespaceHandle(const SubscribeNamespaceHandle&) = delete;
  SubscribeNamespaceHandle& operator=(const SubscribeNamespaceHandle&) = delete;
  SubscribeNamespaceHandle(SubscribeNamespaceHandle&&) = delete;
  SubscribeNamespaceHandle& operator=(SubscribeNamespaceHandle&&) = delete;
  ~SubscribeNamespaceHandle() override {
    unsubscribeNamespace();
  }

  void unsubscribeNamespace() override {
    if (session_) {
      UnsubscribeNamespace msg;

      // v15+: Send requestID
      if (getDraftMajorVersion(*session_->getNegotiatedVersion()) >= 15) {
        msg.requestID = subscribeNamespaceOk_->requestID;
      } else {
        // <v15: Send namespace
        msg.trackNamespacePrefix = trackNamespacePrefix_;
      }

      session_->unsubscribeNamespace(msg);
      session_.reset();
    }
  }

  folly::coro::Task<RequestUpdateResult> requestUpdate(
      RequestUpdate reqUpdate) override {
    co_return folly::makeUnexpected(
        RequestError{
            reqUpdate.requestID,
            RequestErrorCode::NOT_SUPPORTED,
            "REQUEST_UPDATE not supported for SUBSCRIBE_NAMESPACE"});
  }

 private:
  TrackNamespace trackNamespacePrefix_;
  std::shared_ptr<MoQRelaySession> session_;
};

// MoQRelayPendingRequestState - extends base PendingRequestState with
// publishNamespace support
class MoQRelaySession::MoQRelayPendingRequestState
    : public PendingRequestState {
 public:
  // Constructor for publishNamespace types
  explicit MoQRelayPendingRequestState(
      PendingPublishNamespace publishNamespace) {
    type_ = Type::PUBLISH_NAMESPACE;
    new (&publishNamespaceStorage_)
        PendingPublishNamespace(std::move(publishNamespace));
  }

  MoQRelayPendingRequestState(const MoQRelayPendingRequestState&) = delete;
  MoQRelayPendingRequestState& operator=(const MoQRelayPendingRequestState&) =
      delete;
  MoQRelayPendingRequestState(MoQRelayPendingRequestState&&) = delete;
  MoQRelayPendingRequestState& operator=(MoQRelayPendingRequestState&&) =
      delete;

  explicit MoQRelayPendingRequestState(
      folly::coro::Promise<
          folly::Expected<SubscribeNamespaceOk, SubscribeNamespaceError>>
          promise) {
    type_ = Type::SUBSCRIBE_NAMESPACE;
    new (&subscribeNamespaceStorage_) auto(std::move(promise));
  }

  folly::Expected<Type, folly::Unit> setError(
      RequestError error,
      FrameType frameType) override {
    switch (type_) {
      case Type::PUBLISH_NAMESPACE:
        if (auto* publishNamespacePtr = tryGetPublishNamespace(this)) {
          publishNamespacePtr->promise.setValue(
              folly::makeUnexpected(std::move(error)));
          return type_;
        }
        return folly::makeUnexpected(folly::unit);
      case Type::SUBSCRIBE_NAMESPACE:
        if (auto* subscribeNamespacePtr = tryGetSubscribeNamespace(this)) {
          subscribeNamespacePtr->setValue(
              folly::makeUnexpected(std::move(error)));
          return type_;
        }
        return folly::makeUnexpected(folly::unit);
      default:
        return PendingRequestState::setError(std::move(error), frameType);
    }
  }

  // Factory methods for publishNamespace types
  static std::unique_ptr<MoQRelayPendingRequestState> makePublishNamespace(
      PendingPublishNamespace pendingPublishNamespace) {
    return std::make_unique<MoQRelayPendingRequestState>(
        std::move(pendingPublishNamespace));
  }

  static std::unique_ptr<MoQRelayPendingRequestState> makeSubscribeNamespace(
      folly::coro::Promise<
          folly::Expected<SubscribeNamespaceOk, SubscribeNamespaceError>>
          promise) {
    return std::make_unique<MoQRelayPendingRequestState>(std::move(promise));
  }

  // Override destructor to handle publishNamespace storage
  ~MoQRelayPendingRequestState() override {
    switch (getType()) {
      case Type::PUBLISH_NAMESPACE:
        publishNamespaceStorage_.~PendingPublishNamespace();
        break;
      case Type::SUBSCRIBE_NAMESPACE:
        subscribeNamespaceStorage_.~Promise();
        break;
      default:
        // Base class handles other types
        break;
    }
  }

  // Additional tryGet methods for publishNamespace types
  static PendingPublishNamespace* FOLLY_NULLABLE
  tryGetPublishNamespace(PendingRequestState* base) {
    if (base && base->getType() == Type::PUBLISH_NAMESPACE) {
      auto* relay = static_cast<MoQRelayPendingRequestState*>(base);
      return &relay->publishNamespaceStorage_;
    }
    return nullptr;
  }

  static folly::coro::Promise<
      folly::Expected<SubscribeNamespaceOk, SubscribeNamespaceError>>*
      FOLLY_NULLABLE
      tryGetSubscribeNamespace(PendingRequestState* base) {
    if (base && base->getType() == Type::SUBSCRIBE_NAMESPACE) {
      auto* relay = static_cast<MoQRelayPendingRequestState*>(base);
      return &relay->subscribeNamespaceStorage_;
    }
    return nullptr;
  }

 private:
  // Extended storage for publishNamespace types (separate from base union)
  union {
    PendingPublishNamespace publishNamespaceStorage_;
    folly::coro::Promise<
        folly::Expected<SubscribeNamespaceOk, SubscribeNamespaceError>>
        subscribeNamespaceStorage_;
  };
};

void MoQRelaySession::cleanup() {
  // Set up request context so publishNamespaceDone() can identify this session.
  // This is needed because cleanup() is called directly (not from a message
  // handler) and MoQRelay::publishNamespaceDone() uses getRequestSession() to
  // verify ownership.
  folly::RequestContextScopeGuard guard;
  setRequestSession();

  // Clean up publishNamespace maps
  for (auto& ann : publishNamespaceCallbacks_) {
    if (ann.second) {
      ann.second->publishNamespaceCancel(
          PublishNamespaceErrorCode::INTERNAL_ERROR, "Session ended");
    }
  }
  publishNamespaceCallbacks_.clear();
  legacyPublisherNamespaceToReqId_.clear();

  for (auto& ann : publishNamespaceHandles_) {
    ann.second->publishNamespaceDone();
  }
  publishNamespaceHandles_.clear();
  legacySubscriberNamespaceToReqId_.clear();

  // Clean up subscribeNamespace handles
  for (auto& subAnn : subscribeNamespaceHandles_) {
    if (subAnn.second) {
      subAnn.second->unsubscribeNamespace();
    }
  }
  subscribeNamespaceHandles_.clear();
  legacySubscribeNamespaceToReqId_.clear();

  // Call parent cleanup to handle base class cleanup
  MoQSession::cleanup();
}

void MoQRelaySession::onRequestUpdate(RequestUpdate requestUpdate) {
  // Only intercept for v16+ and announcement types
  if (getDraftMajorVersion(*getNegotiatedVersion()) >= 16) {
    auto existingRequestID = requestUpdate.existingRequestID;

    // Check publishNamespaceHandles_ for PUBLISH_NAMESPACE (formerly ANNOUNCE)
    auto announceIt = publishNamespaceHandles_.find(existingRequestID);
    if (announceIt != publishNamespaceHandles_.end()) {
      handlePublishNamespaceRequestUpdate(requestUpdate, announceIt->second);
      return;
    }

    // Check subscribeNamespaceHandles_ for SUBSCRIBE_NAMESPACE (formerly
    // SUBSCRIBE_ANNOUNCES)
    auto subAnnIt = subscribeNamespaceHandles_.find(existingRequestID);
    if (subAnnIt != subscribeNamespaceHandles_.end()) {
      handleSubscribeNamespaceRequestUpdate(requestUpdate, subAnnIt->second);
      return;
    }
  }

  // Delegate to base class for all other cases
  MoQSession::onRequestUpdate(std::move(requestUpdate));
}

void MoQRelaySession::handlePublishNamespaceRequestUpdate(
    RequestUpdate requestUpdate,
    std::shared_ptr<Subscriber::PublishNamespaceHandle> announceHandle) {
  XLOG(DBG1) << __func__ << " requestID=" << requestUpdate.existingRequestID
             << " sess=" << this;

  auto existingRequestID = requestUpdate.existingRequestID;
  auto updateRequestID = requestUpdate.requestID;

  // Handle asynchronously with shared ownership to prevent use-after-free
  co_withExecutor(
      getExecutor(),
      folly::coro::co_invoke(
          [this,
           announceHandle = std::move(announceHandle),
           update = std::move(requestUpdate),
           existingRequestID,
           updateRequestID]() mutable -> folly::coro::Task<void> {
            // Call the handle's requestUpdate
            auto updateResult = co_await co_awaitTry(co_withCancellation(
                cancellationSource_.getToken(),
                announceHandle->requestUpdate(std::move(update))));

            // Only send responses for v15+
            if (getDraftMajorVersion(*getNegotiatedVersion()) >= 15) {
              if (updateResult.hasException()) {
                XLOG(ERR) << "Exception in requestUpdate ex="
                          << updateResult.exception().what();
                requestUpdateError(
                    RequestError{
                        updateRequestID,
                        RequestErrorCode::INTERNAL_ERROR,
                        "Exception in requestUpdate"},
                    existingRequestID);
              } else if (updateResult->hasError()) {
                auto updateErr = updateResult->error();
                requestUpdateError(updateErr, existingRequestID);
              } else {
                RequestOk requestOk{.requestID = updateRequestID};
                requestUpdateOk(requestOk);
              }
            }
          }))
      .start();
}

void MoQRelaySession::handleSubscribeNamespaceRequestUpdate(
    RequestUpdate requestUpdate,
    std::shared_ptr<Publisher::SubscribeNamespaceHandle>
        subscribeNamespaceHandle) {
  XLOG(DBG1) << __func__ << " requestID=" << requestUpdate.existingRequestID
             << " sess=" << this;

  auto existingRequestID = requestUpdate.existingRequestID;
  auto updateRequestID = requestUpdate.requestID;

  // Handle asynchronously with shared ownership to prevent use-after-free
  co_withExecutor(
      getExecutor(),
      folly::coro::co_invoke(
          [this,
           subscribeNamespaceHandle = std::move(subscribeNamespaceHandle),
           update = std::move(requestUpdate),
           existingRequestID,
           updateRequestID]() mutable -> folly::coro::Task<void> {
            // Call the handle's requestUpdate
            auto updateResult = co_await co_awaitTry(co_withCancellation(
                cancellationSource_.getToken(),
                subscribeNamespaceHandle->requestUpdate(std::move(update))));

            // Only send responses for v15+
            if (getDraftMajorVersion(*getNegotiatedVersion()) >= 15) {
              if (updateResult.hasException()) {
                XLOG(ERR) << "Exception in requestUpdate ex="
                          << updateResult.exception().what();
                requestUpdateError(
                    RequestError{
                        updateRequestID,
                        RequestErrorCode::INTERNAL_ERROR,
                        "Exception in requestUpdate"},
                    existingRequestID);
              } else if (updateResult->hasError()) {
                auto updateErr = updateResult->error();
                requestUpdateError(updateErr, existingRequestID);
              } else {
                RequestOk requestOk{.requestID = updateRequestID};
                requestUpdateOk(requestOk);
              }
            }
          }))
      .start();
}

// PublishNamespace publisher methods
folly::coro::Task<Subscriber::PublishNamespaceResult>
MoQRelaySession::publishNamespace(
    PublishNamespace ann,
    std::shared_ptr<PublishNamespaceCallback> publishNamespaceCallback) {
  XLOG(DBG1) << __func__ << " ns=" << ann.trackNamespace << " sess=" << this;

  if (logger_) {
    logger_->logPublishNamespace(ann);
  }

  auto publishNamespaceStartTime = std::chrono::steady_clock::now();
  SCOPE_EXIT {
    auto duration =
        (std::chrono::steady_clock::now() - publishNamespaceStartTime);
    auto durationMsec =
        std::chrono::duration_cast<std::chrono::milliseconds>(duration);
    MOQ_PUBLISHER_STATS(
        publisherStatsCallback_,
        recordPublishNamespaceLatency,
        durationMsec.count());
  };
  const auto& trackNamespace = ann.trackNamespace;
  aliasifyAuthTokens(ann.params);
  ann.requestID = getNextRequestID();
  auto res = moqFrameWriter_.writePublishNamespace(controlWriteBuf_, ann);
  if (!res) {
    XLOG(ERR) << "writePublishNamespace failed sess=" << this;
    co_return folly::makeUnexpected(PublishNamespaceError(
        {ann.requestID,
         PublishNamespaceErrorCode::INTERNAL_ERROR,
         "local write failed"}));
  }
  controlWriteEvent_.signal();
  auto contract = folly::coro::makePromiseContract<
      folly::Expected<PublishNamespaceOk, PublishNamespaceError>>();
  pendingRequests_.emplace(
      ann.requestID,
      MoQRelayPendingRequestState::makePublishNamespace(
          PendingPublishNamespace{
              trackNamespace, // Use saved copy instead of ann.trackNamespace
              std::move(contract.first),
              std::move(publishNamespaceCallback)}));
  auto publishNamespaceResult = co_await std::move(contract.second);
  if (publishNamespaceResult.hasError()) {
    MOQ_PUBLISHER_STATS(
        publisherStatsCallback_,
        onPublishNamespaceError,
        publishNamespaceResult.error().errorCode);
    co_return folly::makeUnexpected(publishNamespaceResult.error());
  } else {
    MOQ_PUBLISHER_STATS(publisherStatsCallback_, onPublishNamespaceSuccess);
    co_return std::make_shared<PublisherPublishNamespaceHandle>(
        std::static_pointer_cast<MoQRelaySession>(shared_from_this()),
        trackNamespace,
        std::move(publishNamespaceResult.value()));
  }
}

void MoQRelaySession::onRequestOk(RequestOk requestOk, FrameType frameType) {
  XLOG(DBG1) << __func__ << " id=" << requestOk.requestID << " sess=" << this;

  auto reqID = requestOk.requestID;
  auto reqIt = pendingRequests_.find(reqID);
  bool shouldErasePendingRequest = true;

  if (reqIt == pendingRequests_.end()) {
    // unknown
    XLOG(ERR) << "No matching publishNamespace reqID=" << reqID
              << " sess=" << this;
    close(SessionCloseErrorCode::PROTOCOL_VIOLATION);
    return;
  }
  if (*getNegotiatedVersion() > 14) {
    frameType = reqIt->second->getOkFrameType();
  }
  switch (frameType) {
    case moxygen::FrameType::TRACK_STATUS_OK: {
      // Use base class helper
      handleTrackStatusOkFromRequestOk(requestOk);
      shouldErasePendingRequest = false;
      break;
    }
    case moxygen::FrameType::SUBSCRIBE_NAMESPACE_OK:
    case moxygen::FrameType::REQUEST_OK: {
      // PUBLISH_NAMESPACE_OK is an alias for REQUEST_OK (both = 0x7)
      switch (reqIt->second->getType()) {
        case PendingRequestState::Type::PUBLISH_NAMESPACE:
          handlePublishNamespaceOkFromRequestOk(requestOk, reqIt);
          break;
        case PendingRequestState::Type::SUBSCRIBE_NAMESPACE:
          handleSubscribeNamespaceOkFromRequestOk(requestOk, reqIt);
          break;
        case PendingRequestState::Type::REQUEST_UPDATE:
          // Use base class helper
          handleSubscribeUpdateOkFromRequestOk(requestOk, reqIt);
          shouldErasePendingRequest = false;
          break;
        default:
          XLOG(ERR) << "Unexpected REQUEST_OK for type "
                    << folly::to_underlying(reqIt->second->getType())
                    << ", sess=" << this;
          close(SessionCloseErrorCode::PROTOCOL_VIOLATION);
          break;
      }
      break;
    }
    default:
      break;
  }
  if (shouldErasePendingRequest) {
    pendingRequests_.erase(reqIt);
  }
}

void MoQRelaySession::onPublishNamespaceCancel(
    PublishNamespaceCancel publishNamespaceCancel) {
  MOQ_PUBLISHER_STATS(publisherStatsCallback_, onPublishNamespaceCancel);

  if (logger_) {
    logger_->logPublishNamespaceCancel(
        publishNamespaceCancel,
        MOQTByteStringType::STRING_VALUE,
        ControlMessageType::PARSED);
  }

  std::shared_ptr<Subscriber::PublishNamespaceCallback> cb;

  RequestID reqId;
  if (getDraftMajorVersion(*getNegotiatedVersion()) >= 16) {
    CHECK(publishNamespaceCancel.requestID.has_value());
    XLOG(DBG1) << __func__ << " requestID=" << *publishNamespaceCancel.requestID
               << " sess=" << this;
    reqId = *publishNamespaceCancel.requestID;
  } else {
    // Legacy: translate NS -> RequestID
    XLOG(DBG1) << __func__ << " ns=" << publishNamespaceCancel.trackNamespace
               << " sess=" << this;
    auto nsIt = legacyPublisherNamespaceToReqId_.find(
        publishNamespaceCancel.trackNamespace);
    if (nsIt == legacyPublisherNamespaceToReqId_.end()) {
      XLOG(ERR) << "Invalid publishNamespace cancel ns="
                << publishNamespaceCancel.trackNamespace;
      return;
    }
    reqId = nsIt->second;
    legacyPublisherNamespaceToReqId_.erase(nsIt);
  }

  auto it = publishNamespaceCallbacks_.find(reqId);
  if (it == publishNamespaceCallbacks_.end()) {
    XLOG(ERR) << "Invalid publishNamespace cancel requestID=" << reqId;
    return;
  }
  cb = std::move(it->second);
  publishNamespaceCallbacks_.erase(it);

  // Common action
  if (cb) {
    cb->publishNamespaceCancel(
        publishNamespaceCancel.errorCode,
        std::move(publishNamespaceCancel.reasonPhrase));
  }
}

void MoQRelaySession::publishNamespaceDone(const PublishNamespaceDone& unann) {
  MOQ_PUBLISHER_STATS(publisherStatsCallback_, onPublishNamespaceDone);

  if (logger_) {
    logger_->logPublishNamespaceDone(unann);
  }

  // Lambda helper to fail a pending publishNamespace and erase it
  auto failPendingPublishNamespace = [this](auto pendingIt, RequestID reqId) {
    if (auto* publishNamespacePtr =
            MoQRelayPendingRequestState::tryGetPublishNamespace(
                pendingIt->second.get())) {
      publishNamespacePtr->promise.setValue(
          folly::makeUnexpected(PublishNamespaceError(
              {reqId,
               PublishNamespaceErrorCode::INTERNAL_ERROR,
               "PublishNamespaceDone before PublishNamespaceOK"})));
    }
    pendingRequests_.erase(pendingIt);
  };

  // Lambda helper to write the publishNamespaceDone frame
  auto writePublishNamespaceDoneToWire = [this, &unann]() {
    auto res =
        moqFrameWriter_.writePublishNamespaceDone(controlWriteBuf_, unann);
    if (!res) {
      XLOG(ERR) << "writePublishNamespaceDone failed sess=" << this;
    }
    controlWriteEvent_.signal();
  };

  bool found = false;
  std::optional<RequestID> reqId;

  // Try resolve requestID
  if (getDraftMajorVersion(*getNegotiatedVersion()) >= 16) {
    CHECK(unann.requestID.has_value());
    XLOG(DBG1) << __func__ << " requestID=" << *unann.requestID
               << " sess=" << this;
    reqId = *unann.requestID;
  } else {
    XLOG(DBG1) << __func__ << " ns=" << unann.trackNamespace
               << " sess=" << this;
    // Legacy: translate NS -> RequestID
    auto nsIt = legacyPublisherNamespaceToReqId_.find(unann.trackNamespace);
    if (nsIt != legacyPublisherNamespaceToReqId_.end()) {
      reqId = nsIt->second;
      legacyPublisherNamespaceToReqId_.erase(nsIt);
    }
    // If not found, reqId remains nullopt - will search pending by namespace
  }

  // Find and remove publishNamespace
  if (reqId.has_value()) {
    auto it = publishNamespaceCallbacks_.find(*reqId);
    if (it != publishNamespaceCallbacks_.end()) {
      publishNamespaceCallbacks_.erase(it);
      found = true;
    } else {
      // Check pending requests by RequestID
      auto pendingIt = pendingRequests_.find(*reqId);
      if (pendingIt != pendingRequests_.end()) {
        failPendingPublishNamespace(pendingIt, *reqId);
        found = true;
      }
    }
  } else {
    // Legacy: search pending by namespace
    // TODO: This is a scan. Optimize
    auto pendingIt = std::find_if(
        pendingRequests_.begin(),
        pendingRequests_.end(),
        [&unann](const auto& pair) {
          if (auto* publishNamespacePtr =
                  MoQRelayPendingRequestState::tryGetPublishNamespace(
                      pair.second.get())) {
            return publishNamespacePtr->trackNamespace == unann.trackNamespace;
          }
          return false;
        });
    if (pendingIt != pendingRequests_.end()) {
      failPendingPublishNamespace(pendingIt, pendingIt->first);
      found = true;
    }
  }

  if (found) {
    writePublishNamespaceDoneToWire();
  } else {
    XLOG(ERR) << "PublishNamespaceDone for unknown publishNamespace, sess="
              << this;
  }
}

// PublishNamespace subscriber methods
void MoQRelaySession::onPublishNamespace(PublishNamespace ann) {
  XLOG(DBG1) << __func__ << " ns=" << ann.trackNamespace << " sess=" << this;

  if (logger_) {
    logger_->logPublishNamespace(
        ann, MOQTByteStringType::STRING_VALUE, ControlMessageType::PARSED);
  }

  if (closeSessionIfRequestIDInvalid(ann.requestID, false, true)) {
    return;
  }

  if (!subscribeHandler_) {
    XLOG(DBG1) << __func__ << "No subscriber callback set";
    publishNamespaceError(
        PublishNamespaceError{
            ann.requestID,
            PublishNamespaceErrorCode::NOT_SUPPORTED,
            "Not a subscriber"});
    return;
  }
  co_withExecutor(exec_.get(), handlePublishNamespace(std::move(ann))).start();
}

folly::coro::Task<void> MoQRelaySession::handlePublishNamespace(
    PublishNamespace publishNamespace) {
  folly::RequestContextScopeGuard guard;
  setRequestSession();
  auto annCb = std::make_shared<SubscriberPublishNamespaceCallback>(
      *this, publishNamespace.trackNamespace, publishNamespace.requestID);
  auto publishNamespaceResult = co_await co_awaitTry(co_withCancellation(
      cancellationSource_.getToken(),
      subscribeHandler_->publishNamespace(publishNamespace, std::move(annCb))));
  if (publishNamespaceResult.hasException()) {
    XLOG(ERR) << "Exception in Subscriber callback ex="
              << publishNamespaceResult.exception().what().toStdString();
    publishNamespaceError(
        PublishNamespaceError{
            publishNamespace.requestID,
            PublishNamespaceErrorCode::INTERNAL_ERROR,
            publishNamespaceResult.exception().what().toStdString()});
    co_return;
  }
  if (publishNamespaceResult->hasError()) {
    XLOG(DBG1) << "Application publishNamespace error err="
               << publishNamespaceResult->error().reasonPhrase;
    auto annErr = std::move(publishNamespaceResult->error());
    annErr.requestID = publishNamespace.requestID; // In case app got it wrong
    // trackNamespace removed from unified RequestError
    publishNamespaceError(annErr);
  } else {
    auto handle = std::move(publishNamespaceResult->value());
    auto publishNamespaceOkMsg = handle->publishNamespaceOk();
    publishNamespaceOk(publishNamespaceOkMsg);
    publishNamespaceHandles_[publishNamespace.requestID] = std::move(handle);
    if (getDraftMajorVersion(*getNegotiatedVersion()) < 16) {
      // Legacy: also store NS->RequestID mapping for lookups
      legacySubscriberNamespaceToReqId_[publishNamespace.trackNamespace] =
          publishNamespace.requestID;
    }
  }
}

void MoQRelaySession::publishNamespaceOk(const PublishNamespaceOk& annOk) {
  XLOG(DBG1) << __func__ << " reqID=" << annOk.requestID << " sess=" << this;

  if (logger_) {
    logger_->logPublishNamespaceOk(annOk);
  }

  MOQ_SUBSCRIBER_STATS(subscriberStatsCallback_, onPublishNamespaceSuccess);
  auto res = moqFrameWriter_.writePublishNamespaceOk(controlWriteBuf_, annOk);
  if (!res) {
    XLOG(ERR) << "writePublishNamespaceOk failed sess=" << this;
    return;
  }
  controlWriteEvent_.signal();
}

void MoQRelaySession::publishNamespaceCancel(
    const PublishNamespaceCancel& annCan) {
  MOQ_SUBSCRIBER_STATS(subscriberStatsCallback_, onPublishNamespaceCancel);
  auto res =
      moqFrameWriter_.writePublishNamespaceCancel(controlWriteBuf_, annCan);
  if (!res) {
    XLOG(ERR) << "writePublishNamespaceCancel failed sess=" << this;
  }
  controlWriteEvent_.signal();

  if (annCan.requestID.has_value()) {
    publishNamespaceHandles_.erase(*annCan.requestID);
  }
  retireRequestID(/*signalWriteLoop=*/false);

  if (logger_) {
    logger_->logPublishNamespaceCancel(annCan);
  }
}

void MoQRelaySession::onPublishNamespaceDone(PublishNamespaceDone unAnn) {
  MOQ_SUBSCRIBER_STATS(subscriberStatsCallback_, onPublishNamespaceDone);

  // Set up request context so publishNamespaceDone() can identify this session.
  // This is needed because MoQRelay::publishNamespaceDone() uses
  // getRequestSession() to verify ownership.
  folly::RequestContextScopeGuard guard;
  setRequestSession();

  if (logger_) {
    logger_->logPublishNamespaceDone(
        unAnn, MOQTByteStringType::STRING_VALUE, ControlMessageType::PARSED);
  }

  // Version-specific lookup, common action
  std::shared_ptr<Subscriber::PublishNamespaceHandle> handle;

  RequestID reqId;
  if (getDraftMajorVersion(*getNegotiatedVersion()) >= 16) {
    CHECK(unAnn.requestID.has_value());
    XLOG(DBG1) << __func__ << " requestID=" << *unAnn.requestID
               << " sess=" << this;
    reqId = *unAnn.requestID;
  } else {
    // Legacy: translate NS -> RequestID
    XLOG(DBG1) << __func__ << " ns=" << unAnn.trackNamespace
               << " sess=" << this;
    auto nsIt = legacySubscriberNamespaceToReqId_.find(unAnn.trackNamespace);
    if (nsIt == legacySubscriberNamespaceToReqId_.end()) {
      XLOG(ERR) << "PublishNamespaceDone for unknown namespace ns="
                << unAnn.trackNamespace;
      return;
    }
    reqId = nsIt->second;
    legacySubscriberNamespaceToReqId_.erase(nsIt);
  }

  auto it = publishNamespaceHandles_.find(reqId);
  if (it == publishNamespaceHandles_.end()) {
    XLOG(ERR) << "PublishNamespaceDone for unknown requestID=" << reqId;
    return;
  }
  handle = std::move(it->second);
  publishNamespaceHandles_.erase(it);

  // Common action
  handle->publishNamespaceDone();
  retireRequestID(/*signalWriteLoop=*/true);
}

// SubscribeAnnounces subscriber methods
folly::coro::Task<Publisher::SubscribeNamespaceResult>
MoQRelaySession::subscribeNamespace(
    SubscribeNamespace sa,
    std::shared_ptr<NamespacePublishHandle> namespacePublishHandle) {
  XLOG(DBG1) << __func__ << " prefix=" << sa.trackNamespacePrefix
             << " sess=" << this;
  const auto& trackNamespace = sa.trackNamespacePrefix;
  aliasifyAuthTokens(sa.params);
  sa.requestID = getNextRequestID();

  auto res = moqFrameWriter_.writeSubscribeNamespace(controlWriteBuf_, sa);
  if (!res) {
    XLOG(ERR) << "writeSubscribeNamespace failed sess=" << this;
    co_return folly::makeUnexpected(SubscribeNamespaceError(
        {RequestID(0),
         SubscribeNamespaceErrorCode::INTERNAL_ERROR,
         "local write failed"}));
  }
  if (logger_) {
    logger_->logSubscribeNamespace(sa);
  }
  controlWriteEvent_.signal();
  auto contract = folly::coro::makePromiseContract<
      folly::Expected<SubscribeNamespaceOk, SubscribeNamespaceError>>();
  pendingRequests_.emplace(
      sa.requestID,
      MoQRelayPendingRequestState::makeSubscribeNamespace(
          std::move(contract.first)));
  auto subAnnResult = co_await std::move(contract.second);
  if (subAnnResult.hasError()) {
    MOQ_SUBSCRIBER_STATS(
        subscriberStatsCallback_,
        onSubscribeNamespaceError,
        subAnnResult.error().errorCode);
    co_return folly::makeUnexpected(subAnnResult.error());
  } else {
    MOQ_SUBSCRIBER_STATS(subscriberStatsCallback_, onSubscribeNamespaceSuccess);
    co_return std::make_shared<SubscribeNamespaceHandle>(
        std::static_pointer_cast<MoQRelaySession>(shared_from_this()),
        trackNamespace,
        std::move(subAnnResult.value()));
  }
}

void MoQRelaySession::unsubscribeNamespace(
    const UnsubscribeNamespace& unsubAnn) {
  // Log the appropriate field based on what's present
  if (unsubAnn.trackNamespacePrefix.has_value()) {
    XLOG(DBG1) << __func__
               << " prefix=" << unsubAnn.trackNamespacePrefix.value()
               << " sess=" << this;
  } else if (unsubAnn.requestID.has_value()) {
    XLOG(DBG1) << __func__ << " requestID=" << unsubAnn.requestID.value()
               << " sess=" << this;
  }

  MOQ_SUBSCRIBER_STATS(subscriberStatsCallback_, onUnsubscribeNamespace);
  auto res =
      moqFrameWriter_.writeUnsubscribeNamespace(controlWriteBuf_, unsubAnn);
  if (!res) {
    XLOG(ERR) << "writeUnsubscribeNamespace failed sess=" << this;
    return;
  }

  if (logger_) {
    logger_->logUnsubscribeNamespace(unsubAnn);
  }

  controlWriteEvent_.signal();
}

// SubscribeNamespace publisher methods
void MoQRelaySession::onSubscribeNamespaceImpl(
    const SubscribeNamespace& sa,
    std::unique_ptr<SubNSReply>&& subNsReply) {
  XLOG(DBG1) << __func__ << " prefix=" << sa.trackNamespacePrefix
             << " sess=" << this;
  if (logger_) {
    logger_->logSubscribeNamespace(
        sa, MOQTByteStringType::STRING_VALUE, ControlMessageType::PARSED);
  }
  if (closeSessionIfRequestIDInvalid(sa.requestID, false, true)) {
    return;
  }
  if (!publishHandler_) {
    XLOG(DBG1) << __func__ << "No publisher callback set";
    subscribeNamespaceError(
        SubscribeNamespaceError{
            sa.requestID,
            SubscribeNamespaceErrorCode::NOT_SUPPORTED,
            "Not a publisher"},
        std::move(subNsReply));
    return;
  }
  co_withExecutor(
      exec_.get(), handleSubscribeNamespace(sa, std::move(subNsReply)))
      .start();
}

folly::coro::Task<void> MoQRelaySession::handleSubscribeNamespace(
    SubscribeNamespace subAnn,
    std::unique_ptr<SubNSReply> subNsReply) {
  folly::RequestContextScopeGuard guard;
  setRequestSession();
  auto subAnnResult = co_await co_awaitTry(co_withCancellation(
      cancellationSource_.getToken(),
      publishHandler_->subscribeNamespace(subAnn, nullptr)));
  if (subAnnResult.hasException()) {
    XLOG(ERR) << "Exception in Publisher callback ex="
              << subAnnResult.exception().what().toStdString();
    subscribeNamespaceError(
        SubscribeNamespaceError{
            subAnn.requestID,
            SubscribeNamespaceErrorCode::INTERNAL_ERROR,
            subAnnResult.exception().what().toStdString()},
        std::move(subNsReply));
    co_return;
  }
  if (subAnnResult->hasError()) {
    XLOG(DBG1) << "Application subAnn error err="
               << subAnnResult->error().reasonPhrase;
    auto subAnnErr = std::move(subAnnResult->error());
    subAnnErr.requestID = subAnn.requestID; // In case app got it wrong
    // trackNamespacePrefix removed from unified RequestError
    subscribeNamespaceError(subAnnErr, std::move(subNsReply));
  } else {
    auto handle = std::move(subAnnResult->value());
    auto subAnnOk = handle->subscribeNamespaceOk();
    subscribeNamespaceOk(subAnnOk, std::move(subNsReply));

    // Store by RequestID (primary key)
    subscribeNamespaceHandles_[subAnn.requestID] = std::move(handle);
    if (getDraftMajorVersion(*getNegotiatedVersion()) < 15) {
      // Legacy: also store NS->RequestID mapping for lookups
      legacySubscribeNamespaceToReqId_[subAnn.trackNamespacePrefix] =
          subAnn.requestID;
    }
  }
}

void MoQRelaySession::subscribeNamespaceOk(
    const SubscribeNamespaceOk& saOk,
    std::unique_ptr<SubNSReply>&& subNsReply) {
  XLOG(DBG1) << __func__ << " id=" << saOk.requestID << " sess=" << this;
  MOQ_PUBLISHER_STATS(publisherStatsCallback_, onSubscribeNamespaceSuccess);
  auto res = subNsReply->ok(saOk);
  if (!res) {
    XLOG(ERR) << "writeSubscribeNamespaceOk failed sess=" << this;
    return;
  }

  if (logger_) {
    logger_->logSubscribeNamespaceOk(saOk);
  }

  controlWriteEvent_.signal();
}

void MoQRelaySession::onUnsubscribeNamespace(UnsubscribeNamespace unsub) {
  if (logger_) {
    logger_->logUnsubscribeNamespace(
        unsub, MOQTByteStringType::STRING_VALUE, ControlMessageType::PARSED);
  }
  MOQ_PUBLISHER_STATS(publisherStatsCallback_, onUnsubscribeNamespace);
  if (!publishHandler_) {
    XLOG(DBG1) << __func__ << "No publisher callback set";
    return;
  }

  // Version-based lookup
  std::shared_ptr<Publisher::SubscribeNamespaceHandle> handle;
  RequestID requestID;
  TrackNamespace ns;

  if (getDraftMajorVersion(*getNegotiatedVersion()) >= 15) {
    // v15+: Direct lookup by Request ID
    if (!unsub.requestID.has_value()) {
      XLOG(ERR) << __func__ << " missing requestID for v15+, sess=" << this;
      return;
    }
    requestID = unsub.requestID.value();
    XLOG(DBG1) << __func__ << " requestID=" << requestID << " sess=" << this;
  } else {
    // <v15: Two-step lookup via namespace
    if (!unsub.trackNamespacePrefix.has_value()) {
      XLOG(ERR) << __func__
                << " missing trackNamespacePrefix for <v15, sess=" << this;
      return;
    }
    ns = unsub.trackNamespacePrefix.value();
    XLOG(DBG1) << __func__ << " prefix=" << ns << " sess=" << this;

    auto nsIt = legacySubscribeNamespaceToReqId_.find(ns);
    if (nsIt == legacySubscribeNamespaceToReqId_.end()) {
      XLOG(ERR) << "Invalid unsub publishNamespace nsp=" << ns;
      return;
    }
    requestID = nsIt->second;
    legacySubscribeNamespaceToReqId_.erase(nsIt);
  }

  auto saIt = subscribeNamespaceHandles_.find(requestID);
  if (saIt == subscribeNamespaceHandles_.end()) {
    XLOG(ERR) << "Invalid unsub publishNamespace requestID=" << requestID;
    return;
  }
  handle = saIt->second;

  // Process unsubscribe
  folly::RequestContextScopeGuard guard;
  setRequestSession();
  handle->unsubscribeNamespace();
  subscribeNamespaceHandles_.erase(requestID);

  retireRequestID(/*signalWriteLoop=*/true);
}

// Helper methods for handling RequestOk
void MoQRelaySession::handlePublishNamespaceOkFromRequestOk(
    const RequestOk& requestOk,
    PendingRequestIterator reqIt) {
  XLOG(DBG1) << __func__ << " reqID=" << requestOk.requestID
             << " sess=" << this;

  if (logger_) {
    logger_->logPublishNamespaceOk(requestOk, ControlMessageType::PARSED);
  }

  auto* publishNamespacePtr =
      MoQRelayPendingRequestState::tryGetPublishNamespace(reqIt->second.get());
  if (!publishNamespacePtr) {
    XLOG(ERR) << "Request ID " << requestOk.requestID
              << " is not an publishNamespace request, sess=" << this;
    close(SessionCloseErrorCode::PROTOCOL_VIOLATION);
    return;
  }

  publishNamespaceCallbacks_[requestOk.requestID] =
      std::move(publishNamespacePtr->callback);
  if (getDraftMajorVersion(*getNegotiatedVersion()) < 16) {
    // Legacy: also store NS->RequestID mapping for lookups
    legacyPublisherNamespaceToReqId_[publishNamespacePtr->trackNamespace] =
        requestOk.requestID;
  }
  publishNamespacePtr->promise.setValue(requestOk);
}

void MoQRelaySession::handleSubscribeNamespaceOkFromRequestOk(
    const RequestOk& requestOk,
    PendingRequestIterator reqIt) {
  XLOG(DBG1) << __func__ << " reqID=" << requestOk.requestID
             << " sess=" << this;

  if (logger_) {
    logger_->logSubscribeNamespaceOk(requestOk, ControlMessageType::PARSED);
  }

  auto* subscribeNamespacePtr =
      MoQRelayPendingRequestState::tryGetSubscribeNamespace(
          reqIt->second.get());
  if (!subscribeNamespacePtr) {
    XLOG(ERR) << "Request ID " << requestOk.requestID
              << " is not a subscribe publishNamespaces request, sess=" << this;
    close(SessionCloseErrorCode::PROTOCOL_VIOLATION);
    return;
  }

  subscribeNamespacePtr->setValue(requestOk);
}

WriteResult SeparateStreamSubNsReply::ok(const SubscribeNamespaceOk& subNsOk) {
  if (errorSent_) {
    // We already sent an ERROR; protocol doesn't allow OK after that.
    return folly::makeUnexpected(quic::TransportErrorCode::PROTOCOL_VIOLATION);
  }
  auto res = moqFrameWriter_.writeSubscribeNamespaceOk(writeBuf_, subNsOk);
  writeHandle_->writeStreamData(writeBuf_.move(), false /* fin */, nullptr);
  okSent_ = true;
  flushPendingMessages();
  return res;
}

WriteResult SeparateStreamSubNsReply::namespaceMsg(const Namespace& msg) {
  if (errorSent_) {
    // No NAMESPACE frames allowed after we send an ERROR on this stream.
    return folly::makeUnexpected(quic::TransportErrorCode::PROTOCOL_VIOLATION);
  }
  auto res = moqFrameWriter_.writeNamespace(writeBuf_, msg);
  namespaceFrameSent_ = true;
  if (okSent_) {
    writeHandle_->writeStreamData(writeBuf_.move(), false /* fin */, nullptr);
  } else {
    pendingBuf_.append(writeBuf_.move());
  }
  return res;
}

WriteResult SeparateStreamSubNsReply::namespaceDoneMsg(
    const NamespaceDone& msg) {
  if (errorSent_) {
    // No NAMESPACE_DONE frames allowed after we send an ERROR on this stream.
    return folly::makeUnexpected(quic::TransportErrorCode::PROTOCOL_VIOLATION);
  }
  auto res = moqFrameWriter_.writeNamespaceDone(writeBuf_, msg);
  if (okSent_) {
    writeHandle_->writeStreamData(writeBuf_.move(), true /* fin */, nullptr);
  } else {
    pendingBuf_.append(writeBuf_.move());
    pendingFin_ = true;
  }
  return res;
}

void SeparateStreamSubNsReply::flushPendingMessages() {
  if (errorSent_) {
    // If we sent error before OK, we should never flush queued NAMESPACE
    // frames.
    pendingBuf_.move();
    pendingFin_ = false;
    return;
  }
  if (!pendingBuf_.empty()) {
    writeHandle_->writeStreamData(pendingBuf_.move(), pendingFin_, nullptr);
  }
}

} // namespace moxygen

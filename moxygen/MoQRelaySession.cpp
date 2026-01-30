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

class MoQRelaySession::SubscriberAnnounceCallback
    : public Subscriber::AnnounceCallback {
 public:
  SubscriberAnnounceCallback(
      MoQRelaySession& session,
      const TrackNamespace& ns,
      RequestID requestID)
      : session_(session), trackNamespace_(ns), requestID_(requestID) {}

  void announceCancel(AnnounceErrorCode errorCode, std::string reasonPhrase)
      override {
    AnnounceCancel annCan;
    annCan.requestID = requestID_;
    if (getDraftMajorVersion(*session_.getNegotiatedVersion()) < 16) {
      annCan.trackNamespace = trackNamespace_;
    }
    annCan.errorCode = errorCode;
    annCan.reasonPhrase = std::move(reasonPhrase);
    session_.announceCancel(annCan);
  }

 private:
  MoQRelaySession& session_;
  TrackNamespace trackNamespace_;
  RequestID requestID_;
};

class MoQRelaySession::PublisherAnnounceHandle
    : public Subscriber::AnnounceHandle {
 public:
  PublisherAnnounceHandle(
      std::shared_ptr<MoQRelaySession> session,
      TrackNamespace trackNamespace,
      AnnounceOk annOk)
      : Subscriber::AnnounceHandle(std::move(annOk)),
        trackNamespace_(std::move(trackNamespace)),
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
      Unannounce unann;
      if (getDraftMajorVersion(*session_->getNegotiatedVersion()) >= 16) {
        unann.requestID = announceOk().requestID;
      } else {
        unann.trackNamespace = trackNamespace_;
      }
      session_->unannounce(unann);
      session_.reset();
    }
  }

 private:
  TrackNamespace trackNamespace_;
  std::shared_ptr<MoQRelaySession> session_;
};

class MoQRelaySession::SubscribeAnnouncesHandle
    : public Publisher::SubscribeAnnouncesHandle {
 public:
  SubscribeAnnouncesHandle(
      std::shared_ptr<MoQRelaySession> session,
      TrackNamespace trackNamespacePrefix,
      SubscribeAnnouncesOk subAnnOk)
      : Publisher::SubscribeAnnouncesHandle(std::move(subAnnOk)),
        trackNamespacePrefix_(std::move(trackNamespacePrefix)),
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
      UnsubscribeAnnounces msg;

      // v15+: Send requestID
      if (getDraftMajorVersion(*session_->getNegotiatedVersion()) >= 15) {
        msg.requestID = subscribeAnnouncesOk_->requestID;
      } else {
        // <v15: Send namespace
        msg.trackNamespacePrefix = trackNamespacePrefix_;
      }

      session_->unsubscribeAnnounces(msg);
      session_.reset();
    }
  }

 private:
  TrackNamespace trackNamespacePrefix_;
  std::shared_ptr<MoQRelaySession> session_;
};

// MoQRelayPendingRequestState - extends base PendingRequestState with
// announcement support
class MoQRelaySession::MoQRelayPendingRequestState
    : public PendingRequestState {
 public:
  // Constructor for announcement types
  explicit MoQRelayPendingRequestState(PendingAnnounce announce) {
    type_ = Type::ANNOUNCE;
    new (&announceStorage_) PendingAnnounce(std::move(announce));
  }

  MoQRelayPendingRequestState(const MoQRelayPendingRequestState&) = delete;
  MoQRelayPendingRequestState& operator=(const MoQRelayPendingRequestState&) =
      delete;
  MoQRelayPendingRequestState(MoQRelayPendingRequestState&&) = delete;
  MoQRelayPendingRequestState& operator=(MoQRelayPendingRequestState&&) =
      delete;

  explicit MoQRelayPendingRequestState(
      folly::coro::Promise<
          folly::Expected<SubscribeAnnouncesOk, SubscribeAnnouncesError>>
          promise) {
    type_ = Type::SUBSCRIBE_ANNOUNCES;
    new (&subscribeAnnouncesStorage_) auto(std::move(promise));
  }

  folly::Expected<Type, folly::Unit> setError(
      RequestError error,
      FrameType frameType) override {
    switch (type_) {
      case Type::ANNOUNCE:
        if (auto* announcePtr = tryGetAnnounce(this)) {
          announcePtr->promise.setValue(
              folly::makeUnexpected(std::move(error)));
          return type_;
        }
        return folly::makeUnexpected(folly::unit);
      case Type::SUBSCRIBE_ANNOUNCES:
        if (auto* subscribeAnnouncesPtr = tryGetSubscribeAnnounces(this)) {
          subscribeAnnouncesPtr->setValue(
              folly::makeUnexpected(std::move(error)));
          return type_;
        }
        return folly::makeUnexpected(folly::unit);
      default:
        return PendingRequestState::setError(std::move(error), frameType);
    }
  }

  // Factory methods for announcement types
  static std::unique_ptr<MoQRelayPendingRequestState> makeAnnounce(
      PendingAnnounce pendingAnnounce) {
    return std::make_unique<MoQRelayPendingRequestState>(
        std::move(pendingAnnounce));
  }

  static std::unique_ptr<MoQRelayPendingRequestState> makeSubscribeAnnounces(
      folly::coro::Promise<
          folly::Expected<SubscribeAnnouncesOk, SubscribeAnnouncesError>>
          promise) {
    return std::make_unique<MoQRelayPendingRequestState>(std::move(promise));
  }

  // Override destructor to handle announcement storage
  ~MoQRelayPendingRequestState() override {
    switch (getType()) {
      case Type::ANNOUNCE:
        announceStorage_.~PendingAnnounce();
        break;
      case Type::SUBSCRIBE_ANNOUNCES:
        subscribeAnnouncesStorage_.~Promise();
        break;
      default:
        // Base class handles other types
        break;
    }
  }

  // Additional tryGet methods for announcement types
  static PendingAnnounce* FOLLY_NULLABLE
  tryGetAnnounce(PendingRequestState* base) {
    if (base && base->getType() == Type::ANNOUNCE) {
      auto* relay = static_cast<MoQRelayPendingRequestState*>(base);
      return &relay->announceStorage_;
    }
    return nullptr;
  }

  static folly::coro::Promise<
      folly::Expected<SubscribeAnnouncesOk, SubscribeAnnouncesError>>*
      FOLLY_NULLABLE
      tryGetSubscribeAnnounces(PendingRequestState* base) {
    if (base && base->getType() == Type::SUBSCRIBE_ANNOUNCES) {
      auto* relay = static_cast<MoQRelayPendingRequestState*>(base);
      return &relay->subscribeAnnouncesStorage_;
    }
    return nullptr;
  }

 private:
  // Extended storage for announcement types (separate from base union)
  union {
    PendingAnnounce announceStorage_;
    folly::coro::Promise<
        folly::Expected<SubscribeAnnouncesOk, SubscribeAnnouncesError>>
        subscribeAnnouncesStorage_;
  };
};

void MoQRelaySession::cleanup() {
  // Set up request context so unannounce() can identify this session.
  // This is needed because cleanup() is called directly (not from a message
  // handler) and MoQRelay::unannounce() uses getRequestSession() to verify
  // ownership.
  folly::RequestContextScopeGuard guard;
  setRequestSession();

  // Clean up announce maps (single source of truth)
  for (auto& ann : publisherAnnounces_) {
    if (ann.second) {
      ann.second->announceCancel(
          AnnounceErrorCode::INTERNAL_ERROR, "Session ended");
    }
  }
  publisherAnnounces_.clear();
  legacyPublisherAnnounceNsToReqId_.clear();

  for (auto& ann : subscriberAnnounces_) {
    ann.second->unannounce();
  }
  subscriberAnnounces_.clear();
  legacySubscriberAnnounceNsToReqId_.clear();

  // Clean up subscribeAnnounces handles
  for (auto& subAnn : subscribeAnnounces_) {
    if (subAnn.second) {
      subAnn.second->unsubscribeAnnounces();
    }
  }
  subscribeAnnounces_.clear();
  legacySubscribeAnnouncesNsToReqId_.clear();

  // Call parent cleanup to handle base class cleanup
  MoQSession::cleanup();
}

// Announce publisher methods
folly::coro::Task<Subscriber::AnnounceResult> MoQRelaySession::announce(
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
  const auto& trackNamespace = ann.trackNamespace;
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
      MoQRelayPendingRequestState::makeAnnounce(
          PendingAnnounce{
              trackNamespace, // Use saved copy instead of ann.trackNamespace
              std::move(contract.first),
              std::move(announceCallback)}));
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
        std::static_pointer_cast<MoQRelaySession>(shared_from_this()),
        trackNamespace,
        std::move(announceResult.value()));
  }
}

void MoQRelaySession::onRequestOk(RequestOk requestOk, FrameType frameType) {
  XLOG(DBG1) << __func__ << " id=" << requestOk.requestID << " sess=" << this;

  auto reqID = requestOk.requestID;
  auto reqIt = pendingRequests_.find(reqID);
  bool shouldErasePendingRequest = true;

  if (reqIt == pendingRequests_.end()) {
    // unknown
    XLOG(ERR) << "No matching announce reqID=" << reqID << " sess=" << this;
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
    case moxygen::FrameType::SUBSCRIBE_ANNOUNCES_OK:
    case moxygen::FrameType::REQUEST_OK: {
      // ANNOUNCE_OK is an alias for REQUEST_OK (both = 0x7)
      switch (reqIt->second->getType()) {
        case PendingRequestState::Type::ANNOUNCE:
          handleAnnounceOkFromRequestOk(requestOk, reqIt);
          break;
        case PendingRequestState::Type::SUBSCRIBE_ANNOUNCES:
          handleSubscribeAnnouncesOkFromRequestOk(requestOk, reqIt);
          break;
        case PendingRequestState::Type::SUBSCRIBE_UPDATE:
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

void MoQRelaySession::onAnnounceCancel(AnnounceCancel announceCancel) {
  MOQ_PUBLISHER_STATS(publisherStatsCallback_, onAnnounceCancel);

  if (logger_) {
    logger_->logAnnounceCancel(
        announceCancel,
        MOQTByteStringType::STRING_VALUE,
        ControlMessageType::PARSED);
  }

  std::shared_ptr<Subscriber::AnnounceCallback> cb;

  RequestID reqId;
  if (getDraftMajorVersion(*getNegotiatedVersion()) >= 16) {
    CHECK(announceCancel.requestID.has_value());
    XLOG(DBG1) << __func__ << " requestID=" << *announceCancel.requestID
               << " sess=" << this;
    reqId = *announceCancel.requestID;
  } else {
    // Legacy: translate NS -> RequestID
    XLOG(DBG1) << __func__ << " ns=" << announceCancel.trackNamespace
               << " sess=" << this;
    auto nsIt =
        legacyPublisherAnnounceNsToReqId_.find(announceCancel.trackNamespace);
    if (nsIt == legacyPublisherAnnounceNsToReqId_.end()) {
      XLOG(ERR) << "Invalid announce cancel ns="
                << announceCancel.trackNamespace;
      return;
    }
    reqId = nsIt->second;
    legacyPublisherAnnounceNsToReqId_.erase(nsIt);
  }

  auto it = publisherAnnounces_.find(reqId);
  if (it == publisherAnnounces_.end()) {
    XLOG(ERR) << "Invalid announce cancel requestID=" << reqId;
    return;
  }
  cb = std::move(it->second);
  publisherAnnounces_.erase(it);

  // Common action
  if (cb) {
    cb->announceCancel(
        announceCancel.errorCode, std::move(announceCancel.reasonPhrase));
  }
}

void MoQRelaySession::unannounce(const Unannounce& unann) {
  MOQ_PUBLISHER_STATS(publisherStatsCallback_, onUnannounce);

  if (logger_) {
    logger_->logUnannounce(unann);
  }

  // Lambda helper to fail a pending announce and erase it
  auto failPendingAnnounce = [this](auto pendingIt, RequestID reqId) {
    if (auto* announcePtr = MoQRelayPendingRequestState::tryGetAnnounce(
            pendingIt->second.get())) {
      announcePtr->promise.setValue(
          folly::makeUnexpected(AnnounceError(
              {reqId,
               AnnounceErrorCode::INTERNAL_ERROR,
               "Unannounce before AnnounceOK"})));
    }
    pendingRequests_.erase(pendingIt);
  };

  // Lambda helper to write the unannounce frame
  auto writeUnannounceToWire = [this, &unann]() {
    auto res = moqFrameWriter_.writeUnannounce(controlWriteBuf_, unann);
    if (!res) {
      XLOG(ERR) << "writeUnannounce failed sess=" << this;
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
    auto nsIt = legacyPublisherAnnounceNsToReqId_.find(unann.trackNamespace);
    if (nsIt != legacyPublisherAnnounceNsToReqId_.end()) {
      reqId = nsIt->second;
      legacyPublisherAnnounceNsToReqId_.erase(nsIt);
    }
    // If not found, reqId remains nullopt - will search pending by namespace
  }

  // Find and remove announce
  if (reqId.has_value()) {
    auto it = publisherAnnounces_.find(*reqId);
    if (it != publisherAnnounces_.end()) {
      publisherAnnounces_.erase(it);
      found = true;
    } else {
      // Check pending requests by RequestID
      auto pendingIt = pendingRequests_.find(*reqId);
      if (pendingIt != pendingRequests_.end()) {
        failPendingAnnounce(pendingIt, *reqId);
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
          if (auto* announcePtr = MoQRelayPendingRequestState::tryGetAnnounce(
                  pair.second.get())) {
            return announcePtr->trackNamespace == unann.trackNamespace;
          }
          return false;
        });
    if (pendingIt != pendingRequests_.end()) {
      failPendingAnnounce(pendingIt, pendingIt->first);
      found = true;
    }
  }

  if (found) {
    writeUnannounceToWire();
  } else {
    XLOG(ERR) << "Unannounce for unknown announce, sess=" << this;
  }
}

// Announce subscriber methods
void MoQRelaySession::onAnnounce(Announce ann) {
  XLOG(DBG1) << __func__ << " ns=" << ann.trackNamespace << " sess=" << this;

  if (logger_) {
    logger_->logAnnounce(
        ann, MOQTByteStringType::STRING_VALUE, ControlMessageType::PARSED);
  }

  if (closeSessionIfRequestIDInvalid(ann.requestID, false, true)) {
    return;
  }

  if (!subscribeHandler_) {
    XLOG(DBG1) << __func__ << "No subscriber callback set";
    announceError(
        AnnounceError{
            ann.requestID,
            AnnounceErrorCode::NOT_SUPPORTED,
            "Not a subscriber"});
    return;
  }
  co_withExecutor(exec_.get(), handleAnnounce(std::move(ann))).start();
}

folly::coro::Task<void> MoQRelaySession::handleAnnounce(Announce announce) {
  folly::RequestContextScopeGuard guard;
  setRequestSession();
  auto annCb = std::make_shared<SubscriberAnnounceCallback>(
      *this, announce.trackNamespace, announce.requestID);
  auto announceResult = co_await co_awaitTry(co_withCancellation(
      cancellationSource_.getToken(),
      subscribeHandler_->announce(announce, std::move(annCb))));
  if (announceResult.hasException()) {
    XLOG(ERR) << "Exception in Subscriber callback ex="
              << announceResult.exception().what().toStdString();
    announceError(
        AnnounceError{
            announce.requestID,
            AnnounceErrorCode::INTERNAL_ERROR,
            announceResult.exception().what().toStdString()});
    co_return;
  }
  if (announceResult->hasError()) {
    XLOG(DBG1) << "Application announce error err="
               << announceResult->error().reasonPhrase;
    auto annErr = std::move(announceResult->error());
    annErr.requestID = announce.requestID; // In case app got it wrong
    // trackNamespace removed from unified RequestError
    announceError(annErr);
  } else {
    auto handle = std::move(announceResult->value());
    auto announceOkMsg = handle->announceOk();
    announceOk(announceOkMsg);
    subscriberAnnounces_[announce.requestID] = std::move(handle);
    if (getDraftMajorVersion(*getNegotiatedVersion()) < 16) {
      // Legacy: also store NS->RequestID mapping for lookups
      legacySubscriberAnnounceNsToReqId_[announce.trackNamespace] =
          announce.requestID;
    }
  }
}

void MoQRelaySession::announceOk(const AnnounceOk& annOk) {
  XLOG(DBG1) << __func__ << " reqID=" << annOk.requestID << " sess=" << this;

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

void MoQRelaySession::announceCancel(const AnnounceCancel& annCan) {
  MOQ_SUBSCRIBER_STATS(subscriberStatsCallback_, onAnnounceCancel);
  auto res = moqFrameWriter_.writeAnnounceCancel(controlWriteBuf_, annCan);
  if (!res) {
    XLOG(ERR) << "writeAnnounceCancel failed sess=" << this;
  }
  controlWriteEvent_.signal();

  if (annCan.requestID.has_value()) {
    subscriberAnnounces_.erase(*annCan.requestID);
  }
  retireRequestID(/*signalWriteLoop=*/false);

  if (logger_) {
    logger_->logAnnounceCancel(annCan);
  }
}

void MoQRelaySession::onUnannounce(Unannounce unAnn) {
  MOQ_SUBSCRIBER_STATS(subscriberStatsCallback_, onUnannounce);

  // Set up request context so unannounce() can identify this session.
  // This is needed because MoQRelay::unannounce() uses getRequestSession()
  // to verify ownership.
  folly::RequestContextScopeGuard guard;
  setRequestSession();

  if (logger_) {
    logger_->logUnannounce(
        unAnn, MOQTByteStringType::STRING_VALUE, ControlMessageType::PARSED);
  }

  // Version-specific lookup, common action
  std::shared_ptr<Subscriber::AnnounceHandle> handle;

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
    auto nsIt = legacySubscriberAnnounceNsToReqId_.find(unAnn.trackNamespace);
    if (nsIt == legacySubscriberAnnounceNsToReqId_.end()) {
      XLOG(ERR) << "Unannounce for unknown namespace ns="
                << unAnn.trackNamespace;
      return;
    }
    reqId = nsIt->second;
    legacySubscriberAnnounceNsToReqId_.erase(nsIt);
  }

  auto it = subscriberAnnounces_.find(reqId);
  if (it == subscriberAnnounces_.end()) {
    XLOG(ERR) << "Unannounce for unknown requestID=" << reqId;
    return;
  }
  handle = std::move(it->second);
  subscriberAnnounces_.erase(it);

  // Common action
  handle->unannounce();
  retireRequestID(/*signalWriteLoop=*/true);
}

// SubscribeAnnounces subscriber methods
folly::coro::Task<Publisher::SubscribeAnnouncesResult>
MoQRelaySession::subscribeAnnounces(SubscribeAnnounces sa) {
  XLOG(DBG1) << __func__ << " prefix=" << sa.trackNamespacePrefix
             << " sess=" << this;
  const auto& trackNamespace = sa.trackNamespacePrefix;
  aliasifyAuthTokens(sa.params);
  sa.requestID = getNextRequestID();

  auto res = moqFrameWriter_.writeSubscribeAnnounces(controlWriteBuf_, sa);
  if (!res) {
    XLOG(ERR) << "writeSubscribeAnnounces failed sess=" << this;
    co_return folly::makeUnexpected(SubscribeAnnouncesError(
        {RequestID(0),
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
      MoQRelayPendingRequestState::makeSubscribeAnnounces(
          std::move(contract.first)));
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
        std::static_pointer_cast<MoQRelaySession>(shared_from_this()),
        trackNamespace,
        std::move(subAnnResult.value()));
  }
}

void MoQRelaySession::unsubscribeAnnounces(
    const UnsubscribeAnnounces& unsubAnn) {
  // Log the appropriate field based on what's present
  if (unsubAnn.trackNamespacePrefix.has_value()) {
    XLOG(DBG1) << __func__
               << " prefix=" << unsubAnn.trackNamespacePrefix.value()
               << " sess=" << this;
  } else if (unsubAnn.requestID.has_value()) {
    XLOG(DBG1) << __func__ << " requestID=" << unsubAnn.requestID.value()
               << " sess=" << this;
  }

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

// SubscribeAnnounces publisher methods
void MoQRelaySession::onSubscribeAnnounces(SubscribeAnnounces sa) {
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
        SubscribeAnnouncesError{
            sa.requestID,
            SubscribeAnnouncesErrorCode::NOT_SUPPORTED,
            "Not a publisher"});
    return;
  }
  co_withExecutor(exec_.get(), handleSubscribeAnnounces(std::move(sa))).start();
}

folly::coro::Task<void> MoQRelaySession::handleSubscribeAnnounces(
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
        SubscribeAnnouncesError{
            subAnn.requestID,
            SubscribeAnnouncesErrorCode::INTERNAL_ERROR,
            subAnnResult.exception().what().toStdString()});
    co_return;
  }
  if (subAnnResult->hasError()) {
    XLOG(DBG1) << "Application subAnn error err="
               << subAnnResult->error().reasonPhrase;
    auto subAnnErr = std::move(subAnnResult->error());
    subAnnErr.requestID = subAnn.requestID; // In case app got it wrong
    // trackNamespacePrefix removed from unified RequestError
    subscribeAnnouncesError(subAnnErr);
  } else {
    auto handle = std::move(subAnnResult->value());
    auto subAnnOk = handle->subscribeAnnouncesOk();
    subscribeAnnouncesOk(subAnnOk);

    // Store by RequestID (primary key)
    subscribeAnnounces_[subAnn.requestID] = std::move(handle);
    if (getDraftMajorVersion(*getNegotiatedVersion()) < 15) {
      // Legacy: also store NS->RequestID mapping for lookups
      legacySubscribeAnnouncesNsToReqId_[subAnn.trackNamespacePrefix] =
          subAnn.requestID;
    }
  }
}

void MoQRelaySession::subscribeAnnouncesOk(const SubscribeAnnouncesOk& saOk) {
  XLOG(DBG1) << __func__ << " id=" << saOk.requestID << " sess=" << this;
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

void MoQRelaySession::onUnsubscribeAnnounces(UnsubscribeAnnounces unsub) {
  if (logger_) {
    logger_->logUnsubscribeAnnounces(
        unsub, MOQTByteStringType::STRING_VALUE, ControlMessageType::PARSED);
  }
  MOQ_PUBLISHER_STATS(publisherStatsCallback_, onUnsubscribeAnnounces);
  if (!publishHandler_) {
    XLOG(DBG1) << __func__ << "No publisher callback set";
    return;
  }

  // Version-based lookup
  std::shared_ptr<Publisher::SubscribeAnnouncesHandle> handle;
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

    auto nsIt = legacySubscribeAnnouncesNsToReqId_.find(ns);
    if (nsIt == legacySubscribeAnnouncesNsToReqId_.end()) {
      XLOG(ERR) << "Invalid unsub announce nsp=" << ns;
      return;
    }
    requestID = nsIt->second;
    legacySubscribeAnnouncesNsToReqId_.erase(nsIt);
  }

  auto saIt = subscribeAnnounces_.find(requestID);
  if (saIt == subscribeAnnounces_.end()) {
    XLOG(ERR) << "Invalid unsub announce requestID=" << requestID;
    return;
  }
  handle = saIt->second;

  // Process unsubscribe
  folly::RequestContextScopeGuard guard;
  setRequestSession();
  handle->unsubscribeAnnounces();
  subscribeAnnounces_.erase(requestID);

  retireRequestID(/*signalWriteLoop=*/true);
}

// Helper methods for handling RequestOk
void MoQRelaySession::handleAnnounceOkFromRequestOk(
    const RequestOk& requestOk,
    PendingRequestIterator reqIt) {
  XLOG(DBG1) << __func__ << " reqID=" << requestOk.requestID
             << " sess=" << this;

  if (logger_) {
    logger_->logAnnounceOk(requestOk, ControlMessageType::PARSED);
  }

  auto* announcePtr =
      MoQRelayPendingRequestState::tryGetAnnounce(reqIt->second.get());
  if (!announcePtr) {
    XLOG(ERR) << "Request ID " << requestOk.requestID
              << " is not an announce request, sess=" << this;
    close(SessionCloseErrorCode::PROTOCOL_VIOLATION);
    return;
  }

  publisherAnnounces_[requestOk.requestID] = std::move(announcePtr->callback);
  if (getDraftMajorVersion(*getNegotiatedVersion()) < 16) {
    // Legacy: also store NS->RequestID mapping for lookups
    legacyPublisherAnnounceNsToReqId_[announcePtr->trackNamespace] =
        requestOk.requestID;
  }
  announcePtr->promise.setValue(requestOk);
}

void MoQRelaySession::handleSubscribeAnnouncesOkFromRequestOk(
    const RequestOk& requestOk,
    PendingRequestIterator reqIt) {
  XLOG(DBG1) << __func__ << " reqID=" << requestOk.requestID
             << " sess=" << this;

  if (logger_) {
    logger_->logSubscribeAnnouncesOk(requestOk, ControlMessageType::PARSED);
  }

  auto* subscribeAnnouncesPtr =
      MoQRelayPendingRequestState::tryGetSubscribeAnnounces(
          reqIt->second.get());
  if (!subscribeAnnouncesPtr) {
    XLOG(ERR) << "Request ID " << requestOk.requestID
              << " is not a subscribe announces request, sess=" << this;
    close(SessionCloseErrorCode::PROTOCOL_VIOLATION);
    return;
  }

  subscribeAnnouncesPtr->setValue(requestOk);
}

} // namespace moxygen

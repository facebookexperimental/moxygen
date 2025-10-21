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
  SubscriberAnnounceCallback(MoQRelaySession& session, const TrackNamespace& ns)
      : session_(session), trackNamespace_(ns) {}

  void announceCancel(AnnounceErrorCode errorCode, std::string reasonPhrase)
      override {
    session_.announceCancel(
        {trackNamespace_, errorCode, std::move(reasonPhrase)});
  }

 private:
  MoQRelaySession& session_;
  TrackNamespace trackNamespace_;
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
      session_->unannounce({trackNamespace_});
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
      session_->unsubscribeAnnounces({trackNamespacePrefix_});
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
  // Clean up announcement-specific state first
  for (auto& ann : publisherAnnounces_) {
    if (ann.second) {
      ann.second->announceCancel(
          AnnounceErrorCode::INTERNAL_ERROR, "Session ended");
    }
  }
  publisherAnnounces_.clear();

  for (auto& ann : subscriberAnnounces_) {
    ann.second->unannounce(); // AnnounceHandle has unannounce() method
  }
  subscriberAnnounces_.clear();

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
      MoQRelayPendingRequestState::makeAnnounce(PendingAnnounce{
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

void MoQRelaySession::onAnnounceOk(AnnounceOk annOk) {
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

  auto* announcePtr =
      MoQRelayPendingRequestState::tryGetAnnounce(annIt->second.get());
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

void MoQRelaySession::onAnnounceCancel(AnnounceCancel announceCancel) {
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

void MoQRelaySession::unannounce(const Unannounce& unann) {
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
          if (auto* announcePtr = MoQRelayPendingRequestState::tryGetAnnounce(
                  pair.second.get())) {
            return announcePtr->trackNamespace == unann.trackNamespace;
          }
          return false;
        });

    if (pendingIt != pendingRequests_.end()) {
      if (auto* announcePtr = MoQRelayPendingRequestState::tryGetAnnounce(
              pendingIt->second.get())) {
        announcePtr->promise.setValue(folly::makeUnexpected(AnnounceError(
            {pendingIt->first,
             AnnounceErrorCode::INTERNAL_ERROR,
             "Unannounce before AnnounceOK"})));
      }
      pendingRequests_.erase(pendingIt);
    } else {
      XLOG(ERR) << "Unannounce (cancelled?) ns=" << unann.trackNamespace;
      return;
    }
  }
  auto res = moqFrameWriter_.writeUnannounce(controlWriteBuf_, unann);
  if (!res) {
    XLOG(ERR) << "writeUnannounce failed sess=" << this;
  }
  controlWriteEvent_.signal();
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
    announceError(AnnounceError{
        ann.requestID, AnnounceErrorCode::NOT_SUPPORTED, "Not a subscriber"});
  }
  co_withExecutor(exec_.get(), handleAnnounce(std::move(ann))).start();
}

folly::coro::Task<void> MoQRelaySession::handleAnnounce(Announce announce) {
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
    announceError(AnnounceError{
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
    announceOkMsg.trackNamespace = announce.trackNamespace;
    announceOk(announceOkMsg);
    // TODO: what about UNANNOUNCE before ANNOUNCE_OK
    subscriberAnnounces_[announce.trackNamespace] = std::move(handle);
  }
}

void MoQRelaySession::announceOk(const AnnounceOk& annOk) {
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

void MoQRelaySession::announceCancel(const AnnounceCancel& annCan) {
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

void MoQRelaySession::onUnannounce(Unannounce unAnn) {
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

void MoQRelaySession::onSubscribeAnnouncesOk(SubscribeAnnouncesOk saOk) {
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

  auto* subscribeAnnouncesPtr =
      MoQRelayPendingRequestState::tryGetSubscribeAnnounces(saIt->second.get());
  if (!subscribeAnnouncesPtr) {
    XLOG(ERR) << "Request ID " << reqID
              << " is not a subscribe announces request, sess=" << this;
    close(SessionCloseErrorCode::PROTOCOL_VIOLATION);
    return;
  }

  subscribeAnnouncesPtr->setValue(std::move(saOk));
  pendingRequests_.erase(saIt);
}

void MoQRelaySession::unsubscribeAnnounces(
    const UnsubscribeAnnounces& unsubAnn) {
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
    subscribeAnnouncesError(SubscribeAnnouncesError{
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
    subscribeAnnouncesError(SubscribeAnnouncesError{
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
    subAnnOk.trackNamespacePrefix = subAnn.trackNamespacePrefix;
    subscribeAnnouncesOk(subAnnOk);
    subscribeAnnounces_[subAnn.trackNamespacePrefix] = std::move(handle);
  }
}

void MoQRelaySession::subscribeAnnouncesOk(const SubscribeAnnouncesOk& saOk) {
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

void MoQRelaySession::onUnsubscribeAnnounces(UnsubscribeAnnounces unsub) {
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
  // TODO: also search pendingRequests_?
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

} // namespace moxygen

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
constexpr std::chrono::seconds kSetupTimeout(5);
}

namespace moxygen {

MoQSession::~MoQSession() {
  XLOG(DBG1) << "requestCancellation from dtor" << " sess=" << this;
  cancellationSource_.requestCancellation();
  for (auto& subTrack : subTracks_) {
    subTrack.second->subscribeError(
        {subTrack.first, 500, "session closed", folly::none});
  }
  for (auto& pendingAnn : pendingAnnounce_) {
    pendingAnn.second.setValue(folly::makeUnexpected(
        AnnounceError({pendingAnn.first, 500, "session closed"})));
  }
  for (auto& pendingSn : pendingSubscribeAnnounces_) {
    pendingSn.second.setValue(folly::makeUnexpected(
        SubscribeAnnouncesError({pendingSn.first, 500, "session closed"})));
  }
}

void MoQSession::start() {
  XLOG(DBG1) << __func__ << " sess=" << this;
  if (dir_ == MoQControlCodec::Direction::CLIENT) {
    auto cs = wt_->createBidiStream();
    if (!cs) {
      XLOG(ERR) << "Failed to get control stream" << " sess=" << this;
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
    readLoop(StreamType::CONTROL, controlStream.readHandle)
        .scheduleOn(evb_)
        .start();
  }
}

void MoQSession::close(folly::Optional<SessionCloseErrorCode> error) {
  if (wt_) {
    // TODO: The error code should be propagated to
    // whatever implemented proxygen::WebTransport.
    // TxnWebTransport current just ignores the errorCode
    auto wt = wt_;
    wt_ = nullptr;
    wt->closeSession(
        error.has_value()
            ? folly::make_optional(folly::to_underlying(error.value()))
            : folly::none);
    XLOG(DBG1) << "requestCancellation from close" << " sess=" << this;
    cancellationSource_.requestCancellation();
  }
}

folly::coro::Task<void> MoQSession::controlWriteLoop(
    proxygen::WebTransport::StreamWriteHandle* controlStream) {
  XLOG(DBG1) << __func__ << " sess=" << this;
  while (true) {
    co_await folly::coro::co_safe_point;
    if (controlWriteBuf_.empty()) {
      controlWriteEvent_.reset();
      auto res = co_await co_awaitTry(controlWriteEvent_.wait());
      if (res.tryGetExceptionObject<folly::FutureTimeout>()) {
      } else if (res.tryGetExceptionObject<folly::OperationCancelled>()) {
        co_return;
      }
    }
    co_await folly::coro::co_safe_point;
    auto writeRes =
        controlStream->writeStreamData(controlWriteBuf_.move(), false);
    if (writeRes) {
      co_await std::move(writeRes.value()).via(evb_);
    }
  }
}

void MoQSession::setup(ClientSetup setup) {
  XCHECK(dir_ == MoQControlCodec::Direction::CLIENT);
  XLOG(DBG1) << __func__ << " sess=" << this;
  auto maxSubscribeId = getMaxSubscribeIdIfPresent(setup.params);
  auto res = writeClientSetup(controlWriteBuf_, std::move(setup));
  if (!res) {
    XLOG(ERR) << "writeClientSetup failed" << " sess=" << this;
    return;
  }
  maxSubscribeID_ = maxSubscribeId;
  sentSetup_.signal();
  controlWriteEvent_.signal();
}

void MoQSession::setup(ServerSetup setup) {
  XLOG(DBG1) << __func__ << " sess=" << this;
  XCHECK(dir_ == MoQControlCodec::Direction::SERVER);
  auto maxSubscribeId = getMaxSubscribeIdIfPresent(setup.params);
  auto res = writeServerSetup(controlWriteBuf_, std::move(setup));
  if (!res) {
    XLOG(ERR) << "writeServerSetup failed" << " sess=" << this;
    return;
  }
  maxSubscribeID_ = maxSubscribeId;
  sentSetup_.signal();
  controlWriteEvent_.signal();
}

folly::coro::Task<void> MoQSession::setupComplete() {
  auto deletedToken = cancellationSource_.getToken();
  auto token = co_await folly::coro::co_current_cancellation_token;
  folly::EventBaseThreadTimekeeper tk(*evb_);
  auto result = co_await co_awaitTry(folly::coro::co_withCancellation(
      folly::CancellationToken::merge(deletedToken, token),
      folly::coro::timeout(
          folly::coro::collectAllTry(sentSetup_.wait(), receivedSetup_.wait()),
          kSetupTimeout,
          &tk)));
  if (deletedToken.isCancellationRequested()) {
    co_return;
  }
  setupComplete_ =
      (result.hasValue() && std::get<0>(result.value()).hasValue() &&
       std::get<1>(result.value()).hasValue());
  if (!token.isCancellationRequested() && !setupComplete_) {
    close();
    co_yield folly::coro::co_error(std::runtime_error("setup failed"));
  }
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
  std::unique_ptr<MoQCodec> codec;
  if (streamType == StreamType::CONTROL) {
    codec = std::make_unique<MoQControlCodec>(dir_, this);
  } else {
    codec = std::make_unique<MoQObjectStreamCodec>(this);
  }
  auto id = readHandle->getID();
  codec->setStreamId(id);

  // TODO: disallow OBJECT on control streams and non-object on non-control
  bool fin = false;
  while (!fin) {
    auto streamData = co_await folly::coro::co_awaitTry(
        readHandle->readStreamData().via(evb_));
    if (streamData.hasException()) {
      XLOG(ERR) << streamData.exception().what() << " id=" << id
                << " sess=" << this;
      co_return;
    } else {
      if (streamData->data || streamData->fin) {
        codec->onIngress(std::move(streamData->data), streamData->fin);
      }
      fin = streamData->fin;
      XLOG_IF(DBG3, fin) << "End of stream id=" << id << " sess=" << this;
    }
  }
}

void MoQSession::onClientSetup(ClientSetup clientSetup) {
  XLOG(DBG1) << __func__ << " sess=" << this;
  if (std::find(
          clientSetup.supportedVersions.begin(),
          clientSetup.supportedVersions.end(),
          kVersionDraftCurrent) == clientSetup.supportedVersions.end()) {
    XLOG(ERR) << "No matching versions" << " sess=" << this;
    for (auto v : clientSetup.supportedVersions) {
      XLOG(ERR) << "client sent=" << v << " sess=" << this;
    }
    close();
    receivedSetup_.cancel();
    return;
  }
  peerMaxSubscribeID_ = getMaxSubscribeIdIfPresent(clientSetup.params);
  receivedSetup_.signal();
  controlMessages_.enqueue(std::move(clientSetup));
  setupComplete().scheduleOn(evb_).start();
}

void MoQSession::onServerSetup(ServerSetup serverSetup) {
  XLOG(DBG1) << __func__ << " sess=" << this;
  if (serverSetup.selectedVersion != kVersionDraftCurrent) {
    XLOG(ERR) << "Invalid version = " << serverSetup.selectedVersion
              << " sess=" << this;
    close();
    receivedSetup_.cancel();
    return;
  }
  peerMaxSubscribeID_ = getMaxSubscribeIdIfPresent(serverSetup.params);
  receivedSetup_.signal();
  controlMessages_.enqueue(std::move(serverSetup));
}

void MoQSession::onObjectHeader(ObjectHeader objHeader) {
  XLOG(DBG1) << "MoQSession::" << __func__ << " " << objHeader
             << " sess=" << this;
  auto trackIt = subTracks_.find(objHeader.subscribeID);
  if (trackIt == subTracks_.end()) {
    // received an object for unknown sub id
    XLOG(ERR) << "unknown sub id=" << objHeader.subscribeID << " sess=" << this;
    return;
  }
  trackIt->second->onObjectHeader(std::move(objHeader));
}

void MoQSession::onObjectPayload(
    uint64_t subscribeID,
    uint64_t /*trackAlias*/,
    uint64_t groupID,
    uint64_t id,
    std::unique_ptr<folly::IOBuf> payload,
    bool eom) {
  XLOG(DBG1) << __func__ << " sess=" << this;
  auto trackIt = subTracks_.find(subscribeID);
  if (trackIt == subTracks_.end()) {
    // received an object for unknown sub id
    XLOG(ERR) << "unknown subscribeID=" << subscribeID << " sess=" << this;
    return;
  }
  trackIt->second->onObjectPayload(groupID, id, std::move(payload), eom);
}

void MoQSession::TrackHandle::onObjectHeader(ObjectHeader objHeader) {
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
             << " eom=" << uint64_t(eom) << " sess=" << this;
  auto objIt = objects_.find(std::make_pair(groupId, id));
  if (objIt == objects_.end()) {
    // error;
    XLOG(ERR) << "unknown object gid=" << groupId << " seq=" << id
              << " sess=" << this;
    return;
  }
  if (payload) {
    XLOG(DBG1) << "payload enqueued" << " sess=" << this;
    objIt->second->payloadQueue.enqueue(std::move(payload));
  }
  if (eom) {
    XLOG(DBG1) << "eom enqueued" << " sess=" << this;
    objIt->second->payloadQueue.enqueue(nullptr);
  }
}

void MoQSession::onSubscribe(SubscribeRequest subscribeRequest) {
  XLOG(DBG1) << __func__ << " sess=" << this;
  const auto subscribeID = subscribeRequest.subscribeID;
  closeSessionIfSubscribeIdInvalid(subscribeID);

  // TODO: The publisher should maintain some state like
  //   Subscribe ID -> Track Name, Locations [currently held in
  //   MoQForwarder] Track Alias -> Track Name
  // If ths session holds this state, it can check for duplicate
  // subscriptions
  pubTracks_[subscribeID].priority = subscribeRequest.priority;
  controlMessages_.enqueue(std::move(subscribeRequest));
}

void MoQSession::onSubscribeUpdate(SubscribeUpdate subscribeUpdate) {
  XLOG(DBG1) << __func__ << " sess=" << this;
  const auto subscribeID = subscribeUpdate.subscribeID;
  if (!pubTracks_.contains(subscribeID)) {
    XLOG(ERR) << "No matching subscribe ID=" << subscribeID << " sess=" << this;
    return;
  }
  closeSessionIfSubscribeIdInvalid(subscribeID);

  pubTracks_[subscribeID].priority = subscribeUpdate.priority;
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
  auto subIt = subTracks_.find(subOk.subscribeID);
  if (subIt == subTracks_.end()) {
    // unknown
    XLOG(ERR) << "No matching subscribe ID=" << subOk.subscribeID
              << " sess=" << this;
    return;
  }
  subIt->second->subscribeOK(subIt->second, subOk.groupOrder, subOk.latest);
}

void MoQSession::onSubscribeError(SubscribeError subErr) {
  XLOG(DBG1) << __func__ << " sess=" << this;
  auto subIt = subTracks_.find(subErr.subscribeID);
  if (subIt == subTracks_.end()) {
    // unknown
    XLOG(ERR) << "No matching subscribe ID=" << subErr.subscribeID
              << " sess=" << this;
    return;
  }
  subIt->second->subscribeError(std::move(subErr));
  subTracks_.erase(subIt);
}

void MoQSession::onSubscribeDone(SubscribeDone subscribeDone) {
  XLOG(DBG1) << __func__ << " sess=" << this;
  auto trackIt = subTracks_.find(subscribeDone.subscribeID);
  if (trackIt == subTracks_.end()) {
    // received an object for unknown sub id
    XLOG(ERR) << "unknown subscribeID=" << subscribeDone.subscribeID
              << " sess=" << this;
    return;
  }
  // TODO: handle final object and status code
  trackIt->second->fin();
  controlMessages_.enqueue(std::move(subscribeDone));
}

void MoQSession::onMaxSubscribeId(MaxSubscribeId maxSubscribeId) {
  XLOG(DBG1) << __func__ << " sess=" << this;
  if (maxSubscribeId.subscribeID > maxSubscribeID_) {
    XLOG(DBG1) << fmt::format(
        "Bumping the maxSubscribeId to: {} from: {}",
        maxSubscribeId.subscribeID,
        maxSubscribeID_);
    maxSubscribeID_ = maxSubscribeId.subscribeID;
    controlMessages_.enqueue(std::move(maxSubscribeId));
    return;
  }

  XLOG(ERR) << fmt::format(
      "Invalid MaxSubscribeId: {}. Current maxSubscribeId:{}",
      maxSubscribeId.subscribeID,
      maxSubscribeID_);
  close(SessionCloseErrorCode::PROTOCOL_VIOLATION);
}

void MoQSession::onFetch(Fetch fetch) {
  XLOG(ERR) << "Not implemented yet";
}

void MoQSession::onFetchCancel(FetchCancel fetchCancel) {
  XLOG(ERR) << "Not implemented yet";
}

void MoQSession::onFetchOk(FetchOk fetchOk) {
  XLOG(ERR) << "Not implemented yet";
}

void MoQSession::onFetchError(FetchError fetchError) {
  XLOG(ERR) << "Not implemented yet";
}

void MoQSession::onAnnounce(Announce ann) {
  XLOG(DBG1) << __func__ << " sess=" << this;
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
    XLOG(ERR) << "writeAnnounce failed" << " sess=" << this;
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
    XLOG(ERR) << "writeAnnounceOk failed" << " sess=" << this;
    return;
  }
  controlWriteEvent_.signal();
}

void MoQSession::announceError(AnnounceError announceError) {
  XLOG(DBG1) << __func__ << " sess=" << this;
  auto res = writeAnnounceError(controlWriteBuf_, std::move(announceError));
  if (!res) {
    XLOG(ERR) << "writeAnnounceError failed" << " sess=" << this;
    return;
  }
  controlWriteEvent_.signal();
}

void MoQSession::unannounce(Unannounce unann) {
  XLOG(DBG1) << __func__ << " sess=" << this;
  auto trackNamespace = unann.trackNamespace;
  auto res = writeUnannounce(controlWriteBuf_, std::move(unann));
  if (!res) {
    XLOG(ERR) << "writeUnannounce failed" << " sess=" << this;
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
    XLOG(ERR) << "writeSubscribeAnnounces failed" << " sess=" << this;
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
    XLOG(ERR) << "writeSubscribeAnnouncesOk failed" << " sess=" << this;
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
    XLOG(ERR) << "writeSubscribeAnnouncesError failed" << " sess=" << this;
    return;
  }
  controlWriteEvent_.signal();
}

folly::coro::AsyncGenerator<
    std::shared_ptr<MoQSession::TrackHandle::ObjectSource>>
MoQSession::TrackHandle::objects() {
  XLOG(DBG1) << __func__ << " sess=" << this;
  auto cancelToken = co_await folly::coro::co_current_cancellation_token;
  auto mergeToken = folly::CancellationToken::merge(cancelToken, cancelToken_);
  while (!mergeToken.isCancellationRequested()) {
    auto obj = co_await folly::coro::co_withCancellation(
        mergeToken, newObjects_.dequeue());
    if (!obj) {
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
  auto subID = nextSubscribeID_++;
  sub.subscribeID = subID;
  sub.trackAlias = sub.subscribeID;
  auto wres = writeSubscribeRequest(controlWriteBuf_, std::move(sub));
  if (!wres) {
    XLOG(ERR) << "writeSubscribeRequest failed" << " sess=" << this;
    co_return folly::makeUnexpected(
        SubscribeError({subID, 500, "local write failed", folly::none}));
  }
  controlWriteEvent_.signal();
  auto res = subTracks_.emplace(
      std::piecewise_construct,
      std::forward_as_tuple(subID),
      std::forward_as_tuple(std::make_shared<TrackHandle>(
          fullTrackName, subID, cancellationSource_.getToken())));
  XCHECK(res.second) << "Duplicate subscribe ID";

  co_return co_await res.first->second->ready();
}

void MoQSession::subscribeOk(SubscribeOk subOk) {
  XLOG(DBG1) << __func__ << " sess=" << this;
  pubTracks_[subOk.subscribeID].groupOrder = subOk.groupOrder;
  auto res = writeSubscribeOk(controlWriteBuf_, subOk);
  if (!res) {
    XLOG(ERR) << "writeSubscribeOk failed" << " sess=" << this;
    return;
  }
  controlWriteEvent_.signal();
}

void MoQSession::subscribeError(SubscribeError subErr) {
  XLOG(DBG1) << __func__ << " sess=" << this;
  pubTracks_.erase(subErr.subscribeID);
  auto res = writeSubscribeError(controlWriteBuf_, std::move(subErr));
  if (!res) {
    XLOG(ERR) << "writeSubscribeError failed" << " sess=" << this;
    return;
  }
  controlWriteEvent_.signal();
}

void MoQSession::unsubscribe(Unsubscribe unsubscribe) {
  XLOG(DBG1) << __func__ << " sess=" << this;
  auto res = writeUnsubscribe(controlWriteBuf_, std::move(unsubscribe));
  if (!res) {
    XLOG(ERR) << "writeUnsubscribe failed" << " sess=" << this;
    return;
  }
  controlWriteEvent_.signal();
}

void MoQSession::subscribeDone(SubscribeDone subDone) {
  XLOG(DBG1) << __func__ << " sess=" << this;
  pubTracks_.erase(subDone.subscribeID);
  auto res = writeSubscribeDone(controlWriteBuf_, std::move(subDone));
  if (!res) {
    XLOG(ERR) << "writeSubscribeDone failed" << " sess=" << this;
    return;
  }

  // If # of closed subscribes is greater than 1/2 of max subscribes, then
  // let's bump the maxSubscribeID by the number of closed subscribes.
  if (++closedSubscribes_ > kMaxConcurrentSubscribes_ / 2) {
    maxSubscribeID_ += closedSubscribes_;
    closedSubscribes_ = 0;
    res =
        writeMaxSubscribeId(controlWriteBuf_, {.subscribeID = maxSubscribeID_});
    if (!res) {
      XLOG(ERR) << "writeMaxSubscribeId failed" << " sess=" << this;
      return;
    }
  }
  controlWriteEvent_.signal();
}

namespace {
constexpr uint32_t IdMask = 0x1FFFFF;
uint64_t groupOrder(GroupOrder groupOrder, uint64_t group) {
  uint32_t truncGroup = static_cast<uint32_t>(group) & IdMask;
  return groupOrder == GroupOrder::OldestFirst ? truncGroup
                                               : (IdMask - truncGroup);
}

uint32_t objOrder(uint64_t objId) {
  return static_cast<uint32_t>(objId) & IdMask;
}
} // namespace

uint64_t MoQSession::order(const ObjectHeader& objHeader) {
  PubTrack pubTrack{
      std::numeric_limits<uint8_t>::max(), GroupOrder::OldestFirst};
  auto pubTrackIt = pubTracks_.find(objHeader.subscribeID);
  if (pubTrackIt != pubTracks_.end()) {
    pubTrack = pubTrackIt->second;
  }
  // 6 reserved bits | 58 bit order
  // 6 reserved | 8 sub pri | 8 pub pri | 21 group order | 21 obj order
  return (
      (uint64_t(pubTrack.priority) << 50) |
      (uint64_t(objHeader.priority) << 42) |
      (groupOrder(pubTrack.groupOrder, objHeader.group) << 21) |
      objOrder(objHeader.id));
}

folly::SemiFuture<folly::Unit> MoQSession::publish(
    const ObjectHeader& objHeader,
    uint64_t payloadOffset,
    std::unique_ptr<folly::IOBuf> payload,
    bool eom) {
  XCHECK_EQ(objHeader.status, ObjectStatus::NORMAL);
  return publishImpl(objHeader, payloadOffset, std::move(payload), eom, false);
}

folly::SemiFuture<folly::Unit> MoQSession::publishStreamPerObject(
    const ObjectHeader& objHeader,
    uint64_t payloadOffset,
    std::unique_ptr<folly::IOBuf> payload,
    bool eom) {
  XCHECK_EQ(objHeader.status, ObjectStatus::NORMAL);
  XCHECK_EQ(objHeader.forwardPreference, ForwardPreference::Subgroup);
  return publishImpl(objHeader, payloadOffset, std::move(payload), eom, true);
}

folly::SemiFuture<folly::Unit> MoQSession::publishStatus(
    const ObjectHeader& objHeader) {
  XCHECK_NE(objHeader.status, ObjectStatus::NORMAL);
  return publishImpl(objHeader, 0, nullptr, true, false);
}

folly::SemiFuture<folly::Unit> MoQSession::publishImpl(
    const ObjectHeader& objHeader,
    uint64_t payloadOffset,
    std::unique_ptr<folly::IOBuf> payload,
    bool eom,
    bool streamPerObject) {
  XLOG(DBG1) << __func__ << " " << objHeader << " sess=" << this;
  // TODO: Should there be verification that subscribeID / trackAlias are
  // valid, current subscriptions, or make the peer do that?
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  bool sendAsDatagram =
      objHeader.forwardPreference == ForwardPreference::Datagram;

  PublishKey publishKey(
      {objHeader.subscribeID,
       objHeader.group,
       objHeader.subgroup,
       objHeader.forwardPreference,
       objHeader.id});
  auto pubDataIt = publishDataMap_.find(publishKey);
  if (pubDataIt == publishDataMap_.end()) {
    XLOG(DBG4) << "New publish key, existing map size="
               << publishDataMap_.size() << " sess=" << this;
    // New publishing key

    // payloadOffset can be > 0 here if wt_->createUniStream() FAILS, that can
    // happen if the subscriber closes session abruptly, then:
    // - We do not add this publishKey to publishDataMap_
    // - Next portion of the object calls this function again with payloadOffset
    // > 0
    if (payloadOffset != 0) {
      XLOG(WARN)
          << __func__
          << " Can't start publishing in the middle. Disgregard data for this new obj with payloadOffset = "
          << payloadOffset << " sess=" << this;
      return folly::makeSemiFuture<folly::Unit>(folly::exception_wrapper(
          std::runtime_error("Can't start publishing in the middle.")));
    }

    // Create a new stream (except for datagram)
    proxygen::WebTransport::StreamWriteHandle* stream = nullptr;
    if (!sendAsDatagram) {
      auto res = wt_->createUniStream();
      if (!res) {
        // failed to create a stream
        XLOG(ERR) << "Failed to create uni stream" << " sess=" << this;
        return folly::makeSemiFuture<folly::Unit>(folly::exception_wrapper(
            std::runtime_error("Failed to create uni stream.")));
      }
      stream = *res;
      XLOG(DBG4) << "New stream created, id: " << stream->getID()
                 << " sess=" << this;
      stream->setPriority(1, order(objHeader), false);
    }

    // Add publishing key
    auto res = publishDataMap_.emplace(
        std::piecewise_construct,
        std::forward_as_tuple(publishKey),
        std::forward_as_tuple(PublishData(
            {((stream) ? stream->getID()
                       : std::numeric_limits<uint64_t>::max()),
             objHeader.group,
             objHeader.subgroup,
             objHeader.id,
             objHeader.length,
             0,
             streamPerObject})));
    pubDataIt = res.first;
    // Serialize multi-object stream header
    if (objHeader.forwardPreference == ForwardPreference::Track ||
        objHeader.forwardPreference == ForwardPreference::Subgroup) {
      writeStreamHeader(writeBuf, objHeader);
    }
  } else {
    XLOG(DBG4) << "Found open pub data" << " sess=" << this;
  }
  // TODO: Missing offset checks
  uint64_t payloadLength = payload ? payload->computeChainDataLength() : 0;
  if (payloadOffset == 0) {
    // new object
    // validate group and object are moving in the right direction
    bool multiObject = false;
    if (objHeader.forwardPreference == ForwardPreference::Track) {
      if (objHeader.group < pubDataIt->second.group) {
        XLOG(ERR) << "Decreasing group in Track" << " sess=" << this;
        return folly::makeSemiFuture<folly::Unit>(folly::exception_wrapper(
            std::runtime_error("Decreasing group in Track.")));
      }
      if (objHeader.group == pubDataIt->second.group) {
        if (objHeader.id < pubDataIt->second.objectID ||
            (objHeader.id == pubDataIt->second.objectID &&
             pubDataIt->second.offset != 0)) {
          XLOG(ERR) << "obj id must increase within group" << " sess=" << this;
          return folly::makeSemiFuture<folly::Unit>(folly::exception_wrapper(
              std::runtime_error("obj id must increase within group.")));
        }
      }
      multiObject = true;
    } else if (objHeader.forwardPreference == ForwardPreference::Subgroup) {
      if (objHeader.status != ObjectStatus::END_OF_SUBGROUP &&
          ((objHeader.id < pubDataIt->second.objectID ||
            (objHeader.id == pubDataIt->second.objectID &&
             pubDataIt->second.offset != 0)))) {
        XLOG(ERR) << "obj id must increase within subgroup" << " sess=" << this;
        return folly::makeSemiFuture<folly::Unit>(folly::exception_wrapper(
            std::runtime_error("obj id must increase within subgroup.")));
      }
      multiObject = true;
    }
    pubDataIt->second.group = objHeader.group;
    pubDataIt->second.objectID = objHeader.id;
    auto objCopy = objHeader;
    if (multiObject && !objCopy.length) {
      if (eom) {
        objCopy.length = payloadLength;
      } else {
        XLOG(ERR) << "Multi object streams require length" << " sess=" << this;
      }
    }
    writeObject(writeBuf, objCopy, nullptr);
  }
  if (pubDataIt->second.objectLength &&
      *pubDataIt->second.objectLength < payloadLength) {
    XLOG(ERR) << "Object length exceeds header length" << " sess=" << this;
    return folly::makeSemiFuture<folly::Unit>(folly::exception_wrapper(
        std::runtime_error("Object length exceeds header length.")));
  }
  writeBuf.append(std::move(payload));
  if (sendAsDatagram) {
    wt_->sendDatagram(writeBuf.move());
    publishDataMap_.erase(pubDataIt);
    return folly::makeSemiFuture();
  } else {
    bool streamEOM =
        (objHeader.status == ObjectStatus::END_OF_GROUP ||
         objHeader.status == ObjectStatus::END_OF_SUBGROUP ||
         objHeader.status == ObjectStatus::END_OF_TRACK_AND_GROUP);
    // TODO: verify that pubDataIt->second.objectLength is empty or 0
    if (eom && pubDataIt->second.streamPerObject) {
      writeObject(
          writeBuf,
          ObjectHeader(
              {objHeader.subscribeID,
               objHeader.trackAlias,
               objHeader.group,
               objHeader.subgroup,
               objHeader.id,
               objHeader.priority,
               ForwardPreference::Subgroup,
               ObjectStatus::END_OF_SUBGROUP,
               0}),
          nullptr);
      streamEOM = true;
    }
    XLOG_IF(DBG1, streamEOM) << "End of stream" << " sess=" << this;
    auto writeRes = wt_->writeStreamData(
        pubDataIt->second.streamID, writeBuf.move(), streamEOM);
    if (!writeRes) {
      XLOG(ERR) << "Failed to write stream data." << " sess=" << this
                << " error=" << static_cast<int>(writeRes.error());
      return folly::makeSemiFuture<folly::Unit>(
          folly::exception_wrapper(WebTransportException(
              writeRes.error(), "Failed to write stream data.")));
    }
    if (streamEOM) {
      publishDataMap_.erase(pubDataIt);
    } else {
      if (eom) {
        pubDataIt->second.offset = 0;
        pubDataIt->second.objectLength.reset();
      } else {
        pubDataIt->second.offset += payloadLength;
        if (pubDataIt->second.objectLength) {
          *pubDataIt->second.objectLength -= payloadLength;
        }
      }
    }
    return std::move(writeRes.value());
  }
}

void MoQSession::onNewUniStream(proxygen::WebTransport::StreamReadHandle* rh) {
  XLOG(DBG1) << __func__ << " sess=" << this;
  if (!setupComplete_) {
    XLOG(ERR) << "Uni stream before setup complete" << " sess=" << this;
    close();
    return;
  }
  // maybe not STREAM_HEADER_SUBGROUP, but at least not control
  readLoop(StreamType::STREAM_HEADER_SUBGROUP, rh).scheduleOn(evb_).start();
}

void MoQSession::onNewBidiStream(proxygen::WebTransport::BidiStreamHandle bh) {
  XLOG(DBG1) << __func__ << " sess=" << this;
  // TODO: prevent second control stream?
  if (dir_ == MoQControlCodec::Direction::CLIENT) {
    XLOG(ERR) << "Received bidi stream on client, kill it" << " sess=" << this;
    bh.writeHandle->resetStream(/*error=*/0);
    bh.readHandle->stopSending(/*error=*/0);
  } else {
    bh.writeHandle->setPriority(0, 0, false);
    readLoop(StreamType::CONTROL, bh.readHandle).scheduleOn(evb_).start();
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
  MoQObjectStreamCodec codec(this);
  codec.onIngress(std::move(datagram), true);
}

void MoQSession::closeSessionIfSubscribeIdInvalid(uint64_t subscribeID) {
  if (maxSubscribeID_ <= subscribeID) {
    XLOG(ERR) << "Invalid subscribeID: " << subscribeID << " sess=" << this;
    close(SessionCloseErrorCode::TOO_MANY_SUBSCRIBES);
  }
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

#include "moxygen/MoQSession.h"
#include <folly/experimental/coro/Collect.h>
#include <folly/io/async/EventBase.h>

#include <folly/logging/xlog.h>

namespace {
constexpr std::chrono::seconds kSetupTimeout(5);
}

namespace moxygen {

MoQSession::~MoQSession() {
  cancellationSource_.requestCancellation();
  for (auto& subTrack : subTracks_) {
    subTrack.second->subscribeError(
        {subTrack.first, 500, "session closed", folly::none});
  }
  for (auto& pendingAnn : pendingAnnounce_) {
    pendingAnn.second.setValue(folly::makeUnexpected(
        AnnounceError({pendingAnn.first, 500, "session closed"})));
  }
}

void MoQSession::start() {
  XLOG(DBG1) << __func__;
  if (dir_ == MoQCodec::Direction::CLIENT) {
    auto cs = wt_->createBidiStream();
    if (!cs) {
      XLOG(ERR) << "Failed to get control stream";
      wt_->closeSession();
      return;
    }
    auto controlStream = cs.value();

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

void MoQSession::close() {
  if (wt_) {
    wt_->closeSession();
    wt_ = nullptr;
  }
}

folly::coro::Task<void> MoQSession::controlWriteLoop(
    proxygen::WebTransport::StreamWriteHandle* controlStream) {
  XLOG(DBG1) << __func__;
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
  XCHECK(dir_ == MoQCodec::Direction::CLIENT);
  XLOG(DBG1) << __func__;
  auto res = writeClientSetup(controlWriteBuf_, std::move(setup));
  if (!res) {
    XLOG(ERR) << "writeClientSetup failed";
    return;
  }
  sentSetup_.signal();
  controlWriteEvent_.signal();
}

void MoQSession::setup(ServerSetup setup) {
  XLOG(DBG1) << __func__;
  XCHECK(dir_ == MoQCodec::Direction::SERVER);
  auto res = writeServerSetup(controlWriteBuf_, std::move(setup));
  if (!res) {
    XLOG(ERR) << "writeServerSetup failed";
    return;
  }
  sentSetup_.signal();
  controlWriteEvent_.signal();
}

folly::coro::Task<void> MoQSession::setupComplete() {
  auto token = co_await folly::coro::co_current_cancellation_token;
  auto result = co_await co_awaitTry(folly::coro::co_withCancellation(
      cancellationSource_.getToken(),
      folly::coro::timeout(
          folly::coro::collectAllTry(sentSetup_.wait(), receivedSetup_.wait()),
          kSetupTimeout)));
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
  XLOG(DBG1) << __func__;
  while (true) {
    auto message =
        co_await folly::coro::co_awaitTry(folly::coro::co_withCancellation(
            cancellationSource_.getToken(), controlMessages_.dequeue()));
    if (message.hasException()) {
      XLOG(ERR) << message.exception().what();
      break;
    }
    co_yield *message;
  }
}

folly::coro::Task<void> MoQSession::readLoop(
    StreamType /*streamType*/,
    proxygen::WebTransport::StreamReadHandle* readHandle) {
  XLOG(DBG1) << __func__;
  MoQCodec codec(dir_, this);
  // TODO: disallow OBJECT on control streams and non-object on non-control
  bool fin = false;
  while (!fin) {
    auto streamData = co_await folly::coro::co_awaitTry(
        readHandle->readStreamData().via(evb_));
    if (streamData.hasException()) {
      XLOG(ERR) << streamData.exception().what();
      co_return;
    } else {
      if (streamData->data || streamData->fin) {
        codec.onIngress(std::move(streamData->data), streamData->fin);
      }
      fin = streamData->fin;
    }
  }
}

void MoQSession::onClientSetup(ClientSetup clientSetup) {
  XLOG(DBG1) << __func__;
  if (std::find(
          clientSetup.supportedVersions.begin(),
          clientSetup.supportedVersions.end(),
          kVersionDraftCurrent) == clientSetup.supportedVersions.end()) {
    XLOG(ERR) << "No matching versions";
    for (auto v : clientSetup.supportedVersions) {
      XLOG(ERR) << "client sent=" << v;
    }
    close();
    receivedSetup_.cancel();
    return;
  }
  receivedSetup_.signal();
  controlMessages_.enqueue(std::move(clientSetup));
}

void MoQSession::onServerSetup(ServerSetup serverSetup) {
  XLOG(DBG1) << __func__;
  if (serverSetup.selectedVersion != kVersionDraftCurrent) {
    XLOG(ERR) << "Invalid version = " << serverSetup.selectedVersion;
    close();
    receivedSetup_.cancel();
    return;
  }
  receivedSetup_.signal();
  controlMessages_.enqueue(std::move(serverSetup));
}

void MoQSession::onObjectHeader(ObjectHeader objHeader) {
  XLOG(DBG1) << __func__;
  auto trackIt = subTracks_.find(objHeader.subscribeID);
  if (trackIt == subTracks_.end()) {
    // received an object for unknown sub id
    XLOG(ERR) << "unknown sub id=" << objHeader.subscribeID;
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
  XLOG(DBG1) << __func__;
  auto trackIt = subTracks_.find(subscribeID);
  if (trackIt == subTracks_.end()) {
    // received an object for unknown sub id
    XLOG(ERR) << "unknown subscribeID=" << subscribeID;
    return;
  }
  trackIt->second->onObjectPayload(groupID, id, std::move(payload), eom);
}

void MoQSession::TrackHandle::onObjectHeader(ObjectHeader objHeader) {
  XLOG(DBG1) << __func__;
  auto res = objects_.emplace(
      std::piecewise_construct,
      std::forward_as_tuple(std::make_pair(objHeader.group, objHeader.id)),
      std::forward_as_tuple(std::make_shared<ObjectSource>()));
  res.first->second->header = std::move(objHeader);
  res.first->second->fullTrackName = fullTrackName_;
  res.first->second->cancelToken = cancelToken_;
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
             << " eom=" << uint64_t(eom);
  auto objIt = objects_.find(std::make_pair(groupId, id));
  if (objIt == objects_.end()) {
    // error;
    XLOG(ERR) << "unknown object gid=" << groupId << " seq=" << id;
    return;
  }
  if (payload) {
    XLOG(DBG1) << "payload enqueued";
    objIt->second->payloadQueue.enqueue(std::move(payload));
  }
  if (eom) {
    XLOG(DBG1) << "eom enqueued";
    objIt->second->payloadQueue.enqueue(nullptr);
  }
}

void MoQSession::onSubscribe(SubscribeRequest subscribeRequest) {
  XLOG(DBG1) << __func__;
  // TODO: The publisher should maintain some state like
  //   Subscribe ID -> Track Name, Locations [currently held in MoQForwarder]
  //   Track Alias -> Track Name
  // If ths session holds this state, it can check for duplicate subscriptions
  controlMessages_.enqueue(std::move(subscribeRequest));
}

void MoQSession::onUnsubscribe(Unsubscribe unsubscribe) {
  XLOG(DBG1) << __func__;
  // How does this impact pending subscribes?
  // and open TrackHandles
  controlMessages_.enqueue(std::move(unsubscribe));
}

void MoQSession::onSubscribeOk(SubscribeOk subOk) {
  XLOG(DBG1) << __func__;
  auto subIt = subTracks_.find(subOk.subscribeID);
  if (subIt == subTracks_.end()) {
    // unknown
    XLOG(ERR) << "No matching subscribe ID=" << subOk.subscribeID;
    return;
  }
  subIt->second->subscribeOK(subIt->second);
}

void MoQSession::onSubscribeError(SubscribeError subErr) {
  XLOG(DBG1) << __func__;
  auto subIt = subTracks_.find(subErr.subscribeID);
  if (subIt == subTracks_.end()) {
    // unknown
    XLOG(ERR) << "No matching subscribe ID=" << subErr.subscribeID;
    return;
  }
  subIt->second->subscribeError(std::move(subErr));
  subTracks_.erase(subIt);
}

void MoQSession::onSubscribeFin(SubscribeFin subscribeFin) {
  XLOG(DBG1) << __func__;
  auto trackIt = subTracks_.find(subscribeFin.subscribeID);
  if (trackIt == subTracks_.end()) {
    // received an object for unknown sub id
    XLOG(ERR) << "unknown subscribeID=" << subscribeFin.subscribeID;
    return;
  }
  trackIt->second->fin();
  controlMessages_.enqueue(std::move(subscribeFin));
}

void MoQSession::onSubscribeRst(SubscribeRst subscribeRst) {
  XLOG(DBG1) << __func__;
  // TODO: handle subTracks
  controlMessages_.enqueue(std::move(subscribeRst));
}

void MoQSession::onAnnounce(Announce ann) {
  XLOG(DBG1) << __func__;
  controlMessages_.enqueue(std::move(ann));
}

void MoQSession::onAnnounceOk(AnnounceOk annOk) {
  XLOG(DBG1) << __func__;
  auto annIt = pendingAnnounce_.find(annOk.trackNamespace);
  if (annIt == pendingAnnounce_.end()) {
    // unknown
    XLOG(ERR) << "No matching announce trackNamespace=" << annOk.trackNamespace;
    return;
  }
  annIt->second.setValue(std::move(annOk));
  pendingAnnounce_.erase(annIt);
}

void MoQSession::onAnnounceError(AnnounceError annErr) {
  XLOG(DBG1) << __func__;
  auto annIt = pendingAnnounce_.find(annErr.trackNamespace);
  if (annIt == pendingAnnounce_.end()) {
    // unknown
    XLOG(ERR) << "No matching announce trackNamespace="
              << annErr.trackNamespace;
    return;
  }
  annIt->second.setValue(folly::makeUnexpected(std::move(annErr)));
  pendingAnnounce_.erase(annIt);
}

void MoQSession::onUnannounce(Unannounce unAnn) {
  XLOG(DBG1) << __func__;
  controlMessages_.enqueue(std::move(unAnn));
}

void MoQSession::onGoaway(Goaway goaway) {
  XLOG(DBG1) << __func__;
  controlMessages_.enqueue(std::move(goaway));
}

void MoQSession::onConnectionError(ErrorCode error) {
  XLOG(DBG1) << __func__;
  XLOG(ERR) << "err=" << folly::to_underlying(error);
  // TODO
}

folly::coro::Task<folly::Expected<AnnounceOk, AnnounceError>>
MoQSession::announce(Announce ann) {
  XLOG(DBG1) << __func__;
  auto trackNamespace = ann.trackNamespace;
  auto res = writeAnnounce(controlWriteBuf_, std::move(ann));
  if (!res) {
    XLOG(ERR) << "writeAnnounce failed";
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
  XLOG(DBG1) << __func__;
  auto res = writeAnnounceOk(controlWriteBuf_, std::move(annOk));
  if (!res) {
    XLOG(ERR) << "writeAnnounceOk failed";
    return;
  }
  controlWriteEvent_.signal();
}

void MoQSession::announceError(AnnounceError annErr) {
  XLOG(DBG1) << __func__;
  auto res = writeAnnounceError(controlWriteBuf_, std::move(annErr));
  if (!res) {
    XLOG(ERR) << "writeAnnounceError failed";
    return;
  }
  controlWriteEvent_.signal();
}

folly::coro::AsyncGenerator<
    std::shared_ptr<MoQSession::TrackHandle::ObjectSource>>
MoQSession::TrackHandle::objects() {
  XLOG(DBG1) << __func__;
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
  XLOG(DBG1) << __func__;
  auto fullTrackName = sub.fullTrackName;
  auto subID = nextSubscribeID_++;
  sub.subscribeID = subID;
  sub.trackAlias = sub.subscribeID;
  auto wres = writeSubscribeRequest(controlWriteBuf_, std::move(sub));
  if (!wres) {
    XLOG(ERR) << "writeSubscribeRequest failed";
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
  XLOG(DBG1) << __func__;
  auto res = writeSubscribeOk(controlWriteBuf_, subOk);
  if (!res) {
    XLOG(ERR) << "writeSubscribeOk failed";
    return;
  }
  controlWriteEvent_.signal();
}

void MoQSession::subscribeError(SubscribeError subErr) {
  XLOG(DBG1) << __func__;
  auto res = writeSubscribeError(controlWriteBuf_, std::move(subErr));
  if (!res) {
    XLOG(ERR) << "writeSubscribeError failed";
    return;
  }
  controlWriteEvent_.signal();
}

void MoQSession::unsubscribe(Unsubscribe unsubscribe) {
  XLOG(DBG1) << __func__;
  auto res = writeUnsubscribe(controlWriteBuf_, std::move(unsubscribe));
  if (!res) {
    XLOG(ERR) << "writeUnsubscribe failed";
    return;
  }
  controlWriteEvent_.signal();
}

void MoQSession::subscribeFin(SubscribeFin subFin) {
  XLOG(DBG1) << __func__;
  auto res = writeSubscribeFin(controlWriteBuf_, std::move(subFin));
  if (!res) {
    XLOG(ERR) << "writeSubscribeFin failed";
    return;
  }
  controlWriteEvent_.signal();
}

void MoQSession::subscribeRst(SubscribeRst subRst) {
  XLOG(DBG1) << __func__;
  auto res = writeSubscribeRst(controlWriteBuf_, std::move(subRst));
  if (!res) {
    XLOG(ERR) << "writeSubscribeRst failed";
    return;
  }
  controlWriteEvent_.signal();
}

void MoQSession::publish(
    const ObjectHeader& objHeader,
    uint64_t payloadOffset,
    std::unique_ptr<folly::IOBuf> payload,
    bool eom) {
  XLOG(DBG1) << __func__ << " sid=" << objHeader.subscribeID
             << " t=" << objHeader.trackAlias << " g=" << objHeader.group
             << " o=" << objHeader.id;
  // TODO: Should there be verification that subscribeID / trackAlias are
  // valid, current subscriptions, or make the peer do that?
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  bool sendAsDatagram =
      objHeader.forwardPreference == ForwardPreference::Datagram &&
      writeBuf.chainLength() < 1200;

  PublishKey publishKey(
      {objHeader.subscribeID,
       objHeader.group,
       objHeader.sendOrder,
       objHeader.forwardPreference,
       objHeader.id});
  auto pubDataIt = publishDataMap_.find(publishKey);
  if (pubDataIt == publishDataMap_.end()) {
    XLOG(DBG4) << "New publish key, existing map size="
               << publishDataMap_.size();
    // New publishing key
    XCHECK_EQ(payloadOffset, 0) << "Can't start publishing in the middle";

    // Create a new stream (except for datagram)
    proxygen::WebTransport::StreamWriteHandle* stream = nullptr;
    if (!sendAsDatagram) {
      auto res = wt_->createUniStream();
      if (!res) {
        // failed to create a stream
        XLOG(ERR) << "Failed to create uni stream";
        return;
      }
      stream = *res;
    }

    // Add publishing key
    auto res = publishDataMap_.emplace(
        std::piecewise_construct,
        std::forward_as_tuple(publishKey),
        std::forward_as_tuple(PublishData(
            {((stream) ? stream->getID()
                       : std::numeric_limits<uint64_t>::max()),
             objHeader.group,
             objHeader.id,
             objHeader.length,
             0})));
    pubDataIt = res.first;
    // Serialize multi-object stream header
    if (objHeader.forwardPreference == ForwardPreference::Track ||
        objHeader.forwardPreference == ForwardPreference::Group) {
      writeStreamHeader(writeBuf, objHeader);
    }
  } else {
    XLOG(DBG4) << "Found open pub data";
  }
  // TODO: Missing offset checks
  uint64_t payloadLength = payload ? payload->computeChainDataLength() : 0;
  if (payloadOffset == 0) {
    // new object
    // validate group and object are moving in the right direction
    bool multiObject = false;
    if (objHeader.forwardPreference == ForwardPreference::Track) {
      if (objHeader.group < pubDataIt->second.group) {
        XLOG(ERR) << "Decreasing group in Track";
        return;
      }
      if (objHeader.group == pubDataIt->second.group) {
        if (objHeader.id < pubDataIt->second.objectID ||
            (objHeader.id == pubDataIt->second.objectID &&
             pubDataIt->second.offset != 0)) {
          XLOG(ERR) << "obj id must increase within group";
          return;
        }
      }
      multiObject = true;
    } else if (objHeader.forwardPreference == ForwardPreference::Group) {
      if (objHeader.id < pubDataIt->second.objectID ||
          (objHeader.id == pubDataIt->second.objectID &&
           pubDataIt->second.offset != 0)) {
        XLOG(ERR) << "obj id must increase within group";
        return;
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
        XLOG(ERR) << "Multi object streams require length";
      }
    }
    writeObject(writeBuf, objCopy, nullptr);
  }
  if (pubDataIt->second.objectLength &&
      *pubDataIt->second.objectLength < payloadLength) {
    XLOG(ERR) << "Object length exceeds header length";
    return;
  }
  writeBuf.append(std::move(payload));
  if (sendAsDatagram) {
    wt_->sendDatagram(writeBuf.move());
    publishDataMap_.erase(pubDataIt);
  } else {
    bool streamEOM =
        eom && objHeader.forwardPreference == ForwardPreference::Object;
    wt_->writeStreamData(
        pubDataIt->second.streamID, writeBuf.move(), streamEOM);
    if (streamEOM) {
      publishDataMap_.erase(pubDataIt);
    } else {
      if (eom) {
        pubDataIt->second.offset = 0;
        pubDataIt->second.objectLength.reset();
        // TODO: we never close multi-object streams
      } else {
        pubDataIt->second.offset += payloadLength;
        if (pubDataIt->second.objectLength) {
          *pubDataIt->second.objectLength -= payloadLength;
        }
      }
    }
  }
}

void MoQSession::onNewUniStream(proxygen::WebTransport::StreamReadHandle* rh) {
  XLOG(DBG1) << __func__;
  if (!setupComplete_) {
    XLOG(ERR) << "Uni stream before setup complete";
    close();
    return;
  }
  readLoop(StreamType::DATA, rh).scheduleOn(evb_).start();
}

void MoQSession::onNewBidiStream(proxygen::WebTransport::BidiStreamHandle bh) {
  XLOG(DBG1) << __func__;
  // TODO: prevent second control stream?
  if (dir_ == MoQCodec::Direction::CLIENT) {
    XLOG(ERR) << "Received bidi stream on client, kill it";
    bh.writeHandle->resetStream(/*error=*/0);
    bh.readHandle->stopSending(/*error=*/0);
  } else {
    readLoop(StreamType::CONTROL, bh.readHandle).scheduleOn(evb_).start();
    auto mergeToken = folly::CancellationToken::merge(
        cancellationSource_.getToken(), bh.writeHandle->getCancelToken());
    folly::coro::co_withCancellation(
        std::move(mergeToken), controlWriteLoop(bh.writeHandle))
        .scheduleOn(evb_)
        .start();
  }
}
} // namespace moxygen

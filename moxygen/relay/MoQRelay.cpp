/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/relay/MoQRelay.h"

namespace moxygen {

void MoQRelay::onAnnounce(Announce&& ann, std::shared_ptr<MoQSession> session) {
  // check auth
  if (ann.trackNamespace.starts_with(allowedNamespacePrefix_)) {
    session->announceOk({ann.trackNamespace});
    announces_.emplace(std::move(ann.trackNamespace), std::move(session));
  } else {
    session->announceError({ann.trackNamespace, 403, "bad namespace"});
  }
}

folly::coro::Task<void> MoQRelay::onSubscribe(
    SubscribeRequest subReq,
    std::shared_ptr<MoQSession> session) {
  auto subscriptionIt = subscriptions_.find(subReq.fullTrackName);
  std::shared_ptr<MoQForwarder> forwarder;
  if (subscriptionIt == subscriptions_.end()) {
    // first subscriber

    // check auth
    // get trackNamespace
    if (subReq.fullTrackName.trackNamespace.empty()) {
      session->subscribeError({subReq.subscribeID, 400, "namespace required"});
      co_return;
    }
    auto upstreamSessionIt =
        announces_.find(subReq.fullTrackName.trackNamespace);
    if (upstreamSessionIt == announces_.end()) {
      // no such namespace has been announced
      session->subscribeError({subReq.subscribeID, 404, "no such namespace"});
      co_return;
    }
    if (session.get() == upstreamSessionIt->second.get()) {
      session->subscribeError({subReq.subscribeID, 400, "self subscribe"});
      co_return;
    }
    // TODO: we only subscribe with the downstream locations.
    auto subRes = co_await upstreamSessionIt->second->subscribe(subReq);
    if (subRes.hasError()) {
      session->subscribeError({subReq.subscribeID, 502, "subscribe failed"});
      co_return;
    }
    forwarder = std::make_shared<MoQForwarder>(
        subReq.fullTrackName, subRes.value()->latest());
    RelaySubscription rsub(
        {forwarder,
         upstreamSessionIt->second,
         (*subRes)->subscribeID(),
         folly::CancellationSource()});
    auto token = rsub.cancellationSource.getToken();
    subscriptions_[subReq.fullTrackName] = std::move(rsub);
    folly::coro::co_withCancellation(
        token, forwardTrack(subRes.value(), forwarder))
        .scheduleOn(upstreamSessionIt->second->getEventBase())
        .start();
  } else {
    forwarder = subscriptionIt->second.forwarder;
  }
  // Add to subscribers list
  forwarder->addSubscriber(
      session, subReq.subscribeID, subReq.trackAlias, subReq);
  session->subscribeOk(
      {subReq.subscribeID, std::chrono::milliseconds(0), forwarder->latest()});
}

folly::coro::Task<void> MoQRelay::forwardTrack(
    std::shared_ptr<MoQSession::TrackHandle> track,
    std::shared_ptr<MoQForwarder> fowarder) {
  while (auto obj = co_await track->objects().next()) {
    XLOG(DBG1) << __func__
               << " new object t=" << obj.value()->fullTrackName.trackNamespace
               << obj.value()->fullTrackName.trackName
               << " g=" << obj.value()->header.group
               << " o=" << obj.value()->header.id;
    folly::IOBufQueue payloadBuf{folly::IOBufQueue::cacheChainLength()};
    uint64_t payloadOffset = 0;
    bool eom = false;
    while (!eom) {
      auto payload = co_await obj.value()->payloadQueue.dequeue();
      if (payload) {
        payloadBuf.append(std::move(payload));
        XLOG(DBG1) << __func__
                   << " object bytes, buflen now=" << payloadBuf.chainLength();
      } else {
        XLOG(DBG1) << __func__
                   << " object eom, buflen now=" << payloadBuf.chainLength();
        eom = true;
      }
      auto payloadLength = payloadBuf.chainLength();
      if (eom || payloadOffset + payloadLength > 1280) {
        fowarder->publish(
            obj.value()->header, payloadBuf.move(), payloadOffset, eom);
        payloadOffset += payloadLength;
      } else {
        XLOG(DBG1) << __func__
                   << " Not publishing yet payloadOffset=" << payloadOffset
                   << " payloadLength=" << payloadLength
                   << " eom=" << uint64_t(eom);
      }
    }
  }
}

void MoQRelay::onUnsubscribe(
    Unsubscribe unsub,
    std::shared_ptr<MoQSession> session) {
  for (auto subscriptionIt = subscriptions_.begin();
       subscriptionIt != subscriptions_.end();) {
    if (subscriptionIt->second.upstream.get() == session.get()) {
      subscriptionIt->second.forwarder->removeSession(
          session, unsub.subscribeID);
      subscriptionIt->second.cancellationSource.requestCancellation();
      if (subscriptionIt->second.forwarder->empty()) {
        XLOG(INFO) << "Removed last subscriber for "
                   << subscriptionIt->first.trackNamespace
                   << subscriptionIt->first.trackName;
        subscriptionIt->second.upstream->unsubscribe(
            {subscriptionIt->second.subscribeID});
        subscriptionIt = subscriptions_.erase(subscriptionIt);
      } else {
        subscriptionIt++;
      }
    }
  }
}

void MoQRelay::removeSession(const std::shared_ptr<MoQSession>& session) {
  for (auto it = announces_.begin(); it != announces_.end();) {
    if (it->second.get() == session.get()) {
      it = announces_.erase(it);
    } else {
      it++;
    }
  }
  for (auto subscriptionIt = subscriptions_.begin();
       subscriptionIt != subscriptions_.end();) {
    if (subscriptionIt->second.upstream.get() == session.get()) {
      subscriptionIt->second.forwarder->error(
          SubscribeDoneStatusCode::SUBSCRIPTION_ENDED, "upstream disconnect");
      subscriptionIt->second.cancellationSource.requestCancellation();
    } else {
      subscriptionIt->second.forwarder->removeSession(session);
    }
    if (subscriptionIt->second.forwarder->empty()) {
      XLOG(INFO) << "Removed last subscriber for "
                 << subscriptionIt->first.trackNamespace
                 << subscriptionIt->first.trackName;
      subscriptionIt->second.upstream->unsubscribe(
          {subscriptionIt->second.subscribeID});
      subscriptionIt = subscriptions_.erase(subscriptionIt);
    } else {
      subscriptionIt++;
    }
  }
}

} // namespace moxygen

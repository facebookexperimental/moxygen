/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/relay/MoQRelay.h"

namespace moxygen {

void MoQRelay::onAnnounce(Announce&& ann, std::shared_ptr<MoQSession> session) {
  // check auth
  if (!ann.trackNamespace.startsWith(allowedNamespacePrefix_)) {
    session->announceError({ann.trackNamespace, 403, "bad namespace"});
    return;
  }
  session->announceOk({ann.trackNamespace});
  announces_.emplace(std::move(ann.trackNamespace), std::move(session));
}

folly::coro::Task<void> MoQRelay::onSubscribe(
    SubscribeRequest subReq,
    std::shared_ptr<MoQSession> session) {
  auto subscriptionIt = subscriptions_.find(subReq.fullTrackName);
  std::shared_ptr<MoQForwarder> forwarder;
  auto subGroupOrder = subReq.groupOrder;
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
    subReq.priority = 1;
    subReq.groupOrder = GroupOrder::Default;
    auto subRes = co_await upstreamSessionIt->second->subscribe(subReq);
    if (subRes.hasError()) {
      session->subscribeError({subReq.subscribeID, 502, "subscribe failed"});
      co_return;
    }
    forwarder = std::make_shared<MoQForwarder>(
        subReq.fullTrackName, subRes.value()->latest());
    forwarder->setGroupOrder(subRes.value()->groupOrder());
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
      {subReq.subscribeID,
       std::chrono::milliseconds(0),
       MoQSession::resolveGroupOrder(forwarder->groupOrder(), subGroupOrder),
       forwarder->latest()});
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
  // TODO: session+subscribe ID should uniquely identify this subscription,
  // we shouldn't need a linear search to find where to remove it.
  for (auto subscriptionIt = subscriptions_.begin();
       subscriptionIt != subscriptions_.end();) {
    auto& subscription = subscriptionIt->second;
    subscription.forwarder->removeSession(session, unsub.subscribeID);
    if (subscription.forwarder->empty()) {
      XLOG(INFO) << "Removed last subscriber for "
                 << subscriptionIt->first.trackNamespace
                 << subscriptionIt->first.trackName;
      subscription.cancellationSource.requestCancellation();
      subscription.upstream->unsubscribe({subscription.subscribeID});
      subscriptionIt = subscriptions_.erase(subscriptionIt);
    } else {
      subscriptionIt++;
    }
  }
}

void MoQRelay::removeSession(const std::shared_ptr<MoQSession>& session) {
  // TODO: remove linear search
  for (auto it = announces_.begin(); it != announces_.end();) {
    if (it->second.get() == session.get()) {
      it = announces_.erase(it);
    } else {
      it++;
    }
  }
  // TODO: we should keep a map from this session to all its subscriptions
  // and remove this linear search also
  for (auto subscriptionIt = subscriptions_.begin();
       subscriptionIt != subscriptions_.end();) {
    auto& subscription = subscriptionIt->second;
    if (subscription.upstream.get() == session.get()) {
      subscription.forwarder->error(
          SubscribeDoneStatusCode::SUBSCRIPTION_ENDED, "upstream disconnect");
      subscription.cancellationSource.requestCancellation();
    } else {
      subscription.forwarder->removeSession(session);
    }
    if (subscription.forwarder->empty()) {
      XLOG(INFO) << "Removed last subscriber for "
                 << subscriptionIt->first.trackNamespace
                 << subscriptionIt->first.trackName;
      subscription.upstream->unsubscribe({subscription.subscribeID});
      subscriptionIt = subscriptions_.erase(subscriptionIt);
    } else {
      subscriptionIt++;
    }
  }
}

} // namespace moxygen

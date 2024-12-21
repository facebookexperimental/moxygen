/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/relay/MoQRelay.h"

namespace moxygen {

MoQRelay::AnnounceNode* MoQRelay::findNamespaceNode(
    const TrackNamespace& ns,
    bool createMissingNodes,
    std::vector<std::shared_ptr<MoQSession>>* sessions) {
  AnnounceNode* nodePtr = &announceRoot_;
  for (auto i = 0ul; i < ns.size(); i++) {
    if (sessions) {
      sessions->insert(
          sessions->end(), nodePtr->sessions.begin(), nodePtr->sessions.end());
    }
    auto& name = ns[i];
    auto it = nodePtr->children.find(name);
    if (it == nodePtr->children.end()) {
      if (createMissingNodes) {
        nodePtr =
            &nodePtr->children.emplace(name, AnnounceNode()).first->second;
      } else {
        XLOG(ERR) << "prefix not found in announce tree";
        return nullptr;
      }
    } else {
      nodePtr = &it->second;
    }
  }
  return nodePtr;
}

void MoQRelay::onAnnounce(Announce&& ann, std::shared_ptr<MoQSession> session) {
  XLOG(DBG1) << __func__ << " ns=" << ann.trackNamespace;
  // check auth
  if (!ann.trackNamespace.startsWith(allowedNamespacePrefix_)) {
    session->announceError({ann.trackNamespace, 403, "bad namespace"});
    return;
  }
  std::vector<std::shared_ptr<MoQSession>> sessions;
  auto nodePtr = findNamespaceNode(
      ann.trackNamespace, /*createMissingNodes=*/true, &sessions);

  // TODO: store auth for forwarding on future SubscribeAnnounces?
  nodePtr->sourceSession = std::move(session);
  nodePtr->sourceSession->announceOk({ann.trackNamespace});
  for (auto& outSession : sessions) {
    auto evb = outSession->getEventBase();
    // We don't really care if we get announce error, I guess?
    outSession->announce(ann).scheduleOn(evb).start();
  }
}

void MoQRelay::onUnannounce(
    Unannounce&& unann,
    const std::shared_ptr<MoQSession>& session) {
  XLOG(DBG1) << __func__ << " ns=" << unann.trackNamespace;
  std::vector<std::shared_ptr<MoQSession>> sessions;
  auto nodePtr = findNamespaceNode(
      unann.trackNamespace, /*createMissingNodes=*/false, &sessions);
  if (!nodePtr) {
    // TODO: maybe error?
    return;
  }
  if (nodePtr->sourceSession.get() == session.get()) {
    nodePtr->sourceSession = nullptr;
    for (auto& outSession : sessions) {
      auto evb = outSession->getEventBase();
      evb->runInEventBaseThread(
          [outSession, unann] { outSession->unannounce(unann); });
    }
  } else {
    if (nodePtr->sourceSession) {
      XLOG(ERR) << "unannounce namespace announced by another session";
    } else {
      XLOG(DBG1) << "unannounce namespace that was not announced";
    }
  }

  // TODO: prune Announce tree
}

void MoQRelay::onSubscribeAnnounces(
    SubscribeAnnounces&& subNs,
    std::shared_ptr<MoQSession> session) {
  XLOG(DBG1) << __func__ << " nsp=" << subNs.trackNamespacePrefix;
  // check auth
  if (subNs.trackNamespacePrefix.empty()) {
    session->subscribeAnnouncesError(
        {subNs.trackNamespacePrefix, 400, "empty"});
    return;
  }
  auto nodePtr = findNamespaceNode(
      subNs.trackNamespacePrefix, /*createMissingNodes=*/true);
  auto sessionPtr = session.get();
  nodePtr->sessions.emplace(std::move(session));
  sessionPtr->subscribeAnnouncesOk({subNs.trackNamespacePrefix});

  // Find all nested Announcements and forward
  std::deque<std::tuple<TrackNamespace, AnnounceNode*>> nodes{
      {subNs.trackNamespacePrefix, nodePtr}};
  auto evb = sessionPtr->getEventBase();
  while (!nodes.empty()) {
    auto [prefix, nodePtr] = std::move(*nodes.begin());
    nodes.pop_front();
    if (nodePtr->sourceSession) {
      // We don't really care if we get announce error, I guess?
      // TODO: Auth/params
      sessionPtr->announce({prefix, {}}).scheduleOn(evb).start();
    }
    for (auto& nextNodeIt : nodePtr->children) {
      TrackNamespace nodePrefix(prefix);
      nodePrefix.append(nextNodeIt.first);
      nodes.emplace_back(std::forward_as_tuple(nodePrefix, &nextNodeIt.second));
    }
  }
}

void MoQRelay::onUnsubscribeAnnounces(
    UnsubscribeAnnounces&& unsubNs,
    const std::shared_ptr<MoQSession>& session) {
  XLOG(DBG1) << __func__ << " nsp=" << unsubNs.trackNamespacePrefix;
  auto nodePtr = findNamespaceNode(
      unsubNs.trackNamespacePrefix, /*createMissingNodes=*/false);
  if (!nodePtr) {
    // TODO: maybe error?
    return;
  }
  auto it = nodePtr->sessions.find(session);
  if (it != nodePtr->sessions.end()) {
    nodePtr->sessions.erase(it);
    return;
  }
  // TODO: error?
  XLOG(DBG1) << "Namespace prefix was not subscribed by this session";

  // TODO: prune Announce tree
}

std::shared_ptr<MoQSession> MoQRelay::findAnnounceSession(
    const TrackNamespace& ns) {
  auto nodePtr = findNamespaceNode(ns, /*createMissingNodes=*/false);
  if (!nodePtr) {
    return nullptr;
  }
  return nodePtr->sourceSession;
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
    auto upstreamSession =
        findAnnounceSession(subReq.fullTrackName.trackNamespace);
    if (!upstreamSession) {
      // no such namespace has been announced
      session->subscribeError({subReq.subscribeID, 404, "no such namespace"});
      co_return;
    }
    if (session.get() == upstreamSession.get()) {
      session->subscribeError({subReq.subscribeID, 400, "self subscribe"});
      co_return;
    }
    // TODO: we only subscribe with the downstream locations.
    subReq.priority = 1;
    subReq.groupOrder = GroupOrder::Default;
    auto subRes = co_await upstreamSession->subscribe(subReq);
    if (subRes.hasError()) {
      session->subscribeError({subReq.subscribeID, 502, "subscribe failed"});
      co_return;
    }
    forwarder = std::make_shared<MoQForwarder>(
        subReq.fullTrackName, subRes.value()->latest());
    forwarder->setGroupOrder(subRes.value()->groupOrder());
    RelaySubscription rsub(
        {forwarder,
         upstreamSession,
         (*subRes)->subscribeID(),
         folly::CancellationSource()});
    auto token = rsub.cancellationSource.getToken();
    subscriptions_[subReq.fullTrackName] = std::move(rsub);
    folly::coro::co_withCancellation(
        token, forwardTrack(subRes.value(), forwarder))
        .scheduleOn(upstreamSession->getEventBase())
        .start();
  } else {
    forwarder = subscriptionIt->second.forwarder;
  }
  // Add to subscribers list
  auto sessionPtr = session.get();
  auto trackPublisher = sessionPtr->subscribeOk(
      {subReq.subscribeID,
       std::chrono::milliseconds(0),
       MoQSession::resolveGroupOrder(forwarder->groupOrder(), subGroupOrder),
       forwarder->latest()});
  if (trackPublisher) {
    auto subscriber = std::make_shared<MoQForwarder::Subscriber>(
        std::move(session),
        subReq.subscribeID,
        subReq.trackAlias,
        toSubscribeRange(subReq, forwarder->latest()),
        std::move(trackPublisher),
        MoQForwarder::Subscriber::SubgroupConsumerMap());
    forwarder->addSubscriber(std::move(subscriber));
  } else {
    XLOG(ERR) << "Downstream subscribeOK failed";
    // TODO: unsubsubscribe upstream
  }
}

folly::coro::Task<void> MoQRelay::forwardTrack(
    std::shared_ptr<MoQSession::TrackHandle> track,
    std::shared_ptr<MoQForwarder> forwarder) {
  while (auto obj = co_await track->objects().next()) {
    XLOG(DBG1) << __func__ << " new object t=" << obj.value()->fullTrackName
               << " g=" << obj.value()->header.group
               << " o=" << obj.value()->header.id;
    folly::IOBufQueue payloadBuf{folly::IOBufQueue::cacheChainLength()};
    bool eom = false;
    // TODO: this is wrong - we're publishing each object in it's own subgroup
    // stream now
    auto res = forwarder->beginSubgroup(
        obj.value()->header.group,
        obj.value()->header.subgroup,
        obj.value()->header.priority);
    if (!res) {
      XLOG(ERR) << "Failed to begin forwarding subgroup";
      // TODO: error
    }
    auto subgroupPub = std::move(res.value());
    subgroupPub->beginObject(
        obj.value()->header.id, *obj.value()->header.length, nullptr);
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
      if (eom || payloadLength > 1280) {
        subgroupPub->objectPayload(payloadBuf.move(), eom);
      } else {
        XLOG(DBG1) << __func__
                   << " Not publishing yet payloadLength=" << payloadLength
                   << " eom=" << uint64_t(eom);
      }
    }
    subgroupPub.reset();
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
    subscription.forwarder->removeSession(
        session,
        {subscription.subscribeID,
         SubscribeDoneStatusCode::UNSUBSCRIBED,
         "",
         subscription.forwarder->latest()});
    if (subscription.forwarder->empty()) {
      XLOG(INFO) << "Removed last subscriber for " << subscriptionIt->first;
      subscription.cancellationSource.requestCancellation();
      subscription.upstream->unsubscribe({subscription.subscribeID});
      subscriptionIt = subscriptions_.erase(subscriptionIt);
    } else {
      subscriptionIt++;
    }
  }
}

void MoQRelay::removeSession(const std::shared_ptr<MoQSession>& session) {
  // TODO: remove linear search by having each session track it's active
  // announcements, subscribes and subscribe namespaces
  std::vector<std::shared_ptr<MoQSession>> notifySessions;
  std::deque<std::tuple<TrackNamespace, AnnounceNode*>> nodes{
      {TrackNamespace(), &announceRoot_}};
  TrackNamespace prefix;
  while (!nodes.empty()) {
    auto [prefix, nodePtr] = std::move(*nodes.begin());
    nodes.pop_front();
    // Implicit UnsubscribeAnnounces
    nodePtr->sessions.erase(session);

    // Add sessions for future Unannounce
    notifySessions.insert(
        notifySessions.end(),
        nodePtr->sessions.begin(),
        nodePtr->sessions.end());

    if (nodePtr->sourceSession == session) {
      // This session is unannouncing
      nodePtr->sourceSession = nullptr;
      for (auto& outSession : notifySessions) {
        auto evb = outSession->getEventBase();
        Unannounce unann{prefix};
        evb->runInEventBaseThread(
            [outSession, unann] { outSession->unannounce(unann); });
      }
    }
    for (auto& nextNode : nodePtr->children) {
      TrackNamespace nodePrefix(prefix);
      nodePrefix.append(nextNode.first);
      nodes.emplace_back(
          std::forward_as_tuple(std::move(nodePrefix), &nextNode.second));
    }
  }

  // TODO: we should keep a map from this session to all its subscriptions
  // and remove this linear search also
  for (auto subscriptionIt = subscriptions_.begin();
       subscriptionIt != subscriptions_.end();) {
    auto& subscription = subscriptionIt->second;
    if (subscription.upstream.get() == session.get()) {
      subscription.forwarder->subscribeDone(
          {SubscribeID(0),
           SubscribeDoneStatusCode::SUBSCRIPTION_ENDED,
           "upstream disconnect",
           subscription.forwarder->latest()});
      subscription.cancellationSource.requestCancellation();
    } else {
      subscription.forwarder->removeSession(session);
    }
    if (subscription.forwarder->empty()) {
      XLOG(INFO) << "Removed last subscriber for " << subscriptionIt->first;
      subscription.upstream->unsubscribe({subscription.subscribeID});
      subscriptionIt = subscriptions_.erase(subscriptionIt);
    } else {
      subscriptionIt++;
    }
  }
}

} // namespace moxygen

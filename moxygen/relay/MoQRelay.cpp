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

class MoQRelay::AnnouncesSubscription
    : public Publisher::SubscribeAnnouncesHandle {
 public:
  AnnouncesSubscription(
      std::shared_ptr<MoQRelay> relay,
      std::shared_ptr<MoQSession> session,
      SubscribeAnnouncesOk ok)
      : Publisher::SubscribeAnnouncesHandle(std::move(ok)),
        relay_(std::move(relay)),
        session_(std::move(session)) {}

  void unsubscribeAnnounces() override {
    if (relay_) {
      relay_->unsubscribeAnnounces(
          subscribeAnnouncesOk_->trackNamespacePrefix, std::move(session_));
      relay_.reset();
    }
  }

 private:
  std::shared_ptr<MoQRelay> relay_;
  std::shared_ptr<MoQSession> session_;
};

folly::coro::Task<Publisher::SubscribeAnnouncesResult>
MoQRelay::subscribeAnnounces(SubscribeAnnounces subNs) {
  XLOG(DBG1) << __func__ << " nsp=" << subNs.trackNamespacePrefix;
  // check auth
  if (subNs.trackNamespacePrefix.empty()) {
    co_return folly::makeUnexpected(
        SubscribeAnnouncesError{subNs.trackNamespacePrefix, 400, "empty"});
  }
  auto session = MoQSession::getRequestSession();
  auto nodePtr = findNamespaceNode(
      subNs.trackNamespacePrefix, /*createMissingNodes=*/true);
  nodePtr->sessions.emplace(session);

  // Find all nested Announcements and forward
  std::deque<std::tuple<TrackNamespace, AnnounceNode*>> nodes{
      {subNs.trackNamespacePrefix, nodePtr}};
  auto evb = session->getEventBase();
  while (!nodes.empty()) {
    auto [prefix, nodePtr] = std::move(*nodes.begin());
    nodes.pop_front();
    if (nodePtr->sourceSession) {
      // We don't really care if we get announce error, I guess?
      // TODO: Auth/params
      session->announce({prefix, {}}).scheduleOn(evb).start();
    }
    for (auto& nextNodeIt : nodePtr->children) {
      TrackNamespace nodePrefix(prefix);
      nodePrefix.append(nextNodeIt.first);
      nodes.emplace_back(std::forward_as_tuple(nodePrefix, &nextNodeIt.second));
    }
  }
  co_return std::make_shared<AnnouncesSubscription>(
      shared_from_this(),
      std::move(session),
      SubscribeAnnouncesOk{subNs.trackNamespacePrefix});
}

void MoQRelay::unsubscribeAnnounces(
    const TrackNamespace& trackNamespacePrefix,
    std::shared_ptr<MoQSession> session) {
  XLOG(DBG1) << __func__ << " nsp=" << trackNamespacePrefix;
  auto nodePtr =
      findNamespaceNode(trackNamespacePrefix, /*createMissingNodes=*/false);
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
    forwarder =
        std::make_shared<MoQForwarder>(subReq.fullTrackName, folly::none);
    // TODO: there's a race condition that the forwarder gets upstream objects
    // before we add the downstream subscriber to it, below
    auto subRes = co_await upstreamSession->subscribe(subReq, forwarder);
    if (subRes.hasError()) {
      session->subscribeError({subReq.subscribeID, 502, "subscribe failed"});
      co_return;
    }
    auto latest = subRes.value()->subscribeOk().latest;
    if (latest) {
      forwarder->updateLatest(latest->group, latest->object);
    }
    forwarder->setGroupOrder(subRes.value()->subscribeOk().groupOrder);
    RelaySubscription rsub(
        {forwarder,
         upstreamSession,
         subRes.value()->subscribeOk().subscribeID,
         subRes.value()});
    subscriptions_[subReq.fullTrackName] = std::move(rsub);
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
        std::move(trackPublisher));
    forwarder->addSubscriber(std::move(subscriber));
  } else {
    XLOG(ERR) << "Downstream subscribeOK failed";
    // TODO: unsubsubscribe upstream
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
      subscription.handle->unsubscribe();
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
    } else {
      subscription.forwarder->removeSession(session);
    }
    if (subscription.forwarder->empty()) {
      XLOG(INFO) << "Removed last subscriber for " << subscriptionIt->first;
      subscription.handle->unsubscribe();
      subscriptionIt = subscriptions_.erase(subscriptionIt);
    } else {
      subscriptionIt++;
    }
  }
}

} // namespace moxygen

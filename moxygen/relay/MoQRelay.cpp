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

folly::coro::Task<Publisher::SubscribeResult> MoQRelay::subscribe(
    SubscribeRequest subReq,
    std::shared_ptr<TrackConsumer> consumer) {
  auto session = MoQSession::getRequestSession();
  auto subscriptionIt = subscriptions_.find(subReq.fullTrackName);
  if (subscriptionIt == subscriptions_.end()) {
    // first subscriber

    // check auth
    // get trackNamespace
    if (subReq.fullTrackName.trackNamespace.empty()) {
      co_return folly::makeUnexpected(
          SubscribeError({subReq.subscribeID, 400, "namespace required"}));
    }
    auto upstreamSession =
        findAnnounceSession(subReq.fullTrackName.trackNamespace);
    if (!upstreamSession) {
      // no such namespace has been announced
      co_return folly::makeUnexpected(
          SubscribeError({subReq.subscribeID, 404, "no such namespace"}));
    }
    if (session.get() == upstreamSession.get()) {
      co_return folly::makeUnexpected(
          SubscribeError({subReq.subscribeID, 400, "self subscribe"}));
    }
    // TODO: we only subscribe with the downstream locations.
    subReq.priority = 1;
    subReq.groupOrder = GroupOrder::Default;
    auto forwarder =
        std::make_shared<MoQForwarder>(subReq.fullTrackName, folly::none);
    forwarder->setCallback(shared_from_this());
    subscriptions_.emplace(
        std::piecewise_construct,
        std::forward_as_tuple(subReq.fullTrackName),
        std::forward_as_tuple(forwarder, upstreamSession));
    // The iterator returned from emplace does not survive across coroutine
    // resumption, so both the guard and updating the RelaySubscription below
    // require another lookup in the subscriptions_ map.
    auto g = folly::makeGuard([this, trackName = subReq.fullTrackName] {
      auto it = subscriptions_.find(trackName);
      if (it != subscriptions_.end()) {
        it->second.promise.setException(std::runtime_error("failed"));
        subscriptions_.erase(it);
      }
    });
    // Add subscriber first in case objects come before subscribe OK.
    auto subscriber = forwarder->addSubscriber(
        std::move(session), subReq, std::move(consumer));
    auto subRes = co_await upstreamSession->subscribe(subReq, forwarder);
    if (subRes.hasError()) {
      co_return folly::makeUnexpected(
          SubscribeError({subReq.subscribeID, 502, "subscribe failed"}));
    }
    g.dismiss();
    auto latest = subRes.value()->subscribeOk().latest;
    if (latest) {
      forwarder->updateLatest(latest->group, latest->object);
    }
    auto pubGroupOrder = subRes.value()->subscribeOk().groupOrder;
    forwarder->setGroupOrder(pubGroupOrder);
    subscriber->setPublisherGroupOrder(pubGroupOrder);
    auto it = subscriptions_.find(subReq.fullTrackName);
    XCHECK(it != subscriptions_.end());
    auto& rsub = it->second;
    rsub.subscribeID = subRes.value()->subscribeOk().subscribeID;
    rsub.handle = std::move(subRes.value());
    rsub.promise.setValue(folly::unit);
    co_return subscriber;
  } else {
    if (!subscriptionIt->second.promise.isFulfilled()) {
      // this will throw if the dependent subscribe failed, which is good
      // because subscriptionIt will be invalid
      co_await subscriptionIt->second.promise.getFuture();
    }
    co_return subscriptionIt->second.forwarder->addSubscriber(
        std::move(session), subReq, std::move(consumer));
  }
}

void MoQRelay::onEmpty(MoQForwarder* forwarder) {
  // TODO: we shouldn't need a linear search if forwarder stores FullTrackName
  for (auto subscriptionIt = subscriptions_.begin();
       subscriptionIt != subscriptions_.end();
       ++subscriptionIt) {
    auto& subscription = subscriptionIt->second;
    if (subscription.forwarder.get() != forwarder) {
      continue;
    }
    XLOG(INFO) << "Removed last subscriber for " << subscriptionIt->first;
    subscription.handle->unsubscribe();
    subscriptionIt = subscriptions_.erase(subscriptionIt);
    return;
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
    subscriptionIt++;
    // these actions may erase the current subscription
    if (subscription.upstream.get() == session.get()) {
      subscription.forwarder->subscribeDone(
          {SubscribeID(0),
           SubscribeDoneStatusCode::SUBSCRIPTION_ENDED,
           "upstream disconnect",
           subscription.forwarder->latest()});
    } else {
      subscription.forwarder->removeSession(session);
    }
  }
}

} // namespace moxygen

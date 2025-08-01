/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/relay/MoQRelay.h"

namespace {
constexpr uint8_t kDefaultUpstreamPriority = 128;
}

namespace moxygen {

std::vector<std::shared_ptr<MoQForwarder>>
MoQRelay::getAllPublishForwardersStartingAt(const AnnounceNode& node) {
  std::vector<std::shared_ptr<MoQForwarder>> moqForwaders;
  std::queue<AnnounceNode> q;
  q.push(node);

  // Simple BFS to find all the Available MoQForwards (ACTIVE PUBLISH CALLS)
  // For example, in a prefix tree, if a node is sub_announced to
  // /moq-date/date_1, It may be forwarded a PUBLISH from any node that
  // starts with /moq-date/date_1/*
  while (!q.empty()) {
    AnnounceNode curr = q.front();
    q.pop();

    // If node has an active PUBLISHES, add forward
    if (!curr.publishForwarders_.empty()) {
      for (const auto& [key, moqForwarder] : curr.publishForwarders_) {
        moqForwaders.push_back(moqForwarder);
      }
    }

    // Add children to queue
    for (const auto& child : curr.children) {
      q.push(*child.second);
    }
  }
  return moqForwaders;
}

std::shared_ptr<MoQRelay::AnnounceNode> MoQRelay::findNamespaceNode(
    const TrackNamespace& ns,
    bool createMissingNodes,
    MatchType matchType,
    std::vector<std::shared_ptr<MoQSession>>* sessions) {
  std::shared_ptr<AnnounceNode> nodePtr(
      std::shared_ptr<void>(), &announceRoot_);
  for (auto i = 0ul; i < ns.size(); i++) {
    if (sessions) {
      sessions->insert(
          sessions->end(), nodePtr->sessions.begin(), nodePtr->sessions.end());
    }
    auto& name = ns[i];
    auto it = nodePtr->children.find(name);
    if (it == nodePtr->children.end()) {
      if (createMissingNodes) {
        auto node = std::make_shared<AnnounceNode>(*this);
        nodePtr->children.emplace(name, node);
        nodePtr = std::move(node);
      } else if (
          matchType == MatchType::Prefix && nodePtr.get() != &announceRoot_) {
        return nodePtr;
      } else {
        XLOG(ERR) << "prefix not found in announce tree";
        return nullptr;
      }
    } else {
      nodePtr = it->second;
    }
  }
  return nodePtr;
}

folly::coro::Task<Subscriber::AnnounceResult> MoQRelay::announce(
    Announce ann,
    std::shared_ptr<Subscriber::AnnounceCallback>) {
  XLOG(DBG1) << __func__ << " ns=" << ann.trackNamespace;
  // check auth
  if (!ann.trackNamespace.startsWith(allowedNamespacePrefix_)) {
    co_return folly::makeUnexpected(AnnounceError{
        ann.requestID,
        ann.trackNamespace,
        AnnounceErrorCode::UNINTERESTED,
        "bad namespace"});
  }
  std::vector<std::shared_ptr<MoQSession>> sessions;
  auto nodePtr = findNamespaceNode(
      ann.trackNamespace,
      /*createMissingNodes=*/true,
      MatchType::Exact,
      &sessions);

  // TODO: store auth for forwarding on future SubscribeAnnounces?
  auto session = MoQSession::getRequestSession();
  nodePtr->sourceSession = std::move(session);
  nodePtr->setAnnounceOk({ann.requestID, ann.trackNamespace});
  for (auto& outSession : sessions) {
    if (outSession != session) {
      auto evb = outSession->getEventBase();
      co_withExecutor(evb, announceToSession(outSession, ann, nodePtr)).start();
    }
  }
  co_return nodePtr;
}

folly::coro::Task<void> MoQRelay::announceToSession(
    std::shared_ptr<MoQSession> session,
    Announce ann,
    std::shared_ptr<AnnounceNode> nodePtr) {
  auto announceHandle = co_await session->announce(ann);
  if (announceHandle.hasError()) {
    XLOG(ERR) << "Announce failed err=" << announceHandle.error().reasonPhrase;
  } else {
    // This can race with unsubscribeAnnounces
    nodePtr->announcements[session] = std::move(announceHandle.value());
  }
}

void MoQRelay::unannounce(const TrackNamespace& trackNamespace, AnnounceNode*) {
  XLOG(DBG1) << __func__ << " ns=" << trackNamespace;
  // Node would be useful if there were back links
  auto nodePtr = findNamespaceNode(trackNamespace);
  XCHECK(nodePtr);
  nodePtr->sourceSession = nullptr;
  for (auto& announcement : nodePtr->announcements) {
    auto evb = announcement.first->getEventBase();
    evb->runInEventBaseThread([announceHandle = announcement.second] {
      announceHandle->unannounce();
    });
  }
  nodePtr->announcements.clear();
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
    co_return folly::makeUnexpected(SubscribeAnnouncesError{
        subNs.requestID,
        subNs.trackNamespacePrefix,
        SubscribeAnnouncesErrorCode::NAMESPACE_PREFIX_UNKNOWN,
        "empty"});
  }
  auto session = MoQSession::getRequestSession();
  auto nodePtr = findNamespaceNode(
      subNs.trackNamespacePrefix, /*createMissingNodes=*/true);
  nodePtr->sessions.emplace(session);

  // Get all possible forwards to PUBLISH on for node
  auto moqForwarders = getAllPublishForwardersStartingAt(*nodePtr);

  for (const auto& moqForwarder : moqForwarders) {
    PublishRequest pubReq = {
        .requestID = subNs.requestID,
        .fullTrackName = moqForwarder->fullTrackName(),
        .trackAlias = moqForwarder->trackAlias().value_or(
            TrackAlias(subNs.requestID.value)),
        .groupOrder = moqForwarder->groupOrder(),
        .forward = true};

    // Initialize PUBLISH Tracks on session
    auto pubRes = session->publish(
        pubReq,
        std::make_shared<RelaySubscriptionHandle>(moqForwarder, session));
    if (pubRes.hasValue()) {
      SubscribeRequest subReq = {
          .requestID = subNs.requestID,
          .fullTrackName = moqForwarder->fullTrackName(),
          .groupOrder = moqForwarder->groupOrder(),
          .locType = LocationType::LargestObject,
          .forward = true,
          .trackAlias = moqForwarder->trackAlias().value_or(
              TrackAlias(subNs.requestID.value))};
      moqForwarder->addSubscriber(session, subReq, pubRes.value().consumer);
    }
  }

  // Find all nested Announcements and forward
  std::deque<std::tuple<TrackNamespace, std::shared_ptr<AnnounceNode>>> nodes{
      {subNs.trackNamespacePrefix, nodePtr}};
  auto evb = session->getEventBase();
  while (!nodes.empty()) {
    auto [prefix, nodePtr] = std::move(*nodes.begin());
    nodes.pop_front();
    if (nodePtr->sourceSession && nodePtr->sourceSession != session) {
      // TODO: Auth/params
      co_withExecutor(
          evb,
          announceToSession(session, {subNs.requestID, prefix, {}}, nodePtr))
          .start();
    }
    for (auto& nextNodeIt : nodePtr->children) {
      TrackNamespace nodePrefix(prefix);
      nodePrefix.append(nextNodeIt.first);
      nodes.emplace_back(std::forward_as_tuple(nodePrefix, nextNodeIt.second));
    }
  }
  co_return std::make_shared<AnnouncesSubscription>(
      shared_from_this(),
      std::move(session),
      SubscribeAnnouncesOk{subNs.requestID, subNs.trackNamespacePrefix});
}

void MoQRelay::unsubscribeAnnounces(
    const TrackNamespace& trackNamespacePrefix,
    std::shared_ptr<MoQSession> session) {
  XLOG(DBG1) << __func__ << " nsp=" << trackNamespacePrefix;
  auto nodePtr = findNamespaceNode(trackNamespacePrefix);
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
  /*
   * This function is called from subscribe() and fetch().
   * We use MatchType::Prefix here because the relay routes SUBSCRIBE and FETCH
   * to the publisher who announced the closest matching broader namespace, not
   * necessarily the exact match.
   */
  auto nodePtr =
      findNamespaceNode(ns, /*createMissingNodes=*/false, MatchType::Prefix);
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
      // message error?
      co_return folly::makeUnexpected(SubscribeError(
          {subReq.requestID,
           SubscribeErrorCode::TRACK_NOT_EXIST,
           "namespace required"}));
    }
    auto upstreamSession =
        findAnnounceSession(subReq.fullTrackName.trackNamespace);
    if (!upstreamSession) {
      // no such namespace has been announced
      co_return folly::makeUnexpected(SubscribeError(
          {subReq.requestID,
           SubscribeErrorCode::TRACK_NOT_EXIST,
           "no such namespace"}));
    }
    if (session.get() == upstreamSession.get()) {
      // message error
      co_return folly::makeUnexpected(SubscribeError(
          {subReq.requestID,
           SubscribeErrorCode::INTERNAL_ERROR,
           "self subscribe"}));
    }
    subReq.priority = kDefaultUpstreamPriority;
    subReq.groupOrder = GroupOrder::Default;
    // We only subscribe upstream with LargestObject. This is to satisfy other
    // subscribers that join with narrower filters
    subReq.locType = LocationType::LargestObject;
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
    // As per the spec, we must set forward = true in the subscribe request
    // to the upstream.
    subReq.forward = true;
    auto subRes = co_await upstreamSession->subscribe(
        subReq, getSubscribeWriteback(subReq.fullTrackName, forwarder));
    if (subRes.hasError()) {
      co_return folly::makeUnexpected(SubscribeError(
          {subReq.requestID,
           subRes.error().errorCode,
           folly::to<std::string>(
               "upstream subscribe failed: ", subRes.error().reasonPhrase)}));
    }
    g.dismiss();
    auto largest = subRes.value()->subscribeOk().largest;
    if (largest) {
      forwarder->updateLargest(largest->group, largest->object);
      subscriber->updateLargest(*largest);
    }
    auto pubGroupOrder = subRes.value()->subscribeOk().groupOrder;
    forwarder->setGroupOrder(pubGroupOrder);
    subscriber->setPublisherGroupOrder(pubGroupOrder);
    auto it = subscriptions_.find(subReq.fullTrackName);
    XCHECK(it != subscriptions_.end());
    auto& rsub = it->second;
    rsub.requestID = subRes.value()->subscribeOk().requestID;
    rsub.handle = std::move(subRes.value());
    rsub.promise.setValue(folly::unit);
    co_return subscriber;
  } else {
    if (!subscriptionIt->second.promise.isFulfilled()) {
      // this will throw if the dependent subscribe failed, which is good
      // because subscriptionIt will be invalid
      co_await subscriptionIt->second.promise.getFuture();
    }
    auto& forwarder = subscriptionIt->second.forwarder;
    if (forwarder->largest() && subReq.locType == LocationType::AbsoluteRange &&
        subReq.endGroup < forwarder->largest()->group) {
      co_return folly::makeUnexpected(SubscribeError{
          subReq.requestID,
          SubscribeErrorCode::INVALID_RANGE,
          "Range in the past, use FETCH"});
      // start may be in the past, it will get adjusted forward to largest
    }
    co_return subscriptionIt->second.forwarder->addSubscriber(
        std::move(session), subReq, std::move(consumer));
  }
}

Subscriber::PublishResult MoQRelay::publish(
    PublishRequest pubReq,
    std::shared_ptr<SubscriptionHandle> handle) {
  if (pubReq.fullTrackName.trackNamespace.empty()) {
    return folly::makeUnexpected(PublishError(
        {pubReq.requestID,
         PublishErrorCode::INTERNAL_ERROR,
         "namespace required"}));
  }

  auto upstreamSession =
      findAnnounceSession(pubReq.fullTrackName.trackNamespace);
  if (!upstreamSession) {
    return folly::makeUnexpected(PublishError(
        {pubReq.requestID,
         PublishErrorCode::UNAUTHORIZED,
         "no such namespace has been announced"}));
  }

  // Find All Nodes that SubscribeAnnounced to this namespace (including prefix
  // ns)
  std::vector<std::shared_ptr<MoQSession>> sessions = {};
  auto nodePtr = findNamespaceNode(
      pubReq.fullTrackName.trackNamespace, true, MatchType::Exact, &sessions);

  // Create Forwarder for this publish
  auto moqForwarder =
      std::make_shared<MoQForwarder>(pubReq.fullTrackName, folly::none);

  // Set Forwarder Params
  moqForwarder->setTrackAlias(pubReq.trackAlias);
  moqForwarder->setGroupOrder(pubReq.groupOrder);

  // Set Forwarder for this node
  nodePtr->publishForwarders_.emplace(
      pubReq.fullTrackName.trackName, moqForwarder);

  // Create a SubReq to create a Subscriber for this Forwarder
  SubscribeRequest subReq = {
      .requestID = pubReq.requestID,
      .fullTrackName = pubReq.fullTrackName,
      .groupOrder = pubReq.groupOrder,
      .locType = LocationType::LargestObject,
      .forward = pubReq.forward,
      .trackAlias = pubReq.trackAlias};

  for (auto& session : sessions) {
    // Initialize PUBLISH Tracks on sessions
    auto pubRes = session->publish(
        pubReq,
        std::make_shared<RelaySubscriptionHandle>(moqForwarder, session));
    if (pubRes.hasError()) {
      return folly::makeUnexpected(PublishError(
          {pubReq.requestID,
           pubRes.error().errorCode,
           folly::to<std::string>(
               "publish failed: ", pubRes.error().reasonPhrase)}));
    }

    // Add Subscriber to Forwarder
    auto subscriber = moqForwarder->addSubscriber(
        session, subReq, std::move(pubRes.value().consumer));
  }
  moqForwarder->setCallback(shared_from_this());

  return PublishConsumerAndReplyTask(
      moqForwarder,
      folly::coro::makeTask<
          folly::Expected<moxygen::PublishOk, moxygen::PublishError>>(
          moxygen::PublishOk{
              pubReq.requestID,
              true,
              kDefaultPriority,
              pubReq.groupOrder,
              moxygen::LocationType::AbsoluteStart,
              moxygen::AbsoluteLocation{0, 0},
              {}}));
}

folly::coro::Task<Publisher::FetchResult> MoQRelay::fetch(
    Fetch fetch,
    std::shared_ptr<FetchConsumer> consumer) {
  auto session = MoQSession::getRequestSession();

  // check auth
  // get trackNamespace
  if (fetch.fullTrackName.trackNamespace.empty()) {
    co_return folly::makeUnexpected(FetchError(
        {fetch.requestID,
         FetchErrorCode::TRACK_NOT_EXIST,
         "namespace required"}));
  }

  auto [standalone, joining] = fetchType(fetch);
  if (joining) {
    auto subscriptionIt = subscriptions_.find(fetch.fullTrackName);
    if (subscriptionIt == subscriptions_.end()) {
      XLOG(ERR) << "No subscription for joining fetch";
      // message error
      co_return folly::makeUnexpected(FetchError(
          {fetch.requestID,
           FetchErrorCode::TRACK_NOT_EXIST,
           "No subscription for joining fetch"}));
    } else if (subscriptionIt->second.promise.isFulfilled()) {
      auto res = subscriptionIt->second.forwarder->resolveJoiningFetch(
          session, *joining);
      if (res.hasError()) {
        co_return folly::makeUnexpected(res.error());
      }
      fetch.args = StandaloneFetch(res.value().start, res.value().end);
      joining = nullptr;
    } else {
      // Upstream is resolving the subscribe, forward joining fetch
      joining->joiningRequestID = subscriptionIt->second.requestID;
    }
  }

  auto upstreamSession =
      findAnnounceSession(fetch.fullTrackName.trackNamespace);
  if (!upstreamSession) {
    // no such namespace has been announced
    co_return folly::makeUnexpected(FetchError(
        {fetch.requestID,
         FetchErrorCode::TRACK_NOT_EXIST,
         "no such namespace"}));
  }
  if (session.get() == upstreamSession.get()) {
    co_return folly::makeUnexpected(FetchError(
        {fetch.requestID, FetchErrorCode::INTERNAL_ERROR, "self fetch"}));
  }
  fetch.priority = kDefaultUpstreamPriority;
  if (!cache_ || joining) {
    // We can't use the cache on an unresolved joining fetch - we don't know
    // which objects are being requested.  However, once we have that resolved,
    // we SHOULD be able to serve from cache.
    if (standalone) {
      XLOG(DBG1) << "Upstream fetch {" << standalone->start.group << ","
                 << standalone->start.object << "}.." << standalone->end.group
                 << "," << standalone->end.object << "}";
    }
    co_return co_await upstreamSession->fetch(fetch, std::move(consumer));
  }
  co_return co_await cache_->fetch(
      fetch, std::move(consumer), std::move(upstreamSession));
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
    if (subscription.handle) {
      subscription.handle->unsubscribe();
    }
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

    auto it = nodePtr->announcements.find(session);
    if (it != nodePtr->announcements.end()) {
      // we've announced this node to the removing session.
      // Do we really need to unannounce?
      it->second->unannounce();
      nodePtr->announcements.erase(it);
    }

    if (nodePtr->sourceSession == session) {
      // This session is unannouncing
      nodePtr->sourceSession = nullptr;
      for (auto& announcement : nodePtr->announcements) {
        auto evb = announcement.first->getEventBase();
        evb->runInEventBaseThread([announceHandle = announcement.second] {
          announceHandle->unannounce();
        });
      }
      nodePtr->announcements.clear();
    }
    for (auto& nextNode : nodePtr->children) {
      TrackNamespace nodePrefix(prefix);
      nodePrefix.append(nextNode.first);
      nodes.emplace_back(
          std::forward_as_tuple(std::move(nodePrefix), nextNode.second.get()));
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
          {RequestID(0),
           SubscribeDoneStatusCode::SUBSCRIPTION_ENDED,
           0, // filled in by session
           "upstream disconnect"});
    } else {
      subscription.forwarder->removeSession(session);
    }
  }
}

} // namespace moxygen

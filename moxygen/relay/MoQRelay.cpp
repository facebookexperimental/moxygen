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
    co_return folly::makeUnexpected(
        AnnounceError{
            ann.requestID, AnnounceErrorCode::UNINTERESTED, "bad namespace"});
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
  nodePtr->trackNamespace_ = ann.trackNamespace;
  nodePtr->setAnnounceOk({ann.requestID, {}});
  for (auto& outSession : sessions) {
    if (outSession != session) {
      auto exec = outSession->getExecutor();
      co_withExecutor(exec, announceToSession(outSession, ann, nodePtr))
          .start();
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
    auto exec = announcement.first->getExecutor();
    exec->add([announceHandle = announcement.second] {
      announceHandle->unannounce();
    });
  }
  nodePtr->announcements.clear();
  // TODO: prune Announce tree
}

Subscriber::PublishResult MoQRelay::publish(
    PublishRequest pub,
    std::shared_ptr<Publisher::SubscriptionHandle> handle) {
  XLOG(DBG1) << __func__ << " ns=" << pub.fullTrackName.trackNamespace;
  if (!pub.fullTrackName.trackNamespace.startsWith(allowedNamespacePrefix_)) {
    return folly::makeUnexpected(
        PublishError{
            pub.requestID, PublishErrorCode::UNINTERESTED, "bad namespace"});
  }

  if (pub.fullTrackName.trackNamespace.empty()) {
    return folly::makeUnexpected(PublishError(
        {pub.requestID,
         PublishErrorCode::INTERNAL_ERROR,
         "namespace required"}));
  }

  // Find All Nodes that SubscribeAnnounced to this namespace (including prefix
  // ns)
  std::vector<std::shared_ptr<MoQSession>> sessions = {};
  auto nodePtr = findNamespaceNode(
      pub.fullTrackName.trackNamespace,
      /*createMissingNodes=*/true,
      MatchType::Exact,
      &sessions);
  sessions.insert(
      sessions.end(), nodePtr->sessions.begin(), nodePtr->sessions.end());

  auto it = subscriptions_.find(pub.fullTrackName);
  if (it != subscriptions_.end()) {
    // someone already announced this FTN, we don't support multipublisher
    XLOG(DBG1) << "New publisher for existing subscription";
    nodePtr->publishes.erase(pub.fullTrackName.trackName);
    it->second.handle->unsubscribe();
    it->second.forwarder->subscribeDone(
        {RequestID(0),
         SubscribeDoneStatusCode::SUBSCRIPTION_ENDED,
         0, // filled in by session
         "upstream disconnect"});
    subscriptions_.erase(it);
  }
  auto session = MoQSession::getRequestSession();
  auto res = nodePtr->publishes.emplace(pub.fullTrackName.trackName, session);
  XCHECK(res.second) << "Duplicate publish";

  // Create Forwarder for this publish
  auto forwarder =
      std::make_shared<MoQForwarder>(pub.fullTrackName, folly::none);

  // Set Forwarder Params
  forwarder->setGroupOrder(pub.groupOrder);

  // Extract delivery timeout from publish request params and store in forwarder
  auto deliveryTimeout = MoQSession::getDeliveryTimeoutIfPresent(
      pub.params, session->getNegotiatedVersion().value());
  if (deliveryTimeout && *deliveryTimeout > 0) {
    forwarder->setDeliveryTimeout(*deliveryTimeout);
  }

  auto subRes = subscriptions_.emplace(
      std::piecewise_construct,
      std::forward_as_tuple(pub.fullTrackName),
      std::forward_as_tuple(forwarder, session));
  auto& rsub = subRes.first->second;
  rsub.promise.setValue(folly::unit);
  rsub.requestID = pub.requestID;
  rsub.handle = std::move(handle);
  rsub.isPublish = true;

  uint64_t nSubscribers = 0;
  for (auto& outSession : sessions) {
    if (outSession != session) {
      nSubscribers++;
      auto exec = outSession->getExecutor();
      co_withExecutor(exec, publishToSession(outSession, forwarder, pub))
          .start();
    }
  }
  forwarder->setCallback(shared_from_this());

  return PublishConsumerAndReplyTask{
      forwarder,
      folly::coro::makeTask<folly::Expected<PublishOk, PublishError>>(PublishOk{
          pub.requestID,
          /*forward=*/(nSubscribers > 0),
          kDefaultPriority,
          pub.groupOrder,
          LocationType::AbsoluteRange,
          kLocationMin,
          kLocationMax.group,
          {}})};
}

folly::coro::Task<void> MoQRelay::publishToSession(
    std::shared_ptr<MoQSession> session,
    std::shared_ptr<MoQForwarder> forwarder,
    PublishRequest pub) {
  pub.forward = false;
  auto subscriber = forwarder->addSubscriber(session, pub);
  auto guard = folly::makeGuard(
      [forwarder, session] { forwarder->removeSession(session); });
  if (pub.largest) {
    subscriber->updateLargest(*pub.largest);
  }
  subscriber->setPublisherGroupOrder(pub.groupOrder);

  auto pubInitial = session->publish(pub, subscriber);
  if (pubInitial.hasError()) {
    XLOG(ERR) << "Publish failed err=" << pubInitial.error().reasonPhrase;
    co_return;
  }
  subscriber->trackConsumer = std::move(pubInitial->consumer);
  auto pubResult = co_await co_awaitTry(std::move(pubInitial->reply));
  if (pubResult.hasException()) {
    XLOG(ERR) << "Publish failed err=" << pubResult.exception().what();
    co_return;
  }
  if (pubResult.value().hasError()) {
    XLOG(ERR) << "Publish failed err="
              << pubResult.value().error().reasonPhrase;
    co_return;
  }
  guard.dismiss();
  XLOG(DBG1) << "Publish OK sess=" << session.get();
  auto& pubOk = pubResult.value().value();
  folly::Optional<AbsoluteLocation> end;
  if (pubOk.endGroup) {
    end = AbsoluteLocation{*pubOk.endGroup, 0};
  }
  subscriber->range =
      toSubscribeRange(pubOk.start, end, pubOk.locType, forwarder->largest());
  subscriber->shouldForward = pubOk.forward;

  // Extract delivery timeout from upstream PUBLISH_OK and propagate to
  // subscriber
  auto deliveryTimeout = MoQSession::getDeliveryTimeoutIfPresent(
      pubOk.params, session->getNegotiatedVersion().value());
  if (deliveryTimeout && *deliveryTimeout > 0) {
    forwarder->setDeliveryTimeout(*deliveryTimeout);
    subscriber->setParam(
        {folly::to_underlying(TrackRequestParamKey::DELIVERY_TIMEOUT),
         "",
         *deliveryTimeout,
         {}});
  }
}

class MoQRelay::AnnouncesSubscription
    : public Publisher::SubscribeAnnouncesHandle {
 public:
  AnnouncesSubscription(
      std::shared_ptr<MoQRelay> relay,
      std::shared_ptr<MoQSession> session,
      SubscribeAnnouncesOk ok,
      TrackNamespace trackNamespacePrefix)
      : Publisher::SubscribeAnnouncesHandle(std::move(ok)),
        relay_(std::move(relay)),
        session_(std::move(session)),
        trackNamespacePrefix_(std::move(trackNamespacePrefix)) {}

  void unsubscribeAnnounces() override {
    if (relay_) {
      relay_->unsubscribeAnnounces(trackNamespacePrefix_, std::move(session_));
      relay_.reset();
    }
  }

 private:
  std::shared_ptr<MoQRelay> relay_;
  std::shared_ptr<MoQSession> session_;
  TrackNamespace trackNamespacePrefix_;
};

folly::coro::Task<Publisher::SubscribeAnnouncesResult>
MoQRelay::subscribeAnnounces(SubscribeAnnounces subNs) {
  XLOG(DBG1) << __func__ << " nsp=" << subNs.trackNamespacePrefix;
  // check auth
  if (subNs.trackNamespacePrefix.empty()) {
    co_return folly::makeUnexpected(
        SubscribeAnnouncesError{
            subNs.requestID,
            SubscribeAnnouncesErrorCode::NAMESPACE_PREFIX_UNKNOWN,
            "empty"});
  }
  auto session = MoQSession::getRequestSession();
  auto nodePtr = findNamespaceNode(
      subNs.trackNamespacePrefix, /*createMissingNodes=*/true);
  nodePtr->sessions.emplace(session);

  // Find all nested Announcements/Publishes and forward
  std::deque<std::tuple<TrackNamespace, std::shared_ptr<AnnounceNode>>> nodes{
      {subNs.trackNamespacePrefix, nodePtr}};
  auto exec = session->getExecutor();
  while (!nodes.empty()) {
    auto [prefix, nodePtr] = std::move(*nodes.begin());
    nodes.pop_front();
    if (nodePtr->sourceSession && nodePtr->sourceSession != session) {
      // TODO: Auth/params
      co_withExecutor(
          exec,
          announceToSession(session, {subNs.requestID, prefix, {}}, nodePtr))
          .start();
    }
    PublishRequest pub{
        0,
        FullTrackName{prefix, ""},
        TrackAlias(0), // filled in by library
        GroupOrder::Default,
        folly::none,
        /*forward=*/false,
        {}};
    for (auto& publishEntry : nodePtr->publishes) {
      auto& publishSession = publishEntry.second;
      pub.fullTrackName.trackName = publishEntry.first;
      auto subscriptionIt = subscriptions_.find(pub.fullTrackName);
      if (subscriptionIt == subscriptions_.end()) {
        XLOG(ERR) << "Invalid state, no subscription for publish ftn="
                  << pub.fullTrackName;
        continue;
      }
      auto& forwarder = subscriptionIt->second.forwarder;
      if (forwarder->empty()) {
        subscriptionIt->second.handle->subscribeUpdate(
            {RequestID(0),
             subscriptionIt->second.handle->subscribeOk().requestID,
             kLocationMin,
             kLocationMax.group,
             kDefaultPriority,
             /*forward=*/true,
             {}});
      }
      pub.groupOrder = forwarder->groupOrder();
      pub.largest = forwarder->largest();
      if (publishSession != session) {
        exec = publishSession->getExecutor();
        co_withExecutor(exec, publishToSession(session, forwarder, pub))
            .start();
      }
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
      SubscribeAnnouncesOk{subNs.requestID, {}},
      subNs.trackNamespacePrefix);
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
    subReq.priority = kDefaultUpstreamPriority;
    subReq.groupOrder = GroupOrder::Default;
    // We only subscribe upstream with LargestObject. This is to satisfy other
    // subscribers that join with narrower filters
    subReq.locType = LocationType::LargestObject;
    auto forwarder =
        std::make_shared<MoQForwarder>(subReq.fullTrackName, folly::none);
    forwarder->setCallback(shared_from_this());
    auto emplaceRes = subscriptions_.emplace(
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
    auto sessionVersion = session->getNegotiatedVersion();
    auto subscriber = forwarder->addSubscriber(
        std::move(session), subReq, std::move(consumer));
    // As per the spec, we must set forward = true in the subscribe request
    // to the upstream.
    subReq.forward = true;

    emplaceRes.first->second.requestID = upstreamSession->peekNextRequestID();
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

    // Store upstream delivery timeout in forwarder
    // add delivery timeout to downstream sub
    auto deliveryTimeout = MoQSession::getDeliveryTimeoutIfPresent(
        subRes.value()->subscribeOk().params, sessionVersion.value());
    if (deliveryTimeout && *deliveryTimeout > 0) {
      forwarder->setDeliveryTimeout(*deliveryTimeout);
      subscriber->setParam(
          {folly::to_underlying(TrackRequestParamKey::DELIVERY_TIMEOUT),
           "",
           *deliveryTimeout,
           {}});
    }

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
      co_return folly::makeUnexpected(
          SubscribeError{
              subReq.requestID,
              SubscribeErrorCode::INVALID_RANGE,
              "Range in the past, use FETCH"});
      // start may be in the past, it will get adjusted forward to largest
    }
    auto subscriber = subscriptionIt->second.forwarder->addSubscriber(
        std::move(session), subReq, std::move(consumer));

    // add delivery timeout to downstream sub
    if (forwarder->upstreamDeliveryTimeout().count() > 0) {
      subscriber->setParam(
          {folly::to_underlying(TrackRequestParamKey::DELIVERY_TIMEOUT),
           "",
           static_cast<uint64_t>(forwarder->upstreamDeliveryTimeout().count()),
           {}});
    }

    co_return subscriber;
  }
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
    // Attempt to find matching upstream subscription (from publish)
    auto subscriptionIt = subscriptions_.find(fetch.fullTrackName);
    if (subscriptionIt != subscriptions_.end()) {
      upstreamSession = subscriptionIt->second.upstream;
    } else {
      // no such namespace has been announced
      co_return folly::makeUnexpected(FetchError(
          {fetch.requestID,
           FetchErrorCode::TRACK_NOT_EXIST,
           "no such namespace"}));
    }
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
      if (subscription.isPublish) {
        // if it's publish, don't unsubscribe, just subscribeUpdate
        // forward=false
        XLOG(DBG1) << "Updating upstream subscription forward=false";
        subscription.handle->subscribeUpdate(
            {RequestID(0),
             subscription.handle->subscribeOk().requestID,
             kLocationMin,
             kLocationMax.group,
             kDefaultPriority,
             /*forward=*/false,
             {}});
      } else {
        subscription.handle->unsubscribe();
      }
    }
    if (!subscription.isPublish) {
      subscriptionIt = subscriptions_.erase(subscriptionIt);
    }
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
        auto exec = announcement.first->getExecutor();
        exec->add([announceHandle = announcement.second] {
          announceHandle->unannounce();
        });
      }
      nodePtr->announcements.clear();
    }
    for (auto pubIt = nodePtr->publishes.begin();
         pubIt != nodePtr->publishes.end();) {
      if (pubIt->second == session) {
        pubIt = nodePtr->publishes.erase(pubIt);
      } else {
        ++pubIt;
      }
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
    auto curIt = subscriptionIt++;
    bool isPublish = subscription.isPublish;
    // these actions may erase the current subscription
    if (subscription.upstream.get() == session.get()) {
      subscription.forwarder->subscribeDone(
          {RequestID(0),
           SubscribeDoneStatusCode::SUBSCRIPTION_ENDED,
           0, // filled in by session
           "upstream disconnect"});
      if (isPublish) {
        subscriptions_.erase(curIt);
      }
    } else {
      subscription.forwarder->removeSession(session);
    }
  }
}

} // namespace moxygen

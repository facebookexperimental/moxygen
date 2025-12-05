/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/relay/MoQRelay.h"
#include "moxygen/MoQFilters.h"

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
        auto node = std::make_shared<AnnounceNode>(*this, nodePtr.get());
        nodePtr->children.emplace(name, node);
        // Don't increment yet - only when content is actually added
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
    std::shared_ptr<Subscriber::AnnounceCallback> callback) {
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

  // Log if there is already a session that has announced this track
  if (nodePtr->sourceSession) {
    XLOG(WARNING) << "Announce: Existing session ("
                  << nodePtr->sourceSession.get()
                  << ") has already announced trackNamespace="
                  << ann.trackNamespace;
    // Since we don't fully support multiple publishers -- cancel the old
    // announcement and remove ongoing subscriptions to this publisher
    // in that namespace.  Note: it could have announced a more specific
    // namespace which hasn't been overridden by the new publisher, but
    // for now we don't support that.
    if (nodePtr->announceCallback) {
      nodePtr->announceCallback->announceCancel(
          AnnounceErrorCode::CANCELLED, "New publisher");
      nodePtr->announceCallback.reset();
    }
    for (auto it = subscriptions_.begin(); it != subscriptions_.end();) {
      // Check if the subscription's FullTrackName is in this namespace
      if (it->first.trackNamespace.startsWith(ann.trackNamespace) &&
          it->second.upstream == nodePtr->sourceSession) {
        XLOG(DBG4) << "Erasing subscription to " << it->first;
        it = subscriptions_.erase(it);
      } else {
        ++it;
      }
    }

    nodePtr->sourceSession.reset();
  }

  // TODO: store auth for forwarding on future SubscribeAnnounces?
  auto session = MoQSession::getRequestSession();
  bool wasEmpty = !nodePtr->hasLocalSessions();
  nodePtr->sourceSession = std::move(session);
  nodePtr->announceCallback = std::move(callback);
  nodePtr->trackNamespace_ = ann.trackNamespace;
  nodePtr->setAnnounceOk({ann.requestID, {}});

  // If this is the first content added to this node, notify parent
  if (wasEmpty && nodePtr->parent_) {
    nodePtr->parent_->incrementActiveChildren();
  }
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

// AnnounceNode ref count management methods for pruning
void MoQRelay::AnnounceNode::incrementActiveChildren() {
  activeChildCount_++;
  // Propagate up if this was the first active child
  if (activeChildCount_ == 1 && parent_ && !hasLocalSessions()) {
    parent_->incrementActiveChildren();
  }
}

void MoQRelay::AnnounceNode::decrementActiveChildren() {
  XCHECK_GT(activeChildCount_, 0);
  activeChildCount_--;
}

// Walk up the tree to find and prune the highest empty ancestor
void MoQRelay::AnnounceNode::tryPruneChild(const std::string& childKey) {
  auto it = children.find(childKey);
  if (it == children.end()) {
    return;
  }

  auto childNode = it->second.get();
  if (childNode->shouldKeep()) {
    return;
  }

  // Walk up the tree, decrementing counts, to find highest empty ancestor
  // Track the key to remove and the parent to remove it from
  std::string keyToRemove = childKey;
  AnnounceNode* parentOfNodeToRemove = this;
  AnnounceNode* current = this;

  while (current) {
    XCHECK_GT(current->activeChildCount_, 0);
    current->activeChildCount_--;

    // If current still has content or children after decrement, stop walking
    // Remove keyToRemove from parentOfNodeToRemove
    if (current->hasLocalSessions() || current->activeChildCount_ > 0) {
      break;
    }

    // Current is now empty too - if it has a parent, continue walking up
    if (!current->parent_) {
      // We've reached root - can't remove root, so stop
      break;
    }

    // Current is empty and not root, so it becomes the new candidate for
    // removal Find the key for current in its parent's children map
    for (const auto& [key, node] : current->parent_->children) {
      if (node.get() == current) {
        keyToRemove = key;
        parentOfNodeToRemove = current->parent_;
        break;
      }
    }

    current = current->parent_;
  }

  // Remove the highest empty node from its parent
  XLOG(DBG1) << "Pruning empty subtree at: " << keyToRemove;
  parentOfNodeToRemove->children.erase(keyToRemove);
}

void MoQRelay::unannounce(const TrackNamespace& trackNamespace, AnnounceNode*) {
  XLOG(DBG1) << __func__ << " ns=" << trackNamespace;
  // Node would be useful if there were back links
  auto nodePtr = findNamespaceNode(trackNamespace);
  if (!nodePtr) {
    // Node was already pruned, nothing to do, maybe app called unannouce twice?
    XLOG(DBG1) << "Node already pruned for ns=" << trackNamespace;
    return;
  }

  // Track if node had local content before modification
  bool hadLocalContent = nodePtr->hasLocalSessions();

  nodePtr->sourceSession = nullptr;
  nodePtr->announceCallback.reset();
  for (auto& announcement : nodePtr->announcements) {
    auto exec = announcement.first->getExecutor();
    exec->add([announceHandle = announcement.second] {
      announceHandle->unannounce();
    });
  }
  nodePtr->announcements.clear();

  // Prune if node became empty and has a parent
  if (hadLocalContent && !nodePtr->shouldKeep() && nodePtr->parent_ &&
      !trackNamespace.trackNamespace.empty()) {
    nodePtr->parent_->tryPruneChild(trackNamespace.trackNamespace.back());
  }
}

void MoQRelay::onPublishDone(const FullTrackName& ftn) {
  XLOG(DBG1) << __func__ << " ftn=" << ftn;

  auto it = subscriptions_.find(ftn);
  if (it != subscriptions_.end()) {
    if (it->second.isPublish) {
      // Remove from publishes map
      auto nodePtr = findNamespaceNode(ftn.trackNamespace);
      if (nodePtr) {
        bool hadLocalContent = nodePtr->hasLocalSessions();
        nodePtr->publishes.erase(ftn.trackName);

        // Prune if node became empty and has a parent
        if (hadLocalContent && !nodePtr->shouldKeep() && nodePtr->parent_ &&
            !ftn.trackNamespace.trackNamespace.empty()) {
          nodePtr->parent_->tryPruneChild(
              ftn.trackNamespace.trackNamespace.back());
        }
      }
    }

    // Clear the handle and upstream - this signals publisher is done
    // Clearing upstream is important to break circular reference:
    // session holds FilterConsumer, relay holds session in upstream
    it->second.handle.reset();
    it->second.upstream.reset();

    // If forwarder has no subscribers, clean up immediately
    // Otherwise onEmpty will be called when last subscriber leaves
    if (it->second.forwarder->empty()) {
      XLOG(DBG1) << "Publisher terminated with no subscribers, cleaning up "
                 << ftn;
      subscriptions_.erase(it);
    }
  }
}

Subscriber::PublishResult MoQRelay::publish(
    PublishRequest pub,
    std::shared_ptr<Publisher::SubscriptionHandle> handle) {
  XLOG(DBG1) << __func__ << " ns=" << pub.fullTrackName.trackNamespace;
  XCHECK(handle) << "Publish handle cannot be null";
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

  auto session = MoQSession::getRequestSession();
  bool wasEmpty = !nodePtr->hasLocalSessions();

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
    XLOG(DBG4) << "Erasing subscription to " << it->first;
    subscriptions_.erase(it);
  }
  auto res = nodePtr->publishes.emplace(pub.fullTrackName.trackName, session);
  XCHECK(res.second) << "Duplicate publish";

  // If this is the first content added to this node, notify parent
  if (wasEmpty && nodePtr->hasLocalSessions() && nodePtr->parent_) {
    nodePtr->parent_->incrementActiveChildren();
  }

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

  // Wrap forwarder in filter to intercept subscribeDone
  auto filterImpl = std::make_shared<TerminationFilter>(
      shared_from_this(), pub.fullTrackName, forwarder);
  std::shared_ptr<TrackConsumer> filter =
      std::static_pointer_cast<TrackConsumer>(filterImpl);

  return PublishConsumerAndReplyTask{
      filter, // Return filter, not forwarder directly
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
  auto guard = folly::makeGuard([subscriber] { subscriber->unsubscribe(); });
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

// Filter TrackConsumer that intercepts subscribeDone to clean up relay state
class MoQRelay::TerminationFilter : public TrackConsumerFilter {
 public:
  TerminationFilter(
      std::shared_ptr<MoQRelay> relay,
      FullTrackName ftn,
      std::shared_ptr<TrackConsumer> downstream)
      : TrackConsumerFilter(std::move(downstream)),
        relay_(std::move(relay)),
        ftn_(std::move(ftn)) {}

  folly::Expected<folly::Unit, MoQPublishError> subscribeDone(
      SubscribeDone subDone) override {
    // Notify relay that publisher is done - this will:
    // 1. Remove from nodePtr->publishes
    // 2. Clear subscription.handle
    if (relay_) {
      relay_->onPublishDone(ftn_);
    }
    // Change the downstream code to something like "upstream ended"?
    return TrackConsumerFilter::subscribeDone(std::move(subDone));
  }

 private:
  std::shared_ptr<MoQRelay> relay_;
  FullTrackName ftn_;
};

std::shared_ptr<TrackConsumer> MoQRelay::getSubscribeWriteback(
    const FullTrackName& ftn,
    std::shared_ptr<TrackConsumer> consumer) {
  auto baseConsumer = cache_
      ? cache_->getSubscribeWriteback(ftn, std::move(consumer))
      : std::move(consumer);
  auto filterConsumer = std::make_shared<TerminationFilter>(
      shared_from_this(), ftn, std::move(baseConsumer));
  return std::static_pointer_cast<TrackConsumer>(filterConsumer);
}

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

  // Check if this is the first session subscriber for this node
  bool wasEmpty = !nodePtr->hasLocalSessions();
  nodePtr->sessions.emplace(session);

  // If this is the first content added to this node, notify parent
  if (wasEmpty && nodePtr->hasLocalSessions() && nodePtr->parent_) {
    nodePtr->parent_->incrementActiveChildren();
  }

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
             subscriptionIt->second.requestID,
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

  // Track if node had local content before modification
  bool hadLocalContent = nodePtr->hasLocalSessions();

  auto it = nodePtr->sessions.find(session);
  if (it != nodePtr->sessions.end()) {
    nodePtr->sessions.erase(it);

    // Prune if node became empty and has a parent
    if (hadLocalContent && !nodePtr->shouldKeep() && nodePtr->parent_ &&
        !trackNamespacePrefix.trackNamespace.empty()) {
      nodePtr->parent_->tryPruneChild(
          trackNamespacePrefix.trackNamespace.back());
    }
    return;
  }
  // TODO: error?
  XLOG(DBG1) << "Namespace prefix was not subscribed by this session";
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

MoQRelay::PublishState MoQRelay::findPublishState(const FullTrackName& ftn) {
  PublishState state;
  auto nodePtr = findNamespaceNode(
      ftn.trackNamespace, /*createMissingNodes=*/false, MatchType::Exact);

  if (!nodePtr) {
    // Node doesn't exist - tree was properly pruned
    return state;
  }

  state.nodeExists = true;

  auto it = nodePtr->publishes.find(ftn.trackName);
  if (it != nodePtr->publishes.end()) {
    state.session = it->second;
  }

  return state;
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
           "no such namespace or track"}));
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
        XLOG(DBG4) << "Erasing subscription to " << it->first;
        subscriptions_.erase(it);
      }
    });
    // Add subscriber first in case objects come before subscribe OK.
    auto sessionVersion = session->getNegotiatedVersion();
    auto subscriber = forwarder->addSubscriber(
        std::move(session), subReq, std::move(consumer));
    // As per the spec, we must set forward = true in the subscribe request
    // to the upstream.
    // But should we if this is forward=0?
    subReq.forward = forwarder->numForwardingSubscribers() > 0;

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
    // is it more correct to co_await folly::coro::co_safe_point here?
    g.dismiss();
    auto largest = subRes.value()->subscribeOk().largest;
    if (largest) {
      forwarder->updateLargest(largest->group, largest->object);
      subscriber->updateLargest(*largest);
    }
    auto pubGroupOrder = subRes.value()->subscribeOk().groupOrder;
    forwarder->setGroupOrder(pubGroupOrder);

    // Store upstream delivery timeout in forwarder
    auto deliveryTimeout = MoQSession::getDeliveryTimeoutIfPresent(
        subRes.value()->subscribeOk().params, sessionVersion.value());

    // Add delivery timeout to downstream subscriber explicitly as this is the
    // first subscriber. Forwarder can add it to subsequent subscribers
    if (deliveryTimeout && *deliveryTimeout > 0) {
      forwarder->setDeliveryTimeout(*deliveryTimeout);
      subscriber->setParam(
          {folly::to_underlying(TrackRequestParamKey::DELIVERY_TIMEOUT),
           *deliveryTimeout});
    }

    subscriber->setPublisherGroupOrder(pubGroupOrder);
    auto it = subscriptions_.find(subReq.fullTrackName);
    // There are cases that remove the subscription like failing to
    // publish a datagram that was received before the subscribeOK
    // and then gets flushed
    if (it == subscriptions_.end()) {
      XLOG(ERR) << "Subscription is GONE, returning exception";
      co_yield folly::coro::co_error(
          std::runtime_error("subscription is gone"));
    }
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
    bool forwarding =
        subscriptionIt->second.forwarder->numForwardingSubscribers() > 0;
    auto subscriber = subscriptionIt->second.forwarder->addSubscriber(
        std::move(session), subReq, std::move(consumer));
    if (!forwarding &&
        subscriptionIt->second.forwarder->numForwardingSubscribers() > 0) {
      subscriptionIt->second.handle->subscribeUpdate(
          {RequestID(0),
           subscriptionIt->second.requestID,
           kLocationMin,
           kLocationMax.group,
           kDefaultPriority,
           /*forward=*/true,
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
  auto subscriptionIt = subscriptions_.find(forwarder->fullTrackName());
  if (subscriptionIt == subscriptions_.end()) {
    return;
  }
  auto& subscription = subscriptionIt->second;

  if (!subscription.handle) {
    // Handle is null - publisher terminated via FilterConsumer
    XLOG(INFO) << "Publisher terminated for " << subscriptionIt->first;
    subscriptions_.erase(subscriptionIt);
    return;
  }

  // Handle exists - just last subscriber left
  XLOG(INFO) << "Last subscriber removed for " << subscriptionIt->first;
  if (subscription.isPublish) {
    // if it's publish, don't unsubscribe, just subscribeUpdate forward=false
    XLOG(DBG1) << "Updating upstream subscription forward=false";
    subscription.handle->subscribeUpdate(
        {RequestID(0),
         subscription.requestID,
         kLocationMin,
         kLocationMax.group,
         kDefaultPriority,
         /*forward=*/false,
         {}});
  } else {
    subscription.handle->unsubscribe();
    XLOG(DBG4) << "Erasing subscription to " << subscriptionIt->first;
    subscriptions_.erase(subscriptionIt);
  }
}

void MoQRelay::forwardChanged(MoQForwarder* forwarder) {
  auto subscriptionIt = subscriptions_.find(forwarder->fullTrackName());
  if (subscriptionIt == subscriptions_.end()) {
    return;
  }
  auto& subscription = subscriptionIt->second;
  if (!subscription.promise.isFulfilled()) {
    // Ignore: it's the first subscriber, forward update not needed
    return;
  }
  XLOG(INFO) << "Updating forward for " << subscriptionIt->first
             << " numForwardingSubs=" << forwarder->numForwardingSubscribers();

  subscription.handle->subscribeUpdate(
      {RequestID(0),
       subscription.requestID,
       kLocationMin,
       kLocationMax.group,
       kDefaultPriority,
       /*forward=*/forwarder->numForwardingSubscribers() > 0,
       {}});
}

} // namespace moxygen

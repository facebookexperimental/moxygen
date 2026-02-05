/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/relay/MoQForwarder.h"
#include <folly/experimental/coro/BlockingWait.h>

namespace moxygen {

// Helper to clone payload
static Payload maybeClone(const Payload& payload) {
  return payload ? payload->clone() : nullptr;
}

// RAII guard for pendingOps_ counter
class PendingOpGuard {
 public:
  explicit PendingOpGuard(std::atomic<uint64_t>* counter) : counter_(counter) {
    counter_->fetch_add(1, std::memory_order_relaxed);
  }

  ~PendingOpGuard() {
    if (counter_) {
      counter_->fetch_sub(1, std::memory_order_release);
    }
  }

  // Movable but not copyable
  PendingOpGuard(PendingOpGuard&& other) noexcept : counter_(other.counter_) {
    other.counter_ = nullptr;
  }

  PendingOpGuard(const PendingOpGuard&) = delete;
  PendingOpGuard& operator=(const PendingOpGuard&) = delete;
  PendingOpGuard& operator=(PendingOpGuard&&) = delete;

 private:
  std::atomic<uint64_t>* counter_;
};

// Helper struct that clones Payload on copy
struct CloneablePayload {
  Payload payload;

  CloneablePayload(Payload p) : payload(std::move(p)) {}

  CloneablePayload(const CloneablePayload& other)
      : payload(other.payload ? other.payload->clone() : nullptr) {}

  CloneablePayload(CloneablePayload&&) = default;
  CloneablePayload& operator=(const CloneablePayload&) = delete;
  CloneablePayload& operator=(CloneablePayload&&) = default;

  operator const Payload&() const {
    return payload;
  }
};

// Static member definition for thread-local executor cache
folly::ThreadLocal<std::vector<MoQForwarder::ExecutorForwarderPair>>
    MoQForwarder::executorForwarderCache_;

// Template implementations

template <typename Fn>
folly::Expected<folly::Unit, MoQPublishError> MoQForwarder::forEachSubscriber(
    Fn&& fn,
    std::optional<AbsoluteLocation> location) {
  if (location) {
    updateLargest(location->group, location->object);
  }

  for (auto subscriberIt = subscribers_.begin();
       subscriberIt != subscribers_.end();) {
    const auto& sub = subscriberIt->second;
    subscriberIt++;
    XCHECK(sub->forwarder) << "Subscriber in map must have valid forwarder";
    fn(sub);
  }

  // Cascade to children asynchronously
  for (auto& [exec, child] : nextForwarders_) {
    exec->add([child,
               fn = fn,
               location,
               guard = PendingOpGuard(pendingOps_)]() mutable {
      child->forEachSubscriber(std::move(fn), location);
      // guard destructor will decrement pendingOps_
    });
  }

  // Check if empty after iteration - subscribers may have been removed in loop
  if (subscribers_.empty() && nextForwarders_.empty()) {
    return folly::makeUnexpected(
        MoQPublishError(MoQPublishError::CANCELLED, "No subscribers"));
  }
  return folly::unit;
}

template <typename Fn>
folly::Expected<folly::Unit, MoQPublishError>
MoQForwarder::SubgroupForwarder::forEachSubscriberSubgroup(
    Fn&& fn,
    std::optional<AbsoluteLocation> location,
    bool makeNew,
    const std::string& callsite) {
  auto identifier = identifier_;
  auto priority = priority_;
  return forwarder_.forEachSubscriber(
      [fn = std::forward<Fn>(fn), makeNew, identifier, priority, callsite](
          const std::shared_ptr<Subscriber>& sub) {
        if (sub->forwarder->largest_ && sub->forwarder->checkRange(*sub)) {
          auto subgroupConsumerIt = sub->subgroups.find(identifier);
          if (subgroupConsumerIt == sub->subgroups.end()) {
            if (!sub->checkShouldForward()) {
              // If shouldForward == false, we shouldn't be creating any
              // subgroups.
              return;
            }
            if (!makeNew) {
              XLOG(DBG2) << "skipping creating subgroup for sub=" << sub.get();
              return;
            }
            XCHECK(sub->trackConsumer);
            XLOG(DBG2) << "Making new subgroup for consumer="
                       << sub->trackConsumer << " " << callsite;
            auto res = sub->trackConsumer->beginSubgroup(
                identifier.group, identifier.subgroup, priority);
            if (res.hasError()) {
              sub->forwarder->removeSubscriberOnError(
                  *sub, res.error(), callsite);
            } else {
              auto emplaceRes = sub->subgroups.emplace(identifier, res.value());
              subgroupConsumerIt = emplaceRes.first;
            }
          }
          if (subgroupConsumerIt != sub->subgroups.end()) {
            if (!sub->checkShouldForward()) {
              // If we're attempting to send anything on an existing subgroup
              // when forward == false, then we reset the stream, so that we
              // don't end up with "holes" in the subgroup. If, at some point in
              // the future, we set forward = true, then we'll create a new
              // stream for the subgroup.
              subgroupConsumerIt->second->reset(
                  ResetStreamErrorCode::INTERNAL_ERROR);
              closeSubgroupForSubscriber(
                  sub,
                  identifier,
                  "SubgroupForwarder::forEachSubscriberSubgroup");
            } else {
              fn(sub, subgroupConsumerIt->second);
            }
          }
        }
      },
      location);
  // Check if empty after iteration - subscribers may have been removed in loop
  if (forwarder_.subscribers_.empty()) {
    return folly::makeUnexpected(
        MoQPublishError(MoQPublishError::CANCELLED, "No subscribers"));
  }
  return folly::unit;
}

// MoQForwarder implementation

MoQForwarder::MoQForwarder(
    FullTrackName ftn,
    folly::Executor* executor,
    std::optional<AbsoluteLocation> largest,
    size_t maxSubscribersPerForwarder)
    : fullTrackName_(std::move(ftn)),
      largest_(std::move(largest)),
      maxSubscribersPerForwarder_(maxSubscribersPerForwarder),
      executor_(executor),
      root_(this) {
  XCHECK(executor_) << "Executor cannot be null";
  // Root forwarder - point to own storage
  pendingOps_ = &pendingOpsStorage_;
  totalSubscribers_ = &totalSubscribersStorage_;
  forwardingSubscribersAtomic_ = &forwardingSubscribersStorage_;
  registerForCurrentThread();
}

MoQForwarder::MoQForwarder(MoQForwarder* parent, folly::Executor* executor)
    : fullTrackName_(parent->fullTrackName_),
      largest_(parent->largest_),
      maxSubscribersPerForwarder_(parent->maxSubscribersPerForwarder_),
      executor_(executor),
      parent_(parent),
      root_(parent->root_) {
  XCHECK(executor_) << "Executor cannot be null";
  XCHECK(parent_) << "Parent cannot be null for child forwarder";
  groupOrder_ = parent->groupOrder_;
  upstreamDeliveryTimeout_ = parent->upstreamDeliveryTimeout_;
  // Child forwarder - point to root's storage
  pendingOps_ = root_->pendingOps_;
  totalSubscribers_ = root_->totalSubscribers_;
  forwardingSubscribersAtomic_ = root_->forwardingSubscribersAtomic_;
  registerForCurrentThread();
}

std::shared_ptr<MoQForwarder> MoQForwarder::createChildForwarder(
    folly::Executor* childExecutor) {
  return std::shared_ptr<MoQForwarder>(new MoQForwarder(this, childExecutor));
}

void MoQForwarder::setDeliveryTimeout(uint64_t timeout) {
  upstreamDeliveryTimeout_ = std::chrono::milliseconds(timeout);
}

void MoQForwarder::setCallback(std::shared_ptr<Callback> callback) {
  callback_ = std::move(callback);
}

folly::coro::Task<std::shared_ptr<MoQForwarder::Subscriber>>
MoQForwarder::addSubscriberAsync(
    std::shared_ptr<MoQSession> session,
    const SubscribeRequest& subReq,
    std::shared_ptr<TrackConsumer> consumer,
    folly::Executor* subscriberExecutor) {
  XCHECK(subscriberExecutor) << "Subscriber executor cannot be null";
  auto trackAlias = trackAlias_.value_or(subReq.requestID.value);
  XCHECK(consumer);
  consumer->setTrackAlias(trackAlias);

  auto subscribeOk = SubscribeOk{
      subReq.requestID,
      trackAlias,
      std::chrono::milliseconds(0),
      MoQSession::resolveGroupOrder(groupOrder_, subReq.groupOrder),
      largest_};

  if (upstreamDeliveryTimeout_.count() > 0) {
    subscribeOk.params.insertParam(
        {folly::to_underlying(TrackRequestParamKey::DELIVERY_TIMEOUT),
         static_cast<uint64_t>(upstreamDeliveryTimeout_.count())});
  }

  co_return co_await addSubscriberInternal(
      std::move(session),
      std::move(subscribeOk),
      subReq.requestID,
      toSubscribeRange(subReq, largest_),
      std::move(consumer),
      subscriberExecutor,
      subReq.forward);
}

folly::coro::Task<std::shared_ptr<MoQForwarder::Subscriber>>
MoQForwarder::addSubscriberAsync(
    std::shared_ptr<MoQSession> session,
    const PublishRequest& pub,
    folly::Executor* subscriberExecutor) {
  XCHECK(subscriberExecutor) << "Subscriber executor cannot be null";
  auto subscribeOk = SubscribeOk{
      pub.requestID,
      pub.trackAlias,
      std::chrono::milliseconds(0),
      pub.groupOrder,
      largest_};

  co_return co_await addSubscriberInternal(
      std::move(session),
      std::move(subscribeOk),
      pub.requestID,
      SubscribeRange{{0, 0}, kLocationMax},
      nullptr,
      subscriberExecutor,
      pub.forward);
}

folly::coro::Task<std::shared_ptr<MoQForwarder::Subscriber>>
MoQForwarder::addSubscriberInternal(
    std::shared_ptr<MoQSession> session,
    SubscribeOk subscribeOk,
    RequestID requestID,
    SubscribeRange range,
    std::shared_ptr<TrackConsumer> consumer,
    folly::Executor* subscriberExecutor,
    bool shouldForward) {
  XLOG(DBG1) << "addSubscriberInternal requestID=" << requestID
             << " subscriberExecutor=" << subscriberExecutor
             << " executor_=" << executor_ << " draining_=" << draining_
             << " track=" << fullTrackName_;
  if (draining_) {
    XLOG(ERR) << "addSubscriber called on draining track";
    co_return nullptr;
  }

  // Route to child if different executor
  if (subscriberExecutor != executor_) {
    XLOG(DBG1) << "Routing to child forwarder with different executor";
    auto it = nextForwarders_.find(subscriberExecutor);
    if (it == nextForwarders_.end()) {
      auto child = createChildForwarder(subscriberExecutor);
      it = nextForwarders_.emplace(subscriberExecutor, child).first;
    }
    // XCHECK: Root can have multiple executor-based children
    // Non-root can only have one child (overflow on same executor)
    if (parent_) {
      XCHECK(nextForwarders_.size() == 1)
          << "Non-root forwarder can only have one child";
    }

    // Track pending operation
    PendingOpGuard guard(pendingOps_);
    co_return co_await it->second->addSubscriberInternal(
        std::move(session),
        std::move(subscribeOk),
        requestID,
        range,
        std::move(consumer),
        subscriberExecutor,
        shouldForward);
  }

  // Same executor - traverse chain to find forwarder with capacity
  MoQForwarder* target = this;
  while (!target->hasCapacity()) {
    auto it = target->nextForwarders_.find(executor_);
    if (it == target->nextForwarders_.end()) {
      // Create child and use it
      auto child = target->createChildForwarder(executor_);
      target->nextForwarders_.emplace(executor_, child);
      target = child.get();
      break;
    }
    // XCHECK: For overflow, we should only have one child on same executor
    XCHECK(!target->parent_ || target->nextForwarders_.size() == 1)
        << "Overflow forwarder should only have one child";
    target = it->second.get();
  }

  // Add to target forwarder
  auto sessionPtr = session.get();
  auto subscriber = std::make_shared<Subscriber>(
      target,
      std::move(subscribeOk),
      std::move(session),
      requestID,
      range,
      std::move(consumer),
      shouldForward);

  target->subscribers_.emplace(sessionPtr, subscriber);

  // Update atomic counters
  target->totalSubscribers_->fetch_add(1, std::memory_order_relaxed);

  if (shouldForward) {
    target->addForwardingSubscriber();
  }

  co_return subscriber;
}

std::shared_ptr<MoQForwarder::Subscriber> MoQForwarder::addSubscriber(
    std::shared_ptr<MoQSession> session,
    const SubscribeRequest& subReq,
    std::shared_ptr<TrackConsumer> consumer) {
  // Synchronous version - use executor_ as subscriberExecutor
  auto task = addSubscriberAsync(session, subReq, consumer, executor_);
  return folly::coro::blockingWait(std::move(task));
}

std::shared_ptr<MoQForwarder::Subscriber> MoQForwarder::addSubscriber(
    std::shared_ptr<MoQSession> session,
    const PublishRequest& pub) {
  // Synchronous version - use executor_ as subscriberExecutor
  auto task = addSubscriberAsync(session, pub, executor_);
  return folly::coro::blockingWait(std::move(task));
}

void MoQForwarder::registerForCurrentThread() {
  auto& cache = *executorForwarderCache_;

  // Check if this executor is already registered on this thread
  for (const auto& pair : cache) {
    if (pair.executor == executor_) {
      return; // Already registered
    }
  }

  // Register as the first forwarder for this executor on this thread
  cache.push_back({executor_, this});
}

MoQForwarder* MoQForwarder::currentForwarderForExecutor(folly::Executor* exec) {
  auto& cache = *executorForwarderCache_;

  for (const auto& pair : cache) {
    if (pair.executor == exec) {
      return pair.forwarder;
    }
  }

  return nullptr;
}

std::shared_ptr<MoQForwarder::Subscriber>
MoQForwarder::findSubscriberForSession(
    const std::shared_ptr<MoQSession>& session) const {
  XCHECK(parent_ == nullptr)
      << "findSubscriberForSession must be called on root";

  auto sessionExec = session->getExecutor();

  MoQForwarder* searchStart;
  if (sessionExec == executor_) {
    // Session is on root's executor - search root's chain
    searchStart = const_cast<MoQForwarder*>(this);
  } else {
    // Session is on different executor - find via thread-local cache
    searchStart = currentForwarderForExecutor(sessionExec);
  }

  // Walk the chain for this executor (handles overflow with 200+ subscribers)
  MoQForwarder* current = searchStart;
  while (current) {
    auto subIt = current->subscribers_.find(session.get());
    if (subIt != current->subscribers_.end()) {
      return subIt->second;
    }

    // Follow overflow chain - safe because we're on the correct executor
    auto it = current->nextForwarders_.find(current->executor_);
    if (it != current->nextForwarders_.end()) {
      current = it->second.get();
    } else {
      break;
    }
  }

  return nullptr;
}

folly::Expected<SubscribeRange, FetchError> MoQForwarder::resolveJoiningFetch(
    const std::shared_ptr<MoQSession>& session,
    const JoiningFetch& joining) const {
  auto subscriber = findSubscriberForSession(session);
  if (!subscriber) {
    XLOG(ERR) << "Session not found";
    return folly::makeUnexpected(
        FetchError{
            RequestID(0),
            FetchErrorCode::TRACK_NOT_EXIST,
            "Session has no active subscribe"});
  }

  if (subscriber->requestID != joining.joiningRequestID) {
    XLOG(ERR) << joining.joiningRequestID
              << " does not name a Subscribe for this track";
    return folly::makeUnexpected(
        FetchError{
            RequestID(0),
            FetchErrorCode::INTERNAL_ERROR,
            "Incorrect RequestID for Track"});
  }

  if (!subscriber->subscribeOk().largest) {
    // No content exists, fetch error
    // Relay caller verifies upstream SubscribeOK has been processed before
    // calling resolveJoiningFetch()
    return folly::makeUnexpected(
        FetchError{RequestID(0), FetchErrorCode::INTERNAL_ERROR, "No largest"});
  }

  CHECK(
      joining.fetchType == FetchType::RELATIVE_JOINING ||
      joining.fetchType == FetchType::ABSOLUTE_JOINING);

  auto& largest = *subscriber->subscribeOk().largest;
  if (joining.fetchType == FetchType::RELATIVE_JOINING) {
    AbsoluteLocation start{largest};
    start.group -=
        (start.group >= joining.joiningStart) ? joining.joiningStart : 0;
    start.object = 0;
    return SubscribeRange{start, {largest.group, largest.object + 1}};
  } else {
    // ABSOLUTE_JOINING
    AbsoluteLocation start{joining.joiningStart, 0};
    return SubscribeRange{start, {largest.group, largest.object + 1}};
  }
}

void MoQForwarder::drainSubscriber(
    const std::shared_ptr<MoQSession>& session,
    PublishDone pubDone,
    const std::string& callsite) {
  XLOG(DBG1) << __func__ << " from " << callsite
             << " session=" << session.get();
  auto subIt = subscribers_.find(session.get());
  if (subIt == subscribers_.end()) {
    XLOG(WARNING) << "Session not found in drainSubscriber from " << callsite
                  << " sess=" << session;
    return;
  }

  auto& subscriber = *subIt->second;

  // Forward the publishDone message WITHOUT resetting subgroups
  pubDone.requestID = subscriber.requestID;
  subscriber.trackConsumer->publishDone(std::move(pubDone));

  // If no open subgroups, delegate to removeSubscriberIt for cleanup
  if (subscriber.subgroups.empty()) {
    // Pass std::nullopt for pubDone since we already forwarded it above
    removeSubscriberIt(subIt, std::nullopt, callsite);
    return;
  }

  // Otherwise, mark receivedPublishDone and wait for subgroups to close
  subscriber.receivedPublishDone_ = true;
  XLOG(DBG1) << "Subscriber " << &subscriber << " is draining with "
             << subscriber.subgroups.size() << " open subgroups";
}

void MoQForwarder::removeSubscriber(
    const std::shared_ptr<MoQSession>& session,
    std::optional<PublishDone> pubDone,
    const std::string& callsite) {
  XLOG(DBG1) << __func__ << " from " << callsite
             << " session=" << session.get();
  auto subIt = subscribers_.find(session.get());
  if (subIt == subscribers_.end()) {
    XLOG(WARNING) << "Session not found in removeSubscriber from " << callsite
                  << " sess=" << session;
    return;
  }
  removeSubscriberIt(subIt, std::move(pubDone), callsite);
}

bool MoQForwarder::empty() const {
  return subscribers_.empty() && nextForwarders_.empty();
}

bool MoQForwarder::allChildrenEmpty() const {
  for (const auto& [exec, child] : nextForwarders_) {
    if (!child->empty() || !child->allChildrenEmpty()) {
      return false;
    }
  }
  return true;
}

void MoQForwarder::checkAndFireOnEmpty() {
  if (subscribers_.empty()) {
    // Only root checks subgroups, children defer to root
    if (root_ != this) {
      // Child forwarder - just check if we and our children are empty
      if (allChildrenEmpty()) {
        // Wait for pending ops before notifying parent
        if (pendingOps_->load(std::memory_order_acquire) > 0) {
          // Reschedule check
          executor_->add(
              [self = shared_from_this()]() { self->checkAndFireOnEmpty(); });
          return;
        }

        // Notify parent that we're empty
        if (parent_) {
          parent_->executor_->add([parent = parent_, self = this]() {
            parent->onChildEmptied(self);
          });
        }
      }
    } else {
      // Root forwarder - check subgroups too
      if (subgroups_.empty()) {
        // Since empty children remove themselves via onChildEmptied(), we can
        // simply check if nextForwarders_ is empty rather than iterating it
        if (nextForwarders_.empty()) {
          // Wait for pending ops before firing callback
          if (pendingOps_->load(std::memory_order_acquire) > 0) {
            // Reschedule check
            executor_->add(
                [self = shared_from_this()]() { self->checkAndFireOnEmpty(); });
            return;
          }

          // Fire callback
          if (callback_) {
            invokeCallback(
                [](Callback* cb, MoQForwarder* fwd) { cb->onEmpty(fwd); });
          }
        }
      } else {
        XLOG(DBG4) << subgroups_.size() << " subgroups remaining";
      }
    }
  }
}

void MoQForwarder::onChildEmptied(MoQForwarder* child) {
  // Find and remove child
  for (auto it = nextForwarders_.begin(); it != nextForwarders_.end(); ++it) {
    if (it->second.get() == child) {
      nextForwarders_.erase(it);
      break;
    }
  }

  // Check if we're empty too
  checkAndFireOnEmpty();
}

void MoQForwarder::invokeCallback(
    std::function<void(Callback*, MoQForwarder*)> fn) {
  // Only root has callback
  if (!root_->callback_) {
    return;
  }

  // Schedule on root's executor, or invoke directly if already on it
  if (executor_ == root_->executor_) {
    if (root_->callback_) {
      fn(root_->callback_.get(), root_);
    }
  } else {
    root_->executor_->add([fn, root = root_]() {
      if (root->callback_) {
        fn(root->callback_.get(), root);
      }
    });
  }
}

void MoQForwarder::removeSubscriberIt(
    folly::F14FastMap<MoQSession*, std::shared_ptr<Subscriber>>::iterator subIt,
    std::optional<PublishDone> pubDone,
    const std::string& callsite) {
  auto& subscriber = *subIt->second;
  XLOG(DBG1) << __func__ << " from=" << callsite
             << " subscriber=" << &subscriber
             << " shouldForward=" << subscriber.shouldForward
             << " receivedPublishDone=" << subscriber.receivedPublishDone_
             << " subgroups.size()=" << subscriber.subgroups.size();

  XLOG(DBG1) << "Resetting " << subscriber.subgroups.size()
             << " open subgroups for subscriber=" << &subscriber;
  for (auto& subgroup : subscriber.subgroups) {
    XLOG(DBG3) << "Resetting subgroup group=" << subgroup.first.group
               << " subgroup=" << subgroup.first.subgroup;
    subgroup.second->reset(ResetStreamErrorCode::CANCELLED);
  }
  if (pubDone) {
    pubDone->requestID = subscriber.requestID;
    subscriber.trackConsumer->publishDone(std::move(*pubDone));
  }

  bool wasForwarding = subscriber.shouldForward;

  // Null out forwarder pointer to prevent UAF if subscriber outlives forwarder
  subscriber.forwarder = nullptr;
  subscribers_.erase(subIt);

  // Update atomic counter
  totalSubscribers_->fetch_sub(1, std::memory_order_relaxed);

  if (wasForwarding) {
    if (totalSubscribers_->load(std::memory_order_relaxed) == 0) {
      // don't trigger a forwardUpdated callback here, we will trigger onEmpty
      forwardingSubscribersAtomic_->fetch_sub(1, std::memory_order_relaxed);
    } else {
      removeForwardingSubscriber();
    }
  }

  XLOG(DBG1) << "subscribers_.size()=" << subscribers_.size();
  checkAndFireOnEmpty();
}

void MoQForwarder::updateLargest(uint64_t group, uint64_t object) {
  AbsoluteLocation now{group, object};
  if (!largest_ || now > *largest_) {
    largest_ = now;
  }
}

void MoQForwarder::setLargest(AbsoluteLocation largest) {
  largest_ = largest;
}

bool MoQForwarder::checkRange(const Subscriber& sub) {
  XCHECK(largest_);
  if (*largest_ < sub.range.start) {
    // future
    return false;
  } else if (*largest_ > sub.range.end) {
    // now past, send publishDone
    // TOOD: maybe this is too early for a relay.
    XLOG(DBG4) << "removeSubscriber from checkRange";
    removeSubscriber(
        sub.session,
        PublishDone{
            sub.requestID,
            PublishDoneStatusCode::SUBSCRIPTION_ENDED,
            0, // filled in by session
            ""},
        "checkRange");
    return false;
  }
  return true;
}

void MoQForwarder::removeSubscriberOnError(
    const Subscriber& sub,
    const MoQPublishError& err,
    const std::string& callsite) {
  XLOG(ERR) << "Removing subscriber after error in " << callsite
            << " err=" << err.what();
  removeSubscriber(
      sub.session,
      PublishDone{
          sub.requestID,
          PublishDoneStatusCode::INTERNAL_ERROR,
          0, // filled in by session
          err.what()},
      callsite);
}

folly::Expected<folly::Unit, MoQPublishError> MoQForwarder::setTrackAlias(
    TrackAlias alias) {
  trackAlias_ = alias;
  return folly::unit;
}

folly::Expected<std::shared_ptr<SubgroupConsumer>, MoQPublishError>
MoQForwarder::beginSubgroup(
    uint64_t groupID,
    uint64_t subgroupID,
    Priority priority) {
  auto subgroupForwarder =
      std::make_shared<SubgroupForwarder>(*this, groupID, subgroupID, priority);
  SubgroupIdentifier subgroupIdentifier({groupID, subgroupID});
  auto res = forEachSubscriber(
      [=](const std::shared_ptr<Subscriber>& sub) {
        if (!sub->forwarder->checkRange(*sub) || !sub->checkShouldForward()) {
          return;
        }
        auto sgRes =
            sub->trackConsumer->beginSubgroup(groupID, subgroupID, priority);
        if (sgRes.hasError()) {
          sub->forwarder->removeSubscriberOnError(
              *sub, sgRes.error(), "beginSubgroup");
        } else {
          sub->subgroups[subgroupIdentifier] = sgRes.value();
        }
      },
      AbsoluteLocation{groupID, 0});
  if (res.hasError()) {
    return folly::makeUnexpected(res.error());
  }
  subgroups_.emplace(subgroupIdentifier, subgroupForwarder);
  return subgroupForwarder;
}

folly::Expected<folly::SemiFuture<folly::Unit>, MoQPublishError>
MoQForwarder::awaitStreamCredit() {
  return folly::makeSemiFuture();
}

folly::Expected<folly::Unit, MoQPublishError> MoQForwarder::objectStream(
    const ObjectHeader& header,
    Payload payload) {
  CloneablePayload cloneable(std::move(payload));
  return forEachSubscriber(
      [header, cloneable](const std::shared_ptr<Subscriber>& sub) {
        if (!sub->forwarder->checkRange(*sub) || !sub->checkShouldForward()) {
          return;
        }
        sub->trackConsumer->objectStream(header, maybeClone(cloneable.payload))
            .onError([sub](const auto& err) {
              sub->forwarder->removeSubscriberOnError(
                  *sub, err, "objectStream");
            });
      },
      AbsoluteLocation{header.group, header.id});
}

folly::Expected<folly::Unit, MoQPublishError> MoQForwarder::datagram(
    const ObjectHeader& header,
    Payload payload) {
  CloneablePayload cloneable(std::move(payload));
  return forEachSubscriber(
      [header, cloneable](const std::shared_ptr<Subscriber>& sub) {
        if (!sub->forwarder->checkRange(*sub) || !sub->checkShouldForward()) {
          return;
        }
        sub->trackConsumer->datagram(header, maybeClone(cloneable.payload))
            .onError([sub](const auto& err) {
              sub->forwarder->removeSubscriberOnError(*sub, err, "datagram");
            });
      },
      AbsoluteLocation{header.group, header.id});
}

folly::Expected<folly::Unit, MoQPublishError> MoQForwarder::publishDone(
    PublishDone pubDone) {
  XLOG(DBG1) << __func__ << " pubDone reason=" << pubDone.reasonPhrase;
  draining_ = true;
  forEachSubscriber([pubDone](const std::shared_ptr<Subscriber>& sub) {
    sub->forwarder->drainSubscriber(
        sub->session,
        PublishDone{
            sub->requestID,
            pubDone.statusCode,
            0, // filled in by session
            pubDone.reasonPhrase},
        "publishDone");
  });
  // Cleanup/draining operation - succeed even with no subscribers
  return folly::unit;
}

void MoQForwarder::addForwardingSubscriber() {
  auto prev =
      forwardingSubscribersAtomic_->fetch_add(1, std::memory_order_relaxed);
  if (prev == 0) {
    invokeCallback(
        [](Callback* cb, MoQForwarder* fwd) { cb->forwardChanged(fwd); });
  }
}

void MoQForwarder::removeForwardingSubscriber() {
  auto prev =
      forwardingSubscribersAtomic_->fetch_sub(1, std::memory_order_relaxed);
  if (prev == 1) {
    invokeCallback(
        [](Callback* cb, MoQForwarder* fwd) { cb->forwardChanged(fwd); });
  }
}

// MoQForwarder::Subscriber implementation

MoQForwarder::Subscriber::Subscriber(
    MoQForwarder* f,
    SubscribeOk ok,
    std::shared_ptr<MoQSession> s,
    RequestID sid,
    SubscribeRange r,
    std::shared_ptr<TrackConsumer> tc,
    bool shouldForwardIn)
    : SubscriptionHandle(std::move(ok)),
      session(std::move(s)),
      requestID(sid),
      range(r),
      trackConsumer(std::move(tc)),
      forwarder(f),
      shouldForward(shouldForwardIn) {}

void MoQForwarder::Subscriber::setPublisherGroupOrder(
    GroupOrder pubGroupOrder) {
  subscribeOk_->groupOrder =
      MoQSession::resolveGroupOrder(pubGroupOrder, subscribeOk_->groupOrder);
}

void MoQForwarder::Subscriber::setSubscribeOkLargest(AbsoluteLocation largest) {
  subscribeOk_->largest = largest;
}

void MoQForwarder::Subscriber::setParam(const TrackRequestParameter& param) {
  for (size_t i = 0; i < subscribeOk_->params.size(); i++) {
    const auto& existingParam = subscribeOk_->params.at(i);
    if (existingParam.key == param.key) {
      subscribeOk_->params.modifyParam(
          i, param.asString, param.asUint64, param.asAuthToken);
      return;
    }
  }
  auto result = subscribeOk_->params.insertParam(param);
  if (result.hasError()) {
    XLOG(ERR) << "setParam: param not allowed for SubscribeOk, key="
              << param.key;
  }
}

folly::coro::Task<folly::Expected<SubscribeUpdateOk, SubscribeUpdateError>>
MoQForwarder::Subscriber::subscribeUpdate(SubscribeUpdate subscribeUpdate) {
  // Validation:
  // - Start location can be updated
  // - End location can increase or decrease
  // - For bounded subscriptions (endGroup > 0), end must be >= start
  // - Forward state is optional and only updated if explicitly provided

  // Only update start if provided
  if (subscribeUpdate.start.has_value()) {
    range.start = *subscribeUpdate.start;
  }

  // Only update end if provided
  if (subscribeUpdate.endGroup.has_value()) {
    AbsoluteLocation newEnd{*subscribeUpdate.endGroup, 0};

    // Validate: for bounded end, the end must not be less than the start
    if (*subscribeUpdate.endGroup > 0 && range.start >= newEnd) {
      XLOG(ERR) << "Invalid subscribeUpdate: end Location " << newEnd
                << " is less than start " << range.start;
      session->close(SessionCloseErrorCode::PROTOCOL_VIOLATION);
      co_return folly::makeUnexpected(SubscribeUpdateError(
          subscribeUpdate.requestID,
          RequestErrorCode::INVALID_RANGE,
          "End Location is less than start"));
    }
    range.end = newEnd;
  }

  auto wasForwarding = shouldForward;
  // Only update forward state if explicitly provided (per draft 15+)
  if (subscribeUpdate.forward.has_value()) {
    shouldForward = *subscribeUpdate.forward;
  }

  if (forwarder) {
    if (shouldForward && !wasForwarding) {
      forwarder->addForwardingSubscriber();
    } else if (wasForwarding && !shouldForward) {
      forwarder->removeForwardingSubscriber();
    }
  }

  co_return SubscribeUpdateOk{.requestID = subscribeUpdate.requestID};
}

void MoQForwarder::Subscriber::unsubscribe() {
  XLOG(ERR) << "SubscriptionHandle::unsubscribe() called for requestID="
            << requestID << " session=" << session.get()
            << " receivedPublishDone=" << receivedPublishDone_
            << " shouldForward=" << shouldForward;
  if (forwarder) {
    forwarder->removeSubscriber(session, std::nullopt, "unsubscribe");
  }
}

bool MoQForwarder::Subscriber::checkShouldForward() {
  if (!shouldForward) {
    XLOG(DBG6) << "shouldForward is false of requestID " << requestID;
  }
  return shouldForward;
}

// MoQForwarder::SubgroupForwarder implementation

MoQForwarder::SubgroupForwarder::SubgroupForwarder(
    MoQForwarder& forwarder,
    uint64_t group,
    uint64_t subgroup,
    Priority priority)
    : forwarder_(forwarder),
      identifier_{group, subgroup},
      priority_(priority) {}

void MoQForwarder::SubgroupForwarder::closeSubgroupForSubscriber(
    const std::shared_ptr<Subscriber>& sub,
    const SubgroupIdentifier& identifier,
    const std::string& callsite) {
  sub->subgroups.erase(identifier);
  // If this subscriber is draining and this was the last subgroup, remove it
  if (sub->shouldRemoveSubscriber()) {
    sub->forwarder->removeSubscriber(sub->session, std::nullopt, callsite);
  }
}

folly::Expected<folly::Unit, MoQPublishError>
MoQForwarder::SubgroupForwarder::removeSubgroupAndCheckEmpty() {
  forwarder_.subgroups_.erase(identifier_);
  forwarder_.checkAndFireOnEmpty();
  return folly::unit;
}

template <typename T>
folly::Expected<T, MoQPublishError>
MoQForwarder::SubgroupForwarder::cleanupOnError(
    const folly::Expected<T, MoQPublishError>& result) {
  if (result.hasError()) {
    XLOG(DBG1) << "Removing subgroup after error: " << result.error().what();
    removeSubgroupAndCheckEmpty();
  }
  return result;
}

folly::Expected<folly::Unit, MoQPublishError>
MoQForwarder::SubgroupForwarder::object(
    uint64_t objectID,
    Payload payload,
    Extensions extensions,
    bool finSubgroup) {
  if (currentObjectLength_) {
    return folly::makeUnexpected(MoQPublishError(
        MoQPublishError::API_ERROR, "Still publishing previous object"));
  }
  CloneablePayload cloneable(std::move(payload));
  auto self = shared_from_this();
  auto identifier = identifier_;
  auto res = forEachSubscriberSubgroup(
      [objectID, finSubgroup, extensions, cloneable, self, identifier](
          const std::shared_ptr<Subscriber>& sub,
          const std::shared_ptr<SubgroupConsumer>& subgroupConsumer) {
        subgroupConsumer
            ->object(
                objectID,
                maybeClone(cloneable.payload),
                extensions,
                finSubgroup)
            .onError([sub](const auto& err) {
              sub->forwarder->removeSubscriberOnError(
                  *sub, err, "SubgroupForwarder::object");
            });
        if (finSubgroup) {
          closeSubgroupForSubscriber(
              sub, identifier, "SubgroupForwarder::object");
        }
      },
      AbsoluteLocation{identifier_.group, objectID});
  if (finSubgroup) {
    return removeSubgroupAndCheckEmpty();
  }
  // Data operation - propagate CANCELLED if no subscribers
  return cleanupOnError(res);
}

folly::Expected<folly::Unit, MoQPublishError>
MoQForwarder::SubgroupForwarder::beginObject(
    uint64_t objectID,
    uint64_t length,
    Payload initialPayload,
    Extensions extensions) {
  // TODO: use a shared class for object publish state validation
  if (currentObjectLength_) {
    return folly::makeUnexpected(MoQPublishError(
        MoQPublishError::API_ERROR, "Still publishing previous object"));
  }
  auto payloadLength =
      (initialPayload) ? initialPayload->computeChainDataLength() : 0;
  if (length > payloadLength) {
    currentObjectLength_ = length - payloadLength;
  }
  CloneablePayload cloneable(std::move(initialPayload));
  auto self = shared_from_this();
  return cleanupOnError(forEachSubscriberSubgroup(
      [objectID, length, extensions, cloneable, self](
          const std::shared_ptr<Subscriber>& sub,
          const std::shared_ptr<SubgroupConsumer>& subgroupConsumer) {
        subgroupConsumer
            ->beginObject(
                objectID, length, maybeClone(cloneable.payload), extensions)
            .onError([sub](const auto& err) {
              sub->forwarder->removeSubscriberOnError(
                  *sub, err, "SubgroupForwarder::beginObject");
            });
      },
      AbsoluteLocation{identifier_.group, objectID}));
}

folly::Expected<folly::Unit, MoQPublishError>
MoQForwarder::SubgroupForwarder::endOfGroup(uint64_t endOfGroupObjectID) {
  if (currentObjectLength_) {
    return folly::makeUnexpected(MoQPublishError(
        MoQPublishError::API_ERROR, "Still publishing previous object"));
  }
  auto self = shared_from_this();
  auto identifier = identifier_;
  forEachSubscriberSubgroup(
      [endOfGroupObjectID, self, identifier](
          const std::shared_ptr<Subscriber>& sub,
          const std::shared_ptr<SubgroupConsumer>& subgroupConsumer) {
        subgroupConsumer->endOfGroup(endOfGroupObjectID)
            .onError([sub](const auto& err) {
              sub->forwarder->removeSubscriberOnError(
                  *sub, err, "SubgroupForwarder::endOfGroup");
            });
        closeSubgroupForSubscriber(
            sub, identifier, "SubgroupForwarder::endOfGroup");
      },
      AbsoluteLocation{identifier_.group, endOfGroupObjectID});
  // Cleanup operation - succeed even with no subscribers
  return removeSubgroupAndCheckEmpty();
}

folly::Expected<folly::Unit, MoQPublishError>
MoQForwarder::SubgroupForwarder::endOfTrackAndGroup(
    uint64_t endOfTrackObjectID) {
  if (currentObjectLength_) {
    return folly::makeUnexpected(MoQPublishError(
        MoQPublishError::API_ERROR, "Still publishing previous object"));
  }
  auto self = shared_from_this();
  auto identifier = identifier_;
  forEachSubscriberSubgroup(
      [endOfTrackObjectID, self, identifier](
          const std::shared_ptr<Subscriber>& sub,
          const std::shared_ptr<SubgroupConsumer>& subgroupConsumer) {
        subgroupConsumer->endOfTrackAndGroup(endOfTrackObjectID)
            .onError([sub](const auto& err) {
              sub->forwarder->removeSubscriberOnError(
                  *sub, err, "SubgroupForwarder::endOfTrackAndGroup");
            });
        closeSubgroupForSubscriber(
            sub, identifier, "SubgroupForwarder::endOfTrackAndGroup");
      },
      AbsoluteLocation{identifier_.group, endOfTrackObjectID});
  // Cleanup operation - succeed even with no subscribers
  return removeSubgroupAndCheckEmpty();
}

folly::Expected<folly::Unit, MoQPublishError>
MoQForwarder::SubgroupForwarder::endOfSubgroup() {
  if (currentObjectLength_) {
    return folly::makeUnexpected(MoQPublishError(
        MoQPublishError::API_ERROR, "Still publishing previous object"));
  }
  auto self = shared_from_this();
  auto identifier = identifier_;
  forEachSubscriberSubgroup(
      [self, identifier](
          const std::shared_ptr<Subscriber>& sub,
          const std::shared_ptr<SubgroupConsumer>& subgroupConsumer) {
        subgroupConsumer->endOfSubgroup().onError([sub](const auto& err) {
          sub->forwarder->removeSubscriberOnError(
              *sub, err, "SubgroupForwarder::endOfSubgroup");
        });
        closeSubgroupForSubscriber(
            sub, identifier, "SubgroupForwarder::endOfSubgroup");
      },
      std::nullopt,
      /*makeNew=*/false,
      "endOfSubgroup");
  // Cleanup operation - succeed even with no subscribers
  return removeSubgroupAndCheckEmpty();
}

void MoQForwarder::SubgroupForwarder::reset(ResetStreamErrorCode error) {
  auto identifier = identifier_;
  forEachSubscriberSubgroup(
      [error, identifier](
          const std::shared_ptr<Subscriber>& sub,
          const std::shared_ptr<SubgroupConsumer>& subgroupConsumer) {
        subgroupConsumer->reset(error);
        closeSubgroupForSubscriber(sub, identifier, "SubgroupForwarder::reset");
      },
      std::nullopt);
  removeSubgroupAndCheckEmpty();
}

folly::Expected<ObjectPublishStatus, MoQPublishError>
MoQForwarder::SubgroupForwarder::objectPayload(
    Payload payload,
    bool finSubgroup) {
  if (!currentObjectLength_) {
    return folly::makeUnexpected(MoQPublishError(
        MoQPublishError::API_ERROR, "Haven't started publishing object"));
  }
  auto payloadLength = (payload) ? payload->computeChainDataLength() : 0;
  if (payloadLength > *currentObjectLength_) {
    return folly::makeUnexpected(
        MoQPublishError(MoQPublishError::API_ERROR, "Payload exceeded length"));
  }
  *currentObjectLength_ -= payloadLength;
  CloneablePayload cloneable(std::move(payload));
  auto self = shared_from_this();
  auto identifier = identifier_;
  auto res = forEachSubscriberSubgroup(
      [finSubgroup, cloneable, self, identifier](
          const std::shared_ptr<Subscriber>& sub,
          const std::shared_ptr<SubgroupConsumer>& subgroupConsumer) {
        XCHECK(subgroupConsumer);
        subgroupConsumer
            ->objectPayload(maybeClone(cloneable.payload), finSubgroup)
            .onError([sub](const auto& err) {
              sub->forwarder->removeSubscriberOnError(
                  *sub, err, "SubgroupForwarder::objectPayload");
            });
        if (finSubgroup) {
          closeSubgroupForSubscriber(
              sub, identifier, "SubgroupForwarder::objectPayload");
        }
      },
      std::nullopt,
      /*makeNew=*/false);
  // Check if object is complete and subgroup should be closed
  if (*currentObjectLength_ == 0) {
    currentObjectLength_.reset();
    if (finSubgroup) {
      removeSubgroupAndCheckEmpty();
      return ObjectPublishStatus::DONE;
    }
    // Data operation - propagate CANCELLED if no subscribers
    auto cleanupRes = cleanupOnError(res);
    if (cleanupRes.hasError()) {
      return folly::makeUnexpected(cleanupRes.error());
    }
    return ObjectPublishStatus::DONE;
  }

  // Data operation, propagate CANCELLED if no subscribers
  auto cleanupRes = cleanupOnError(res);
  if (cleanupRes.hasError()) {
    return folly::makeUnexpected(cleanupRes.error());
  }
  return ObjectPublishStatus::IN_PROGRESS;
}

} // namespace moxygen

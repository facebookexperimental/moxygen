/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/lane_manager/MoQLaneManager.h"

#include "moxygen/MoQFilters.h"
#include "moxygen/MoQSession.h"
#include "moxygen/events/MoQExecutor.h"
#include "moxygen/lane_manager/SwitchExecutor.h"

#include <folly/Function.h>
#include <folly/ScopeGuard.h>
#include <folly/coro/SharedPromise.h>
#include <folly/futures/Future.h>
#include <folly/logging/xlog.h>

#include <algorithm>
#include <vector>

namespace moxygen::lane_manager {

namespace {

void addInit(
    Extensions& extensions,
    std::shared_ptr<const folly::IOBuf>& init) {
  if (init) {
    extensions.insertMutableExtension(
        Extension{kInitPropertyType, init->clone()});
    init.reset();
  }
}

// Puts the lane's init on the subgroup's first object.
class InitSubgroupConsumer : public SubgroupConsumerFilter {
 public:
  InitSubgroupConsumer(
      std::shared_ptr<SubgroupConsumer> downstream,
      std::shared_ptr<const folly::IOBuf> init)
      : SubgroupConsumerFilter(std::move(downstream)), init_(std::move(init)) {}

  folly::Expected<folly::Unit, MoQPublishError> object(
      uint64_t objectID,
      Payload payload,
      Extensions extensions,
      bool finSubgroup) override {
    addInit(extensions, init_);
    return downstream_->object(
        objectID, std::move(payload), std::move(extensions), finSubgroup);
  }

  folly::Expected<folly::Unit, MoQPublishError> beginObject(
      uint64_t objectID,
      uint64_t length,
      Payload initialPayload,
      Extensions extensions) override {
    addInit(extensions, init_);
    return downstream_->beginObject(
        objectID, length, std::move(initialPayload), std::move(extensions));
  }

 private:
  std::shared_ptr<const folly::IOBuf> init_;
};

// Opens the subgroup on the client; `init`, if set, goes on its first object.
folly::Expected<std::shared_ptr<SubgroupConsumer>, MoQPublishError>
openSubgroup(
    TrackConsumer& client,
    std::shared_ptr<const folly::IOBuf>& init,
    uint64_t group,
    uint64_t subgroup,
    Priority priority,
    BeginSubgroupOptions options) {
  options.includeExtensions = true;
  auto consumer = client.beginSubgroup(group, subgroup, priority, options);
  if (consumer.hasError() || !init) {
    return consumer;
  }
  return std::make_shared<InitSubgroupConsumer>(
      std::move(consumer.value()), std::exchange(init, nullptr));
}

// Keeps a subgroup's calls until release() sends them on or drops them. What
// it keeps counts against its subscription's byte budget; going over it, or a
// failed send, is reported to `failure`.
class HeldSubgroup : public SubgroupConsumer {
 public:
  struct Budget {
    size_t limit{0};
    size_t bytes{0};
    bool closed{false}; // the subscription ended
  };
  using Failure = std::function<void(const MoQPublishError&)>;

  HeldSubgroup(std::shared_ptr<Budget> budget, Failure failure)
      : budget_(std::move(budget)), failure_(std::move(failure)) {}

  HeldSubgroup(const HeldSubgroup&) = delete;
  HeldSubgroup& operator=(const HeldSubgroup&) = delete;
  HeldSubgroup(HeldSubgroup&&) = delete;
  HeldSubgroup& operator=(HeldSubgroup&&) = delete;

  ~HeldSubgroup() override {
    budget_->bytes -= bytes_;
  }

  // Replays what was kept to `downstream`, then forwards; null drops it all.
  void release(std::shared_ptr<SubgroupConsumer> downstream) {
    released_ = true;
    downstream_ = std::move(downstream);
    budget_->bytes -= std::exchange(bytes_, 0);
    for (auto& op : std::exchange(ops_, {})) {
      if (!downstream_) {
        break;
      }
      if (auto result = op(*downstream_); result.hasError()) {
        fail(result.error());
      }
    }
  }

  folly::Expected<folly::Unit, MoQPublishError> object(
      uint64_t objectID,
      Payload payload,
      Extensions extensions,
      bool finSubgroup) override {
    const auto bytes = sizeOf(payload, extensions);
    return apply(
        [objectID,
         payload = std::move(payload),
         extensions = std::move(extensions),
         finSubgroup](SubgroupConsumer& consumer) mutable {
          return consumer.object(
              objectID, std::move(payload), std::move(extensions), finSubgroup);
        },
        bytes);
  }

  void checkpoint() override {
    (void)apply([](SubgroupConsumer& consumer) {
      consumer.checkpoint();
      return folly::Expected<folly::Unit, MoQPublishError>(folly::unit);
    });
  }

  folly::Expected<folly::Unit, MoQPublishError> beginObject(
      uint64_t objectID,
      uint64_t length,
      Payload initialPayload,
      Extensions extensions) override {
    const auto bytes = sizeOf(initialPayload, extensions);
    return apply(
        [objectID,
         length,
         payload = std::move(initialPayload),
         extensions =
             std::move(extensions)](SubgroupConsumer& consumer) mutable {
          return consumer.beginObject(
              objectID, length, std::move(payload), std::move(extensions));
        },
        bytes);
  }

  folly::Expected<ObjectPublishStatus, MoQPublishError> objectPayload(
      Payload payload,
      bool finSubgroup) override {
    if (downstream_) {
      auto result = downstream_->objectPayload(std::move(payload), finSubgroup);
      if (result.hasError()) {
        fail(result.error());
      }
      return result;
    }
    const auto bytes = sizeOf(payload, {});
    auto result = apply(
        [payload = std::move(payload),
         finSubgroup](SubgroupConsumer& consumer) mutable {
          return consumer.objectPayload(std::move(payload), finSubgroup)
              .then([](ObjectPublishStatus) { return folly::unit; });
        },
        bytes);
    if (result.hasError()) {
      return folly::makeUnexpected(result.error());
    }
    return ObjectPublishStatus::IN_PROGRESS;
  }

  folly::Expected<folly::Unit, MoQPublishError> endOfGroup(
      uint64_t endOfGroupObjectID) override {
    return apply([endOfGroupObjectID](SubgroupConsumer& consumer) {
      return consumer.endOfGroup(endOfGroupObjectID);
    });
  }

  folly::Expected<folly::Unit, MoQPublishError> endOfTrackAndGroup(
      uint64_t endOfTrackObjectID) override {
    return apply([endOfTrackObjectID](SubgroupConsumer& consumer) {
      return consumer.endOfTrackAndGroup(endOfTrackObjectID);
    });
  }

  folly::Expected<folly::Unit, MoQPublishError> endOfSubgroup() override {
    return apply(
        [](SubgroupConsumer& consumer) { return consumer.endOfSubgroup(); });
  }

  void reset(ResetStreamErrorCode error) override {
    (void)apply([error](SubgroupConsumer& consumer) {
      consumer.reset(error);
      return folly::Expected<folly::Unit, MoQPublishError>(folly::unit);
    });
  }

 private:
  using Op = folly::Function<folly::Expected<folly::Unit, MoQPublishError>(
      SubgroupConsumer&)>;

  // Bytes a kept call is charged: its payload and properties plus a fixed
  // allowance for the call itself.
  static constexpr size_t kCallBytes = 64;

  static size_t sizeOf(const Payload& payload, const Extensions& extensions) {
    size_t bytes =
        kCallBytes + (payload ? payload->computeChainDataLength() : 0);
    for (const auto* entries :
         {&extensions.getMutableExtensions(),
          &extensions.getImmutableExtensions()}) {
      for (const auto& extension : *entries) {
        bytes += sizeof(Extension) +
            (extension.arrayValue
                 ? extension.arrayValue->computeChainDataLength()
                 : 0);
      }
    }
    return bytes;
  }

  folly::Expected<folly::Unit, MoQPublishError> fail(MoQPublishError error) {
    if (!error_) {
      error_ = std::move(error);
      ops_.clear();
      budget_->bytes -= std::exchange(bytes_, 0);
      if (auto downstream = std::exchange(downstream_, nullptr)) {
        downstream->reset(ResetStreamErrorCode::CANCELLED);
      }
      failure_(*error_);
    }
    return folly::makeUnexpected(*error_);
  }

  folly::Expected<folly::Unit, MoQPublishError> apply(
      Op op,
      size_t bytes = kCallBytes) {
    if (error_) {
      return folly::makeUnexpected(*error_);
    }
    if (downstream_) {
      auto result = op(*downstream_);
      return result.hasError() ? fail(result.error()) : result;
    }
    if (released_) {
      return folly::unit;
    }
    if (budget_->closed) {
      return folly::makeUnexpected(
          MoQPublishError{MoQPublishError::CANCELLED, "unsubscribed"});
    }
    if (bytes > budget_->limit - budget_->bytes) {
      return fail(
          MoQPublishError{
              MoQPublishError::TOO_FAR_BEHIND, "switch hold budget exhausted"});
    }
    ops_.push_back(std::move(op));
    bytes_ += bytes;
    budget_->bytes += bytes;
    return folly::unit;
  }

  std::vector<Op> ops_;
  std::shared_ptr<SubgroupConsumer> downstream_;
  std::shared_ptr<Budget> budget_;
  Failure failure_;
  std::optional<MoQPublishError> error_;
  size_t bytes_{0};
  bool released_{false};
};

// Runs `onExpired` when the timeout fires; cancelling does not run it.
class Timer : public quic::QuicTimerCallback {
 public:
  explicit Timer(folly::Function<void()> onExpired)
      : onExpired_(std::move(onExpired)) {}

  void timeoutExpired() noexcept override {
    onExpired_();
  }

  void callbackCanceled() noexcept override {}

 private:
  folly::Function<void()> onExpired_;
};

// Puts the lane's init on the FETCH's first object.
class InitFetchConsumer : public FetchConsumer {
 public:
  InitFetchConsumer(
      std::shared_ptr<FetchConsumer> downstream,
      std::shared_ptr<const folly::IOBuf> init)
      : downstream_(std::move(downstream)), init_(std::move(init)) {}

  folly::Expected<folly::Unit, MoQPublishError> object(
      uint64_t groupID,
      uint64_t subgroupID,
      uint64_t objectID,
      Payload payload,
      Extensions extensions,
      bool finFetch,
      bool forwardingPreferenceIsDatagram) override {
    addInit(extensions, init_);
    return downstream_->object(
        groupID,
        subgroupID,
        objectID,
        std::move(payload),
        std::move(extensions),
        finFetch,
        forwardingPreferenceIsDatagram);
  }
  void checkpoint() override {
    downstream_->checkpoint();
  }
  folly::Expected<folly::Unit, MoQPublishError> beginObject(
      uint64_t groupID,
      uint64_t subgroupID,
      uint64_t objectID,
      uint64_t length,
      Payload initialPayload,
      Extensions extensions) override {
    addInit(extensions, init_);
    return downstream_->beginObject(
        groupID,
        subgroupID,
        objectID,
        length,
        std::move(initialPayload),
        std::move(extensions));
  }
  folly::Expected<ObjectPublishStatus, MoQPublishError> objectPayload(
      Payload payload,
      bool finSubgroup) override {
    return downstream_->objectPayload(std::move(payload), finSubgroup);
  }
  folly::Expected<folly::Unit, MoQPublishError> endOfGroup(
      uint64_t groupID,
      uint64_t subgroupID,
      uint64_t objectID,
      bool finFetch) override {
    return downstream_->endOfGroup(groupID, subgroupID, objectID, finFetch);
  }
  folly::Expected<folly::Unit, MoQPublishError> endOfTrackAndGroup(
      uint64_t groupID,
      uint64_t subgroupID,
      uint64_t objectID) override {
    return downstream_->endOfTrackAndGroup(groupID, subgroupID, objectID);
  }
  folly::Expected<folly::Unit, MoQPublishError> endOfFetch() override {
    return downstream_->endOfFetch();
  }
  void reset(ResetStreamErrorCode error) override {
    downstream_->reset(error);
  }
  void goaway(Goaway goaway) override {
    downstream_->goaway(std::move(goaway));
  }
  folly::Expected<folly::SemiFuture<uint64_t>, MoQPublishError>
  awaitReadyToConsume() override {
    return downstream_->awaitReadyToConsume();
  }
  folly::Expected<folly::Unit, MoQPublishError> endOfUnknownRange(
      uint64_t groupID,
      uint64_t objectID,
      bool finFetch) override {
    return downstream_->endOfUnknownRange(groupID, objectID, finFetch);
  }

 private:
  std::shared_ptr<FetchConsumer> downstream_;
  std::shared_ptr<const folly::IOBuf> init_;
};

} // namespace

struct MoQLaneManager::SubscriptionSet
    : public std::enable_shared_from_this<SubscriptionSet> {
  struct Held {
    uint64_t subgroup;
    Priority priority;
    BeginSubgroupOptions options;
    std::shared_ptr<HeldSubgroup> consumer;
  };

  SubscriptionSet(
      std::weak_ptr<MoQLaneManager> manager,
      Role role,
      std::shared_ptr<TrackConsumer> client,
      std::shared_ptr<SwitchingPolicy> policy,
      MoQExecutor& executor)
      : role(role),
        client(std::move(client)),
        policy(std::move(policy)),
        executor(executor),
        holdTimer([this, manager = std::move(manager)] {
          // The replay can drop the client's handle, and with it this set.
          auto self = shared_from_this();
          if (auto locked = manager.lock()) {
            locked->expireHold(*this);
          }
        }) {}

  Role role;
  std::shared_ptr<TrackConsumer> client;
  std::shared_ptr<SwitchingPolicy> policy;
  MoQExecutor& executor;
  RequestID requestID{0};
  // The virtual track, set once its SUBSCRIBE succeeds.
  std::optional<FullTrackName> track;
  std::shared_ptr<HeldSubgroup::Budget> holdBudget{
      std::make_shared<HeldSubgroup::Budget>()};
  bool failed{false};
  bool closed{false};
  std::string lane; // the one the virtual SUBSCRIBE_OK came from
  // Lanes with a SUBSCRIBE_OK.
  folly::F14FastMap<std::string, std::shared_ptr<SubscriptionHandle>> laneSubs;
  std::optional<SwitchExecutor> switcher;
  // Goes on the live lane's next object.
  std::shared_ptr<const folly::IOBuf> init;
  std::vector<Held> held; // the held group's subgroups
  Timer holdTimer;
  std::optional<uint64_t> liveGroup; // the last group sent from the live lane
  std::optional<uint64_t> selectedGroup; // the last group select() ran for
  // Set once `lane` has its SUBSCRIBE_OK, or the subscribe ended without one.
  folly::coro::SharedPromise<void> started;
};

// Like MoQProxy's handles, dropping one unsubscribes.
class MoQLaneManager::VirtualHandle : public Publisher::SubscriptionHandle {
 public:
  VirtualHandle(
      std::shared_ptr<MoQLaneManager> manager,
      std::weak_ptr<SubscriptionSet> set,
      SubscribeOk ok)
      : SubscriptionHandle(std::move(ok)),
        manager_(std::move(manager)),
        set_(std::move(set)) {}

  ~VirtualHandle() override {
    unsubscribe();
  }

  VirtualHandle(const VirtualHandle&) = delete;
  VirtualHandle& operator=(const VirtualHandle&) = delete;
  VirtualHandle(VirtualHandle&&) = delete;
  VirtualHandle& operator=(VirtualHandle&&) = delete;

  void unsubscribe() override {
    auto manager = std::exchange(manager_, nullptr);
    if (auto set = set_.lock(); manager && set) {
      manager->unsubscribe(*set);
    }
  }

  folly::coro::Task<RequestUpdateResult> requestUpdate(
      RequestUpdate update) override {
    co_return folly::makeUnexpected(
        RequestError{update.requestID, RequestErrorCode::NOT_SUPPORTED, ""});
  }

 private:
  std::shared_ptr<MoQLaneManager> manager_;
  std::weak_ptr<SubscriptionSet> set_;
};

// Feeds a lane to its subscription set. The client keeps the virtual track's
// alias.
class MoQLaneManager::LaneConsumer : public TrackConsumerFilter {
 public:
  LaneConsumer(
      std::weak_ptr<MoQLaneManager> manager,
      const std::shared_ptr<SubscriptionSet>& set,
      std::string lane)
      : TrackConsumerFilter(set->client),
        manager_(std::move(manager)),
        set_(set),
        lane_(std::move(lane)) {}

  folly::Expected<folly::Unit, MoQPublishError> setTrackAlias(
      TrackAlias /*alias*/) override {
    return folly::unit;
  }

  folly::Expected<std::shared_ptr<SubgroupConsumer>, MoQPublishError>
  beginSubgroup(
      uint64_t groupID,
      uint64_t subgroupID,
      Priority priority,
      BeginSubgroupOptions options) override {
    auto manager = manager_.lock();
    auto set = set_.lock();
    if (!manager || !set) {
      return folly::makeUnexpected(
          MoQPublishError(MoQPublishError::CANCELLED, "unsubscribed"));
    }
    auto subgroup = manager->beginSubgroup(
        *set, lane_, groupID, subgroupID, priority, options);
    manager->select(*set);
    return subgroup;
  }

  // Switching works on subgroup streams only.
  folly::Expected<folly::Unit, MoQPublishError> objectStream(
      const ObjectHeader& /*header*/,
      Payload /*payload*/,
      bool /*lastInGroup*/) override {
    return folly::makeUnexpected(
        MoQPublishError{
            MoQPublishError::MALFORMED_TRACK, "lanes need subgroup streams"});
  }

  folly::Expected<folly::Unit, MoQPublishError> datagram(
      const ObjectHeader& /*header*/,
      Payload /*payload*/,
      bool /*lastInGroup*/) override {
    return folly::makeUnexpected(
        MoQPublishError{
            MoQPublishError::MALFORMED_TRACK, "lanes need subgroup streams"});
  }

  folly::Expected<folly::Unit, MoQPublishError> publishDone(
      PublishDone pubDone) override {
    // The client may drop the last handle, and with it the manager, inside
    // publishDone().
    auto manager = manager_.lock();
    auto set = set_.lock();
    if (!manager || !set || set->closed) {
      return folly::unit;
    }
    if (lane_ != set->switcher->liveLane()) {
      set->laneSubs.erase(lane_);
      set->switcher->onLaneEnded(lane_, Clock::now());
      manager->applyOutcomes(*set);
      return folly::unit;
    }
    if (set->switcher->deadline()) {
      set->switcher->cancel(Clock::now()); // the held group goes out first
      manager->applyOutcomes(*set);
    }
    auto result = downstream_->publishDone(std::move(pubDone));
    manager->unsubscribe(*set);
    return result;
  }

 private:
  std::weak_ptr<MoQLaneManager> manager_;
  std::weak_ptr<SubscriptionSet> set_;
  std::string lane_;
};

MoQLaneManager::MoQLaneManager(
    std::shared_ptr<Publisher> proxy,
    TrackNamespace broadcast,
    std::chrono::milliseconds defaultMaxWait,
    LaneManagerAbrOptions abr)
    : proxy_(std::move(proxy)),
      broadcast_(std::move(broadcast)),
      defaultMaxWait_(defaultMaxWait),
      abr_(std::move(abr)),
      catalog_(
          proxy_,
          broadcast_,
          std::chrono::seconds(5),
          abr_.immutableCatalog ? CatalogReader::Mode::ImmutableSnapshot
                                : CatalogReader::Mode::Live) {}

MoQLaneManager::~MoQLaneManager() {
  for (auto& [role, set] : sets_) {
    close(*set);
    for (auto& [lane, handle] : set->laneSubs) {
      handle->unsubscribe();
    }
  }
}

folly::coro::Task<Publisher::SubscribeResult> MoQLaneManager::subscribe(
    SubscribeRequest request,
    std::shared_ptr<TrackConsumer> client,
    Role role,
    std::shared_ptr<SwitchingPolicy> policy) {
  auto self = shared_from_this();
  auto [it, inserted] = sets_.try_emplace(role);
  if (!inserted) {
    co_return folly::makeUnexpected(
        SubscribeError{
            request.requestID, RequestErrorCode::DUPLICATE_SUBSCRIPTION, ""});
  }
  auto set = std::make_shared<SubscriptionSet>(
      self,
      role,
      client,
      std::move(policy),
      *MoQSession::getRequestContext().executor);
  set->requestID = request.requestID;
  set->holdBudget->limit = abr_.holdLimits.bytes;
  it->second = set;
  auto current = [&] {
    auto found = sets_.find(role);
    return found != sets_.end() && found->second == set;
  };
  // Frees the role on every exit but success, a cancelled await included.
  auto release = folly::makeGuard([&] {
    set->started.setValue();
    close(*set);
    if (current()) {
      sets_.erase(role);
    }
  });
  auto fail = [&](RequestErrorCode code, std::string reason) {
    return folly::makeUnexpected(
        SubscribeError{request.requestID, code, std::move(reason)});
  };
  if (!set->policy) {
    co_return fail(RequestErrorCode::NOT_SUPPORTED, "no switching policy");
  }
  auto opened =
      co_await folly::coro::co_awaitTry(catalog_.open(request.requestID));
  if (opened.hasException<folly::FutureTimeout>()) {
    SubscribeError error{
        request.requestID, RequestErrorCode::TIMEOUT, "no catalog yet"};
    error.retryInterval = std::chrono::milliseconds(1); // retry any time
    co_return folly::makeUnexpected(std::move(error));
  }
  if (!opened.value()) {
    co_return fail(RequestErrorCode::DOES_NOT_EXIST, "no catalog");
  }
  const auto* lanes = catalog_.switchingSet(role);
  if (!lanes) {
    co_return fail(RequestErrorCode::DOES_NOT_EXIST, "no lanes");
  }
  const TrackAlias alias{request.requestID.value};
  if (client->setTrackAlias(alias).hasError()) {
    co_return fail(RequestErrorCode::INTERNAL_ERROR, "track alias");
  }

  auto candidates = *lanes;
  std::string reason;
  while (!candidates.lanes.empty()) {
    auto lane = set->policy->initialLane(candidates);
    const auto* info = candidates.find(lane);
    if (!info) {
      info = &candidates.lanes.front();
      lane = info->trackName;
    }
    set->switcher.emplace(lane, defaultMaxWait_);
    set->init = nullptr;
    if (!abr_.initializationFromCatalog) {
      set->init = info->init;
    }
    auto laneRequest = request;
    laneRequest.fullTrackName = FullTrackName{broadcast_, lane};
    auto subscribed = co_await proxy_->subscribe(
        std::move(laneRequest),
        std::make_shared<LaneConsumer>(self, set, lane));
    if (subscribed.hasValue()) {
      if (!current()) {
        subscribed.value()->unsubscribe();
        co_return fail(RequestErrorCode::INTERNAL_ERROR, "ended");
      }
      XLOG(DBG1) << "[lane_manager] " << broadcast_.describe() << " starts on "
                 << lane;
      const auto& laneOk = subscribed.value()->subscribeOk();
      SubscribeOk ok;
      ok.requestID = request.requestID;
      ok.trackAlias = alias;
      ok.expires = laneOk.expires;
      ok.groupOrder = laneOk.groupOrder;
      ok.largest = laneOk.largest;
      ok.extensions = laneOk.extensions;
      set->lane = lane;
      set->laneSubs.emplace(lane, std::move(subscribed.value()));
      for (const auto& other : candidates.lanes) {
        if (other.trackName == lane) {
          continue;
        }
        auto otherRequest = request;
        otherRequest.fullTrackName = FullTrackName{broadcast_, other.trackName};
        co_withExecutor(
            &set->executor,
            subscribeLane(
                proxy_,
                std::move(otherRequest),
                std::make_shared<LaneConsumer>(self, set, other.trackName),
                set))
            .start();
      }
      release.dismiss();
      set->started.setValue();
      set->track = request.fullTrackName;
      if (abr_.onStarted) {
        abr_.onStarted(*set->track, candidates);
      }
      co_return std::make_shared<VirtualHandle>(self, set, std::move(ok));
    }
    reason = subscribed.error().reasonPhrase;
    XLOG(WARN) << "[lane_manager] " << broadcast_.describe() << " lane " << lane
               << " failed: " << reason;
    std::erase_if(candidates.lanes, [&](const LaneInfo& candidate) {
      return candidate.trackName == lane;
    });
  }
  co_return fail(RequestErrorCode::INTERNAL_ERROR, reason);
}

folly::coro::Task<void> MoQLaneManager::subscribeLane(
    std::shared_ptr<Publisher> proxy,
    SubscribeRequest request,
    std::shared_ptr<LaneConsumer> consumer,
    std::weak_ptr<SubscriptionSet> set) {
  auto lane = request.fullTrackName.trackName;
  auto subscribed =
      co_await proxy->subscribe(std::move(request), std::move(consumer));
  if (subscribed.hasError()) {
    XLOG(WARN) << "[lane_manager] lane " << lane
               << " failed: " << subscribed.error().reasonPhrase;
    co_return;
  }
  if (auto locked = set.lock(); locked && !locked->closed) {
    locked->laneSubs.emplace(std::move(lane), std::move(subscribed.value()));
  } else {
    subscribed.value()->unsubscribe();
  }
}

folly::Expected<std::shared_ptr<SubgroupConsumer>, MoQPublishError>
MoQLaneManager::beginSubgroup(
    SubscriptionSet& set,
    std::string_view lane,
    uint64_t group,
    uint64_t subgroup,
    Priority priority,
    BeginSubgroupOptions options) {
  if (set.closed || set.failed) {
    return folly::makeUnexpected(
        MoQPublishError(MoQPublishError::CANCELLED, "unsubscribed"));
  }
  auto& switcher = *set.switcher;
  const auto now = Clock::now();
  const auto deadline = switcher.deadline();
  const auto action = switcher.onGroup(lane, group, now);
  applyOutcomes(set);
  if (const auto held = switcher.deadline(); held && held != deadline) {
    set.executor.scheduleTimeout(
        &set.holdTimer,
        std::chrono::ceil<std::chrono::milliseconds>(*held - now));
  }
  if (action == SwitchExecutor::Action::Forward) {
    set.liveGroup = std::max(group, set.liveGroup.value_or(0));
    return openSubgroup(
        *set.client, set.init, group, subgroup, priority, options);
  }
  auto consumer = std::make_shared<HeldSubgroup>(
      set.holdBudget,
      [weak = weak_from_this(),
       weakSet = std::weak_ptr(set.shared_from_this())](
          const MoQPublishError& error) {
        auto manager = weak.lock();
        auto current = weakSet.lock();
        if (manager && current) {
          manager->fail(*current, error);
        }
      });
  if (action == SwitchExecutor::Action::Hold) {
    if (set.held.size() >= abr_.holdLimits.subgroups) {
      const MoQPublishError error{
          MoQPublishError::TOO_FAR_BEHIND, "too many held subgroups"};
      fail(set, error);
      return folly::makeUnexpected(error);
    }
    set.held.push_back({subgroup, priority, options, consumer});
  } else {
    consumer->release(nullptr);
  }
  return consumer;
}

void MoQLaneManager::expireHold(SubscriptionSet& set) {
  if (set.closed || set.failed) {
    return;
  }
  const auto now = Clock::now();
  set.switcher->onDeadline(now);
  applyOutcomes(set);
  if (const auto held = set.switcher->deadline()) {
    // The timer ran early.
    set.executor.scheduleTimeout(
        &set.holdTimer,
        std::max(
            std::chrono::milliseconds(1),
            std::chrono::ceil<std::chrono::milliseconds>(*held - now)));
  }
  select(set);
}

void MoQLaneManager::applyOutcomes(SubscriptionSet& set) {
  for (const auto& outcome : set.switcher->takeOutcomes()) {
    set.holdTimer.cancelTimerCallback();
    const bool committed = outcome.kind == SwitchOutcome::Kind::Committed;
    for (auto& held : std::exchange(set.held, {})) {
      std::shared_ptr<SubgroupConsumer> sent;
      if (!committed && !set.failed) {
        auto opened = openSubgroup(
            *set.client,
            set.init,
            outcome.group,
            held.subgroup,
            held.priority,
            held.options);
        if (opened.hasValue()) {
          sent = std::move(opened.value());
        } else {
          fail(set, opened.error());
        }
      }
      held.consumer->release(std::move(sent));
    }
    if (!committed) {
      set.liveGroup = outcome.group;
    } else if (const auto* lanes = catalog_.switchingSet(set.role);
               lanes && !abr_.initializationFromCatalog) {
      if (const auto* target = lanes->find(outcome.targetLane)) {
        set.init = target->init;
      }
    }
    XLOG(DBG1) << "[lane_manager] " << broadcast_.describe() << " "
               << outcome.targetLane << (committed ? " is live" : " missed")
               << " at group " << outcome.group;
    notify(set, outcome);
  }
}

void MoQLaneManager::notify(
    SubscriptionSet& set,
    const SwitchOutcome& outcome) {
  set.policy->onSwitchOutcome(outcome);
  if (abr_.onOutcome && set.track) {
    abr_.onOutcome(*set.track, outcome);
  }
}

void MoQLaneManager::fail(SubscriptionSet& set, const MoQPublishError& error) {
  if (set.closed || std::exchange(set.failed, true)) {
    return;
  }
  set.holdBudget->closed = true;
  set.executor.add([weak = weak_from_this(),
                    weakSet = std::weak_ptr(set.shared_from_this()),
                    error] {
    auto manager = weak.lock();
    auto current = weakSet.lock();
    if (manager && current && !current->closed) {
      (void)current->client->publishDone(
          PublishDone{
              current->requestID,
              PublishDoneStatusCode::INTERNAL_ERROR,
              0,
              error.what()});
      manager->unsubscribe(*current);
    }
  });
}

void MoQLaneManager::select(SubscriptionSet& set) {
  auto& switcher = *set.switcher;
  const auto* lanes = catalog_.switchingSet(set.role);
  if (set.closed || set.failed || switcher.pending() || !lanes ||
      !set.liveGroup || set.liveGroup == set.selectedGroup) {
    return;
  }
  set.selectedGroup = set.liveGroup;
  auto eligible = *lanes;
  std::erase_if(eligible.lanes, [&](const LaneInfo& lane) {
    return lane.trackName != switcher.liveLane() &&
        !set.laneSubs.contains(lane.trackName);
  });
  const auto now = Clock::now();
  auto plan =
      set.policy->select(eligible, switcher.liveLane(), *set.liveGroup, now);
  if (plan && switcher.plan(eligible, *plan, *set.liveGroup)) {
    notify(
        set,
        SwitchOutcome{
            SwitchOutcome::Kind::Prepared,
            switcher.liveLane(),
            plan->targetLane,
            *set.liveGroup,
            now});
  }
}

folly::coro::Task<Publisher::FetchResult> MoQLaneManager::fetch(
    Fetch fetch,
    std::shared_ptr<FetchConsumer> client,
    Role role) {
  if (!std::holds_alternative<JoiningFetch>(fetch.args)) {
    co_return folly::makeUnexpected(
        FetchError{fetch.requestID, FetchErrorCode::NOT_SUPPORTED, ""});
  }
  auto self = shared_from_this();
  auto it = sets_.find(role);
  if (it == sets_.end()) {
    co_return folly::makeUnexpected(
        FetchError{fetch.requestID, FetchErrorCode::DOES_NOT_EXIST, ""});
  }
  // A joining FETCH can arrive while its SUBSCRIBE still awaits the lane.
  auto set = it->second;
  co_await set->started.getFuture();
  it = sets_.find(role);
  const auto* lanes = catalog_.switchingSet(role);
  const auto* lane = it != sets_.end() && it->second == set && lanes
      ? lanes->find(set->lane)
      : nullptr;
  if (!lane) {
    co_return folly::makeUnexpected(
        FetchError{fetch.requestID, FetchErrorCode::DOES_NOT_EXIST, ""});
  }
  fetch.fullTrackName = FullTrackName{broadcast_, lane->trackName};
  std::shared_ptr<const folly::IOBuf> init;
  if (!abr_.initializationFromCatalog) {
    init = lane->init;
  }
  co_return co_await proxy_->fetch(
      std::move(fetch),
      std::make_shared<InitFetchConsumer>(std::move(client), std::move(init)));
}

void MoQLaneManager::unsubscribe(const SubscriptionSet& set) {
  auto it = sets_.find(set.role);
  if (it == sets_.end() || it->second.get() != &set) {
    return;
  }
  close(*it->second);
  auto laneSubs = std::move(it->second->laneSubs);
  sets_.erase(it);
  for (auto& [lane, handle] : laneSubs) {
    handle->unsubscribe();
  }
}

void MoQLaneManager::close(SubscriptionSet& set) {
  if (std::exchange(set.closed, true)) {
    return;
  }
  set.holdTimer.cancelTimerCallback();
  set.holdBudget->closed = true;
  for (auto& held : std::exchange(set.held, {})) {
    held.consumer->release(nullptr);
  }
  if (set.track && abr_.onClosed) {
    abr_.onClosed(*set.track);
  }
}

void MoQLaneManager::onPolicyChanged(Role role) {
  auto found = sets_.find(role);
  if (found == sets_.end() || found->second->closed || found->second->failed) {
    return;
  }
  auto set = found->second;
  set->switcher->cancel(Clock::now());
  applyOutcomes(*set);
  set->selectedGroup.reset();
  select(*set);
}

} // namespace moxygen::lane_manager

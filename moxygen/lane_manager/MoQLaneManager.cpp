/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/lane_manager/MoQLaneManager.h"

#include "moxygen/MoQFilters.h"

#include <folly/ScopeGuard.h>
#include <folly/futures/Future.h>
#include <folly/logging/xlog.h>

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

class VirtualHandle : public Publisher::SubscriptionHandle {
 public:
  VirtualHandle(
      std::shared_ptr<MoQLaneManager> manager,
      Role role,
      SubscribeOk ok)
      : SubscriptionHandle(std::move(ok)),
        manager_(std::move(manager)),
        role_(role) {}

  void unsubscribe() override {
    if (auto manager = std::exchange(manager_, nullptr)) {
      manager->unsubscribe(role_);
    }
  }

  folly::coro::Task<RequestUpdateResult> requestUpdate(
      RequestUpdate update) override {
    co_return folly::makeUnexpected(
        RequestError{update.requestID, RequestErrorCode::NOT_SUPPORTED, ""});
  }

 private:
  std::shared_ptr<MoQLaneManager> manager_;
  Role role_;
};

} // namespace

// Forwards a lane to the client, which keeps the virtual track's alias.
class MoQLaneManager::LaneConsumer : public TrackConsumerFilter {
 public:
  LaneConsumer(
      std::weak_ptr<MoQLaneManager> manager,
      Role role,
      std::shared_ptr<TrackConsumer> client,
      std::shared_ptr<const folly::IOBuf> init)
      : TrackConsumerFilter(std::move(client)),
        manager_(std::move(manager)),
        role_(role),
        init_(std::move(init)) {}

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
    options.includeExtensions = true;
    auto subgroup =
        downstream_->beginSubgroup(groupID, subgroupID, priority, options);
    if (subgroup.hasError() || !init_) {
      return subgroup;
    }
    return std::make_shared<InitSubgroupConsumer>(
        std::move(subgroup.value()), std::exchange(init_, nullptr));
  }

  folly::Expected<folly::Unit, MoQPublishError> objectStream(
      const ObjectHeader& header,
      Payload payload,
      bool lastInGroup) override {
    auto withInit = header;
    addInit(withInit.extensions, init_);
    return downstream_->objectStream(withInit, std::move(payload), lastInGroup);
  }

  folly::Expected<folly::Unit, MoQPublishError> publishDone(
      PublishDone pubDone) override {
    // The client may drop the last handle, and with it the manager, inside
    // publishDone().
    auto manager = manager_.lock();
    auto result = downstream_->publishDone(std::move(pubDone));
    if (manager) {
      manager->unsubscribe(role_);
    }
    return result;
  }

 private:
  std::weak_ptr<MoQLaneManager> manager_;
  Role role_;
  std::shared_ptr<const folly::IOBuf> init_;
};

MoQLaneManager::MoQLaneManager(
    std::shared_ptr<Publisher> proxy,
    TrackNamespace broadcast)
    : proxy_(std::move(proxy)),
      broadcast_(std::move(broadcast)),
      catalog_(proxy_, broadcast_) {}

MoQLaneManager::~MoQLaneManager() {
  for (auto& [role, set] : sets_) {
    for (auto& [lane, handle] : set.laneSubs) {
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
  if (!sets_.try_emplace(role).second) {
    co_return folly::makeUnexpected(
        SubscribeError{
            request.requestID, RequestErrorCode::DUPLICATE_SUBSCRIPTION, ""});
  }
  // Frees the role on every exit but success, a cancelled await included.
  auto release = folly::makeGuard([&] { sets_.erase(role); });
  auto fail = [&](RequestErrorCode code, std::string reason) {
    return folly::makeUnexpected(
        SubscribeError{request.requestID, code, std::move(reason)});
  };
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
    auto lane = policy->initialLane(candidates);
    const auto* info = candidates.find(lane);
    if (!info) {
      info = &candidates.lanes.front();
      lane = info->trackName;
    }
    auto laneRequest = request;
    laneRequest.fullTrackName = FullTrackName{broadcast_, lane};
    auto subscribed = co_await proxy_->subscribe(
        std::move(laneRequest),
        std::make_shared<LaneConsumer>(self, role, client, info->init));
    if (subscribed.hasValue()) {
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
      auto it = sets_.find(role);
      if (it == sets_.end()) {
        subscribed.value()->unsubscribe();
        co_return fail(RequestErrorCode::INTERNAL_ERROR, "ended");
      }
      it->second.laneSubs.emplace(lane, std::move(subscribed.value()));
      release.dismiss();
      co_return std::make_shared<VirtualHandle>(self, role, std::move(ok));
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

void MoQLaneManager::unsubscribe(Role role) {
  auto it = sets_.find(role);
  if (it == sets_.end()) {
    return;
  }
  auto laneSubs = std::move(it->second.laneSubs);
  sets_.erase(it);
  for (auto& [lane, handle] : laneSubs) {
    handle->unsubscribe();
  }
}

} // namespace moxygen::lane_manager

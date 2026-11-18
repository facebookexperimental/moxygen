/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include "moxygen/MoQConsumers.h"
#include "moxygen/Publisher.h"
#include "moxygen/lane_manager/CatalogReader.h"
#include "moxygen/lane_manager/SwitchingPolicy.h"

#include <folly/container/F14Map.h>
#include <folly/coro/Task.h>

#include <chrono>
#include <memory>
#include <string>

namespace moxygen::lane_manager {

// Object property carrying a lane's init segment.
inline constexpr uint64_t kInitPropertyType = 0x3801;

// Serves one client session's virtual tracks for one broadcast, one per role,
// from the broadcast's lanes, which it SUBSCRIBEs through `proxy`. Every lane
// of the role is subscribed, so the policy can switch between them at group
// boundaries. `defaultMaxWait` is the wait for plans that set none.
class MoQLaneManager : public std::enable_shared_from_this<MoQLaneManager> {
 public:
  MoQLaneManager(
      std::shared_ptr<Publisher> proxy,
      TrackNamespace broadcast,
      std::chrono::milliseconds defaultMaxWait);
  ~MoQLaneManager();

  MoQLaneManager(const MoQLaneManager&) = delete;
  MoQLaneManager& operator=(const MoQLaneManager&) = delete;
  MoQLaneManager(MoQLaneManager&&) = delete;
  MoQLaneManager& operator=(MoQLaneManager&&) = delete;

  // Called on the client session's executor, in its request context.
  folly::coro::Task<Publisher::SubscribeResult> subscribe(
      SubscribeRequest request,
      std::shared_ptr<TrackConsumer> client,
      Role role,
      std::shared_ptr<SwitchingPolicy> policy);

  // Serves a joining FETCH on the virtual track from the lane its SUBSCRIBE_OK
  // came from.
  folly::coro::Task<Publisher::FetchResult>
  fetch(Fetch fetch, std::shared_ptr<FetchConsumer> client, Role role);

 private:
  class LaneConsumer;
  class VirtualHandle;
  struct SubscriptionSet;

  static folly::coro::Task<void> subscribeLane(
      std::shared_ptr<Publisher> proxy,
      SubscribeRequest request,
      std::shared_ptr<LaneConsumer> consumer,
      std::weak_ptr<SubscriptionSet> set);

  // Sends, drops or holds a lane's new subgroup.
  folly::Expected<std::shared_ptr<SubgroupConsumer>, MoQPublishError>
  beginSubgroup(
      SubscriptionSet& set,
      std::string_view lane,
      uint64_t group,
      uint64_t subgroup,
      Priority priority,
      BeginSubgroupOptions options);
  void expireHold(SubscriptionSet& set);
  // Ends `set` if it is still its role's; the lanes are UNSUBSCRIBEd.
  void unsubscribe(const SubscriptionSet& set);
  void applyOutcomes(SubscriptionSet& set);
  // Asks the policy for a plan once per live group, while no switch is
  // pending.
  void select(SubscriptionSet& set);

  std::shared_ptr<Publisher> proxy_;
  TrackNamespace broadcast_;
  std::chrono::milliseconds defaultMaxWait_;
  CatalogReader catalog_;
  folly::F14FastMap<Role, std::shared_ptr<SubscriptionSet>> sets_;
};

} // namespace moxygen::lane_manager

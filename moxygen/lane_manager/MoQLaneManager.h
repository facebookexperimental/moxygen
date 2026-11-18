/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include "moxygen/Publisher.h"
#include "moxygen/lane_manager/CatalogReader.h"
#include "moxygen/lane_manager/SwitchingPolicy.h"

#include <folly/container/F14Map.h>
#include <folly/coro/Task.h>

#include <memory>
#include <string>

namespace moxygen::lane_manager {

// Object property carrying a lane's init segment.
inline constexpr uint64_t kInitPropertyType = 0x3801;

// Serves one client session's virtual tracks for one broadcast, one per role,
// from the broadcast's lanes, which it SUBSCRIBEs through `proxy`.
class MoQLaneManager : public std::enable_shared_from_this<MoQLaneManager> {
 public:
  MoQLaneManager(std::shared_ptr<Publisher> proxy, TrackNamespace broadcast);
  ~MoQLaneManager();

  MoQLaneManager(const MoQLaneManager&) = delete;
  MoQLaneManager& operator=(const MoQLaneManager&) = delete;
  MoQLaneManager(MoQLaneManager&&) = delete;
  MoQLaneManager& operator=(MoQLaneManager&&) = delete;

  folly::coro::Task<Publisher::SubscribeResult> subscribe(
      SubscribeRequest request,
      std::shared_ptr<TrackConsumer> client,
      Role role,
      std::shared_ptr<SwitchingPolicy> policy);

  void unsubscribe(Role role);

 private:
  class LaneConsumer;

  struct SubscriptionSet {
    folly::F14FastMap<std::string, std::shared_ptr<SubscriptionHandle>>
        laneSubs;
  };

  std::shared_ptr<Publisher> proxy_;
  TrackNamespace broadcast_;
  CatalogReader catalog_;
  folly::F14FastMap<Role, SubscriptionSet> sets_;
};

} // namespace moxygen::lane_manager

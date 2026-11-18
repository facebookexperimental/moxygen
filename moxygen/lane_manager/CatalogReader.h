/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include "moxygen/Publisher.h"
#include "moxygen/lane_manager/SwitchingSet.h"

#include <folly/coro/SharedPromise.h>
#include <folly/coro/Task.h>

#include <chrono>
#include <memory>
#include <optional>

namespace moxygen::lane_manager {

enum class Role { Video, Audio };

// Keeps a broadcast's catalog current: a SUBSCRIBE for updates and a joining
// FETCH for the catalog already published.
class CatalogReader {
 public:
  CatalogReader(
      std::shared_ptr<Publisher> publisher,
      TrackNamespace broadcast,
      std::chrono::milliseconds firstCatalogWait = std::chrono::seconds(5));
  ~CatalogReader();

  CatalogReader(const CatalogReader&) = delete;
  CatalogReader& operator=(const CatalogReader&) = delete;
  CatalogReader(CatalogReader&&) = delete;
  CatalogReader& operator=(CatalogReader&&) = delete;

  // The first call subscribes. Every call completes once the first catalog is
  // read, or with false if the SUBSCRIBE fails or the reader is closed. Throws
  // folly::FutureTimeout if no catalog arrives within `firstCatalogWait`.
  folly::coro::Task<bool> open(RequestID requestID);
  void close();

  // The role's lanes that share its first track's altGroup.
  const SwitchingSet* switchingSet(Role role) const;

 private:
  class Callback;

  void onCatalog(Payload payload);

  std::shared_ptr<Publisher> publisher_;
  FullTrackName track_;
  std::shared_ptr<Callback> callback_;
  std::shared_ptr<Publisher::SubscriptionHandle> subscription_;
  std::shared_ptr<Publisher::FetchHandle> fetch_;
  std::optional<SwitchingSet> video_;
  std::optional<SwitchingSet> audio_;
  std::chrono::milliseconds firstCatalogWait_;
  folly::coro::SharedPromise<void> ready_;
  bool opened_{false};
  bool closed_{false};
};

} // namespace moxygen::lane_manager

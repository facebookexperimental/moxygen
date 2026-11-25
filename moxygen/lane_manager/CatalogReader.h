/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include "moxygen/Publisher.h"
#include "moxygen/lane_manager/SwitchingSet.h"

#include <folly/coro/Task.h>

#include <chrono>
#include <functional>
#include <memory>
#include <optional>

namespace moxygen::lane_manager {

enum class Role { Video, Audio };

// Reads a catalog through live updates or one immutable snapshot.
class CatalogReader {
 public:
  enum class Mode { Live, ImmutableSnapshot };

  CatalogReader(
      std::shared_ptr<Publisher> proxy,
      TrackNamespace broadcast,
      std::chrono::milliseconds firstCatalogWait = std::chrono::seconds(5),
      Mode mode = Mode::Live);
  ~CatalogReader();

  CatalogReader(const CatalogReader&) = delete;
  CatalogReader& operator=(const CatalogReader&) = delete;
  CatalogReader(CatalogReader&&) = delete;
  CatalogReader& operator=(CatalogReader&&) = delete;

  // Every call completes once the first catalog is read, or with false when
  // acquisition fails, the request ends, or the reader is closed. Throws
  // folly::FutureTimeout if no catalog arrives within `firstCatalogWait`.
  folly::coro::Task<bool> subscribe(RequestID requestID);
  // The acquisition's terminal FETCH error, shared by all callers.
  std::optional<FetchError> error() const;
  void close();
  void setUpdateCallback(std::function<void()> onUpdate);

  // The role's lanes that share its first track's altGroup.
  const SwitchingSet* switchingSet(Role role) const;

 private:
  struct Acquisition;
  class Callback;

  void startSubscription(RequestID requestID);
  static folly::coro::Task<void> fetchInitialCatalog(
      std::shared_ptr<Acquisition> acquisition,
      std::shared_ptr<Callback> callback,
      RequestID requestID);
  static folly::coro::Task<void> fetchSnapshot(
      std::shared_ptr<Publisher> proxy,
      std::shared_ptr<Acquisition> acquisition,
      std::shared_ptr<Callback> callback,
      Fetch fetch);
  static folly::coro::Task<void> waitForSnapshot(
      std::shared_ptr<Acquisition> acquisition,
      std::shared_ptr<Callback> callback,
      RequestID requestID);
  void fail(FetchError error);
  void onCatalog(Payload payload);
  void onSubscriptionDone();

  std::shared_ptr<Publisher> proxy_;
  FullTrackName track_;
  std::shared_ptr<Callback> callback_;
  std::shared_ptr<Acquisition> acquisition_;
  std::optional<SwitchingSet> video_;
  std::optional<SwitchingSet> audio_;
  std::chrono::milliseconds firstCatalogWait_;
  Mode mode_;
  std::function<void()> onUpdate_;
};

} // namespace moxygen::lane_manager

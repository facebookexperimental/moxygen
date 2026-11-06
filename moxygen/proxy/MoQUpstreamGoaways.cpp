/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/proxy/MoQUpstreamGoaways.h"

#include <utility>
#include <vector>

#include "moxygen/MoQSession.h"

namespace moxygen {

class MoQUpstreamGoaways::SessionObserver final
    : public Subscriber,
      public std::enable_shared_from_this<SessionObserver> {
 public:
  SessionObserver(std::weak_ptr<MoQUpstreamGoaways> registry, SessionId id)
      : registry_(std::move(registry)), sessionId_(id) {}

  SessionObserver(const SessionObserver&) = delete;
  SessionObserver& operator=(const SessionObserver&) = delete;
  SessionObserver(SessionObserver&&) = delete;
  SessionObserver& operator=(SessionObserver&&) = delete;

  ~SessionObserver() override {
    if (auto registry = registry_.lock()) {
      registry->forget(sessionId_);
    }
  }

  uint64_t add(Callback callback) {
    auto id = nextId_++;
    callbacks_.emplace(id, std::make_shared<Callback>(std::move(callback)));
    return id;
  }

  void remove(uint64_t id) {
    callbacks_.erase(id);
  }

  void goaway(Goaway goaway) override {
    // A callback may stop watches, or close the session, which releases this
    // observer.
    auto self = shared_from_this();
    std::vector<uint64_t> ids;
    ids.reserve(callbacks_.size());
    for (const auto& entry : callbacks_) {
      ids.push_back(entry.first);
    }
    for (auto id : ids) {
      auto it = callbacks_.find(id);
      if (it == callbacks_.end()) {
        continue;
      }
      auto callback = it->second;
      (*callback)(goaway);
    }
  }

 private:
  std::weak_ptr<MoQUpstreamGoaways> registry_;
  SessionId sessionId_;
  uint64_t nextId_{1};
  folly::F14FastMap<uint64_t, std::shared_ptr<Callback>> callbacks_;
};

MoQUpstreamGoaways::Watch::Watch(
    std::weak_ptr<SessionObserver> observer,
    uint64_t id)
    : observer_(std::move(observer)), id_(id) {}

MoQUpstreamGoaways::Watch::Watch(Watch&& other) noexcept
    : observer_(std::move(other.observer_)), id_(std::exchange(other.id_, 0)) {}

MoQUpstreamGoaways::Watch& MoQUpstreamGoaways::Watch::operator=(
    Watch&& other) noexcept {
  if (this != &other) {
    reset();
    observer_ = std::move(other.observer_);
    id_ = std::exchange(other.id_, 0);
  }
  return *this;
}

MoQUpstreamGoaways::Watch::~Watch() {
  reset();
}

void MoQUpstreamGoaways::Watch::reset() {
  if (auto observer = std::exchange(observer_, {}).lock()) {
    observer->remove(id_);
  }
  id_ = 0;
}

MoQUpstreamGoaways::Watch MoQUpstreamGoaways::watch(
    MoQSession& session,
    Callback callback) {
  auto& slot = observers_[session.sessionId()];
  auto observer = slot.lock();
  if (!observer) {
    observer = std::make_shared<SessionObserver>(
        weak_from_this(), session.sessionId());
    slot = observer;
    session.setSubscribeHandler(observer);
  }
  return Watch(observer, observer->add(std::move(callback)));
}

void MoQUpstreamGoaways::forget(SessionId sessionId) {
  auto it = observers_.find(sessionId);
  if (it != observers_.end() && it->second.expired()) {
    observers_.erase(it);
  }
}

} // namespace moxygen

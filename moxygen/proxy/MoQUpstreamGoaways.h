/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/Function.h>
#include <folly/container/F14Map.h>
#include <cstdint>
#include <memory>

#include "moxygen/MoQTypes.h"

namespace moxygen {

class MoQSession;

// Tells each track subscribed through an upstream session when that session
// sends a GOAWAY. A MoQSession has a single subscribe handler, so every track
// on a session shares one observer, installed as that handler. A session must
// be watched through one MoQUpstreamGoaways for its whole life and have no
// other subscribe handler: a second registry, or anything else that sets the
// handler, silently stops the first registry's watches. Not thread-safe: the
// registry, its tracks and the sessions they watch run on one executor.
class MoQUpstreamGoaways
    : public std::enable_shared_from_this<MoQUpstreamGoaways> {
 private:
  class SessionObserver;

 public:
  using Callback = folly::Function<void(const Goaway&)>;

  // Stops its callback when reset or destroyed.
  class Watch {
   public:
    Watch() = default;
    Watch(Watch&& other) noexcept;
    Watch& operator=(Watch&& other) noexcept;
    Watch(const Watch&) = delete;
    Watch& operator=(const Watch&) = delete;
    ~Watch();

    void reset();

   private:
    friend class MoQUpstreamGoaways;
    Watch(std::weak_ptr<SessionObserver> observer, uint64_t id);

    std::weak_ptr<SessionObserver> observer_;
    uint64_t id_{0};
  };

  // Replaces any subscribe handler the session already has with this
  // session's observer, unless the observer is already installed. A session
  // sends only one GOAWAY, so a watch on a session that is already going away
  // never fires.
  [[nodiscard]] Watch watch(MoQSession& session, Callback callback);

  // Sessions with a live observer.
  size_t watchedSessions() const {
    return observers_.size();
  }

 private:
  void forget(SessionId sessionId);

  folly::F14FastMap<SessionId, std::weak_ptr<SessionObserver>, SessionId::hash>
      observers_;
};

} // namespace moxygen

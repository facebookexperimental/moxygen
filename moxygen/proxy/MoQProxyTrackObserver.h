/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/ScopeGuard.h>

#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>

#include "moxygen/MoQConsumers.h"
#include "moxygen/MoQTypes.h"

namespace folly {
class IOBuf;
} // namespace folly

namespace moxygen {

struct MoQProxyObjectId {
  uint64_t group{0};
  uint64_t subgroup{0};
  uint64_t object{0};

  bool operator==(const MoQProxyObjectId&) const = default;
};

// Part of an upstream object, as it reaches the proxy and before it fans out.
struct MoQProxyObjectChunk {
  MoQProxyObjectId id;
  // The object's extensions on its first chunk; null on later ones.
  const Extensions* extensions{nullptr};
  // Null when the chunk carries no bytes.
  const folly::IOBuf* payload{nullptr};
  bool first{false};
  // The object's last chunk.
  bool complete{false};
};

// The proxy handed an object's last byte to at least one downstream
// subscription.
struct MoQProxyObjectForwarded {
  MoQProxyObjectId id;
  // The first of those writes, by the track's clock.
  std::chrono::steady_clock::time_point firstWriteAt;
};

// Watches one track: what its upstreams say about it and the objects it takes
// from them. Calls come on the track's executor, in the order the proxy handles
// them, and must not call back into the proxy.
class MoQProxyTrackObserver {
 public:
  virtual ~MoQProxyTrackObserver() = default;

  // The track extensions in the SUBSCRIBE_OK of each upstream the track comes
  // to read from: its first, and each that replaces it after a switch.
  virtual void onUpstreamExtensions(const Extensions& /* extensions */) {}

  // What downstream subscriptions are told about the track, given what its
  // first upstream said. Called once, after onUpstreamExtensions() for that
  // upstream; unchanged unless the observer rewrites it.
  virtual Extensions downstreamExtensions(Extensions upstream) {
    return upstream;
  }

  // Every chunk of every object the track takes from its upstream, once,
  // however many subscriptions it then goes to: objects on subgroups, object
  // streams and datagrams. Object statuses aren't reported.
  virtual void onObjectData(const MoQProxyObjectChunk& chunk) = 0;

  // Once the forwarder call that hands an object's last byte downstream
  // returns, if that call wrote it to any subscription. At most once per
  // object.
  virtual void onObjectForwarded(const MoQProxyObjectForwarded& forwarded) = 0;
};

// The observer of a track, made when the proxy creates the track; null leaves
// the track unobserved, at no cost.
using MoQProxyTrackObserverFactory =
    std::function<std::shared_ptr<MoQProxyTrackObserver>(const FullTrackName&)>;

// Connects a track to its observer: the track hands it each upstream chunk,
// and runs each forwarder call that writes an object's last byte through
// forwardLastByte(). The downstream consumers wrap() returns count the writes.
class MoQProxyObjectTap
    : public std::enable_shared_from_this<MoQProxyObjectTap> {
 public:
  using Clock = std::function<std::chrono::steady_clock::time_point()>;

  MoQProxyObjectTap(
      std::shared_ptr<MoQProxyTrackObserver> observer,
      Clock clock);

  void onUpstreamExtensions(const Extensions& extensions) {
    observer_->onUpstreamExtensions(extensions);
  }

  Extensions downstreamExtensions(Extensions upstream) {
    return observer_->downstreamExtensions(std::move(upstream));
  }

  void onChunk(const MoQProxyObjectChunk& chunk) {
    observer_->onObjectData(chunk);
  }

  template <typename Forward>
  auto forwardLastByte(const MoQProxyObjectId& id, Forward&& forward) {
    if (counting_) {
      return forward();
    }
    beginCounting();
    // A forward that throws reports nothing, and later objects still count.
    auto stopCounting = folly::makeGuard([this] { counting_ = false; });
    auto result = forward();
    stopCounting.dismiss();
    endCounting(id);
    return result;
  }

  // The consumer for a downstream subscription, which counts its writes.
  std::shared_ptr<TrackConsumer> wrap(std::shared_ptr<TrackConsumer> consumer);

  // A downstream subscription accepted a write.
  void onDownstreamWrite();

 private:
  void beginCounting();
  void endCounting(const MoQProxyObjectId& id);

  std::shared_ptr<MoQProxyTrackObserver> observer_;
  Clock clock_;
  bool counting_{false};
  bool wrote_{false};
  std::chrono::steady_clock::time_point firstWriteAt_;
};

} // namespace moxygen

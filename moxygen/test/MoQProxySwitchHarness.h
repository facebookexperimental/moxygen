/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <chrono>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "moxygen/MoQSession.h"
#include "moxygen/proxy/MoQUpstreamProvider.h"

// Building blocks for tests that move a MoQProxy track between upstreams:
// hubs that publish known content and inject faults, a provider that routes
// the proxy to them, and a viewer that checks everything it receives.

namespace moxygen::test {

// Test extensions on every object: the ID of the hub that published it, and
// the hub's ID for the subscription that carried it.
constexpr uint64_t kHubIdExtensionType = 0x7E0;
constexpr uint64_t kSubscriptionIdExtensionType = 0x7E2;

// How every hub lays out a group.
struct SwitchTrackShape {
  enum class Delivery { SubgroupStreams, ObjectStreams, Datagrams };

  uint64_t objectsPerGroup{4};
  // On subgroup streams, object o of a group belongs to subgroup
  // o % subgroupsPerGroup.
  uint64_t subgroupsPerGroup{1};
  Delivery delivery{Delivery::SubgroupStreams};

  // Each object stream is a subgroup of its own, named after its object.
  uint64_t subgroupOf(uint64_t objectID) const {
    return delivery == Delivery::ObjectStreams ? objectID
                                               : objectID % subgroupsPerGroup;
  }
  bool startsSubgroup(uint64_t objectID) const {
    return delivery == Delivery::ObjectStreams || objectID < subgroupsPerGroup;
  }
  bool endsSubgroup(uint64_t objectID) const {
    return delivery == Delivery::ObjectStreams ||
        objectID + subgroupsPerGroup >= objectsPerGroup;
  }
};

// What the hubs publish, and what viewers check against. An object's payload
// is a pure function of its track and location, so a viewer can verify it
// whichever hub sent it.
class SwitchOrigin {
 public:
  explicit SwitchOrigin(SwitchTrackShape shape = {}) : shape_(shape) {}

  static std::string payload(
      const FullTrackName& track,
      AbsoluteLocation location);

  const SwitchTrackShape& shape() const {
    return shape_;
  }

  // Every location any hub has published on the track.
  const std::set<AbsoluteLocation>& published(const FullTrackName& track);
  void notePublished(const FullTrackName& track, AbsoluteLocation location);

 private:
  SwitchTrackShape shape_;
  std::map<FullTrackName, std::set<AbsoluteLocation>> published_;
};

// A hub, or an origin, serving any track to whoever subscribes. A subscription
// gets the objects its filter selects, resolved against the largest object
// published when it arrives. Tests publish through the hub and inject faults
// on the sessions it is attached to. Once a session is attached, subscribe
// only through a session: the hub looks up the request's session, which
// MoQSession::getRequestSession() XCHECKs outside one.
class SwitchTestHub : public Publisher,
                      public std::enable_shared_from_this<SwitchTestHub> {
 public:
  SwitchTestHub(uint64_t id, std::shared_ptr<SwitchOrigin> origin)
      : id_(id), origin_(std::move(origin)) {}

  folly::coro::Task<SubscribeResult> subscribe(
      SubscribeRequest request,
      std::shared_ptr<TrackConsumer> consumer) override;

  // Publishes objects [0, endObject) of a group to every subscription to the
  // track, continuing where an earlier call stopped. A subgroup stream ends
  // with the subgroup's last object.
  void publish(const FullTrackName& track, uint64_t group, uint64_t endObject);
  void publishGroup(const FullTrackName& track, uint64_t group);

  // The hub serves on the server side of these sessions.
  void attach(const std::shared_ptr<MoQSession>& session);

  void sendGoaway(const Goaway& goaway);
  // A request-stream GOAWAY on each subscription to the track (draft 18+).
  void sendRequestGoaway(
      const FullTrackName& track,
      std::chrono::milliseconds timeout = {});
  // Resets the subscriptions' open subgroup streams, then sends PUBLISH_DONE,
  // as a session does when a request-stream GOAWAY times out. A relay passing
  // on its upstream's PUBLISH_DONE lets open streams finish instead.
  void endSubscriptions(
      const FullTrackName& track,
      PublishDoneStatusCode statusCode,
      ResetStreamErrorCode reset = ResetStreamErrorCode::CANCELLED);
  void closeSessions(
      SessionCloseErrorCode error = SessionCloseErrorCode::NO_ERROR);

  size_t subscriptions(const FullTrackName& track) const;
  // The filter of every SUBSCRIBE the hub has received for the track, in
  // order.
  std::vector<LocationFilter> subscribeFilters(
      const FullTrackName& track) const;

  uint64_t id() const {
    return id_;
  }

 private:
  class Handle;
  struct Subscription {
    FullTrackName track;
    RequestID requestID;
    SubscribeRange range;
    std::weak_ptr<MoQSession> session;
    std::shared_ptr<TrackConsumer> consumer;
    // Subgroup streams still open, by group and subgroup.
    std::map<std::pair<uint64_t, uint64_t>, std::shared_ptr<SubgroupConsumer>>
        openSubgroups;
  };

  void publishObject(
      uint64_t subscriptionID,
      Subscription& subscription,
      AbsoluteLocation location);
  std::vector<uint64_t> subscriptionsTo(const FullTrackName& track) const;
  void unsubscribe(uint64_t subscriptionID);

  uint64_t id_;
  std::shared_ptr<SwitchOrigin> origin_;
  uint64_t nextSubscriptionID_{0};
  std::map<uint64_t, Subscription> subscriptions_;
  std::map<std::pair<FullTrackName, uint64_t>, uint64_t> nextObject_;
  std::map<FullTrackName, AbsoluteLocation> largest_;
  std::map<FullTrackName, std::vector<LocationFilter>> filters_;
  std::vector<std::weak_ptr<MoQSession>> sessions_;
};

// Routes the proxy to the first available upstream, as a consistent-hash pool
// keeps choosing one hub until health checks mark it down. An upstream whose
// session closed is skipped, as a pool drops a closed connection.
class ScriptedUpstreamProvider : public MoQUpstreamProvider {
 public:
  struct Attempt {
    // The upstream returned; none if the attempt failed.
    std::optional<size_t> upstream;
    bool hasFallbackProvider{false};
  };

  // Returns the upstream's index.
  size_t addUpstream(std::shared_ptr<MoQSession> session);
  void setAvailable(size_t upstream, bool available);
  void failNextAttempts(size_t count);

  const std::vector<Attempt>& attempts() const {
    return attempts_;
  }

  folly::coro::Task<MoQUpstreamSessionResult> getSession(
      const FullTrackName& fullTrackName,
      const TrackRequestParameters& params,
      bool hasFallbackProvider) override;

 private:
  struct Upstream {
    std::shared_ptr<MoQSession> session;
    bool available{true};
  };

  std::vector<Upstream> upstreams_;
  size_t failuresLeft_{0};
  std::vector<Attempt> attempts_;
};

// A viewer that checks each object against the origin as it arrives.
class CheckingViewer : public TrackConsumer,
                       public std::enable_shared_from_this<CheckingViewer> {
 public:
  CheckingViewer(std::shared_ptr<SwitchOrigin> origin, FullTrackName track)
      : origin_(std::move(origin)), track_(std::move(track)) {}

  folly::Expected<folly::Unit, MoQPublishError> setTrackAlias(
      TrackAlias) override {
    return folly::unit;
  }
  folly::Expected<std::shared_ptr<SubgroupConsumer>, MoQPublishError>
  beginSubgroup(
      uint64_t groupID,
      uint64_t subgroupID,
      Priority priority,
      BeginSubgroupOptions options) override;
  folly::Expected<folly::SemiFuture<folly::Unit>, MoQPublishError>
  awaitStreamCredit() override {
    return folly::makeSemiFuture();
  }
  folly::Expected<folly::Unit, MoQPublishError> objectStream(
      const ObjectHeader& header,
      Payload payload,
      bool lastInGroup) override;
  folly::Expected<folly::Unit, MoQPublishError> datagram(
      const ObjectHeader& header,
      Payload payload,
      bool lastInGroup) override;
  folly::Expected<folly::Unit, MoQPublishError> publishDone(
      PublishDone pubDone) override;

  // Where objects come from: a hub, and its subscription that carried them.
  struct Source {
    uint64_t hub;
    uint64_t subscription;

    auto operator<=>(const Source&) const = default;
  };

  // A reset subgroup stream.
  struct Reset {
    uint64_t group;
    uint64_t subgroup;
    ResetStreamErrorCode error;

    bool operator==(const Reset&) const = default;
  };

  // A subgroup stream, as it was opened.
  struct OpenedSubgroup {
    uint64_t group;
    uint64_t subgroup;
    bool beginsWithFirstObject;

    bool operator==(const OpenedSubgroup&) const = default;
  };

  // Corrupt, duplicate or out-of-order objects, objects before expectedStart,
  // objects on the wrong subgroup, data or a second end on a subgroup stream
  // that already ended, groups
  // mixing sources, a source whose first group is not the one the viewer starts
  // in (expectedStart's, else its lowest) but lacks its first object, any
  // PUBLISH_DONE or stream reset the test did not expect, a second
  // PUBLISH_DONE, and an expected one that never arrived. Check it once the run
  // settles: an object 0 still in flight shows up as a violation until it
  // arrives.
  std::vector<std::string> violations() const;
  // Objects the origin published at or after expectedStart, or without one the
  // first object received, that never arrived. Empty if nothing arrived and
  // there is no expectedStart, so check received() too.
  std::vector<AbsoluteLocation> missing() const;

  const std::vector<AbsoluteLocation>& received() const {
    return received_;
  }
  std::map<uint64_t, uint64_t> hubOfGroup() const;
  const std::map<uint64_t, Source>& sourceOfGroup() const {
    return sourceOfGroup_;
  }
  const std::optional<PublishDone>& done() const {
    return done_;
  }
  // Subgroup streams, by group and subgroup, that neither finished nor were
  // reset.
  std::vector<std::pair<uint64_t, uint64_t>> unfinishedSubgroups() const;
  const std::vector<Reset>& resets() const {
    return resets_;
  }
  const std::vector<OpenedSubgroup>& openedSubgroups() const {
    return opened_;
  }

  // Groups that may reach the viewer without their first objects, exempt from
  // the object-0 check; missing() still counts what they lack.
  std::set<uint64_t> expectedGapGroups;
  // The PUBLISH_DONE the viewer must get, if any.
  std::optional<PublishDoneStatusCode> expectedEnd;
  std::optional<ResetStreamErrorCode> expectedReset;
  // Where the viewer's objects should start: an object before it is a
  // violation, and missing() counts from it.
  std::optional<AbsoluteLocation> expectedStart;

 private:
  class Subgroup;

  void receive(
      AbsoluteLocation location,
      const Payload& payload,
      const Extensions& extensions);
  void onReset(uint64_t group, uint64_t subgroup, ResetStreamErrorCode error);

  std::shared_ptr<SwitchOrigin> origin_;
  FullTrackName track_;
  std::vector<std::string> violations_;
  std::vector<AbsoluteLocation> received_;
  std::set<AbsoluteLocation> receivedSet_;
  std::map<uint64_t, Source> sourceOfGroup_;
  // Each source's lowest group.
  std::map<Source, uint64_t> firstGroupOf_;
  std::multiset<std::pair<uint64_t, uint64_t>> openSubgroups_;
  std::vector<OpenedSubgroup> opened_;
  std::vector<Reset> resets_;
  std::optional<PublishDone> done_;
};

} // namespace moxygen::test

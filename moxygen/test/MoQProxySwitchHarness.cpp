/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/test/MoQProxySwitchHarness.h"

#include <fmt/core.h>
#include <algorithm>

namespace moxygen::test {
namespace {

std::string describe(AbsoluteLocation location) {
  return fmt::format("{}/{}", location.group, location.object);
}

std::string bytesOf(const Payload& payload) {
  return payload ? payload->to<std::string>() : std::string();
}

} // namespace

std::string SwitchOrigin::payload(
    const FullTrackName& track,
    AbsoluteLocation location) {
  return fmt::format("{} {}", track.describe(), describe(location));
}

const std::set<AbsoluteLocation>& SwitchOrigin::published(
    const FullTrackName& track) {
  return published_[track];
}

void SwitchOrigin::notePublished(
    const FullTrackName& track,
    AbsoluteLocation location) {
  published_[track].insert(location);
}

class SwitchTestHub::Handle final : public Publisher::SubscriptionHandle {
 public:
  Handle(SubscribeOk ok, std::weak_ptr<SwitchTestHub> hub, uint64_t id)
      : SubscriptionHandle(std::move(ok)), hub_(std::move(hub)), id_(id) {}

  void unsubscribe() override {
    if (auto hub = hub_.lock()) {
      hub->unsubscribe(id_);
    }
  }

  folly::coro::Task<RequestUpdateResult> requestUpdate(
      RequestUpdate update) override {
    co_return folly::makeUnexpected(
        RequestError{
            update.requestID,
            RequestErrorCode::NOT_SUPPORTED,
            "test hub takes no updates"});
  }

 private:
  std::weak_ptr<SwitchTestHub> hub_;
  uint64_t id_;
};

folly::coro::Task<Publisher::SubscribeResult> SwitchTestHub::subscribe(
    SubscribeRequest request,
    std::shared_ptr<TrackConsumer> consumer) {
  const TrackAlias alias(request.requestID.value);
  consumer->setTrackAlias(alias);
  SubscribeOk ok{
      .requestID = request.requestID,
      .trackAlias = alias,
      .groupOrder = GroupOrder::OldestFirst};
  if (auto largest = largest_.find(request.fullTrackName);
      largest != largest_.end()) {
    ok.largest = largest->second;
  }
  filters_[request.fullTrackName].push_back(request.filter);
  // Draft 16+ carries the publisher's group order only as a track property.
  ok.extensions.insertMutableExtension(Extension(
      kPublisherGroupOrderExtensionType,
      folly::to_underlying(GroupOrder::OldestFirst)));
  const auto id = nextSubscriptionID_++;
  subscriptions_.emplace(
      id,
      Subscription{
          .track = request.fullTrackName,
          .requestID = request.requestID,
          .range = request.filter.resolve(ok.largest, /*isFetch=*/false),
          // Only a session's request context has a request session.
          .session =
              sessions_.empty() ? nullptr : MoQSession::getRequestSession(),
          .consumer = std::move(consumer)});
  co_return std::make_shared<Handle>(std::move(ok), weak_from_this(), id);
}

void SwitchTestHub::publish(
    const FullTrackName& track,
    uint64_t group,
    uint64_t endObject) {
  auto& next = nextObject_[{track, group}];
  for (; next < endObject; ++next) {
    const AbsoluteLocation location{group, next};
    origin_->notePublished(track, location);
    auto [largest, inserted] = largest_.try_emplace(track, location);
    if (!inserted) {
      largest->second = std::max(largest->second, location);
    }
    // Publishing can end a subscription.
    for (auto id : subscriptionsTo(track)) {
      auto it = subscriptions_.find(id);
      if (it == subscriptions_.end()) {
        continue;
      }
      const auto& range = it->second.range;
      if (range.start <= location && location <= range.end) {
        publishObject(id, it->second, location);
      }
    }
  }
}

void SwitchTestHub::publishGroup(const FullTrackName& track, uint64_t group) {
  publish(track, group, origin_->shape().objectsPerGroup);
}

void SwitchTestHub::publishObject(
    uint64_t subscriptionID,
    Subscription& subscription,
    AbsoluteLocation location) {
  const auto& shape = origin_->shape();
  const auto subgroup = shape.subgroupOf(location.object);
  const bool lastInGroup = location.object + 1 == shape.objectsPerGroup;
  auto payload = folly::IOBuf::copyBuffer(
      SwitchOrigin::payload(subscription.track, location));
  Extensions extensions;
  extensions.insertMutableExtension(Extension(kHubIdExtensionType, id_));
  extensions.insertMutableExtension(
      Extension(kSubscriptionIdExtensionType, subscriptionID));
  switch (shape.delivery) {
    case SwitchTrackShape::Delivery::Datagrams:
      subscription.consumer->datagram(
          ObjectHeader(
              location.group,
              subgroup,
              location.object,
              kDefaultPriority,
              ObjectStatus::NORMAL,
              std::move(extensions)),
          std::move(payload),
          lastInGroup);
      return;
    case SwitchTrackShape::Delivery::ObjectStreams:
      subscription.consumer->objectStream(
          ObjectHeader(
              location.group,
              subgroup,
              location.object,
              kDefaultPriority,
              ObjectStatus::NORMAL,
              std::move(extensions)),
          std::move(payload),
          lastInGroup);
      return;
    case SwitchTrackShape::Delivery::SubgroupStreams:
      break;
  }
  const auto key = std::make_pair(location.group, subgroup);
  auto open = subscription.openSubgroups.find(key);
  if (open == subscription.openSubgroups.end()) {
    TrackConsumer::BeginSubgroupOptions options;
    // A subscription that starts mid-group opens streams mid-subgroup.
    options.beginsWithFirstObject = shape.startsSubgroup(location.object);
    options.containsLastInGroup =
        shape.subgroupOf(shape.objectsPerGroup - 1) == subgroup;
    auto stream = subscription.consumer->beginSubgroup(
        location.group, subgroup, kDefaultPriority, options);
    if (stream.hasError()) {
      return;
    }
    open = subscription.openSubgroups.emplace(key, stream.value()).first;
  }
  const bool fin = shape.endsSubgroup(location.object);
  open->second->object(
      location.object, std::move(payload), std::move(extensions), fin);
  if (fin) {
    subscription.openSubgroups.erase(open);
  }
}

void SwitchTestHub::attach(const std::shared_ptr<MoQSession>& session) {
  sessions_.push_back(session);
}

void SwitchTestHub::sendGoaway(const Goaway& goaway) {
  for (const auto& weakSession : sessions_) {
    if (auto session = weakSession.lock()) {
      session->goaway(goaway);
    }
  }
}

void SwitchTestHub::sendRequestGoaway(
    const FullTrackName& track,
    std::chrono::milliseconds timeout) {
  for (auto id : subscriptionsTo(track)) {
    const auto& subscription = subscriptions_.at(id);
    if (auto session = subscription.session.lock()) {
      session->requestStreamGoaway(subscription.requestID, "", timeout);
    }
  }
}

void SwitchTestHub::endSubscriptions(
    const FullTrackName& track,
    PublishDoneStatusCode statusCode,
    ResetStreamErrorCode reset) {
  for (auto id : subscriptionsTo(track)) {
    auto it = subscriptions_.find(id);
    if (it == subscriptions_.end()) {
      continue;
    }
    auto consumer = std::move(it->second.consumer);
    auto requestID = it->second.requestID;
    auto openSubgroups = std::move(it->second.openSubgroups);
    subscriptions_.erase(it);
    for (auto& [_, subgroup] : openSubgroups) {
      subgroup->reset(reset);
    }
    consumer->publishDone(
        PublishDone{requestID, statusCode, 0, "test hub ended the track"});
  }
}

void SwitchTestHub::closeSessions(SessionCloseErrorCode error) {
  for (const auto& weakSession : std::exchange(sessions_, {})) {
    if (auto session = weakSession.lock()) {
      session->close(error);
    }
  }
}

size_t SwitchTestHub::subscriptions(const FullTrackName& track) const {
  return subscriptionsTo(track).size();
}

std::vector<LocationFilter> SwitchTestHub::subscribeFilters(
    const FullTrackName& track) const {
  auto it = filters_.find(track);
  return it == filters_.end() ? std::vector<LocationFilter>{} : it->second;
}

std::vector<uint64_t> SwitchTestHub::subscriptionsTo(
    const FullTrackName& track) const {
  std::vector<uint64_t> ids;
  for (const auto& [id, subscription] : subscriptions_) {
    if (subscription.track == track) {
      ids.push_back(id);
    }
  }
  return ids;
}

void SwitchTestHub::unsubscribe(uint64_t subscriptionID) {
  subscriptions_.erase(subscriptionID);
}

size_t ScriptedUpstreamProvider::addUpstream(
    std::shared_ptr<MoQSession> session) {
  upstreams_.push_back(Upstream{.session = std::move(session)});
  return upstreams_.size() - 1;
}

void ScriptedUpstreamProvider::setAvailable(size_t upstream, bool available) {
  upstreams_.at(upstream).available = available;
}

void ScriptedUpstreamProvider::failNextAttempts(size_t count) {
  failuresLeft_ = count;
}

folly::coro::Task<MoQUpstreamSessionResult>
ScriptedUpstreamProvider::getSession(
    const FullTrackName&,
    const TrackRequestParameters&,
    bool) {
  auto& attempt = attempts_.emplace_back();
  if (failuresLeft_ > 0) {
    --failuresLeft_;
    co_return folly::makeUnexpected(
        MoQUpstreamProviderError{"scripted provider failure"});
  }
  for (size_t i = 0; i < upstreams_.size(); i++) {
    if (upstreams_[i].available) {
      attempt.upstream = i;
      co_return upstreams_[i].session;
    }
  }
  co_return folly::makeUnexpected(
      MoQUpstreamProviderError{"no upstream available"});
}

// Checks one subgroup stream: its objects belong to its subgroup and arrive
// in increasing ID order, and it ends exactly once.
class CheckingViewer::Subgroup final : public SubgroupConsumer {
 public:
  Subgroup(
      std::shared_ptr<CheckingViewer> viewer,
      uint64_t group,
      uint64_t subgroup)
      : viewer_(std::move(viewer)), group_(group), subgroup_(subgroup) {
    viewer_->openSubgroups_.emplace(group_, subgroup_);
  }

  folly::Expected<folly::Unit, MoQPublishError> object(
      uint64_t objectID,
      Payload payload,
      Extensions extensions,
      bool finSubgroup) override {
    deliver(objectID, payload, extensions);
    if (finSubgroup) {
      end();
    }
    return folly::unit;
  }

  folly::Expected<folly::Unit, MoQPublishError> beginObject(
      uint64_t objectID,
      uint64_t length,
      Payload initialPayload,
      Extensions extensions) override {
    partial_ = Partial{
        objectID, length, std::move(initialPayload), std::move(extensions)};
    return completePartial();
  }

  folly::Expected<ObjectPublishStatus, MoQPublishError> objectPayload(
      Payload payload,
      bool finSubgroup) override {
    if (!partial_) {
      viewer_->violations_.push_back(
          fmt::format("payload without an object in group {}", group_));
      return ObjectPublishStatus::DONE;
    }
    if (partial_->payload) {
      partial_->payload->appendToChain(std::move(payload));
    } else {
      partial_->payload = std::move(payload);
    }
    completePartial();
    if (finSubgroup) {
      end();
    }
    return partial_ ? ObjectPublishStatus::IN_PROGRESS
                    : ObjectPublishStatus::DONE;
  }

  folly::Expected<folly::Unit, MoQPublishError> endOfGroup(uint64_t) override {
    end();
    return folly::unit;
  }
  folly::Expected<folly::Unit, MoQPublishError> endOfTrackAndGroup(
      uint64_t) override {
    end();
    return folly::unit;
  }
  folly::Expected<folly::Unit, MoQPublishError> endOfSubgroup() override {
    end();
    return folly::unit;
  }

  void reset(ResetStreamErrorCode error) override {
    if (end()) {
      viewer_->onReset(group_, subgroup_, error);
    }
  }

 private:
  struct Partial {
    uint64_t objectID;
    uint64_t length;
    Payload payload;
    Extensions extensions;
  };

  // Returns false, and flags it, if the stream already ended.
  bool end() {
    if (ended_) {
      viewer_->violations_.push_back(
          fmt::format("subgroup {}/{} ended twice", group_, subgroup_));
      return false;
    }
    ended_ = true;
    viewer_->openSubgroups_.erase(
        viewer_->openSubgroups_.find({group_, subgroup_}));
    return true;
  }

  folly::Expected<folly::Unit, MoQPublishError> completePartial() {
    const auto received =
        partial_->payload ? partial_->payload->computeChainDataLength() : 0;
    if (received >= partial_->length) {
      auto partial = std::move(*partial_);
      partial_.reset();
      deliver(partial.objectID, partial.payload, partial.extensions);
    }
    return folly::unit;
  }

  void deliver(
      uint64_t objectID,
      const Payload& payload,
      const Extensions& extensions) {
    if (ended_) {
      viewer_->violations_.push_back(
          fmt::format(
              "object {}/{} after its subgroup ended", group_, objectID));
    }
    if (viewer_->origin_->shape().subgroupOf(objectID) != subgroup_) {
      viewer_->violations_.push_back(
          fmt::format(
              "object {}/{} on subgroup {}", group_, objectID, subgroup_));
    }
    if (lastObject_ && objectID <= *lastObject_) {
      viewer_->violations_.push_back(
          fmt::format(
              "object {}/{} after object {} in its subgroup",
              group_,
              objectID,
              *lastObject_));
    }
    lastObject_ = objectID;
    viewer_->receive({group_, objectID}, payload, extensions);
  }

  std::shared_ptr<CheckingViewer> viewer_;
  uint64_t group_;
  uint64_t subgroup_;
  std::optional<uint64_t> lastObject_;
  std::optional<Partial> partial_;
  bool ended_{false};
};

folly::Expected<std::shared_ptr<SubgroupConsumer>, MoQPublishError>
CheckingViewer::beginSubgroup(
    uint64_t groupID,
    uint64_t subgroupID,
    Priority,
    BeginSubgroupOptions options) {
  opened_.push_back(
      OpenedSubgroup{groupID, subgroupID, options.beginsWithFirstObject});
  return std::make_shared<Subgroup>(shared_from_this(), groupID, subgroupID);
}

folly::Expected<folly::Unit, MoQPublishError> CheckingViewer::objectStream(
    const ObjectHeader& header,
    Payload payload,
    bool) {
  if (origin_->shape().subgroupOf(header.id) != header.subgroup) {
    violations_.push_back(
        fmt::format(
            "object {}/{} on subgroup {}",
            header.group,
            header.id,
            header.subgroup));
  }
  receive({header.group, header.id}, payload, header.extensions);
  return folly::unit;
}

folly::Expected<folly::Unit, MoQPublishError>
CheckingViewer::datagram(const ObjectHeader& header, Payload payload, bool) {
  receive({header.group, header.id}, payload, header.extensions);
  return folly::unit;
}

folly::Expected<folly::Unit, MoQPublishError> CheckingViewer::publishDone(
    PublishDone pubDone) {
  if (done_) {
    violations_.push_back(
        fmt::format(
            "second PUBLISH_DONE {}: {}",
            folly::to_underlying(pubDone.statusCode),
            pubDone.reasonPhrase));
    return folly::unit;
  }
  if (pubDone.statusCode != expectedEnd) {
    violations_.push_back(
        fmt::format(
            "unexpected PUBLISH_DONE {}: {}",
            folly::to_underlying(pubDone.statusCode),
            pubDone.reasonPhrase));
  }
  done_ = std::move(pubDone);
  return folly::unit;
}

std::vector<std::string> CheckingViewer::violations() const {
  auto violations = violations_;
  if (expectedStart) {
    for (auto it = receivedSet_.begin();
         it != receivedSet_.end() && *it < *expectedStart;
         ++it) {
      violations.push_back(
          fmt::format(
              "object {} before the expected start {}",
              describe(*it),
              describe(*expectedStart)));
    }
  }
  if (expectedEnd && !done_) {
    violations.push_back(
        fmt::format(
            "expected PUBLISH_DONE {} never arrived",
            folly::to_underlying(*expectedEnd)));
  }
  // Every source starts at a group boundary, except one whose first group is
  // the group the viewer starts in. Checked here, because object 0 may arrive
  // after later objects of its group, or after another source's objects.
  for (const auto& [source, group] : firstGroupOf_) {
    const auto startGroup =
        expectedStart ? expectedStart->group : receivedSet_.begin()->group;
    if (group != startGroup && !receivedSet_.contains({group, 0})) {
      violations.push_back(
          fmt::format(
              "hub {} subscription {} started in group {} without object 0",
              source.hub,
              source.subscription,
              group));
    }
  }
  return violations;
}

std::map<uint64_t, uint64_t> CheckingViewer::hubOfGroup() const {
  std::map<uint64_t, uint64_t> hubs;
  for (const auto& [group, source] : sourceOfGroup_) {
    hubs.emplace(group, source.hub);
  }
  return hubs;
}

std::vector<std::pair<uint64_t, uint64_t>> CheckingViewer::unfinishedSubgroups()
    const {
  return {openSubgroups_.begin(), openSubgroups_.end()};
}

std::vector<AbsoluteLocation> CheckingViewer::missing() const {
  std::vector<AbsoluteLocation> missing;
  if (!expectedStart && receivedSet_.empty()) {
    return missing;
  }
  const auto& published = origin_->published(track_);
  for (auto it = published.lower_bound(
           expectedStart ? *expectedStart : *receivedSet_.begin());
       it != published.end();
       ++it) {
    if (!receivedSet_.contains(*it)) {
      missing.push_back(*it);
    }
  }
  return missing;
}

void CheckingViewer::receive(
    AbsoluteLocation location,
    const Payload& payload,
    const Extensions& extensions) {
  received_.push_back(location);
  if (!receivedSet_.insert(location).second) {
    violations_.push_back(
        fmt::format("object {} received twice", describe(location)));
  }
  if (bytesOf(payload) != SwitchOrigin::payload(track_, location)) {
    violations_.push_back(
        fmt::format(
            "object {} has payload '{}'",
            describe(location),
            bytesOf(payload)));
  }
  auto hub = extensions.getIntExtension(kHubIdExtensionType);
  auto subscription = extensions.getIntExtension(kSubscriptionIdExtensionType);
  if (!hub || !subscription) {
    violations_.push_back(
        fmt::format("object {} has no source", describe(location)));
    return;
  }
  const Source source{*hub, *subscription};
  auto [firstGroup, newSource] =
      firstGroupOf_.try_emplace(source, location.group);
  if (!newSource) {
    firstGroup->second = std::min(firstGroup->second, location.group);
  }
  auto [groupSource, inserted] =
      sourceOfGroup_.try_emplace(location.group, source);
  if (inserted || groupSource->second == source) {
    return;
  }
  if (groupSource->second.hub != source.hub) {
    violations_.push_back(
        fmt::format(
            "group {} came from hubs {} and {}",
            location.group,
            groupSource->second.hub,
            source.hub));
  } else {
    violations_.push_back(
        fmt::format(
            "group {} came from subscriptions {} and {} on hub {}",
            location.group,
            groupSource->second.subscription,
            source.subscription,
            source.hub));
  }
}

void CheckingViewer::onReset(
    uint64_t group,
    uint64_t subgroup,
    ResetStreamErrorCode error) {
  resets_.push_back(Reset{group, subgroup, error});
  if (error != expectedReset) {
    violations_.push_back(
        fmt::format(
            "subgroup {}/{} reset with {}",
            group,
            subgroup,
            folly::to_underlying(error)));
  }
}

} // namespace moxygen::test

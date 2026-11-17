/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/proxy/MoQProxyTrackObserver.h"

#include <utility>

#include "moxygen/MoQFilters.h"

namespace moxygen {
namespace {

template <typename T>
folly::Expected<T, MoQPublishError> counted(
    MoQProxyObjectTap& tap,
    folly::Expected<T, MoQPublishError> result) {
  if (result.hasValue()) {
    tap.onDownstreamWrite();
  }
  return result;
}

class CountingSubgroupConsumer final : public SubgroupConsumerFilter {
 public:
  CountingSubgroupConsumer(
      std::shared_ptr<SubgroupConsumer> downstream,
      std::shared_ptr<MoQProxyObjectTap> tap)
      : SubgroupConsumerFilter(std::move(downstream)), tap_(std::move(tap)) {}

  folly::Expected<folly::Unit, MoQPublishError> object(
      uint64_t objectID,
      Payload payload,
      Extensions extensions,
      bool finSubgroup) override {
    return counted(
        *tap_,
        downstream_->object(
            objectID, std::move(payload), std::move(extensions), finSubgroup));
  }

  folly::Expected<folly::Unit, MoQPublishError> beginObject(
      uint64_t objectID,
      uint64_t length,
      Payload initialPayload,
      Extensions extensions) override {
    return counted(
        *tap_,
        downstream_->beginObject(
            objectID,
            length,
            std::move(initialPayload),
            std::move(extensions)));
  }

  folly::Expected<ObjectPublishStatus, MoQPublishError> objectPayload(
      Payload payload,
      bool finSubgroup) override {
    return counted(
        *tap_, downstream_->objectPayload(std::move(payload), finSubgroup));
  }

 private:
  std::shared_ptr<MoQProxyObjectTap> tap_;
};

class CountingTrackConsumer final : public TrackConsumerFilter {
 public:
  CountingTrackConsumer(
      std::shared_ptr<TrackConsumer> downstream,
      std::shared_ptr<MoQProxyObjectTap> tap)
      : TrackConsumerFilter(std::move(downstream)), tap_(std::move(tap)) {}

  folly::Expected<std::shared_ptr<SubgroupConsumer>, MoQPublishError>
  beginSubgroup(
      uint64_t groupID,
      uint64_t subgroupID,
      Priority priority,
      BeginSubgroupOptions options) override {
    auto subgroup =
        downstream_->beginSubgroup(groupID, subgroupID, priority, options);
    if (subgroup.hasError()) {
      return subgroup;
    }
    return std::make_shared<CountingSubgroupConsumer>(
        std::move(subgroup.value()), tap_);
  }

  folly::Expected<folly::Unit, MoQPublishError> objectStream(
      const ObjectHeader& header,
      Payload payload,
      bool lastInGroup) override {
    return counted(
        *tap_,
        downstream_->objectStream(header, std::move(payload), lastInGroup));
  }

  folly::Expected<folly::Unit, MoQPublishError> datagram(
      const ObjectHeader& header,
      Payload payload,
      bool lastInGroup) override {
    return counted(
        *tap_, downstream_->datagram(header, std::move(payload), lastInGroup));
  }

  folly::Expected<std::shared_ptr<FetchConsumer>, MoQPublishError> beginFill(
      RequestID requestID) override {
    return downstream_->beginFill(requestID);
  }

  void goaway(Goaway goaway) override {
    downstream_->goaway(std::move(goaway));
  }

 private:
  std::shared_ptr<MoQProxyObjectTap> tap_;
};

} // namespace

MoQProxyObjectTap::MoQProxyObjectTap(
    std::shared_ptr<MoQProxyTrackObserver> observer,
    Clock clock)
    : observer_(std::move(observer)), clock_(std::move(clock)) {}

std::shared_ptr<TrackConsumer> MoQProxyObjectTap::wrap(
    std::shared_ptr<TrackConsumer> consumer) {
  return std::make_shared<CountingTrackConsumer>(
      std::move(consumer), shared_from_this());
}

void MoQProxyObjectTap::onDownstreamWrite() {
  if (!counting_) {
    return;
  }
  if (!wrote_) {
    wrote_ = true;
    firstWriteAt_ = clock_();
  }
}

void MoQProxyObjectTap::beginCounting() {
  counting_ = true;
  wrote_ = false;
}

void MoQProxyObjectTap::endCounting(const MoQProxyObjectId& id) {
  counting_ = false;
  if (!wrote_) {
    return;
  }
  observer_->onObjectForwarded(
      MoQProxyObjectForwarded{.id = id, .firstWriteAt = firstWriteAt_});
}

} // namespace moxygen

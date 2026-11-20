/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <chrono>
#include <functional>
#include <memory>
#include <optional>
#include <utility>

#include "moxygen/MoQConsumers.h"
#include "moxygen/stats/MoQProxyStats.h"

namespace moxygen {

// Tracks response exposure and first-object timing for one upstream FETCH.
class MoQProxyFetchConsumer final : public FetchConsumer {
 public:
  using Clock = std::function<std::chrono::steady_clock::time_point()>;

  MoQProxyFetchConsumer(
      std::shared_ptr<FetchConsumer> consumer,
      std::shared_ptr<MoQProxyStatsCallback> statsCallback,
      Clock clock = std::chrono::steady_clock::now)
      : consumer_(std::move(consumer)),
        statsCallback_(std::move(statsCallback)),
        clock_(std::move(clock)) {}

  void startTimeToFirstObject() {
    if (statsCallback_) {
      startedAt_ = clock_();
    }
  }

  bool exposed() const {
    return exposed_;
  }

  // For a refused FETCH whose stream may still deliver once another upstream
  // serves the consumer.
  void detach() {
    startedAt_.reset();
    consumer_.reset();
  }

  folly::Expected<folly::Unit, MoQPublishError> object(
      uint64_t groupID,
      uint64_t subgroupID,
      uint64_t objectID,
      Payload payload,
      Extensions extensions,
      bool finFetch,
      bool forwardingPreferenceIsDatagram) override {
    if (!consumer_) {
      return detached();
    }
    recordFirstObject();
    exposed_ = true;
    return consumer_->object(
        groupID,
        subgroupID,
        objectID,
        std::move(payload),
        std::move(extensions),
        finFetch,
        forwardingPreferenceIsDatagram);
  }

  void checkpoint() override {
    if (consumer_) {
      consumer_->checkpoint();
    }
  }

  folly::Expected<folly::Unit, MoQPublishError> beginObject(
      uint64_t groupID,
      uint64_t subgroupID,
      uint64_t objectID,
      uint64_t length,
      Payload initialPayload,
      Extensions extensions) override {
    if (!consumer_) {
      return detached();
    }
    recordFirstObject();
    exposed_ = true;
    return consumer_->beginObject(
        groupID,
        subgroupID,
        objectID,
        length,
        std::move(initialPayload),
        std::move(extensions));
  }

  folly::Expected<ObjectPublishStatus, MoQPublishError> objectPayload(
      Payload payload,
      bool finSubgroup) override {
    if (!consumer_) {
      return detached();
    }
    exposed_ = true;
    return consumer_->objectPayload(std::move(payload), finSubgroup);
  }

  folly::Expected<folly::Unit, MoQPublishError> endOfGroup(
      uint64_t groupID,
      uint64_t subgroupID,
      uint64_t objectID,
      bool finFetch) override {
    if (finFetch) {
      startedAt_.reset();
    }
    if (!consumer_) {
      return detached();
    }
    exposed_ = true;
    return consumer_->endOfGroup(groupID, subgroupID, objectID, finFetch);
  }

  folly::Expected<folly::Unit, MoQPublishError> endOfTrackAndGroup(
      uint64_t groupID,
      uint64_t subgroupID,
      uint64_t objectID) override {
    startedAt_.reset();
    if (!consumer_) {
      return detached();
    }
    exposed_ = true;
    return consumer_->endOfTrackAndGroup(groupID, subgroupID, objectID);
  }

  folly::Expected<folly::Unit, MoQPublishError> endOfFetch() override {
    startedAt_.reset();
    if (!consumer_) {
      return detached();
    }
    exposed_ = true;
    return consumer_->endOfFetch();
  }

  void reset(ResetStreamErrorCode error) override {
    startedAt_.reset();
    if (consumer_) {
      exposed_ = true;
      consumer_->reset(error);
    }
  }

  void goaway(Goaway goaway) override {
    if (consumer_) {
      exposed_ = true;
      consumer_->goaway(std::move(goaway));
    }
  }

  folly::Expected<folly::SemiFuture<uint64_t>, MoQPublishError>
  awaitReadyToConsume() override {
    if (!consumer_) {
      return detached();
    }
    return consumer_->awaitReadyToConsume();
  }

  folly::Expected<folly::Unit, MoQPublishError> endOfUnknownRange(
      uint64_t groupID,
      uint64_t objectID,
      bool finFetch) override {
    if (finFetch) {
      startedAt_.reset();
    }
    if (!consumer_) {
      return detached();
    }
    exposed_ = true;
    return consumer_->endOfUnknownRange(groupID, objectID, finFetch);
  }

 private:
  void recordFirstObject() {
    if (auto startedAt = std::exchange(startedAt_, std::nullopt)) {
      const auto duration =
          std::chrono::duration_cast<std::chrono::milliseconds>(
              clock_() - *startedAt);
      statsCallback_->recordUpstreamTimeToFirstFetchObject(duration.count());
    }
  }

  static folly::Unexpected<MoQPublishError> detached() {
    return folly::makeUnexpected(
        MoQPublishError(MoQPublishError::CANCELLED, "FETCH abandoned"));
  }

  std::shared_ptr<FetchConsumer> consumer_;
  std::shared_ptr<MoQProxyStatsCallback> statsCallback_;
  Clock clock_;
  std::optional<std::chrono::steady_clock::time_point> startedAt_;
  bool exposed_{false};
};

} // namespace moxygen

/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <moxygen/MoQConsumers.h>

namespace moxygen {
class TrackConsumerFilter : public TrackConsumer {
 public:
  explicit TrackConsumerFilter(std::shared_ptr<TrackConsumer> downstream)
      : downstream_(std::move(downstream)) {}

  folly::Expected<folly::Unit, MoQPublishError> setTrackAlias(
      TrackAlias alias) override {
    return downstream_->setTrackAlias(alias);
  }

  folly::Expected<std::shared_ptr<SubgroupConsumer>, MoQPublishError>
  beginSubgroup(
      uint64_t groupID,
      uint64_t subgroupID,
      Priority priority,
      bool containsLastInGroup = false) override {
    return downstream_->beginSubgroup(
        groupID, subgroupID, priority, containsLastInGroup);
  }

  folly::Expected<folly::SemiFuture<folly::Unit>, MoQPublishError>
  awaitStreamCredit() override {
    return downstream_->awaitStreamCredit();
  }

  folly::Expected<folly::Unit, MoQPublishError> objectStream(
      const ObjectHeader& header,
      Payload payload,
      bool lastInGroup = false) override {
    return downstream_->objectStream(header, std::move(payload), lastInGroup);
  }

  folly::Expected<folly::Unit, MoQPublishError> datagram(
      const ObjectHeader& header,
      Payload payload,
      bool lastInGroup = false) override {
    return downstream_->datagram(header, std::move(payload), lastInGroup);
  }

  folly::Expected<folly::Unit, MoQPublishError> publishDone(
      PublishDone pubDone) override {
    return downstream_->publishDone(std::move(pubDone));
  }

  void setDeliveryCallback(
      std::shared_ptr<DeliveryCallback> callback) override {
    downstream_->setDeliveryCallback(std::move(callback));
  }

 private:
  std::shared_ptr<TrackConsumer> downstream_;
};

class SubgroupConsumerFilter : public SubgroupConsumer {
 public:
  explicit SubgroupConsumerFilter(std::shared_ptr<SubgroupConsumer> downstream)
      : downstream_(std::move(downstream)) {}

  folly::Expected<folly::Unit, MoQPublishError> object(
      uint64_t objectID,
      Payload payload,
      Extensions extensions = noExtensions(),
      bool finSubgroup = false) override {
    return downstream_->object(
        objectID, std::move(payload), std::move(extensions), finSubgroup);
  }

  void checkpoint() override {
    downstream_->checkpoint();
  }

  folly::Expected<folly::Unit, MoQPublishError> beginObject(
      uint64_t objectID,
      uint64_t length,
      Payload initialPayload,
      Extensions extensions = noExtensions()) override {
    return downstream_->beginObject(
        objectID, length, std::move(initialPayload), std::move(extensions));
  }

  folly::Expected<ObjectPublishStatus, MoQPublishError> objectPayload(
      Payload payload,
      bool finSubgroup = false) override {
    return downstream_->objectPayload(std::move(payload), finSubgroup);
  }

  folly::Expected<folly::Unit, MoQPublishError> endOfGroup(
      uint64_t endOfGroupObjectID) override {
    return downstream_->endOfGroup(endOfGroupObjectID);
  }

  folly::Expected<folly::Unit, MoQPublishError> endOfTrackAndGroup(
      uint64_t endOfTrackObjectID) override {
    return downstream_->endOfTrackAndGroup(endOfTrackObjectID);
  }

  folly::Expected<folly::Unit, MoQPublishError> endOfSubgroup() override {
    return downstream_->endOfSubgroup();
  }

  void reset(ResetStreamErrorCode error) override {
    downstream_->reset(error);
  }

  folly::Expected<folly::SemiFuture<uint64_t>, MoQPublishError>
  awaitReadyToConsume() override {
    return downstream_->awaitReadyToConsume();
  }

 private:
  std::shared_ptr<SubgroupConsumer> downstream_;
};

} // namespace moxygen

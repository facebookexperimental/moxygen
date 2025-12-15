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
  beginSubgroup(uint64_t groupID, uint64_t subgroupID, Priority priority)
      override {
    return downstream_->beginSubgroup(groupID, subgroupID, priority);
  }

  folly::Expected<folly::SemiFuture<folly::Unit>, MoQPublishError>
  awaitStreamCredit() override {
    return downstream_->awaitStreamCredit();
  }

  folly::Expected<folly::Unit, MoQPublishError> objectStream(
      const ObjectHeader& header,
      Payload payload) override {
    return downstream_->objectStream(header, std::move(payload));
  }

  folly::Expected<folly::Unit, MoQPublishError> datagram(
      const ObjectHeader& header,
      Payload payload) override {
    return downstream_->datagram(header, std::move(payload));
  }

  folly::Expected<folly::Unit, MoQPublishError>
  groupNotExists(uint64_t groupID, uint64_t subgroup, Priority pri) override {
    return downstream_->groupNotExists(groupID, subgroup, pri);
  }

  folly::Expected<folly::Unit, MoQPublishError> subscribeDone(
      SubscribeDone subDone) override {
    return downstream_->subscribeDone(std::move(subDone));
  }

  void setDeliveryCallback(
      std::shared_ptr<DeliveryCallback> callback) override {
    downstream_->setDeliveryCallback(std::move(callback));
  }

 private:
  std::shared_ptr<TrackConsumer> downstream_;
};

} // namespace moxygen

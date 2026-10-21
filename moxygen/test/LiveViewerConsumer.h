/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <set>
#include <vector>

#include "moxygen/MoQConsumers.h"

namespace moxygen::test {

// A live viewer that accepts everything and records which locations reached
// it. Objects listed in stoppedObjects are refused as if by STOP_SENDING.
class LiveViewerConsumer : public TrackConsumer {
 public:
  class Subgroup : public SubgroupConsumer {
   public:
    Subgroup(LiveViewerConsumer& owner, uint64_t group)
        : owner_(owner), group_(group) {}

    folly::Expected<folly::Unit, MoQPublishError>
    object(uint64_t objectID, Payload, Extensions, bool) override {
      return owner_.deliver({group_, objectID});
    }
    folly::Expected<folly::Unit, MoQPublishError>
    beginObject(uint64_t objectID, uint64_t, Payload, Extensions) override {
      return owner_.deliver({group_, objectID});
    }
    folly::Expected<ObjectPublishStatus, MoQPublishError> objectPayload(
        Payload,
        bool) override {
      return ObjectPublishStatus::DONE;
    }
    folly::Expected<folly::Unit, MoQPublishError> endOfGroup(
        uint64_t) override {
      return folly::unit;
    }
    folly::Expected<folly::Unit, MoQPublishError> endOfTrackAndGroup(
        uint64_t) override {
      return folly::unit;
    }
    folly::Expected<folly::Unit, MoQPublishError> endOfSubgroup() override {
      return folly::unit;
    }
    void reset(ResetStreamErrorCode) override {}

   private:
    LiveViewerConsumer& owner_;
    uint64_t group_;
  };

  folly::Expected<folly::Unit, MoQPublishError> setTrackAlias(
      TrackAlias) override {
    return folly::unit;
  }
  folly::Expected<std::shared_ptr<SubgroupConsumer>, MoQPublishError>
  beginSubgroup(uint64_t groupID, uint64_t, Priority, BeginSubgroupOptions)
      override {
    return std::make_shared<Subgroup>(*this, groupID);
  }
  folly::Expected<folly::SemiFuture<folly::Unit>, MoQPublishError>
  awaitStreamCredit() override {
    return folly::makeSemiFuture();
  }
  folly::Expected<folly::Unit, MoQPublishError>
  objectStream(const ObjectHeader& header, Payload, bool) override {
    return deliver({header.group, header.id});
  }
  folly::Expected<folly::Unit, MoQPublishError>
  datagram(const ObjectHeader& header, Payload, bool) override {
    return deliver({header.group, header.id});
  }
  folly::Expected<folly::Unit, MoQPublishError> publishDone(
      PublishDone) override {
    return folly::unit;
  }

  std::vector<AbsoluteLocation> received;
  std::set<AbsoluteLocation> stoppedObjects;

 private:
  folly::Expected<folly::Unit, MoQPublishError> deliver(
      AbsoluteLocation location) {
    if (stoppedObjects.contains(location)) {
      return folly::makeUnexpected(
          MoQPublishError(MoQPublishError::CANCELLED, "STOP_SENDING"));
    }
    received.push_back(location);
    return folly::unit;
  }
};

} // namespace moxygen::test

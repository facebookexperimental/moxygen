/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <fmt/core.h>
#include <folly/futures/Future.h>
#include <deque>
#include <limits>
#include <optional>
#include <string>
#include <vector>

#include "moxygen/MoQConsumers.h"

namespace moxygen::test {

// Records every accepted write. Readiness turns pending once readyWrites
// writes were accepted. The next blockedWrites writes are accepted but report
// BLOCKED, and leave readiness pending until makeReady().
class RecordingFetchConsumer : public FetchConsumer {
 public:
  folly::Expected<folly::Unit, MoQPublishError> object(
      uint64_t groupID,
      uint64_t /*subgroupID*/,
      uint64_t objectID,
      Payload payload,
      Extensions /*extensions*/,
      bool finFetch,
      bool /*forwardingPreferenceIsDatagram*/) override {
    auto res = accept(
        fmt::format(
            "object {}/{} {}",
            groupID,
            objectID,
            payload ? payload->computeChainDataLength() : 0));
    const bool accepted =
        res.hasValue() || res.error().code == MoQPublishError::BLOCKED;
    if (accepted && finFetch) {
      // A receiver can see the stream's FIN with its last object.
      log.emplace_back("endOfFetch");
    }
    return res;
  }

  folly::Expected<folly::Unit, MoQPublishError> beginObject(
      uint64_t groupID,
      uint64_t /*subgroupID*/,
      uint64_t objectID,
      uint64_t length,
      Payload initialPayload,
      Extensions /*extensions*/) override {
    return accept(
        fmt::format(
            "begin {}/{} {} {}",
            groupID,
            objectID,
            length,
            initialPayload->computeChainDataLength()));
  }

  folly::Expected<ObjectPublishStatus, MoQPublishError> objectPayload(
      Payload payload,
      bool /*finSubgroup*/) override {
    auto res =
        accept(fmt::format("payload {}", payload->computeChainDataLength()));
    if (res.hasError()) {
      return folly::makeUnexpected(res.error());
    }
    return ObjectPublishStatus::IN_PROGRESS;
  }

  folly::Expected<folly::Unit, MoQPublishError> endOfGroup(
      uint64_t groupID,
      uint64_t /*subgroupID*/,
      uint64_t objectID,
      bool /*finFetch*/) override {
    return accept(fmt::format("endOfGroup {}/{}", groupID, objectID));
  }

  folly::Expected<folly::Unit, MoQPublishError> endOfTrackAndGroup(
      uint64_t groupID,
      uint64_t /*subgroupID*/,
      uint64_t objectID) override {
    return accept(fmt::format("endOfTrack {}/{}", groupID, objectID));
  }

  folly::Expected<folly::Unit, MoQPublishError> endOfFetch() override {
    return accept("endOfFetch");
  }

  void reset(ResetStreamErrorCode error) override {
    resetCode = error;
  }

  folly::Expected<folly::SemiFuture<uint64_t>, MoQPublishError>
  awaitReadyToConsume() override {
    readinessCalls++;
    if (log.size() < readyWrites) {
      return folly::makeSemiFuture<uint64_t>(0);
    }
    auto [promise, future] = folly::makePromiseContract<uint64_t>();
    pending.push_back(std::move(promise));
    return std::move(future);
  }

  void makeReady() {
    readyWrites = std::numeric_limits<size_t>::max();
    auto promises = std::move(pending);
    pending.clear();
    for (auto& promise : promises) {
      promise.setValue(0);
    }
  }

  std::vector<std::string> log;
  size_t readinessCalls{0};
  size_t readyWrites{std::numeric_limits<size_t>::max()};
  size_t blockedWrites{0};
  std::optional<ResetStreamErrorCode> resetCode;
  std::deque<folly::Promise<uint64_t>> pending;

 private:
  folly::Expected<folly::Unit, MoQPublishError> accept(std::string entry) {
    log.push_back(std::move(entry));
    if (blockedWrites > 0) {
      blockedWrites--;
      readyWrites = std::min(readyWrites, log.size());
      return folly::makeUnexpected(
          MoQPublishError(MoQPublishError::BLOCKED, "no stream credit"));
    }
    return folly::unit;
  }
};

} // namespace moxygen::test

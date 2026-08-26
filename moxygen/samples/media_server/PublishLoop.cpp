/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <moxygen/samples/media_server/PublishLoop.h>

#include <moxygen/MoQConsumers.h>
#include <moxygen/MoQPublishError.h>

#include <folly/coro/Sleep.h>
#include <folly/logging/xlog.h>

#include <chrono>
#include <limits>

namespace moxygen::media_server {

namespace {

// Writes one object onto an open subgroup; on BLOCKED, waits for stream credit.
// Returns false if the subgroup errored and should be dropped.
folly::coro::Task<bool> writeObject(SubgroupConsumer& sg, MediaObject obj) {
  auto res = sg.object(
      obj.object, std::move(obj.payload), std::move(obj.extensions), false);
  if (res.hasValue()) {
    co_return true;
  }
  if (res.error().code == MoQPublishError::BLOCKED) {
    XLOG(DBG1) << "[PublishLoop] subgroup BLOCKED; awaiting credit";
    auto awaitRes = sg.awaitReadyToConsume();
    if (awaitRes.hasError()) {
      co_return false;
    }
    co_await std::move(awaitRes.value());
    co_return true;
  }
  XLOG(ERR) << "[PublishLoop] subgroup object error: " << res.error().what();
  co_return false;
}

} // namespace

folly::coro::Task<void> runPublishLoop(
    std::shared_ptr<SegmentSource> source,
    std::shared_ptr<MoQForwarder> forwarder,
    bool waitForSubscriber) {
  const auto spec = source->spec();
  XLOG(INFO) << "[PublishLoop] start track=" << spec.name
             << " mode=" << static_cast<int>(spec.mode)
             << " waitForSubscriber=" << waitForSubscriber;

  if (waitForSubscriber) {
    while (forwarder->empty()) {
      co_await folly::coro::sleep(std::chrono::milliseconds(50));
    }
    XLOG(INFO) << "[PublishLoop] track=" << spec.name
               << " first subscriber present; starting emission";
  }

  std::shared_ptr<SubgroupConsumer> sg;
  uint64_t curGroup = std::numeric_limits<uint64_t>::max();
  uint64_t lastObjectId = 0;
  uint64_t published = 0;

  // A cancelled co_await (stack torn down after its last subscriber left)
  // throws out of the loop below, skipping the publishDone at the end - which
  // is what we want: a reclaimed stack must NOT signal end-of-track. Only a
  // source that genuinely runs out (generator completes) reaches publishDone.
  auto gen = source->objects();
  while (auto item = co_await gen.next()) {
    MediaObject obj = std::move(*item);
    const uint64_t group = obj.group;
    const uint64_t object = obj.object;

    if (spec.mode == ForwardMode::StreamPerObject) {
      forwarder->setLargest(AbsoluteLocation{group, object});
      if (forwarder->empty()) {
        continue;
      }
      ObjectHeader header{
          group,
          /*subgroupIn=*/0,
          object,
          spec.priority,
          ObjectStatus::NORMAL,
          std::move(obj.extensions),
          std::nullopt};
      auto res = forwarder->objectStream(header, std::move(obj.payload), false);
      if (res.hasError()) {
        XLOG(ERR) << "[PublishLoop] objectStream error: " << res.error().what();
        continue;
      }
      ++published;
      continue;
    }

    // SubgroupPerGroup: open a fresh subgroup on each group boundary.
    if (group != curGroup) {
      if (sg) {
        sg->endOfGroup(lastObjectId + 1);
        sg.reset();
      }
      curGroup = group;
      if (!forwarder->empty()) {
        auto res =
            forwarder->beginSubgroup(curGroup, /*subgroupID=*/0, spec.priority);
        if (res.hasError()) {
          XLOG(ERR) << "[PublishLoop] beginSubgroup error: "
                    << res.error().what();
        } else {
          sg = std::move(res.value());
          XLOG(INFO) << "[PublishLoop] track=" << spec.name
                     << " published group=" << curGroup;
        }
      }
    }

    if (!sg) {
      // No subscribers yet, or joined mid-group: advance the live edge and drop
      // until the next group boundary (a subscriber must start at a group
      // head).
      forwarder->setLargest(AbsoluteLocation{group, object});
      continue;
    }

    lastObjectId = object;
    const bool ok = co_await writeObject(*sg, std::move(obj));
    if (!ok) {
      sg.reset();
      continue;
    }
    ++published;
  }

  if (sg) {
    sg->endOfGroup(lastObjectId + 1);
  }
  // The source generator completed: this is a finite/ended track. Signal
  // end-of-track; the forwarder fans publishDone out to subscribers and fires
  // its onPublishDone callback so the broadcast can reap the stack.
  auto doneRes = forwarder->publishDone(
      PublishDone{
          RequestID{0},
          PublishDoneStatusCode::TRACK_ENDED,
          /*streamCount=*/published,
          "end of media"});
  if (doneRes.hasError()) {
    XLOG(ERR) << "[PublishLoop] publishDone error: " << doneRes.error().what();
  }
  XLOG(INFO) << "[PublishLoop] end track=" << spec.name
             << " published=" << published << "; sent publishDone";
}

} // namespace moxygen::media_server

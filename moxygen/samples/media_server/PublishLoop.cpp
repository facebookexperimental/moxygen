/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <moxygen/samples/media_server/PublishLoop.h>

#include <moxygen/MoQConsumers.h>
#include <moxygen/MoQPublishError.h>

#include <folly/coro/Baton.h>
#include <folly/coro/Sleep.h>
#include <folly/coro/WithCancellation.h>
#include <folly/logging/xlog.h>

#include <algorithm>
#include <chrono>
#include <limits>
#include <optional>
#include <utility>
#include <vector>

namespace moxygen::media_server {

namespace {

// Writes one object onto an open subgroup; on BLOCKED, waits for stream credit.
// Returns false if the subgroup errored and should be dropped.
folly::coro::Task<bool> writeObject(SubgroupConsumer& sg, MediaObject obj) {
  auto res = sg.object(
      obj.object,
      std::move(obj.payload),
      std::move(obj.extensions),
      obj.endOfGroup);
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

struct SubgroupBatch {
  uint64_t group{0};
  uint64_t subgroup{0};
  Priority priority{kDefaultPriority};
  std::chrono::steady_clock::time_point scheduledAt;
  bool endOfGroup{false};
  bool publish{false};
  std::vector<MediaObject> objects;
};

struct PublishState {
  uint64_t published{0};
  size_t pending{0};
  std::shared_ptr<folly::coro::Baton> drained;
};

std::chrono::milliseconds maxPublishDelay(const SubgroupBatch& batch) {
  std::chrono::milliseconds delay{0};
  for (const auto& object : batch.objects) {
    delay = std::max(delay, object.publishDelay);
  }
  return delay;
}

folly::coro::Task<void> sleepUntil(
    std::chrono::steady_clock::time_point deadline) {
  const auto now = std::chrono::steady_clock::now();
  if (now < deadline) {
    co_await folly::coro::sleep(
        std::chrono::duration_cast<folly::HighResDuration>(deadline - now));
  }
}

folly::coro::Task<uint64_t> publishSubgroup(
    SubgroupBatch batch,
    const std::shared_ptr<SegmentSource>& source,
    const std::shared_ptr<MoQForwarder>& forwarder) {
  const auto maxDelay = maxPublishDelay(batch);
  bool releaseStarted = false;
  const auto firstDelay = batch.objects.front().publishDelay;
  if (firstDelay > std::chrono::milliseconds::zero()) {
    co_await sleepUntil(batch.scheduledAt + firstDelay);
    source->onSubgroupPublishStarted(batch.group, batch.subgroup);
    releaseStarted = true;
  }
  auto begin =
      forwarder->beginSubgroup(batch.group, batch.subgroup, batch.priority);
  if (begin.hasError()) {
    XLOG(ERR) << "[PublishLoop] beginSubgroup error: " << begin.error().what();
    source->onSubgroupPublished(batch.group, batch.subgroup, 0);
    co_return 0;
  }

  auto subgroup = std::move(begin.value());
  uint64_t published = 0;
  uint64_t lastObjectId = 0;
  for (auto& object : batch.objects) {
    if (object.publishDelay > std::chrono::milliseconds::zero()) {
      co_await sleepUntil(batch.scheduledAt + object.publishDelay);
      if (!releaseStarted) {
        source->onSubgroupPublishStarted(batch.group, batch.subgroup);
        releaseStarted = true;
      }
    }
    lastObjectId = object.object;
    if (!co_await writeObject(*subgroup, std::move(object))) {
      source->onSubgroupPublished(batch.group, batch.subgroup, published);
      co_return published;
    }
    ++published;
  }

  auto close = batch.endOfGroup ? subgroup->endOfGroup(lastObjectId + 1)
                                : subgroup->endOfSubgroup();
  if (close.hasError()) {
    XLOG(ERR) << "[PublishLoop] subgroup close error: " << close.error().what();
  }
  source->onSubgroupPublished(batch.group, batch.subgroup, published);
  XLOG(INFO) << "[PublishLoop] track=" << source->spec().name
             << " published group=" << batch.group
             << " subgroup=" << batch.subgroup << " objects=" << published
             << " delayedMs=" << maxDelay.count();
  co_return published;
}

folly::coro::Task<void> publishDelayed(
    SubgroupBatch batch,
    std::shared_ptr<SegmentSource> source,
    std::shared_ptr<MoQForwarder> forwarder,
    folly::CancellationToken cancellationToken,
    std::shared_ptr<PublishState> state) {
  auto result = co_await folly::coro::co_awaitTry(
      folly::coro::co_withCancellation(
          cancellationToken,
          publishSubgroup(
              std::move(batch), std::move(source), std::move(forwarder))));
  if (result.hasValue()) {
    state->published += result.value();
  }
  --state->pending;
  if (state->pending == 0 && state->drained) {
    state->drained->post();
  }
}

folly::coro::Task<void> dispatchSubgroup(
    SubgroupBatch batch,
    const std::shared_ptr<SegmentSource>& source,
    const std::shared_ptr<MoQForwarder>& forwarder,
    folly::Executor* executor,
    folly::CancellationToken cancellationToken,
    const std::shared_ptr<PublishState>& state) {
  if (!batch.publish) {
    const auto& last = batch.objects.back();
    forwarder->setLargest(AbsoluteLocation{last.group, last.object});
    co_return;
  }
  if (maxPublishDelay(batch) > std::chrono::milliseconds::zero()) {
    ++state->pending;
    folly::coro::co_withExecutor(
        executor,
        publishDelayed(
            std::move(batch), source, forwarder, cancellationToken, state))
        .start();
    co_return;
  }
  state->published +=
      co_await publishSubgroup(std::move(batch), source, forwarder);
}

} // namespace

folly::coro::Task<void> runPublishLoop(
    std::shared_ptr<SegmentSource> source,
    std::shared_ptr<MoQForwarder> forwarder,
    folly::Executor* executor,
    folly::CancellationToken cancellationToken,
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

  uint64_t sourceGroup = std::numeric_limits<uint64_t>::max();
  bool publishCurrentGroup = false;
  std::optional<SubgroupBatch> batch;
  auto state = std::make_shared<PublishState>();

  // A cancelled co_await (stack torn down after its last subscriber left)
  // throws out of the loop below, skipping the publishDone at the end - which
  // is what we want: a reclaimed stack must NOT signal end-of-track. Only a
  // source that genuinely runs out (generator completes) reaches publishDone.
  auto gen = source->objects();
  while (auto item = co_await gen.next()) {
    MediaObject obj = std::move(*item);
    const uint64_t group = obj.group;
    const uint64_t subgroup = obj.subgroup;
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
      ++state->published;
      continue;
    }

    if (group != sourceGroup) {
      sourceGroup = group;
      publishCurrentGroup = !forwarder->empty();
    }

    if (batch && (batch->group != group || batch->subgroup != subgroup)) {
      XLOG(WARN) << "[PublishLoop] source changed subgroup without an explicit "
                    "end; closing it";
      co_await dispatchSubgroup(
          std::move(*batch),
          source,
          forwarder,
          executor,
          cancellationToken,
          state);
      batch.reset();
    }
    if (!batch) {
      batch = SubgroupBatch{
          .group = group,
          .subgroup = subgroup,
          .priority = spec.priority,
          .scheduledAt = std::chrono::steady_clock::now(),
          .endOfGroup = false,
          .publish = publishCurrentGroup};
    }
    const bool endOfSubgroup = obj.endOfSubgroup;
    batch->endOfGroup = batch->endOfGroup || obj.endOfGroup;
    batch->objects.push_back(std::move(obj));
    if (endOfSubgroup) {
      co_await dispatchSubgroup(
          std::move(*batch),
          source,
          forwarder,
          executor,
          cancellationToken,
          state);
      batch.reset();
    }
  }

  if (batch) {
    co_await dispatchSubgroup(
        std::move(*batch),
        source,
        forwarder,
        executor,
        cancellationToken,
        state);
  }
  if (state->pending > 0) {
    state->drained = std::make_shared<folly::coro::Baton>();
    co_await *state->drained;
  }
  // The source generator completed: this is a finite/ended track. Signal
  // end-of-track; the forwarder fans publishDone out to subscribers and fires
  // its onPublishDone callback so the broadcast can reap the stack.
  auto doneRes = forwarder->publishDone(
      PublishDone{
          RequestID{0},
          PublishDoneStatusCode::TRACK_ENDED,
          /*streamCount=*/state->published,
          "end of media"});
  if (doneRes.hasError()) {
    XLOG(ERR) << "[PublishLoop] publishDone error: " << doneRes.error().what();
  }
  XLOG(INFO) << "[PublishLoop] end track=" << spec.name
             << " published=" << state->published << "; sent publishDone";
}

} // namespace moxygen::media_server

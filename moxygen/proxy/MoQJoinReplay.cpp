/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/proxy/MoQJoinReplay.h"

#include <folly/ScopeGuard.h>
#include <folly/coro/CurrentExecutor.h>
#include <folly/coro/DetachOnCancel.h>
#include <folly/coro/FutureUtil.h>
#include <folly/coro/Timeout.h>
#include <folly/logging/xlog.h>
#include <algorithm>

#include "moxygen/util/TimedBaton.h"

namespace moxygen {
namespace {

using WriteResult = folly::Expected<folly::Unit, MoQPublishError>;
// Attempts capture by reference: GCC 11 destroys a closure that shares a
// full-expression with co_await twice, over-releasing what it owns.
using WriteAttempt = folly::Function<WriteResult()>;

// Finished: the write was accepted and ended the FETCH stream.
enum class WriteOutcome { Accepted, Finished, Cancelled, Stalled, Failed };

// Paces writes on the consumer's readiness. A readiness wait longer than the
// stall timeout stalls the replay.
class ReplayWriter {
 public:
  ReplayWriter(
      std::shared_ptr<FetchConsumer> consumer,
      const MoQReplayConfig& config,
      folly::CancellationToken token)
      : consumer_(std::move(consumer)),
        config_(config),
        token_(std::move(token)) {}

  folly::coro::Task<WriteOutcome> write(WriteAttempt attempt) {
    if (token_.isCancellationRequested()) {
      co_return WriteOutcome::Cancelled;
    }
    auto readiness = co_await awaitReadiness();
    if (readiness != WriteOutcome::Accepted) {
      co_return readiness;
    }
    auto res = attempt();
    // BLOCKED also accepted the write; the next one waits for readiness.
    if (res.hasValue() || res.error().code == MoQPublishError::BLOCKED) {
      co_return WriteOutcome::Accepted;
    }
    if (res.error().code == MoQPublishError::CANCELLED) {
      co_return WriteOutcome::Cancelled;
    }
    XLOG(ERR) << "Replay write failed: " << res.error().what();
    co_return WriteOutcome::Failed;
  }

 private:
  folly::coro::Task<WriteOutcome> awaitReadiness() {
    auto ready = consumer_->awaitReadyToConsume();
    if (ready.hasError()) {
      co_return ready.error().code == MoQPublishError::CANCELLED
          ? WriteOutcome::Cancelled
          : WriteOutcome::Failed;
    }
    if (ready->isReady()) {
      co_return WriteOutcome::Accepted;
    }
    // The interrupt lets a transport drop its pending wait; detaching keeps
    // the timeout prompt for transports that ignore it.
    auto* executor = co_await folly::coro::co_current_executor;
    auto readiness = folly::coro::detachOnCancel(
        folly::coro::toTaskInterruptOnCancel(
            std::move(ready.value()).via(folly::getKeepAliveToken(executor))));
    auto wait = folly::coro::timeout(
        std::move(readiness), config_.stallTimeout, config_.timekeeper.get());
    auto result = co_await folly::coro::co_awaitTry(std::move(wait));
    if (result.hasValue()) {
      co_return WriteOutcome::Accepted;
    }
    if (result.hasException<folly::FutureTimeout>()) {
      co_return WriteOutcome::Stalled;
    }
    if (result.hasException<folly::OperationCancelled>()) {
      co_return WriteOutcome::Cancelled;
    }
    co_return WriteOutcome::Failed;
  }

  std::shared_ptr<FetchConsumer> consumer_;
  const MoQReplayConfig& config_;
  folly::CancellationToken token_;
};

folly::coro::Task<WriteOutcome> writeEntry(
    ReplayWriter& writer,
    std::shared_ptr<FetchConsumer> consumer,
    MoQReplayPlan::Entry entry) {
  const auto location = entry.location;
  auto object = std::move(entry.object);
  const auto subgroup = object->subgroup;
  if (object->status == ObjectStatus::END_OF_GROUP) {
    co_return co_await writer.write([&] {
      return consumer->endOfGroup(
          location.group, subgroup, location.object, /*finFetch=*/false);
    });
  }
  if (object->status == ObjectStatus::END_OF_TRACK) {
    auto outcome = co_await writer.write([&] {
      return consumer->endOfTrackAndGroup(
          location.group, subgroup, location.object);
    });
    // END_OF_TRACK also ends the FETCH stream.
    co_return outcome == WriteOutcome::Accepted ? WriteOutcome::Finished
                                                : outcome;
  }
  co_return co_await writer.write([&] {
    return consumer->object(
        location.group,
        subgroup,
        location.object,
        object->payload ? object->payload->clone() : nullptr,
        object->extensions,
        /*finFetch=*/false,
        object->datagram);
  });
}

folly::coro::Task<MoQGroupCache::Coverage> waitForCoverage(
    std::shared_ptr<MoQGroupCache> cache,
    AbsoluteLocation start,
    AbsoluteLocation end) {
  while (true) {
    auto coverage = cache->inspect(start, end);
    if (coverage != MoQGroupCache::Coverage::AwaitIngress) {
      co_return coverage;
    }
    // Registered before suspending, so no change can be missed.
    auto changed = std::make_shared<TimedBaton>();
    const auto waiter = cache->addWaiter([changed] { changed->signal(); });
    SCOPE_EXIT {
      cache->removeWaiter(waiter);
    };
    co_await changed->wait();
  }
}

} // namespace

folly::coro::Task<MoQGroupCache::Coverage> awaitCacheCoverage(
    std::shared_ptr<MoQGroupCache> cache,
    AbsoluteLocation start,
    AbsoluteLocation end,
    std::chrono::milliseconds maxWait,
    folly::Timekeeper* timekeeper) {
  auto wait = folly::coro::timeout(
      waitForCoverage(cache, start, end), maxWait, timekeeper);
  auto result = co_await folly::coro::co_awaitTry(std::move(wait));
  if (result.hasValue()) {
    co_return *result;
  }
  co_return cache->inspect(start, end);
}

folly::coro::Task<MoQReplayOutcome> runReplay(
    MoQReplayPlan plan,
    std::shared_ptr<FetchConsumer> consumer,
    MoQReplayConfig config) {
  ReplayWriter writer(
      consumer, config, co_await folly::coro::co_current_cancellation_token);
  const size_t writesPerYield = std::max<size_t>(config.writesPerYield, 1);
  auto outcome = WriteOutcome::Accepted;
  size_t writes = 0;
  for (size_t i = 0; i < plan.entries.size(); i++) {
    auto& entry = plan.entries[i];
    // END_OF_TRACK finishes the stream, so it is written only as the final
    // entry. A newest-first plan holds it in its first group; there FETCH_OK
    // alone reports the end of the track.
    if (entry.object->status == ObjectStatus::END_OF_TRACK &&
        i + 1 < plan.entries.size()) {
      entry.object.reset();
      continue;
    }
    // Each written object is released as soon as it is handed off.
    outcome = co_await writeEntry(writer, consumer, std::move(entry));
    if (outcome != WriteOutcome::Accepted) {
      break;
    }
    if (++writes % writesPerYield == 0) {
      co_await folly::coro::co_reschedule_on_current_executor;
    }
  }
  if (outcome == WriteOutcome::Accepted) {
    outcome = co_await writer.write([&] { return consumer->endOfFetch(); });
  }
  switch (outcome) {
    // Accepted here is the end of FETCH itself.
    case WriteOutcome::Accepted:
    case WriteOutcome::Finished:
      co_return MoQReplayOutcome::Finished;
    case WriteOutcome::Failed:
      consumer->reset(ResetStreamErrorCode::INTERNAL_ERROR);
      co_return MoQReplayOutcome::Failed;
    case WriteOutcome::Stalled:
      XLOG(DBG1) << "Replay made no progress within the stall timeout";
      consumer->reset(ResetStreamErrorCode::CANCELLED);
      co_return MoQReplayOutcome::Stalled;
    case WriteOutcome::Cancelled:
      consumer->reset(ResetStreamErrorCode::CANCELLED);
      co_return MoQReplayOutcome::Cancelled;
  }
  co_return MoQReplayOutcome::Failed;
}

folly::coro::Task<Publisher::FetchHandle::RequestUpdateResult>
MoQLocalFetchHandle::requestUpdate(RequestUpdate requestUpdate) {
  co_return folly::makeUnexpected(
      RequestError{
          requestUpdate.requestID,
          RequestErrorCode::NOT_SUPPORTED,
          "REQUEST_UPDATE not supported for a cached FETCH"});
}

} // namespace moxygen

/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/coro/AsyncScope.h>
#include <folly/coro/Task.h>
#include <folly/dynamic.h>
#include <chrono>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "moxygen/test/MoQProxySwitchHarness.h"

namespace moxygen::test {

struct SwitchCheckerOptions {
  using Clock = std::chrono::steady_clock;

  std::vector<FullTrackName> tracks;
  // How many groups before its start group each joiner's history begins.
  uint64_t joinGroups{1};
  // The PUBLISH_DONE every viewer must get, and the code its open subgroup
  // streams may be reset with; any other is a violation.
  std::optional<PublishDoneStatusCode> expectedEnd;
  std::optional<ResetStreamErrorCode> expectedReset;
  // The longest a viewer may go without an object.
  std::optional<std::chrono::milliseconds> maxStall;
  std::function<Clock::time_point()> clock{[] { return Clock::now(); }};
};

// Watches tracks as viewers do, through a proxy or straight from switch test
// origins, and reports what each viewer got against what the origins
// publish: the checks of CheckingViewer and CheckingFetch, the objects that
// never arrived, which origin served each group, and the longest time each
// viewer went without an object.
//
// The checker can't see what the origins publish, but they publish every
// group whole, in order, so it counts as published every object from the
// first group any viewer received to the last object any viewer received. A
// viewer's missing objects are the published ones up to its own last that it
// doesn't have; the published ones after its last say how far behind it is.
// The part of a joiner's history before the first group any viewer received
// can't be checked, and is reported as unverified.
class SwitchChecker {
 public:
  using Clock = SwitchCheckerOptions::Clock;

  SwitchChecker(SwitchTrackShape shape, SwitchCheckerOptions options);

  // A viewer of each track, on session.
  folly::coro::Task<void> addViewers(std::shared_ptr<MoQSession> session);
  // A joiner of each track, on session: it subscribes, then a joining FETCH
  // gets its history, from joinGroups groups before its start group up to its
  // start. Returns once subscribed; the history arrives in the background.
  // On a draft with FETCH filling instead, such as draft 22, a joiner only
  // watches, and its history is reported as skipped.
  folly::coro::Task<void> addJoiners(std::shared_ptr<MoQSession> session);
  // A viewer that couldn't connect.
  void addConnectError(std::string error);
  // Where the report's times count from, besides the checker's creation.
  void startWatch();

  // Whether everything still due has arrived: every SUBSCRIBE has its reply,
  // every joiner's FETCH its reply and its history's end or reset, and every
  // subgroup stream of an ended track, or of a group before the newest a
  // viewer got, its end or reset.
  bool settled() const;
  // "ok" is false if there are no viewers, a session couldn't connect, the
  // objects received span implausibly many groups, or a viewer:
  //   - failed to subscribe, has no SUBSCRIBE reply, or saw a violation;
  //   - received nothing, unless it got the expected PUBLISH_DONE;
  //   - went longer than maxStall without an object, counting up to its
  //     PUBLISH_DONE once it has one;
  //   - received an object outside the track's shape;
  //   - has a subgroup stream unfinished though its track ended, or though a
  //     later group arrived;
  //   - is a joiner whose history failed, was reset or hasn't ended, or
  //     whose FETCH_OK names an end other than its start.
  // Missing objects don't make it false: whether a gap is allowed is the
  // scenario's call. Times are in milliseconds since the checker was
  // created.
  folly::dynamic report(Clock::time_point now);
  // Waits for every joiner's FETCH reply. Close the sessions first to cut
  // short the replies still pending. Call it once, last, before the checker
  // goes away.
  folly::coro::Task<void> join();

 private:
  struct Viewer {
    FullTrackName track;
    bool joiner{false};
    std::shared_ptr<CheckingViewer> live;
    Clock::time_point requestedAt;
    // When its SUBSCRIBE_OK arrived.
    std::optional<Clock::time_point> subscribedAt;
    std::shared_ptr<Publisher::SubscriptionHandle> handle;
    std::optional<std::string> error;
    // A joiner's history.
    std::shared_ptr<CheckingFetch> history;
    std::shared_ptr<Publisher::FetchHandle> historyHandle;
    std::optional<std::string> historyError;
    // Why the joiner has no history to check.
    std::optional<std::string> historySkipped;
  };

  folly::coro::Task<Viewer*> subscribe(
      const std::shared_ptr<MoQSession>& session,
      const FullTrackName& track,
      bool joiner);
  folly::coro::Task<void> fetchHistory(
      std::shared_ptr<MoQSession> session,
      Viewer& joiner);
  // Everything the origins have published, as far as the viewers can tell;
  // an error for a track whose objects span implausibly many.
  std::vector<std::string> notePublished();
  // The first location counted as published on the track, if any.
  std::optional<AbsoluteLocation> firstPublished(
      const FullTrackName& track) const;
  // The checker's own findings about a viewer, beyond its checks'.
  std::vector<std::string> findings(const Viewer& viewer, Clock::time_point now)
      const;
  std::optional<std::string> outsideTheShape(
      const std::vector<AbsoluteLocation>& received) const;
  std::chrono::milliseconds sinceCreated(Clock::time_point time) const;
  std::chrono::milliseconds longestStall(
      const Viewer& viewer,
      Clock::time_point now) const;
  folly::dynamic reportOn(const Viewer& viewer, Clock::time_point now) const;
  folly::dynamic historyOf(const Viewer& joiner) const;
  folly::dynamic ranges(const std::vector<AbsoluteLocation>& locations) const;

  // What the viewers check against; its published set is inferred.
  std::shared_ptr<SwitchOrigin> reference_;
  SwitchCheckerOptions options_;
  Clock::time_point created_;
  std::optional<Clock::time_point> watchStarted_;
  std::vector<std::unique_ptr<Viewer>> viewers_;
  std::vector<std::string> connectErrors_;
  folly::coro::AsyncScope fetches_;
};

} // namespace moxygen::test

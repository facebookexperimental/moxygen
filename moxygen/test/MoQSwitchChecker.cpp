/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/test/MoQSwitchChecker.h"

#include <fmt/core.h>
#include <folly/coro/Result.h>

#include <algorithm>
#include <iterator>
#include "moxygen/MoQVersions.h"

namespace moxygen::test {
namespace {

using std::chrono::duration_cast;
using std::chrono::milliseconds;

// More objects than this between the first and last any viewer received of
// a track means a corrupt group ID, not a run.
constexpr uint64_t kMaxPublishedSpan{10'000'000};

std::string describe(AbsoluteLocation location) {
  return fmt::format("{}/{}", location.group, location.object);
}

// The origins publish every object of every group, in order.
AbsoluteLocation following(
    AbsoluteLocation location,
    uint64_t objectsPerGroup) {
  return location.object + 1 < objectsPerGroup
      ? AbsoluteLocation{location.group, location.object + 1}
      : AbsoluteLocation{location.group + 1, 0};
}

folly::dynamic strings(const std::vector<std::string>& values) {
  folly::dynamic array = folly::dynamic::array;
  for (const auto& value : values) {
    array.push_back(value);
  }
  return array;
}

// Runs of consecutive groups from the same hub.
folly::dynamic hubRuns(const std::map<uint64_t, uint64_t>& hubOfGroup) {
  folly::dynamic runs = folly::dynamic::array;
  for (const auto& [group, hub] : hubOfGroup) {
    auto* run = runs.empty() ? nullptr : &runs[runs.size() - 1];
    if (run && (*run)["hub"].asInt() == int64_t(hub) &&
        (*run)["last"].asInt() + 1 == int64_t(group)) {
      (*run)["last"] = int64_t(group);
    } else {
      runs.push_back(
          folly::dynamic::object("first", group)("last", group)("hub", hub));
    }
  }
  return runs;
}

folly::dynamic resetsOf(const CheckingViewer& viewer) {
  folly::dynamic resets = folly::dynamic::array;
  for (const auto& reset : viewer.resets()) {
    resets.push_back(
        folly::dynamic::object("group", reset.group)(
            "subgroup", reset.subgroup)(
            "error", folly::to_underlying(reset.error)));
  }
  return resets;
}

// The subgroup streams the viewer should have seen end or reset: all of them
// once its track ended, otherwise those of groups before the newest it got.
std::vector<std::pair<uint64_t, uint64_t>> overdueSubgroups(
    const CheckingViewer& viewer) {
  auto overdue = viewer.unfinishedSubgroups();
  if (viewer.done()) {
    return overdue;
  }
  const auto& received = viewer.received();
  const auto newest =
      received.empty() ? 0 : std::ranges::max_element(received)->group;
  std::erase_if(
      overdue, [&](auto subgroup) { return subgroup.first >= newest; });
  return overdue;
}

} // namespace

SwitchChecker::SwitchChecker(
    SwitchTrackShape shape,
    SwitchCheckerOptions options)
    : reference_(std::make_shared<SwitchOrigin>(shape)),
      options_(std::move(options)),
      created_(options_.clock()) {}

folly::coro::Task<void> SwitchChecker::addViewers(
    std::shared_ptr<MoQSession> session) {
  for (const auto& track : options_.tracks) {
    co_await subscribe(session, track, /*joiner=*/false);
  }
}

folly::coro::Task<void> SwitchChecker::addJoiners(
    std::shared_ptr<MoQSession> session) {
  for (const auto& track : options_.tracks) {
    auto* joiner = co_await subscribe(session, track, /*joiner=*/true);
    if (!joiner->error) {
      fetches_.add(
          folly::coro::co_withExecutor(
              co_await folly::coro::co_current_executor,
              fetchHistory(session, *joiner)));
    }
  }
}

folly::coro::Task<void> SwitchChecker::join() {
  co_await fetches_.joinAsync();
}

void SwitchChecker::addConnectError(std::string error) {
  connectErrors_.push_back(std::move(error));
}

void SwitchChecker::startWatch() {
  watchStarted_ = options_.clock();
}

folly::coro::Task<SwitchChecker::Viewer*> SwitchChecker::subscribe(
    const std::shared_ptr<MoQSession>& session,
    const FullTrackName& track,
    bool joiner) {
  auto owned = std::make_unique<Viewer>(Viewer{
      .track = track,
      .joiner = joiner,
      .live = std::make_shared<CheckingViewer>(reference_, track),
      .requestedAt = options_.clock()});
  auto* viewer = owned.get();
  viewers_.push_back(std::move(owned));
  viewer->live->expectedEnd = options_.expectedEnd;
  viewer->live->expectedReset = options_.expectedReset;
  viewer->live->clock = options_.clock;
  const auto filter = LocationFilter::nextObject();
  auto request = SubscribeRequest::make(track);
  request.filter = filter;
  auto reply = co_await folly::coro::co_awaitTry(
      session->subscribe(std::move(request), viewer->live));
  if (reply.hasException()) {
    viewer->error = reply.exception().what().toStdString();
  } else if (reply->hasError()) {
    viewer->error = reply->error().reasonPhrase;
  } else {
    viewer->subscribedAt = options_.clock();
    viewer->handle = std::move(reply->value());
    viewer->live->expectedStart =
        filter.resolve(viewer->handle->subscribeOk().largest, /*isFetch=*/false)
            .start;
  }
  co_return viewer;
}

folly::coro::Task<void> SwitchChecker::fetchHistory(
    std::shared_ptr<MoQSession> session,
    Viewer& joiner) {
  if (const auto version = session->getNegotiatedVersion();
      version && useFillFetch(*version)) {
    joiner.historySkipped = fmt::format(
        "draft {} has no joining FETCH", getDraftMajorVersion(*version));
    co_return;
  }
  const auto start = *joiner.live->expectedStart;
  const auto fromGroup =
      start.group - std::min(start.group, options_.joinGroups);
  joiner.history = std::make_shared<CheckingFetch>(reference_, joiner.track);
  joiner.history->expectedStart = AbsoluteLocation{fromGroup, 0};
  joiner.history->expectedEnd = start;
  Fetch fetch(
      RequestID(0),
      joiner.handle->subscribeOk().requestID,
      /*joiningStart=*/fromGroup,
      FetchType::ABSOLUTE_JOINING,
      kDefaultPriority,
      GroupOrder::OldestFirst);
  fetch.fullTrackName = joiner.track;
  auto reply = co_await folly::coro::co_awaitTry(
      session->fetch(std::move(fetch), joiner.history));
  if (reply.hasException()) {
    joiner.historyError = reply.exception().what().toStdString();
  } else if (reply->hasError()) {
    joiner.historyError = reply->error().reasonPhrase;
  } else {
    joiner.historyHandle = std::move(reply->value());
  }
}

bool SwitchChecker::settled() const {
  return std::ranges::all_of(viewers_, [](const auto& viewer) {
    if (viewer->error) {
      return true;
    }
    if (!viewer->handle || !overdueSubgroups(*viewer->live).empty()) {
      return false;
    }
    if (!viewer->joiner || viewer->historyError || viewer->historySkipped) {
      return true;
    }
    const auto& history = viewer->history;
    return viewer->historyHandle && (history->ended() || history->resetError());
  });
}

std::vector<std::string> SwitchChecker::notePublished() {
  std::vector<std::string> errors;
  const auto objectsPerGroup = reference_->shape().objectsPerGroup;
  for (const auto& track : options_.tracks) {
    std::optional<AbsoluteLocation> first;
    std::optional<AbsoluteLocation> last;
    auto note = [&](AbsoluteLocation location) {
      first = first ? std::min(*first, location) : location;
      last = last ? std::max(*last, location) : location;
    };
    for (const auto& viewer : viewers_) {
      if (viewer->track != track) {
        continue;
      }
      for (const auto& location : viewer->live->received()) {
        note(location);
      }
      if (viewer->history) {
        for (const auto& location : viewer->history->received()) {
          note(location);
        }
      }
    }
    if (!first) {
      continue;
    }
    const auto groups = last->group - first->group;
    if (groups >= kMaxPublishedSpan / objectsPerGroup) {
      errors.push_back(
          fmt::format(
              "{}: objects from {} to {} span too many groups",
              track.describe(),
              describe(*first),
              describe(*last)));
      continue;
    }
    for (AbsoluteLocation location{first->group, 0}; location <= *last;
         location = following(location, objectsPerGroup)) {
      reference_->notePublished(track, location);
    }
  }
  return errors;
}

std::optional<AbsoluteLocation> SwitchChecker::firstPublished(
    const FullTrackName& track) const {
  const auto& published = reference_->published(track);
  return published.empty() ? std::nullopt : std::optional(*published.begin());
}

std::chrono::milliseconds SwitchChecker::sinceCreated(
    Clock::time_point time) const {
  return duration_cast<milliseconds>(time - created_);
}

// From SUBSCRIBE_OK, to each object in turn, then to the PUBLISH_DONE or, with
// none, to now.
std::chrono::milliseconds SwitchChecker::longestStall(
    const Viewer& viewer,
    Clock::time_point now) const {
  const auto& live = *viewer.live;
  auto previous = *viewer.subscribedAt;
  Clock::duration longest{0};
  for (auto arrival : live.arrivals()) {
    longest = std::max(longest, arrival - previous);
    previous = std::max(previous, arrival);
  }
  longest = std::max(longest, live.doneAt().value_or(now) - previous);
  return duration_cast<milliseconds>(longest);
}

std::optional<std::string> SwitchChecker::outsideTheShape(
    const std::vector<AbsoluteLocation>& received) const {
  const auto objectsPerGroup = reference_->shape().objectsPerGroup;
  auto outside = std::ranges::find_if(received, [&](auto location) {
    return location.object >= objectsPerGroup;
  });
  if (outside == received.end()) {
    return std::nullopt;
  }
  return fmt::format(
      "object {} is outside the track's shape of {} objects a group",
      describe(*outside),
      objectsPerGroup);
}

std::vector<std::string> SwitchChecker::findings(
    const Viewer& viewer,
    Clock::time_point now) const {
  std::vector<std::string> found;
  const auto& live = *viewer.live;
  const bool endedAsExpected = live.done() && options_.expectedEnd &&
      live.done()->statusCode == *options_.expectedEnd;
  if (live.received().empty() && !endedAsExpected) {
    found.emplace_back("received nothing");
  }
  if (const auto stall = longestStall(viewer, now);
      options_.maxStall && stall > *options_.maxStall) {
    found.push_back(
        fmt::format(
            "went {}ms without an object, over the {}ms allowed",
            stall.count(),
            options_.maxStall->count()));
  }
  if (auto outside = outsideTheShape(live.received())) {
    found.push_back(std::move(*outside));
  }
  for (const auto& [group, subgroup] : overdueSubgroups(live)) {
    found.push_back(
        fmt::format(
            "subgroup {}/{} never finished though {}",
            group,
            subgroup,
            live.done() ? "the track ended" : "a later group arrived"));
  }
  return found;
}

folly::dynamic SwitchChecker::ranges(
    const std::vector<AbsoluteLocation>& locations) const {
  const auto objectsPerGroup = reference_->shape().objectsPerGroup;
  folly::dynamic array = folly::dynamic::array;
  for (size_t i = 0; i < locations.size();) {
    auto j = i;
    while (j + 1 < locations.size() &&
           locations[j + 1] == following(locations[j], objectsPerGroup)) {
      ++j;
    }
    array.push_back(
        folly::dynamic::object("from", describe(locations[i]))(
            "to", describe(locations[j])));
    i = j + 1;
  }
  return array;
}

folly::dynamic SwitchChecker::reportOn(
    const Viewer& viewer,
    Clock::time_point now) const {
  folly::dynamic entry = folly::dynamic::object(
      "track", viewer.track.describe())("joiner", viewer.joiner)(
      "requested_at_ms", sinceCreated(viewer.requestedAt).count());
  if (viewer.error || !viewer.subscribedAt) {
    entry["error"] = viewer.error.value_or("no SUBSCRIBE reply");
    return entry;
  }
  entry["subscribe_ms"] =
      duration_cast<milliseconds>(*viewer.subscribedAt - viewer.requestedAt)
          .count();
  const auto& live = *viewer.live;
  const auto& received = live.received();
  const auto start = *live.expectedStart;
  entry["start"] = describe(start);
  entry["received"] = received.size();
  std::vector<AbsoluteLocation> missing = live.missing();
  // Published objects past the viewer's last, or past its start if it has
  // none, are ones it is behind on rather than missing.
  auto behindFrom = start;
  if (!received.empty()) {
    const auto last = *std::ranges::max_element(received);
    entry["first"] = describe(*std::ranges::min_element(received));
    entry["last"] = describe(last);
    behindFrom = following(last, reference_->shape().objectsPerGroup);
  }
  const auto behind = std::ranges::count_if(
      reference_->published(viewer.track),
      [&](auto location) { return !(location < behindFrom); });
  std::erase_if(
      missing, [&](auto location) { return !(location < behindFrom); });
  entry["missing"] = ranges(missing);
  entry["behind"] = behind;
  auto violations = live.violations();
  std::ranges::move(findings(viewer, now), std::back_inserter(violations));
  entry["violations"] = strings(violations);
  entry["groups_by_hub"] = hubRuns(live.hubOfGroup());
  if (const auto& done = live.done()) {
    entry["publish_done"] = folly::dynamic::object(
        "status", folly::to_underlying(done->statusCode))(
        "reason", done->reasonPhrase);
  }
  entry["resets"] = resetsOf(live);
  folly::dynamic unfinished = folly::dynamic::array;
  for (const auto& [group, subgroup] : live.unfinishedSubgroups()) {
    unfinished.push_back(fmt::format("{}/{}", group, subgroup));
  }
  entry["unfinished_subgroups"] = std::move(unfinished);
  entry["longest_stall_ms"] = longestStall(viewer, now).count();
  if (viewer.joiner) {
    entry["history"] = historyOf(viewer);
  }
  return entry;
}

folly::dynamic SwitchChecker::historyOf(const Viewer& joiner) const {
  if (joiner.historySkipped) {
    return folly::dynamic::object("skipped", *joiner.historySkipped);
  }
  if (joiner.historyError) {
    return folly::dynamic::object("error", *joiner.historyError);
  }
  if (!joiner.historyHandle) {
    return folly::dynamic::object("error", "no FETCH reply");
  }
  const auto& history = *joiner.history;
  const auto start = *history.expectedStart;
  const auto end = *history.expectedEnd;
  auto violations = history.violations();
  if (auto outside = outsideTheShape(history.received())) {
    violations.push_back(std::move(*outside));
  }
  if (const auto& reset = history.resetError()) {
    violations.push_back(
        fmt::format("reset with {}", folly::to_underlying(*reset)));
  } else if (!history.ended()) {
    violations.emplace_back("unfinished");
  }
  const auto okEnd = joiner.historyHandle->fetchOk().endLocation;
  if (okEnd != end) {
    violations.push_back(
        fmt::format(
            "FETCH_OK ends at {}, not {}", describe(okEnd), describe(end)));
  }
  folly::dynamic summary = folly::dynamic::object("start", describe(start))(
      "end", describe(end))("received", history.received().size())(
      "ended", history.ended())("missing", ranges(history.missing(start, end)))(
      "violations", strings(violations))("fetch_ok_end", describe(okEnd));
  const auto verifiedFrom =
      std::min(end, firstPublished(joiner.track).value_or(end));
  if (start < verifiedFrom) {
    summary["unverified"] = folly::dynamic::object("from", describe(start))(
        "until", describe(verifiedFrom));
  }
  return summary;
}

folly::dynamic SwitchChecker::report(Clock::time_point now) {
  const auto errors = notePublished();
  bool ok = connectErrors_.empty() && errors.empty() && !viewers_.empty();
  folly::dynamic viewers = folly::dynamic::array;
  for (auto& viewer : viewers_) {
    auto entry = reportOn(*viewer, now);
    ok = ok && !entry.count("error") && entry["violations"].empty();
    if (const auto* history = entry.get_ptr("history");
        history && !history->count("skipped")) {
      ok = ok && !history->count("error") && (*history)["violations"].empty();
    }
    viewers.push_back(std::move(entry));
  }
  folly::dynamic result =
      folly::dynamic::object("ok", ok)("viewers", std::move(viewers))(
          "errors", strings(errors))("connect_errors", strings(connectErrors_));
  if (watchStarted_) {
    result["watch_started_at_ms"] = sinceCreated(*watchStarted_).count();
  }
  return result;
}

} // namespace moxygen::test

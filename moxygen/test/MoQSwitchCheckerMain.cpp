/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

// A checker for cross-process switch tests: viewers of switch test origins'
// tracks, straight or through a proxy, for --duration_ms, then a JSON report
// of what each got (see SwitchChecker). Exits 0 if the report is ok, 1 if a
// viewer saw a violation or failed, and 2 if a session couldn't connect.

#include <folly/Conv.h>
#include <folly/FileUtil.h>
#include <folly/String.h>
#include <folly/coro/AsyncScope.h>
#include <folly/coro/BlockingWait.h>
#include <folly/coro/CurrentExecutor.h>
#include <folly/coro/Sleep.h>
#include <folly/init/Init.h>
#include <folly/io/async/ScopedEventBaseThread.h>
#include <folly/json.h>
#include <folly/synchronization/Baton.h>
#include <iostream>

#include "moxygen/MoQVersions.h"
#include "moxygen/events/MoQFollyExecutorImpl.h"
#include "moxygen/samples/util/Utils.h"
#include "moxygen/test/MoQSwitchChecker.h"
#include "moxygen/test/MoQSwitchOrigin.h"
#include "moxygen/util/InsecureVerifierDangerousDoNotUseInProduction.h"

DEFINE_string(url, "", "URL of the proxy or origin to watch (required)");
DEFINE_string(transport, "h3wt", "Transport: 'quic', 'h3wt' or 'qmux'");
DEFINE_bool(insecure, false, "Skip certificate validation");
DEFINE_string(
    versions,
    "",
    "Comma-separated MoQ draft versions (e.g. \"16,18\"). Empty = all "
    "supported. Joiners need a draft before FETCH filling, such as 18.");
DEFINE_int32(connect_timeout_ms, 2000, "Connect timeout");
DEFINE_int32(
    transaction_timeout_ms,
    10000,
    "How long the setup and each SUBSCRIBE or FETCH may wait for a reply");
DEFINE_string(
    tracks,
    "live/video",
    "Comma-separated tracks to watch, each namespace/.../name");
DEFINE_uint64(
    objects_per_group,
    4,
    "Objects per group, as the origins publish");
DEFINE_uint64(subgroups_per_group, 1, "Subgroups per group, on subgroups");
DEFINE_string(delivery, "subgroups", "subgroups, objects or datagrams");
DEFINE_int32(viewers, 1, "Sessions watching every track from the start");
DEFINE_string(
    join_at_ms,
    "",
    "Comma-separated times into the run, each under --duration_ms, at which a "
    "session joins every track with a joining FETCH");
DEFINE_uint64(
    join_groups,
    1,
    "How many groups before its start group a joiner's history begins");
DEFINE_string(
    expected_end,
    "",
    "The PUBLISH_DONE status code every viewer must get; empty expects none");
DEFINE_string(
    expected_reset,
    "",
    "The code viewers' open subgroup streams may be reset with; empty "
    "expects no reset");
DEFINE_int64(
    max_stall_ms,
    0,
    "The longest a viewer may go without an object; 0 allows any");
DEFINE_int64(
    duration_ms,
    10000,
    "How long to watch, once the viewers have subscribed");
DEFINE_int64(
    settle_ms,
    3000,
    "How long after --duration_ms to wait for replies, joiners' histories and "
    "ended tracks' streams to arrive");
DEFINE_string(report, "", "Where to write the JSON report; empty = stdout");

namespace {
using namespace moxygen;
using namespace moxygen::test;
using Clock = SwitchChecker::Clock;
using std::chrono::milliseconds;

constexpr int kExitViolations = 1;
constexpr int kExitConnectFailed = 2;
constexpr milliseconds kSettlePoll{20};

std::optional<std::vector<milliseconds>> parseJoinTimes(
    const std::string& times,
    milliseconds duration) {
  std::vector<std::string_view> parts;
  folly::split(',', times, parts, /*ignoreEmpty=*/true);
  std::vector<milliseconds> joinTimes;
  for (auto part : parts) {
    auto time = folly::tryTo<int64_t>(part);
    if (!time || *time < 0 || milliseconds(*time) >= duration) {
      return std::nullopt;
    }
    joinTimes.emplace_back(*time);
  }
  std::ranges::sort(joinTimes);
  return joinTimes;
}

folly::coro::Task<void> sleepUntil(Clock::time_point deadline) {
  if (const auto now = Clock::now(); now < deadline) {
    co_await folly::coro::sleep(
        std::chrono::duration_cast<milliseconds>(deadline - now));
  }
}

class CheckerRun {
 public:
  CheckerRun(
      std::shared_ptr<MoQFollyExecutorImpl> executor,
      proxygen::URL url,
      samples::TransportType transport,
      std::vector<milliseconds> joinTimes,
      SwitchChecker& checker)
      : executor_(std::move(executor)),
        url_(std::move(url)),
        transport_(transport),
        joinTimes_(std::move(joinTimes)),
        checker_(checker) {}

  // Each join starts on time, however long earlier ones take to connect and
  // subscribe.
  folly::coro::Task<folly::dynamic> run() {
    for (int i = 0; i < FLAGS_viewers; i++) {
      if (auto session = co_await connect()) {
        co_await checker_.addViewers(std::move(session));
      }
    }
    checker_.startWatch();
    const auto start = Clock::now();
    const auto executor = co_await folly::coro::co_current_executor;
    for (auto at : joinTimes_) {
      co_await sleepUntil(start + at);
      joins_.add(folly::coro::co_withExecutor(executor, join()));
    }
    co_await sleepUntil(start + milliseconds(FLAGS_duration_ms));
    // Each join's connect and SUBSCRIBE have time limits, so the report
    // always includes every join, or its error.
    co_await joins_.joinAsync();
    const auto settleBy = Clock::now() + milliseconds(FLAGS_settle_ms);
    while (!checker_.settled() && Clock::now() < settleBy) {
      co_await folly::coro::sleep(kSettlePoll);
    }
    auto report = checker_.report(Clock::now());
    closeSessions();
    co_await checker_.join();
    clients_.clear();
    co_return report;
  }

 private:
  folly::coro::Task<void> join() {
    if (auto session = co_await connect()) {
      co_await checker_.addJoiners(std::move(session));
    }
  }

  void closeSessions() {
    for (auto& client : clients_) {
      if (client->moqSession_ && !client->moqSession_->isClosed()) {
        client->moqSession_->close(SessionCloseErrorCode::NO_ERROR);
      }
    }
  }

  folly::coro::Task<std::shared_ptr<MoQSession>> connect() {
    // Named locals: gcc 11 mishandles temporaries in a co_await expression.
    quic::TransportSettings transportSettings;
    transportSettings.orderedReadCallbacks = true;
    const auto alpns = getMoqtProtocols(FLAGS_versions, true);
    std::shared_ptr<fizz::CertificateVerifier> verifier;
    if (FLAGS_insecure) {
      verifier = std::make_shared<
          test::InsecureVerifierDangerousDoNotUseInProduction>();
    }
    auto client = samples::makeRelayClientTransport(
        executor_, url_, std::move(verifier), transport_);
    if (!client) {
      checker_.addConnectError("no client for the URL and transport");
      co_return nullptr;
    }
    auto setup = co_await folly::coro::co_awaitTry(client->setupMoQSession(
        milliseconds(FLAGS_connect_timeout_ms),
        milliseconds(FLAGS_transaction_timeout_ms),
        /*publishHandler=*/nullptr,
        /*subscribeHandler=*/nullptr,
        transportSettings,
        alpns));
    if (setup.hasException() || !client->moqSession_) {
      checker_.addConnectError(
          setup.hasException() ? setup.exception().what().toStdString()
                               : "no session");
      co_return nullptr;
    }
    auto session = client->moqSession_;
    // A request with no reply times out, rather than hold up the run.
    auto settings = session->getMoqSettings();
    settings.requestTimeout = milliseconds(FLAGS_transaction_timeout_ms);
    session->setMoqSettings(settings);
    clients_.push_back(std::move(client));
    co_return session;
  }

  std::shared_ptr<MoQFollyExecutorImpl> executor_;
  proxygen::URL url_;
  samples::TransportType transport_;
  std::vector<milliseconds> joinTimes_;
  SwitchChecker& checker_;
  std::vector<std::unique_ptr<MoQClientBase>> clients_;
  folly::coro::AsyncScope joins_;
};

int exitCode(const folly::dynamic& report) {
  if (!report["connect_errors"].empty()) {
    return kExitConnectFailed;
  }
  return report["ok"].asBool() ? 0 : kExitViolations;
}

} // namespace

int main(int argc, char* argv[]) {
  folly::Init init(&argc, &argv, true);
  proxygen::URL url(FLAGS_url);
  XLOG_IF(FATAL, !url.isValid() || !url.hasHost())
      << "--url must be a valid URL";
  auto transport = samples::parseTransportType(FLAGS_transport);
  XLOG_IF(FATAL, !transport) << "Invalid --transport";
  XLOG_IF(
      FATAL,
      FLAGS_objects_per_group == 0 ||
          FLAGS_objects_per_group > kMaxObjectsPerGroup ||
          FLAGS_subgroups_per_group == 0)
      << "--objects_per_group must be in 1.." << kMaxObjectsPerGroup
      << ", and --subgroups_per_group positive";
  XLOG_IF(FATAL, FLAGS_transaction_timeout_ms <= 0)
      << "--transaction_timeout_ms must be positive";
  XLOG_IF(FATAL, FLAGS_duration_ms <= 0 || FLAGS_settle_ms < 0)
      << "--duration_ms must be positive and --settle_ms not negative";
  XLOG_IF(FATAL, FLAGS_max_stall_ms < 0) << "--max_stall_ms can't be negative";
  const auto delivery = parseDelivery(FLAGS_delivery);
  XLOG_IF(FATAL, !delivery)
      << "--delivery must be subgroups, objects or datagrams";
  auto tracks = parseTrackNames(FLAGS_tracks);
  XLOG_IF(FATAL, !tracks) << "Invalid --tracks: " << FLAGS_tracks;
  auto joinTimes =
      parseJoinTimes(FLAGS_join_at_ms, milliseconds(FLAGS_duration_ms));
  XLOG_IF(FATAL, !joinTimes)
      << "--join_at_ms must list times in [0, --duration_ms)";
  XLOG_IF(
      FATAL, FLAGS_viewers < 0 || (FLAGS_viewers == 0 && joinTimes->empty()))
      << "Watch with at least one viewer or joiner";

  SwitchCheckerOptions options{
      .tracks = std::move(*tracks), .joinGroups = FLAGS_join_groups};
  if (!FLAGS_expected_end.empty()) {
    options.expectedEnd = parsePublishDoneStatusCode(FLAGS_expected_end);
    XLOG_IF(FATAL, !options.expectedEnd)
        << "Invalid --expected_end: " << FLAGS_expected_end;
  }
  if (!FLAGS_expected_reset.empty()) {
    options.expectedReset = parseResetStreamErrorCode(FLAGS_expected_reset);
    XLOG_IF(FATAL, !options.expectedReset)
        << "Invalid --expected_reset: " << FLAGS_expected_reset;
  }
  if (FLAGS_max_stall_ms > 0) {
    options.maxStall = milliseconds(FLAGS_max_stall_ms);
  }

  // The sessions and the checker run on one worker.
  folly::ScopedEventBaseThread worker("MoQSwitchChecker");
  auto& workerEvb = *worker.getEventBase();
  auto report = folly::coro::blockingWait(
      folly::coro::co_withExecutor(
          &workerEvb,
          folly::coro::co_invoke([&]() -> folly::coro::Task<folly::dynamic> {
            SwitchChecker checker(
                SwitchTrackShape{
                    .objectsPerGroup = FLAGS_objects_per_group,
                    .subgroupsPerGroup = FLAGS_subgroups_per_group,
                    .delivery = *delivery},
                std::move(options));
            CheckerRun run(
                std::make_shared<MoQFollyExecutorImpl>(&workerEvb),
                url,
                *transport,
                std::move(*joinTimes),
                checker);
            co_return co_await run.run();
          })));

  const auto json = folly::toPrettyJson(report) + "\n";
  if (FLAGS_report.empty()) {
    std::cout << json;
  } else if (!folly::writeFile(json, FLAGS_report.c_str())) {
    XLOG(FATAL) << "Can't write the report to " << FLAGS_report;
  }

  // Closing QUIC connections finish on the worker loop; give them a moment
  // before it is destroyed.
  constexpr uint32_t kShutdownGraceMs = 500;
  folly::Baton<> drained;
  workerEvb.runInEventBaseThread([&workerEvb, &drained] {
    workerEvb.runAfterDelay([&drained] { drained.post(); }, kShutdownGraceMs);
  });
  drained.wait();
  return exitCode(report);
}

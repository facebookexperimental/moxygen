/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <folly/coro/BlockingWait.h>
#include <folly/coro/Collect.h>
#include <folly/init/Init.h>
#include <folly/logging/xlog.h>
#include "moxygen/moqtest/MoQTestPublisher.h"
#include "moxygen/moqtest/Utils.h"
#include "moxygen/relay/MoQRelayClient.h"
#include "moxygen/samples/util/Utils.h"
#include "moxygen/util/InsecureVerifierDangerousDoNotUseInProduction.h"
#include "moxygen/util/SignalHandler.h"

DEFINE_string(url, "http://localhost:9999/moq-relay", "Relay URL");
DEFINE_string(
    track,
    "",
    "moq-test track namespace to PUBLISH, e.g. "
    "'moq-test-00/0/0/0/2/5/5/1024/100/50/1/1/0/-1/-1/0'. Repeat to publish "
    "more than one track. The namespace encodes the track parameters.");
DEFINE_int32(connect_timeout, 1000, "Connect timeout (ms)");
DEFINE_int32(transaction_timeout, 120000, "Transaction timeout (ms)");
DEFINE_string(
    transport,
    "h3wt",
    "Transport: 'quic' (raw QUIC), 'h3wt' (HTTP/3 + WebTransport, default), "
    "'qmux' (QMUX-on-TCP, TLS via Fizz mandatory).");
DEFINE_string(
    versions,
    "",
    "Comma-separated MoQ draft versions (e.g. '14,16'). Empty = all supported.");
DEFINE_bool(
    include_timestamp_extension,
    false,
    "Stamp each object with a send-time millisecond timestamp extension.");

namespace {

// gflags has no native repeated-string flag, so collect --track from argv.
std::vector<std::string> collectTracks(int argc, char** argv) {
  std::vector<std::string> tracks;
  constexpr folly::StringPiece kPrefix{"--track="};
  for (int i = 1; i < argc; i++) {
    folly::StringPiece arg(argv[i]);
    if (arg.startsWith(kPrefix)) {
      arg.advance(kPrefix.size());
      tracks.emplace_back(arg.str());
    }
  }
  return tracks;
}

} // namespace

int main(int argc, char** argv) {
  auto trackArgs = collectTracks(argc, argv);
  gflags::ParseCommandLineFlags(&argc, &argv, false);
  folly::Init init(&argc, &argv);

  if (trackArgs.empty()) {
    XLOG(ERR) << "At least one --track is required";
    return 1;
  }

  // Decode up front so a bad namespace fails before we connect.
  std::vector<std::pair<moxygen::FullTrackName, moxygen::MoQTestParameters>>
      tracks;
  for (const auto& trackArg : trackArgs) {
    moxygen::TrackNamespace ns(trackArg, "/");
    auto params = moxygen::convertTrackNamespaceToMoqTestParam(&ns);
    if (params.hasError()) {
      XLOG(ERR) << "Invalid --track=" << trackArg << ": "
                << params.error().what();
      return 1;
    }
    moxygen::FullTrackName ftn;
    ftn.trackNamespace = ns;
    ftn.trackName = "test";
    tracks.emplace_back(std::move(ftn), params.value());
  }

  // Not selectClientTransport(): that also requires the deprecated
  // --quic_transport flag, which this binary has no reason to carry.
  auto transportType = moxygen::samples::parseTransportType(FLAGS_transport);
  if (!transportType) {
    XLOG(ERR) << "Invalid --transport=" << FLAGS_transport
              << " (must be one of: quic, h3wt, qmux)";
    return 1;
  }

  proxygen::URL url(FLAGS_url);
  if (!url.isValid() || !url.hasHost()) {
    XLOG(ERR) << "Invalid url: " << FLAGS_url;
    return 1;
  }

  folly::EventBase evb;
  auto moqEvb = std::make_shared<moxygen::MoQFollyExecutorImpl>(&evb);
  auto publisher = std::make_shared<moxygen::MoQTestPublisher>();
  publisher->setIncludeTimestampExtension(FLAGS_include_timestamp_extension);

  auto relayClient = std::make_unique<moxygen::MoQRelayClient>(
      moxygen::samples::makeRelayClientTransport(
          moqEvb,
          std::move(url),
          moxygen::MoQRelaySession::createRelaySessionFactory(),
          std::make_shared<
              moxygen::test::InsecureVerifierDangerousDoNotUseInProduction>(),
          *transportType));

  // A signal means stop now, mid-track. drain() would wait for the publishes
  // to finish, which is the opposite of what we want, so close outright.
  // SignalHandler terminates the loop and restores the default disposition, so
  // a second Ctrl-C still force-quits.
  moxygen::SignalHandler signalHandler(&evb, [&](int) {
    publisher->cancelAll();
    if (auto session = relayClient->getSession()) {
      session->close(moxygen::SessionCloseErrorCode::NO_ERROR);
    }
  });

  XLOG(INFO) << "Connecting to " << FLAGS_url;
  // Pass the EventBase so blockingWait drives it; the loop below has not
  // started yet, and without this the setup task would never be run.
  folly::coro::blockingWait(
      folly::coro::co_withExecutor(
          &evb,
          relayClient->setup(
              /*publisher=*/publisher,
              /*subscriber=*/nullptr,
              std::chrono::milliseconds(FLAGS_connect_timeout),
              std::chrono::milliseconds(FLAGS_transaction_timeout),
              quic::TransportSettings(),
              moxygen::getMoqtProtocols(FLAGS_versions, true))),
      &evb);

  auto session = relayClient->getSession();
  if (!session) {
    XLOG(ERR) << "Failed to establish a session with " << FLAGS_url;
    return 1;
  }

  // Each track parks in publishTrack until the peer turns forwarding on, so
  // publish them concurrently rather than serially.
  std::vector<folly::coro::Task<void>> publishes;
  publishes.reserve(tracks.size());
  for (size_t i = 0; i < tracks.size(); i++) {
    XLOG(INFO) << "PUBLISH " << tracks[i].first.trackNamespace;
    publishes.emplace_back(publisher->publishTrack(
        session, tracks[i].first, tracks[i].second, moxygen::RequestID(i)));
  }

  bool anyFailed = false;
  folly::coro::co_withExecutor(
      &evb,
      folly::coro::co_invoke(
          [&publishes, &relayClient, &evb, &anyFailed]()
              -> folly::coro::Task<void> {
            auto results =
                co_await folly::coro::collectAllTryRange(std::move(publishes));
            for (const auto& result : results) {
              // A cancelled publish is a signal-initiated shutdown, not a
              // failure.
              if (result.hasException<folly::OperationCancelled>()) {
                continue;
              }
              if (result.hasException()) {
                anyFailed = true;
                XLOG(ERR) << "PUBLISH failed: "
                          << result.exception().what().toStdString();
              }
            }
            XLOG(INFO) << "All tracks done";
            // Nothing is outstanding now, so drain for a clean close and let
            // the loop finish flushing it.
            if (auto session = relayClient->getSession()) {
              session->drain();
            }
            evb.terminateLoopSoon();
          }))
      .start();

  evb.loop();
  return anyFailed ? 1 : 0;
}

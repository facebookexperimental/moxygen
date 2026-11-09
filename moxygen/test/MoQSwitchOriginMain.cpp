/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

// A switch test origin for cross-process switch tests: it publishes live
// tracks whose content is a pure function of the track, the location and the
// time, so origins started at different moments serve the same groups at the
// same moments, each object stamped with --hub_id. Faults are injected with
// one-line commands on --control_port (see SwitchOriginControl), or with
// process signals.

#include <folly/init/Init.h>
#include <folly/io/async/AsyncTimeout.h>
#include <folly/io/async/ScopedEventBaseThread.h>
#include <folly/synchronization/Baton.h>
#include <proxygen/httpserver/samples/hq/FizzContext.h>
#include <cstdlib>
#include <iostream>
#include <limits>
#include <optional>

#include "moxygen/MoQServer.h"
#include "moxygen/MoQVersions.h"
#include "moxygen/test/MoQSwitchOrigin.h"
#include "moxygen/util/SignalHandler.h"

DEFINE_string(cert, "", "Cert path");
DEFINE_string(key, "", "Key path");
DEFINE_bool(insecure, false, "Serve a self-signed certificate");
DEFINE_string(endpoint, "/moq-origin", "End point");
DEFINE_int32(port, 9668, "MoQ server port");
DEFINE_string(
    versions,
    "",
    "Comma-separated MoQ draft versions (e.g. \"16,18\"). Empty = all supported.");
DEFINE_int64(
    hub_id,
    -1,
    "Stamped on every object, telling origins apart; required, and different "
    "on each origin");
DEFINE_string(
    tracks,
    "live/video",
    "Comma-separated tracks to publish, each namespace/.../name");
DEFINE_uint64(objects_per_group, 4, "Objects per group");
DEFINE_uint64(subgroups_per_group, 1, "Subgroups per group, on subgroups");
DEFINE_string(delivery, "subgroups", "subgroups, objects or datagrams");
DEFINE_int64(group_duration_ms, 2000, "How long each group lasts");
DEFINE_int64(
    clock_offset_ms,
    0,
    "How far this origin runs ahead of the shared clock, or behind if negative");
DEFINE_int32(
    control_port,
    0,
    "TCP port for fault commands, one per line; 0 serves none");
DEFINE_string(
    control_address,
    "::1",
    "Address the control port listens on; it takes no authentication");
DEFINE_int32(tick_ms, 5, "How often the origin publishes what fell due");

namespace {
using namespace moxygen;
using namespace moxygen::test;

class OriginServer : public MoQServer {
 public:
  template <typename... BaseArgs>
  explicit OriginServer(
      std::shared_ptr<SwitchTestHub> hub,
      BaseArgs&&... baseArgs)
      : MoQServer(std::forward<BaseArgs>(baseArgs)...), hub_(std::move(hub)) {}

  void onNewSession(std::shared_ptr<MoQSession> clientSession) override {
    clientSession->setPublishHandler(hub_);
    hub_->attach(clientSession);
  }

 private:
  std::shared_ptr<SwitchTestHub> hub_;
};

std::chrono::milliseconds sinceEpoch() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::system_clock::now().time_since_epoch());
}

bool validPort(int32_t port) {
  return port > 0 && port <= std::numeric_limits<uint16_t>::max();
}

} // namespace

int main(int argc, char* argv[]) {
  folly::Init init(&argc, &argv, true);
  XLOG_IF(FATAL, FLAGS_hub_id < 0) << "--hub_id is required";
  XLOG_IF(
      FATAL,
      FLAGS_objects_per_group == 0 ||
          FLAGS_objects_per_group > kMaxObjectsPerGroup ||
          FLAGS_subgroups_per_group == 0)
      << "--objects_per_group must be in 1.." << kMaxObjectsPerGroup
      << ", and --subgroups_per_group positive";
  XLOG_IF(
      FATAL,
      FLAGS_group_duration_ms <= 0 ||
          FLAGS_group_duration_ms > kMaxGroupDuration.count())
      << "--group_duration_ms must be in 1.." << kMaxGroupDuration.count();
  XLOG_IF(FATAL, std::abs(FLAGS_clock_offset_ms) > kMaxClockOffset.count())
      << "--clock_offset_ms must be within " << kMaxClockOffset.count();
  XLOG_IF(FATAL, FLAGS_tick_ms <= 0) << "--tick_ms must be positive";
  XLOG_IF(FATAL, !validPort(FLAGS_port)) << "--port must be in 1..65535";
  XLOG_IF(FATAL, FLAGS_control_port != 0 && !validPort(FLAGS_control_port))
      << "--control_port must be 0 or in 1..65535";
  std::optional<folly::SocketAddress> controlAddress;
  if (FLAGS_control_port > 0) {
    try {
      controlAddress.emplace(FLAGS_control_address, FLAGS_control_port);
    } catch (const std::exception& ex) {
      XLOG(FATAL) << "Invalid --control_address: " << ex.what();
    }
    XLOG_IF(WARN, !controlAddress->isLoopbackAddress())
        << "The control port, which takes no authentication, listens beyond "
        << "loopback on " << controlAddress->describe();
  }

  const auto delivery = parseDelivery(FLAGS_delivery);
  XLOG_IF(FATAL, !delivery)
      << "--delivery must be subgroups, objects or datagrams";
  auto tracks = parseTrackNames(FLAGS_tracks);
  XLOG_IF(FATAL, !tracks) << "Invalid --tracks: " << FLAGS_tracks;

  // The hub, its sessions and the control server all run on one worker, and
  // are destroyed there.
  folly::ScopedEventBaseThread worker("MoQSwitchOrigin");
  auto& workerEvb = *worker.getEventBase();

  const SwitchTrackShape shape{
      .objectsPerGroup = FLAGS_objects_per_group,
      .subgroupsPerGroup = FLAGS_subgroups_per_group,
      .delivery = *delivery};
  auto hub = std::make_shared<SwitchTestHub>(
      static_cast<uint64_t>(FLAGS_hub_id),
      std::make_shared<SwitchOrigin>(shape));
  auto publisher = std::make_unique<SwitchClockPublisher>(
      hub,
      shape.objectsPerGroup,
      SwitchClockPublisher::Options{
          .tracks = std::move(*tracks),
          .groupDuration = std::chrono::milliseconds(FLAGS_group_duration_ms),
          .offset = std::chrono::milliseconds(FLAGS_clock_offset_ms)});
  auto control = std::make_unique<SwitchOriginControl>(hub, *publisher);

  std::vector<std::string> alpns = {"h3"};
  auto moqt = getMoqtProtocols(FLAGS_versions, true);
  alpns.insert(alpns.end(), moqt.begin(), moqt.end());
  auto fizzContext = FLAGS_insecure
      ? quic::samples::createFizzServerContextWithInsecureDefault(
            alpns, fizz::server::ClientAuthMode::None, "", "")
      : quic::samples::createFizzServerContext(
            alpns,
            fizz::server::ClientAuthMode::Optional,
            FLAGS_cert,
            FLAGS_key);
  auto server = std::make_shared<OriginServer>(
      hub,
      std::move(fizzContext),
      FLAGS_endpoint,
      MoQServer::Options{
          .transportSettings = MoQServer::defaultTransportSettings()});
  server->start(folly::SocketAddress("::", FLAGS_port), {&workerEvb});

  std::unique_ptr<SwitchOriginControlServer> controlServer;
  std::unique_ptr<folly::AsyncTimeout> tick;
  workerEvb.runInEventBaseThreadAndWait([&] {
    if (controlAddress) {
      controlServer =
          std::make_unique<SwitchOriginControlServer>(&workerEvb, *control);
      try {
        controlServer->start(*controlAddress);
      } catch (const std::exception& ex) {
        XLOG(FATAL) << "Can't serve the control port: " << ex.what();
      }
    }
    tick = folly::AsyncTimeout::make(workerEvb, [&]() noexcept {
      publisher->publishUntil(sinceEpoch());
      tick->scheduleTimeout(std::chrono::milliseconds(FLAGS_tick_ms));
    });
    tick->scheduleTimeout(std::chrono::milliseconds(0));
  });
  // Scripts wait for this line.
  std::cout << "ready hub_id=" << FLAGS_hub_id << " port=" << FLAGS_port
            << " control_port=" << FLAGS_control_port << std::endl;

  folly::EventBase evb;
  SignalHandler signalHandler(&evb, [&](int) {
    XLOG(INFO) << "Caught signal, stopping switch origin";
    workerEvb.runInEventBaseThreadAndWait([&] {
      tick.reset();
      if (controlServer) {
        controlServer->stop();
      }
      hub->closeSessions(SessionCloseErrorCode::NO_ERROR);
    });
  });
  evb.loopForever();
  server->stop();
  workerEvb.runInEventBaseThreadAndWait([&] {
    controlServer.reset();
    control.reset();
    publisher.reset();
    server.reset();
    hub.reset();
  });
  // Closing QUIC connections finish on the worker loop; give them a moment
  // before it is destroyed.
  constexpr uint32_t kShutdownGraceMs = 500;
  folly::Baton<> drained;
  workerEvb.runInEventBaseThread([&workerEvb, &drained] {
    workerEvb.runAfterDelay([&drained] { drained.post(); }, kShutdownGraceMs);
  });
  drained.wait();
  return 0;
}

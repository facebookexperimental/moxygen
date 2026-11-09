/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/SocketAddress.h>
#include <folly/io/async/AsyncServerSocket.h>
#include <folly/io/async/EventBase.h>
#include <chrono>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "moxygen/test/MoQProxySwitchHarness.h"

// A switch test hub as a standalone origin: live tracks published on a shared
// clock, and faults injected by one-line commands.

namespace moxygen::test {

// Bounds on what shapes a track and its clock.
constexpr uint64_t kMaxObjectsPerGroup{1'000'000};
constexpr std::chrono::milliseconds kMaxGroupDuration{std::chrono::hours(1)};
constexpr std::chrono::milliseconds kMaxClockOffset{std::chrono::hours(24)};

// "a/b/name" names track "name" in namespace (a, b).
std::optional<FullTrackName> parseTrackName(std::string_view spec);
// Comma-separated tracks, each as parseTrackName takes it; at least one, and
// none twice.
std::optional<std::vector<FullTrackName>> parseTrackNames(
    std::string_view specs);
// "subgroups", "objects" or "datagrams".
std::optional<SwitchTrackShape::Delivery> parseDelivery(
    std::string_view delivery);
// A PUBLISH_DONE status code or a stream reset error code, by its draft 18
// value; values the draft doesn't define are refused.
std::optional<PublishDoneStatusCode> parsePublishDoneStatusCode(
    std::string_view code);
std::optional<ResetStreamErrorCode> parseResetStreamErrorCode(
    std::string_view code);

// Publishes live tracks on a hub as a function of the time since the Unix
// epoch, so origins started at different moments publish the same groups at
// the same moments. Group g starts at g * groupDuration on that clock, shifted
// by offset, and its objects are spread evenly over it. Times before the
// epoch, once shifted, count as the epoch. objectsPerGroup, groupDuration and
// offset must be within the bounds above.
class SwitchClockPublisher {
 public:
  struct Options {
    std::vector<FullTrackName> tracks;
    std::chrono::milliseconds groupDuration{std::chrono::seconds(2)};
    // Ahead of the shared clock, or behind it if negative.
    std::chrono::milliseconds offset{0};
  };

  SwitchClockPublisher(
      std::shared_ptr<SwitchTestHub> hub,
      uint64_t objectsPerGroup,
      Options options);

  // Publishes every object due by now. The first call starts at object 0 of
  // the group now falls in, with every object of it already due. While
  // stalled, publishes nothing; resuming publishes what fell due.
  void publishUntil(std::chrono::milliseconds sinceEpoch);
  void stall() {
    stalled_ = true;
  }
  void resume() {
    stalled_ = false;
  }

 private:
  // The objects due by then: the group, and how many of its objects.
  std::pair<uint64_t, uint64_t> dueBy(
      std::chrono::milliseconds sinceEpoch) const;

  std::shared_ptr<SwitchTestHub> hub_;
  uint64_t objectsPerGroup_;
  Options options_;
  std::optional<uint64_t> group_;
  bool stalled_{false};
};

// Runs one fault-injection command on an origin and answers it with one line:
// "ok", a value, or "error: <why>". A command that would inject nothing is an
// error: one for a track with no subscriptions, a GOAWAY no session or
// subscription takes, a close with no sessions. Commands:
//   goaway [timeout_ms [new_session_uri]]
//   request-goaway <track> [timeout_ms]
//   publish-done <track> <status code> [reset code]: resets the open subgroup
//     streams, with CANCELLED unless given, then sends PUBLISH_DONE
//   finish <track> <status code>: sends PUBLISH_DONE, and later publishing
//     finishes the open subgroup streams, as a relay passing it on
//   close
//   stall | resume
//   subscriptions <track>
class SwitchOriginControl {
 public:
  SwitchOriginControl(
      std::shared_ptr<SwitchTestHub> hub,
      SwitchClockPublisher& publisher)
      : hub_(std::move(hub)), publisher_(publisher) {}

  std::string run(std::string_view line);

 private:
  std::string goaway(const std::vector<std::string_view>& args);
  std::string requestGoaway(const std::vector<std::string_view>& args);
  std::string endTrack(const std::vector<std::string_view>& args);

  std::shared_ptr<SwitchTestHub> hub_;
  SwitchClockPublisher& publisher_;
};

// Serves a SwitchOriginControl on a TCP address: each line a connection sends
// is a command, logged and answered with one line. A line longer than
// kMaxLineLength is answered with an error and dropped. Runs on evb; replies
// to a client that isn't reading queue without bound.
class SwitchOriginControlServer
    : public folly::AsyncServerSocket::AcceptCallback,
      private folly::EventBase::LoopCallback {
 public:
  static constexpr size_t kMaxLineLength{4096};

  SwitchOriginControlServer(
      folly::EventBase* evb,
      SwitchOriginControl& control);
  ~SwitchOriginControlServer() override;

  void start(const folly::SocketAddress& address);
  // Where it listens, with the port the system chose for port 0.
  folly::SocketAddress address() const;
  void stop();

  void connectionAccepted(
      folly::NetworkSocket fd,
      const folly::SocketAddress& clientAddr,
      AcceptInfo info) noexcept override;
  void acceptError(folly::exception_wrapper ew) noexcept override;

 private:
  class Connection;

  void close(Connection* connection);
  // Destroys the connections closed since the last loop.
  void runLoopCallback() noexcept override;

  folly::EventBase* evb_;
  SwitchOriginControl& control_;
  std::shared_ptr<folly::AsyncServerSocket> socket_;
  std::map<Connection*, std::unique_ptr<Connection>> connections_;
  std::vector<std::unique_ptr<Connection>> closed_;
};

} // namespace moxygen::test

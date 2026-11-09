/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/test/MoQSwitchOrigin.h"

#include <fmt/core.h>
#include <folly/Conv.h>
#include <folly/String.h>
#include <folly/io/IOBuf.h>
#include <folly/io/async/AsyncSocket.h>
#include <folly/logging/xlog.h>
#include <algorithm>
#include <array>
#include <utility>

namespace moxygen::test {

std::optional<FullTrackName> parseTrackName(std::string_view spec) {
  std::vector<std::string> parts;
  folly::split('/', spec, parts);
  if (parts.size() < 2) {
    return std::nullopt;
  }
  for (const auto& part : parts) {
    if (part.empty()) {
      return std::nullopt;
    }
  }
  auto name = std::move(parts.back());
  parts.pop_back();
  return FullTrackName{TrackNamespace(std::move(parts)), std::move(name)};
}

std::optional<std::vector<FullTrackName>> parseTrackNames(
    std::string_view specs) {
  std::vector<std::string_view> parts;
  folly::split(',', specs, parts, /*ignoreEmpty=*/true);
  std::vector<FullTrackName> names;
  for (auto part : parts) {
    auto name = parseTrackName(folly::trimWhitespace(part));
    if (!name || std::ranges::find(names, *name) != names.end()) {
      return std::nullopt;
    }
    names.push_back(std::move(*name));
  }
  if (names.empty()) {
    return std::nullopt;
  }
  return names;
}

std::optional<SwitchTrackShape::Delivery> parseDelivery(
    std::string_view delivery) {
  if (delivery == "subgroups") {
    return SwitchTrackShape::Delivery::SubgroupStreams;
  }
  if (delivery == "objects") {
    return SwitchTrackShape::Delivery::ObjectStreams;
  }
  if (delivery == "datagrams") {
    return SwitchTrackShape::Delivery::Datagrams;
  }
  return std::nullopt;
}

std::optional<PublishDoneStatusCode> parsePublishDoneStatusCode(
    std::string_view code) {
  static constexpr std::array kCodes{
      PublishDoneStatusCode::INTERNAL_ERROR,
      PublishDoneStatusCode::UNAUTHORIZED,
      PublishDoneStatusCode::TRACK_ENDED,
      PublishDoneStatusCode::SUBSCRIPTION_ENDED,
      PublishDoneStatusCode::GOING_AWAY,
      PublishDoneStatusCode::TOO_FAR_BEHIND,
      PublishDoneStatusCode::EXPIRED,
      PublishDoneStatusCode::UPDATE_FAILED,
      PublishDoneStatusCode::EXCESSIVE_LOAD,
      PublishDoneStatusCode::MALFORMED_TRACK};
  auto value = folly::tryTo<uint32_t>(code);
  if (!value) {
    return std::nullopt;
  }
  auto status = static_cast<PublishDoneStatusCode>(*value);
  return std::ranges::find(kCodes, status) != kCodes.end()
      ? std::optional(status)
      : std::nullopt;
}

std::optional<ResetStreamErrorCode> parseResetStreamErrorCode(
    std::string_view code) {
  static constexpr std::array kCodes{
      ResetStreamErrorCode::INTERNAL_ERROR,
      ResetStreamErrorCode::CANCELLED,
      ResetStreamErrorCode::DELIVERY_TIMEOUT,
      ResetStreamErrorCode::SESSION_CLOSED,
      ResetStreamErrorCode::GOING_AWAY,
      ResetStreamErrorCode::TOO_FAR_BEHIND,
      ResetStreamErrorCode::UNKNOWN_OBJECT_STATUS,
      ResetStreamErrorCode::EXPIRED_AUTH_TOKEN,
      ResetStreamErrorCode::EXCESSIVE_LOAD,
      ResetStreamErrorCode::MALFORMED_TRACK};
  auto value = folly::tryTo<uint32_t>(code);
  if (!value) {
    return std::nullopt;
  }
  auto error = static_cast<ResetStreamErrorCode>(*value);
  return std::ranges::find(kCodes, error) != kCodes.end() ? std::optional(error)
                                                          : std::nullopt;
}

SwitchClockPublisher::SwitchClockPublisher(
    std::shared_ptr<SwitchTestHub> hub,
    uint64_t objectsPerGroup,
    Options options)
    : hub_(std::move(hub)),
      objectsPerGroup_(objectsPerGroup),
      options_(std::move(options)) {
  XCHECK(objectsPerGroup_ > 0 && objectsPerGroup_ <= kMaxObjectsPerGroup);
  XCHECK(
      options_.groupDuration.count() > 0 &&
      options_.groupDuration <= kMaxGroupDuration);
  XCHECK(
      options_.offset >= -kMaxClockOffset &&
      options_.offset <= kMaxClockOffset);
}

std::pair<uint64_t, uint64_t> SwitchClockPublisher::dueBy(
    std::chrono::milliseconds sinceEpoch) const {
  const auto shifted = static_cast<uint64_t>(
      std::max<int64_t>((sinceEpoch + options_.offset).count(), 0));
  const auto duration = static_cast<uint64_t>(options_.groupDuration.count());
  return {
      shifted / duration, shifted % duration * objectsPerGroup_ / duration + 1};
}

void SwitchClockPublisher::publishUntil(std::chrono::milliseconds sinceEpoch) {
  if (stalled_) {
    return;
  }
  const auto [group, due] = dueBy(sinceEpoch);
  if (!group_) {
    group_ = group;
  }
  for (; *group_ < group; ++*group_) {
    for (const auto& track : options_.tracks) {
      hub_->publish(track, *group_, objectsPerGroup_);
    }
  }
  for (const auto& track : options_.tracks) {
    hub_->publish(track, group, due);
  }
}

namespace {

std::string error(std::string_view why) {
  return fmt::format("error: {}", why);
}

// The argument at i as a number, 0 if there is none.
std::optional<uint64_t> numberOrZero(
    const std::vector<std::string_view>& args,
    size_t i) {
  if (i >= args.size()) {
    return 0;
  }
  auto value = folly::tryTo<uint64_t>(args[i]);
  return value ? std::optional(*value) : std::nullopt;
}

} // namespace

std::string SwitchOriginControl::run(std::string_view line) {
  std::vector<std::string_view> args;
  folly::split(' ', line, args, /*ignoreEmpty=*/true);
  if (args.empty()) {
    return error("empty command");
  }
  const auto command = args.front();
  if (command == "goaway") {
    return goaway(args);
  }
  if (command == "request-goaway") {
    return requestGoaway(args);
  }
  if (command == "publish-done" || command == "finish") {
    return endTrack(args);
  }
  if (command == "close" && args.size() == 1) {
    return hub_->closeSessions() > 0 ? "ok" : error("no sessions to close");
  }
  if (command == "stall" && args.size() == 1) {
    publisher_.stall();
    return "ok";
  }
  if (command == "resume" && args.size() == 1) {
    publisher_.resume();
    return "ok";
  }
  if (command == "subscriptions") {
    auto name = args.size() == 2 ? parseTrackName(args[1]) : std::nullopt;
    if (!name) {
      return error("usage: subscriptions <track>");
    }
    return std::to_string(hub_->subscriptions(*name));
  }
  return error(fmt::format("unknown command: {}", line));
}

std::string SwitchOriginControl::goaway(
    const std::vector<std::string_view>& args) {
  auto timeout = numberOrZero(args, 1);
  if (!timeout || args.size() > 3) {
    return error("usage: goaway [timeout_ms [new_session_uri]]");
  }
  const auto sent = hub_->sendGoaway(
      Goaway{
          .newSessionUri = args.size() > 2 ? std::string(args[2]) : "",
          .timeout = *timeout});
  return sent > 0 ? "ok"
                  : error("no session took it: none, or each already has one");
}

std::string SwitchOriginControl::requestGoaway(
    const std::vector<std::string_view>& args) {
  auto name = args.size() > 1 ? parseTrackName(args[1]) : std::nullopt;
  auto timeout = numberOrZero(args, 2);
  if (!name || !timeout || args.size() > 3) {
    return error("usage: request-goaway <track> [timeout_ms]");
  }
  if (hub_->subscriptions(*name) == 0) {
    return error(fmt::format("no subscriptions to {}", name->describe()));
  }
  const auto sent =
      hub_->sendRequestGoaway(*name, std::chrono::milliseconds(*timeout));
  return sent > 0
      ? "ok"
      : error(
            "no subscription took it: each is before draft 18 or already "
            "has one");
}

std::string SwitchOriginControl::endTrack(
    const std::vector<std::string_view>& args) {
  const bool finish = args.front() == "finish";
  auto name = args.size() > 1 ? parseTrackName(args[1]) : std::nullopt;
  auto status =
      args.size() > 2 ? parsePublishDoneStatusCode(args[2]) : std::nullopt;
  auto reset = args.size() > 3 ? parseResetStreamErrorCode(args[3])
                               : std::optional(ResetStreamErrorCode::CANCELLED);
  if (!name || !status || !reset || args.size() > (finish ? 3 : 4)) {
    return error(
        finish ? "usage: finish <track> <status code>"
               : "usage: publish-done <track> <status code> [reset code]");
  }
  if (hub_->subscriptions(*name) == 0) {
    return error(fmt::format("no subscriptions to {}", name->describe()));
  }
  if (finish) {
    hub_->finishSubscriptions(*name, *status);
  } else {
    hub_->endSubscriptions(*name, *status, *reset);
  }
  return "ok";
}

class SwitchOriginControlServer::Connection
    : public folly::AsyncReader::ReadCallback {
 public:
  Connection(
      SwitchOriginControlServer& server,
      folly::AsyncSocket::UniquePtr socket)
      : server_(server), socket_(std::move(socket)) {}

  // Its socket would otherwise call readEOF() as it closes.
  ~Connection() override {
    socket_->setReadCB(nullptr);
  }

  void start() {
    socket_->setReadCB(this);
  }

  void getReadBuffer(void** buf, size_t* len) override {
    *buf = buffer_.data();
    *len = buffer_.size();
  }

  // A reply that fails closes the connection, which then answers nothing
  // more.
  void readDataAvailable(size_t len) noexcept override {
    pending_.append(buffer_.data(), len);
    for (auto end = pending_.find('\n'); end != std::string::npos && !closed_;
         end = pending_.find('\n')) {
      auto line = pending_.substr(0, end);
      pending_.erase(0, end + 1);
      if (std::exchange(skipping_, false)) {
        continue;
      }
      if (line.size() > kMaxLineLength) {
        reply(error("line too long"));
        continue;
      }
      const auto command = folly::trimWhitespace(line);
      auto answer = server_.control_.run(command);
      XLOG(INFO) << "Control: " << command << " -> " << answer;
      reply(answer);
    }
    if (pending_.size() > kMaxLineLength && !closed_) {
      pending_.clear();
      if (!std::exchange(skipping_, true)) {
        reply(error("line too long"));
      }
    }
  }

  void readEOF() noexcept override {
    close();
  }

  void readErr(const folly::AsyncSocketException&) noexcept override {
    close();
  }

 private:
  void reply(const std::string& text) {
    socket_->writeChain(nullptr, folly::IOBuf::copyBuffer(text + "\n"));
  }

  void close() {
    if (!std::exchange(closed_, true)) {
      server_.close(this);
    }
  }

  SwitchOriginControlServer& server_;
  folly::AsyncSocket::UniquePtr socket_;
  std::array<char, 1024> buffer_{};
  std::string pending_;
  // Dropping the rest of a line already answered as too long.
  bool skipping_{false};
  bool closed_{false};
};

SwitchOriginControlServer::SwitchOriginControlServer(
    folly::EventBase* evb,
    SwitchOriginControl& control)
    : evb_(evb),
      control_(control),
      socket_(folly::AsyncServerSocket::newSocket(evb)) {}

SwitchOriginControlServer::~SwitchOriginControlServer() {
  stop();
}

// The accept callback runs inline on evb, so stopping is done when
// stopAccepting() returns.
void SwitchOriginControlServer::start(const folly::SocketAddress& address) {
  socket_->bind(address);
  socket_->listen(16);
  socket_->addAcceptCallback(this, nullptr);
  socket_->startAccepting();
}

folly::SocketAddress SwitchOriginControlServer::address() const {
  return socket_->getAddress();
}

void SwitchOriginControlServer::stop() {
  socket_->stopAccepting();
  auto open = std::exchange(connections_, {});
  open.clear();
  closed_.clear();
}

void SwitchOriginControlServer::connectionAccepted(
    folly::NetworkSocket fd,
    const folly::SocketAddress&,
    AcceptInfo) noexcept {
  auto connection = std::make_unique<Connection>(
      *this, folly::AsyncSocket::newSocket(evb_, fd));
  auto* raw = connection.get();
  connections_.emplace(raw, std::move(connection));
  raw->start();
}

void SwitchOriginControlServer::acceptError(
    folly::exception_wrapper ew) noexcept {
  XLOG(ERR) << "Control accept failed: " << ew.what();
}

// From the connection's own callbacks, so it is destroyed once they return.
void SwitchOriginControlServer::close(Connection* connection) {
  auto it = connections_.find(connection);
  if (it == connections_.end()) {
    return;
  }
  closed_.push_back(std::move(it->second));
  connections_.erase(it);
  if (!isLoopCallbackScheduled()) {
    evb_->runInLoop(this);
  }
}

void SwitchOriginControlServer::runLoopCallback() noexcept {
  closed_.clear();
}

} // namespace moxygen::test

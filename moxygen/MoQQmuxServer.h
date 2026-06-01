/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <fizz/server/FizzServerContext.h>
#include <folly/CancellationToken.h>
#include <folly/SocketAddress.h>
#include <folly/io/async/AsyncServerSocket.h>
#include <folly/io/async/AsyncTransport.h>
#include <folly/io/async/ScopedEventBaseThread.h>
#include <folly/synchronization/Baton.h>
#include <folly/system/HardwareConcurrency.h>
#include <proxygen/lib/transport/qmux/QmuxFramer.h>
#include <proxygen/lib/transport/qmux/QmuxSession.h>
#include <quic/state/TransportSettings.h>
#include <moxygen/MoQServerBase.h>
#include <moxygen/QmuxUtils.h>
#include <chrono>
#include <memory>
#include <unordered_set>
#include <vector>

namespace folly {
class EventBase;
} // namespace folly

namespace moxygen {

// MoQ server that accepts MoQ sessions over QMUX-on-TCP.
class MoQQmuxServer : public MoQServerBase {
 public:
  struct Config {
    using milliseconds = std::chrono::milliseconds;
    static constexpr auto kDefaultHandshakeTimeout = milliseconds(5'000);

    proxygen::qmux::QxTransportParams selfTransportParams{
        qmuxParamsFromTransportSettings(quic::TransportSettings{})};
    milliseconds handshakeTimeout{kDefaultHandshakeTimeout};
    proxygen::qmux::QmuxSession::Config sessionConfig{};
    size_t serverThreads{folly::available_concurrency()};
  };

  MoQQmuxServer(
      std::string endpoint,
      std::shared_ptr<const fizz::server::FizzServerContext> fizzContext)
      : MoQQmuxServer(std::move(endpoint), std::move(fizzContext), Config{}) {}

  MoQQmuxServer(
      std::string endpoint,
      std::shared_ptr<const fizz::server::FizzServerContext> fizzContext,
      Config config);

  ~MoQQmuxServer() override;

  MoQQmuxServer(const MoQQmuxServer&) = delete;
  MoQQmuxServer(MoQQmuxServer&&) = delete;
  MoQQmuxServer& operator=(const MoQQmuxServer&) = delete;
  MoQQmuxServer& operator=(MoQQmuxServer&&) = delete;

  void start(const folly::SocketAddress& addr) override {
    start(addr, {});
  }

  void start(
      const folly::SocketAddress& addr,
      std::vector<folly::EventBase*> evbs);

  void stop() override;

  [[nodiscard]] folly::SocketAddress getAddress() const override {
    return boundAddr_;
  }

  // Returns the live worker pool — caller-supplied or internally spawned.
  // Empty before start() / after stop().
  [[nodiscard]] const std::vector<folly::EventBase*>& getWorkerEvbs()
      const noexcept {
    return workerEvbs_;
  }

 private:
  class WorkerAcceptCallback;

  struct WorkerShutdownState {
    std::unordered_set<MoQSession*> liveSessions;
    size_t inflightAccepts{0};
    bool draining{false};
    folly::Baton<> done;
  };

  folly::coro::Task<void> handleAccept(
      folly::EventBase* workerEvb,
      std::shared_ptr<MoQExecutor> executor,
      folly::AsyncTransport::UniquePtr asyncSocket,
      WorkerShutdownState* state);

  bool isInWorkerPool() const noexcept {
    for (auto* evb : workerEvbs_) {
      if (evb->isInEventBaseThread()) {
        return true;
      }
    }
    return false;
  }

  const Config config_;
  const std::shared_ptr<const fizz::server::FizzServerContext> fizzContext_;

  folly::CancellationSource cancelSource_;
  std::vector<std::unique_ptr<WorkerShutdownState>> workerShutdownState_;
  std::vector<folly::EventBase*> workerEvbs_;
  std::vector<std::unique_ptr<folly::ScopedEventBaseThread>> ownedWorkers_;
  std::vector<std::shared_ptr<folly::AsyncServerSocket>> serverSockets_;
  std::vector<std::unique_ptr<WorkerAcceptCallback>> workerCallbacks_;
  folly::SocketAddress boundAddr_;
  bool stopped_{false};
};

} // namespace moxygen

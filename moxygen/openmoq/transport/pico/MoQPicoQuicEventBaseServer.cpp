/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/openmoq/transport/pico/MoQPicoQuicEventBaseServer.h"
#include <folly/logging/xlog.h>
#include <moxygen/openmoq/transport/pico/PicoH3WebTransport.h>
#include <moxygen/openmoq/transport/pico/PicoQuicExecutor.h>
#include <moxygen/openmoq/transport/pico/PicoQuicSocketHandler.h>
#include <moxygen/openmoq/transport/pico/PicoQuicWebTransport.h>
#include <picoquic.h>

namespace moxygen {

struct MoQPicoQuicEventBaseServer::Impl {
  std::unique_ptr<PicoQuicSocketHandler> handler;
  std::shared_ptr<PicoQuicExecutor> ownedExecutor;
  std::atomic<bool> running_{false};
};

MoQPicoQuicEventBaseServer::MoQPicoQuicEventBaseServer(
    std::string cert,
    std::string key,
    std::string endpoint,
    folly::Executor::KeepAlive<folly::EventBase> evb,
    std::string versions,
    PicoWebTransportConfig wtConfig)
    : MoQPicoServerBase(
          std::move(cert),
          std::move(key),
          std::move(endpoint),
          std::move(versions),
          std::move(wtConfig)),
      impl_(std::make_unique<Impl>()),
      evb_(std::move(evb)) {}

MoQPicoQuicEventBaseServer::~MoQPicoQuicEventBaseServer() {
  stop();
}

void MoQPicoQuicEventBaseServer::start(const folly::SocketAddress& addr) {
  if (impl_->running_.exchange(true)) {
    XLOG(WARN) << "Server already running";
    return;
  }

  // Use PicoQuicExecutor to enable synchronous task draining after packet receive.
  // This matches the threaded server's behavior where drainTasks() is called
  // after each packet loop iteration.
  impl_->ownedExecutor = std::make_shared<PicoQuicExecutor>();
  executor_ = impl_->ownedExecutor;

  if (!createQuicContext()) {
    impl_->ownedExecutor.reset();
    executor_.reset();
    impl_->running_ = false;
    return;
  }

  XLOG(INFO) << "Starting MoQPicoQuicEventBaseServer on " << addr.describe();

  impl_->handler =
      std::make_unique<PicoQuicSocketHandler>(evb_.get(), quic_);

  // Set callback to drain executor tasks after processing packets.
  // This mimics PicoQuicExecutor's loop callback which calls drainTasks()
  // after packet receive/send operations.
  impl_->handler->setDrainTasksCallback([executor = impl_->ownedExecutor.get()] {
    uint64_t currentTime = picoquic_current_time();
    executor->drainTasks();
    executor->processExpiredTimers(currentTime);
  });

  impl_->handler->start(addr);
}

void MoQPicoQuicEventBaseServer::onWebTransportCreated(
    PicoQuicWebTransport& wt) noexcept {
  wt.setUpdateWakeTimeoutCallback(
      [handler = impl_->handler.get()] { handler->updateWakeTimeout(); });
}

void MoQPicoQuicEventBaseServer::onH3WebTransportCreated(
    PicoH3WebTransport& wt) noexcept {
  wt.setUpdateWakeTimeoutCallback(
      [handler = impl_->handler.get()] { handler->updateWakeTimeout(); });
}

void MoQPicoQuicEventBaseServer::stop() {
  if (!impl_->running_.exchange(false)) {
    return;
  }

  XLOG(INFO) << "Stopping MoQPicoQuicEventBaseServer";

  if (impl_->handler) {
    impl_->handler->stop();
    impl_->handler.reset();
  }

  destroyQuicContext();
  impl_->ownedExecutor.reset();
  executor_.reset();
  evb_ = {}; // release KeepAlive so EVB destructor doesn't spin

  XLOG(INFO) << "MoQPicoQuicEventBaseServer stopped";
}

} // namespace moxygen

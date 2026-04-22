/*
 * Copyright (c) OpenMOQ contributors.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/openmoq/transport/pico/MoQPicoQuicEventBaseServer.h"
#include <folly/logging/xlog.h>
#include <moxygen/events/MoQFollyExecutorImpl.h>
#include <moxygen/openmoq/transport/pico/PicoQuicSocketHandler.h>
#include <moxygen/openmoq/transport/pico/PicoWebTransportBase.h>
#include <picoquic.h>

namespace moxygen {

struct MoQPicoQuicEventBaseServer::Impl {
  std::unique_ptr<PicoQuicSocketHandler> handler;
  std::atomic<bool> running_{false};
};

MoQPicoQuicEventBaseServer::MoQPicoQuicEventBaseServer(
    std::string cert,
    std::string key,
    std::string endpoint,
    folly::Executor::KeepAlive<folly::EventBase> evb,
    std::string versions,
    PicoTransportConfig transportConfig,
    PicoWebTransportConfig wtConfig)
    : MoQPicoServerBase(
          std::move(cert),
          std::move(key),
          std::move(endpoint),
          std::move(versions),
          std::move(transportConfig),
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

  executor_ = std::make_shared<MoQFollyExecutorImpl>(evb_.get());

  if (!createQuicContext()) {
    executor_.reset();
    impl_->running_ = false;
    return;
  }

  XLOG(INFO) << "Starting MoQPicoQuicEventBaseServer on " << addr.describe();

  impl_->handler = std::make_unique<PicoQuicSocketHandler>(evb_.get(), quic_);
  impl_->handler->start(addr);
}

void MoQPicoQuicEventBaseServer::onWebTransportCreated(
    PicoWebTransportBase& wt) noexcept {
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
  executor_.reset();
  evb_ = {}; // release KeepAlive so EVB destructor doesn't spin

  XLOG(INFO) << "MoQPicoQuicEventBaseServer stopped";
}

} // namespace moxygen

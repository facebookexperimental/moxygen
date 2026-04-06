/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/openmoq/transport/pico/MoQPicoQuicServer.h"
#include <folly/logging/xlog.h>
#include <moxygen/openmoq/transport/pico/PicoQuicExecutor.h>
#include <picoquic_packet_loop.h>

namespace moxygen {

struct MoQPicoQuicServer::Impl {
  picoquic_packet_loop_param_t loopParam_{};
  picoquic_network_thread_ctx_t* networkThreadCtx_{nullptr};
  folly::SocketAddress serverAddr_;
  std::atomic<bool> running_{false};
  // Owns the PicoQuicExecutor; base class executor_ holds a shared alias.
  std::shared_ptr<PicoQuicExecutor> ownedExecutor_;
};

MoQPicoQuicServer::MoQPicoQuicServer(
    std::string cert,
    std::string key,
    std::string endpoint,
    std::string versions,
    PicoWebTransportConfig wtConfig)
    : MoQPicoServerBase(
          std::move(cert),
          std::move(key),
          std::move(endpoint),
          std::move(versions),
          std::move(wtConfig)),
      impl_(std::make_unique<Impl>()) {}

MoQPicoQuicServer::~MoQPicoQuicServer() {
  stop();
}

void MoQPicoQuicServer::start(const folly::SocketAddress& addr) {
  if (impl_->running_.exchange(true)) {
    XLOG(WARN) << "Server already running";
    return;
  }

  impl_->serverAddr_ = addr;
  impl_->ownedExecutor_ = std::make_shared<PicoQuicExecutor>();
  executor_ = impl_->ownedExecutor_; // base class shared_ptr alias

  if (!createQuicContext()) {
    impl_->ownedExecutor_.reset();
    executor_.reset();
    impl_->running_ = false;
    return;
  }

  XLOG(INFO) << "Starting MoQPicoQuicServer on " << addr.describe();

  impl_->loopParam_ = {};
  impl_->loopParam_.local_port = static_cast<uint16_t>(addr.getPort());

  int ret = 0;
  impl_->networkThreadCtx_ = picoquic_start_network_thread(
      quic_,
      &impl_->loopParam_,
      reinterpret_cast<picoquic_packet_loop_cb_fn>(
          PicoQuicExecutor::getLoopCallback()),
      executor_.get(),
      &ret);

  if (impl_->networkThreadCtx_ == nullptr) {
    XLOG(ERR) << "Failed to start network thread, ret=" << ret;
    destroyQuicContext();
    impl_->ownedExecutor_.reset();
    executor_.reset();
    impl_->running_ = false;
    return;
  }

  XLOG(INFO) << "MoQPicoQuicServer network thread started";
}

void MoQPicoQuicServer::stop() {
  if (!impl_->running_.exchange(false)) {
    return;
  }

  XLOG(INFO) << "Stopping MoQPicoQuicServer";

  if (impl_->networkThreadCtx_) {
    picoquic_delete_network_thread(impl_->networkThreadCtx_);
    impl_->networkThreadCtx_ = nullptr;
  }

  destroyQuicContext();
  impl_->ownedExecutor_.reset();
  executor_.reset();

  XLOG(INFO) << "MoQPicoQuicServer stopped";
}

} // namespace moxygen

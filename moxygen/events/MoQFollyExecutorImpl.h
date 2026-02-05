/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/io/async/EventBase.h>
#include <quic/common/events/FollyQuicEventBase.h>
#include <moxygen/events/MoQExecutor.h>

namespace moxygen {

class MoQFollyExecutorImpl : public MoQExecutor,
                             public quic::FollyQuicEventBase {
 public:
  explicit MoQFollyExecutorImpl(folly::EventBase* evb)
      : quic::FollyQuicEventBase(evb) {}

  ~MoQFollyExecutorImpl() override {
    // Wait for all KeepAlive tokens to be released before destruction
    joinKeepAlive();
  }

  // Not copyable or movable - destructor calls joinKeepAlive()
  MoQFollyExecutorImpl(const MoQFollyExecutorImpl&) = delete;
  MoQFollyExecutorImpl& operator=(const MoQFollyExecutorImpl&) = delete;
  MoQFollyExecutorImpl(MoQFollyExecutorImpl&&) = delete;
  MoQFollyExecutorImpl& operator=(MoQFollyExecutorImpl&&) = delete;

  void add(folly::Func func) override;

  // Timeout scheduling methods
  void scheduleTimeout(
      quic::QuicTimerCallback* callback,
      std::chrono::milliseconds timeout) override;
};

} // namespace moxygen

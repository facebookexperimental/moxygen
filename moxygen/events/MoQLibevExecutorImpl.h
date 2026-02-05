/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <quic/common/events/LibevQuicEventBase.h>
#include <moxygen/events/MoQExecutor.h>

#include <folly/IntrusiveList.h>

namespace moxygen {

class MoQLibevExecutorImpl : public MoQExecutor,
                             public quic::LibevQuicEventBase {
 public:
  explicit MoQLibevExecutorImpl(
      std::unique_ptr<quic::LibevQuicEventBase::EvLoopWeak> loop);

  ~MoQLibevExecutorImpl() override {
    // Wait for all KeepAlive tokens to be released before destruction
    joinKeepAlive();
  }

  // Not copyable or movable - destructor calls joinKeepAlive()
  MoQLibevExecutorImpl(const MoQLibevExecutorImpl&) = delete;
  MoQLibevExecutorImpl& operator=(const MoQLibevExecutorImpl&) = delete;
  MoQLibevExecutorImpl(MoQLibevExecutorImpl&&) = delete;
  MoQLibevExecutorImpl& operator=(MoQLibevExecutorImpl&&) = delete;

  // Implementation of folly::Executor::add
  void add(folly::Func func) override;

  // Timeout scheduling methods
  void scheduleTimeout(
      quic::QuicTimerCallback* callback,
      std::chrono::milliseconds timeout) override;
};

} // namespace moxygen

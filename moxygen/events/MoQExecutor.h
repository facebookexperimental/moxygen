/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/DefaultKeepAliveExecutor.h>
#include <quic/common/events/QuicEventBase.h>
#include <chrono>

namespace moxygen {

class MoQExecutor : public folly::DefaultKeepAliveExecutor {
 public:
  using KeepAlive = folly::Executor::KeepAlive<MoQExecutor>;

  virtual ~MoQExecutor() override = default;

  // Returns a KeepAlive token for this executor
  KeepAlive keepAlive() {
    return getKeepAliveToken(this);
  }

  template <
      typename T,
      typename = std::enable_if_t<std::is_base_of_v<MoQExecutor, T>>>
  T* getTypedExecutor() {
    auto exec = dynamic_cast<T*>(this);
    if (exec) {
      return exec;
    } else {
      return nullptr;
    }
  }

  // Timeout scheduling methods
  virtual void scheduleTimeout(
      quic::QuicTimerCallback* callback,
      std::chrono::milliseconds timeout) = 0;
};

} // namespace moxygen

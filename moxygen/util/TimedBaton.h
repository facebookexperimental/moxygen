/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/experimental/coro/Promise.h>
#include <folly/experimental/coro/Timeout.h>

namespace moxygen {

class TimedBaton {
 public:
  TimedBaton() {
    reset();
  }

  void reset() {
    std::tie(promise_, future_) = folly::coro::makePromiseContract<void>();
  }

  folly::coro::Task<void> wait(
      std::chrono::milliseconds timeout,
      folly::Timekeeper* tk = nullptr) {
    return folly::coro::timeout(std::move(future_), timeout, tk);
  }

  folly::coro::Task<void> wait() {
    co_await std::move(future_);
  }

  void signal() {
    promise_.setValue();
  }

  void cancel() {
    promise_.setException(folly::OperationCancelled());
  }

 private:
  folly::coro::Promise<void> promise_;
  folly::coro::Future<void> future_;
};

} // namespace moxygen

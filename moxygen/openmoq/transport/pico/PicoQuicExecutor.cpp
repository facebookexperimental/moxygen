/*
 * Copyright (c) OpenMOQ contributors.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/openmoq/transport/pico/PicoQuicExecutor.h"
#include <algorithm>
#include <folly/logging/xlog.h>
#include <functional>
#include <mutex>
#include <picoquic.h>
#include <picoquic_packet_loop.h>
#include <queue>
#include <quic/common/events/QuicEventBase.h>
#include <vector>

namespace moxygen {

struct PicoQuicExecutor::Impl {
  struct TimerEntry {
    uint64_t expiryTime;
    quic::QuicTimerCallback *callback;
    bool cancelled{false};

    bool operator>(const TimerEntry &other) const {
      return expiryTime > other.expiryTime;
    }
  };

  // Implementation of QuicTimerCallback::TimerCallbackImpl for cancellation
  class TimerCallbackImplHandle
      : public quic::QuicTimerCallback::TimerCallbackImpl {
  public:
    explicit TimerCallbackImplHandle(std::shared_ptr<TimerEntry> entry)
        : entry_(std::move(entry)) {}

    void cancelImpl() noexcept override {
      if (auto entry = entry_.lock()) {
        entry->cancelled = true;
      }
    }

    bool isScheduledImpl() const noexcept override {
      if (auto entry = entry_.lock()) {
        return !entry->cancelled;
      }
      return false;
    }

    std::chrono::milliseconds getTimeRemainingImpl() const noexcept override {
      auto entry = entry_.lock();
      if (!entry || entry->cancelled) {
        return std::chrono::milliseconds(0);
      }

      uint64_t currentTime = picoquic_current_time();
      if (entry->expiryTime <= currentTime) {
        return std::chrono::milliseconds(0);
      }

      // picoquic times are in microseconds
      uint64_t remainingUs = entry->expiryTime - currentTime;
      return std::chrono::milliseconds(remainingUs / 1000);
    }

  private:
    std::weak_ptr<TimerEntry> entry_;
  };

  Impl()
      : timers_([](const std::shared_ptr<TimerEntry> &a,
                   const std::shared_ptr<TimerEntry> &b) {
          return a->expiryTime > b->expiryTime; // min-heap
        }) {}

  mutable std::mutex taskMutex_;
  std::queue<folly::Func> tasks_;

  mutable std::mutex timerMutex_;
  std::priority_queue<std::shared_ptr<TimerEntry>,
                      std::vector<std::shared_ptr<TimerEntry>>,
                      std::function<bool(const std::shared_ptr<TimerEntry> &,
                                         const std::shared_ptr<TimerEntry> &)>>
      timers_; // min-heap by expiry time

  static int loopCallbackStatic(picoquic_quic_t *quic,
                                picoquic_packet_loop_cb_enum cb_mode,
                                void *callback_ctx, void *callback_arg);

  int loopCallback(PicoQuicExecutor *executor, picoquic_quic_t *quic,
                   picoquic_packet_loop_cb_enum cb_mode, void *callback_arg);
};

PicoQuicExecutor::PicoQuicExecutor() : impl_(std::make_unique<Impl>()) {}

PicoQuicExecutor::~PicoQuicExecutor() = default;

void PicoQuicExecutor::add(folly::Func f) {
  std::lock_guard<std::mutex> lock(impl_->taskMutex_);
  impl_->tasks_.push(std::move(f));
  XLOG(DBG6) << "PicoQuicExecutor::add: task added, queue size="
             << impl_->tasks_.size();
}

void PicoQuicExecutor::scheduleTimeout(quic::QuicTimerCallback *callback,
                                       std::chrono::milliseconds timeout) {
  uint64_t currentTime = picoquic_current_time();
  uint64_t expiryTime =
      currentTime + static_cast<uint64_t>(timeout.count()) * 1000;

  auto entry = std::make_shared<Impl::TimerEntry>();
  entry->expiryTime = expiryTime;
  entry->callback = callback;
  entry->cancelled = false;

  auto *implHandle = new Impl::TimerCallbackImplHandle(entry);
  quic::QuicEventBase::setImplHandle(callback, implHandle);

  std::lock_guard<std::mutex> lock(impl_->timerMutex_);
  impl_->timers_.push(entry);
}

void *PicoQuicExecutor::getLoopCallback() {
  return reinterpret_cast<void *>(&Impl::loopCallbackStatic);
}

void PicoQuicExecutor::drainTasks() {
  std::queue<folly::Func> localTasks;
  {
    std::lock_guard<std::mutex> lock(impl_->taskMutex_);
    std::swap(localTasks, impl_->tasks_);
  }

  size_t taskCount = localTasks.size();
  if (taskCount > 0) {
    XLOG(DBG4) << "PicoQuicExecutor::drainTasks: draining " << taskCount
               << " tasks";
  }

  while (!localTasks.empty()) {
    auto task = std::move(localTasks.front());
    localTasks.pop();
    try {
      task();
    } catch (const std::exception &ex) {
      XLOG(ERR) << "Exception in executor task: " << ex.what();
    }
  }

  if (taskCount > 0) {
    XLOG(DBG4) << "PicoQuicExecutor::drainTasks: completed " << taskCount
               << " tasks";
  }
}

void PicoQuicExecutor::processExpiredTimers(uint64_t currentTime) {
  std::vector<quic::QuicTimerCallback *> expiredCallbacks;

  {
    std::lock_guard<std::mutex> lock(impl_->timerMutex_);
    while (!impl_->timers_.empty() &&
           impl_->timers_.top()->expiryTime <= currentTime) {
      auto entry = impl_->timers_.top();
      impl_->timers_.pop();

      if (!entry->cancelled) {
        expiredCallbacks.push_back(entry->callback);
      }
    }
  }

  for (auto *callback : expiredCallbacks) {
    try {
      callback->timeoutExpired();
    } catch (const std::exception &ex) {
      XLOG(ERR) << "Exception in timer callback: " << ex.what();
    }
  }
}

bool PicoQuicExecutor::hasPendingWork() const {
  std::lock_guard<std::mutex> lock(impl_->taskMutex_);
  return !impl_->tasks_.empty();
}

int64_t PicoQuicExecutor::getNextTimeoutDelta(uint64_t currentTime) const {
  std::lock_guard<std::mutex> lock(impl_->timerMutex_);

  if (impl_->timers_.empty()) {
    return INT64_MAX;
  }

  uint64_t nextExpiry = impl_->timers_.top()->expiryTime;
  if (nextExpiry <= currentTime) {
    return 0;
  }

  return static_cast<int64_t>(nextExpiry - currentTime);
}

int PicoQuicExecutor::Impl::loopCallbackStatic(
    picoquic_quic_t *quic, picoquic_packet_loop_cb_enum cb_mode,
    void *callback_ctx, void *callback_arg) {
  auto *executor = static_cast<PicoQuicExecutor *>(callback_ctx);
  if (!executor) {
    return 0;
  }
  return executor->impl_->loopCallback(executor, quic, cb_mode, callback_arg);
}

int PicoQuicExecutor::Impl::loopCallback(
    PicoQuicExecutor *executor, picoquic_quic_t * /*quic*/,
    picoquic_packet_loop_cb_enum cb_mode, void *callback_arg) {

  switch (cb_mode) {
  case picoquic_packet_loop_ready: {
    auto *options = static_cast<picoquic_packet_loop_options_t *>(callback_arg);
    if (options) {
      options->do_time_check = 1;
    }
    break;
  }

  case picoquic_packet_loop_time_check: {
    auto *timeArg = static_cast<packet_loop_time_check_arg_t *>(callback_arg);
    if (!timeArg) {
      break;
    }

    bool hasPending = executor->hasPendingWork();
    if (hasPending) {
      XLOG(DBG6) << "PicoQuicExecutor: hasPendingWork=true, "
                 << "setting delta_t=0";
      timeArg->delta_t = 0;
    } else {
      int64_t timerDelta = executor->getNextTimeoutDelta(timeArg->current_time);
      timeArg->delta_t = std::min(timeArg->delta_t, timerDelta);
      constexpr int64_t kMaxLoopSleepUs = 200000; // 200ms in microseconds
      timeArg->delta_t = std::min(timeArg->delta_t, kMaxLoopSleepUs);
    }
    break;
  }

  case picoquic_packet_loop_after_receive: {
    XLOG(DBG6) << "PicoQuicExecutor: after_receive";
    uint64_t currentTime = picoquic_current_time();
    executor->drainTasks();
    executor->processExpiredTimers(currentTime);
    break;
  }

  case picoquic_packet_loop_after_send: {
    XLOG(DBG6) << "PicoQuicExecutor: after_send";
    uint64_t currentTime = picoquic_current_time();
    executor->drainTasks();
    executor->processExpiredTimers(currentTime);
    break;
  }

  default:
    break;
  }

  return 0;
}

} // namespace moxygen

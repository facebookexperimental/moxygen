/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <moxygen/events/MoQDeliveryTimer.h>

#include <folly/io/async/EventBase.h>
#include <folly/portability/GMock.h>
#include <folly/portability/GTest.h>
#include <moxygen/events/MoQFollyExecutorImpl.h>

using namespace moxygen;
using namespace testing;

namespace {

class MoQDeliveryTimerTest : public testing::Test {
 protected:
  void SetUp() override {
    evb_ = std::make_unique<folly::EventBase>();
    executor_ = std::make_unique<MoQFollyExecutorImpl>(evb_.get());
    deliveryTimeout_ = std::chrono::milliseconds(100);
  }

  void TearDown() override {
    evb_.reset();
  }

  std::unique_ptr<folly::EventBase> evb_;
  std::unique_ptr<MoQFollyExecutorImpl> executor_;
  std::chrono::milliseconds deliveryTimeout_{};
};

TEST_F(MoQDeliveryTimerTest, BasicTimerExpiry) {
  bool callbackInvoked = false;
  ResetStreamErrorCode errorCode = ResetStreamErrorCode::INTERNAL_ERROR;

  auto timer = std::make_unique<MoQDeliveryTimer>(
      executor_->keepAlive(),
      deliveryTimeout_,
      [&callbackInvoked, &errorCode](ResetStreamErrorCode code) {
        callbackInvoked = true;
        errorCode = code;
      });

  uint64_t objectId = 1;
  timer->startTimer(objectId, std::chrono::microseconds(0));

  EXPECT_FALSE(callbackInvoked);

  evb_->scheduleAt(
      [&evb = evb_]() { evb->terminateLoopSoon(); },
      std::chrono::steady_clock::now() + deliveryTimeout_ * 2 +
          std::chrono::milliseconds(50));
  evb_->loopForever();

  EXPECT_TRUE(callbackInvoked);
  EXPECT_EQ(errorCode, ResetStreamErrorCode::DELIVERY_TIMEOUT);
}

TEST_F(MoQDeliveryTimerTest, CancelActiveTimer) {
  bool callbackInvoked = false;

  auto timer = std::make_unique<MoQDeliveryTimer>(
      executor_->keepAlive(),
      deliveryTimeout_,
      [&callbackInvoked](ResetStreamErrorCode) { callbackInvoked = true; });

  uint64_t objectId = 1;
  timer->startTimer(objectId, std::chrono::microseconds(0));

  evb_->loopOnce();

  timer->cancelTimer(objectId);

  evb_->scheduleAt(
      [&evb = evb_]() { evb->terminateLoopSoon(); },
      std::chrono::steady_clock::now() + deliveryTimeout_ * 2 +
          std::chrono::milliseconds(50));
  evb_->loopForever();

  EXPECT_FALSE(callbackInvoked);
}

TEST_F(MoQDeliveryTimerTest, CancelAllTimers) {
  int callbackCount = 0;

  auto timer = std::make_unique<MoQDeliveryTimer>(
      executor_->keepAlive(),
      deliveryTimeout_,
      [&callbackCount](ResetStreamErrorCode) { callbackCount++; });

  timer->startTimer(1, std::chrono::microseconds(0));
  timer->startTimer(2, std::chrono::microseconds(0));
  timer->startTimer(3, std::chrono::microseconds(0));

  evb_->loopOnce();

  timer->cancelAllTimers();

  evb_->scheduleAt(
      [&evb = evb_]() { evb->terminateLoopSoon(); },
      std::chrono::steady_clock::now() + deliveryTimeout_ * 2 +
          std::chrono::milliseconds(50));
  evb_->loopForever();

  EXPECT_EQ(callbackCount, 0);
}

TEST_F(MoQDeliveryTimerTest, DuplicateTimerStart) {
  bool callbackInvoked = false;
  ResetStreamErrorCode errorCode = ResetStreamErrorCode::CANCELLED;

  auto timer = std::make_unique<MoQDeliveryTimer>(
      executor_->keepAlive(),
      deliveryTimeout_,
      [&callbackInvoked, &errorCode](ResetStreamErrorCode code) {
        callbackInvoked = true;
        errorCode = code;
      });

  uint64_t objectId = 1;
  timer->startTimer(objectId, std::chrono::microseconds(0));

  evb_->loopOnce();

  timer->startTimer(objectId, std::chrono::microseconds(0));

  EXPECT_TRUE(callbackInvoked);
  EXPECT_EQ(errorCode, ResetStreamErrorCode::INTERNAL_ERROR);
}

TEST_F(MoQDeliveryTimerTest, CancelNonExistentTimer) {
  bool callbackInvoked = false;
  ResetStreamErrorCode errorCode = ResetStreamErrorCode::CANCELLED;

  auto timer = std::make_unique<MoQDeliveryTimer>(
      executor_->keepAlive(),
      deliveryTimeout_,
      [&callbackInvoked, &errorCode](ResetStreamErrorCode code) {
        callbackInvoked = true;
        errorCode = code;
      });

  uint64_t objectId = 999;
  timer->cancelTimer(objectId);

  EXPECT_FALSE(callbackInvoked);
}

} // namespace

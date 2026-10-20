/*
 * Copyright (c) OpenMOQ contributors.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/UniStreamQueue.h"

#include <folly/executors/ManualExecutor.h>
#include <folly/logging/xlog.h>
#include <folly/portability/GTest.h>
#include <proxygen/lib/http/webtransport/test/FakeSharedWebTransport.h>

using namespace moxygen;
using proxygen::WebTransport;
using proxygen::test::FakeSharedWebTransport;

namespace {

struct NoopHandler : public proxygen::WebTransportHandler {
  void onNewUniStream(WebTransport::StreamReadHandle*) noexcept override {}
  void onNewBidiStream(WebTransport::BidiStreamHandle) noexcept override {}
  void onDatagram(std::unique_ptr<folly::IOBuf>) noexcept override {}
  void onSessionEnd(folly::Optional<uint32_t>) noexcept override {}
  void onSessionDrain() noexcept override {}
};

quic::HTTPPriorityQueue::Priority pri(uint8_t urgency, uint32_t order = 0) {
  return quic::HTTPPriorityQueue::Priority(urgency, false, order);
}

class UniStreamQueueTest : public testing::Test {
 protected:
  void SetUp() override {
    std::tie(wt_, peerWt_) = FakeSharedWebTransport::makeSharedWebTransport();
    wt_->setPeerHandler(&peerHandler_);
    wt_->setMaxLocalUniStreams(0);
  }

  void TearDown() override {
    source_.requestCancellation();
  }

  void makeQueue(UniStreamQueue::AwaitCredit awaitCredit) {
    queue_ = std::make_unique<UniStreamQueue>(
        wt_.get(), &exec_, std::move(awaitCredit), source_.getToken());
  }

  void makeQueue() {
    makeQueue([this] { return wt_->awaitUniStreamCredit(); });
  }

  // Records which request was granted, and whether it got a stream.
  UniStreamQueue::Grant record(int tag) {
    return [this, tag](WebTransport::StreamWriteHandle* writeHandle) {
      granted_.emplace_back(tag, writeHandle != nullptr);
    };
  }

  folly::ManualExecutor exec_;
  // Cancelled before any queue is destroyed, as the owner must.
  folly::CancellationSource source_;
  NoopHandler peerHandler_;
  std::unique_ptr<FakeSharedWebTransport> wt_;
  std::unique_ptr<FakeSharedWebTransport> peerWt_;
  std::unique_ptr<UniStreamQueue> queue_;
  std::vector<std::pair<int, bool>> granted_;
};

TEST_F(UniStreamQueueTest, GrantsFromTheExecutor) {
  makeQueue();
  wt_->setMaxLocalUniStreams(1);
  ASSERT_TRUE(queue_->request(pri(3), record(1)).has_value());
  EXPECT_TRUE(granted_.empty());
  exec_.drain();
  EXPECT_EQ(granted_, (std::vector<std::pair<int, bool>>{{1, true}}));
  EXPECT_EQ(queue_->size(), 0);
}

TEST_F(UniStreamQueueTest, GrantsByPriorityThenArrival) {
  makeQueue();
  (void)queue_->request(pri(5), record(1));
  (void)queue_->request(pri(2, 7), record(2));
  (void)queue_->request(pri(2, 3), record(3));
  (void)queue_->request(pri(2, 3), record(4));
  exec_.drain();
  EXPECT_TRUE(granted_.empty());

  wt_->setMaxLocalUniStreams(4);
  exec_.drain();
  EXPECT_EQ(
      granted_,
      (std::vector<std::pair<int, bool>>{
          {3, true}, {4, true}, {2, true}, {1, true}}));
}

// A request made from inside a grant joins the same pass. When credit runs out
// it keeps waiting, and no second wait fails it.
TEST_F(UniStreamQueueTest, RequestFromInsideGrantKeepsWaiting) {
  makeQueue();
  (void)queue_->request(
      pri(3), [this](WebTransport::StreamWriteHandle* writeHandle) {
        granted_.emplace_back(1, writeHandle != nullptr);
        (void)queue_->request(pri(3), record(2));
      });
  (void)queue_->request(pri(3), record(3));
  wt_->setMaxLocalUniStreams(2);
  exec_.drain();
  EXPECT_EQ(
      granted_, (std::vector<std::pair<int, bool>>{{1, true}, {3, true}}));
  EXPECT_EQ(queue_->size(), 1);
}

TEST_F(UniStreamQueueTest, WaitsAgainWhenCreditRunsOut) {
  makeQueue();
  (void)queue_->request(pri(3), record(1));
  (void)queue_->request(pri(3), record(2));
  wt_->setMaxLocalUniStreams(1);
  exec_.drain();
  ASSERT_EQ(granted_, (std::vector<std::pair<int, bool>>{{1, true}}));
  EXPECT_EQ(wt_->pendingUniCreditWaiters(), 1);

  wt_->releaseUniCredit(*wt_->openLocalUniStreams().begin());
  exec_.drain();
  EXPECT_EQ(
      granted_, (std::vector<std::pair<int, bool>>{{1, true}, {2, true}}));
}

TEST_F(UniStreamQueueTest, CancelDropsTheGrant) {
  makeQueue();
  auto first = queue_->request(pri(3), record(1));
  (void)queue_->request(pri(3), record(2));
  ASSERT_TRUE(first.has_value());
  queue_->cancel(*first);
  queue_->cancel(*first);
  EXPECT_EQ(queue_->size(), 1);

  wt_->setMaxLocalUniStreams(2);
  exec_.drain();
  EXPECT_EQ(granted_, (std::vector<std::pair<int, bool>>{{2, true}}));
  EXPECT_EQ(wt_->openLocalUniStreams().size(), 1);
}

TEST_F(UniStreamQueueTest, CloseFailsPendingAndLaterRequests) {
  makeQueue();
  (void)queue_->request(pri(5), record(1));
  (void)queue_->request(pri(2), record(2));
  queue_->close();
  EXPECT_EQ(
      granted_, (std::vector<std::pair<int, bool>>{{2, false}, {1, false}}));
  EXPECT_FALSE(queue_->request(pri(3), record(3)).has_value());

  wt_->setMaxLocalUniStreams(1);
  exec_.drain();
  EXPECT_EQ(granted_.size(), 2);
  EXPECT_TRUE(wt_->openLocalUniStreams().empty());
}

TEST_F(UniStreamQueueTest, FailedCreditWaitFailsRequests) {
  makeQueue([] {
    return folly::makeSemiFuture<folly::Unit>(std::runtime_error("closed"));
  });
  (void)queue_->request(pri(3), record(1));
  (void)queue_->request(pri(3), record(2));
  exec_.drain();
  EXPECT_EQ(
      granted_, (std::vector<std::pair<int, bool>>{{1, false}, {2, false}}));
}

// Ready credit with failing creation fails the requests instead of retrying
// forever.
TEST_F(UniStreamQueueTest, ReadyCreditWithoutStreamsDoesNotSpin) {
  makeQueue([] { return folly::makeSemiFuture(); });
  (void)queue_->request(pri(3), record(1));
  (void)queue_->request(pri(3), record(2));
  exec_.drain();
  EXPECT_EQ(
      granted_, (std::vector<std::pair<int, bool>>{{1, false}, {2, false}}));
}

TEST_F(UniStreamQueueTest, DestroyedWhileWaiting) {
  makeQueue();
  (void)queue_->request(pri(3), record(1));
  exec_.drain();
  source_.requestCancellation();
  queue_.reset();
  wt_->setMaxLocalUniStreams(1);
  exec_.drain();
  EXPECT_TRUE(granted_.empty());
  EXPECT_TRUE(wt_->openLocalUniStreams().empty());
}

TEST_F(UniStreamQueueTest, GrantCanDestroyTheQueue) {
  makeQueue();
  (void)queue_->request(
      pri(2), [this](WebTransport::StreamWriteHandle* writeHandle) {
        granted_.emplace_back(1, writeHandle != nullptr);
        source_.requestCancellation();
        queue_.reset();
      });
  (void)queue_->request(pri(3), record(2));
  wt_->setMaxLocalUniStreams(2);
  exec_.drain();
  EXPECT_EQ(granted_, (std::vector<std::pair<int, bool>>{{1, true}}));
  EXPECT_EQ(wt_->openLocalUniStreams().size(), 1);
}

} // namespace

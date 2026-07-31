/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <folly/coro/BlockingWait.h>
#include <folly/portability/GMock.h>
#include <folly/portability/GTest.h>
#include <moxygen/MoQTrackProperties.h>
#include <moxygen/relay/MoQForwarder.h>
#include <moxygen/test/MockMoQSession.h>
#include <moxygen/test/Mocks.h>
#include <moxygen/test/TestUtils.h>

using namespace testing;
using namespace moxygen;

namespace moxygen::test {

namespace {
struct DummyExecutor : public folly::Executor {
  void add(folly::Func) override {}
};

struct ForwardChangedTracker : public MoQForwarder::Callback {
  void onEmpty(MoQForwarder*) override {
    emptyCalls++;
  }
  void forwardChanged(MoQForwarder*) override {
    forwardChangedCalls++;
  }
  int emptyCalls{0};
  int forwardChangedCalls{0};
};
} // namespace

const TrackNamespace kOpenFwdTestNamespace{{"test", "namespace"}};
const FullTrackName kOpenFwdTestTrackName{kOpenFwdTestNamespace, "track1"};

class OpenMOQForwarderTest : public ::testing::Test {
 protected:
  std::shared_ptr<MockMoQSession> createMockSession() {
    auto session = std::make_shared<NiceMock<MockMoQSession>>();
    ON_CALL(*session, getNegotiatedVersion())
        .WillByDefault(Return(std::optional<uint64_t>(kVersionDraftCurrent)));
    return session;
  }

  std::shared_ptr<MockTrackConsumer> createMockConsumer() {
    auto consumer = std::make_shared<NiceMock<MockTrackConsumer>>();
    ON_CALL(*consumer, setTrackAlias(_))
        .WillByDefault(
            Return(folly::makeExpected<MoQPublishError>(folly::unit)));
    ON_CALL(*consumer, publishDone(_))
        .WillByDefault(
            Return(folly::makeExpected<MoQPublishError>(folly::unit)));
    return consumer;
  }

  std::shared_ptr<MockSubgroupConsumer> createMockSubgroupConsumer() {
    auto sg = std::make_shared<NiceMock<MockSubgroupConsumer>>();
    ON_CALL(*sg, beginObject(_, _, _, _))
        .WillByDefault(
            Return(folly::makeExpected<MoQPublishError>(folly::unit)));
    ON_CALL(*sg, objectPayload(_, _))
        .WillByDefault(Return(
            folly::makeExpected<MoQPublishError>(
                ObjectPublishStatus::IN_PROGRESS)));
    ON_CALL(*sg, endOfSubgroup())
        .WillByDefault(
            Return(folly::makeExpected<MoQPublishError>(folly::unit)));
    ON_CALL(*sg, endOfGroup(_))
        .WillByDefault(
            Return(folly::makeExpected<MoQPublishError>(folly::unit)));
    ON_CALL(*sg, endOfTrackAndGroup(_))
        .WillByDefault(
            Return(folly::makeExpected<MoQPublishError>(folly::unit)));
    return sg;
  }

  std::shared_ptr<MoQForwarder::Subscriber> addSubscriber(
      MoQForwarder& forwarder,
      std::shared_ptr<MoQSession> session,
      std::shared_ptr<TrackConsumer> consumer,
      RequestID requestID = RequestID(0),
      LocationType locType = LocationType::LargestObject) {
    SubscribeRequest sub;
    sub.fullTrackName = forwarder.fullTrackName();
    sub.requestID = requestID;
    sub.locType = locType;
    return forwarder.addSubscriber(
        std::move(session), sub, std::move(consumer));
  }
};

TEST_F(OpenMOQForwarderTest, SubscriberIsPinnedReflectsPinnedField) {
  auto forwarder = std::make_shared<MoQForwarder>(kOpenFwdTestTrackName);
  auto session = createMockSession();
  auto consumer = createMockConsumer();
  auto subscriber = addSubscriber(*forwarder, session, consumer);
  ASSERT_NE(subscriber, nullptr);

  EXPECT_FALSE(subscriber->isPinned());
  subscriber->pinned = true;
  EXPECT_TRUE(subscriber->isPinned());
}

TEST_F(OpenMOQForwarderTest, PassiveSubscriberDoesNotTriggerForwardChanged) {
  auto forwarder = std::make_shared<MoQForwarder>(kOpenFwdTestTrackName);
  auto cb = std::make_shared<ForwardChangedTracker>();
  forwarder->setCallback(cb);

  auto session = createMockSession();
  auto consumer = createMockConsumer();

  auto handle = forwarder->addSubscriber(
      session, /*forward=*/true, consumer, /*passive=*/true);
  ASSERT_NE(handle, nullptr);
  EXPECT_EQ(cb->forwardChangedCalls, 0);
  EXPECT_EQ(forwarder->numForwardingSubscribers(), 0u);

  forwarder->removeSubscriber(session, std::nullopt, "test");
  EXPECT_EQ(cb->forwardChangedCalls, 0);
}

TEST_F(OpenMOQForwarderTest, OnEmptyFiresWhenOnlyPassiveRemain) {
  auto forwarder = std::make_shared<MoQForwarder>(kOpenFwdTestTrackName);
  auto cb = std::make_shared<ForwardChangedTracker>();
  forwarder->setCallback(cb);

  auto realSession = createMockSession();
  auto passiveSession = createMockSession();
  auto realConsumer = createMockConsumer();
  auto passiveConsumer = createMockConsumer();

  forwarder->addSubscriber(realSession, /*forward=*/true, realConsumer);
  forwarder->addSubscriber(
      passiveSession, /*forward=*/true, passiveConsumer, /*passive=*/true);

  forwarder->removeSubscriber(realSession, std::nullopt, "test");
  EXPECT_EQ(cb->emptyCalls, 1);
  EXPECT_EQ(forwarder->subscriberCount(), 1u);
}

TEST_F(
    OpenMOQForwarderTest,
    ForwardChangedFiresOnlyOnRealSubscriberTransitions) {
  auto forwarder = std::make_shared<MoQForwarder>(kOpenFwdTestTrackName);
  auto cb = std::make_shared<ForwardChangedTracker>();
  forwarder->setCallback(cb);

  auto realSession1 = createMockSession();
  auto realSession2 = createMockSession();
  auto passiveSession = createMockSession();
  auto consumer = createMockConsumer();

  forwarder->addSubscriber(realSession1, /*forward=*/true, consumer);
  EXPECT_EQ(cb->forwardChangedCalls, 1);

  forwarder->addSubscriber(
      passiveSession, /*forward=*/true, consumer, /*passive=*/true);
  EXPECT_EQ(cb->forwardChangedCalls, 1);

  forwarder->addSubscriber(realSession2, /*forward=*/true, consumer);
  EXPECT_EQ(cb->forwardChangedCalls, 1);
  EXPECT_EQ(forwarder->numForwardingSubscribers(), 2u);

  forwarder->removeSubscriber(realSession1, std::nullopt, "test");
  EXPECT_EQ(cb->forwardChangedCalls, 1);
  EXPECT_EQ(forwarder->numForwardingSubscribers(), 1u);

  forwarder->removeSubscriber(realSession2, std::nullopt, "test");
  EXPECT_EQ(cb->forwardChangedCalls, 1);
  EXPECT_EQ(cb->emptyCalls, 1);
}

TEST_F(OpenMOQForwarderTest, ChannelSubscriberReceivesSubgroupObjects) {
  auto forwarder = std::make_shared<MoQForwarder>(kOpenFwdTestTrackName);
  auto consumer = createMockConsumer();
  DummyExecutor exec;

  auto handle =
      forwarder->addChannelSubscriber(&exec, /*forward=*/true, consumer);
  ASSERT_NE(handle, nullptr);
  EXPECT_EQ(handle->session, nullptr);
  EXPECT_EQ(forwarder->numForwardingSubscribers(), 1u);

  std::shared_ptr<MockSubgroupConsumer> sg;
  EXPECT_CALL(*consumer, beginSubgroup(0, 0, _, _))
      .WillOnce([this, &sg](uint64_t, uint64_t, uint8_t, BeginSubgroupOptions) {
        sg = createMockSubgroupConsumer();
        EXPECT_CALL(*sg, object(0, _, _, false)).WillOnce(Return(folly::unit));
        EXPECT_CALL(*sg, endOfSubgroup()).WillOnce(Return(folly::unit));
        return folly::
            makeExpected<MoQPublishError, std::shared_ptr<SubgroupConsumer>>(
                sg);
      });

  auto sgRes = forwarder->beginSubgroup(0, 0, 0);
  ASSERT_TRUE(sgRes.hasValue());
  auto pubSg = *sgRes;
  EXPECT_TRUE(pubSg->object(0, test::makeBuf(4)).hasValue());
  EXPECT_TRUE(pubSg->endOfSubgroup().hasValue());
}

TEST_F(OpenMOQForwarderTest, RemoveChannelSubscriberByHandle) {
  auto forwarder = std::make_shared<MoQForwarder>(kOpenFwdTestTrackName);
  auto consumer = createMockConsumer();
  DummyExecutor exec;

  auto handle =
      forwarder->addChannelSubscriber(&exec, /*forward=*/true, consumer);
  ASSERT_NE(handle, nullptr);
  EXPECT_EQ(forwarder->subscriberCount(), 1u);
  EXPECT_EQ(forwarder->numForwardingSubscribers(), 1u);

  EXPECT_CALL(*consumer, publishDone(_))
      .WillOnce(Return(folly::makeExpected<MoQPublishError>(folly::unit)));

  forwarder->removeChannelSubscriber(
      handle,
      PublishDone{
          RequestID(0), PublishDoneStatusCode::SUBSCRIPTION_ENDED, 0, ""});

  EXPECT_TRUE(forwarder->empty());
  EXPECT_EQ(forwarder->numForwardingSubscribers(), 0u);
}

TEST_F(OpenMOQForwarderTest, RemoveChannelSubscriberByExec) {
  auto forwarder = std::make_shared<MoQForwarder>(kOpenFwdTestTrackName);
  auto consumer = createMockConsumer();
  DummyExecutor exec;

  auto handle =
      forwarder->addChannelSubscriber(&exec, /*forward=*/true, consumer);
  ASSERT_NE(handle, nullptr);
  EXPECT_EQ(forwarder->subscriberCount(), 1u);

  forwarder->removeChannelSubscriberByExec(&exec, std::nullopt);
  EXPECT_TRUE(forwarder->empty());
}

TEST_F(OpenMOQForwarderTest, PublishDoneFansOutToMixedSubscribers) {
  auto forwarder = std::make_shared<MoQForwarder>(kOpenFwdTestTrackName);
  auto sessionConsumer = createMockConsumer();
  auto channelConsumer = createMockConsumer();
  auto session = createMockSession();
  DummyExecutor exec;

  addSubscriber(*forwarder, session, sessionConsumer, RequestID(1));
  forwarder->addChannelSubscriber(&exec, /*forward=*/true, channelConsumer);
  EXPECT_EQ(forwarder->subscriberCount(), 2u);

  EXPECT_CALL(*sessionConsumer, publishDone(_))
      .WillOnce(Return(folly::makeExpected<MoQPublishError>(folly::unit)));
  EXPECT_CALL(*channelConsumer, publishDone(_))
      .WillOnce(Return(folly::makeExpected<MoQPublishError>(folly::unit)));

  forwarder->publishDone(
      PublishDone{
          RequestID(0), PublishDoneStatusCode::SUBSCRIPTION_ENDED, 0, "done"});

  EXPECT_TRUE(forwarder->empty());
}

TEST_F(OpenMOQForwarderTest, DuplicateChannelSubscriberSameExecIsNoOp) {
  auto forwarder = std::make_shared<MoQForwarder>(kOpenFwdTestTrackName);
  DummyExecutor exec;
  auto consumer1 = createMockConsumer();
  auto consumer2 = createMockConsumer();

  auto handle1 =
      forwarder->addChannelSubscriber(&exec, /*forward=*/true, consumer1);
  ASSERT_NE(handle1, nullptr);
  EXPECT_EQ(forwarder->subscriberCount(), 1u);
  EXPECT_EQ(forwarder->numForwardingSubscribers(), 1u);

  auto handle2 =
      forwarder->addChannelSubscriber(&exec, /*forward=*/true, consumer2);
  ASSERT_NE(handle2, nullptr);
  EXPECT_EQ(handle1.get(), handle2.get());
  EXPECT_EQ(forwarder->subscriberCount(), 1u);
  EXPECT_EQ(forwarder->numForwardingSubscribers(), 1u);
}

// Test: Once all real subscribers have tombstoned a subgroup, duplicate
// beginSubgroup returns CANCELLED - a passive observer's reset doesn't mask it.
TEST_F(OpenMOQForwarderTest, PassiveResetDoesNotMaskDuplicateSubgroupCancel) {
  auto forwarder = std::make_shared<MoQForwarder>(kOpenFwdTestTrackName);
  auto realSession = createMockSession();
  auto passiveSession = createMockSession();
  auto realConsumer = createMockConsumer();
  auto passiveConsumer = createMockConsumer();

  std::shared_ptr<MockSubgroupConsumer> realSg;
  EXPECT_CALL(*realConsumer, beginSubgroup(0, 0, _, _))
      .WillOnce(
          [this, &realSg](uint64_t, uint64_t, uint8_t, BeginSubgroupOptions) {
            realSg = createMockSubgroupConsumer();
            // Simulate stop_sending: soft error tombstones the subgroup.
            EXPECT_CALL(*realSg, object(0, _, _, false))
                .WillOnce(Return(
                    folly::makeUnexpected(MoQPublishError(
                        MoQPublishError::CANCELLED, "STOP_SENDING"))));
            return folly::makeExpected<
                MoQPublishError,
                std::shared_ptr<SubgroupConsumer>>(realSg);
          });

  std::shared_ptr<MockSubgroupConsumer> passiveSg;
  EXPECT_CALL(*passiveConsumer, beginSubgroup(0, 0, _, _))
      .WillOnce([this, &passiveSg](
                    uint64_t, uint64_t, uint8_t, BeginSubgroupOptions) {
        passiveSg = createMockSubgroupConsumer();
        EXPECT_CALL(*passiveSg, object(0, _, _, false))
            .WillOnce(Return(folly::unit));
        // The duplicate resets the passive observer's stale consumer...
        EXPECT_CALL(*passiveSg, reset(ResetStreamErrorCode::CANCELLED));
        return folly::
            makeExpected<MoQPublishError, std::shared_ptr<SubgroupConsumer>>(
                passiveSg);
      });

  auto realHandle =
      addSubscriber(*forwarder, realSession, realConsumer, RequestID(1));
  ASSERT_NE(realHandle, nullptr);
  auto passiveHandle = forwarder->addSubscriber(
      passiveSession, /*forward=*/true, passiveConsumer, /*passive=*/true);
  ASSERT_NE(passiveHandle, nullptr);

  auto pubSg = forwarder->beginSubgroup(0, 0, 0).value();
  EXPECT_TRUE(pubSg->object(0, test::makeBuf(4)).hasValue());

  // ...but must not count toward anyReset: CANCELLED propagates.
  auto dupRes = forwarder->beginSubgroup(0, 0, 0);
  ASSERT_TRUE(dupRes.hasError());
  EXPECT_EQ(dupRes.error().code, MoQPublishError::CANCELLED);
}

// Test: A passive subscriber with no subgroup state is not a reopen candidate.
TEST_F(OpenMOQForwarderTest, PassiveDoesNotCountAsReopenCandidate) {
  auto forwarder = std::make_shared<MoQForwarder>(kOpenFwdTestTrackName);
  auto realSession = createMockSession();
  auto passiveSession = createMockSession();
  auto realConsumer = createMockConsumer();
  auto passiveConsumer = createMockConsumer();

  std::shared_ptr<MockSubgroupConsumer> realSg;
  EXPECT_CALL(*realConsumer, beginSubgroup(0, 0, _, _))
      .WillOnce(
          [this, &realSg](uint64_t, uint64_t, uint8_t, BeginSubgroupOptions) {
            realSg = createMockSubgroupConsumer();
            EXPECT_CALL(*realSg, object(0, _, _, false))
                .WillOnce(Return(
                    folly::makeUnexpected(MoQPublishError(
                        MoQPublishError::CANCELLED, "STOP_SENDING"))));
            return folly::makeExpected<
                MoQPublishError,
                std::shared_ptr<SubgroupConsumer>>(realSg);
          });
  // The passive observer joins after the subgroup's objects were delivered
  // (object delivery lazily opens subgroups for earlier joiners), so it has
  // no subgroup entry; it must not be offered the duplicate subgroup.
  EXPECT_CALL(*passiveConsumer, beginSubgroup(_, _, _, _)).Times(0);

  auto realHandle =
      addSubscriber(*forwarder, realSession, realConsumer, RequestID(1));
  ASSERT_NE(realHandle, nullptr);

  auto pubSg = forwarder->beginSubgroup(0, 0, 0).value();
  EXPECT_TRUE(pubSg->object(0, test::makeBuf(4)).hasValue());

  auto passiveHandle = forwarder->addSubscriber(
      passiveSession, /*forward=*/true, passiveConsumer, /*passive=*/true);
  ASSERT_NE(passiveHandle, nullptr);

  auto dupRes = forwarder->beginSubgroup(0, 0, 0);
  ASSERT_TRUE(dupRes.hasError());
  EXPECT_EQ(dupRes.error().code, MoQPublishError::CANCELLED);
}

TEST_F(OpenMOQForwarderTest, ChannelSubscriberDrainsWhenSubgroupsOpen) {
  auto forwarder = std::make_shared<MoQForwarder>(kOpenFwdTestTrackName);
  auto consumer = createMockConsumer();
  DummyExecutor exec;

  std::shared_ptr<MockSubgroupConsumer> sg;
  EXPECT_CALL(*consumer, beginSubgroup(0, 0, _, _))
      .WillOnce([this, &sg](uint64_t, uint64_t, uint8_t, BeginSubgroupOptions) {
        sg = createMockSubgroupConsumer();
        return folly::
            makeExpected<MoQPublishError, std::shared_ptr<SubgroupConsumer>>(
                sg);
      });

  forwarder->addChannelSubscriber(&exec, /*forward=*/true, consumer);
  auto pubSg = forwarder->beginSubgroup(0, 0, 0).value();
  ASSERT_TRUE(pubSg->beginObject(0, 10, 0).hasValue());

  EXPECT_CALL(*consumer, publishDone(_))
      .WillOnce(Return(folly::makeExpected<MoQPublishError>(folly::unit)));
  EXPECT_CALL(*sg, reset(_)).Times(0);

  forwarder->publishDone(
      PublishDone{
          RequestID(0), PublishDoneStatusCode::SUBSCRIPTION_ENDED, 0, ""});

  EXPECT_EQ(forwarder->subscriberCount(), 1u);

  EXPECT_CALL(*sg, objectPayload(_, true));
  EXPECT_TRUE(pubSg->objectPayload(test::makeBuf(10), true));

  EXPECT_TRUE(forwarder->empty());
}

TEST_F(
    OpenMOQForwarderTest,
    ChannelSubscriberRequestUpdateInvalidRangeNoCrash) {
  auto forwarder = std::make_shared<MoQForwarder>(kOpenFwdTestTrackName);
  auto consumer = createMockConsumer();
  DummyExecutor exec;

  forwarder->setLargest({5, 0});
  auto handle =
      forwarder->addChannelSubscriber(&exec, /*forward=*/true, consumer);
  ASSERT_NE(handle, nullptr);

  RequestUpdate validUpdate;
  validUpdate.requestID = RequestID(1);
  validUpdate.endGroup = 10;
  auto res1 = folly::coro::blockingWait(handle->requestUpdate(validUpdate));
  EXPECT_TRUE(res1.hasValue());

  // start moves ahead of bounded end → invalid range; session is null so
  // the close call must not crash.
  RequestUpdate invalidUpdate;
  invalidUpdate.requestID = RequestID(2);
  invalidUpdate.start = AbsoluteLocation{20, 0};
  invalidUpdate.endGroup = 3;
  auto res2 = folly::coro::blockingWait(handle->requestUpdate(invalidUpdate));
  EXPECT_FALSE(res2.hasValue());
}

} // namespace moxygen::test

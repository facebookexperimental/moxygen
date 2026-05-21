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

const TrackNamespace kFwdTestNamespace{{"test", "namespace"}};
const FullTrackName kFwdTestTrackName{kFwdTestNamespace, "track1"};

class MoQForwarderTest : public ::testing::Test {
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

  // Add a subscriber to a forwarder using only forwarder APIs.
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

namespace {

// Simulates the production scenario where onEmpty destroys the forwarder.
class ForwarderDestroyingCallback : public MoQForwarder::Callback {
 public:
  explicit ForwarderDestroyingCallback(
      std::shared_ptr<MoQForwarder>& forwarderRef)
      : forwarderRef_(forwarderRef) {}

  void onEmpty(MoQForwarder*) override {
    forwarderRef_.reset();
  }

 private:
  std::shared_ptr<MoQForwarder>& forwarderRef_;
};

struct TestNGRCallback : public MoQForwarder::Callback {
  void onEmpty(MoQForwarder*) override {}
  void newGroupRequested(MoQForwarder*, uint64_t group) override {
    calls.push_back(group);
  }
  std::vector<uint64_t> calls;
};

auto makeNGRParams(uint64_t val) {
  RequestUpdate upd;
  upd.params.insertParam(Parameter(
      folly::to_underlying(TrackRequestParamKey::NEW_GROUP_REQUEST), val));
  return upd.params;
}

} // namespace

// Test: Forwarder only gives late-joining subscribers a subgroup at object
// boundaries, never mid-object.
TEST_F(MoQForwarderTest, ForwarderOnlyCreatesSubgroupsBeforeObjectData) {
  auto subscriber1 = createMockSession();
  auto subscriber2 = createMockSession();
  auto subscriber3 = createMockSession();

  auto mockConsumer1 = createMockConsumer();
  auto mockConsumer2 = createMockConsumer();
  auto mockConsumer3 = createMockConsumer();

  EXPECT_CALL(*mockConsumer1, beginSubgroup(_, _, _, _))
      .WillOnce([this](uint64_t, uint64_t, uint8_t, bool) {
        return folly::
            makeExpected<MoQPublishError, std::shared_ptr<SubgroupConsumer>>(
                createMockSubgroupConsumer());
      });

  EXPECT_CALL(*mockConsumer2, beginSubgroup(_, _, _, _))
      .WillOnce([this](uint64_t, uint64_t, uint8_t, bool) {
        return folly::
            makeExpected<MoQPublishError, std::shared_ptr<SubgroupConsumer>>(
                createMockSubgroupConsumer());
      });

  EXPECT_CALL(*mockConsumer3, beginSubgroup(_, _, _, _)).Times(0);

  auto forwarder = std::make_shared<MoQForwarder>(kFwdTestTrackName);

  auto sub1Handle =
      addSubscriber(*forwarder, subscriber1, mockConsumer1, RequestID(1));
  ASSERT_NE(sub1Handle, nullptr);

  auto sgRes = forwarder->beginSubgroup(0, 0, 0);
  EXPECT_TRUE(sgRes.hasValue());
  auto subgroup = *sgRes;

  EXPECT_TRUE(subgroup->beginObject(0, 4, 0).hasValue());
  EXPECT_TRUE(subgroup->objectPayload(folly::IOBuf::copyBuffer("test"), false)
                  .hasValue());

  auto sub2Handle =
      addSubscriber(*forwarder, subscriber2, mockConsumer2, RequestID(2));
  ASSERT_NE(sub2Handle, nullptr);

  EXPECT_TRUE(subgroup->beginObject(1, 9, 0).hasValue());

  auto sub3Handle =
      addSubscriber(*forwarder, subscriber3, mockConsumer3, RequestID(3));
  ASSERT_NE(sub3Handle, nullptr);

  EXPECT_TRUE(
      subgroup->objectPayload(folly::IOBuf::copyBuffer("more data"), false)
          .hasValue());
  EXPECT_TRUE(subgroup->endOfSubgroup().hasValue());
}

// Test: Graceful session draining - publishDone drains subscribers but does
// not reset open subgroups. Draining subscribers are removed when their last
// subgroup closes.
TEST_F(MoQForwarderTest, GracefulSessionDraining) {
  std::array<std::shared_ptr<MoQSession>, 3> subscribers;
  for (auto& s : subscribers) {
    s = createMockSession();
  }

  auto forwarder = std::make_shared<MoQForwarder>(kFwdTestTrackName);

  std::array<std::shared_ptr<MockSubgroupConsumer>, 2> sub0_sgs;
  std::shared_ptr<MockSubgroupConsumer> sub1_sg0;

  std::array<std::shared_ptr<MockTrackConsumer>, 3> consumers;
  for (auto& c : consumers) {
    c = createMockConsumer();
  }

  for (uint64_t i = 0; i < 2; ++i) {
    EXPECT_CALL(*consumers[0], beginSubgroup(i, 0, _, _))
        .WillOnce([this, i, &sub0_sgs](uint64_t, uint64_t, uint8_t, bool) {
          sub0_sgs[i] = createMockSubgroupConsumer();
          return folly::
              makeExpected<MoQPublishError, std::shared_ptr<SubgroupConsumer>>(
                  sub0_sgs[i]);
        });
  }

  EXPECT_CALL(*consumers[1], beginSubgroup(1, 0, _, _))
      .WillOnce([this, &sub1_sg0](uint64_t, uint64_t, uint8_t, bool) {
        sub1_sg0 = createMockSubgroupConsumer();
        return folly::
            makeExpected<MoQPublishError, std::shared_ptr<SubgroupConsumer>>(
                sub1_sg0);
      });

  std::array<std::shared_ptr<SubgroupConsumer>, 2> sgs;
  addSubscriber(*forwarder, subscribers[0], consumers[0], RequestID(1));
  sgs[0] = forwarder->beginSubgroup(0, 0, 0).value();
  addSubscriber(*forwarder, subscribers[1], consumers[1], RequestID(2));
  sgs[1] = forwarder->beginSubgroup(1, 0, 0).value();
  addSubscriber(*forwarder, subscribers[2], consumers[2], RequestID(3));

  for (auto& consumer : consumers) {
    EXPECT_CALL(*consumer, publishDone(_))
        .WillOnce(Return(folly::makeExpected<MoQPublishError>(folly::unit)));
  }

  EXPECT_CALL(*sub0_sgs[0], reset(_)).Times(0);
  EXPECT_CALL(*sub0_sgs[1], reset(_)).Times(0);
  EXPECT_CALL(*sub1_sg0, reset(_)).Times(0);

  forwarder->publishDone(
      PublishDone{
          RequestID(0),
          PublishDoneStatusCode::SUBSCRIPTION_ENDED,
          0,
          "publisher ended"});

  EXPECT_TRUE(sgs[0]->endOfSubgroup().hasValue());
  EXPECT_TRUE(sgs[1]->endOfSubgroup().hasValue());
}

// Test: removeSubscriber immediately resets all open subgroups.
TEST_F(MoQForwarderTest, RemoveSessionResetsOpenSubgroups) {
  auto subscriber = createMockSession();

  auto forwarder = std::make_shared<MoQForwarder>(kFwdTestTrackName);

  std::array<std::shared_ptr<MockSubgroupConsumer>, 2> sgs;
  auto consumer = createMockConsumer();

  for (uint64_t i = 0; i < sgs.size(); ++i) {
    EXPECT_CALL(*consumer, beginSubgroup(i, 0, _, _))
        .WillOnce([this, i, &sgs](uint64_t, uint64_t, uint8_t, bool) {
          sgs[i] = createMockSubgroupConsumer();
          EXPECT_CALL(*sgs[i], object(0, testing::_, testing::_, false))
              .WillOnce(testing::Return(folly::unit));
          return folly::
              makeExpected<MoQPublishError, std::shared_ptr<SubgroupConsumer>>(
                  sgs[i]);
        });
  }

  auto subHandle =
      addSubscriber(*forwarder, subscriber, consumer, RequestID(1));
  ASSERT_NE(subHandle, nullptr);

  std::array<std::shared_ptr<SubgroupConsumer>, 2> pubSgs;
  for (uint64_t i = 0; i < pubSgs.size(); ++i) {
    auto sg = forwarder->beginSubgroup(i, 0, 0);
    ASSERT_TRUE(sg.hasValue());
    EXPECT_TRUE((*sg)->object(0, test::makeBuf(10)).hasValue());
    pubSgs[i] = *sg;
  }

  for (auto& sg : sgs) {
    EXPECT_CALL(*sg, reset(ResetStreamErrorCode::CANCELLED));
  }

  subHandle->unsubscribe();

  for (uint64_t i = 0; i < pubSgs.size(); ++i) {
    EXPECT_TRUE(pubSgs[i]->endOfSubgroup().hasValue());
  }
}

// Test: Draining subscriber is removed when subgroup closes with error.
TEST_F(MoQForwarderTest, DrainingSubscriberRemovedOnSubgroupError) {
  auto subscriber = createMockSession();

  auto forwarder = std::make_shared<MoQForwarder>(kFwdTestTrackName);

  std::array<std::shared_ptr<MockSubgroupConsumer>, 3> sgs;
  auto consumer = createMockConsumer();

  for (uint64_t i = 0; i < sgs.size(); ++i) {
    EXPECT_CALL(*consumer, beginSubgroup(i, 0, _, _))
        .WillOnce([this, i, &sgs](uint64_t, uint64_t, uint8_t, bool) {
          sgs[i] = createMockSubgroupConsumer();
          return folly::
              makeExpected<MoQPublishError, std::shared_ptr<SubgroupConsumer>>(
                  sgs[i]);
        });
  }

  addSubscriber(*forwarder, subscriber, consumer, RequestID(1));

  std::array<std::shared_ptr<SubgroupConsumer>, 3> pubSgs;
  for (uint64_t i = 0; i < 3; ++i) {
    pubSgs[i] = forwarder->beginSubgroup(i, 0, 0).value();
    EXPECT_TRUE((pubSgs[i])->beginObject(0, 10, 0).hasValue());
  }

  EXPECT_CALL(*consumer, publishDone(_))
      .WillOnce(Return(folly::makeExpected<MoQPublishError>(folly::unit)));

  EXPECT_CALL(*sgs[0], reset(_)).Times(0);
  EXPECT_CALL(*sgs[1], reset(_)).Times(0);
  EXPECT_CALL(*sgs[2], reset(_)).Times(0);

  forwarder->publishDone(
      PublishDone{
          RequestID(0),
          PublishDoneStatusCode::SUBSCRIPTION_ENDED,
          0,
          "publisher ended"});

  EXPECT_CALL(*sgs[0], reset(ResetStreamErrorCode::INTERNAL_ERROR));
  pubSgs[0]->reset(ResetStreamErrorCode::INTERNAL_ERROR);

  EXPECT_CALL(*sgs[1], objectPayload(testing::_, true));
  EXPECT_TRUE(pubSgs[1]->objectPayload(test::makeBuf(10), true));

  EXPECT_CALL(*sgs[2], reset(ResetStreamErrorCode::INTERNAL_ERROR));
  pubSgs[2]->reset(ResetStreamErrorCode::INTERNAL_ERROR);
}

// Test: Subscriber that unsubscribes does not receive objects published after
// publishDone. A late subscriber joining after publishDone is rejected.
TEST_F(MoQForwarderTest, SubscriberUnsubscribeDoesNotReceiveNewObjects) {
  auto subscriber1 = createMockSession();
  auto subscriber2 = createMockSession();

  auto mockConsumer1 = createMockConsumer();
  auto mockConsumer2 = createMockConsumer();

  EXPECT_CALL(*mockConsumer1, beginSubgroup(_, _, _, _))
      .WillOnce([&](uint64_t, uint64_t, uint8_t, bool) {
        auto sg = std::make_shared<NiceMock<MockSubgroupConsumer>>();
        EXPECT_CALL(*sg, beginObject(_, _, _, _)).WillOnce(Return(folly::unit));
        EXPECT_CALL(*sg, reset(_));
        return folly::
            makeExpected<MoQPublishError, std::shared_ptr<SubgroupConsumer>>(
                sg);
      });

  EXPECT_CALL(*mockConsumer2, beginSubgroup(_, _, _, _)).Times(0);

  auto forwarder = std::make_shared<MoQForwarder>(kFwdTestTrackName);

  auto sub1Handle =
      addSubscriber(*forwarder, subscriber1, mockConsumer1, RequestID(1));
  ASSERT_NE(sub1Handle, nullptr);

  auto sgRes = forwarder->beginSubgroup(0, 0, 0);
  EXPECT_TRUE(sgRes.hasValue());
  auto subgroup = *sgRes;

  EXPECT_CALL(*mockConsumer1, publishDone(testing::_));
  EXPECT_TRUE(forwarder
                  ->publishDone(
                      {RequestID(1),
                       PublishDoneStatusCode::TRACK_ENDED,
                       0,
                       "track ended"})
                  .hasValue());

  // Forwarder is draining — new subscriber is rejected.
  auto sub2Handle =
      addSubscriber(*forwarder, subscriber2, mockConsumer2, RequestID(2));
  EXPECT_EQ(sub2Handle, nullptr);

  EXPECT_TRUE(subgroup->beginObject(0, 4, 0).hasValue());
  subgroup->reset(ResetStreamErrorCode::SESSION_CLOSED);
}

// Test: Data operations return CANCELLED when all subscribers fail and are
// removed.
TEST_F(MoQForwarderTest, DataOperationCancelledWhenAllSubscribersFail) {
  std::array<std::shared_ptr<MoQSession>, 2> subscribers;
  std::array<std::shared_ptr<MockTrackConsumer>, 2> consumers;
  std::array<std::shared_ptr<MockSubgroupConsumer>, 2> sgs;

  for (auto& s : subscribers) {
    s = createMockSession();
  }
  for (auto& c : consumers) {
    c = createMockConsumer();
  }

  for (size_t i = 0; i < 2; ++i) {
    EXPECT_CALL(*consumers[i], beginSubgroup(0, 0, _, _))
        .WillOnce([this, i, &sgs](uint64_t, uint64_t, uint8_t, bool) {
          sgs[i] = createMockSubgroupConsumer();
          EXPECT_CALL(*sgs[i], objectPayload(_, false))
              .WillOnce(Return(
                  folly::makeExpected<MoQPublishError>(
                      ObjectPublishStatus::IN_PROGRESS)))
              .WillOnce(Return(
                  folly::makeUnexpected(MoQPublishError(
                      MoQPublishError::WRITE_ERROR, "write failed"))));
          return folly::
              makeExpected<MoQPublishError, std::shared_ptr<SubgroupConsumer>>(
                  sgs[i]);
        });
  }

  auto forwarder = std::make_shared<MoQForwarder>(kFwdTestTrackName);
  for (size_t i = 0; i < 2; ++i) {
    auto h = addSubscriber(
        *forwarder, subscribers[i], consumers[i], RequestID(i + 1));
    ASSERT_NE(h, nullptr);
  }

  auto sgRes = forwarder->beginSubgroup(0, 0, 0);
  ASSERT_TRUE(sgRes.hasValue());
  auto subgroup = *sgRes;
  EXPECT_TRUE(subgroup->beginObject(0, 100, 0).hasValue());

  auto res1 = subgroup->objectPayload(folly::IOBuf::copyBuffer("data1"), false);
  EXPECT_TRUE(res1.hasValue());
  EXPECT_EQ(*res1, ObjectPublishStatus::IN_PROGRESS);

  auto res2 = subgroup->objectPayload(folly::IOBuf::copyBuffer("data2"), false);
  EXPECT_FALSE(res2.hasValue());
  EXPECT_EQ(res2.error().code, MoQPublishError::CANCELLED);

  subgroup->reset(ResetStreamErrorCode::INTERNAL_ERROR);
}

// Test: Partial subscriber failure does not cancel data operations while other
// subscribers remain.
TEST_F(MoQForwarderTest, PartialSubscriberFailureDoesNotCancelData) {
  std::array<std::shared_ptr<MoQSession>, 3> subscribers;
  std::array<std::shared_ptr<MockTrackConsumer>, 3> consumers;
  std::array<std::shared_ptr<MockSubgroupConsumer>, 3> sgs;

  for (auto& s : subscribers) {
    s = createMockSession();
  }
  for (auto& c : consumers) {
    c = createMockConsumer();
  }

  EXPECT_CALL(*consumers[0], beginSubgroup(0, 0, _, _))
      .WillOnce([this, &sgs](uint64_t, uint64_t, uint8_t, bool) {
        sgs[0] = createMockSubgroupConsumer();
        EXPECT_CALL(*sgs[0], objectPayload(_, false))
            .WillOnce(Return(
                folly::makeExpected<MoQPublishError>(
                    ObjectPublishStatus::IN_PROGRESS)))
            .WillOnce(Return(
                folly::makeUnexpected(MoQPublishError(
                    MoQPublishError::WRITE_ERROR, "write failed"))));
        EXPECT_CALL(*sgs[0], reset(_)).Times(1);
        return folly::
            makeExpected<MoQPublishError, std::shared_ptr<SubgroupConsumer>>(
                sgs[0]);
      });

  for (size_t i = 1; i < 3; ++i) {
    EXPECT_CALL(*consumers[i], beginSubgroup(0, 0, _, _))
        .WillOnce([this, i, &sgs](uint64_t, uint64_t, uint8_t, bool) {
          sgs[i] = createMockSubgroupConsumer();
          EXPECT_CALL(*sgs[i], objectPayload(_, false))
              .WillRepeatedly(Return(
                  folly::makeExpected<MoQPublishError>(
                      ObjectPublishStatus::IN_PROGRESS)));
          EXPECT_CALL(*sgs[i], reset(ResetStreamErrorCode::SESSION_CLOSED))
              .Times(1);
          return folly::
              makeExpected<MoQPublishError, std::shared_ptr<SubgroupConsumer>>(
                  sgs[i]);
        });
  }

  auto forwarder = std::make_shared<MoQForwarder>(kFwdTestTrackName);
  for (size_t i = 0; i < 3; ++i) {
    auto h = addSubscriber(
        *forwarder, subscribers[i], consumers[i], RequestID(i + 1));
    ASSERT_NE(h, nullptr);
  }

  auto sgRes = forwarder->beginSubgroup(0, 0, 0);
  ASSERT_TRUE(sgRes.hasValue());
  auto subgroup = *sgRes;
  EXPECT_TRUE(subgroup->beginObject(0, 100, 0).hasValue());

  auto res1 = subgroup->objectPayload(folly::IOBuf::copyBuffer("data1"), false);
  EXPECT_TRUE(res1.hasValue());
  EXPECT_EQ(*res1, ObjectPublishStatus::IN_PROGRESS);

  auto res2 = subgroup->objectPayload(folly::IOBuf::copyBuffer("data2"), false);
  EXPECT_TRUE(res2.hasValue());
  EXPECT_EQ(*res2, ObjectPublishStatus::IN_PROGRESS);

  subgroup->reset(ResetStreamErrorCode::SESSION_CLOSED);
}

// Test: SubscribeUpdate can decrease start location via requestUpdate.
TEST_F(MoQForwarderTest, SubscribeUpdateStartLocationCanDecrease) {
  auto subscriberSession = createMockSession();

  auto forwarder = std::make_shared<MoQForwarder>(kFwdTestTrackName);
  auto consumer = createMockConsumer();

  SubscribeRequest sub;
  sub.fullTrackName = kFwdTestTrackName;
  sub.requestID = RequestID(1);
  sub.locType = LocationType::AbsoluteStart;
  sub.start = AbsoluteLocation{10, 0};
  sub.endGroup = 0;

  auto subscriber = forwarder->addSubscriber(subscriberSession, sub, consumer);
  ASSERT_NE(subscriber, nullptr);

  EXPECT_EQ(subscriber->range.start, (AbsoluteLocation{10, 0}));

  SubscribeUpdate subscribeUpdate{
      RequestID(2),
      sub.requestID,
      AbsoluteLocation{5, 0},
      0,
      kDefaultPriority,
      true};

  auto updateRes =
      folly::coro::blockingWait(subscriber->requestUpdate(subscribeUpdate));
  EXPECT_TRUE(updateRes.hasValue())
      << "RequestUpdate with decreased start should succeed";

  EXPECT_EQ(subscriber->range.start, (AbsoluteLocation{5, 0}))
      << "Start location should be updated to {5, 0}";
}

// Test: Subgroup is tombstoned after CANCELLED error (soft error), keeping the
// subscription alive for subsequent subgroups.
TEST_F(MoQForwarderTest, SubgroupTombstonedAfterCancelledError) {
  auto subscriber = createMockSession();

  auto forwarder = std::make_shared<MoQForwarder>(kFwdTestTrackName);
  auto consumer = createMockConsumer();
  std::shared_ptr<MockSubgroupConsumer> sg;

  EXPECT_CALL(*consumer, beginSubgroup(0, 0, _, _))
      .WillOnce([this, &sg](uint64_t, uint64_t, uint8_t, bool) {
        sg = createMockSubgroupConsumer();
        EXPECT_CALL(*sg, object(0, _, _, false)).WillOnce(Return(folly::unit));
        EXPECT_CALL(*sg, object(1, _, _, false))
            .WillOnce(Return(
                folly::makeUnexpected(MoQPublishError(
                    MoQPublishError::CANCELLED, "STOP_SENDING"))));
        return folly::
            makeExpected<MoQPublishError, std::shared_ptr<SubgroupConsumer>>(
                sg);
      });

  std::shared_ptr<MockSubgroupConsumer> sg2;
  EXPECT_CALL(*consumer, beginSubgroup(1, 0, _, _))
      .WillOnce([this, &sg2](uint64_t, uint64_t, uint8_t, bool) {
        sg2 = createMockSubgroupConsumer();
        EXPECT_CALL(*sg2, endOfSubgroup()).WillOnce(Return(folly::unit));
        return folly::
            makeExpected<MoQPublishError, std::shared_ptr<SubgroupConsumer>>(
                sg2);
      });

  auto subHandle =
      addSubscriber(*forwarder, subscriber, consumer, RequestID(1));
  ASSERT_NE(subHandle, nullptr);

  auto subgroup1Res = forwarder->beginSubgroup(0, 0, 0);
  ASSERT_TRUE(subgroup1Res.hasValue());
  auto subgroup1 = *subgroup1Res;

  EXPECT_TRUE(subgroup1->object(0, test::makeBuf(10)).hasValue());
  EXPECT_TRUE(subgroup1->object(1, test::makeBuf(10)).hasValue());

  auto subgroup2Res = forwarder->beginSubgroup(1, 0, 0);
  ASSERT_TRUE(subgroup2Res.hasValue());
  auto subgroup2 = *subgroup2Res;

  EXPECT_TRUE(subgroup2->endOfSubgroup().hasValue());
  EXPECT_TRUE(subgroup1->endOfSubgroup().hasValue());
}

// Test: Objects published to a tombstoned subgroup are skipped for that
// subscriber.
TEST_F(MoQForwarderTest, TombstonedSubgroupIgnoresSubsequentObjects) {
  auto subscriber = createMockSession();

  auto forwarder = std::make_shared<MoQForwarder>(kFwdTestTrackName);
  auto consumer = createMockConsumer();
  std::shared_ptr<MockSubgroupConsumer> sg;

  EXPECT_CALL(*consumer, beginSubgroup(0, 0, _, _))
      .WillOnce([this, &sg](uint64_t, uint64_t, uint8_t, bool) {
        sg = createMockSubgroupConsumer();
        EXPECT_CALL(*sg, object(0, _, _, false))
            .WillOnce(Return(
                folly::makeUnexpected(MoQPublishError(
                    MoQPublishError::CANCELLED, "delivery timeout"))));
        EXPECT_CALL(*sg, object(1, _, _, _)).Times(0);
        EXPECT_CALL(*sg, object(2, _, _, _)).Times(0);
        return folly::
            makeExpected<MoQPublishError, std::shared_ptr<SubgroupConsumer>>(
                sg);
      });

  auto subHandle =
      addSubscriber(*forwarder, subscriber, consumer, RequestID(1));
  ASSERT_NE(subHandle, nullptr);

  auto subgroupRes = forwarder->beginSubgroup(0, 0, 0);
  ASSERT_TRUE(subgroupRes.hasValue());
  auto subgroup = *subgroupRes;

  EXPECT_TRUE(subgroup->object(0, test::makeBuf(10)).hasValue());

  auto res1 = subgroup->object(1, test::makeBuf(10));
  EXPECT_FALSE(res1.hasValue());
  EXPECT_EQ(res1.error().code, MoQPublishError::CANCELLED);

  auto res2 = subgroup->object(2, test::makeBuf(10));
  EXPECT_FALSE(res2.hasValue());
  EXPECT_EQ(res2.error().code, MoQPublishError::CANCELLED);

  subgroup->reset(ResetStreamErrorCode::SESSION_CLOSED);
}

// Test: Late joiner gets a subgroup even after another subscriber tombstoned
// it.
TEST_F(MoQForwarderTest, LateJoinerGetsSubgroupAfterTombstone) {
  auto subscriber1 = createMockSession();
  auto subscriber2 = createMockSession();

  auto forwarder = std::make_shared<MoQForwarder>(kFwdTestTrackName);

  auto consumer1 = createMockConsumer();
  std::shared_ptr<MockSubgroupConsumer> sg1;

  EXPECT_CALL(*consumer1, beginSubgroup(0, 0, _, _))
      .WillOnce([this, &sg1](uint64_t, uint64_t, uint8_t, bool) {
        sg1 = createMockSubgroupConsumer();
        EXPECT_CALL(*sg1, object(0, _, _, false))
            .WillOnce(Return(
                folly::makeUnexpected(MoQPublishError(
                    MoQPublishError::CANCELLED, "STOP_SENDING"))));
        return folly::
            makeExpected<MoQPublishError, std::shared_ptr<SubgroupConsumer>>(
                sg1);
      });

  auto consumer2 = createMockConsumer();
  std::shared_ptr<MockSubgroupConsumer> sg2;

  EXPECT_CALL(*consumer2, beginSubgroup(0, 0, _, _))
      .WillOnce([this, &sg2](uint64_t, uint64_t, uint8_t, bool) {
        sg2 = createMockSubgroupConsumer();
        EXPECT_CALL(*sg2, object(1, _, _, false)).WillOnce(Return(folly::unit));
        EXPECT_CALL(*sg2, endOfSubgroup()).WillOnce(Return(folly::unit));
        return folly::
            makeExpected<MoQPublishError, std::shared_ptr<SubgroupConsumer>>(
                sg2);
      });

  auto sub1Handle =
      addSubscriber(*forwarder, subscriber1, consumer1, RequestID(1));
  ASSERT_NE(sub1Handle, nullptr);

  auto subgroupRes = forwarder->beginSubgroup(0, 0, 0);
  ASSERT_TRUE(subgroupRes.hasValue());
  auto subgroup = *subgroupRes;

  EXPECT_TRUE(subgroup->object(0, test::makeBuf(10)).hasValue());

  auto sub2Handle =
      addSubscriber(*forwarder, subscriber2, consumer2, RequestID(2));
  ASSERT_NE(sub2Handle, nullptr);

  EXPECT_TRUE(subgroup->object(1, test::makeBuf(10)).hasValue());
  EXPECT_TRUE(subgroup->endOfSubgroup().hasValue());
}

// Test: Hard errors (WRITE_ERROR) remove the entire subscription.
TEST_F(MoQForwarderTest, HardErrorsRemoveSubscriber) {
  auto subscriber = createMockSession();

  auto forwarder = std::make_shared<MoQForwarder>(kFwdTestTrackName);
  auto consumer = createMockConsumer();
  std::shared_ptr<MockSubgroupConsumer> sg;

  EXPECT_CALL(*consumer, beginSubgroup(0, 0, _, _))
      .WillOnce([this, &sg](uint64_t, uint64_t, uint8_t, bool) {
        sg = createMockSubgroupConsumer();
        EXPECT_CALL(*sg, object(0, _, _, false))
            .WillOnce(Return(
                folly::makeUnexpected(MoQPublishError(
                    MoQPublishError::WRITE_ERROR, "transport broken"))));
        EXPECT_CALL(*sg, reset(_)).Times(1);
        return folly::
            makeExpected<MoQPublishError, std::shared_ptr<SubgroupConsumer>>(
                sg);
      });

  EXPECT_CALL(*consumer, publishDone(_))
      .WillOnce(Return(folly::makeExpected<MoQPublishError>(folly::unit)));

  EXPECT_CALL(*consumer, beginSubgroup(1, _, _, _)).Times(0);

  auto subHandle =
      addSubscriber(*forwarder, subscriber, consumer, RequestID(1));
  ASSERT_NE(subHandle, nullptr);

  auto subgroup1Res = forwarder->beginSubgroup(0, 0, 0);
  ASSERT_TRUE(subgroup1Res.hasValue());
  auto subgroup1 = *subgroup1Res;

  auto res = subgroup1->object(0, test::makeBuf(10));
  EXPECT_FALSE(res.hasValue());
  EXPECT_EQ(res.error().code, MoQPublishError::CANCELLED);

  auto subgroup2Res = forwarder->beginSubgroup(1, 0, 0);
  EXPECT_FALSE(subgroup2Res.hasValue());
  EXPECT_EQ(subgroup2Res.error().code, MoQPublishError::CANCELLED);

  subgroup1->reset(ResetStreamErrorCode::SESSION_CLOSED);
}

// Test: endOfSubgroup returning WRITE_ERROR (hard error) does not crash.
TEST_F(MoQForwarderTest, EndOfSubgroupHardErrorDoesNotCrash) {
  auto subscriberSession = createMockSession();

  auto forwarder = std::make_shared<MoQForwarder>(kFwdTestTrackName);
  auto consumer = createMockConsumer();
  std::shared_ptr<MockSubgroupConsumer> sg;

  EXPECT_CALL(*consumer, beginSubgroup(0, 0, _, _))
      .WillOnce([this, &sg](uint64_t, uint64_t, uint8_t, bool) {
        sg = createMockSubgroupConsumer();
        EXPECT_CALL(*sg, endOfSubgroup())
            .WillOnce(Return(
                folly::makeUnexpected(MoQPublishError(
                    MoQPublishError::WRITE_ERROR, "transport broken"))));
        return folly::
            makeExpected<MoQPublishError, std::shared_ptr<SubgroupConsumer>>(
                sg);
      });

  EXPECT_CALL(*consumer, publishDone(_))
      .WillOnce(Return(folly::makeExpected<MoQPublishError>(folly::unit)));

  auto subHandle =
      addSubscriber(*forwarder, subscriberSession, consumer, RequestID(1));
  ASSERT_NE(subHandle, nullptr);

  auto subgroupRes = forwarder->beginSubgroup(0, 0, 0);
  ASSERT_TRUE(subgroupRes.hasValue());
  auto subgroup = *subgroupRes;

  subgroup->endOfSubgroup();
}

// Test: Resetting a subgroup while the forwarder's last subscriber is draining
// must not crash (use-after-free scenario).
TEST_F(MoQForwarderTest, ResetDuringDrainingDoesNotCrash) {
  auto session = createMockSession();

  auto forwarder = std::make_shared<MoQForwarder>(kFwdTestTrackName);
  auto callback = std::make_shared<ForwarderDestroyingCallback>(forwarder);
  forwarder->setCallback(callback);

  auto consumer = createMockConsumer();
  std::shared_ptr<MockSubgroupConsumer> sg;

  EXPECT_CALL(*consumer, beginSubgroup(0, 0, _, _))
      .WillOnce([this, &sg](uint64_t, uint64_t, uint8_t, bool) {
        sg = createMockSubgroupConsumer();
        return folly::
            makeExpected<MoQPublishError, std::shared_ptr<SubgroupConsumer>>(
                sg);
      });

  EXPECT_CALL(*consumer, publishDone(_))
      .WillOnce(Return(folly::makeExpected<MoQPublishError>(folly::unit)));

  SubscribeRequest sub;
  sub.fullTrackName = kFwdTestTrackName;
  sub.requestID = RequestID(1);
  sub.locType = LocationType::LargestGroup;
  forwarder->addSubscriber(session, sub, consumer);

  auto subgroupRes = forwarder->beginSubgroup(0, 0, 0);
  ASSERT_TRUE(subgroupRes.hasValue());
  auto subgroup = *subgroupRes;

  forwarder->publishDone(
      PublishDone{
          RequestID(0),
          PublishDoneStatusCode::SUBSCRIPTION_ENDED,
          0,
          "publisher ended"});

  EXPECT_CALL(*sg, reset(_)).Times(1);
  subgroup->reset(ResetStreamErrorCode::INTERNAL_ERROR);

  EXPECT_EQ(forwarder, nullptr);
}

// Test: Same as above but with multiple subscribers draining simultaneously.
TEST_F(MoQForwarderTest, ResetDuringDrainingMultipleSubscribersDoesNotCrash) {
  auto session1 = createMockSession();
  auto session2 = createMockSession();

  auto forwarder = std::make_shared<MoQForwarder>(kFwdTestTrackName);
  auto callback = std::make_shared<ForwarderDestroyingCallback>(forwarder);
  forwarder->setCallback(callback);

  std::shared_ptr<MockSubgroupConsumer> sg1, sg2;
  auto consumer1 = createMockConsumer();
  auto consumer2 = createMockConsumer();

  EXPECT_CALL(*consumer1, beginSubgroup(0, 0, _, _))
      .WillOnce([this, &sg1](uint64_t, uint64_t, uint8_t, bool) {
        sg1 = createMockSubgroupConsumer();
        return folly::
            makeExpected<MoQPublishError, std::shared_ptr<SubgroupConsumer>>(
                sg1);
      });

  EXPECT_CALL(*consumer2, beginSubgroup(0, 0, _, _))
      .WillOnce([this, &sg2](uint64_t, uint64_t, uint8_t, bool) {
        sg2 = createMockSubgroupConsumer();
        return folly::
            makeExpected<MoQPublishError, std::shared_ptr<SubgroupConsumer>>(
                sg2);
      });

  EXPECT_CALL(*consumer1, publishDone(_))
      .WillOnce(Return(folly::makeExpected<MoQPublishError>(folly::unit)));
  EXPECT_CALL(*consumer2, publishDone(_))
      .WillOnce(Return(folly::makeExpected<MoQPublishError>(folly::unit)));

  SubscribeRequest sub1;
  sub1.fullTrackName = kFwdTestTrackName;
  sub1.requestID = RequestID(1);
  sub1.locType = LocationType::LargestGroup;
  forwarder->addSubscriber(session1, sub1, consumer1);

  SubscribeRequest sub2;
  sub2.fullTrackName = kFwdTestTrackName;
  sub2.requestID = RequestID(2);
  sub2.locType = LocationType::LargestGroup;
  forwarder->addSubscriber(session2, sub2, consumer2);

  auto subgroupRes = forwarder->beginSubgroup(0, 0, 0);
  ASSERT_TRUE(subgroupRes.hasValue());
  auto subgroup = *subgroupRes;

  forwarder->publishDone(
      PublishDone{
          RequestID(0),
          PublishDoneStatusCode::SUBSCRIPTION_ENDED,
          0,
          "publisher ended"});

  EXPECT_CALL(*sg1, reset(_)).Times(1);
  EXPECT_CALL(*sg2, reset(_)).Times(1);
  subgroup->reset(ResetStreamErrorCode::INTERNAL_ERROR);

  EXPECT_EQ(forwarder, nullptr);
}

// Test: Extensions set on the forwarder are included in SubscribeOk for all
// subscribers.
TEST_F(MoQForwarderTest, ExtensionsIncludedInSubscribeOkForSubscribers) {
  auto session1 = createMockSession();
  auto session2 = createMockSession();

  auto forwarder = std::make_shared<MoQForwarder>(kFwdTestTrackName);
  Extensions ext;
  ext.insertMutableExtension(Extension{kDeliveryTimeoutExtensionType, 7000});
  ext.insertMutableExtension(Extension{0xDEAD'0000, 123});
  forwarder->setExtensions(ext);

  auto consumer1 = createMockConsumer();
  SubscribeRequest sub1;
  sub1.fullTrackName = kFwdTestTrackName;
  sub1.requestID = RequestID(1);
  sub1.locType = LocationType::LargestGroup;
  auto subscriber1 = forwarder->addSubscriber(session1, sub1, consumer1);
  ASSERT_NE(subscriber1, nullptr);

  EXPECT_EQ(
      subscriber1->subscribeOk().extensions.getIntExtension(
          kDeliveryTimeoutExtensionType),
      7000);
  EXPECT_EQ(
      subscriber1->subscribeOk().extensions.getIntExtension(0xDEAD'0000), 123);

  auto consumer2 = createMockConsumer();
  SubscribeRequest sub2;
  sub2.fullTrackName = kFwdTestTrackName;
  sub2.requestID = RequestID(2);
  sub2.locType = LocationType::LargestGroup;
  auto subscriber2 = forwarder->addSubscriber(session2, sub2, consumer2);
  ASSERT_NE(subscriber2, nullptr);

  EXPECT_EQ(
      subscriber2->subscribeOk().extensions.getIntExtension(
          kDeliveryTimeoutExtensionType),
      7000);
  EXPECT_EQ(
      subscriber2->subscribeOk().extensions.getIntExtension(0xDEAD'0000), 123);

  auto session3 = createMockSession();
  auto subscriber3 = forwarder->addSubscriber(session3, /*forward=*/false);
  ASSERT_NE(subscriber3, nullptr);

  EXPECT_EQ(
      subscriber3->subscribeOk().extensions.getIntExtension(
          kDeliveryTimeoutExtensionType),
      7000);
  EXPECT_EQ(
      subscriber3->subscribeOk().extensions.getIntExtension(0xDEAD'0000), 123);
}

// Test: tryProcessNewGroupRequest gating and clearing logic.
TEST_F(MoQForwarderTest, ForwarderNGRGatingAndClearingLogic) {
  auto cb = std::make_shared<TestNGRCallback>();
  auto forwarder = std::make_shared<MoQForwarder>(kFwdTestTrackName);
  forwarder->setCallback(cb);

  forwarder->tryProcessNewGroupRequest(makeNGRParams(5));
  EXPECT_TRUE(cb->calls.empty());

  PublishRequest pub;
  setPublisherDynamicGroups(pub, true);
  forwarder->setExtensions(pub.extensions);

  forwarder->tryProcessNewGroupRequest(makeNGRParams(5));
  ASSERT_EQ(cb->calls.size(), 1u);
  EXPECT_EQ(cb->calls[0], 5u);
  cb->calls.clear();

  forwarder->tryProcessNewGroupRequest(makeNGRParams(5));
  EXPECT_TRUE(cb->calls.empty());

  forwarder->setLargest({10, 0});
  forwarder->tryProcessNewGroupRequest(makeNGRParams(5));
  forwarder->tryProcessNewGroupRequest(makeNGRParams(10));
  EXPECT_TRUE(cb->calls.empty());
  forwarder->tryProcessNewGroupRequest(makeNGRParams(11));
  ASSERT_EQ(cb->calls.size(), 1u);
  EXPECT_EQ(cb->calls[0], 11u);
  cb->calls.clear();

  forwarder->tryProcessNewGroupRequest(makeNGRParams(11));
  forwarder->tryProcessNewGroupRequest(makeNGRParams(10));
  EXPECT_TRUE(cb->calls.empty());
  forwarder->tryProcessNewGroupRequest(makeNGRParams(12));
  ASSERT_EQ(cb->calls.size(), 1u);
  EXPECT_EQ(cb->calls[0], 12u);
  cb->calls.clear();

  forwarder->updateLargest(13, 0);
  forwarder->tryProcessNewGroupRequest(makeNGRParams(14));
  ASSERT_EQ(cb->calls.size(), 1u);
  EXPECT_EQ(cb->calls[0], 14u);
}

// Test: publishDone on a forwarder with a forward-only subscriber (null
// trackConsumer) must not crash.
TEST_F(MoQForwarderTest, PublishDoneWithForwardOnlySubscriber) {
  auto session = createMockSession();

  auto forwarder = std::make_shared<MoQForwarder>(kFwdTestTrackName);

  auto subscriber = forwarder->addSubscriber(session, /*forward=*/true);
  ASSERT_NE(subscriber, nullptr);
  EXPECT_EQ(subscriber->trackConsumer, nullptr);

  auto res = forwarder->publishDone(
      PublishDone{
          RequestID(0),
          PublishDoneStatusCode::SUBSCRIPTION_ENDED,
          0,
          "publisher ended"});
  EXPECT_TRUE(res.hasValue());

  EXPECT_TRUE(forwarder->empty());
}

// Test: removeSubscriber with a PublishDone on a forward-only subscriber (null
// trackConsumer) must not crash.
TEST_F(MoQForwarderTest, RemoveForwardOnlySubscriberWithPublishDone) {
  auto session = createMockSession();

  auto forwarder = std::make_shared<MoQForwarder>(kFwdTestTrackName);

  auto subscriber = forwarder->addSubscriber(session, /*forward=*/true);
  ASSERT_NE(subscriber, nullptr);
  EXPECT_EQ(subscriber->trackConsumer, nullptr);

  forwarder->removeSubscriber(
      session,
      PublishDone{
          RequestID(0),
          PublishDoneStatusCode::SUBSCRIPTION_ENDED,
          0,
          "session disconnect"},
      "test");
  EXPECT_TRUE(forwarder->empty());
}

// Repro for moxygen#168: a PUBLISH_OK arriving after beginObject has already
// fanned out must not orphan the subscriber's open SubgroupConsumer.
// Sequence:
//   1. Subscribe (LargestObject -> range.start resolves to {0,0} with no data).
//   2. Publisher begins multi-chunk object 5; forwarder creates sg and
//      delivers the initial payload.
//   3. PUBLISH_OK arrives late (LargestObject) -> range.start becomes {0,6}.
//   4. Publisher sends the object 5 continuation. Before the fix, the
//      forwarder re-checked range.start on the existing sg and silently
//      skipped it, stranding it with a partial object and tripping
//      validatePublish on the next beginObject downstream.
TEST_F(MoQForwarderTest, SubscriberOnPublishOkDoesNotStrandPartialObject) {
  auto subscriberSession = createMockSession();
  auto mockConsumer = createMockConsumer();

  constexpr uint64_t kObjectID = 5;
  constexpr uint64_t kObjectLength = 100;
  constexpr uint64_t kInitialLength = 20;
  std::string downstreamPayload;

  EXPECT_CALL(*mockConsumer, beginSubgroup(_, _, _, _))
      .WillOnce([&](uint64_t, uint64_t, uint8_t, bool) {
        auto sg = createMockSubgroupConsumer();
        EXPECT_CALL(*sg, beginObject(kObjectID, kObjectLength, _, _))
            .WillOnce([&](uint64_t, uint64_t, Payload p, Extensions) {
              downstreamPayload += p->moveToFbString().toStdString();
              return folly::makeExpected<MoQPublishError>(folly::unit);
            });
        EXPECT_CALL(*sg, objectPayload(_, _)).WillOnce([&](Payload p, bool) {
          downstreamPayload += p->moveToFbString().toStdString();
          return folly::makeExpected<MoQPublishError>(
              ObjectPublishStatus::IN_PROGRESS);
        });
        return folly::
            makeExpected<MoQPublishError, std::shared_ptr<SubgroupConsumer>>(
                sg);
      });

  auto forwarder = std::make_shared<MoQForwarder>(kFwdTestTrackName);

  auto subscriber =
      addSubscriber(*forwarder, subscriberSession, mockConsumer, RequestID(1));
  ASSERT_NE(subscriber, nullptr);

  auto subgroupRes = forwarder->beginSubgroup(0, 0, 0);
  ASSERT_TRUE(subgroupRes.hasValue());
  auto subgroup = *subgroupRes;

  auto initial = folly::IOBuf::copyBuffer(std::string(kInitialLength, 'a'));
  ASSERT_TRUE(
      subgroup->beginObject(kObjectID, kObjectLength, std::move(initial), {})
          .hasValue());

  // Late PUBLISH_OK with LargestObject: range.start becomes largest.object + 1.
  PublishOk pubOk{
      RequestID(1),
      true, // forward
      0,    // subscriberPriority
      GroupOrder::OldestFirst,
      LocationType::LargestObject,
      std::nullopt, // start
      std::nullopt, // endGroup
      TrackRequestParameters(FrameType::PUBLISH_OK)};
  subscriber->onPublishOk(pubOk);
  EXPECT_EQ(subscriber->range.start.object, kObjectID + 1);

  auto continuation = folly::IOBuf::copyBuffer(
      std::string(kObjectLength - kInitialLength, 'b'));
  ASSERT_TRUE(
      subgroup->objectPayload(std::move(continuation), /*finStream=*/false)
          .hasValue());

  ASSERT_TRUE(subgroup->endOfSubgroup().hasValue());

  EXPECT_EQ(
      downstreamPayload,
      std::string(kInitialLength, 'a') +
          std::string(kObjectLength - kInitialLength, 'b'));
}

} // namespace moxygen::test

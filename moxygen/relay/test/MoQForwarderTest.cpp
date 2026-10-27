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
  std::shared_ptr<MockMoQSession> createMockSession(
      uint64_t version = kVersionDraftCurrent) {
    auto session = std::make_shared<NiceMock<MockMoQSession>>();
    ON_CALL(*session, getNegotiatedVersion())
        .WillByDefault(Return(std::optional<uint64_t>(version)));
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

  // Like MoQSession's track publisher, refuses new subgroups, objectStreams and
  // datagrams once publishDone() has been called.
  std::shared_ptr<MockTrackConsumer> createSessionLikeConsumer() {
    auto consumer = createMockConsumer();
    auto done = std::make_shared<bool>(false);
    auto publishAfterDone = [] {
      return folly::makeUnexpected(MoQPublishError(
          MoQPublishError::API_ERROR, "Publish after publishDone"));
    };
    ON_CALL(*consumer, publishDone(_)).WillByDefault([done](PublishDone) {
      *done = true;
      return folly::makeExpected<MoQPublishError>(folly::unit);
    });
    ON_CALL(*consumer, beginSubgroup(_, _, _, _))
        .WillByDefault(
            [this, done, publishAfterDone](
                uint64_t,
                uint64_t,
                uint8_t,
                moxygen::TrackConsumer::BeginSubgroupOptions)
                -> folly::Expected<
                    std::shared_ptr<SubgroupConsumer>,
                    MoQPublishError> {
              if (*done) {
                return publishAfterDone();
              }
              auto sg = createMockSubgroupConsumer();
              ON_CALL(*sg, object(_, _, _, _))
                  .WillByDefault(Return(
                      folly::makeExpected<MoQPublishError>(folly::unit)));
              return std::shared_ptr<SubgroupConsumer>(sg);
            });
    auto refuseAfterDone = [done, publishAfterDone](
                               const ObjectHeader&, Payload, bool)
        -> folly::Expected<folly::Unit, MoQPublishError> {
      if (*done) {
        return publishAfterDone();
      }
      return folly::unit;
    };
    ON_CALL(*consumer, objectStream(_, _, _)).WillByDefault(refuseAfterDone);
    ON_CALL(*consumer, datagram(_, _, _)).WillByDefault(refuseAfterDone);
    return consumer;
  }

  void endPublish(MoQForwarder& forwarder) {
    forwarder.publishDone(
        PublishDone{
            RequestID(0),
            PublishDoneStatusCode::SUBSCRIPTION_ENDED,
            0,
            "publisher ended"});
  }

  // Add a subscriber to a forwarder using only forwarder APIs.
  std::shared_ptr<MoQForwarder::Subscriber> addSubscriber(
      MoQForwarder& forwarder,
      std::shared_ptr<MoQSession> session,
      std::shared_ptr<TrackConsumer> consumer,
      RequestID requestID = RequestID(0),
      LocationFilter filter = LocationFilter::nextObject()) {
    SubscribeRequest sub;
    sub.fullTrackName = forwarder.fullTrackName();
    sub.requestID = requestID;
    sub.filter = filter;
    return forwarder.addSubscriber(
        session->sessionId(), sub, std::move(consumer));
  }

  AssertionResult applyForwardUpdate(
      const std::shared_ptr<MoQForwarder::Subscriber>& subscriber,
      RequestID requestID,
      bool forward) {
    RequestUpdate update;
    update.requestID = requestID;
    update.existingRequestID = RequestID(1);
    update.forward = forward;
    auto res = folly::coro::blockingWait(subscriber->requestUpdate(update));
    if (!res.hasValue()) {
      return AssertionFailure() << "REQUEST_UPDATE failed";
    }
    return AssertionSuccess();
  }

  AssertionResult pauseAndResumeForwarding(
      const std::shared_ptr<MoQForwarder::Subscriber>& subscriber) {
    auto pauseRes = applyForwardUpdate(subscriber, RequestID(2), false);
    if (!pauseRes) {
      return pauseRes;
    }
    return applyForwardUpdate(subscriber, RequestID(3), true);
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

struct CountingCallback : public MoQForwarder::Callback {
  void onEmpty(MoQForwarder*) override {
    onEmptyCount++;
  }
  void forwardChanged(MoQForwarder*, bool forward) override {
    forwardChanges.push_back(forward);
  }
  size_t onEmptyCount{0};
  std::vector<bool> forwardChanges;
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

auto subgroupConsumerResult(std::shared_ptr<SubgroupConsumer> subgroup) {
  return folly::
      makeExpected<MoQPublishError, std::shared_ptr<SubgroupConsumer>>(
          std::move(subgroup));
}

auto stopSendingError() {
  return folly::makeUnexpected(
      MoQPublishError(MoQPublishError::CANCELLED, "STOP_SENDING"));
}

void expectBeginSubgroups(
    MockTrackConsumer& consumer,
    std::shared_ptr<SubgroupConsumer> first,
    std::shared_ptr<SubgroupConsumer> second) {
  EXPECT_CALL(consumer, beginSubgroup(0, 0, _, _))
      .WillOnce(Return(subgroupConsumerResult(std::move(first))))
      .WillOnce(Return(subgroupConsumerResult(std::move(second))));
}

void expectStopSending(MockSubgroupConsumer& subgroup, uint64_t objectID) {
  EXPECT_CALL(subgroup, object(objectID, _, _, false))
      .WillOnce(Return(stopSendingError()));
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
      .WillOnce([this](
                    uint64_t,
                    uint64_t,
                    uint8_t,
                    moxygen::TrackConsumer::BeginSubgroupOptions) {
        return folly::
            makeExpected<MoQPublishError, std::shared_ptr<SubgroupConsumer>>(
                createMockSubgroupConsumer());
      });

  EXPECT_CALL(*mockConsumer2, beginSubgroup(_, _, _, _))
      .WillOnce([this](
                    uint64_t,
                    uint64_t,
                    uint8_t,
                    moxygen::TrackConsumer::BeginSubgroupOptions) {
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

// Test: the subgroup ID encoding a subscriber gets depends on where its stream
// starts. The implicit "subgroup ID = first object ID" form is only correct for
// a stream whose opening object has the subgroup ID for its object ID.
TEST_F(MoQForwarderTest, SubgroupIDFormatDependsOnJoinPoint) {
  auto captureBeginSubgroup =
      [this](moxygen::TrackConsumer::BeginSubgroupOptions& captured) {
        auto consumer = createMockConsumer();
        EXPECT_CALL(*consumer, beginSubgroup(0, 4, _, _))
            .WillOnce(
                [this, &captured](
                    uint64_t,
                    uint64_t,
                    uint8_t,
                    moxygen::TrackConsumer::BeginSubgroupOptions options) {
                  captured = options;
                  auto sg = createMockSubgroupConsumer();
                  ON_CALL(*sg, object(_, _, _, _))
                      .WillByDefault(Return(
                          folly::makeExpected<MoQPublishError>(folly::unit)));
                  return subgroupConsumerResult(sg);
                });
        return consumer;
      };

  auto forwarder = std::make_shared<MoQForwarder>(kFwdTestTrackName);

  moxygen::TrackConsumer::BeginSubgroupOptions atOpen;
  auto atOpenConsumer = captureBeginSubgroup(atOpen);
  ASSERT_NE(
      addSubscriber(
          *forwarder, createMockSession(), atOpenConsumer, RequestID(1)),
      nullptr);

  moxygen::TrackConsumer::BeginSubgroupOptions publishOptions;
  publishOptions.beginsWithFirstObject = true;
  publishOptions.subgroupIDFormat = SubgroupIDFormat::FirstObject;
  auto sgRes = forwarder->beginSubgroup(0, 4, 0, publishOptions);
  ASSERT_TRUE(sgRes.hasValue());
  auto subgroup = *sgRes;

  // Joins after the subgroup opened but before its first object arrives.
  moxygen::TrackConsumer::BeginSubgroupOptions beforeFirstObject;
  auto beforeFirstObjectConsumer = captureBeginSubgroup(beforeFirstObject);
  ASSERT_NE(
      addSubscriber(
          *forwarder,
          createMockSession(),
          beforeFirstObjectConsumer,
          RequestID(2)),
      nullptr);

  EXPECT_TRUE(subgroup->object(4, test::makeBuf(10)).hasValue());

  moxygen::TrackConsumer::BeginSubgroupOptions midSubgroup;
  auto midSubgroupConsumer = captureBeginSubgroup(midSubgroup);
  ASSERT_NE(
      addSubscriber(
          *forwarder, createMockSession(), midSubgroupConsumer, RequestID(3)),
      nullptr);

  EXPECT_TRUE(subgroup->object(5, test::makeBuf(10)).hasValue());

  // Both of these streams open on object 4, which is the subgroup ID, so they
  // keep the implicit encoding.
  EXPECT_EQ(atOpen.subgroupIDFormat, SubgroupIDFormat::FirstObject);
  EXPECT_TRUE(atOpen.beginsWithFirstObject);
  EXPECT_EQ(beforeFirstObject.subgroupIDFormat, SubgroupIDFormat::FirstObject);
  EXPECT_TRUE(beforeFirstObject.beginsWithFirstObject);

  // This one opens on object 5, which would decode as subgroup 5, so it has to
  // carry the subgroup ID.
  EXPECT_EQ(midSubgroup.subgroupIDFormat, SubgroupIDFormat::Present);
  EXPECT_FALSE(midSubgroup.beginsWithFirstObject);
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
        .WillOnce([this, i, &sub0_sgs](
                      uint64_t,
                      uint64_t,
                      uint8_t,
                      moxygen::TrackConsumer::BeginSubgroupOptions) {
          sub0_sgs[i] = createMockSubgroupConsumer();
          return folly::
              makeExpected<MoQPublishError, std::shared_ptr<SubgroupConsumer>>(
                  sub0_sgs[i]);
        });
  }

  EXPECT_CALL(*consumers[1], beginSubgroup(1, 0, _, _))
      .WillOnce([this, &sub1_sg0](
                    uint64_t,
                    uint64_t,
                    uint8_t,
                    moxygen::TrackConsumer::BeginSubgroupOptions) {
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

// Test: A draining subscriber is not given a subgroup it never opened when
// that subgroup delivers more objects after publishDone, and the subgroup it
// is draining still finishes.
TEST_F(MoQForwarderTest, DrainingSubscriberDoesNotOpenLateSubgroup) {
  auto session0 = createMockSession();
  auto session1 = createMockSession();
  auto forwarder = std::make_shared<MoQForwarder>(kFwdTestTrackName);
  auto consumer0 = createSessionLikeConsumer();
  auto consumer1 = createSessionLikeConsumer();
  auto drainingSubgroup = createMockSubgroupConsumer();
  EXPECT_CALL(*consumer1, beginSubgroup(1, 0, _, _))
      .WillOnce(Return(subgroupConsumerResult(drainingSubgroup)));
  EXPECT_CALL(*consumer1, beginSubgroup(0, 0, _, _)).Times(0);
  EXPECT_CALL(*consumer1, publishDone(_));

  // Group 0 is open before subscriber 1 joins and delivers nothing more until
  // after publishDone.
  addSubscriber(*forwarder, session0, consumer0, RequestID(1));
  auto sg0 = forwarder->beginSubgroup(0, 0, 0).value();
  EXPECT_TRUE(sg0->object(0, test::makeBuf(10)).hasValue());
  addSubscriber(*forwarder, session1, consumer1, RequestID(2));
  auto sg1 = forwarder->beginSubgroup(1, 0, 0).value();
  endPublish(*forwarder);

  EXPECT_CALL(*drainingSubgroup, reset(_)).Times(0);
  EXPECT_CALL(*drainingSubgroup, object(0, _, _, _))
      .WillOnce(Return(folly::makeExpected<MoQPublishError>(folly::unit)));
  EXPECT_CALL(*drainingSubgroup, endOfSubgroup());

  EXPECT_TRUE(sg0->object(1, test::makeBuf(10)).hasValue());
  EXPECT_TRUE(sg1->object(0, test::makeBuf(10)).hasValue());
  EXPECT_TRUE(sg1->endOfSubgroup().hasValue());
  EXPECT_TRUE(sg0->endOfSubgroup().hasValue());
}

// Test: A subgroup begun after publishDone is CANCELLED rather than registered
// with no subscriber able to take it, so the forwarder still empties once the
// draining subscriber finishes.
TEST_F(MoQForwarderTest, SubgroupBegunAfterPublishDoneIsCancelled) {
  auto session = createMockSession();
  auto forwarder = std::make_shared<MoQForwarder>(kFwdTestTrackName);
  auto callback = std::make_shared<CountingCallback>();
  forwarder->setCallback(callback);
  auto consumer = createSessionLikeConsumer();
  auto drainingSubgroup = createMockSubgroupConsumer();
  EXPECT_CALL(*consumer, beginSubgroup(0, 0, _, _))
      .WillOnce(Return(subgroupConsumerResult(drainingSubgroup)));
  EXPECT_CALL(*consumer, beginSubgroup(1, 0, _, _)).Times(0);
  EXPECT_CALL(*consumer, publishDone(_));

  addSubscriber(*forwarder, session, consumer, RequestID(1));
  auto sg0 = forwarder->beginSubgroup(0, 0, 0).value();
  endPublish(*forwarder);

  EXPECT_CALL(*drainingSubgroup, reset(_)).Times(0);
  EXPECT_CALL(*drainingSubgroup, endOfSubgroup());

  auto sg1 = forwarder->beginSubgroup(1, 0, 0);
  ASSERT_TRUE(sg1.hasError());
  EXPECT_EQ(sg1.error().code, MoQPublishError::CANCELLED);
  EXPECT_EQ(callback->onEmptyCount, 0);
  EXPECT_TRUE(sg0->endOfSubgroup().hasValue());
  EXPECT_EQ(callback->onEmptyCount, 1);
}

// Test: A draining subscriber is not sent objectStreams or datagrams, and the
// subgroup it is draining still finishes.
TEST_F(MoQForwarderTest, DrainingSubscriberIsNotSentObjectStreamsOrDatagrams) {
  auto session = createMockSession();
  auto forwarder = std::make_shared<MoQForwarder>(kFwdTestTrackName);
  auto consumer = createSessionLikeConsumer();
  auto drainingSubgroup = createMockSubgroupConsumer();
  EXPECT_CALL(*consumer, beginSubgroup(0, 0, _, _))
      .WillOnce(Return(subgroupConsumerResult(drainingSubgroup)));
  EXPECT_CALL(*consumer, objectStream(_, _, _)).Times(0);
  EXPECT_CALL(*consumer, datagram(_, _, _)).Times(0);
  EXPECT_CALL(*consumer, publishDone(_));

  addSubscriber(*forwarder, session, consumer, RequestID(1));
  auto sg0 = forwarder->beginSubgroup(0, 0, 0).value();
  endPublish(*forwarder);

  EXPECT_CALL(*drainingSubgroup, reset(_)).Times(0);
  EXPECT_CALL(*drainingSubgroup, endOfSubgroup());

  (void)forwarder->objectStream(
      ObjectHeader(1, 0, 0), test::makeBuf(10), false);
  (void)forwarder->datagram(ObjectHeader(1, 0, 1), test::makeBuf(10), false);
  EXPECT_TRUE(sg0->endOfSubgroup().hasValue());
}

// Test: An object on a subgroup that only a draining subscriber could still
// have taken is CANCELLED.
TEST_F(MoQForwarderTest, DrainingSubscriberDoesNotKeepLaggingSubgroupAlive) {
  auto session = createMockSession();
  auto forwarder = std::make_shared<MoQForwarder>(kFwdTestTrackName);
  auto consumer = createSessionLikeConsumer();
  EXPECT_CALL(*consumer, beginSubgroup(0, 1, _, _));
  EXPECT_CALL(*consumer, beginSubgroup(0, 0, _, _)).Times(0);

  // Starting at object 5, the subscriber opens subgroup 1 there and would take
  // subgroup 0 only from a later object.
  addSubscriber(
      *forwarder,
      session,
      consumer,
      RequestID(1),
      LocationFilter::absoluteStart({0, 5}));
  auto lagging = forwarder->beginSubgroup(0, 0, 0).value();
  auto sg1 = forwarder->beginSubgroup(0, 1, 0).value();
  EXPECT_TRUE(sg1->object(5, test::makeBuf(10)).hasValue());
  EXPECT_TRUE(lagging->object(2, test::makeBuf(10)).hasValue());
  endPublish(*forwarder);

  auto res = lagging->object(3, test::makeBuf(10));
  ASSERT_TRUE(res.hasError());
  EXPECT_EQ(res.error().code, MoQPublishError::CANCELLED);
}

// Test: Re-beginning a subgroup that only a draining subscriber lacks is
// CANCELLED, since that subscriber cannot reopen it.
TEST_F(MoQForwarderTest, DuplicateSubgroupIsNotReopenedForDrainingSubscriber) {
  auto session = createMockSession();
  auto forwarder = std::make_shared<MoQForwarder>(kFwdTestTrackName);
  auto consumer = createSessionLikeConsumer();
  EXPECT_CALL(*consumer, beginSubgroup(0, 1, _, _));
  EXPECT_CALL(*consumer, beginSubgroup(0, 0, _, _)).Times(0);

  addSubscriber(
      *forwarder,
      session,
      consumer,
      RequestID(1),
      LocationFilter::absoluteStart({0, 5}));
  ASSERT_TRUE(forwarder->beginSubgroup(0, 0, 0).hasValue());
  auto sg1 = forwarder->beginSubgroup(0, 1, 0).value();
  EXPECT_TRUE(sg1->object(5, test::makeBuf(10)).hasValue());
  forwarder->drainSubscriber(
      session->sessionId(),
      PublishDone{
          RequestID(1),
          PublishDoneStatusCode::SUBSCRIPTION_ENDED,
          0,
          "subscriber drained"},
      "test");

  auto duplicate = forwarder->beginSubgroup(0, 0, 0);
  ASSERT_TRUE(duplicate.hasError());
  EXPECT_EQ(duplicate.error().code, MoQPublishError::CANCELLED);
}

// Test: removeSubscriber immediately resets all open subgroups.
TEST_F(MoQForwarderTest, RemoveSessionResetsOpenSubgroups) {
  auto subscriber = createMockSession();

  auto forwarder = std::make_shared<MoQForwarder>(kFwdTestTrackName);

  std::array<std::shared_ptr<MockSubgroupConsumer>, 2> sgs;
  auto consumer = createMockConsumer();

  for (uint64_t i = 0; i < sgs.size(); ++i) {
    EXPECT_CALL(*consumer, beginSubgroup(i, 0, _, _))
        .WillOnce([this, i, &sgs](
                      uint64_t,
                      uint64_t,
                      uint8_t,
                      moxygen::TrackConsumer::BeginSubgroupOptions) {
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
        .WillOnce([this, i, &sgs](
                      uint64_t,
                      uint64_t,
                      uint8_t,
                      moxygen::TrackConsumer::BeginSubgroupOptions) {
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
      .WillOnce([&](uint64_t,
                    uint64_t,
                    uint8_t,
                    moxygen::TrackConsumer::BeginSubgroupOptions) {
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
        .WillOnce([this, i, &sgs](
                      uint64_t,
                      uint64_t,
                      uint8_t,
                      moxygen::TrackConsumer::BeginSubgroupOptions) {
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
      .WillOnce([this, &sgs](
                    uint64_t,
                    uint64_t,
                    uint8_t,
                    moxygen::TrackConsumer::BeginSubgroupOptions) {
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
        .WillOnce([this, i, &sgs](
                      uint64_t,
                      uint64_t,
                      uint8_t,
                      moxygen::TrackConsumer::BeginSubgroupOptions) {
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
  sub.filter = LocationFilter::absoluteStart({10, 0});

  auto subscriber =
      forwarder->addSubscriber(subscriberSession->sessionId(), sub, consumer);
  ASSERT_NE(subscriber, nullptr);

  EXPECT_EQ(subscriber->range.start, (AbsoluteLocation{10, 0}));

  SubscribeUpdate subscribeUpdate{
      RequestID(2),
      sub.requestID,
      LocationFilter::absoluteStart({5, 0}),
      kDefaultPriority,
      true};

  auto updateRes =
      folly::coro::blockingWait(subscriber->requestUpdate(subscribeUpdate));
  EXPECT_TRUE(updateRes.hasValue())
      << "RequestUpdate with decreased start should succeed";

  EXPECT_EQ(subscriber->range.start, (AbsoluteLocation{5, 0}))
      << "Start location should be updated to {5, 0}";
}

// Test: a requestUpdate whose new end is before its new start is a
// request-level error, and leaves the subscription's range untouched rather
// than half applied.
TEST_F(MoQForwarderTest, SubscribeUpdateEndBeforeStartLeavesRangeUnchanged) {
  auto forwarder = std::make_shared<MoQForwarder>(kFwdTestTrackName);
  auto consumer = createMockConsumer();

  SubscribeRequest sub;
  sub.fullTrackName = kFwdTestTrackName;
  sub.requestID = RequestID(1);
  sub.filter = LocationFilter::range({10, 0}, {20, 0});

  auto subscriber =
      forwarder->addSubscriber(createMockSession()->sessionId(), sub, consumer);
  ASSERT_NE(subscriber, nullptr);
  const auto originalRange = subscriber->range;

  SubscribeUpdate subscribeUpdate{
      RequestID(2),
      sub.requestID,
      LocationFilter::range({15, 0}, {14, 0}),
      kDefaultPriority,
      true};

  auto updateRes =
      folly::coro::blockingWait(subscriber->requestUpdate(subscribeUpdate));
  ASSERT_TRUE(updateRes.hasError());
  EXPECT_EQ(updateRes.error().errorCode, RequestErrorCode::INVALID_RANGE);

  EXPECT_EQ(subscriber->range.start, originalRange.start);
  EXPECT_EQ(subscriber->range.end, originalRange.end);
}

// A relative or Next Object start in an update resolves against Largest
// Object, like the same filter on a SUBSCRIBE.
TEST_F(MoQForwarderTest, RequestUpdateRelativeStartResolvesAgainstLargest) {
  auto forwarder = std::make_shared<MoQForwarder>(
      kFwdTestTrackName, AbsoluteLocation{10, 3});
  SubscribeRequest sub;
  sub.fullTrackName = kFwdTestTrackName;
  sub.requestID = RequestID(1);
  sub.filter = LocationFilter::range({2, 0}, {4, 0});
  auto subscriber = forwarder->addSubscriber(
      createMockSession()->sessionId(), sub, createMockConsumer());
  ASSERT_NE(subscriber, nullptr);

  RequestUpdate update;
  update.requestID = RequestID(2);
  update.existingRequestID = sub.requestID;
  update.filter = LocationFilter::relativeStart(1);
  ASSERT_TRUE(
      folly::coro::blockingWait(subscriber->requestUpdate(update)).hasValue());
  EXPECT_EQ(subscriber->range.start, (AbsoluteLocation{10, 0}));

  update.requestID = RequestID(3);
  update.filter = LocationFilter::nextObject();
  ASSERT_TRUE(
      folly::coro::blockingWait(subscriber->requestUpdate(update)).hasValue());
  EXPECT_EQ(subscriber->range.start, (AbsoluteLocation{10, 4}));

  // An absolute start stays as written, even below Largest Object.
  update.requestID = RequestID(4);
  update.filter = LocationFilter::absoluteStart({5, 1});
  ASSERT_TRUE(
      folly::coro::blockingWait(subscriber->requestUpdate(update)).hasValue());
  EXPECT_EQ(subscriber->range.start, (AbsoluteLocation{5, 1}));
}

// With no objects yet, a relative start in an update lands where the same
// filter on a SUBSCRIBE would.
TEST_F(MoQForwarderTest, RequestUpdateRelativeStartWithoutLargest) {
  auto forwarder = std::make_shared<MoQForwarder>(kFwdTestTrackName);
  SubscribeRequest sub;
  sub.fullTrackName = kFwdTestTrackName;
  sub.requestID = RequestID(1);
  sub.filter = LocationFilter::range({2, 0}, {4, 0});
  auto subscriber = forwarder->addSubscriber(
      createMockSession()->sessionId(), sub, createMockConsumer());
  ASSERT_NE(subscriber, nullptr);

  SubscribeRequest relativeSub = sub;
  relativeSub.requestID = RequestID(5);
  relativeSub.filter = LocationFilter::relativeStart(1);
  auto reference = forwarder->addSubscriber(
      createMockSession()->sessionId(), relativeSub, createMockConsumer());
  ASSERT_NE(reference, nullptr);

  RequestUpdate update;
  update.requestID = RequestID(2);
  update.existingRequestID = sub.requestID;
  update.filter = LocationFilter::relativeStart(1);
  ASSERT_TRUE(
      folly::coro::blockingWait(subscriber->requestUpdate(update)).hasValue());
  EXPECT_EQ(subscriber->range.start, reference->range.start);
  EXPECT_EQ(subscriber->range.end, reference->range.end);
}

TEST_F(
    MoQForwarderTest,
    RequestUpdateForwardResumeReopensTombstonedSubgroupDraft18) {
  auto forwarder = std::make_shared<MoQForwarder>(kFwdTestTrackName);
  auto consumer = createMockConsumer();
  auto sg1 = createMockSubgroupConsumer();
  auto sg2 = createMockSubgroupConsumer();

  expectBeginSubgroups(*consumer, sg1, sg2);
  expectStopSending(*sg1, 0);
  EXPECT_CALL(*sg2, object(1, _, _, false)).WillOnce(Return(folly::unit));

  auto subscriber = addSubscriber(
      *forwarder, createMockSession(kVersionDraft18), consumer, RequestID(1));
  ASSERT_NE(subscriber, nullptr);

  auto subgroupRes = forwarder->beginSubgroup(0, 0, 0);
  ASSERT_TRUE(subgroupRes.hasValue());
  auto subgroup = *subgroupRes;

  EXPECT_TRUE(subgroup->object(0, test::makeBuf(10)).hasValue());
  ASSERT_TRUE(pauseAndResumeForwarding(subscriber));

  EXPECT_TRUE(subgroup->object(1, test::makeBuf(10)).hasValue());
  subgroup->reset(ResetStreamErrorCode::SESSION_CLOSED);
}

TEST_F(
    MoQForwarderTest,
    RequestUpdateForwardResumeAllowsDuplicateSubgroupDraft18) {
  auto forwarder = std::make_shared<MoQForwarder>(kFwdTestTrackName);
  auto consumer = createMockConsumer();
  auto sg1 = createMockSubgroupConsumer();
  auto sg2 = createMockSubgroupConsumer();

  expectBeginSubgroups(*consumer, sg1, sg2);
  expectStopSending(*sg1, 0);

  auto subscriber = addSubscriber(
      *forwarder, createMockSession(kVersionDraft18), consumer, RequestID(1));
  ASSERT_NE(subscriber, nullptr);

  auto subgroup1Res = forwarder->beginSubgroup(0, 0, 0);
  ASSERT_TRUE(subgroup1Res.hasValue());
  auto subgroup1 = *subgroup1Res;

  EXPECT_TRUE(subgroup1->object(0, test::makeBuf(10)).hasValue());
  ASSERT_TRUE(pauseAndResumeForwarding(subscriber));

  auto subgroup2Res = forwarder->beginSubgroup(0, 0, 0);
  ASSERT_TRUE(subgroup2Res.hasValue());
  EXPECT_NE(subgroup1, *subgroup2Res);
  EXPECT_TRUE((*subgroup2Res)->endOfSubgroup().hasValue());
}

TEST_F(
    MoQForwarderTest,
    RequestUpdateForwardResumeReopensTombstonedSubgroupPreDraft18) {
  auto forwarder = std::make_shared<MoQForwarder>(kFwdTestTrackName);
  auto consumer = createMockConsumer();
  auto sg1 = createMockSubgroupConsumer();
  auto sg2 = createMockSubgroupConsumer();

  expectBeginSubgroups(*consumer, sg1, sg2);
  expectStopSending(*sg1, 0);
  EXPECT_CALL(*sg2, object(1, _, _, false)).WillOnce(Return(folly::unit));

  auto subscriber = addSubscriber(
      *forwarder, createMockSession(kVersionDraft17), consumer, RequestID(1));
  ASSERT_NE(subscriber, nullptr);

  auto subgroupRes = forwarder->beginSubgroup(0, 0, 0);
  ASSERT_TRUE(subgroupRes.hasValue());
  auto subgroup = *subgroupRes;

  EXPECT_TRUE(subgroup->object(0, test::makeBuf(10)).hasValue());
  ASSERT_TRUE(pauseAndResumeForwarding(subscriber));

  EXPECT_TRUE(subgroup->object(1, test::makeBuf(10)).hasValue());
  subgroup->reset(ResetStreamErrorCode::SESSION_CLOSED);
}

// Test: Subgroup is tombstoned after CANCELLED error (soft error), keeping the
// subscription alive for subsequent subgroups.
TEST_F(MoQForwarderTest, SubgroupTombstonedAfterCancelledError) {
  auto subscriber = createMockSession();

  auto forwarder = std::make_shared<MoQForwarder>(kFwdTestTrackName);
  auto consumer = createMockConsumer();
  std::shared_ptr<MockSubgroupConsumer> sg;

  EXPECT_CALL(*consumer, beginSubgroup(0, 0, _, _))
      .WillOnce([this, &sg](
                    uint64_t,
                    uint64_t,
                    uint8_t,
                    moxygen::TrackConsumer::BeginSubgroupOptions) {
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
      .WillOnce([this, &sg2](
                    uint64_t,
                    uint64_t,
                    uint8_t,
                    moxygen::TrackConsumer::BeginSubgroupOptions) {
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
      .WillOnce([this, &sg](
                    uint64_t,
                    uint64_t,
                    uint8_t,
                    moxygen::TrackConsumer::BeginSubgroupOptions) {
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

// Test: a subscriber that renews its interest with forward=true is already
// forwarding, so clearing its tombstones cannot wait for a false->true flip.
TEST_F(MoQForwarderTest, ForwardUpdateClearsTombstoneWhileForwarding) {
  auto subscriber = createMockSession();

  auto forwarder = std::make_shared<MoQForwarder>(kFwdTestTrackName);
  auto consumer = createMockConsumer();
  std::shared_ptr<MockSubgroupConsumer> refused;
  std::shared_ptr<MockSubgroupConsumer> renewed;

  EXPECT_CALL(*consumer, beginSubgroup(0, 0, _, _))
      .WillOnce([this, &refused](
                    uint64_t,
                    uint64_t,
                    uint8_t,
                    moxygen::TrackConsumer::BeginSubgroupOptions) {
        refused = createMockSubgroupConsumer();
        EXPECT_CALL(*refused, object(0, _, _, false))
            .WillOnce(Return(
                folly::makeUnexpected(MoQPublishError(
                    MoQPublishError::CANCELLED, "stop sending"))));
        return folly::
            makeExpected<MoQPublishError, std::shared_ptr<SubgroupConsumer>>(
                refused);
      })
      .WillOnce([this, &renewed](
                    uint64_t,
                    uint64_t,
                    uint8_t,
                    moxygen::TrackConsumer::BeginSubgroupOptions) {
        renewed = createMockSubgroupConsumer();
        EXPECT_CALL(*renewed, object(1, _, _, false))
            .WillOnce(Return(folly::unit));
        return folly::
            makeExpected<MoQPublishError, std::shared_ptr<SubgroupConsumer>>(
                renewed);
      });

  auto subHandle =
      addSubscriber(*forwarder, subscriber, consumer, RequestID(1));
  ASSERT_NE(subHandle, nullptr);

  auto subgroupRes = forwarder->beginSubgroup(0, 0, 0);
  ASSERT_TRUE(subgroupRes.hasValue());
  auto subgroup = *subgroupRes;

  EXPECT_TRUE(subgroup->object(0, test::makeBuf(10)).hasValue());
  EXPECT_TRUE(applyForwardUpdate(subHandle, RequestID(2), /*forward=*/true));

  EXPECT_TRUE(subgroup->object(1, test::makeBuf(10)).hasValue());
  EXPECT_NE(renewed, nullptr);

  subgroup->reset(ResetStreamErrorCode::SESSION_CLOSED);
}

// Test: A subscriber whose only subgroup was tombstoned is removed by
// publishDone.  The subgroup's stream is already gone, so nothing else would
// ever close it and the subscriber would drain forever.
TEST_F(MoQForwarderTest, DrainingSubscriberRemovedWhenOnlyTombstonesRemain) {
  auto subscriber = createMockSession();

  auto forwarder = std::make_shared<MoQForwarder>(kFwdTestTrackName);
  auto consumer = createMockConsumer();
  std::shared_ptr<MockSubgroupConsumer> sg;

  EXPECT_CALL(*consumer, beginSubgroup(0, 0, _, _))
      .WillOnce([this, &sg](
                    uint64_t,
                    uint64_t,
                    uint8_t,
                    moxygen::TrackConsumer::BeginSubgroupOptions) {
        sg = createMockSubgroupConsumer();
        EXPECT_CALL(*sg, object(0, _, _, false))
            .WillOnce(Return(
                folly::makeUnexpected(MoQPublishError(
                    MoQPublishError::CANCELLED, "delivery timeout"))));
        return subgroupConsumerResult(sg);
      });

  addSubscriber(*forwarder, subscriber, consumer, RequestID(1));

  auto subgroup = forwarder->beginSubgroup(0, 0, 0).value();
  // The delivery timeout tombstones the subgroup, then the next object finds
  // no live consumer and drops the SubgroupForwarder.
  EXPECT_TRUE(subgroup->object(0, test::makeBuf(10)).hasValue());
  auto res = subgroup->object(1, test::makeBuf(10));
  ASSERT_TRUE(res.hasError());
  EXPECT_EQ(res.error().code, MoQPublishError::CANCELLED);

  EXPECT_CALL(*consumer, publishDone(_))
      .WillOnce(Return(folly::makeExpected<MoQPublishError>(folly::unit)));
  EXPECT_CALL(*sg, reset(_)).Times(0);

  forwarder->publishDone(
      PublishDone{
          RequestID(0),
          PublishDoneStatusCode::TRACK_ENDED,
          0,
          "publisher ended"});

  EXPECT_TRUE(forwarder->empty());
}

// Test: Tombstoning the last open subgroup of an already-draining subscriber
// removes it.
TEST_F(MoQForwarderTest, TombstoneAfterPublishDoneRemovesDrainingSubscriber) {
  auto subscriber = createMockSession();

  auto forwarder = std::make_shared<MoQForwarder>(kFwdTestTrackName);
  auto consumer = createMockConsumer();
  std::shared_ptr<MockSubgroupConsumer> sg;

  EXPECT_CALL(*consumer, beginSubgroup(0, 0, _, _))
      .WillOnce([this, &sg](
                    uint64_t,
                    uint64_t,
                    uint8_t,
                    moxygen::TrackConsumer::BeginSubgroupOptions) {
        sg = createMockSubgroupConsumer();
        EXPECT_CALL(*sg, objectPayload(_, false))
            .WillOnce(Return(
                folly::makeUnexpected(MoQPublishError(
                    MoQPublishError::CANCELLED, "delivery timeout"))));
        return subgroupConsumerResult(sg);
      });

  addSubscriber(*forwarder, subscriber, consumer, RequestID(1));

  auto subgroup = forwarder->beginSubgroup(0, 0, 0).value();
  EXPECT_TRUE(subgroup->beginObject(0, 10, 0).hasValue());

  EXPECT_CALL(*consumer, publishDone(_))
      .WillOnce(Return(folly::makeExpected<MoQPublishError>(folly::unit)));

  forwarder->publishDone(
      PublishDone{
          RequestID(0),
          PublishDoneStatusCode::TRACK_ENDED,
          0,
          "publisher ended"});
  EXPECT_FALSE(forwarder->empty());

  // Losing the last subscriber propagates CANCELLED back to the publisher.
  auto res = subgroup->objectPayload(test::makeBuf(5), false);
  ASSERT_TRUE(res.hasError());
  EXPECT_EQ(res.error().code, MoQPublishError::CANCELLED);
  EXPECT_TRUE(forwarder->empty());
}

// Test: A soft error on an object that also finishes the subgroup leaves no
// tombstone behind.  Nothing reopens a finished subgroup to clear it later, so
// one left here would outlive the subgroup for the life of the subscription.
TEST_F(MoQForwarderTest, TombstoneClearedWhenSubgroupFinishes) {
  auto subscriber = createMockSession();

  auto forwarder = std::make_shared<MoQForwarder>(kFwdTestTrackName);
  auto consumer = createMockConsumer();
  std::shared_ptr<MockSubgroupConsumer> sg;

  EXPECT_CALL(*consumer, beginSubgroup(0, 0, _, _))
      .WillOnce([this, &sg](
                    uint64_t,
                    uint64_t,
                    uint8_t,
                    moxygen::TrackConsumer::BeginSubgroupOptions) {
        sg = createMockSubgroupConsumer();
        EXPECT_CALL(*sg, object(0, _, _, true))
            .WillOnce(Return(
                folly::makeUnexpected(MoQPublishError(
                    MoQPublishError::CANCELLED, "delivery timeout"))));
        return subgroupConsumerResult(sg);
      });

  auto sub = addSubscriber(*forwarder, subscriber, consumer, RequestID(1));
  ASSERT_NE(sub, nullptr);

  auto subgroup = forwarder->beginSubgroup(0, 0, 0).value();
  subgroup->object(0, test::makeBuf(10), noExtensions(), /*finSubgroup=*/true);

  EXPECT_TRUE(sub->subgroups.empty());
  EXPECT_TRUE(sub->tombstonedSubgroups.empty());
}

// Test: onEmpty fires exactly once when beginSubgroup loses its last
// subscriber.  The new subgroup is not in subgroups_ yet, so the removal
// inside the loop is already what empties the forwarder.
TEST_F(MoQForwarderTest, BeginSubgroupErrorFiresOnEmptyOnce) {
  auto subscriber = createMockSession();

  auto forwarder = std::make_shared<MoQForwarder>(kFwdTestTrackName);
  auto callback = std::make_shared<CountingCallback>();
  forwarder->setCallback(callback);
  auto consumer = createMockConsumer();

  EXPECT_CALL(*consumer, beginSubgroup(0, 0, _, _))
      .WillOnce(Return(
          folly::makeUnexpected(MoQPublishError(
              MoQPublishError::WRITE_ERROR, "transport broken"))));
  EXPECT_CALL(*consumer, publishDone(_))
      .WillOnce(Return(folly::makeExpected<MoQPublishError>(folly::unit)));

  addSubscriber(*forwarder, subscriber, consumer, RequestID(1));

  auto res = forwarder->beginSubgroup(0, 0, 0);
  ASSERT_TRUE(res.hasError());
  EXPECT_EQ(res.error().code, MoQPublishError::CANCELLED);
  EXPECT_TRUE(forwarder->empty());
  EXPECT_EQ(callback->onEmptyCount, 1);
}

// Test: A draining subscriber with both a tombstoned and a still-open subgroup
// survives until the open one closes.
TEST_F(MoQForwarderTest, DrainingSubscriberWaitsForNonTombstonedSubgroup) {
  auto subscriber = createMockSession();

  auto forwarder = std::make_shared<MoQForwarder>(kFwdTestTrackName);
  auto consumer = createMockConsumer();
  std::array<std::shared_ptr<MockSubgroupConsumer>, 2> sgs;

  EXPECT_CALL(*consumer, beginSubgroup(0, 0, _, _))
      .WillOnce([this, &sgs](
                    uint64_t,
                    uint64_t,
                    uint8_t,
                    moxygen::TrackConsumer::BeginSubgroupOptions) {
        sgs[0] = createMockSubgroupConsumer();
        EXPECT_CALL(*sgs[0], object(0, _, _, false))
            .WillOnce(Return(
                folly::makeUnexpected(MoQPublishError(
                    MoQPublishError::CANCELLED, "delivery timeout"))));
        return subgroupConsumerResult(sgs[0]);
      });
  EXPECT_CALL(*consumer, beginSubgroup(1, 0, _, _))
      .WillOnce([this, &sgs](
                    uint64_t,
                    uint64_t,
                    uint8_t,
                    moxygen::TrackConsumer::BeginSubgroupOptions) {
        sgs[1] = createMockSubgroupConsumer();
        EXPECT_CALL(*sgs[1], endOfSubgroup()).WillOnce(Return(folly::unit));
        return subgroupConsumerResult(sgs[1]);
      });

  auto sub = addSubscriber(*forwarder, subscriber, consumer, RequestID(1));
  ASSERT_NE(sub, nullptr);

  auto subgroup0 = forwarder->beginSubgroup(0, 0, 0).value();
  auto subgroup1 = forwarder->beginSubgroup(1, 0, 0).value();
  EXPECT_TRUE(subgroup0->object(0, test::makeBuf(10)).hasValue());
  EXPECT_EQ(sub->tombstonedSubgroups.size(), 1);

  EXPECT_CALL(*consumer, publishDone(_))
      .WillOnce(Return(folly::makeExpected<MoQPublishError>(folly::unit)));
  forwarder->publishDone(
      PublishDone{
          RequestID(0),
          PublishDoneStatusCode::TRACK_ENDED,
          0,
          "publisher ended"});
  // The tombstone must not retire the subscriber while group 1 is still open.
  EXPECT_FALSE(forwarder->empty());

  EXPECT_TRUE(subgroup1->endOfSubgroup().hasValue());
  EXPECT_TRUE(forwarder->empty());
}

// Test: Tombstoning one subscriber mid-iteration does not disturb the others.
// The forwarder removes subscribers from inside its own subscriber loop, so
// this pins the iteration against that.
TEST_F(MoQForwarderTest, TombstoneDuringDrainLeavesOtherSubscribersIntact) {
  std::array<std::shared_ptr<MoQSession>, 3> subscribers;
  std::array<std::shared_ptr<MockTrackConsumer>, 3> consumers;
  std::array<std::shared_ptr<MockSubgroupConsumer>, 3> sgs;
  for (size_t i = 0; i < 3; ++i) {
    subscribers[i] = createMockSession();
    consumers[i] = createMockConsumer();
  }

  auto forwarder = std::make_shared<MoQForwarder>(kFwdTestTrackName);

  for (size_t i = 0; i < 3; ++i) {
    EXPECT_CALL(*consumers[i], beginSubgroup(0, 0, _, _))
        .WillOnce([this, i, &sgs](
                      uint64_t,
                      uint64_t,
                      uint8_t,
                      moxygen::TrackConsumer::BeginSubgroupOptions) {
          sgs[i] = createMockSubgroupConsumer();
          if (i == 1) {
            EXPECT_CALL(*sgs[i], object(0, _, _, false))
                .WillOnce(Return(
                    folly::makeUnexpected(MoQPublishError(
                        MoQPublishError::CANCELLED, "delivery timeout"))));
          } else {
            EXPECT_CALL(*sgs[i], object(0, _, _, false))
                .WillOnce(Return(folly::unit));
            EXPECT_CALL(*sgs[i], endOfSubgroup()).WillOnce(Return(folly::unit));
          }
          return subgroupConsumerResult(sgs[i]);
        });
  }

  std::array<std::shared_ptr<MoQForwarder::Subscriber>, 3> subs;
  for (size_t i = 0; i < 3; ++i) {
    subs[i] = addSubscriber(
        *forwarder, subscribers[i], consumers[i], RequestID(i + 1));
    ASSERT_NE(subs[i], nullptr);
  }

  auto subgroup = forwarder->beginSubgroup(0, 0, 0).value();

  // Subscriber 1 tombstones; the other two keep the subgroup open.
  EXPECT_TRUE(subgroup->object(0, test::makeBuf(10)).hasValue());
  EXPECT_EQ(subs[1]->tombstonedSubgroups.size(), 1);
  EXPECT_TRUE(subs[0]->subgroups.count({0, 0}));
  EXPECT_TRUE(subs[2]->subgroups.count({0, 0}));

  for (auto& consumer : consumers) {
    EXPECT_CALL(*consumer, publishDone(_))
        .WillOnce(Return(folly::makeExpected<MoQPublishError>(folly::unit)));
  }
  forwarder->publishDone(
      PublishDone{
          RequestID(0),
          PublishDoneStatusCode::TRACK_ENDED,
          0,
          "publisher ended"});

  // Only the tombstoned subscriber is retired by the drain; detach() nulls
  // forwarder on exactly the one that was removed.
  EXPECT_FALSE(forwarder->empty());
  EXPECT_EQ(subs[1]->forwarder, nullptr);
  EXPECT_NE(subs[0]->forwarder, nullptr);
  EXPECT_NE(subs[2]->forwarder, nullptr);

  EXPECT_TRUE(subgroup->endOfSubgroup().hasValue());
  EXPECT_TRUE(forwarder->empty());
}

// Test: A duplicate subgroup resets the subscriber's open stream for it.  If
// that was the last stream a draining subscriber was waiting on, and the
// subgroup can't be reopened, the subscriber has to be retired.
TEST_F(MoQForwarderTest, DuplicateSubgroupRetiresDrainingSubscriber) {
  auto subscriber = createMockSession();

  auto forwarder = std::make_shared<MoQForwarder>(kFwdTestTrackName);
  auto consumer = createMockConsumer();
  std::shared_ptr<MockSubgroupConsumer> sg;

  EXPECT_CALL(*consumer, beginSubgroup(0, 0, _, _))
      .WillOnce([this, &sg](
                    uint64_t,
                    uint64_t,
                    uint8_t,
                    moxygen::TrackConsumer::BeginSubgroupOptions) {
        sg = createMockSubgroupConsumer();
        return subgroupConsumerResult(sg);
      });

  auto callback = std::make_shared<CountingCallback>();
  forwarder->setCallback(callback);

  auto sub = addSubscriber(*forwarder, subscriber, consumer, RequestID(1));
  ASSERT_NE(sub, nullptr);
  forwarder->beginSubgroup(0, 0, 0).value();

  EXPECT_CALL(*consumer, publishDone(_))
      .WillOnce(Return(folly::makeExpected<MoQPublishError>(folly::unit)));
  forwarder->publishDone(
      PublishDone{
          RequestID(0),
          PublishDoneStatusCode::TRACK_ENDED,
          0,
          "publisher ended"});
  EXPECT_FALSE(forwarder->empty());

  // forward=0 stops the duplicate from being reopened for this subscriber.
  ASSERT_TRUE(applyForwardUpdate(sub, RequestID(2), false));

  EXPECT_CALL(*sg, reset(ResetStreamErrorCode::CANCELLED));
  auto res = forwarder->beginSubgroup(0, 0, 0);

  ASSERT_TRUE(res.hasError());
  EXPECT_EQ(res.error().code, MoQPublishError::CANCELLED);
  EXPECT_TRUE(forwarder->empty());
  // Retiring the subscriber here is the one case where nothing downstream
  // fires onEmpty, so the duplicate path has to do it itself.
  EXPECT_EQ(callback->onEmptyCount, 1);
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
      .WillOnce([this, &sg1](
                    uint64_t,
                    uint64_t,
                    uint8_t,
                    moxygen::TrackConsumer::BeginSubgroupOptions) {
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
      .WillOnce([this, &sg2](
                    uint64_t,
                    uint64_t,
                    uint8_t,
                    moxygen::TrackConsumer::BeginSubgroupOptions) {
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
      .WillOnce([this, &sg](
                    uint64_t,
                    uint64_t,
                    uint8_t,
                    moxygen::TrackConsumer::BeginSubgroupOptions) {
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
      .WillOnce([this, &sg](
                    uint64_t,
                    uint64_t,
                    uint8_t,
                    moxygen::TrackConsumer::BeginSubgroupOptions) {
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
      .WillOnce([this, &sg](
                    uint64_t,
                    uint64_t,
                    uint8_t,
                    moxygen::TrackConsumer::BeginSubgroupOptions) {
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
  sub.filter = LocationFilter::relativeStart(1);
  forwarder->addSubscriber(session->sessionId(), sub, consumer);

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
      .WillOnce([this, &sg1](
                    uint64_t,
                    uint64_t,
                    uint8_t,
                    moxygen::TrackConsumer::BeginSubgroupOptions) {
        sg1 = createMockSubgroupConsumer();
        return folly::
            makeExpected<MoQPublishError, std::shared_ptr<SubgroupConsumer>>(
                sg1);
      });

  EXPECT_CALL(*consumer2, beginSubgroup(0, 0, _, _))
      .WillOnce([this, &sg2](
                    uint64_t,
                    uint64_t,
                    uint8_t,
                    moxygen::TrackConsumer::BeginSubgroupOptions) {
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
  sub1.filter = LocationFilter::relativeStart(1);
  forwarder->addSubscriber(session1->sessionId(), sub1, consumer1);

  SubscribeRequest sub2;
  sub2.fullTrackName = kFwdTestTrackName;
  sub2.requestID = RequestID(2);
  sub2.filter = LocationFilter::relativeStart(1);
  forwarder->addSubscriber(session2->sessionId(), sub2, consumer2);

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

// Test: A hard subgroup error removes the last subscriber, which removes the
// subgroup, which fires onEmpty and destroys the forwarder.  An upstream owner
// still holding the SubgroupForwarder (e.g. MoQCache's writeback) then delivers
// the terminal reset() - that must not touch the destroyed forwarder.
TEST_F(MoQForwarderTest, SubgroupDetachedWhenErrorDestroysForwarder) {
  auto session = createMockSession();

  auto forwarder = std::make_shared<MoQForwarder>(kFwdTestTrackName);
  auto callback = std::make_shared<ForwarderDestroyingCallback>(forwarder);
  forwarder->setCallback(callback);

  auto consumer = createMockConsumer();
  auto sg = createMockSubgroupConsumer();
  EXPECT_CALL(*consumer, beginSubgroup(0, 0, _, _))
      .WillOnce(Return(subgroupConsumerResult(sg)));
  EXPECT_CALL(*consumer, publishDone(_))
      .WillOnce(Return(folly::makeExpected<MoQPublishError>(folly::unit)));
  EXPECT_CALL(*sg, object(0, _, _, false))
      .WillOnce(Return(
          folly::makeUnexpected(MoQPublishError(
              MoQPublishError::WRITE_ERROR,
              "Write after stream complete or reset"))));

  addSubscriber(*forwarder, session, consumer, RequestID(1));

  auto subgroupRes = forwarder->beginSubgroup(0, 0, 0);
  ASSERT_TRUE(subgroupRes.hasValue());
  auto subgroup = *subgroupRes;

  // The hard error tears down the subscriber and then the whole forwarder.
  EXPECT_CALL(*sg, reset(_)).Times(1);
  EXPECT_TRUE(subgroup->object(0, nullptr, noExtensions(), false).hasError());
  ASSERT_EQ(forwarder, nullptr);

  // The subgroup outlived the forwarder, so it must be detached.
  auto objRes = subgroup->object(1, nullptr, noExtensions(), false);
  ASSERT_TRUE(objRes.hasError());
  EXPECT_EQ(objRes.error().msg, "Forwarder detached");
  subgroup->reset(ResetStreamErrorCode::CANCELLED);
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
  sub1.filter = LocationFilter::relativeStart(1);
  auto subscriber1 =
      forwarder->addSubscriber(session1->sessionId(), sub1, consumer1);
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
  sub2.filter = LocationFilter::relativeStart(1);
  auto subscriber2 =
      forwarder->addSubscriber(session2->sessionId(), sub2, consumer2);
  ASSERT_NE(subscriber2, nullptr);

  EXPECT_EQ(
      subscriber2->subscribeOk().extensions.getIntExtension(
          kDeliveryTimeoutExtensionType),
      7000);
  EXPECT_EQ(
      subscriber2->subscribeOk().extensions.getIntExtension(0xDEAD'0000), 123);

  auto session3 = createMockSession();
  auto subscriber3 =
      forwarder->addSubscriber(session3->sessionId(), /*forward=*/false);
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

  auto subscriber =
      forwarder->addSubscriber(session->sessionId(), /*forward=*/true);
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

  auto subscriber =
      forwarder->addSubscriber(session->sessionId(), /*forward=*/true);
  ASSERT_NE(subscriber, nullptr);
  EXPECT_EQ(subscriber->trackConsumer, nullptr);

  forwarder->removeSubscriber(
      session->sessionId(),
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
      .WillOnce([&](uint64_t,
                    uint64_t,
                    uint8_t,
                    moxygen::TrackConsumer::BeginSubgroupOptions) {
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
      LocationFilter::nextObject(),
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

// Test: Destroying the forwarder detaches all subscribers; subsequent
// unsubscribe / requestUpdate calls on the surviving Subscriber handle must
// not crash.
TEST_F(MoQForwarderTest, SubscriberDetachedOnForwarderDestruction) {
  auto session = createMockSession();
  auto consumer = createMockConsumer();

  auto forwarder = std::make_shared<MoQForwarder>(kFwdTestTrackName);
  auto subscriber = addSubscriber(*forwarder, session, consumer, RequestID(1));
  ASSERT_NE(subscriber, nullptr);
  ASSERT_NE(subscriber->forwarder, nullptr);

  forwarder.reset();
  EXPECT_EQ(subscriber->forwarder, nullptr);

  // unsubscribe is a no-op after detach.
  subscriber->unsubscribe();

  // requestUpdate succeeds and updates local range without touching forwarder.
  // The forward-state update is silently skipped after detach.
  RequestUpdate upd;
  upd.requestID = RequestID(1);
  upd.filter = LocationFilter::absoluteStart({5, 0});
  upd.forward = false;
  auto result =
      folly::coro::blockingWait(subscriber->requestUpdate(std::move(upd)));
  ASSERT_TRUE(result.hasValue());
  EXPECT_EQ(subscriber->range.start.group, 5u);

  // onPublishOk is a no-op after detach.
  PublishOk pubOk{
      RequestID(1),
      true,
      0,
      GroupOrder::OldestFirst,
      LocationFilter::nextObject(),
      TrackRequestParameters(FrameType::PUBLISH_OK)};
  subscriber->onPublishOk(pubOk);
}

// Test: destroying the forwarder while a subgroup is still open resets the
// downstream subgroup consumer. The SubgroupForwarder must reset open consumers
// during detach(), while subscribers are still reachable: once forwarder_ is
// null, forEachSubscriberSubgroup bails and any later reset() is lost, leaving
// the consumer without its required terminal.
TEST_F(MoQForwarderTest, SubgroupConsumersResetOnForwarderDestruction) {
  auto session = createMockSession();
  auto consumer = createMockConsumer();

  std::shared_ptr<MockSubgroupConsumer> sg;
  EXPECT_CALL(*consumer, beginSubgroup(0, 0, _, _))
      .WillOnce([this, &sg](
                    uint64_t,
                    uint64_t,
                    uint8_t,
                    moxygen::TrackConsumer::BeginSubgroupOptions) {
        sg = createMockSubgroupConsumer();
        return folly::
            makeExpected<MoQPublishError, std::shared_ptr<SubgroupConsumer>>(
                sg);
      });

  auto forwarder = std::make_shared<MoQForwarder>(kFwdTestTrackName);
  auto subscriber = addSubscriber(*forwarder, session, consumer, RequestID(1));
  ASSERT_NE(subscriber, nullptr);

  // Open a subgroup, leaving the downstream consumer attached to the
  // subscriber. The publisher keeps its handle to the SubgroupForwarder.
  auto sgRes = forwarder->beginSubgroup(0, 0, 0);
  ASSERT_TRUE(sgRes.hasValue());
  auto pubSg = *sgRes;
  ASSERT_NE(sg, nullptr);

  // Destroying the forwarder detaches the open subgroup; the still-attached
  // downstream consumer must be reset so its stream is properly terminated.
  EXPECT_CALL(*sg, reset(ResetStreamErrorCode::SESSION_CLOSED));
  forwarder.reset();

  // Resetting the publisher's now-detached handle is a safe no-op (the consumer
  // was already reset during detach, so no second reset is delivered).
  pubSg->reset(ResetStreamErrorCode::SESSION_CLOSED);
}

// Test: A Joining Fetch is only permitted when the associated subscription is
// in Forward State 1. Against a non-forwarding subscription it must be rejected
// with a request-level INVALID_RANGE error rather than a session error.
TEST_F(MoQForwarderTest, JoiningFetchNonForwardingSubscriptionIsRequestError) {
  auto forwarder = std::make_shared<MoQForwarder>(
      kFwdTestTrackName, AbsoluteLocation{10, 5});
  auto session = createMockSession();

  SubscribeRequest sub;
  sub.fullTrackName = kFwdTestTrackName;
  sub.requestID = RequestID(2);
  sub.filter = LocationFilter::nextObject();
  sub.forward = false;
  auto subscriber =
      forwarder->addSubscriber(session->sessionId(), sub, createMockConsumer());
  ASSERT_NE(subscriber, nullptr);
  ASSERT_FALSE(subscriber->shouldForward);

  JoiningFetch joining(RequestID(2), 2, FetchType::RELATIVE_JOINING);
  auto res = forwarder->resolveJoiningFetch(session->sessionId(), joining);
  ASSERT_TRUE(res.hasError());
  EXPECT_EQ(res.error().errorCode, FetchErrorCode::INVALID_RANGE);
}

// Test: the Joining Location follows the last Forward State 0 -> 1 transition,
// so a subscription that pauses while the track advances rejoins at the point
// where delivery resumes. REQUEST_UPDATE_OK carries the same Location.
TEST_F(MoQForwarderTest, JoiningFetchAfterResumeAnchorsAtResumeLargest) {
  auto forwarder = std::make_shared<MoQForwarder>(
      kFwdTestTrackName, AbsoluteLocation{10, 5});
  auto session = createMockSession();

  SubscribeRequest sub;
  sub.fullTrackName = kFwdTestTrackName;
  sub.requestID = RequestID(1);
  sub.filter = LocationFilter::nextObject();
  sub.forward = true;
  auto subscriber =
      forwarder->addSubscriber(session->sessionId(), sub, createMockConsumer());
  ASSERT_NE(subscriber, nullptr);

  ASSERT_TRUE(applyForwardUpdate(subscriber, RequestID(2), false));
  forwarder->updateLargest(14, 2);

  RequestUpdate resume;
  resume.requestID = RequestID(3);
  resume.existingRequestID = RequestID(1);
  resume.forward = true;
  auto ok = folly::coro::blockingWait(subscriber->requestUpdate(resume));
  ASSERT_TRUE(ok.hasValue());
  ASSERT_EQ(ok->requestSpecificParams.size(), 1);
  EXPECT_EQ(
      ok->requestSpecificParams[0].key,
      folly::to_underlying(TrackRequestParamKey::LARGEST_OBJECT));
  EXPECT_EQ(
      ok->requestSpecificParams[0].largestObject, (AbsoluteLocation{14, 2}));

  JoiningFetch joining(RequestID(1), 2, FetchType::RELATIVE_JOINING);
  auto res = forwarder->resolveJoiningFetch(session->sessionId(), joining);
  ASSERT_TRUE(res.hasValue());
  EXPECT_EQ(res->start, (AbsoluteLocation{12, 0}));
  EXPECT_EQ(res->end, (AbsoluteLocation{14, 3}));
}

// Test: a subscription established by PUBLISH with Forward State 1 joins at
// the Largest that the PUBLISH carried, even after the track moves past it.
TEST_F(MoQForwarderTest, JoiningFetchOnPublishUsesPublishedLargest) {
  auto forwarder = std::make_shared<MoQForwarder>(
      kFwdTestTrackName, AbsoluteLocation{10, 5});
  auto session = createMockSession();

  auto subscriber =
      forwarder->addSubscriber(session->sessionId(), /*forward=*/true);
  ASSERT_NE(subscriber, nullptr);
  EXPECT_EQ(subscriber->getPublishRequest().largest, (AbsoluteLocation{10, 5}));

  forwarder->updateLargest(12, 0);

  JoiningFetch joining(RequestID(0), 0, FetchType::RELATIVE_JOINING);
  auto res = forwarder->resolveJoiningFetch(session->sessionId(), joining);
  ASSERT_TRUE(res.hasValue());
  EXPECT_EQ(res->start, (AbsoluteLocation{10, 0}));
  EXPECT_EQ(res->end, (AbsoluteLocation{10, 6}));
}

// Test: a hard error removes the only subscriber while forEachSubscriber is
// still iterating. With no open subgroups the forwarder is now empty, so
// onEmpty fires and drops the last reference to it. forEachSubscriber reads
// subscribers_ after the callback returns, which is a use-after-free unless
// onEmpty is deferred until the operation unwinds.
TEST_F(MoQForwarderTest, LastSubscriberRemovedDuringObjectStreamIteration) {
  auto session = createMockSession();

  auto forwarder = std::make_shared<MoQForwarder>(kFwdTestTrackName);
  auto callback = std::make_shared<ForwarderDestroyingCallback>(forwarder);
  forwarder->setCallback(callback);

  auto consumer = createMockConsumer();
  EXPECT_CALL(*consumer, objectStream(_, _, _))
      .WillOnce(Return(
          folly::makeUnexpected(MoQPublishError(
              MoQPublishError::WRITE_ERROR, "Write after stream complete"))));
  EXPECT_CALL(*consumer, publishDone(_))
      .WillOnce(Return(folly::makeExpected<MoQPublishError>(folly::unit)));

  addSubscriber(*forwarder, session, consumer, RequestID(1));

  auto res = forwarder->objectStream(
      ObjectHeader(0, 0, 0, 0, uint64_t(10)), test::makeBuf(10));
  EXPECT_TRUE(res.hasError());

  EXPECT_EQ(forwarder, nullptr);
}

// The deprecated session overloads are kept for out-of-tree callers during the
// transition. They must reach the same subscriber as the SessionId ones.
TEST_F(MoQForwarderTest, DeprecatedSessionOverloadsResolveToTheSameSubscriber) {
  auto forwarder = std::make_shared<MoQForwarder>(kFwdTestTrackName);
  auto session = createMockSession();

  SubscribeRequest sub;
  sub.fullTrackName = kFwdTestTrackName;
  sub.requestID = RequestID(1);
  sub.filter = LocationFilter::relativeStart(1);
  auto viaSession =
      forwarder->addSubscriber(session, sub, createMockConsumer());
  ASSERT_NE(viaSession, nullptr);
  EXPECT_EQ(viaSession->sessionId, session->sessionId());

  // The SessionId overload finds the entry the session overload added.
  auto viaId =
      forwarder->addSubscriber(session->sessionId(), sub, createMockConsumer());
  EXPECT_EQ(viaId.get(), viaSession.get());

  forwarder->removeSubscriber(session, std::nullopt, "test");
  EXPECT_TRUE(forwarder->empty());
}

namespace {

std::shared_ptr<MockSubgroupConsumer> acceptingSubgroupConsumer() {
  auto sg = std::make_shared<NiceMock<MockSubgroupConsumer>>();
  ON_CALL(*sg, object(_, _, _, _))
      .WillByDefault(Return(folly::makeExpected<MoQPublishError>(folly::unit)));
  ON_CALL(*sg, beginObject(_, _, _, _))
      .WillByDefault(Return(folly::makeExpected<MoQPublishError>(folly::unit)));
  ON_CALL(*sg, objectPayload(_, _))
      .WillByDefault(Return(
          folly::makeExpected<MoQPublishError>(ObjectPublishStatus::DONE)));
  ON_CALL(*sg, endOfSubgroup())
      .WillByDefault(Return(folly::makeExpected<MoQPublishError>(folly::unit)));
  ON_CALL(*sg, endOfGroup(_))
      .WillByDefault(Return(folly::makeExpected<MoQPublishError>(folly::unit)));
  return sg;
}

} // namespace

// Test: once a subscriber's filter starts at (43,3), a late object from an
// older group stays off its live subscription even after the track's Largest
// has passed the filter start.
TEST_F(MoQForwarderTest, LateOlderGroupObjectNotForwardedAfterLargestAdvances) {
  auto forwarder = std::make_shared<MoQForwarder>(kFwdTestTrackName);
  // The early subscriber joins before any data, so it receives everything.
  auto earlyConsumer = createMockConsumer();
  EXPECT_CALL(*earlyConsumer, beginSubgroup(_, _, _, _))
      .WillRepeatedly(
          Return(subgroupConsumerResult(acceptingSubgroupConsumer())));
  ASSERT_NE(
      addSubscriber(
          *forwarder, createMockSession(), earlyConsumer, RequestID(1)),
      nullptr);
  auto olderSubgroup = forwarder->beginSubgroup(42, 1, 0);
  ASSERT_TRUE(olderSubgroup.hasValue());
  auto currentSubgroup = forwarder->beginSubgroup(43, 0, 0);
  ASSERT_TRUE(currentSubgroup.hasValue());
  for (uint64_t objectID = 0; objectID < 3; objectID++) {
    EXPECT_TRUE(
        (*currentSubgroup)->object(objectID, test::makeBuf(10)).hasValue());
  }

  // Joins with Largest (43,2): its live range starts at (43,3).
  auto lateConsumer = createMockConsumer();
  auto lateSubgroup = acceptingSubgroupConsumer();
  EXPECT_CALL(*lateConsumer, beginSubgroup(42, _, _, _)).Times(0);
  EXPECT_CALL(*lateConsumer, beginSubgroup(43, 0, _, _))
      .WillOnce(Return(subgroupConsumerResult(lateSubgroup)));
  EXPECT_CALL(*lateSubgroup, object(3, _, _, _));
  ASSERT_NE(
      addSubscriber(
          *forwarder, createMockSession(), lateConsumer, RequestID(2)),
      nullptr);

  EXPECT_TRUE((*currentSubgroup)->object(3, test::makeBuf(10)).hasValue());
  EXPECT_TRUE((*olderSubgroup)->object(31, test::makeBuf(10)).hasValue());
}

// Test: a stream that only carries objects below a subscriber's start in that
// group opens for it at the first admitted object, which is not the stream's
// first object. The upstream stream must survive the excluded objects.
TEST_F(MoQForwarderTest, SubgroupInStartGroupOpensAtFirstAdmittedObject) {
  auto forwarder = std::make_shared<MoQForwarder>(
      kFwdTestTrackName, AbsoluteLocation{43, 2});
  auto consumer = createMockConsumer();
  auto subgroup = acceptingSubgroupConsumer();
  TrackConsumer::BeginSubgroupOptions opened;
  EXPECT_CALL(*consumer, beginSubgroup(43, 1, _, _))
      .WillOnce([&](uint64_t,
                    uint64_t,
                    uint8_t,
                    TrackConsumer::BeginSubgroupOptions options) {
        opened = options;
        return subgroupConsumerResult(subgroup);
      });
  EXPECT_CALL(*subgroup, object(1, _, _, _)).Times(0);
  EXPECT_CALL(*subgroup, object(5, _, _, _));
  ASSERT_NE(
      addSubscriber(*forwarder, createMockSession(), consumer, RequestID(1)),
      nullptr);

  TrackConsumer::BeginSubgroupOptions publishOptions;
  publishOptions.beginsWithFirstObject = true;
  auto sgRes = forwarder->beginSubgroup(43, 1, 0, publishOptions);
  ASSERT_TRUE(sgRes.hasValue());
  EXPECT_TRUE((*sgRes)->object(1, test::makeBuf(10)).hasValue());
  EXPECT_TRUE((*sgRes)->object(5, test::makeBuf(10)).hasValue());

  EXPECT_FALSE(opened.beginsWithFirstObject);
}

// Test: a range of one later object in the group keeps the subgroup open
// through the objects before it.
TEST_F(MoQForwarderTest, SingleObjectRangeKeepsSubgroupOpen) {
  auto forwarder = std::make_shared<MoQForwarder>(
      kFwdTestTrackName, AbsoluteLocation{43, 2});
  auto consumer = createMockConsumer();
  auto subgroup = acceptingSubgroupConsumer();
  EXPECT_CALL(*consumer, beginSubgroup(43, 1, _, _))
      .WillOnce(Return(subgroupConsumerResult(subgroup)));
  EXPECT_CALL(*subgroup, object(5, _, _, _));
  ASSERT_NE(
      addSubscriber(
          *forwarder,
          createMockSession(),
          consumer,
          RequestID(1),
          LocationFilter::range({43, 5}, {43, 5})),
      nullptr);

  auto sgRes = forwarder->beginSubgroup(43, 1, 0);
  ASSERT_TRUE(sgRes.hasValue());
  EXPECT_TRUE((*sgRes)->object(1, test::makeBuf(10)).hasValue());
  EXPECT_TRUE((*sgRes)->object(5, test::makeBuf(10)).hasValue());
}

// Test: datagrams and single-object streams are filtered by their own
// location, not by the track-wide Largest.
TEST_F(MoQForwarderTest, DatagramAndObjectStreamFilteredByObjectLocation) {
  auto forwarder = std::make_shared<MoQForwarder>(
      kFwdTestTrackName, AbsoluteLocation{43, 2});
  auto consumer = createMockConsumer();
  EXPECT_CALL(*consumer, datagram(_, _, _))
      .WillOnce([](const ObjectHeader& header, Payload, bool) {
        EXPECT_EQ(header.group, 43);
        EXPECT_EQ(header.id, 4);
        return folly::makeExpected<MoQPublishError>(folly::unit);
      });
  EXPECT_CALL(*consumer, objectStream(_, _, _)).Times(0);
  ASSERT_NE(
      addSubscriber(*forwarder, createMockSession(), consumer, RequestID(1)),
      nullptr);

  EXPECT_TRUE(forwarder
                  ->datagram(
                      ObjectHeader(43, 0, 4, 0, uint64_t(1)),
                      folly::IOBuf::copyBuffer("x"))
                  .hasValue());
  EXPECT_TRUE(forwarder
                  ->datagram(
                      ObjectHeader(42, 0, 31, 0, uint64_t(1)),
                      folly::IOBuf::copyBuffer("x"))
                  .hasValue());
  EXPECT_TRUE(forwarder
                  ->objectStream(
                      ObjectHeader(43, 0, 1, 0, uint64_t(1)),
                      folly::IOBuf::copyBuffer("x"))
                  .hasValue());
}

// Test: an END_OF_GROUP status below a subscriber's start must not open a
// downstream stream for it.
TEST_F(MoQForwarderTest, ExcludedStatusObjectDoesNotOpenStream) {
  auto forwarder = std::make_shared<MoQForwarder>(
      kFwdTestTrackName, AbsoluteLocation{43, 2});
  auto consumer = createMockConsumer();
  EXPECT_CALL(*consumer, beginSubgroup(_, _, _, _)).Times(0);
  ASSERT_NE(
      addSubscriber(*forwarder, createMockSession(), consumer, RequestID(1)),
      nullptr);

  auto sgRes = forwarder->beginSubgroup(42, 0, 0);
  ASSERT_TRUE(sgRes.hasValue());
  EXPECT_TRUE((*sgRes)->endOfGroup(60).hasValue());
}

// Test: with cache interest, a subgroup that no subscriber accepts stays
// registered as a successful no-op. A later subscriber attaches at the next
// object boundary, with a stream that does not claim the subgroup's first
// object and carries an explicit subgroup ID.
TEST_F(MoQForwarderTest, CacheInterestRetainsIdleSubgroupForLateSubscriber) {
  auto forwarder = std::make_shared<MoQForwarder>(kFwdTestTrackName);
  auto firstConsumer = createMockConsumer();
  auto firstSubgroup = acceptingSubgroupConsumer();
  EXPECT_CALL(*firstConsumer, beginSubgroup(43, 4, _, _))
      .WillOnce(Return(subgroupConsumerResult(firstSubgroup)));
  EXPECT_CALL(*firstSubgroup, object(4, _, _, _));
  EXPECT_CALL(*firstSubgroup, object(5, _, _, _))
      .WillOnce(Return(stopSendingError()));
  ASSERT_NE(
      addSubscriber(
          *forwarder, createMockSession(), firstConsumer, RequestID(1)),
      nullptr);

  TrackConsumer::BeginSubgroupOptions publishOptions;
  publishOptions.beginsWithFirstObject = true;
  publishOptions.subgroupIDFormat = SubgroupIDFormat::FirstObject;
  auto sgRes =
      forwarder->beginSubgroupWithCacheInterest(43, 4, 0, publishOptions);
  ASSERT_TRUE(sgRes.hasValue());
  std::shared_ptr<SubgroupConsumer> subgroup = *sgRes;
  EXPECT_TRUE(subgroup->object(4, test::makeBuf(10)).hasValue());
  // The only subscriber stops this subgroup; the cache still consumes it.
  EXPECT_TRUE(subgroup->object(5, test::makeBuf(10)).hasValue());
  EXPECT_TRUE(subgroup->object(6, test::makeBuf(10)).hasValue());
  EXPECT_TRUE((*sgRes)->attached());

  auto lateConsumer = createMockConsumer();
  auto lateSubgroup = acceptingSubgroupConsumer();
  TrackConsumer::BeginSubgroupOptions lateOptions;
  EXPECT_CALL(*lateConsumer, beginSubgroup(43, 4, _, _))
      .WillOnce([&](uint64_t,
                    uint64_t,
                    uint8_t,
                    TrackConsumer::BeginSubgroupOptions options) {
        lateOptions = options;
        return subgroupConsumerResult(lateSubgroup);
      });
  EXPECT_CALL(*lateSubgroup, beginObject(7, 8, _, _));
  EXPECT_CALL(*lateSubgroup, objectPayload(_, false));
  ASSERT_NE(
      addSubscriber(
          *forwarder, createMockSession(), lateConsumer, RequestID(2)),
      nullptr);

  EXPECT_TRUE(
      subgroup->beginObject(7, 8, folly::IOBuf::copyBuffer("1234")).hasValue());
  auto payloadRes =
      subgroup->objectPayload(folly::IOBuf::copyBuffer("5678"), false);
  ASSERT_TRUE(payloadRes.hasValue());
  EXPECT_EQ(*payloadRes, ObjectPublishStatus::DONE);

  EXPECT_FALSE(lateOptions.beginsWithFirstObject);
  EXPECT_EQ(lateOptions.subgroupIDFormat, SubgroupIDFormat::Present);
}

// Test: cache interest never keeps a forwarder without subscribers alive, so
// the owner still learns the track became empty.
TEST_F(MoQForwarderTest, CacheInterestDoesNotOutliveLastSubscriber) {
  auto forwarder = std::make_shared<MoQForwarder>(kFwdTestTrackName);
  auto callback = std::make_shared<CountingCallback>();
  forwarder->setCallback(callback);
  auto consumer = createMockConsumer();
  EXPECT_CALL(*consumer, beginSubgroup(_, _, _, _))
      .WillOnce(Return(subgroupConsumerResult(acceptingSubgroupConsumer())));
  auto subscriber =
      addSubscriber(*forwarder, createMockSession(), consumer, RequestID(1));
  ASSERT_NE(subscriber, nullptr);
  auto sgRes = forwarder->beginSubgroupWithCacheInterest(1, 0, 0);
  ASSERT_TRUE(sgRes.hasValue());

  subscriber->unsubscribe();
  std::shared_ptr<SubgroupConsumer> subgroup = *sgRes;
  auto res = subgroup->object(0, test::makeBuf(10));

  EXPECT_TRUE(res.hasError());
  EXPECT_EQ(callback->onEmptyCount, 1);
}

// A subscription handle may unsubscribe from inside publishDone, as the
// proxy's downstream handle does when its session drops it. Draining and
// removal must not touch the subscriber's map entry afterwards.
TEST_F(MoQForwarderTest, PublishDoneMayReenterUnsubscribe) {
  auto forwarder = std::make_shared<MoQForwarder>(kFwdTestTrackName);
  auto callback = std::make_shared<CountingCallback>();
  forwarder->setCallback(callback);
  auto draining = createMockConsumer();
  auto removed = createMockConsumer();
  std::shared_ptr<MoQForwarder::Subscriber> drainingSubscriber;
  std::shared_ptr<MoQForwarder::Subscriber> removedSubscriber;
  EXPECT_CALL(*draining, publishDone(_)).WillOnce([&](const PublishDone&) {
    drainingSubscriber->unsubscribe();
    return folly::makeExpected<MoQPublishError>(folly::unit);
  });
  EXPECT_CALL(*removed, publishDone(_)).WillOnce([&](const PublishDone&) {
    removedSubscriber->unsubscribe();
    return folly::makeExpected<MoQPublishError>(folly::unit);
  });
  auto removedSession = createMockSession();
  drainingSubscriber =
      addSubscriber(*forwarder, createMockSession(), draining, RequestID(1));
  removedSubscriber =
      addSubscriber(*forwarder, removedSession, removed, RequestID(2));

  forwarder->removeSubscriber(
      removedSession->sessionId(),
      PublishDone{
          RequestID(2), PublishDoneStatusCode::SUBSCRIPTION_ENDED, 0, "gone"},
      "test");
  forwarder->publishDone(
      PublishDone{RequestID(0), PublishDoneStatusCode::TRACK_ENDED, 0, "done"});

  EXPECT_TRUE(forwarder->empty());
  EXPECT_EQ(callback->onEmptyCount, 1);
}

// Removing one subscriber can remove the last other one from its publishDone.
// The forwarder then becomes empty once; it does not first report that
// forwarding stopped.
TEST_F(MoQForwarderTest, ReentrantRemovalOfLastOtherSubscriberOnlyEmpties) {
  auto forwarder = std::make_shared<MoQForwarder>(kFwdTestTrackName);
  auto callback = std::make_shared<CountingCallback>();
  forwarder->setCallback(callback);
  auto first = createMockConsumer();
  auto second = createMockConsumer();
  std::shared_ptr<MoQForwarder::Subscriber> secondSubscriber;
  EXPECT_CALL(*first, publishDone(_)).WillOnce([&](const PublishDone&) {
    secondSubscriber->unsubscribe();
    return folly::makeExpected<MoQPublishError>(folly::unit);
  });
  auto firstSession = createMockSession();
  addSubscriber(*forwarder, firstSession, first, RequestID(1));
  secondSubscriber =
      addSubscriber(*forwarder, createMockSession(), second, RequestID(2));

  forwarder->removeSubscriber(
      firstSession->sessionId(),
      PublishDone{
          RequestID(1), PublishDoneStatusCode::SUBSCRIPTION_ENDED, 0, "gone"},
      "test");

  EXPECT_TRUE(forwarder->empty());
  EXPECT_EQ(callback->onEmptyCount, 1);
  const std::vector<bool> startedOnly{true};
  EXPECT_EQ(callback->forwardChanges, startedOnly);
}

// A fill reaching further back than the filter does not widen what the
// forwarder sends; the publisher session serves the fill.
TEST_F(MoQForwarderTest, FillDoesNotWidenTheSubscriptionRange) {
  auto forwarder = std::make_shared<MoQForwarder>(kFwdTestTrackName);
  forwarder->setLargest(AbsoluteLocation{50, 0});
  auto session = createMockSession();

  SubscribeRequest sub;
  sub.fullTrackName = forwarder->fullTrackName();
  sub.requestID = RequestID(1);
  sub.filter = LocationFilter::range(AbsoluteLocation{40, 0}, 60);
  sub.fill = FillParameters{LocationFilter::relativeStart(30), {}, {}, {}};

  auto subscriber =
      forwarder->addSubscriber(session->sessionId(), sub, createMockConsumer());
  ASSERT_NE(subscriber, nullptr);
  // addSubscriber clamps the start to the Next Object; the fill must not
  // move it.
  EXPECT_EQ(subscriber->range.start, (AbsoluteLocation{50, 1}));
  EXPECT_EQ(subscriber->range.end.group, 60);

  forwarder->removeSubscriber(session, std::nullopt, "test");
}

// Pausing is subscription state and a fill is not, so an update carrying both
// applies the pause and leaves the fill for the publisher session.
TEST_F(MoQForwarderTest, RequestUpdateWithFillStillAppliesForwardState) {
  auto forwarder = std::make_shared<MoQForwarder>(kFwdTestTrackName);
  forwarder->setLargest(AbsoluteLocation{50, 0});
  auto session = createMockSession();
  auto subscriber =
      addSubscriber(*forwarder, session, createMockConsumer(), RequestID(1));
  ASSERT_NE(subscriber, nullptr);
  ASSERT_TRUE(subscriber->checkShouldForward());

  RequestUpdate update;
  update.requestID = RequestID(2);
  update.existingRequestID = RequestID(1);
  update.forward = false;
  update.fill = FillParameters{LocationFilter::relativeStart(3), {}, {}, {}};
  auto res = folly::coro::blockingWait(subscriber->requestUpdate(update));
  ASSERT_TRUE(res.hasValue());
  EXPECT_FALSE(subscriber->checkShouldForward());

  forwarder->removeSubscriber(session, std::nullopt, "test");
}

} // namespace moxygen::test

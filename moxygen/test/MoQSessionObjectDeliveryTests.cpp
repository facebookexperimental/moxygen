/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/test/MoQSessionTestCommon.h"

using namespace moxygen;
using namespace moxygen::test;
using testing::_;

namespace moxygen { namespace test {

// Implementation of object validation test helper
folly::coro::Task<void> MoQSessionTest::publishValidationTest(
    TestLogicFn testLogic) {
  co_await setupMoQSession();
  auto sg1 = std::make_shared<testing::StrictMock<MockSubgroupConsumer>>();
  expectSubscribe(
      [this, testLogic, sg1](auto sub, auto pub) -> TaskSubscribeResult {
        EXPECT_CALL(
            *serverPublisherStatsCallback_, onSubscriptionStreamOpened());
        EXPECT_CALL(
            *clientSubscriberStatsCallback_, onSubscriptionStreamOpened());
        auto sgp = pub->beginSubgroup(0, 0, 0).value();
        eventBase_.add([testLogic, sub, pub, sgp, sg1]() {
          testLogic(sub, pub, sgp, sg1);
        });
        co_return makeSubscribeOkResult(sub, AbsoluteLocation{0, 0});
      });

  folly::coro::Baton resetBaton;
  EXPECT_CALL(*subscribeCallback_, beginSubgroup(0, 0, 0))
      .WillOnce(testing::Return(sg1));
  EXPECT_CALL(*sg1, reset(ResetStreamErrorCode::INTERNAL_ERROR))
      .WillOnce([&](auto) { resetBaton.post(); });
  expectSubscribeDone();
  EXPECT_CALL(*serverPublisherStatsCallback_, onSubscriptionStreamClosed());
  EXPECT_CALL(*clientSubscriberStatsCallback_, onSubscriptionStreamClosed());
  auto res = co_await clientSession_->subscribe(
      getSubscribe(kTestTrackName), subscribeCallback_);
  co_await subscribeDone_;
  co_await resetBaton;
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

}} // namespace moxygen::test

// === OBJECT DELIVERY tests ===

CO_TEST_P_X(MoQSessionTest, DoubleBeginObject) {
  co_await publishValidationTest([](auto sub, auto pub, auto sgp, auto sgc) {
    EXPECT_CALL(*sgc, beginObject(1, 100, _, _))
        .WillOnce(
            testing::Return(
                folly::Expected<folly::Unit, MoQPublishError>(folly::unit)));
    EXPECT_TRUE(sgp->beginObject(1, 100, test::makeBuf(10)));
    EXPECT_EQ(
        sgp->beginObject(2, 100, test::makeBuf(10)).error().code,
        MoQPublishError::API_ERROR);
    pub->subscribeDone(getTrackEndedSubscribeDone(sub.requestID));
  });
}
CO_TEST_P_X(MoQSessionTest, ObjectPayloadTooLong) {
  co_await publishValidationTest([](auto sub, auto pub, auto sgp, auto sgc) {
    EXPECT_CALL(*sgc, beginObject(1, 100, _, _))
        .WillOnce(
            testing::Return(
                folly::Expected<folly::Unit, MoQPublishError>(folly::unit)));
    EXPECT_TRUE(sgp->beginObject(1, 100, test::makeBuf(10)).hasValue());
    auto payloadFail =
        sgp->objectPayload(folly::IOBuf::copyBuffer(std::string(200, 'x')));
    EXPECT_EQ(payloadFail.error().code, MoQPublishError::API_ERROR);
    pub->subscribeDone(getTrackEndedSubscribeDone(sub.requestID));
  });
}
CO_TEST_P_X(MoQSessionTest, ObjectPayloadEarlyFin) {
  co_await publishValidationTest([](auto sub, auto pub, auto sgp, auto sgc) {
    EXPECT_CALL(*sgc, beginObject(1, 100, _, _))
        .WillOnce(
            testing::Return(
                folly::Expected<folly::Unit, MoQPublishError>(folly::unit)));
    EXPECT_TRUE(sgp->beginObject(1, 100, test::makeBuf(10)).hasValue());

    // Attempt to send an object payload with length 20 and fin=true, which
    // should fail
    auto payloadFinFail = sgp->objectPayload(
        folly::IOBuf::copyBuffer(std::string(20, 'x')), true);
    EXPECT_EQ(payloadFinFail.error().code, MoQPublishError::API_ERROR);

    pub->subscribeDone(getTrackEndedSubscribeDone(sub.requestID));
  });
}
CO_TEST_P_X(MoQSessionTest, PublisherResetAfterBeginObject) {
  co_await publishValidationTest([](auto sub, auto pub, auto sgp, auto sgc) {
    EXPECT_CALL(*sgc, beginObject(1, 100, _, _))
        .WillOnce(
            testing::Return(
                folly::Expected<folly::Unit, MoQPublishError>(folly::unit)));
    EXPECT_TRUE(sgp->beginObject(1, 100, test::makeBuf(10)));

    // Call reset after beginObject
    sgp->reset(ResetStreamErrorCode::INTERNAL_ERROR);

    // Attempt to send an object payload after reset, which should fail
    auto payloadFail =
        sgp->objectPayload(folly::IOBuf::copyBuffer(std::string(20, 'x')));
    EXPECT_EQ(payloadFail.error().code, MoQPublishError::CANCELLED);

    pub->subscribeDone(getTrackEndedSubscribeDone(sub.requestID));
  });
}
CO_TEST_P_X(MoQSessionTest, ObjectStatus) {
  co_await setupMoQSession();
  std::shared_ptr<TrackConsumer> trackConsumer;
  expectSubscribe(
      [this, &trackConsumer](auto sub, auto pub) -> TaskSubscribeResult {
        trackConsumer = pub;
        eventBase_.add([pub, sub] {
          auto sgp1 = pub->beginSubgroup(0, 0, 0).value();
          sgp1->object(0, moxygen::test::makeBuf(10));
          sgp1->objectNotExists(1);
          sgp1->object(2, moxygen::test::makeBuf(11));
          sgp1->endOfGroup(3, noExtensions());
          pub->groupNotExists(1, 0, 0);
          auto sgp2 = pub->beginSubgroup(2, 0, 0).value();
          sgp2->object(0, moxygen::test::makeBuf(10));
          sgp2->endOfTrackAndGroup(2);
        });
        co_return makeSubscribeOkResult(sub);
      });
  auto sg1 = std::make_shared<testing::StrictMock<MockSubgroupConsumer>>();
  EXPECT_CALL(*subscribeCallback_, beginSubgroup(0, 0, 0))
      .WillOnce(testing::Return(sg1));
  EXPECT_CALL(*sg1, object(0, _, _, false))
      .WillOnce(testing::Return(folly::unit));
  EXPECT_CALL(*sg1, objectNotExists(1, _, _))
      .WillOnce(testing::Return(folly::unit));
  EXPECT_CALL(*sg1, object(2, _, _, false))
      .WillOnce(testing::Return(folly::unit));
  EXPECT_CALL(*sg1, endOfGroup(3, _)).WillOnce(testing::Return(folly::unit));

  auto sg2 = std::make_shared<testing::StrictMock<MockSubgroupConsumer>>();
  EXPECT_CALL(*subscribeCallback_, beginSubgroup(1, 0, 0))
      .WillOnce(testing::Return(sg2));
  EXPECT_CALL(*subscribeCallback_, groupNotExists(1, 0, 0, _))
      .WillOnce(testing::Return(folly::unit));

  auto sg3 = std::make_shared<testing::StrictMock<MockSubgroupConsumer>>();
  EXPECT_CALL(*subscribeCallback_, beginSubgroup(2, 0, 0))
      .WillOnce(testing::Return(sg3));
  EXPECT_CALL(*sg3, object(0, _, _, false))
      .WillOnce(testing::Return(folly::unit));
  folly::coro::Baton endOfTrackAndGroupBaton;
  EXPECT_CALL(*sg3, endOfTrackAndGroup(2, _)).WillOnce(testing::Invoke([&]() {
    endOfTrackAndGroupBaton.post();
    return folly::unit;
  }));
  auto subscribeRequest = getSubscribe(kTestTrackName);
  auto res =
      co_await clientSession_->subscribe(subscribeRequest, subscribeCallback_);
  co_await endOfTrackAndGroupBaton;
  expectSubscribeDone();
  trackConsumer->subscribeDone(
      getTrackEndedSubscribeDone(subscribeRequest.requestID));
  co_await subscribeDone_;
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}
CO_TEST_P_X(MoQSessionTest, Datagrams) {
  co_await setupMoQSession();
  expectSubscribe([](auto sub, auto pub) -> TaskSubscribeResult {
    pub->datagram(
        ObjectHeader(0, 0, 1, 0, 11), folly::IOBuf::copyBuffer("hello world"));
    pub->datagram(
        ObjectHeader(0, 0, 2, 0, ObjectStatus::OBJECT_NOT_EXIST), nullptr);
    pub->subscribeDone(getTrackEndedSubscribeDone(sub.requestID));
    co_return makeSubscribeOkResult(sub, AbsoluteLocation{0, 0});
  });
  {
    testing::InSequence enforceOrder;
    EXPECT_CALL(*subscribeCallback_, datagram(_, _))
        .WillOnce(testing::Invoke([&](const auto& header, auto) {
          EXPECT_EQ(header.length, 11);
          return folly::unit;
        }));
    EXPECT_CALL(*subscribeCallback_, datagram(_, _))
        .WillOnce(testing::Invoke([&](const auto& header, auto) {
          EXPECT_EQ(header.status, ObjectStatus::OBJECT_NOT_EXIST);
          return folly::unit;
        }));
  }
  expectSubscribeDone();
  auto res = co_await clientSession_->subscribe(
      getSubscribe(kTestTrackName), subscribeCallback_);
  EXPECT_FALSE(res.hasError());
  co_await subscribeDone_;
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}
CO_TEST_P_X(MoQSessionTest, DatagramBeforeSessionSetup) {
  clientSession_->start();
  EXPECT_FALSE(clientWt_->isSessionClosed());
  clientSession_->onDatagram(folly::IOBuf::copyBuffer("hello world"));
  EXPECT_TRUE(clientWt_->isSessionClosed());
  co_return;
}
CO_TEST_P_X(MoQSessionTest, TooFarBehindOneSubgroup) {
  co_await setupMoQSession();

  MoQSettings moqSettings;
  moqSettings.bufferingThresholds.perSubscription = 100;
  serverSession_->setMoqSettings(moqSettings);

  expectSubscribe([this](auto sub, auto pub) -> TaskSubscribeResult {
    eventBase_.add(
        [this, pub, sub, serverWt = serverWt_.get(), eventBase = &eventBase_] {
          EXPECT_CALL(
              *serverPublisherStatsCallback_, onSubscriptionStreamOpened());
          EXPECT_CALL(
              *clientSubscriberStatsCallback_, onSubscriptionStreamOpened());
          auto sgp = pub->beginSubgroup(0, 0, 0).value();
          auto objectResult = sgp->object(0, moxygen::test::makeBuf(10));
          EXPECT_TRUE(objectResult.hasValue());

          // Run this stuff later on, otherwise the test will hang because of
          // the discrepancy in the stream count because the stream would have
          // been reset before the subgroup header got across.
          eventBase->add([pub, sub, serverWt, sgp] {
            // Start buffering data
            serverWt->writeHandles[2]->setImmediateDelivery(false);
            auto objectResult2 = sgp->object(1, moxygen::test::makeBuf(101));
            EXPECT_TRUE(objectResult2.hasError());
          });
        });
    co_return makeSubscribeOkResult(sub);
  });

  EXPECT_CALL(*serverPublisherStatsCallback_, onSubscriptionStreamClosed());
  EXPECT_CALL(*clientSubscriberStatsCallback_, onSubscriptionStreamClosed());
  expectSubscribeDone();
  auto mockSubgroupConsumer =
      std::make_shared<testing::StrictMock<MockSubgroupConsumer>>();
  EXPECT_CALL(*subscribeCallback_, beginSubgroup(0, 0, 0))
      .WillOnce(testing::Return(mockSubgroupConsumer));
  EXPECT_CALL(*mockSubgroupConsumer, object(0, _, _, _))
      .WillOnce(testing::Return(folly::unit));
  EXPECT_CALL(*mockSubgroupConsumer, reset(_));
  auto res = co_await clientSession_->subscribe(
      getSubscribe(kTestTrackName), subscribeCallback_);
  co_await subscribeDone_;
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}
CO_TEST_P_X(MoQSessionTest, FreeUpBufferSpaceOneSubgroup) {
  co_await setupMoQSession();

  MoQSettings moqSettings;
  moqSettings.bufferingThresholds.perSubscription = 100;
  serverSession_->setMoqSettings(moqSettings);

  expectSubscribe([this](auto sub, auto pub) -> TaskSubscribeResult {
    eventBase_.add([this, pub, sub, serverWt = serverWt_.get()] {
      EXPECT_CALL(*serverPublisherStatsCallback_, onSubscriptionStreamOpened());
      EXPECT_CALL(
          *clientSubscriberStatsCallback_, onSubscriptionStreamOpened());
      auto sgp = pub->beginSubgroup(0, 0, 0).value();
      auto objectResult = sgp->object(0, moxygen::test::makeBuf(10));
      EXPECT_TRUE(objectResult.hasValue());

      // Start buffering data
      serverWt->writeHandles[2]->setImmediateDelivery(false);
      for (uint32_t i = 0; i < 10; i++) {
        // Run this stuff later on, otherwise the test will hang because of
        // the discrepancy in the stream count because the stream would have
        // been reset before the subgroup header got across.
        objectResult = sgp->object(i + 1, moxygen::test::makeBuf(50));
        serverWt->writeHandles[2]->deliverInflightData();
        EXPECT_FALSE(objectResult.hasError());
      }
      pub->subscribeDone(getTrackEndedSubscribeDone(sub.requestID));
    });
    co_return makeSubscribeOkResult(sub);
  });

  EXPECT_CALL(*serverPublisherStatsCallback_, onSubscriptionStreamClosed());
  EXPECT_CALL(*clientSubscriberStatsCallback_, onSubscriptionStreamClosed());
  expectSubscribeDone();
  auto mockSubgroupConsumer =
      std::make_shared<testing::StrictMock<MockSubgroupConsumer>>();
  EXPECT_CALL(*subscribeCallback_, beginSubgroup(0, 0, 0))
      .WillOnce(testing::Return(mockSubgroupConsumer));
  EXPECT_CALL(*mockSubgroupConsumer, object(_, _, _, _))
      .WillRepeatedly(testing::Return(folly::unit));
  EXPECT_CALL(*mockSubgroupConsumer, reset(_));
  auto res = co_await clientSession_->subscribe(
      getSubscribe(kTestTrackName), subscribeCallback_);
  co_await subscribeDone_;
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}
CO_TEST_P_X(MoQSessionTest, TooFarBehindMultipleSubgroups) {
  co_await setupMoQSession();

  MoQSettings moqSettings;
  moqSettings.bufferingThresholds.perSubscription = 100;
  serverSession_->setMoqSettings(moqSettings);

  expectSubscribe([this](auto sub, auto pub) -> TaskSubscribeResult {
    eventBase_.add([this,
                    pub,
                    sub,
                    serverWt = serverWt_.get(),
                    eventBase = &eventBase_] {
      std::vector<std::shared_ptr<SubgroupConsumer>> subgroupConsumers;

      EXPECT_CALL(*serverPublisherStatsCallback_, onSubscriptionStreamOpened())
          .Times(3);
      EXPECT_CALL(*clientSubscriberStatsCallback_, onSubscriptionStreamOpened())
          .Times(3);
      for (uint32_t subgroupId = 0; subgroupId < 3; subgroupId++) {
        subgroupConsumers.push_back(
            pub->beginSubgroup(0, subgroupId, 0).value());
        auto objectResult = subgroupConsumers[subgroupId]->object(
            0, moxygen::test::makeBuf(10));
        EXPECT_TRUE(objectResult.hasValue());
      }

      EXPECT_CALL(*serverPublisherStatsCallback_, onSubscriptionStreamClosed())
          .Times(3);
      EXPECT_CALL(*clientSubscriberStatsCallback_, onSubscriptionStreamClosed())
          .Times(3);

      // Run this stuff later on, otherwise the test will hang because of
      // the discrepancy in the stream count because the stream would have
      // been reset before the subgroup header got across.
      eventBase->add([pub, sub, serverWt, subgroupConsumers] {
        for (uint32_t subgroupId = 0; subgroupId < 2; subgroupId++) {
          serverWt->writeHandles[2 + subgroupId * 4]->setImmediateDelivery(
              false);
          auto objectResult = subgroupConsumers[subgroupId]->object(
              1, moxygen::test::makeBuf(30));
          EXPECT_TRUE(objectResult.hasValue());
        }

        serverWt->writeHandles[10]->setImmediateDelivery(false);
        auto objectResult =
            subgroupConsumers[2]->object(1, moxygen::test::makeBuf(40));
        EXPECT_TRUE(objectResult.hasError());
      });
    });
    co_return makeSubscribeOkResult(sub);
  });

  expectSubscribeDone();
  auto mockSubgroupConsumer =
      std::make_shared<testing::StrictMock<MockSubgroupConsumer>>();
  EXPECT_CALL(*subscribeCallback_, beginSubgroup(_, _, _))
      .WillRepeatedly(testing::Return(mockSubgroupConsumer));
  EXPECT_CALL(*mockSubgroupConsumer, object(_, _, _, _))
      .WillRepeatedly(testing::Return(folly::unit));
  EXPECT_CALL(*mockSubgroupConsumer, reset(_)).Times(3);
  auto res = co_await clientSession_->subscribe(
      getSubscribe(kTestTrackName), subscribeCallback_);
  co_await subscribeDone_;
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}
CO_TEST_P_X(MoQSessionTest, PublisherAliveUntilAllBytesDelivered) {
  co_await setupMoQSession();
  folly::coro::Baton barricade;
  std::shared_ptr<SubgroupConsumer> subgroupConsumer = nullptr;
  expectSubscribe(
      [this, &subgroupConsumer](auto sub, auto pub) -> TaskSubscribeResult {
        eventBase_.add([this, pub, sub, &subgroupConsumer] {
          EXPECT_CALL(
              *serverPublisherStatsCallback_, onSubscriptionStreamOpened());
          EXPECT_CALL(
              *clientSubscriberStatsCallback_, onSubscriptionStreamOpened());
          auto sgp = pub->beginSubgroup(0, 0, 0).value();
          sgp->object(0, moxygen::test::makeBuf(10), Extensions(), false);
          subgroupConsumer = sgp;
        });
        co_return makeSubscribeOkResult(sub);
      });
  auto sg = std::make_shared<testing::StrictMock<MockSubgroupConsumer>>();
  EXPECT_CALL(*subscribeCallback_, beginSubgroup(0, 0, 0))
      .WillOnce(testing::Invoke([&] {
        eventBase_.add([&] {
          serverWt_->writeHandles[2]->setImmediateDelivery(false);
          EXPECT_CALL(
              *serverPublisherStatsCallback_, onSubscriptionStreamClosed());
          EXPECT_CALL(
              *clientSubscriberStatsCallback_, onSubscriptionStreamClosed());
          subgroupConsumer->object(
              1, moxygen::test::makeBuf(10), Extensions(), true);
          barricade.post();
        });
        return sg;
      }));
  EXPECT_CALL(*sg, object(_, _, _, _))
      .WillRepeatedly(testing::Return(folly::unit));
  EXPECT_CALL(*sg, reset(_));
  auto res = co_await clientSession_->subscribe(
      getSubscribe(kTestTrackName), subscribeCallback_);
  co_await barricade;
  serverWt_->writeHandles[2]->deliverInflightData();
  EXPECT_CALL(*subscribeCallback_, subscribeDone(_))
      .WillOnce(testing::Return(folly::unit));
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}
CO_TEST_P_X(MoQSessionTest, TestOnObjectPayload) {
  co_await setupMoQSession();

  std::shared_ptr<SubgroupConsumer> subgroupPublisher = nullptr;
  std::shared_ptr<TrackConsumer> trackConsumer = nullptr;
  expectSubscribe([&](auto sub, auto pub) -> TaskSubscribeResult {
    auto sgp = pub->beginSubgroup(0, 0, 0).value();
    subgroupPublisher = sgp;
    trackConsumer = pub;
    sgp->beginObject(0, 100, test::makeBuf(10)).hasValue();
    co_return makeSubscribeOkResult(sub, AbsoluteLocation{0, 0});
  });

  auto sg = std::make_shared<testing::StrictMock<MockSubgroupConsumer>>();
  EXPECT_CALL(*subscribeCallback_, beginSubgroup(0, 0, 0))
      .WillOnce(testing::Return(sg));

  folly::coro::Baton receivedBeginObject;
  EXPECT_CALL(*sg, beginObject(0, _, _, _))
      .WillOnce(testing::Invoke([&receivedBeginObject]() {
        receivedBeginObject.post();
        return folly::unit;
      }));

  auto subscribeRequest = getSubscribe(kTestTrackName);
  auto subscribeRes =
      co_await clientSession_->subscribe(subscribeRequest, subscribeCallback_);

  co_await receivedBeginObject;

  auto payloadSendResult = subgroupPublisher->objectPayload(
      folly::IOBuf::copyBuffer(std::string(90, 'x')), true);
  EXPECT_TRUE(payloadSendResult.hasValue());
  folly::coro::Baton receivedObjectPayload;
  EXPECT_CALL(*sg, objectPayload(_, _))
      .WillOnce(testing::Invoke([&receivedObjectPayload]() {
        receivedObjectPayload.post();
        return ObjectPublishStatus::DONE;
      }));
  EXPECT_CALL(*sg, endOfSubgroup());
  co_await receivedObjectPayload;

  expectSubscribeDone();
  trackConsumer->subscribeDone(
      getTrackEndedSubscribeDone(subscribeRequest.requestID));
  co_await subscribeDone_;
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

// Mock delivery callback for testing
class MockDeliveryCallback : public DeliveryCallback {
 public:
  MOCK_METHOD(
      void,
      onDelivered,
      (const folly::Optional<TrackAlias>&,
       uint64_t groupId,
       uint64_t subgroupId,
       uint64_t objectId),
      (override));

  MOCK_METHOD(
      void,
      onDeliveryCancelled,
      (const folly::Optional<TrackAlias>&,
       uint64_t groupId,
       uint64_t subgroupId,
       uint64_t objectId),
      (override));
};

CO_TEST_P_X(MoQSessionTest, DeliveryCallbackBasic) {
  co_await setupMoQSession();
  auto deliveryCallback =
      std::make_shared<testing::StrictMock<MockDeliveryCallback>>();

  expectSubscribe(
      [this, deliveryCallback](auto sub, auto pub) -> TaskSubscribeResult {
        // Set the delivery callback
        pub->setDeliveryCallback(deliveryCallback);

        eventBase_.add(
            [this, pub, sub, serverWt = serverWt_.get(), deliveryCallback] {
              EXPECT_CALL(
                  *serverPublisherStatsCallback_, onSubscriptionStreamOpened());
              EXPECT_CALL(
                  *clientSubscriberStatsCallback_,
                  onSubscriptionStreamOpened());
              auto sgp = pub->beginSubgroup(0, 0, 0).value();

              // Buffer data to control delivery timing
              serverWt->writeHandles[2]->setImmediateDelivery(false);
              auto objectResult = sgp->object(0, moxygen::test::makeBuf(10));
              EXPECT_TRUE(objectResult.hasValue());

              // Manually trigger delivery - this should invoke the callback
              // Expect the delivery callback to be invoked for the object
              EXPECT_CALL(*deliveryCallback, onDelivered(_, 0, 0, 0))
                  .WillOnce(testing::Return());
              serverWt->writeHandles[2]->deliverInflightData();

              sgp->endOfSubgroup();
              serverWt->writeHandles[2]->deliverInflightData();
              pub->subscribeDone(getTrackEndedSubscribeDone(sub.requestID));
            });
        co_return makeSubscribeOkResult(sub);
      });

  auto sg = std::make_shared<testing::StrictMock<MockSubgroupConsumer>>();
  EXPECT_CALL(*subscribeCallback_, beginSubgroup(0, 0, 0))
      .WillOnce(testing::Return(sg));
  EXPECT_CALL(*sg, object(0, _, _, _)).WillOnce(testing::Return(folly::unit));
  expectSubscribeDone();

  auto res = co_await clientSession_->subscribe(
      getSubscribe(kTestTrackName), subscribeCallback_);
  co_await subscribeDone_;
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}
CO_TEST_P_X(MoQSessionTest, DeliveryCallbackObjectSplitInTwo) {
  co_await setupMoQSession();
  auto deliveryCallback =
      std::make_shared<testing::StrictMock<MockDeliveryCallback>>();

  expectSubscribe(
      [this, deliveryCallback](auto sub, auto pub) -> TaskSubscribeResult {
        // Set the delivery callback
        pub->setDeliveryCallback(deliveryCallback);

        eventBase_.add(
            [this, pub, sub, serverWt = serverWt_.get(), deliveryCallback] {
              EXPECT_CALL(
                  *serverPublisherStatsCallback_, onSubscriptionStreamOpened());
              EXPECT_CALL(
                  *clientSubscriberStatsCallback_,
                  onSubscriptionStreamOpened());
              auto sgp = pub->beginSubgroup(0, 0, 0).value();

              serverWt->writeHandles[2]->setImmediateDelivery(false);

              // Begin object with initial payload (5 bytes) out of total 10
              // bytes
              auto beginObjectResult =
                  sgp->beginObject(0, 10, moxygen::test::makeBuf(5));
              EXPECT_TRUE(beginObjectResult.hasValue());

              serverWt->writeHandles[2]->deliverInflightData();

              // Expect the delivery callback to be invoked for the object
              EXPECT_CALL(*deliveryCallback, onDelivered(_, 0, 0, 0)).Times(1);

              // Send remaining payload (5 bytes) to complete the object
              auto payloadResult =
                  sgp->objectPayload(moxygen::test::makeBuf(5));
              EXPECT_TRUE(payloadResult.hasValue());

              serverWt->writeHandles[2]->deliverInflightData();

              sgp->endOfSubgroup();
              serverWt->writeHandles[2]->deliverInflightData();
              pub->subscribeDone(getTrackEndedSubscribeDone(sub.requestID));
            });
        co_return makeSubscribeOkResult(sub);
      });

  auto sg = std::make_shared<testing::StrictMock<MockSubgroupConsumer>>();
  EXPECT_CALL(*subscribeCallback_, beginSubgroup(0, 0, 0))
      .WillOnce(testing::Return(sg));
  EXPECT_CALL(*sg, object(0, _, _, _)).WillOnce(testing::Return(folly::unit));
  expectSubscribeDone();

  auto res = co_await clientSession_->subscribe(
      getSubscribe(kTestTrackName), subscribeCallback_);
  co_await subscribeDone_;
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}
CO_TEST_P_X(MoQSessionTest, DeliveryCallbackMultipleStreams) {
  co_await setupMoQSession();
  auto deliveryCallback =
      std::make_shared<testing::StrictMock<MockDeliveryCallback>>();

  expectSubscribe(
      [this, deliveryCallback, serverWt = serverWt_.get()](
          auto sub, auto pub) -> TaskSubscribeResult {
        // Set the delivery callback
        pub->setDeliveryCallback(deliveryCallback);

        eventBase_.add([this, pub, sub, deliveryCallback] {
          EXPECT_CALL(
              *serverPublisherStatsCallback_, onSubscriptionStreamOpened())
              .Times(testing::AtLeast(1));
          EXPECT_CALL(
              *clientSubscriberStatsCallback_, onSubscriptionStreamOpened())
              .Times(testing::AtLeast(1));

          // Create multiple subgroups and objects across different streams
          // Stream 1: Group 0, Subgroup 0
          auto sgp1 = pub->beginSubgroup(0, 0, 0).value();
          auto objectResult1 = sgp1->object(0, moxygen::test::makeBuf(10));
          EXPECT_TRUE(objectResult1.hasValue());
          auto objectResult2 = sgp1->object(1, moxygen::test::makeBuf(20));
          EXPECT_TRUE(objectResult2.hasValue());
          sgp1->endOfSubgroup();

          // Stream 2: Group 0, Subgroup 1
          auto sgp2 = pub->beginSubgroup(0, 1, 0).value();
          auto objectResult3 = sgp2->object(0, moxygen::test::makeBuf(15));
          EXPECT_TRUE(objectResult3.hasValue());
          sgp2->endOfSubgroup();

          // Stream 3: Group 1, Subgroup 0
          auto sgp3 = pub->beginSubgroup(1, 0, 0).value();
          auto objectResult4 = sgp3->object(0, moxygen::test::makeBuf(25));
          EXPECT_TRUE(objectResult4.hasValue());
          sgp3->endOfSubgroup();

          pub->subscribeDone(getTrackEndedSubscribeDone(sub.requestID));
        });
        co_return makeSubscribeOkResult(sub);
      });

  // Set up mock subgroup consumers for each subgroup
  auto sg1 = std::make_shared<testing::StrictMock<MockSubgroupConsumer>>();
  auto sg2 = std::make_shared<testing::StrictMock<MockSubgroupConsumer>>();
  auto sg3 = std::make_shared<testing::StrictMock<MockSubgroupConsumer>>();

  EXPECT_CALL(*subscribeCallback_, beginSubgroup(0, 0, 0))
      .WillOnce(testing::Return(sg1));
  EXPECT_CALL(*sg1, object(0, _, _, _)).WillOnce(testing::Return(folly::unit));
  EXPECT_CALL(*sg1, object(1, _, _, _)).WillOnce(testing::Return(folly::unit));

  EXPECT_CALL(*subscribeCallback_, beginSubgroup(0, 1, 0))
      .WillOnce(testing::Return(sg2));
  EXPECT_CALL(*sg2, object(0, _, _, _)).WillOnce(testing::Return(folly::unit));

  EXPECT_CALL(*subscribeCallback_, beginSubgroup(1, 0, 0))
      .WillOnce(testing::Return(sg3));
  EXPECT_CALL(*sg3, object(0, _, _, _)).WillOnce(testing::Return(folly::unit));

  expectSubscribeDone();

  EXPECT_CALL(*deliveryCallback, onDelivered(_, 0, 0, 0));
  EXPECT_CALL(*deliveryCallback, onDelivered(_, 0, 0, 1));
  EXPECT_CALL(*deliveryCallback, onDelivered(_, 0, 1, 0));
  EXPECT_CALL(*deliveryCallback, onDelivered(_, 1, 0, 0));

  auto res = co_await clientSession_->subscribe(
      getSubscribe(kTestTrackName), subscribeCallback_);

  co_await subscribeDone_;
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}
using V15PlusTests = MoQSessionTest;

INSTANTIATE_TEST_SUITE_P(
    V15PlusTests,
    V15PlusTests,
    testing::Values(VersionParams{{kVersionDraft15}, kVersionDraft15}));
CO_TEST_P_X(V15PlusTests, SubgroupPriorityFallback) {
  co_await setupMoQSession();
  std::shared_ptr<TrackConsumer> trackConsumer;

  // Set publisher priority to 64 (non-default value)
  constexpr uint8_t kPublisherPriority = 64;

  expectSubscribe(
      [this, &trackConsumer, kPublisherPriority](
          auto sub, auto pub) -> TaskSubscribeResult {
        trackConsumer = pub;
        eventBase_.add([pub, sub] {
          // Begin subgroup with default priority (128)
          // This will be sent on wire WITH a priority field because the
          // PUBLISHER updated the default value to 64.
          auto sgp = pub->beginSubgroup(0, 0, kDefaultPriority).value();
          sgp->object(0, moxygen::test::makeBuf(10));
          sgp->endOfTrackAndGroup(1);
          pub->subscribeDone(getTrackEndedSubscribeDone(sub.requestID));
        });
        // Return SubscribeOk with PUBLISHER_PRIORITY parameter
        co_return makeSubscribeOkResult(sub, folly::none, kPublisherPriority);
      });

  auto sg1 = std::make_shared<testing::StrictMock<MockSubgroupConsumer>>();
  folly::coro::Baton endOfTrackReceived;
  // Subgroup has kDefaultPriority (128)
  EXPECT_CALL(*subscribeCallback_, beginSubgroup(0, 0, kDefaultPriority))
      .WillOnce(testing::Return(sg1));
  EXPECT_CALL(*sg1, object(0, _, _, false))
      .WillOnce(testing::Return(folly::unit));
  EXPECT_CALL(*sg1, endOfTrackAndGroup(1, _)).WillOnce(testing::Invoke([&]() {
    endOfTrackReceived.post();
    return folly::unit;
  }));
  expectSubscribeDone();

  auto subscribeRequest = getSubscribe(kTestTrackName);
  auto res =
      co_await clientSession_->subscribe(subscribeRequest, subscribeCallback_);

  co_await endOfTrackReceived;
  co_await subscribeDone_;
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}
CO_TEST_P_X(V15PlusTests, SubgroupExplicitPriority) {
  co_await setupMoQSession();
  std::shared_ptr<TrackConsumer> trackConsumer;

  constexpr uint8_t kObjectPriority = 32;

  expectSubscribe(
      [this, &trackConsumer](auto sub, auto pub) -> TaskSubscribeResult {
        trackConsumer = pub;
        eventBase_.add([pub, sub] {
          // Begin subgroup with explicit priority
          auto sgp = pub->beginSubgroup(0, 0, kObjectPriority).value();
          sgp->object(0, moxygen::test::makeBuf(10));
          sgp->endOfTrackAndGroup(1);
          pub->subscribeDone(getTrackEndedSubscribeDone(sub.requestID));
        });
        // Return SubscribeOk with PUBLISHER_PRIORITY parameter
        constexpr uint8_t kPublisherPriority = 64;
        co_return makeSubscribeOkResult(sub, folly::none, kPublisherPriority);
      });

  auto sg1 = std::make_shared<testing::StrictMock<MockSubgroupConsumer>>();
  folly::coro::Baton endOfTrackReceived;
  // When object has explicit priority, it should use that (32) not publisher
  // priority
  EXPECT_CALL(*subscribeCallback_, beginSubgroup(0, 0, kObjectPriority))
      .WillOnce(testing::Return(sg1));
  EXPECT_CALL(*sg1, object(0, _, _, false))
      .WillOnce(testing::Return(folly::unit));
  EXPECT_CALL(*sg1, endOfTrackAndGroup(1, _)).WillOnce(testing::Invoke([&]() {
    endOfTrackReceived.post();
    return folly::unit;
  }));
  expectSubscribeDone();

  auto subscribeRequest = getSubscribe(kTestTrackName);
  auto res =
      co_await clientSession_->subscribe(subscribeRequest, subscribeCallback_);

  co_await endOfTrackReceived;
  co_await subscribeDone_;
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}
CO_TEST_P_X(V15PlusTests, ObjectStatusPriorityFallback) {
  co_await setupMoQSession();
  std::shared_ptr<TrackConsumer> trackConsumer;

  constexpr uint8_t kPublisherPriority = 64;

  expectSubscribe(
      [this, &trackConsumer, kPublisherPriority](
          auto sub, auto pub) -> TaskSubscribeResult {
        trackConsumer = pub;
        eventBase_.add([pub, sub] {
          // Send objectNotExists with default priority
          auto sgp = pub->beginSubgroup(0, 0, kDefaultPriority).value();
          sgp->objectNotExists(0);
          sgp->endOfTrackAndGroup(1);
          pub->subscribeDone(getTrackEndedSubscribeDone(sub.requestID));
        });
        // Return SubscribeOk with PUBLISHER_PRIORITY parameter
        co_return makeSubscribeOkResult(sub, folly::none, kPublisherPriority);
      });

  auto sg1 = std::make_shared<testing::StrictMock<MockSubgroupConsumer>>();
  folly::coro::Baton endOfTrackReceived;
  // Object status with no explicit priority should use publisher priority
  EXPECT_CALL(*subscribeCallback_, beginSubgroup(0, 0, kDefaultPriority))
      .WillOnce(testing::Return(sg1));
  EXPECT_CALL(*sg1, objectNotExists(0, _, _))
      .WillOnce(testing::Return(folly::unit));
  EXPECT_CALL(*sg1, endOfTrackAndGroup(1, _)).WillOnce(testing::Invoke([&]() {
    endOfTrackReceived.post();
    return folly::unit;
  }));
  expectSubscribeDone();

  auto subscribeRequest = getSubscribe(kTestTrackName);
  auto res =
      co_await clientSession_->subscribe(subscribeRequest, subscribeCallback_);

  co_await endOfTrackReceived;
  co_await subscribeDone_;
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}
CO_TEST_P_X(V15PlusTests, PublisherPriorityDefaultValue) {
  co_await setupMoQSession();
  std::shared_ptr<TrackConsumer> trackConsumer;

  expectSubscribe(
      [this, &trackConsumer](auto sub, auto pub) -> TaskSubscribeResult {
        trackConsumer = pub;
        eventBase_.add([pub, sub] {
          // Begin subgroup with default priority
          auto sgp = pub->beginSubgroup(0, 0, kDefaultPriority).value();
          sgp->object(0, moxygen::test::makeBuf(10));
          sgp->endOfTrackAndGroup(1);
          pub->subscribeDone(getTrackEndedSubscribeDone(sub.requestID));
        });
        // Return SubscribeOk WITHOUT PUBLISHER_PRIORITY parameter
        // Should default to 128
        co_return makeSubscribeOkResult(sub);
      });

  auto sg1 = std::make_shared<testing::StrictMock<MockSubgroupConsumer>>();
  folly::coro::Baton endOfTrackReceived;
  // Without PUBLISHER_PRIORITY param, should default to kDefaultPriority (128)
  EXPECT_CALL(*subscribeCallback_, beginSubgroup(0, 0, kDefaultPriority))
      .WillOnce(testing::Return(sg1));
  EXPECT_CALL(*sg1, object(0, _, _, false))
      .WillOnce(testing::Return(folly::unit));
  EXPECT_CALL(*sg1, endOfTrackAndGroup(1, _)).WillOnce(testing::Invoke([&]() {
    endOfTrackReceived.post();
    return folly::unit;
  }));
  expectSubscribeDone();

  auto subscribeRequest = getSubscribe(kTestTrackName);
  auto res =
      co_await clientSession_->subscribe(subscribeRequest, subscribeCallback_);

  co_await endOfTrackReceived;
  co_await subscribeDone_;
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

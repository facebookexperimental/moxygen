/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/test/MoQSessionTestCommon.h"

using namespace moxygen;
using namespace moxygen::test;
using testing::_;

// === SUBSCRIBE tests ===

CO_TEST_P_X(MoQSessionTest, ServerInitiatedSubscribe) {
  co_await setupMoQSession();
  expectSubscribe(
      [this](auto sub, auto pub) -> TaskSubscribeResult {
        eventBase_.add([pub, sub] {
          auto sgp = pub->beginSubgroup(0, 0, 0).value();
          sgp->object(0, moxygen::test::makeBuf(10));
          sgp->object(1, moxygen::test::makeBuf(10), noExtensions(), true);
          pub->subscribeDone(getTrackEndedSubscribeDone(sub.requestID));
        });
        co_return makeSubscribeOkResult(sub);
      },
      MoQControlCodec::Direction::CLIENT);

  auto sg1 = std::make_shared<testing::StrictMock<MockSubgroupConsumer>>();
  EXPECT_CALL(*subscribeCallback_, beginSubgroup(0, 0, 0))
      .WillOnce(testing::Return(sg1));
  EXPECT_CALL(*sg1, object(0, _, _, false))
      .WillOnce(testing::Return(folly::unit));
  EXPECT_CALL(*sg1, object(1, _, _, true))
      .WillOnce(testing::Return(folly::unit));
  expectSubscribeDone(MoQControlCodec::Direction::SERVER);
  auto res = co_await serverSession_->subscribe(
      getSubscribe(kTestTrackName), subscribeCallback_);
  co_await subscribeDone_;
  serverSession_->close(SessionCloseErrorCode::NO_ERROR);
}
CO_TEST_P_X(MoQSessionTest, MaxRequestID) {
  co_await setupMoQSession();
  {
    testing::InSequence enforceOrder;
    expectSubscribe(
        [](auto sub, auto) -> TaskSubscribeResult {
          co_return folly::makeUnexpected(
              SubscribeError{
                  sub.requestID, SubscribeErrorCode::UNAUTHORIZED, "bad"});
        },
        MoQControlCodec::Direction::SERVER,
        SubscribeErrorCode::UNAUTHORIZED);
    expectSubscribe([this](auto sub, auto pub) -> TaskSubscribeResult {
      eventBase_.add([pub, sub] {
        pub->subscribeDone(getTrackEndedSubscribeDone(sub.requestID));
      });
      co_return makeSubscribeOkResult(sub);
    });
    expectSubscribe([](auto sub, auto) -> TaskSubscribeResult {
      co_return makeSubscribeOkResult(sub);
    });
    expectSubscribe([](auto sub, auto) -> TaskSubscribeResult {
      co_return makeSubscribeOkResult(sub);
    });
  }
  auto trackPublisher1 =
      std::make_shared<testing::StrictMock<MockTrackConsumer>>();
  auto trackPublisher2 =
      std::make_shared<testing::StrictMock<MockTrackConsumer>>();
  auto trackPublisher3 =
      std::make_shared<testing::StrictMock<MockTrackConsumer>>();
  // Expect setTrackAlias to be called for each publisher and return folly::unit
  // if called
  EXPECT_CALL(*trackPublisher1, setTrackAlias(_))
      .WillRepeatedly(
          testing::Return(
              folly::Expected<folly::Unit, MoQPublishError>(folly::unit)));
  EXPECT_CALL(*trackPublisher2, setTrackAlias(_))
      .WillRepeatedly(
          testing::Return(
              folly::Expected<folly::Unit, MoQPublishError>(folly::unit)));
  EXPECT_CALL(*trackPublisher3, setTrackAlias(_))
      .WillRepeatedly(
          testing::Return(
              folly::Expected<folly::Unit, MoQPublishError>(folly::unit)));

  EXPECT_CALL(
      *clientSubscriberStatsCallback_,
      onSubscribeError(SubscribeErrorCode::UNAUTHORIZED));
  auto res = co_await clientSession_->subscribe(
      getSubscribe(kTestTrackName), trackPublisher1);
  co_await folly::coro::co_reschedule_on_current_executor;
  // This is true because initial is 4 in this test case and we grant credit
  // every 50%.
  auto expectedSubId = 3 * getRequestIDMultiplier();
  EXPECT_EQ(serverSession_->maxRequestID(), expectedSubId);

  // subscribe again but this time we get a DONE
  EXPECT_CALL(*trackPublisher2, subscribeDone(_))
      .WillOnce(testing::Return(folly::unit));
  EXPECT_CALL(*clientSubscriberStatsCallback_, onSubscribeSuccess());
  res = co_await clientSession_->subscribe(
      getSubscribe(kTestTrackName), trackPublisher2);
  co_await folly::coro::co_reschedule_on_current_executor;
  expectedSubId += getRequestIDMultiplier();
  EXPECT_EQ(serverSession_->maxRequestID(), expectedSubId);

  // subscribe three more times, last one should fail, the first two will get
  // subscribeDone via the session closure
  EXPECT_CALL(*trackPublisher3, subscribeDone(_))
      .WillOnce(testing::Return(folly::unit))
      .WillOnce(testing::Return(folly::unit));

  EXPECT_CALL(*clientSubscriberStatsCallback_, onSubscribeSuccess()).Times(2);
  auto sub = getSubscribe(kTestTrackName);
  res = co_await clientSession_->subscribe(sub, trackPublisher3);
  res = co_await clientSession_->subscribe(sub, trackPublisher3);
  EXPECT_CALL(
      *clientSubscriberStatsCallback_,
      onSubscribeError(SubscribeErrorCode::INTERNAL_ERROR));
  EXPECT_CALL(*clientSubscriberStatsCallback_, recordSubscribeLatency(_));
  res = co_await clientSession_->subscribe(sub, trackPublisher3);
  EXPECT_TRUE(res.hasError());
}
CO_TEST_P_X(MoQSessionTest, SubscribeUpdate) {
  co_await setupMoQSession();
  std::shared_ptr<MockSubscriptionHandle> mockSubscriptionHandle = nullptr;
  std::shared_ptr<TrackConsumer> trackConsumer = nullptr;
  expectSubscribeDone();
  expectSubscribe(
      [&mockSubscriptionHandle, &trackConsumer](
          auto sub, auto pub) -> TaskSubscribeResult {
        trackConsumer = pub;
        mockSubscriptionHandle =
            makeSubscribeOkResult(sub, AbsoluteLocation{0, 0});
        co_return mockSubscriptionHandle;
      });

  auto subscribeRequest = getSubscribe(kTestTrackName);
  auto res =
      co_await clientSession_->subscribe(subscribeRequest, subscribeCallback_);
  auto subscribeHandler = res.value();

  SubscribeUpdate subscribeUpdate{
      subscribeRequest.requestID,
      RequestID(0),
      AbsoluteLocation{0, 0},
      10,
      kDefaultPriority + 1,
      true,
      {}};
  EXPECT_CALL(*clientSubscriberStatsCallback_, onSubscribeUpdate());
  EXPECT_CALL(*serverPublisherStatsCallback_, onSubscribeUpdate());
  subscribeHandler->subscribeUpdate(subscribeUpdate);
  folly::coro::Baton subscribeUpdateInvoked;
  EXPECT_CALL(*mockSubscriptionHandle, subscribeUpdate)
      .WillOnce(testing::Invoke([&](auto) { subscribeUpdateInvoked.post(); }));
  co_await subscribeUpdateInvoked;
  trackConsumer->subscribeDone(
      getTrackEndedSubscribeDone(subscribeRequest.requestID));
  co_await subscribeDone_;
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}
CO_TEST_P_X(MoQSessionTest, SubscribeForwardingFalse) {
  co_await setupMoQSession();
  expectSubscribe([](auto sub, auto pub) -> TaskSubscribeResult {
    auto pubResult1 = pub->datagram(
        ObjectHeader(0, 0, 1, 0, 11), folly::IOBuf::copyBuffer("hello world"));
    EXPECT_TRUE(pubResult1.hasError());
    auto pubResult2 = pub->objectStream(ObjectHeader(0, 0, 1, 0, 11), nullptr);
    EXPECT_TRUE(pubResult2.hasError());
    auto pubResult3 = pub->beginSubgroup(0, 0, 0);
    EXPECT_TRUE(pubResult3.hasError());
    auto pubResult4 = pub->groupNotExists(0, 0, 0, noExtensions());
    EXPECT_TRUE(pubResult4.hasError());
    pub->subscribeDone(getTrackEndedSubscribeDone(sub.requestID));
    co_return makeSubscribeOkResult(sub, AbsoluteLocation{0, 0});
  });
  expectSubscribeDone();
  auto subscribeRequest = getSubscribe(kTestTrackName);
  subscribeRequest.forward = false;
  auto res =
      co_await clientSession_->subscribe(subscribeRequest, subscribeCallback_);
  EXPECT_FALSE(res.hasError());
  co_await subscribeDone_;
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}
CO_TEST_P_X(MoQSessionTest, SubscribeUpdateForwardingFalse) {
  co_await setupMoQSession();
  std::shared_ptr<SubgroupConsumer> subgroupConsumer = nullptr;
  std::shared_ptr<TrackConsumer> trackConsumer = nullptr;
  std::shared_ptr<MockSubscriptionHandle> mockSubscriptionHandle = nullptr;
  expectSubscribeDone();
  expectSubscribe(
      [&subgroupConsumer, &trackConsumer, &mockSubscriptionHandle, this](
          auto sub, auto pub) -> TaskSubscribeResult {
        trackConsumer = pub;
        EXPECT_CALL(
            *serverPublisherStatsCallback_, onSubscriptionStreamOpened());
        EXPECT_CALL(
            *clientSubscriberStatsCallback_, onSubscriptionStreamOpened());
        auto pubResult = pub->beginSubgroup(0, 0, 0);
        EXPECT_FALSE(pubResult.hasError());
        subgroupConsumer = pubResult.value();
        auto objectResult =
            subgroupConsumer->object(0, moxygen::test::makeBuf(10));
        EXPECT_FALSE(objectResult.hasError());
        mockSubscriptionHandle =
            makeSubscribeOkResult(sub, AbsoluteLocation{0, 0});
        co_return mockSubscriptionHandle;
      });

  auto mockSubgroupConsumer =
      std::make_shared<testing::StrictMock<MockSubgroupConsumer>>();
  folly::coro::Baton subgroupCreated;
  EXPECT_CALL(*subscribeCallback_, beginSubgroup(0, 0, 0))
      .WillOnce(testing::Invoke([&](auto, auto, auto) {
        subgroupCreated.post();
        return mockSubgroupConsumer;
      }));
  EXPECT_CALL(*mockSubgroupConsumer, object(0, _, _, _))
      .WillOnce(testing::Return(folly::unit));
  EXPECT_CALL(*mockSubgroupConsumer, reset(_));
  auto subscribeRequest = getSubscribe(kTestTrackName);
  auto res =
      co_await clientSession_->subscribe(subscribeRequest, subscribeCallback_);
  auto subscribeHandler = res.value();

  co_await subgroupCreated;

  SubscribeUpdate subscribeUpdate{
      subscribeRequest.requestID,
      RequestID(0),
      AbsoluteLocation{0, 0},
      10,
      kDefaultPriority,
      false,
      {}};
  subscribeHandler->subscribeUpdate(subscribeUpdate);
  folly::coro::Baton subscribeUpdateInvoked;
  EXPECT_CALL(*mockSubscriptionHandle, subscribeUpdate)
      .WillOnce(testing::Invoke([&](auto /*blag*/) {
        subscribeUpdateInvoked.post();
      }));
  co_await subscribeUpdateInvoked;
  EXPECT_CALL(*serverPublisherStatsCallback_, onSubscriptionStreamClosed());
  EXPECT_CALL(*clientSubscriberStatsCallback_, onSubscriptionStreamClosed());
  auto pubResult = subgroupConsumer->object(1, moxygen::test::makeBuf(10));
  EXPECT_TRUE(pubResult.hasError());
  trackConsumer->subscribeDone(
      getTrackEndedSubscribeDone(subscribeRequest.requestID));
  co_await subscribeDone_;
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}
CO_TEST_P_X(MoQSessionTest, SubscribeDoneStreamCount) {
  co_await setupMoQSession();
  expectSubscribe([this](auto sub, auto pub) -> TaskSubscribeResult {
    eventBase_.add([this, pub, sub] {
      EXPECT_CALL(*serverPublisherStatsCallback_, onSubscriptionStreamOpened())
          .Times(2);
      EXPECT_CALL(*clientSubscriberStatsCallback_, onSubscriptionStreamOpened())
          .Times(2);
      EXPECT_CALL(*serverPublisherStatsCallback_, onSubscriptionStreamClosed())
          .Times(2);
      EXPECT_CALL(*clientSubscriberStatsCallback_, onSubscriptionStreamClosed())
          .Times(2);
      pub->objectStream(
          ObjectHeader(0, 0, 0, 0, 10), moxygen::test::makeBuf(10));
      auto sgp = pub->beginSubgroup(0, 1, 0).value();
      sgp->object(1, moxygen::test::makeBuf(10));
      sgp->object(2, moxygen::test::makeBuf(10), noExtensions(), true);
      pub->subscribeDone(getTrackEndedSubscribeDone(sub.requestID));
    });
    co_return makeSubscribeOkResult(sub);
  });
  auto sg1 = std::make_shared<testing::StrictMock<MockSubgroupConsumer>>();
  auto sg2 = std::make_shared<testing::StrictMock<MockSubgroupConsumer>>();
  EXPECT_CALL(*subscribeCallback_, beginSubgroup(0, 0, 0))
      .WillOnce(testing::Return(sg1));
  EXPECT_CALL(*subscribeCallback_, beginSubgroup(0, 1, 0))
      .WillOnce(testing::Return(sg2));
  EXPECT_CALL(*sg1, object(0, _, _, true))
      .WillOnce(testing::Return(folly::unit));
  EXPECT_CALL(*sg2, object(1, _, _, false))
      .WillOnce(testing::Return(folly::unit));
  EXPECT_CALL(*sg2, object(2, _, _, true))
      .WillOnce(testing::Return(folly::unit));
  expectSubscribeDone();
  auto res = co_await clientSession_->subscribe(
      getSubscribe(kTestTrackName), subscribeCallback_);
  co_await subscribeDone_;
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}
CO_TEST_P_X(MoQSessionTest, SubscribeDoneFromSubscribe) {
  co_await setupMoQSession();
  expectSubscribe([](auto sub, auto pub) -> TaskSubscribeResult {
    pub->subscribeDone(getTrackEndedSubscribeDone(sub.requestID));
    co_return makeSubscribeOkResult(sub);
  });
  expectSubscribeDone();
  auto res = co_await clientSession_->subscribe(
      getSubscribe(kTestTrackName), subscribeCallback_);
  co_await subscribeDone_;
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}
CO_TEST_P_X(MoQSessionTest, SubscribeDoneAPIErrors) {
  co_await setupMoQSession();
  expectSubscribe([](auto sub, auto pub) -> TaskSubscribeResult {
    pub->subscribeDone(getTrackEndedSubscribeDone(sub.requestID));
    // All these APIs fail after SUBSCRIBE_DONE
    EXPECT_EQ(
        pub->beginSubgroup(1, 1, 1).error().code, MoQPublishError::API_ERROR);
    EXPECT_EQ(
        pub->awaitStreamCredit().error().code, MoQPublishError::API_ERROR);
    EXPECT_EQ(
        pub->datagram(ObjectHeader(2, 2, 2, 2, 10), moxygen::test::makeBuf(10))
            .error()
            .code,
        MoQPublishError::API_ERROR);
    EXPECT_EQ(
        pub->subscribeDone(getTrackEndedSubscribeDone(sub.requestID))
            .error()
            .code,
        MoQPublishError::API_ERROR);
    co_return makeSubscribeOkResult(sub);
  });

  expectSubscribeDone();
  auto res = co_await clientSession_->subscribe(
      getSubscribe(kTestTrackName), subscribeCallback_);
  co_await subscribeDone_;
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}
CO_TEST_P_X(MoQSessionTest, SubscribeWithParams) {
  co_await setupMoQSession();

  expectSubscribe([](auto sub, auto pub) -> TaskSubscribeResult {
    EXPECT_EQ(sub.params.size(), 1);
    EXPECT_EQ(
        sub.params.at(0).key,
        folly::to_underlying(TrackRequestParamKey::AUTHORIZATION_TOKEN));
    EXPECT_EQ(sub.params.at(0).asAuthToken.tokenValue, "auth_token_value");

    pub->subscribeDone(getTrackEndedSubscribeDone(sub.requestID));
    co_return makeSubscribeOkResult(sub);
  });

  expectSubscribeDone();

  SubscribeRequest subscribeRequest = getSubscribe(kTestTrackName);
  subscribeRequest.params.insertParam(Parameter(
      folly::to_underlying(TrackRequestParamKey::DELIVERY_TIMEOUT), 5000));
  subscribeRequest.params.insertParam(
      getAuthParam(getServerSelectedVersion(), "auth_token_value"));

  auto res =
      co_await clientSession_->subscribe(subscribeRequest, subscribeCallback_);
  EXPECT_FALSE(res.hasError());
  co_await subscribeDone_;
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}
CO_TEST_P_X(MoQSessionTest, Unsubscribe) {
  co_await setupMoQSession();
  std::shared_ptr<SubgroupConsumer> subgroupConsumer = nullptr;
  std::shared_ptr<TrackConsumer> trackConsumer = nullptr;
  std::shared_ptr<MockSubscriptionHandle> mockSubscriptionHandle = nullptr;
  expectSubscribe(
      [this, &subgroupConsumer, &trackConsumer, &mockSubscriptionHandle](
          auto sub, auto pub) -> TaskSubscribeResult {
        trackConsumer = pub;
        EXPECT_CALL(
            *serverPublisherStatsCallback_, onSubscriptionStreamOpened());
        EXPECT_CALL(
            *clientSubscriberStatsCallback_, onSubscriptionStreamOpened());
        auto pubResult = pub->beginSubgroup(0, 0, 0);
        EXPECT_FALSE(pubResult.hasError());
        subgroupConsumer = pubResult.value();
        subgroupConsumer->object(
            0, moxygen::test::makeBuf(10), Extensions(), false);
        mockSubscriptionHandle =
            makeSubscribeOkResult(sub, AbsoluteLocation{0, 0});
        co_return mockSubscriptionHandle;
      });
  auto subscribeRequest = getSubscribe(kTestTrackName);
  EXPECT_CALL(*clientSubscriberStatsCallback_, onSubscribeSuccess());
  folly::coro::Baton subgroupCreated;
  auto sg = std::make_shared<testing::StrictMock<MockSubgroupConsumer>>();
  EXPECT_CALL(*subscribeCallback_, beginSubgroup(0, 0, 0))
      .WillOnce(testing::Invoke([&]() {
        subgroupCreated.post();
        return sg;
      }));
  EXPECT_CALL(*sg, object(_, _, _, _))
      .WillRepeatedly(testing::Return(folly::unit));
  auto res =
      co_await clientSession_->subscribe(subscribeRequest, subscribeCallback_);
  co_await subgroupCreated;
  auto subscribeHandler = res.value();
  folly::coro::Baton unsubscribeInvoked;
  EXPECT_CALL(*clientSubscriberStatsCallback_, onUnsubscribe());
  EXPECT_CALL(*serverPublisherStatsCallback_, onUnsubscribe());
  EXPECT_CALL(*mockSubscriptionHandle, unsubscribe)
      .WillOnce(testing::Invoke([&]() { unsubscribeInvoked.post(); }));
  EXPECT_CALL(*serverPublisherStatsCallback_, onSubscriptionStreamClosed());
  EXPECT_CALL(*clientSubscriberStatsCallback_, onSubscriptionStreamClosed());
  subscribeHandler->unsubscribe();
  co_await unsubscribeInvoked;
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}
CO_TEST_P_X(MoQSessionTest, SubscribeException) {
  co_await setupMoQSession();
  std::shared_ptr<SubgroupConsumer> subgroupConsumer = nullptr;
  std::shared_ptr<TrackConsumer> trackConsumer = nullptr;
  std::shared_ptr<MockSubscriptionHandle> mockSubscriptionHandle = nullptr;
  EXPECT_CALL(
      *getPublisher(MoQControlCodec::Direction::SERVER), subscribe(_, _))
      .WillOnce(
          testing::Invoke(
              [&](SubscribeRequest /* sub */,
                  std::shared_ptr<TrackConsumer> /* pub */)
                  -> TaskSubscribeResult {
                co_yield folly::coro::co_error(
                    folly::exception_wrapper(
                        std::runtime_error("Unsubscribe unsuccessful")));
              }));
  auto subscribeRequest = getSubscribe(kTestTrackName);
  auto res =
      co_await clientSession_->subscribe(subscribeRequest, subscribeCallback_);
  EXPECT_TRUE(res.hasError());
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}
CO_TEST_P_X(MoQSessionTest, SubscribeDuringDrain) {
  co_await setupMoQSession();

  // Make a FETCH request so that we don't immediately close when drain()
  // is called.
  expectFetch([](Fetch fetch, auto fetchPub) -> TaskFetchResult {
    auto standalone = std::get_if<StandaloneFetch>(&fetch.args);
    EXPECT_NE(standalone, nullptr);
    EXPECT_EQ(fetch.fullTrackName, kTestTrackName);
    fetchPub->object(
        standalone->start.group,
        /*subgroupID=*/0,
        standalone->start.object,
        moxygen::test::makeBuf(100),
        noExtensions(),
        /*finFetch=*/true);
    co_return makeFetchOkResult(fetch, AbsoluteLocation{100, 100});
  });

  EXPECT_CALL(
      *fetchCallback_, object(0, 0, 0, HasChainDataLengthOf(100), _, true));
  auto res =
      co_await clientSession_->fetch(getFetch({0, 0}, {0, 1}), fetchCallback_);
  EXPECT_FALSE(res.hasError());

  clientSession_->drain();

  // Attempting to subscribe during the drain should return an error
  auto subscribeRequest = getSubscribe(kTestTrackName);
  auto subscribeRes =
      co_await clientSession_->subscribe(subscribeRequest, subscribeCallback_);
  EXPECT_TRUE(subscribeRes.hasError());
  EXPECT_EQ(subscribeRes.error().errorCode, SubscribeErrorCode::INTERNAL_ERROR);
}
CO_TEST_P_X(MoQSessionTest, SubscribeOKAfterSubgroup) {
  co_await setupMoQSession();

  std::shared_ptr<SubgroupConsumer> subgroupConsumer = nullptr;
  std::shared_ptr<TrackConsumer> trackConsumer = nullptr;
  std::shared_ptr<MockSubscriptionHandle> mockSubscriptionHandle = nullptr;

  expectSubscribe(
      [&trackConsumer, &mockSubscriptionHandle, this](
          const auto& sub, auto pub) -> TaskSubscribeResult {
        trackConsumer = pub;
        EXPECT_CALL(
            *serverPublisherStatsCallback_, onSubscriptionStreamOpened());
        EXPECT_CALL(
            *clientSubscriberStatsCallback_, onSubscriptionStreamOpened());
        pub->setTrackAlias(TrackAlias(nextAlias_.value++));
        auto pubResult = pub->objectStream(
            ObjectHeader(0, 0, 0, 0, 10), moxygen::test::makeBuf(10));
        EXPECT_FALSE(pubResult.hasError());
        mockSubscriptionHandle =
            makeSubscribeOkResult(sub, AbsoluteLocation{0, 0});
        return folly::coro::co_invoke(
            [mockSubscriptionHandle]() -> TaskSubscribeResult {
              co_await folly::coro::co_reschedule_on_current_executor;
              co_return mockSubscriptionHandle;
            });
      });

  auto subscribeRequest = getSubscribe(kTestTrackName);
  auto sg = std::make_shared<testing::StrictMock<MockSubgroupConsumer>>();
  folly::coro::Baton objectDelivered;

  // Expect object to be delivered after SUBSCRIBE_OK arrives
  EXPECT_CALL(*subscribeCallback_, beginSubgroup(0, 0, 0))
      .WillOnce(testing::Return(sg));
  EXPECT_CALL(*sg, object(0, _, _, true))
      .WillOnce(testing::Invoke([&objectDelivered]() {
        objectDelivered.post();
        return folly::unit;
      }));

  auto res =
      co_await clientSession_->subscribe(subscribeRequest, subscribeCallback_);
  EXPECT_FALSE(res.hasError());
  co_await objectDelivered;
  expectSubscribeDone();
  trackConsumer->subscribeDone(
      getTrackEndedSubscribeDone(subscribeRequest.requestID));
  co_await subscribeDone_;
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}
CO_TEST_P_X(
    MoQSessionTest,
    SubscribeOKArrivesAfterSubgroupTimeout_SubscribeSucceeds) {
  co_await setupMoQSession();
  MoQSettings moqSettings;
  moqSettings.unknownAliasTimeout = std::chrono::milliseconds(500);
  moqSettings.publishDoneStreamCountTimeout = std::chrono::milliseconds(500);
  clientSession_->setMoqSettings(moqSettings);
  std::shared_ptr<TrackConsumer> trackConsumer;
  std::shared_ptr<SubgroupConsumer> subgroupPublisher;
  std::shared_ptr<MockSubscriptionHandle> mockSubscriptionHandle;

  // The app should NOT get onSubgroup or object
  auto sg = std::make_shared<testing::StrictMock<MockSubgroupConsumer>>();
  EXPECT_CALL(*subscribeCallback_, beginSubgroup(_, _, _)).Times(0);
  EXPECT_CALL(*sg, object(_, _, _, _)).Times(0);

  // Simulate server subscribe handler that delays returning SubscribeOk
  expectSubscribe(
      [this, &trackConsumer, &subgroupPublisher, &mockSubscriptionHandle](
          auto sub, auto pub) -> TaskSubscribeResult {
        trackConsumer = pub;
        mockSubscriptionHandle =
            makeSubscribeOkResult(sub, AbsoluteLocation{0, 0});

        // Open subgroup and begin object, but do not return SubscribeOk yet
        eventBase_.add([&subgroupPublisher, pub] {
          // Begin subgroup and begin object with partial payload
          auto sgp = pub->beginSubgroup(0, 0, 0).value();
          subgroupPublisher = sgp;
          // Begin object with length 10, send 5 bytes
          auto beginRes = sgp->beginObject(0, 10, moxygen::test::makeBuf(5));
          EXPECT_TRUE(beginRes.hasValue());
        });

        // Wait longer than the unknown alias timeout
        co_await folly::coro::sleep(std::chrono::seconds(1));

        // Now return SubscribeOk (arrives after timeout)
        co_return mockSubscriptionHandle;
      });

  // Start the subscribe and await it (should succeed, but subgroup timed out)
  auto subscribeRequest = getSubscribe(kTestTrackName);
  auto res =
      co_await clientSession_->subscribe(subscribeRequest, subscribeCallback_);

  // The result should be a success (subscribe still succeeds)
  EXPECT_TRUE(res.hasValue());

  // Now attempt to publish the rest of the object; should return error
  // (subgroupPublisher should be valid, but the stream should be reset/stopped)
  EXPECT_TRUE(subgroupPublisher != nullptr);
  auto payloadRes =
      subgroupPublisher->objectPayload(moxygen::test::makeBuf(5), true);
  EXPECT_TRUE(payloadRes.hasError());
  EXPECT_EQ(payloadRes.error().code, MoQPublishError::CANCELLED);

  // The app should not get onSubgroup or object (enforced by StrictMock above)
  expectSubscribeDone();
  trackConsumer->subscribeDone(
      getTrackEndedSubscribeDone(subscribeRequest.requestID));
  co_await subscribeDone_;

  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}
CO_TEST_P_X(MoQSessionTest, SubscribeOKArrivesOneByteAtATime) {
  co_await setupMoQSession();

  std::shared_ptr<SubgroupConsumer> subgroupConsumer = nullptr;
  std::shared_ptr<TrackConsumer> trackConsumer = nullptr;
  std::shared_ptr<MockSubscriptionHandle> mockSubscriptionHandle = nullptr;

  expectSubscribe(
      [&trackConsumer, this](auto sub, auto pub) -> TaskSubscribeResult {
        trackConsumer = pub;
        EXPECT_CALL(
            *serverPublisherStatsCallback_, onSubscriptionStreamOpened());
        EXPECT_CALL(
            *clientSubscriberStatsCallback_, onSubscriptionStreamOpened());
        auto sgp = pub->beginSubgroup(0, 0, 0).value();
        co_await folly::coro::co_reschedule_on_current_executor;

        serverWt_->writeHandles[2]->setImmediateDelivery(false);
        sgp->object(0, moxygen::test::makeBuf(10), noExtensions(), true);
        for (auto i = 0; i < 3; i++) {
          serverWt_->writeHandles[2]->deliverInflightData(1);
          co_await folly::coro::co_reschedule_on_current_executor;
        }
        serverWt_->writeHandles[2]->setImmediateDelivery(true);
        serverWt_->writeHandles[2]->deliverInflightData();
        co_return makeSubscribeOkResult(sub, AbsoluteLocation{0, 0});
      });

  auto subscribeRequest = getSubscribe(kTestTrackName);
  auto sg = std::make_shared<testing::StrictMock<MockSubgroupConsumer>>();
  EXPECT_CALL(*subscribeCallback_, beginSubgroup(0, 0, 0))
      .WillOnce(testing::Return(sg));
  EXPECT_CALL(*sg, object(_, _, _, _))
      .WillRepeatedly(testing::Return(folly::unit));

  auto res =
      co_await clientSession_->subscribe(subscribeRequest, subscribeCallback_);
  EXPECT_FALSE(res.hasError());

  expectSubscribeDone();
  trackConsumer->subscribeDone(
      getTrackEndedSubscribeDone(subscribeRequest.requestID));
  co_await subscribeDone_;
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}
CO_TEST_P_X(MoQSessionTest, SubscribeOKNeverArrives) {
  co_await setupMoQSession();

  std::shared_ptr<SubgroupConsumer> subgroupConsumer = nullptr;
  std::shared_ptr<TrackConsumer> trackConsumer = nullptr;

  expectSubscribe(
      [&trackConsumer, this](
          const auto& sub, const auto& pub) -> TaskSubscribeResult {
        trackConsumer = pub;
        EXPECT_CALL(
            *serverPublisherStatsCallback_, onSubscriptionStreamOpened());
        EXPECT_CALL(
            *clientSubscriberStatsCallback_, onSubscriptionStreamOpened())
            .Times(0);
        auto sgp = pub->beginSubgroup(0, 0, 0).value();
        sgp->object(0, moxygen::test::makeBuf(10));
        return folly::coro::co_invoke([]() -> TaskSubscribeResult {
          co_await folly::coro::co_reschedule_on_current_executor;
          co_return folly::makeUnexpected(
              SubscribeError{
                  RequestID(0),
                  SubscribeErrorCode::INTERNAL_ERROR,
                  "Subscribe OK never arrived"});
        });
      },
      MoQControlCodec::Direction::SERVER,
      SubscribeErrorCode::INTERNAL_ERROR);

  auto subscribeRequest = getSubscribe(kTestTrackName);

  auto res =
      co_await clientSession_->subscribe(subscribeRequest, subscribeCallback_);
  EXPECT_TRUE(res.hasError());
  // Verify that the publisher's WebTransport received a stop sending on the
  // object stream
  auto waits = 0;
  while (!serverWt_->writeHandles[2]->getWriteErr().hasValue() && waits++ < 6) {
    co_await folly::coro::sleep(std::chrono::seconds(1));
  }
  EXPECT_TRUE(serverWt_->writeHandles[2]->getWriteErr().hasValue());
  EXPECT_EQ(serverWt_->writeHandles[2]->writeException()->error, 0);

  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}
CO_TEST_P_X(MoQSessionTest, SubscriberCancelsBeforeSubscribeOK) {
  co_await setupMoQSession();

  std::shared_ptr<SubgroupConsumer> subgroupConsumer = nullptr;
  std::shared_ptr<TrackConsumer> trackConsumer = nullptr;
  std::shared_ptr<MockSubscriptionHandle> mockSubscriptionHandle = nullptr;
  folly::coro::Baton streamBaton;
  expectSubscribe(
      [&trackConsumer, &mockSubscriptionHandle, &streamBaton, this](
          const auto& sub, auto pub) -> TaskSubscribeResult {
        trackConsumer = pub;
        EXPECT_CALL(
            *serverPublisherStatsCallback_, onSubscriptionStreamOpened());
        auto sgp = pub->beginSubgroup(0, 0, 0).value();
        sgp->object(0, moxygen::test::makeBuf(10));
        streamBaton.post();
        mockSubscriptionHandle =
            makeSubscribeOkResult(sub, AbsoluteLocation{0, 0});
        return folly::coro::co_invoke(
            [mockSubscriptionHandle]() -> TaskSubscribeResult {
              co_await folly::coro::co_reschedule_on_current_executor;
              co_return mockSubscriptionHandle;
            });
      });

  auto subscribeRequest = getSubscribe(kTestTrackName);
  auto sg = std::make_shared<testing::StrictMock<MockSubgroupConsumer>>();
  EXPECT_CALL(*subscribeCallback_, beginSubgroup(0, 0, 0)).Times(0);
  folly::CancellationSource cancelSource;
  auto subscribeFut =
      folly::coro::co_withExecutor(
          &eventBase_,
          folly::coro::co_withCancellation(
              cancelSource.getToken(),
              clientSession_->subscribe(subscribeRequest, subscribeCallback_)))
          .start()
          .via(&eventBase_);
  co_await folly::coro::co_reschedule_on_current_executor;
  cancelSource.requestCancellation();
  EXPECT_THROW(co_await std::move(subscribeFut), folly::OperationCancelled);
  // Verify that the publisher's WebTransport received a stop sending on the
  // object stream
  co_await streamBaton;
  auto waits = 0;
  while (!serverWt_->writeHandles[2]->getWriteErr().hasValue() && waits++ < 6) {
    co_await folly::coro::sleep(std::chrono::milliseconds(250));
  }
  EXPECT_TRUE(serverWt_->writeHandles[2]->getWriteErr().hasValue());
  EXPECT_EQ(serverWt_->writeHandles[2]->writeException()->error, 0);

  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
  // This don't get called by session
  clientSubscriberStatsCallback_->recordSubscribeLatency(0);
}
CO_TEST_P_X(MoQSessionTest, UnsubscribeWithinSubscribeDone) {
  co_await setupMoQSession();
  std::shared_ptr<MockSubscriptionHandle> mockSubscriptionHandle = nullptr;
  std::shared_ptr<TrackConsumer> trackConsumer = nullptr;
  expectSubscribe(
      [&mockSubscriptionHandle, &trackConsumer](
          auto sub, auto pub) -> TaskSubscribeResult {
        mockSubscriptionHandle =
            makeSubscribeOkResult(sub, AbsoluteLocation{0, 0});
        trackConsumer = pub;
        co_return mockSubscriptionHandle;
      });

  auto subscribeRequest = getSubscribe(kTestTrackName);
  auto res =
      co_await clientSession_->subscribe(subscribeRequest, subscribeCallback_);
  auto subscribeHandler = res.value();

  EXPECT_CALL(*clientSubscriberStatsCallback_, onUnsubscribe());

  EXPECT_CALL(*subscribeCallback_, subscribeDone(_))
      .WillOnce(testing::Invoke([&](const auto&) {
        subscribeDone_.post();
        subscribeHandler->unsubscribe();
        return folly::unit;
      }));

  trackConsumer->subscribeDone(
      getTrackEndedSubscribeDone(subscribeRequest.requestID));
  co_await subscribeDone_;
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}
CO_TEST_P_X(MoQSessionTest, SubscribeDoneIgnoredAfterClose) {
  co_await setupMoQSession();

  // Set up a subscription
  RequestID reqID;
  std::shared_ptr<TrackConsumer> handle;
  expectSubscribe([&reqID, &handle](auto sub, auto pub) -> TaskSubscribeResult {
    reqID = sub.requestID;
    // Publish a datagram
    pub->datagram(
        ObjectHeader(0, 0, 0, 0, 11), folly::IOBuf::copyBuffer("hello world"));

    handle = std::move(pub);
    handle->subscribeDone(getTrackEndedSubscribeDone(reqID));
    co_return makeSubscribeOkResult(sub, AbsoluteLocation{0, 0});
  });

  auto subscribeRequest = getSubscribe(kTestTrackName);
  EXPECT_CALL(*subscribeCallback_, datagram(_, _))
      .WillOnce(testing::Invoke([&](auto, auto) {
        clientSession_->close(SessionCloseErrorCode::NO_ERROR);
        handle->subscribeDone(getTrackEndedSubscribeDone(reqID));
        return folly::unit;
      }));
  EXPECT_CALL(*subscribeCallback_, subscribeDone(_))
      .WillOnce(testing::Return(folly::unit));
  auto res =
      co_await clientSession_->subscribe(subscribeRequest, subscribeCallback_);
  EXPECT_FALSE(res.hasError());
}
using V14PlusTests = MoQSessionTest;

INSTANTIATE_TEST_SUITE_P(
    V14PlusTests,
    V14PlusTests,
    testing::Values(
        VersionParams{{kVersionDraft12, kVersionDraft14}, kVersionDraft14}));
CO_TEST_P_X(V14PlusTests, SubscribeUpdateWithRequestID) {
  co_await setupMoQSession();
  std::shared_ptr<MockSubscriptionHandle> mockSubscriptionHandle = nullptr;
  std::shared_ptr<TrackConsumer> trackConsumer = nullptr;
  expectSubscribeDone();
  expectSubscribe(
      [&mockSubscriptionHandle, &trackConsumer](
          auto sub, auto pub) -> TaskSubscribeResult {
        trackConsumer = pub;
        mockSubscriptionHandle =
            makeSubscribeOkResult(sub, AbsoluteLocation{0, 0});
        co_return mockSubscriptionHandle;
      });

  auto subscribeRequest = getSubscribe(kTestTrackName);
  auto res =
      co_await clientSession_->subscribe(subscribeRequest, subscribeCallback_);
  auto subscribeHandler = res.value();

  SubscribeUpdate subscribeUpdate{
      RequestID(0), // Will be assigned by session based on version
      subscribeRequest.requestID,
      AbsoluteLocation{0, 0},
      10,
      kDefaultPriority + 1,
      true,
      {}};

  EXPECT_CALL(*clientSubscriberStatsCallback_, onSubscribeUpdate());
  EXPECT_CALL(*serverPublisherStatsCallback_, onSubscribeUpdate());

  subscribeHandler->subscribeUpdate(subscribeUpdate);
  folly::coro::Baton subscribeUpdateInvoked;
  EXPECT_CALL(*mockSubscriptionHandle, subscribeUpdate)
      .WillOnce(testing::Invoke([&](const auto& actualUpdate) {
        // Verify that subscriptionRequestID has original requestID value
        EXPECT_EQ(
            actualUpdate.subscriptionRequestID.value,
            subscribeRequest.requestID.value);
        // Verify that requestID has been assigned by session
        EXPECT_EQ(
            actualUpdate.requestID.value, subscribeRequest.requestID.value + 2);
        subscribeUpdateInvoked.post();
      }));
  co_await subscribeUpdateInvoked;
  trackConsumer->subscribeDone(
      getTrackEndedSubscribeDone(subscribeRequest.requestID));
  co_await subscribeDone_;
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}
CO_TEST_P_X(MoQSessionTest, ServerClosesDuringSubscribeHandler) {
  co_await setupMoQSession();

  // The server's subscribe handler will close the server session immediately
  expectSubscribe([this](auto sub, auto pub) -> TaskSubscribeResult {
    // Close the server session as soon as the subscribe handler is invoked
    serverSession_->close(SessionCloseErrorCode::INTERNAL_ERROR);
    // Optionally, return a valid handle (should not matter)
    co_return makeSubscribeOkResult(sub, AbsoluteLocation{0, 0});
  });

  // The client should see an error or session closure
  auto subscribeRequest = getSubscribe(kTestTrackName);
  auto res =
      co_await clientSession_->subscribe(subscribeRequest, subscribeCallback_);
  // The result may be an error due to the server closing the session
  EXPECT_TRUE(res.hasError());
  // Optionally, check the error code if available
  if (res.hasError()) {
    EXPECT_EQ(res.error().errorCode, SubscribeErrorCode::INTERNAL_ERROR);
  }
  // Ensure the client session is also closed
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

// Test for UAF fix: handle unsubscribe delete immediately after subscribe
// This tests the fix where unsubscribe is called right after subscribe returns,
// ensuring the code detects cancellation and avoids accessing deleted state.
CO_TEST_P_X(MoQSessionTest, UnsubscribeImmediatelyAfterSubscribeReturns) {
  co_await setupMoQSession();

  expectSubscribe(
      [this](auto sub, auto pub) -> TaskSubscribeResult {
        auto sgp = pub->beginSubgroup(0, 0, 0);
        XCHECK(sgp.hasValue());
        sgp.value()->object(0, moxygen::test::makeBuf(10));
        serverWt_->writeHandles[2]->setImmediateDelivery(false);
        serverWt_->writeHandles[2]->deliverInflightData(2); // type and alias
        co_await folly::coro::co_reschedule_on_current_executor;
        co_await folly::coro::co_reschedule_on_current_executor;
        auto result = makeSubscribeOkResult(sub);
        co_return result;
      },
      MoQControlCodec::Direction::SERVER);

  // Create a mock callback
  auto callback = std::make_shared<testing::StrictMock<MockTrackConsumer>>();
  EXPECT_CALL(*callback, setTrackAlias(_))
      .WillOnce(
          testing::Return(
              folly::Expected<folly::Unit, MoQPublishError>(folly::unit)));
  auto subscribeRequest = getSubscribe(kTestTrackName);
  auto res = co_await clientSession_->subscribe(subscribeRequest, callback);
  EXPECT_FALSE(res.hasError());
  // subgroup unblocked, but unsubscribe removes state
  res.value()->unsubscribe();

  co_await folly::coro::co_reschedule_on_current_executor;

  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

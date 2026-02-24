/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/test/MoQSessionTestCommon.h"

using namespace moxygen;
using namespace moxygen::test;
using testing::_;

// === FETCH tests ===

CO_TEST_P_X(MoQSessionTest, Fetch) {
  co_await setupMoQSession();
  // Usage
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

  folly::coro::Baton baton;
  EXPECT_CALL(
      *fetchCallback_, object(0, 0, 0, HasChainDataLengthOf(100), _, true, _))
      .WillOnce([&] {
        baton.post();
        return folly::unit;
      });
  expectFetchSuccess();
  EXPECT_CALL(*clientSubscriberStatsCallback_, recordFetchLatency(_));
  auto res =
      co_await clientSession_->fetch(getFetch({0, 0}, {0, 1}), fetchCallback_);
  EXPECT_FALSE(res.hasError());
  co_await baton;
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}
CO_TEST_P_X(MoQSessionTest, RelativeJoiningFetch) {
  co_await setupMoQSession();
  expectSubscribe([](auto sub, auto pub) -> TaskSubscribeResult {
    pub->datagram(
        ObjectHeader(0, 0, 1, 0, 11), folly::IOBuf::copyBuffer("hello world"));
    pub->publishDone(getTrackEndedPublishDone(sub.requestID));
    co_return makeSubscribeOkResult(sub, AbsoluteLocation{0, 0});
  });
  expectFetch([](Fetch fetch, auto fetchPub) -> TaskFetchResult {
    auto joining = std::get_if<JoiningFetch>(&fetch.args);
    EXPECT_NE(joining, nullptr);
    EXPECT_EQ(fetch.fullTrackName, FullTrackName(kTestTrackName));
    fetchPub->object(
        /*groupID=*/0,
        /*subgroupID=*/0,
        /*objectID=*/0,
        moxygen::test::makeBuf(100),
        noExtensions(),
        /*finFetch=*/true);
    co_return makeFetchOkResult(fetch, AbsoluteLocation{100, 100});
  });
  EXPECT_CALL(*subscribeCallback_, datagram(_, _, _))
      .WillOnce([&](const auto& header, auto, bool) {
        EXPECT_EQ(header.length, 11);
        return folly::unit;
      });
  expectPublishDone();
  folly::coro::Baton fetchBaton;
  EXPECT_CALL(
      *fetchCallback_, object(0, 0, 0, HasChainDataLengthOf(100), _, true, _))
      .WillOnce([&] {
        fetchBaton.post();
        return folly::unit;
      });
  auto res = co_await clientSession_->join(
      getSubscribe(kTestTrackName),
      subscribeCallback_,
      1,
      129,
      GroupOrder::Default,
      {},
      fetchCallback_,
      FetchType::RELATIVE_JOINING);
  EXPECT_FALSE(res.subscribeResult.hasError());
  EXPECT_FALSE(res.fetchResult.hasError());
  co_await publishDone_;
  co_await fetchBaton;
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}
CO_TEST_P_X(MoQSessionTest, BadRelativeJoiningFetch) {
  co_await setupMoQSession();
  auto res = co_await clientSession_->fetch(
      Fetch(
          RequestID(0),
          RequestID(17),
          1,
          FetchType::RELATIVE_JOINING,
          128,
          GroupOrder::Default),
      fetchCallback_);
  EXPECT_TRUE(res.hasError());
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}
CO_TEST_P_X(MoQSessionTest, AbsoluteJoiningFetch) {
  co_await setupMoQSession();
  expectSubscribe([](auto sub, auto pub) -> TaskSubscribeResult {
    for (uint32_t group = 6; group < 10; group++) {
      pub->datagram(
          ObjectHeader(group, 0, 0, 0, 11),
          folly::IOBuf::copyBuffer("hello world"));
    }
    pub->publishDone(getTrackEndedPublishDone(sub.requestID));
    co_return makeSubscribeOkResult(sub, AbsoluteLocation{0, 0});
  });
  expectFetch([](Fetch fetch, auto fetchPub) -> TaskFetchResult {
    auto joining = std::get_if<JoiningFetch>(&fetch.args);
    EXPECT_NE(joining, nullptr);
    EXPECT_EQ(fetch.fullTrackName, FullTrackName(kTestTrackName));
    for (uint32_t group = 2; group < 6; group++) {
      auto objectPubResult = fetchPub->object(
          /*groupID=*/group,
          /*subgroupID=*/0,
          /*objectID=*/0,
          moxygen::test::makeBuf(100),
          noExtensions(),
          /*finFetch=*/(group == 5));
      EXPECT_TRUE(objectPubResult.hasValue());
    }
    co_return makeFetchOkResult(fetch, AbsoluteLocation{100, 100});
  });
  EXPECT_CALL(*subscribeCallback_, datagram(_, _, _))
      .WillRepeatedly([&](const auto& header, auto, bool) {
        EXPECT_EQ(header.length, 11);
        return folly::unit;
      });
  expectPublishDone();
  folly::coro::Baton fetchBaton;
  for (uint32_t group = 2; group < 6; group++) {
    EXPECT_CALL(
        *fetchCallback_,
        object(group, 0, 0, HasChainDataLengthOf(100), _, _, _))
        .WillRepeatedly([&] {
          fetchBaton.post();
          return folly::unit;
        });
  }
  auto res = co_await clientSession_->join(
      getSubscribe(kTestTrackName),
      subscribeCallback_,
      2 /* joiningStart */,
      129 /* fetchPri */,
      GroupOrder::Default,
      {},
      fetchCallback_,
      FetchType::ABSOLUTE_JOINING);
  EXPECT_FALSE(res.subscribeResult.hasError());
  EXPECT_FALSE(res.fetchResult.hasError());
  co_await publishDone_;
  co_await fetchBaton;
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}
CO_TEST_P_X(MoQSessionTest, BadAbsoluteJoiningFetch) {
  co_await setupMoQSession();
  auto res = co_await clientSession_->fetch(
      Fetch(
          RequestID(0),
          RequestID(17),
          1,
          FetchType::ABSOLUTE_JOINING,
          128,
          GroupOrder::Default),
      fetchCallback_);
  EXPECT_TRUE(res.hasError());
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}
CO_TEST_P_X(MoQSessionTest, FetchCleanupFromStreamFin) {
  co_await setupMoQSession();

  std::shared_ptr<FetchConsumer> fetchPub;
  expectFetch([&fetchPub](Fetch fetch, auto inFetchPub) -> TaskFetchResult {
    EXPECT_EQ(fetch.fullTrackName, kTestTrackName);
    fetchPub = std::move(inFetchPub);
    co_return makeFetchOkResult(fetch, AbsoluteLocation{100, 100});
  });

  expectFetchSuccess();
  auto res =
      co_await clientSession_->fetch(getFetch({0, 0}, {0, 1}), fetchCallback_);
  EXPECT_FALSE(res.hasError());
  // publish here now we know FETCH_OK has been received at client
  XCHECK(fetchPub);
  fetchPub->object(
      /*groupID=*/0,
      /*subgroupID=*/0,
      /*objectID=*/0,
      moxygen::test::makeBuf(100),
      noExtensions(),
      /*finFetch=*/true);
  folly::coro::Baton baton;
  EXPECT_CALL(
      *fetchCallback_, object(0, 0, 0, HasChainDataLengthOf(100), _, true, _))
      .WillOnce([&] {
        baton.post();
        return folly::unit;
      });
  co_await baton;
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}
CO_TEST_P_X(MoQSessionTest, FetchError) {
  co_await setupMoQSession();
  EXPECT_CALL(
      *serverPublisherStatsCallback_,
      onFetchError(FetchErrorCode::INVALID_RANGE));
  EXPECT_CALL(
      *clientSubscriberStatsCallback_,
      onFetchError(FetchErrorCode::INVALID_RANGE));
  auto res =
      co_await clientSession_->fetch(getFetch({0, 2}, {0, 1}), fetchCallback_);
  EXPECT_TRUE(res.hasError());
  EXPECT_EQ(res.error().errorCode, FetchErrorCode::INVALID_RANGE);
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}
CO_TEST_P_X(MoQSessionTest, FetchPublisherError) {
  co_await setupMoQSession();
  expectFetch(
      [](Fetch fetch, auto) -> TaskFetchResult {
        co_return folly::makeUnexpected(
            FetchError{
                fetch.requestID,
                FetchErrorCode::TRACK_NOT_EXIST,
                "Bad trackname"});
      },
      FetchErrorCode::TRACK_NOT_EXIST);
  auto res =
      co_await clientSession_->fetch(getFetch({0, 0}, {0, 1}), fetchCallback_);
  EXPECT_TRUE(res.hasError());
  EXPECT_EQ(res.error().errorCode, FetchErrorCode::TRACK_NOT_EXIST);
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}
CO_TEST_P_X(MoQSessionTest, FetchPublisherThrow) {
  co_await setupMoQSession();
  expectFetch(
      [](Fetch fetch, auto) -> TaskFetchResult {
        throw std::runtime_error("panic!");
        co_return makeFetchOkResult(fetch, AbsoluteLocation{100, 100});
      },
      FetchErrorCode::INTERNAL_ERROR);
  auto res =
      co_await clientSession_->fetch(getFetch({0, 0}, {0, 1}), fetchCallback_);
  EXPECT_TRUE(res.hasError());
  EXPECT_EQ(res.error().errorCode, FetchErrorCode::INTERNAL_ERROR);
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}
CO_TEST_P_X(MoQSessionTest, FetchCancel) {
  co_await setupMoQSession();
  std::shared_ptr<FetchConsumer> fetchPub;
  expectFetch([&fetchPub](Fetch fetch, auto inFetchPub) -> TaskFetchResult {
    auto standalone = std::get_if<StandaloneFetch>(&fetch.args);
    EXPECT_NE(standalone, nullptr);
    EXPECT_EQ(fetch.fullTrackName, kTestTrackName);
    fetchPub = std::move(inFetchPub);
    fetchPub->object(
        standalone->start.group,
        /*subgroupID=*/0,
        standalone->start.object,
        moxygen::test::makeBuf(100));
    // published 1 object
    co_return makeFetchOkResult(fetch, AbsoluteLocation{100, 100});
  });
  EXPECT_CALL(
      *fetchCallback_, object(0, 0, 0, HasChainDataLengthOf(100), _, false, _))
      .WillOnce(testing::Return(folly::unit));
  // TODO: fetchCancel removes the callback - should it also deliver a
  // reset() call to the callback?
  // EXPECT_CALL(*fetchCallback, reset(ResetStreamErrorCode::CANCELLED));
  expectFetchSuccess();
  auto res =
      co_await clientSession_->fetch(getFetch({0, 0}, {0, 2}), fetchCallback_);
  EXPECT_FALSE(res.hasError());
  res.value()->fetchCancel();
  co_await folly::coro::co_reschedule_on_current_executor;
  co_await folly::coro::co_reschedule_on_current_executor;
  co_await folly::coro::co_reschedule_on_current_executor;
  XCHECK(fetchPub);
  auto res2 = fetchPub->object(
      /*groupID=*/0,
      /*subgroupID=*/0,
      /*objectID=*/1,
      moxygen::test::makeBuf(100),
      noExtensions(),
      /*finFetch=*/true);
  // publish after fetchCancel fails
  EXPECT_TRUE(res2.hasError());
  co_await folly::coro::co_reschedule_on_current_executor;
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}
CO_TEST_P_X(MoQSessionTest, FetchEarlyCancel) {
  co_await setupMoQSession();
  expectFetch([](Fetch fetch, auto) -> TaskFetchResult {
    EXPECT_EQ(fetch.fullTrackName, kTestTrackName);
    co_return makeFetchOkResult(fetch, AbsoluteLocation{100, 100});
  });
  expectFetchSuccess();
  auto res =
      co_await clientSession_->fetch(getFetch({0, 0}, {0, 2}), fetchCallback_);
  EXPECT_FALSE(res.hasError());
  // TODO: this no-ops right now so there's nothing to verify
  res.value()->fetchCancel();
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}
CO_TEST_P_X(MoQSessionTest, FetchBadLength) {
  co_await setupMoQSession();
  expectFetch([](Fetch fetch, auto fetchPub) -> TaskFetchResult {
    auto standalone = std::get_if<StandaloneFetch>(&fetch.args);
    EXPECT_NE(standalone, nullptr);
    EXPECT_EQ(fetch.fullTrackName, kTestTrackName);
    auto objPub = fetchPub->beginObject(
        standalone->start.group,
        /*subgroupID=*/0,
        standalone->start.object,
        100,
        moxygen::test::makeBuf(10));
    // this should close the session too
    fetchPub->endOfFetch();
    co_return makeFetchOkResult(fetch, AbsoluteLocation{100, 100});
  });
  expectFetchSuccess();
  auto res =
      co_await clientSession_->fetch(getFetch({0, 0}, {0, 1}), fetchCallback_);
  EXPECT_FALSE(res.hasError());
  // FETCH_OK comes but the FETCH stream is reset and we timeout waiting
  // for a new object.
  auto contract = folly::coro::makePromiseContract<folly::Unit>();
  ON_CALL(*fetchCallback_, beginObject(_, _, _, _, _, _)).WillByDefault([&] {
    contract.first.setValue();
    return folly::Expected<folly::Unit, MoQPublishError>(folly::unit);
  });
  EXPECT_THROW(
      co_await folly::coro::timeout(
          std::move(contract.second), std::chrono::milliseconds(100)),
      folly::FutureTimeout);
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}
CO_TEST_P_X(MoQSessionTest, FetchOverLimit) {
  co_await setupMoQSession();
  expectFetch([](Fetch fetch, auto) -> TaskFetchResult {
    EXPECT_EQ(fetch.fullTrackName, kTestTrackName);
    co_return makeFetchOkResult(fetch, AbsoluteLocation{100, 100});
  });
  expectFetch([](Fetch fetch, auto) -> TaskFetchResult {
    EXPECT_EQ(fetch.fullTrackName, kTestTrackName);
    co_return makeFetchOkResult(fetch, AbsoluteLocation{100, 100});
  });

  auto fetchCallback1 =
      std::make_shared<testing::StrictMock<MockFetchConsumer>>();
  auto fetchCallback2 =
      std::make_shared<testing::StrictMock<MockFetchConsumer>>();
  auto fetchCallback3 =
      std::make_shared<testing::StrictMock<MockFetchConsumer>>();
  Fetch fetch = getFetch({0, 0}, {0, 1});
  EXPECT_CALL(*clientSubscriberStatsCallback_, onFetchSuccess()).Times(2);
  auto res = co_await clientSession_->fetch(fetch, fetchCallback1);
  res = co_await clientSession_->fetch(fetch, fetchCallback2);
  EXPECT_CALL(
      *clientSubscriberStatsCallback_,
      onFetchError(FetchErrorCode::INTERNAL_ERROR));
  res = co_await clientSession_->fetch(fetch, fetchCallback3);
  EXPECT_TRUE(res.hasError());
}
CO_TEST_P_X(MoQSessionTest, FetchOutOfOrder) {
  co_await setupMoQSession();
  std::shared_ptr<FetchConsumer> fetchPub;
  expectFetch(
      [&fetchPub, this](Fetch fetch, auto inFetchPub) -> TaskFetchResult {
        EXPECT_EQ(fetch.fullTrackName, kTestTrackName);
        fetchPub = std::move(inFetchPub);
        eventBase_.add([this, fetchPub] {
          fetchPub->object(1, 0, 1, moxygen::test::makeBuf(100));
          // delay the bad API call one more loop, so the FETCH_HEADER comes
          // through - can remove with checkpoint() someday
          eventBase_.add([fetchPub] {
            // object 0 after object 1
            EXPECT_EQ(
                fetchPub->object(1, 0, 0, moxygen::test::makeBuf(100))
                    .error()
                    .code,
                MoQPublishError::API_ERROR);
            // group 0 after group 1
            EXPECT_EQ(
                fetchPub->object(0, 0, 2, moxygen::test::makeBuf(100))
                    .error()
                    .code,
                MoQPublishError::API_ERROR);
            EXPECT_EQ(
                fetchPub->beginObject(0, 0, 0, 100, test::makeBuf(10))
                    .error()
                    .code,
                MoQPublishError::API_ERROR);
            EXPECT_EQ(
                fetchPub->endOfGroup(0, 0, 0).error().code,
                MoQPublishError::API_ERROR);
            EXPECT_EQ(
                fetchPub->endOfTrackAndGroup(0, 0, 0).error().code,
                MoQPublishError::API_ERROR);
            // writeHandle gone
            EXPECT_EQ(
                fetchPub->endOfFetch().error().code,
                MoQPublishError::CANCELLED);
          });
        });
        co_return makeFetchOkResult(fetch, AbsoluteLocation{100, 100});
      });

  EXPECT_CALL(
      *fetchCallback_, object(1, 0, 1, HasChainDataLengthOf(100), _, false, _))
      .WillOnce(testing::Return(folly::unit));
  EXPECT_CALL(*fetchCallback_, reset(ResetStreamErrorCode::INTERNAL_ERROR));

  auto res = co_await clientSession_->fetch(
      getFetch(kLocationMin, kLocationMax), fetchCallback_);
  EXPECT_FALSE(res.hasError());
  co_await folly::coro::co_reschedule_on_current_executor;
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

// Test that when a fetch callback returns an error, the fetch stream terminates
CO_TEST_P_X(MoQSessionTest, FetchCallbackErrorTerminatesStream) {
  co_await setupMoQSession();
  std::shared_ptr<FetchConsumer> fetchConsumer;
  expectFetch(
      [this, &fetchConsumer](auto fetch, auto consumer) -> TaskFetchResult {
        fetchConsumer = consumer;
        eventBase_.add([consumer, fetch] {
          // Send first object - should succeed
          auto res1 = consumer->object(
              0, 0, 0, moxygen::test::makeBuf(10), noExtensions(), false);
          // Send second object - consumer will error, stream should terminate
          auto res2 = consumer->object(
              0, 0, 1, moxygen::test::makeBuf(10), noExtensions(), false);
          // Send third object - should not be delivered
          auto res3 = consumer->object(
              0, 0, 2, moxygen::test::makeBuf(10), noExtensions(), false);
        });
        co_return makeFetchOkResult(fetch, AbsoluteLocation{0, 0});
      });

  {
    testing::InSequence enforceOrder;
    // First object succeeds
    EXPECT_CALL(*fetchCallback_, object(0, 0, 0, _, _, false, _))
        .WillOnce(testing::Return(folly::unit));
    // Second object returns error
    EXPECT_CALL(*fetchCallback_, object(0, 0, 1, _, _, false, _))
        .WillOnce(
            testing::Return(
                folly::makeUnexpected(MoQPublishError(
                    MoQPublishError::CANCELLED, "test fetch error"))));
    // Third object should NOT be delivered
    EXPECT_CALL(*fetchCallback_, object(0, 0, 2, _, _, false, _)).Times(0);
  }
  // The ERROR_TERMINATE from the object error triggers reset on the
  // FetchConsumer to properly clean up the stream state.
  EXPECT_CALL(*fetchCallback_, reset(_)).Times(1);

  auto fetch = getFetch(AbsoluteLocation{0, 0}, AbsoluteLocation{0, 2});
  auto res = co_await clientSession_->fetch(fetch, fetchCallback_);
  EXPECT_FALSE(res.hasError());

  co_await folly::coro::co_reschedule_on_current_executor;
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

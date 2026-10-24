/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/FillFetch.h"
#include "moxygen/test/MoQSessionTestCommon.h"

#include "moxygen/MoQTrackProperties.h"

using namespace moxygen;
using namespace moxygen::test;
using testing::_;

CO_TEST_P_X(Draft22Test, FetchOmitsUnrequestedTrackProperties) {
  co_await setupMoQSession();
  expectFetch([](Fetch fetch, auto) -> TaskFetchResult {
    EXPECT_FALSE(fetch.includeProperties);
    auto ok = makeFetchOkResult(fetch, AbsoluteLocation{0, 1})->fetchOk();
    ok.extensions.insertMutableExtension(
        Extension{kPublisherPriorityExtensionType, 100});
    co_return std::make_shared<MockFetchHandle>(std::move(ok));
  });
  expectFetchSuccess();

  auto request = getFetch({0, 0}, {0, 1});
  request.includeProperties = false;
  auto result = co_await clientSession_->fetch(request, fetchCallback_);
  EXPECT_TRUE(result.hasValue());
  if (result.hasError()) {
    clientSession_->close(SessionCloseErrorCode::NO_ERROR);
    co_return;
  }
  EXPECT_TRUE(result.value()->fetchOk().extensions.empty());

  EXPECT_CALL(*fetchCallback_, reset(_));
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

// === FETCH tests ===

CO_TEST_P_X(Draft18Test, FetchSerializationErrorDoesNotSendRequest) {
  co_await setupMoQSession();

  auto fetch = getFetch({0, 0}, {0, 1});
  fetch.params.setMajorVersion(22);
  auto insertResult = fetch.params.insertParam(Parameter(
      folly::to_underlying(TrackRequestParamKey::INCLUDE_PROPERTIES),
      uint64_t(0)));
  EXPECT_TRUE(insertResult.hasValue());
  if (insertResult.hasError()) {
    clientSession_->close(SessionCloseErrorCode::NO_ERROR);
    co_return;
  }

  EXPECT_CALL(
      *clientSubscriberStatsCallback_,
      onFetchError(FetchErrorCode::INTERNAL_ERROR));
  EXPECT_CALL(*clientSubscriberStatsCallback_, recordFetchLatency(_));
  auto result = co_await clientSession_->fetch(fetch, fetchCallback_);
  EXPECT_TRUE(result.hasError());
  if (result.hasError()) {
    EXPECT_EQ(result.error().errorCode, FetchErrorCode::INTERNAL_ERROR);
    EXPECT_EQ(result.error().reasonPhrase, "local write failed");
  }

  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

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

// endOfGroup on a FETCH stream serializes nothing. If it did, the
// subscriber would read those bytes as the next object and desync.
CO_TEST_P_X(MoQSessionTest, FetchEndOfGroupBetweenObjects) {
  co_await setupMoQSession();
  expectFetch([](Fetch fetch, auto fetchPub) -> TaskFetchResult {
    EXPECT_TRUE(
        fetchPub->object(0, 0, 0, moxygen::test::makeBuf(100), noExtensions())
            .hasValue());
    EXPECT_TRUE(fetchPub->endOfGroup(0, 0, 1, /*finFetch=*/false).hasValue());
    EXPECT_TRUE(fetchPub
                    ->object(
                        1,
                        0,
                        0,
                        moxygen::test::makeBuf(100),
                        noExtensions(),
                        /*finFetch=*/true)
                    .hasValue());
    co_return makeFetchOkResult(fetch, AbsoluteLocation{1, 1});
  });

  folly::coro::Baton baton;
  {
    testing::InSequence seq;
    EXPECT_CALL(
        *fetchCallback_,
        object(0, 0, 0, HasChainDataLengthOf(100), _, false, _))
        .WillOnce(testing::Return(folly::unit));
    EXPECT_CALL(
        *fetchCallback_, object(1, 0, 0, HasChainDataLengthOf(100), _, true, _))
        .WillOnce([&] {
          baton.post();
          return folly::unit;
        });
  }
  expectFetchSuccess();
  EXPECT_CALL(*clientSubscriberStatsCallback_, recordFetchLatency(_));
  auto res =
      co_await clientSession_->fetch(getFetch({0, 0}, {1, 1}), fetchCallback_);
  EXPECT_FALSE(res.hasError());
  co_await baton;
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

CO_TEST_P_X(MoQSessionTest, FetchNewestFirstResponseObjects) {
  if (getDraftMajorVersion(getServerSelectedVersion()) < 18) {
    co_return;
  }

  co_await setupMoQSession();
  expectFetch([](Fetch fetch, auto fetchPub) -> TaskFetchResult {
    EXPECT_EQ(fetch.groupOrder, GroupOrder::NewestFirst);
    auto standalone = std::get_if<StandaloneFetch>(&fetch.args);
    EXPECT_NE(standalone, nullptr);
    EXPECT_EQ(fetch.fullTrackName, kTestTrackName);
    auto first = fetchPub->object(
        /*groupID=*/10,
        /*subgroupID=*/0,
        /*objectID=*/5,
        moxygen::test::makeBuf(100),
        noExtensions());
    EXPECT_TRUE(first.hasValue());
    auto second = fetchPub->object(
        /*groupID=*/7,
        /*subgroupID=*/0,
        /*objectID=*/1,
        moxygen::test::makeBuf(100),
        noExtensions(),
        /*finFetch=*/true);
    EXPECT_TRUE(second.hasValue());
    co_return std::make_shared<MockFetchHandle>(FetchOk{
        fetch.requestID,
        GroupOrder::NewestFirst,
        /*endOfTrack=*/false,
        AbsoluteLocation{7, 1}});
  });

  folly::coro::Baton baton;
  {
    testing::InSequence seq;
    EXPECT_CALL(
        *fetchCallback_,
        object(10, 0, 5, HasChainDataLengthOf(100), _, false, _))
        .WillOnce(testing::Return(folly::unit));
    EXPECT_CALL(
        *fetchCallback_, object(7, 0, 1, HasChainDataLengthOf(100), _, true, _))
        .WillOnce([&] {
          baton.post();
          return folly::unit;
        });
  }
  expectFetchSuccess();
  auto fetch = getFetch({7, 1}, {10, 5});
  fetch.groupOrder = GroupOrder::NewestFirst;
  auto res = co_await clientSession_->fetch(fetch, fetchCallback_);
  EXPECT_FALSE(res.hasError());
  co_await baton;
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

CO_TEST_P_X(MoQSessionTest, RelativeJoiningFetch) {
  co_await setupMoQSession();
  expectSubscribe([this](auto sub, auto pub) -> TaskSubscribeResult {
    serveFill(sub.requestID, sub.fill, pub, AbsoluteLocation{0, 0});
    pub->datagram(
        ObjectHeader(0, 0, 1, 0, 11), folly::IOBuf::copyBuffer("hello world"));
    pub->publishDone(getTrackEndedPublishDone(sub.requestID));
    co_return makeSubscribeOkResult(sub, AbsoluteLocation{0, 0});
  });
  auto fillFetch = useFillFetch(GetParam().serverVersion);
  auto serveJoin = [fillFetch](Fetch fetch, auto fetchPub) -> TaskFetchResult {
    // Draft 20 serves the join as a fill with a resolved standalone range.
    if (fillFetch) {
      auto standalone = std::get_if<StandaloneFetch>(&fetch.args);
      EXPECT_NE(standalone, nullptr);
      if (standalone) {
        EXPECT_EQ(standalone->start, (AbsoluteLocation{0, 0}));
      }
    } else {
      EXPECT_NE(std::get_if<JoiningFetch>(&fetch.args), nullptr);
    }
    EXPECT_EQ(fetch.fullTrackName, FullTrackName(kTestTrackName));
    fetchPub->object(
        /*groupID=*/0,
        /*subgroupID=*/0,
        /*objectID=*/0,
        moxygen::test::makeBuf(100),
        noExtensions(),
        /*finFetch=*/true);
    co_return makeFetchOkResult(fetch, AbsoluteLocation{100, 100});
  };
  if (fillFetch) {
    expectFillFetch(serveJoin);
  } else {
    expectFetch(serveJoin);
  }
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
  EXPECT_CALL(*clientSubscriberStatsCallback_, onSubscriptionStreamOpened())
      .Times(0);
  EXPECT_CALL(*clientSubscriberStatsCallback_, onSubscriptionStreamClosed())
      .Times(0);
  expectBeginFill();
  auto res = co_await clientSession_->subscribe(
      subscribeWithFill(
          kTestTrackName, /*joiningStart=*/1, /*fillPriority=*/129),
      subscribeCallback_);
  EXPECT_FALSE(res.hasError());
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
  // The join starts after Largest Object, so nothing is backfilled. Only drafts
  // below 20 still answer the Joining FETCH.
  auto fillFetch = useFillFetch(GetParam().serverVersion);
  if (!fillFetch) {
    expectFetch([](Fetch fetch, auto fetchPub) -> TaskFetchResult {
      EXPECT_NE(std::get_if<JoiningFetch>(&fetch.args), nullptr);
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
  }
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
  EXPECT_CALL(*clientSubscriberStatsCallback_, onSubscriptionStreamOpened())
      .Times(0);
  EXPECT_CALL(*clientSubscriberStatsCallback_, onSubscriptionStreamClosed())
      .Times(0);
  expectBeginFill();
  if (fillFetch) {
    // An empty fill gets only the reset at subscription end.
    EXPECT_CALL(*fetchCallback_, reset(ResetStreamErrorCode::CANCELLED));
  }
  auto sub = subscribeWithFill(kTestTrackName, 0, /*fillPriority=*/129);
  sub.fill->locationFilter = LocationFilter::absoluteStart({2, 0});
  auto res = co_await clientSession_->subscribe(sub, subscribeCallback_);
  EXPECT_FALSE(res.hasError());
  co_await publishDone_;
  if (!fillFetch) {
    co_await fetchBaton;
  }
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

// The Joining FETCH that carries a fill names the SUBSCRIBE's request ID.
CO_TEST_P_X(MoQSessionTest, FillJoiningFetchNamesSubscribeRequestID) {
  if (useFillFetch(getServerSelectedVersion())) {
    // Draft 20 does not send a Joining FETCH for a fill.
    co_return;
  }
  co_await setupMoQSession();
  // Both are captured rather than compared in the handlers: the handlers race
  // under collectAll, and RequestID{} is 0, which is a real ID.
  std::optional<RequestID> subscribeRequestID;
  std::optional<RequestID> joiningRequestID;
  expectSubscribe(
      [&subscribeRequestID](auto sub, auto pub) -> TaskSubscribeResult {
        subscribeRequestID = sub.requestID;
        pub->datagram(
            ObjectHeader(0, 0, 1, 0, 11),
            folly::IOBuf::copyBuffer("hello world"));
        pub->publishDone(getTrackEndedPublishDone(sub.requestID));
        co_return makeSubscribeOkResult(sub, AbsoluteLocation{0, 0});
      });
  expectFetch(
      [&joiningRequestID](Fetch fetch, auto fetchPub) -> TaskFetchResult {
        auto joining = std::get_if<JoiningFetch>(&fetch.args);
        EXPECT_NE(joining, nullptr);
        if (joining) {
          joiningRequestID = joining->joiningRequestID;
        }
        EXPECT_EQ(fetch.fullTrackName, FullTrackName(kTestTrackName));
        auto objectPubResult = fetchPub->object(
            /*groupID=*/0,
            /*subgroupID=*/0,
            /*objectID=*/0,
            moxygen::test::makeBuf(100),
            noExtensions(),
            /*finFetch=*/true);
        EXPECT_TRUE(objectPubResult.hasValue());
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
  EXPECT_CALL(*clientSubscriberStatsCallback_, onSubscriptionStreamOpened())
      .Times(0);
  EXPECT_CALL(*clientSubscriberStatsCallback_, onSubscriptionStreamClosed())
      .Times(0);
  expectBeginFill();
  auto res = co_await clientSession_->subscribe(
      subscribeWithFill(
          kTestTrackName, /*joiningStart=*/1, /*fillPriority=*/129),
      subscribeCallback_);
  EXPECT_FALSE(res.hasError());
  co_await publishDone_;
  co_await fetchBaton;
  // Both handlers have run by now. Requiring subscribeRequestID to be engaged
  // keeps the comparison below from passing on two nullopts.
  EXPECT_TRUE(subscribeRequestID.has_value());
  EXPECT_EQ(joiningRequestID, subscribeRequestID);
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

// Below draft 20, a fill for an absolute range goes out as a standalone FETCH
// for exactly that range.
CO_TEST_P_X(MoQSessionTest, FillAbsoluteRangeIsAStandaloneFetch) {
  if (useFillFetch(getServerSelectedVersion())) {
    // Draft 20 serves the fill on its own stream.
    co_return;
  }
  co_await setupMoQSession();
  expectSubscribe([](auto sub, auto) -> TaskSubscribeResult {
    co_return makeSubscribeOkResult(sub, AbsoluteLocation{3, 0});
  });
  std::optional<StandaloneFetch> range;
  expectFetch([&range](Fetch fetch, auto fetchPub) -> TaskFetchResult {
    if (auto standalone = std::get_if<StandaloneFetch>(&fetch.args)) {
      range = *standalone;
    }
    fetchPub->object(
        /*groupID=*/1,
        /*subgroupID=*/0,
        /*objectID=*/1,
        moxygen::test::makeBuf(10),
        noExtensions(),
        /*finFetch=*/true);
    co_return makeFetchOkResult(fetch, AbsoluteLocation{3, 0});
  });
  folly::coro::Baton fetchDone;
  EXPECT_CALL(*fetchCallback_, object(1, 0, 1, _, _, true, _)).WillOnce([&] {
    fetchDone.post();
    return folly::unit;
  });
  EXPECT_CALL(*fetchCallback_, reset(_)).WillRepeatedly(testing::Return());
  EXPECT_CALL(*subscribeCallback_, publishDone(_))
      .WillRepeatedly(testing::Return(folly::unit));
  expectBeginFill();

  auto sub = getSubscribe(kTestTrackName);
  sub.fill = FillParameters{
      LocationFilter::range({1, 1}, AbsoluteLocation{2, 1}), {}, {}, {}};
  auto res = co_await clientSession_->subscribe(sub, subscribeCallback_);
  CO_ASSERT_TRUE(!res.hasError());
  co_await fetchDone;
  CO_ASSERT_TRUE(range.has_value());
  EXPECT_EQ(range->start, (AbsoluteLocation{1, 1}));
  EXPECT_EQ(range->end, (AbsoluteLocation{2, 2}));
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
                FetchErrorCode::DOES_NOT_EXIST,
                "Bad trackname"});
      },
      FetchErrorCode::DOES_NOT_EXIST);
  auto res =
      co_await clientSession_->fetch(getFetch({0, 0}, {0, 1}), fetchCallback_);
  EXPECT_TRUE(res.hasError());
  EXPECT_EQ(res.error().errorCode, FetchErrorCode::DOES_NOT_EXIST);
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
  EXPECT_CALL(*fetchCallback_, reset(ResetStreamErrorCode::CANCELLED));
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
  EXPECT_CALL(*fetchCallback_, reset(ResetStreamErrorCode::CANCELLED));
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
  EXPECT_CALL(*fetchCallback_, reset(ResetStreamErrorCode::SESSION_CLOSED));
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}
CO_TEST_P_X(PreDraft18Test, FetchOverLimit) {
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
  EXPECT_CALL(*fetchCallback1, reset(ResetStreamErrorCode::SESSION_CLOSED));
  EXPECT_CALL(*fetchCallback2, reset(ResetStreamErrorCode::SESSION_CLOSED));
  res = co_await clientSession_->fetch(fetch, fetchCallback3);
  EXPECT_TRUE(res.hasError());
}
// Draft-18 mirror of FetchOverLimit: each fetch consumes a client-initiated
// bidi stream for the request. When the bidi stream limit is reached, the
// next fetch fails with INTERNAL_ERROR from createBidiStream.
CO_TEST_P_X(Draft18Test, FetchOverBidiStreamLimit) {
  constexpr uint64_t kLimit = 2;
  clientWt_->setMaxLocalBidiStreams(kLimit);
  co_await setupMoQSession();

  expectFetch([](Fetch fetch, auto) -> TaskFetchResult {
    co_return makeFetchOkResult(fetch, AbsoluteLocation{100, 100});
  });
  expectFetch([](Fetch fetch, auto) -> TaskFetchResult {
    co_return makeFetchOkResult(fetch, AbsoluteLocation{100, 100});
  });

  auto fetchCb = std::make_shared<testing::StrictMock<MockFetchConsumer>>();
  Fetch fetch = getFetch({0, 0}, {0, 1});

  EXPECT_CALL(*clientSubscriberStatsCallback_, onFetchSuccess()).Times(kLimit);
  for (uint64_t i = 0; i < kLimit; ++i) {
    auto res = co_await clientSession_->fetch(fetch, fetchCb);
    EXPECT_FALSE(res.hasError()) << "fetch " << i << " should succeed";
  }

  EXPECT_CALL(
      *clientSubscriberStatsCallback_,
      onFetchError(FetchErrorCode::INTERNAL_ERROR));
  auto blocked = co_await clientSession_->fetch(fetch, fetchCb);
  EXPECT_TRUE(blocked.hasError());
  EXPECT_EQ(blocked.error().errorCode, FetchErrorCode::INTERNAL_ERROR);

  EXPECT_CALL(*fetchCb, reset(ResetStreamErrorCode::SESSION_CLOSED))
      .Times(kLimit);
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
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

  EXPECT_CALL(*clientSubscriberStatsCallback_, onSubscriptionStreamOpened())
      .Times(0);
  EXPECT_CALL(*clientSubscriberStatsCallback_, onSubscriptionStreamClosed())
      .Times(0);
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

  EXPECT_CALL(*clientSubscriberStatsCallback_, onSubscriptionStreamOpened())
      .Times(0);
  EXPECT_CALL(*clientSubscriberStatsCallback_, onSubscriptionStreamClosed())
      .Times(0);
  auto fetch = getFetch(AbsoluteLocation{0, 0}, AbsoluteLocation{0, 2});
  auto res = co_await clientSession_->fetch(fetch, fetchCallback_);
  EXPECT_FALSE(res.hasError());

  co_await folly::coro::co_reschedule_on_current_executor;
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

// A standalone FETCH needs an absolute range, so both forms relative to
// Largest Object are refused.
CO_TEST_P_X(Draft20Test, FetchRelativeToLargestIsRejected) {
  co_await setupMoQSession();
  EXPECT_CALL(*serverPublisher, fetch(_, _)).Times(0);
  EXPECT_CALL(*serverPublisherStatsCallback_, onFetchError(_)).Times(2);
  EXPECT_CALL(*clientSubscriberStatsCallback_, onFetchError(_)).Times(2);

  for (const auto& filter :
       {LocationFilter::nextObject(), LocationFilter::relativeStart(3)}) {
    auto fetch = getFetch({0, 0}, {0, 0});
    fetch.filter = filter;
    auto res = co_await clientSession_->fetch(fetch, fetchCallback_);
    CO_ASSERT_TRUE(res.hasError());
    EXPECT_EQ(res.error().errorCode, FetchErrorCode::INVALID_RANGE);
  }
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

// === Draft 18+ bidi early-close coverage ===

// Peer FINs the FETCH bidi response stream without sending FETCH_OK or
// FETCH_ERROR; the sender's coroutine resolves with a synthesized error.
CO_TEST_P_X(Draft18Test, FetchFailsOnPeerFinWithoutReply) {
  co_await setupMoQSession();

  folly::coro::Baton serverSawFetch;
  folly::coro::Baton releaseHandler;
  EXPECT_CALL(*serverPublisher, fetch(_, _))
      .WillOnce([&](Fetch fetch, auto /*pub*/) -> TaskFetchResult {
        serverSawFetch.post();
        co_await releaseHandler;
        co_return makeFetchOkResult(fetch, AbsoluteLocation{0, 0});
      });

  std::optional<FetchErrorCode> errorCode;
  folly::coro::Baton done;
  folly::coro::co_withExecutor(
      MoQExecutor_.get(),
      folly::coro::co_invoke([&]() -> folly::coro::Task<void> {
        auto result = co_await clientSession_->fetch(
            getFetch({0, 0}, {0, 1}), fetchCallback_);
        if (result.hasError()) {
          errorCode = result.error().errorCode;
        }
        done.post();
      }))
      .start();

  co_await serverSawFetch;
  // Stream id 0 is the client-initiated FETCH bidi.
  serverWt_->writeHandles.at(0)->writeStreamData(
      nullptr, /*fin=*/true, nullptr);

  co_await done;
  EXPECT_TRUE(errorCode.has_value());
  if (errorCode.has_value()) {
    EXPECT_EQ(*errorCode, FetchErrorCode::CANCELLED);
  }

  releaseHandler.post();
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

// Same as above but the peer RESETs the bidi (exceptionalExit path) rather
// than FINing it cleanly.
CO_TEST_P_X(Draft18Test, FetchFailsOnPeerResetWithoutReply) {
  co_await setupMoQSession();

  folly::coro::Baton serverSawFetch;
  folly::coro::Baton releaseHandler;
  EXPECT_CALL(*serverPublisher, fetch(_, _))
      .WillOnce([&](Fetch fetch, auto /*pub*/) -> TaskFetchResult {
        serverSawFetch.post();
        co_await releaseHandler;
        co_return makeFetchOkResult(fetch, AbsoluteLocation{0, 0});
      });

  std::optional<FetchErrorCode> errorCode;
  folly::coro::Baton done;
  folly::coro::co_withExecutor(
      MoQExecutor_.get(),
      folly::coro::co_invoke([&]() -> folly::coro::Task<void> {
        auto result = co_await clientSession_->fetch(
            getFetch({0, 0}, {0, 1}), fetchCallback_);
        if (result.hasError()) {
          errorCode = result.error().errorCode;
        }
        done.post();
      }))
      .start();

  co_await serverSawFetch;
  serverWt_->writeHandles.at(0)->resetStream(
      folly::to_underlying(ResetStreamErrorCode::CANCELLED));

  co_await done;
  EXPECT_TRUE(errorCode.has_value());
  if (errorCode.has_value()) {
    EXPECT_EQ(*errorCode, FetchErrorCode::CANCELLED);
  }

  releaseHandler.post();
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

// Peer STOP_SENDING on FETCH bidi → onFetchCancel + server cleanup.
CO_TEST_P_X(Draft18Test, FetchBidiStreamStopSending) {
  co_await setupMoQSession();
  std::shared_ptr<MockFetchHandle> pubHandle;
  expectFetch([&pubHandle](Fetch fetch, auto /*fetchPub*/) -> TaskFetchResult {
    pubHandle = makeFetchOkResult(fetch, AbsoluteLocation{100, 100});
    co_return pubHandle;
  });
  expectFetchSuccess();
  EXPECT_CALL(*clientSubscriberStatsCallback_, recordFetchLatency(_));
  auto res =
      co_await clientSession_->fetch(getFetch({0, 0}, {0, 1}), fetchCallback_);
  EXPECT_FALSE(res.hasError());

  folly::coro::Baton cancelBaton;
  EXPECT_CALL(*pubHandle, fetchCancel()).WillOnce([&] { cancelBaton.post(); });

  // Client STOP_SENDING on its FETCH bidi (id 0) → server onFetchCancel.
  clientWt_->readHandles.at(0)->stopSending(0);
  co_await cancelBaton;

  EXPECT_CALL(*fetchCallback_, reset(ResetStreamErrorCode::SESSION_CLOSED));
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

CO_TEST_P_X(Draft18Test, FetchCancelStopsDataStream) {
  co_await setupMoQSession();
  expectFetch([](Fetch fetch, auto fetchPub) -> TaskFetchResult {
    fetchPub->object(0, 0, 0, moxygen::test::makeBuf(100));
    co_return makeFetchOkResult(fetch, AbsoluteLocation{100, 100});
  });

  folly::coro::Baton objectReceived;
  EXPECT_CALL(*fetchCallback_, object(_, _, _, _, _, false, _)).WillOnce([&] {
    objectReceived.post();
    return folly::unit;
  });
  auto res =
      co_await clientSession_->fetch(getFetch({0, 0}, {0, 1}), fetchCallback_);
  EXPECT_TRUE(res.hasValue());
  co_await objectReceived;

  auto dataStream = serverWt_->writeHandles.at(serverObjectStreamId());
  auto* dataWriteHandle =
      static_cast<proxygen::WebTransport::StreamWriteHandle*>(dataStream.get());
  std::optional<uint32_t> stopSendingCode;
  folly::CancellationCallback stopSendingCallback(
      dataWriteHandle->getCancelToken(), [&] {
        if (auto* exception = dataWriteHandle->exception()) {
          stopSendingCode = exception->error;
        }
      });

  EXPECT_CALL(*fetchCallback_, reset(ResetStreamErrorCode::CANCELLED));
  res.value()->fetchCancel();
  EXPECT_EQ(stopSendingCode, 0);

  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

// Subscriber-initiated fetchCancel() (RST write + STOP_SENDING read) must
// reach the publisher's fetchCancel handle — peer-initiated path's mirror.
CO_TEST_P_X(Draft18Test, FetchBidiStreamFetchCancel) {
  co_await setupMoQSession();
  std::shared_ptr<MockFetchHandle> pubHandle;
  expectFetch([&pubHandle](Fetch fetch, auto /*fetchPub*/) -> TaskFetchResult {
    pubHandle = makeFetchOkResult(fetch, AbsoluteLocation{100, 100});
    co_return pubHandle;
  });
  expectFetchSuccess();
  EXPECT_CALL(*clientSubscriberStatsCallback_, recordFetchLatency(_));
  auto res =
      co_await clientSession_->fetch(getFetch({0, 0}, {0, 1}), fetchCallback_);
  EXPECT_FALSE(res.hasError());

  folly::coro::Baton cancelBaton;
  EXPECT_CALL(*pubHandle, fetchCancel()).WillOnce([&] { cancelBaton.post(); });
  EXPECT_CALL(*fetchCallback_, reset(ResetStreamErrorCode::CANCELLED));

  res.value()->fetchCancel();
  co_await cancelBaton;

  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

CO_TEST_P_X(Draft18Test, FetchSessionCleanupBeforeDataStreamOpens) {
  co_await setupMoQSession();

  expectFetch([](Fetch fetch, auto /*fetchPub*/) -> TaskFetchResult {
    co_return makeFetchOkResult(fetch, AbsoluteLocation{100, 100});
  });
  expectFetchSuccess();
  EXPECT_CALL(*clientSubscriberStatsCallback_, recordFetchLatency(_));
  auto res =
      co_await clientSession_->fetch(getFetch({0, 0}, {0, 1}), fetchCallback_);
  EXPECT_TRUE(res.hasValue());

  folly::coro::Baton resetBaton;
  EXPECT_CALL(*fetchCallback_, reset(ResetStreamErrorCode::SESSION_CLOSED))
      .WillOnce([&] { resetBaton.post(); });

  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
  co_await resetBaton;
}

CO_TEST_P_X(MoQSessionTest, FetchSessionCleanupWithOpenDataStream) {
  co_await setupMoQSession();

  expectFetch([this](Fetch fetch, auto fetchPub) -> TaskFetchResult {
    eventBase_.add([fetchPub = std::move(fetchPub)] {
      EXPECT_TRUE(
          fetchPub
              ->object(
                  0, 0, 0, moxygen::test::makeBuf(100), noExtensions(), false)
              .hasValue());
    });
    co_return makeFetchOkResult(fetch, AbsoluteLocation{100, 100});
  });

  folly::coro::Baton objectReceived;
  EXPECT_CALL(
      *fetchCallback_, object(0, 0, 0, HasChainDataLengthOf(100), _, false, _))
      .WillOnce([&] {
        objectReceived.post();
        return folly::unit;
      });
  expectFetchSuccess();
  EXPECT_CALL(*clientSubscriberStatsCallback_, recordFetchLatency(_));
  auto res =
      co_await clientSession_->fetch(getFetch({0, 0}, {0, 1}), fetchCallback_);
  EXPECT_TRUE(res.hasValue());
  co_await objectReceived;

  folly::coro::Baton resetBaton;
  EXPECT_CALL(*fetchCallback_, reset(ResetStreamErrorCode::SESSION_CLOSED))
      .WillOnce([&] { resetBaton.post(); });
  auto dataStream = serverWt_->writeHandles.at(serverObjectStreamId());

  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
  co_await resetBaton;

  auto* dataException = dataStream->writeException();
  EXPECT_NE(dataException, nullptr);
  if (dataException) {
    const uint32_t expectedWireCode =
        getDraftMajorVersion(getServerSelectedVersion()) >= 18 ? 0x3 : 0x2;
    EXPECT_EQ(dataException->error, expectedWireCode);
  }
}

// Once the FETCH data stream FINs, peer STOP_SENDING on the bidi is
// informational and must not re-fire fetchCancel on the torn-down handle.
CO_TEST_P_X(Draft18Test, NoFetchCancelAfterFetchComplete) {
  co_await setupMoQSession();
  std::shared_ptr<MockFetchHandle> pubHandle;
  expectFetch([&pubHandle](Fetch fetch, auto fetchPub) -> TaskFetchResult {
    auto standalone = std::get_if<StandaloneFetch>(&fetch.args);
    EXPECT_NE(standalone, nullptr);
    fetchPub->object(
        standalone->start.group,
        /*subgroupID=*/0,
        standalone->start.object,
        moxygen::test::makeBuf(100),
        noExtensions(),
        /*finFetch=*/true);
    pubHandle = makeFetchOkResult(fetch, AbsoluteLocation{100, 100});
    co_return pubHandle;
  });

  folly::coro::Baton objBaton;
  EXPECT_CALL(
      *fetchCallback_, object(0, 0, 0, HasChainDataLengthOf(100), _, true, _))
      .WillOnce([&] {
        objBaton.post();
        return folly::unit;
      });
  expectFetchSuccess();
  EXPECT_CALL(*clientSubscriberStatsCallback_, recordFetchLatency(_));
  auto res =
      co_await clientSession_->fetch(getFetch({0, 0}, {0, 1}), fetchCallback_);
  EXPECT_FALSE(res.hasError());
  co_await objBaton;

  // Handle is torn down; STOP_SENDING must not fire fetchCancel.
  EXPECT_CALL(*pubHandle, fetchCancel()).Times(0);
  clientWt_->readHandles.at(0)->stopSending(0);

  co_await folly::coro::co_reschedule_on_current_executor;
  co_await folly::coro::co_reschedule_on_current_executor;
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

// Same as the FIN / RESET pair above, but the peer STOP_SENDINGs our write
// half, which cancels the read loop before it can fail the pending FETCH.
CO_TEST_P_X(Draft18Test, FetchFailsOnPeerStopSendingWithoutReply) {
  co_await setupMoQSession();

  folly::coro::Baton serverSawFetch;
  folly::coro::Baton releaseHandler;
  EXPECT_CALL(*serverPublisher, fetch(_, _))
      .WillOnce([&](Fetch fetch, auto /*pub*/) -> TaskFetchResult {
        serverSawFetch.post();
        co_await releaseHandler;
        co_return makeFetchOkResult(fetch, AbsoluteLocation{0, 0});
      });

  std::optional<FetchErrorCode> errorCode;
  folly::coro::Baton done;
  folly::coro::co_withExecutor(
      MoQExecutor_.get(),
      folly::coro::co_invoke([&]() -> folly::coro::Task<void> {
        auto result = co_await clientSession_->fetch(
            getFetch({0, 0}, {0, 1}), fetchCallback_);
        if (result.hasError()) {
          errorCode = result.error().errorCode;
        }
        done.post();
      }))
      .start();

  co_await serverSawFetch;
  serverWt_->readHandles.at(0)->stopSending(
      folly::to_underlying(ResetStreamErrorCode::CANCELLED));

  co_await done;
  EXPECT_TRUE(errorCode.has_value());
  if (errorCode.has_value()) {
    EXPECT_EQ(*errorCode, FetchErrorCode::CANCELLED);
  }

  releaseHandler.post();
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

// After FETCH_OK the data streams own completion, so a peer FIN on the bidi is
// informational. The fetch must stay registered and keep receiving objects --
// tearing it down here would drop the rest of the response on the floor.
CO_TEST_P_X(Draft18Test, FetchSurvivesPeerFinAfterFetchOk) {
  co_await setupMoQSession();

  std::shared_ptr<FetchConsumer> fetchPub;
  expectFetch([&fetchPub](Fetch fetch, auto pub) -> TaskFetchResult {
    fetchPub = std::move(pub);
    co_return makeFetchOkResult(fetch, AbsoluteLocation{100, 100});
  });
  expectFetchSuccess();
  EXPECT_CALL(*clientSubscriberStatsCallback_, recordFetchLatency(_));
  auto res =
      co_await clientSession_->fetch(getFetch({0, 0}, {0, 1}), fetchCallback_);
  EXPECT_FALSE(res.hasError());
  EXPECT_NE(fetchPub, nullptr);
  if (!fetchPub) {
    co_return;
  }

  // Peer FINs the already-answered FETCH bidi (stream id 0).
  serverWt_->writeHandles.at(0)->writeStreamData(
      nullptr, /*fin=*/true, nullptr);
  co_await rescheduleN(4);

  bool objectDelivered = false;
  EXPECT_CALL(
      *fetchCallback_, object(0, 0, 0, HasChainDataLengthOf(100), _, true, _))
      .WillOnce([&] {
        objectDelivered = true;
        return folly::unit;
      });
  fetchPub->object(
      0, 0, 0, moxygen::test::makeBuf(100), noExtensions(), /*finFetch=*/true);
  co_await rescheduleN(4);
  EXPECT_TRUE(objectDelivered)
      << "peer FIN after FETCH_OK tore down the live fetch";

  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

// Failing a request because its bidi closed early must run the drain check:
// if it was the last open request on a draining session, the session closes.
CO_TEST_P_X(Draft18Test, DrainClosesSessionWhenEarlyCloseEndsLastFetch) {
  co_await setupMoQSession();

  folly::coro::Baton serverSawFetch;
  folly::coro::Baton releaseHandler;
  EXPECT_CALL(*serverPublisher, fetch(_, _))
      .WillOnce([&](Fetch fetch, auto /*pub*/) -> TaskFetchResult {
        serverSawFetch.post();
        co_await releaseHandler;
        co_return makeFetchOkResult(fetch, AbsoluteLocation{0, 0});
      });

  folly::coro::Baton done;
  folly::coro::co_withExecutor(
      MoQExecutor_.get(),
      folly::coro::co_invoke([&]() -> folly::coro::Task<void> {
        auto result = co_await clientSession_->fetch(
            getFetch({0, 0}, {0, 1}), fetchCallback_);
        EXPECT_TRUE(result.hasError());
        done.post();
      }))
      .start();

  co_await serverSawFetch;

  // The in-flight fetch holds the session open through the drain.
  clientSession_->drain();
  EXPECT_FALSE(clientSession_->isClosed());

  // Failing the fetch closes the drained session, which clears the fake
  // transport's handle maps; keep this handle alive across its own call.
  auto readHandle = serverWt_->readHandles.at(0);
  readHandle->stopSending(
      folly::to_underlying(ResetStreamErrorCode::CANCELLED));
  co_await done;
  co_await rescheduleN(4);

  EXPECT_TRUE(clientSession_->isClosed())
      << "draining session did not close after its last fetch was failed";

  releaseHandler.post();
}

// A publisher that cannot serve a fill opens the stream and resets it.
CO_TEST_P_X(Draft20Test, FailedFillOpensAndResetsItsStream) {
  co_await setupMoQSession();
  expectSubscribe([this](auto sub, auto pub) -> TaskSubscribeResult {
    serveFill(sub.requestID, sub.fill, pub, AbsoluteLocation{4, 0});
    co_return makeSubscribeOkResult(sub, AbsoluteLocation{4, 0});
  });
  folly::coro::Baton fetchCalled;
  EXPECT_CALL(*serverPublisher, fetch(_, _))
      .WillOnce([&](Fetch fetch, auto) -> TaskFetchResult {
        fetchCalled.post();
        co_return folly::makeUnexpected(
            FetchError{
                fetch.requestID,
                FetchErrorCode::INTERNAL_ERROR,
                "cannot serve"});
      });
  // A fill does not get a reply, so neither fetch outcome is reported.
  EXPECT_CALL(*serverPublisherStatsCallback_, onFetchError(_)).Times(0);
  EXPECT_CALL(*serverPublisherStatsCallback_, onFetchSuccess()).Times(0);

  EXPECT_CALL(*subscribeCallback_, publishDone(_))
      .WillRepeatedly(testing::Return(folly::unit));
  // This transport drops the FETCH_HEADER along with the reset, so the fill
  // consumer is told only when the session closes.
  EXPECT_CALL(*fetchCallback_, reset(_)).WillRepeatedly(testing::Return());
  expectBeginFill();
  auto res = co_await clientSession_->subscribe(
      subscribeWithFill(kTestTrackName, /*joiningStart=*/1),
      subscribeCallback_);
  EXPECT_FALSE(res.hasError());
  co_await fetchCalled;
  co_await rescheduleN(4);

  // The stream must exist and carry a reset. This transport drops the header
  // on reset; a real connection has usually sent it already.
  auto streamIt = serverWt_->writeHandles.find(serverObjectStreamId());
  CO_ASSERT_NE(streamIt, serverWt_->writeHandles.end());
  EXPECT_TRUE(streamIt->second->getWriteErr().has_value());
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

// Cancelling a fill stops only its stream; a FETCH_CANCEL for the shared
// request ID would hit the subscription.
CO_TEST_P_X(Draft20Test, FillFetchCancelStopsDataStream) {
  co_await setupMoQSession();
  expectSubscribe([this](auto sub, auto pub) -> TaskSubscribeResult {
    serveFill(sub.requestID, sub.fill, pub, AbsoluteLocation{0, 0});
    pub->publishDone(getTrackEndedPublishDone(sub.requestID));
    co_return makeSubscribeOkResult(sub, AbsoluteLocation{0, 0});
  });
  // No fin, so the fill stream is still open when the cancel arrives.
  expectFillFetch([](Fetch fetch, auto fetchPub) -> TaskFetchResult {
    fetchPub->object(0, 0, 0, moxygen::test::makeBuf(100));
    co_return makeFetchOkResult(fetch, AbsoluteLocation{100, 100});
  });
  expectPublishDone();

  folly::coro::Baton objectReceived;
  EXPECT_CALL(*fetchCallback_, object(_, _, _, _, _, false, _)).WillOnce([&] {
    objectReceived.post();
    return folly::unit;
  });
  expectBeginFill();
  auto res = co_await clientSession_->subscribe(
      subscribeWithFill(
          kTestTrackName, /*joiningStart=*/1, /*fillPriority=*/129),
      subscribeCallback_);
  EXPECT_FALSE(res.hasError());
  co_await publishDone_;
  co_await objectReceived;

  auto dataStream = serverWt_->writeHandles.at(serverObjectStreamId());
  auto* dataWriteHandle =
      static_cast<proxygen::WebTransport::StreamWriteHandle*>(dataStream.get());
  std::optional<uint32_t> stopSendingCode;
  folly::CancellationCallback stopSendingCallback(
      dataWriteHandle->getCancelToken(), [&] {
        if (auto* exception = dataWriteHandle->exception()) {
          stopSendingCode = exception->error;
        }
      });

  EXPECT_CALL(*fetchCallback_, reset(ResetStreamErrorCode::CANCELLED));
  // The fill carries the subscribe's request ID.
  res.value()->cancelFill(res.value()->subscribeOk().requestID);
  EXPECT_EQ(
      stopSendingCode,
      toWireResetStreamErrorCode(
          ResetStreamErrorCode::CANCELLED, GetParam().serverVersion));

  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

// Unsubscribing resets the subscription's open fill streams.
CO_TEST_P_X(Draft20Test, UnsubscribeResetsAnOpenFill) {
  co_await setupMoQSession();
  expectSubscribe([this](auto sub, auto pub) -> TaskSubscribeResult {
    serveFill(sub.requestID, sub.fill, pub, AbsoluteLocation{4, 0});
    co_return makeSubscribeOkResult(sub, AbsoluteLocation{4, 0});
  });
  // No fin, so the fill stream is still open when the unsubscribe arrives.
  expectFillFetch([](Fetch fetch, auto fetchPub) -> TaskFetchResult {
    fetchPub->object(0, 0, 0, moxygen::test::makeBuf(100));
    co_return makeFetchOkResult(fetch, AbsoluteLocation{100, 100});
  });
  folly::coro::Baton objectReceived;
  EXPECT_CALL(*fetchCallback_, object(_, _, _, _, _, false, _)).WillOnce([&] {
    objectReceived.post();
    return folly::unit;
  });
  EXPECT_CALL(*fetchCallback_, reset(_)).WillRepeatedly(testing::Return());
  EXPECT_CALL(*subscribeCallback_, publishDone(_))
      .WillRepeatedly(testing::Return(folly::unit));
  expectBeginFill();

  auto res = co_await clientSession_->subscribe(
      subscribeWithFill(kTestTrackName, /*joiningStart=*/1),
      subscribeCallback_);
  CO_ASSERT_TRUE(!res.hasError());
  co_await objectReceived;

  res.value()->unsubscribe();
  co_await rescheduleN(4);

  auto streamIt = serverWt_->writeHandles.find(serverObjectStreamId());
  CO_ASSERT_NE(streamIt, serverWt_->writeHandles.end());
  EXPECT_TRUE(streamIt->second->getWriteErr().has_value());
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

// A paused SUBSCRIBE does not open a fill.
CO_TEST_P_X(Draft20Test, PausedSubscribeOpensNoFill) {
  co_await setupMoQSession();
  expectSubscribe([this](auto sub, auto pub) -> TaskSubscribeResult {
    serveFill(sub.requestID, sub.fill, pub, AbsoluteLocation{4, 0});
    co_return makeSubscribeOkResult(sub, AbsoluteLocation{4, 0});
  });
  // beginFill runs when the SUBSCRIBE is sent; pausing only stops the
  // publisher from serving the fill.
  EXPECT_CALL(*serverPublisher, fetch(_, _)).Times(0);
  EXPECT_CALL(*fetchCallback_, reset(_)).WillRepeatedly(testing::Return());
  EXPECT_CALL(*subscribeCallback_, publishDone(_))
      .WillRepeatedly(testing::Return(folly::unit));
  expectBeginFill();

  auto sub = subscribeWithFill(kTestTrackName, /*joiningStart=*/1);
  sub.forward = false;
  auto res = co_await clientSession_->subscribe(sub, subscribeCallback_);
  CO_ASSERT_TRUE(!res.hasError());
  co_await rescheduleN(4);
  EXPECT_EQ(serverWt_->writeHandles.count(serverObjectStreamId()), 0);
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

// With no order on the subscription or the fill, the subscriber requests
// Ascending, so parsing the fill stream does not depend on SUBSCRIBE_OK.
CO_TEST_P_X(Draft20Test, FillWithDefaultOrderRequestsAscending) {
  co_await setupMoQSession();
  std::optional<GroupOrder> fillOrder;
  expectSubscribe([&fillOrder](auto sub, auto) -> TaskSubscribeResult {
    fillOrder = sub.fill->groupOrder;
    co_return makeSubscribeOkResult(sub, AbsoluteLocation{4, 0});
  });
  EXPECT_CALL(*fetchCallback_, reset(_)).WillRepeatedly(testing::Return());
  EXPECT_CALL(*subscribeCallback_, publishDone(_))
      .WillRepeatedly(testing::Return(folly::unit));
  expectBeginFill();

  auto sub = subscribeWithFill(kTestTrackName, /*joiningStart=*/1);
  sub.groupOrder = GroupOrder::Default;
  auto res = co_await clientSession_->subscribe(sub, subscribeCallback_);
  CO_ASSERT_TRUE(!res.hasError());
  EXPECT_EQ(fillOrder, GroupOrder::OldestFirst);
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

// A failed SUBSCRIBE ends the fill it registered.
CO_TEST_P_X(Draft20Test, SubscribeErrorEndsTheFill) {
  co_await setupMoQSession();
  expectSubscribe(
      [](auto sub, auto) -> TaskSubscribeResult {
        co_return folly::makeUnexpected(
            SubscribeError{
                sub.requestID, SubscribeErrorCode::UNAUTHORIZED, "bad"});
      },
      MoQControlCodec::Direction::SERVER,
      SubscribeErrorCode::UNAUTHORIZED);
  EXPECT_CALL(
      *clientSubscriberStatsCallback_,
      onSubscribeError(SubscribeErrorCode::UNAUTHORIZED));
  EXPECT_CALL(*fetchCallback_, reset(ResetStreamErrorCode::CANCELLED));
  expectBeginFill();

  auto res = co_await clientSession_->subscribe(
      subscribeWithFill(kTestTrackName, /*joiningStart=*/1),
      subscribeCallback_);
  CO_ASSERT_TRUE(res.hasError());
  testing::Mock::VerifyAndClearExpectations(fetchCallback_.get());
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

// beginFill opens each requested fill once, and nothing else.
CO_TEST_P_X(Draft20Test, BeginFillOpensEachRequestedFillOnce) {
  co_await setupMoQSession();
  expectSubscribe([](auto sub, auto pub) -> TaskSubscribeResult {
    EXPECT_TRUE(pub->beginFill(RequestID(sub.requestID.value + 2)).hasError());
    auto fill = pub->beginFill(sub.requestID);
    EXPECT_TRUE(fill.hasValue());
    EXPECT_TRUE(pub->beginFill(sub.requestID).hasError());
    if (fill) {
      fill.value()->endOfFetch();
    }
    co_return makeSubscribeOkResult(sub, AbsoluteLocation{4, 0});
  });
  folly::coro::Baton fillEnded;
  expectBeginFill();
  EXPECT_CALL(*subscribeCallback_, publishDone(_))
      .WillRepeatedly(testing::Return(folly::unit));
  EXPECT_CALL(*fetchCallback_, endOfFetch()).WillOnce([&] {
    fillEnded.post();
    return folly::unit;
  });

  auto res = co_await clientSession_->subscribe(
      subscribeWithFill(kTestTrackName, /*joiningStart=*/1),
      subscribeCallback_);
  CO_ASSERT_TRUE(!res.hasError());
  co_await fillEnded;
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

// A fill cannot open after PUBLISH_DONE, which would not count it.
CO_TEST_P_X(Draft20Test, BeginFillRefusedAfterPublishDone) {
  co_await setupMoQSession();
  expectSubscribe([](auto sub, auto pub) -> TaskSubscribeResult {
    pub->publishDone(getTrackEndedPublishDone(sub.requestID));
    EXPECT_TRUE(pub->beginFill(sub.requestID).hasError());
    co_return makeSubscribeOkResult(sub, AbsoluteLocation{4, 0});
  });
  EXPECT_CALL(*fetchCallback_, reset(_)).WillRepeatedly(testing::Return());
  expectBeginFill();
  expectPublishDone();

  auto res = co_await clientSession_->subscribe(
      subscribeWithFill(kTestTrackName, /*joiningStart=*/1),
      subscribeCallback_);
  CO_ASSERT_TRUE(!res.hasError());
  co_await publishDone_;
  EXPECT_EQ(serverWt_->writeHandles.count(serverObjectStreamId()), 0);
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

// A subscription paused by a REQUEST_UPDATE does not open a later fill.
CO_TEST_P_X(Draft20Test, FillOnAPausedSubscriptionOpensNoStream) {
  co_await setupMoQSession();
  std::shared_ptr<MockSubscriptionHandle> mockSubscriptionHandle;
  expectSubscribe(
      [&mockSubscriptionHandle](auto sub, auto /*pub*/) -> TaskSubscribeResult {
        mockSubscriptionHandle =
            makeSubscribeOkResult(sub, AbsoluteLocation{15, 20});
        co_return mockSubscriptionHandle;
      });
  EXPECT_CALL(*serverPublisher, fetch(_, _)).Times(0);
  EXPECT_CALL(*fetchCallback_, reset(_)).WillRepeatedly(testing::Return());
  EXPECT_CALL(*subscribeCallback_, beginFill(_))
      .WillRepeatedly([this](RequestID) {
        return folly::Expected<std::shared_ptr<FetchConsumer>, MoQPublishError>(
            fetchCallback_);
      });
  EXPECT_CALL(*subscribeCallback_, publishDone(_))
      .WillRepeatedly(testing::Return(folly::unit));
  EXPECT_CALL(*clientSubscriberStatsCallback_, onRequestUpdate())
      .Times(testing::AnyNumber());
  EXPECT_CALL(*serverPublisherStatsCallback_, onRequestUpdate())
      .Times(testing::AnyNumber());

  auto res = co_await clientSession_->subscribe(
      getSubscribe(kTestTrackName), subscribeCallback_);
  CO_ASSERT_TRUE(!res.hasError());
  auto subscribeHandle = res.value();
  CO_ASSERT_NE(mockSubscriptionHandle, nullptr);
  EXPECT_CALL(*mockSubscriptionHandle, requestUpdateCalled)
      .WillRepeatedly([](const auto&) {});
  EXPECT_CALL(*mockSubscriptionHandle, requestUpdateResult)
      .WillRepeatedly(testing::Return(RequestOk{.requestID = RequestID(0)}));

  RequestUpdate pause;
  pause.existingRequestID = subscribeHandle->subscribeOk().requestID;
  pause.forward = false;
  EXPECT_FALSE((co_await subscribeHandle->requestUpdate(pause)).hasError());

  RequestUpdate askFill;
  askFill.existingRequestID = subscribeHandle->subscribeOk().requestID;
  askFill.fill =
      FillParameters{LocationFilter::absoluteStart({10, 0}), {}, {}, {}};
  EXPECT_FALSE((co_await subscribeHandle->requestUpdate(askFill)).hasError());
  co_await rescheduleN(4);
  EXPECT_EQ(serverWt_->writeHandles.count(serverObjectStreamId()), 0);
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

// A reset fill ends only its own stream; the subscription keeps delivering.
CO_TEST_P_X(Draft20Test, FillResetLeavesTheSubscriptionAlive) {
  co_await setupMoQSession();
  std::shared_ptr<TrackConsumer> trackConsumer;
  expectSubscribe(
      [this, &trackConsumer](auto sub, auto pub) -> TaskSubscribeResult {
        trackConsumer = pub;
        serveFill(sub.requestID, sub.fill, pub, AbsoluteLocation{4, 0});
        co_return makeSubscribeOkResult(sub, AbsoluteLocation{4, 0});
      });
  folly::coro::Baton fetchCalled;
  EXPECT_CALL(*serverPublisher, fetch(_, _))
      .WillOnce([&](Fetch fetch, auto) -> TaskFetchResult {
        fetchCalled.post();
        co_return folly::makeUnexpected(
            FetchError{
                fetch.requestID,
                FetchErrorCode::INTERNAL_ERROR,
                "cannot serve"});
      });
  EXPECT_CALL(*fetchCallback_, reset(_)).WillRepeatedly(testing::Return());
  expectBeginFill();
  expectPublishDone();

  auto res = co_await clientSession_->subscribe(
      subscribeWithFill(kTestTrackName, /*joiningStart=*/1),
      subscribeCallback_);
  CO_ASSERT_TRUE(!res.hasError());
  co_await fetchCalled;
  co_await rescheduleN(4);

  // The fill stream was opened and reset.
  auto streamIt = serverWt_->writeHandles.find(serverObjectStreamId());
  CO_ASSERT_NE(streamIt, serverWt_->writeHandles.end());
  EXPECT_TRUE(streamIt->second->getWriteErr().has_value());

  // The subscription still delivers after its fill died.
  folly::coro::Baton datagramReceived;
  EXPECT_CALL(*subscribeCallback_, datagram(_, _, _)).WillOnce([&] {
    datagramReceived.post();
    return folly::unit;
  });
  trackConsumer->datagram(
      ObjectHeader(5, 0, 0, 0, 11), folly::IOBuf::copyBuffer("hello world"));
  co_await datagramReceived;

  trackConsumer->publishDone(
      getTrackEndedPublishDone(res.value()->subscribeOk().requestID));
  co_await publishDone_;
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

// Draft 20 counts a fill stream in PUBLISH_DONE's Stream Count.
CO_TEST_P_X(Draft20Test, PublishDoneStreamCountIncludesTheFill) {
  co_await setupMoQSession();
  std::shared_ptr<TrackConsumer> trackConsumer;
  expectSubscribe(
      [this, &trackConsumer](auto sub, auto pub) -> TaskSubscribeResult {
        trackConsumer = pub;
        serveFill(sub.requestID, sub.fill, pub, AbsoluteLocation{4, 0});
        co_return makeSubscribeOkResult(sub, AbsoluteLocation{4, 0});
      });
  expectFillFetch([](Fetch fetch, auto fetchPub) -> TaskFetchResult {
    fetchPub->object(
        /*groupID=*/3,
        /*subgroupID=*/0,
        /*objectID=*/0,
        moxygen::test::makeBuf(100),
        noExtensions(),
        /*finFetch=*/true);
    co_return makeFetchOkResult(fetch, AbsoluteLocation{4, 0});
  });
  folly::coro::Baton fillObject;
  EXPECT_CALL(*fetchCallback_, object(3, 0, 0, _, _, true, _)).WillOnce([&] {
    fillObject.post();
    return folly::unit;
  });
  expectBeginFill();

  uint64_t observedStreamCount = 0;
  folly::coro::Baton donePosted;
  EXPECT_CALL(*subscribeCallback_, publishDone(_))
      .WillOnce([&](const PublishDone& done) {
        observedStreamCount = done.streamCount;
        donePosted.post();
        return folly::unit;
      });
  EXPECT_CALL(*serverPublisherStatsCallback_, onPublishDone(_));
  EXPECT_CALL(*serverPublisherStatsCallback_, onSubscriptionEnd());
  EXPECT_CALL(*clientSubscriberStatsCallback_, onPublishDone(_));
  EXPECT_CALL(*clientSubscriberStatsCallback_, onSubscriptionEnd());

  auto res = co_await clientSession_->subscribe(
      subscribeWithFill(kTestTrackName, /*joiningStart=*/1),
      subscribeCallback_);
  CO_ASSERT_TRUE(!res.hasError());
  co_await fillObject;

  trackConsumer->publishDone(
      getTrackEndedPublishDone(res.value()->subscribeOk().requestID));
  co_await donePosted;

  // The fill stream is the only stream this subscription opened.
  EXPECT_EQ(observedStreamCount, 1);
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

// A fill inherits the subscription's priority, group order and authorization,
// except where FILL_PARAMETERS override them.
TEST(MakeFillFetch, InheritsTheSubscription) {
  TrackRequestParameters params{FrameType::SUBSCRIBE};
  params.insertParam(Parameter(
      folly::to_underlying(TrackRequestParamKey::AUTHORIZATION_TOKEN),
      AuthToken{0, "subscription-secret", true}));
  const auto filter = LocationFilter::absoluteStart({2, 0});
  const AbsoluteLocation largest{4, 0};

  auto fetch = makeFillFetch(
      kTestTrackName,
      RequestID(7),
      filter,
      FillParameters{},
      largest,
      /*priority=*/64,
      GroupOrder::NewestFirst,
      params);
  ASSERT_TRUE(fetch.has_value());
  EXPECT_EQ(fetch->requestID, RequestID(7));
  EXPECT_EQ(fetch->priority, 64);
  EXPECT_EQ(fetch->groupOrder, GroupOrder::NewestFirst);
  EXPECT_EQ(
      std::get<StandaloneFetch>(fetch->args).start, (AbsoluteLocation{2, 0}));
  ASSERT_EQ(fetch->params.size(), 1);
  EXPECT_EQ(fetch->params.at(0).asAuthToken.tokenValue, "subscription-secret");

  auto overridden = makeFillFetch(
      kTestTrackName,
      RequestID(7),
      filter,
      FillParameters{
          LocationFilter::absoluteStart({3, 0}),
          8,
          GroupOrder::OldestFirst,
          {}},
      largest,
      /*priority=*/64,
      GroupOrder::NewestFirst,
      params);
  ASSERT_TRUE(overridden.has_value());
  EXPECT_EQ(overridden->priority, 8);
  EXPECT_EQ(overridden->groupOrder, GroupOrder::OldestFirst);
  EXPECT_EQ(
      std::get<StandaloneFetch>(overridden->args).start,
      (AbsoluteLocation{3, 0}));
}

// With no Largest, or a range past it, there is nothing to fill.
TEST(MakeFillFetch, NothingToFill) {
  const TrackRequestParameters params{FrameType::SUBSCRIBE};
  EXPECT_FALSE(makeFillFetch(
                   kTestTrackName,
                   RequestID(7),
                   LocationFilter::absoluteStart({0, 0}),
                   FillParameters{},
                   std::nullopt,
                   kDefaultPriority,
                   GroupOrder::OldestFirst,
                   params)
                   .has_value());
  EXPECT_FALSE(makeFillFetch(
                   kTestTrackName,
                   RequestID(7),
                   LocationFilter::absoluteStart({9, 0}),
                   FillParameters{},
                   AbsoluteLocation{4, 0},
                   kDefaultPriority,
                   GroupOrder::OldestFirst,
                   params)
                   .has_value());
}

// Regression: FETCH publishers live in pubTracks_ but never fire
// onSubscriptionBegin, so session close must not decrement the active gauge.
CO_TEST_P_X(MoQSessionTest, NoSubscriptionEndForInFlightFetchAtClose) {
  co_await setupMoQSession();

  folly::coro::Baton serverSawFetch;
  folly::coro::Baton releaseHandler;
  EXPECT_CALL(*serverPublisher, fetch(_, _))
      .WillOnce([&](Fetch fetch, auto /*pub*/) -> TaskFetchResult {
        serverSawFetch.post();
        co_await releaseHandler;
        co_return makeFetchOkResult(fetch, AbsoluteLocation{0, 0});
      });

  EXPECT_CALL(*serverPublisherStatsCallback_, onSubscriptionBegin()).Times(0);
  EXPECT_CALL(*serverPublisherStatsCallback_, onSubscriptionEnd()).Times(0);

  folly::coro::Baton fetchReturned;
  folly::coro::co_withExecutor(
      MoQExecutor_.get(),
      folly::coro::co_invoke([&]() -> folly::coro::Task<void> {
        co_await clientSession_->fetch(
            getFetch({0, 0}, {0, 1}), fetchCallback_);
        fetchReturned.post();
      }))
      .start();

  // The FetchPublisherImpl is in the server's pubTracks_ and stays there:
  // the handler never returns, so neither fetchOk nor fetchError fires.
  co_await serverSawFetch;
  serverSession_->close(SessionCloseErrorCode::NO_ERROR);
  releaseHandler.post();
  // The detached coro captures test locals by reference; it must finish first.
  co_await fetchReturned;
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

// A fetch whose caller is cancelled before FETCH_OK must reset and release its
// consumer, and call fetchCancel() on the publisher's handle.
CO_TEST_P_X(MoQSessionTest, FetchCallerCancelledBeforeFetchOk) {
  co_await setupMoQSession();

  folly::coro::Baton serverSawFetch;
  folly::coro::Baton releaseHandler;
  bool serverSawFetchCancel = false;
  std::shared_ptr<MockFetchHandle> serverHandle;
  EXPECT_CALL(*serverPublisher, fetch(_, _))
      .WillOnce([&](Fetch fetch, auto /*pub*/) -> TaskFetchResult {
        serverSawFetch.post();
        co_await releaseHandler;
        serverHandle = makeFetchOkResult(fetch, AbsoluteLocation{0, 0});
        EXPECT_CALL(*serverHandle, fetchCancel()).WillRepeatedly([&] {
          serverSawFetchCancel = true;
        });
        co_return serverHandle;
      });

  auto consumer = std::make_shared<testing::StrictMock<MockFetchConsumer>>();
  EXPECT_CALL(*consumer, reset(ResetStreamErrorCode::CANCELLED));
  std::weak_ptr<FetchConsumer> weakConsumer = consumer;
  folly::CancellationSource cancelSource;
  auto fetchFut = folly::coro::co_withExecutor(
                      &eventBase_,
                      folly::coro::co_withCancellation(
                          cancelSource.getToken(),
                          clientSession_->fetch(
                              getFetch({0, 0}, {0, 1}), std::move(consumer))))
                      .start()
                      .via(&eventBase_);
  co_await serverSawFetch;

  cancelSource.requestCancellation();
  EXPECT_THROW(co_await std::move(fetchFut), folly::OperationCancelled);
  releaseHandler.post();
  co_await folly::coro::sleep(std::chrono::milliseconds(200));

  EXPECT_TRUE(weakConsumer.expired());
  EXPECT_TRUE(serverSawFetchCancel);
  if (serverHandle) {
    testing::Mock::VerifyAndClearExpectations(serverHandle.get());
  }
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

CO_TEST_P_X(MoQSessionTest, FetchTimesOutBeforeFetchOk) {
  co_await setupMoQSession();
  MoQSettings moqSettings;
  moqSettings.requestTimeout = std::chrono::milliseconds(50);
  clientSession_->setMoqSettings(moqSettings);

  folly::coro::Baton releaseHandler;
  bool serverSawFetchCancel = false;
  std::shared_ptr<MockFetchHandle> serverHandle;
  EXPECT_CALL(*serverPublisher, fetch(_, _))
      .WillOnce([&](Fetch fetch, auto /*pub*/) -> TaskFetchResult {
        co_await releaseHandler;
        serverHandle = makeFetchOkResult(fetch, AbsoluteLocation{0, 0});
        EXPECT_CALL(*serverHandle, fetchCancel()).WillRepeatedly([&] {
          serverSawFetchCancel = true;
        });
        co_return serverHandle;
      });
  EXPECT_CALL(
      *clientSubscriberStatsCallback_, onFetchError(FetchErrorCode::TIMEOUT));
  auto consumer = std::make_shared<testing::StrictMock<MockFetchConsumer>>();
  std::weak_ptr<FetchConsumer> weakConsumer = consumer;

  auto res = co_await clientSession_->fetch(
      getFetch({0, 0}, {0, 1}), std::move(consumer));
  EXPECT_TRUE(res.hasError());
  if (res.hasError()) {
    EXPECT_EQ(res.error().errorCode, FetchErrorCode::TIMEOUT);
  }
  releaseHandler.post();
  co_await folly::coro::sleep(std::chrono::milliseconds(200));

  EXPECT_TRUE(weakConsumer.expired());
  EXPECT_TRUE(serverSawFetchCancel);
  EXPECT_FALSE(clientSession_->isClosed());
  if (serverHandle) {
    testing::Mock::VerifyAndClearExpectations(serverHandle.get());
  }
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

CO_TEST_P_X(MoQSessionTest, FetchThatStallsAfterFetchOkIsReset) {
  co_await setupMoQSession();
  MoQSettings moqSettings;
  moqSettings.requestTimeout = std::chrono::milliseconds(100);
  clientSession_->setMoqSettings(moqSettings);

  bool serverSawFetchCancel = false;
  std::shared_ptr<FetchConsumer> serverPub;
  std::shared_ptr<MockFetchHandle> serverHandle;
  EXPECT_CALL(*serverPublisher, fetch(_, _))
      .WillOnce([&](Fetch fetch, auto pub) -> TaskFetchResult {
        serverPub = std::move(pub);
        serverHandle = makeFetchOkResult(fetch, AbsoluteLocation{0, 0});
        EXPECT_CALL(*serverHandle, fetchCancel()).WillRepeatedly([&] {
          serverSawFetchCancel = true;
        });
        co_return serverHandle;
      });
  expectFetchSuccess();
  EXPECT_CALL(*clientSubscriberStatsCallback_, recordFetchLatency(_));
  bool consumerReset = false;
  auto consumer = std::make_shared<testing::StrictMock<MockFetchConsumer>>();
  EXPECT_CALL(*consumer, reset(ResetStreamErrorCode::DELIVERY_TIMEOUT))
      .WillOnce([&] { consumerReset = true; });

  auto res = co_await clientSession_->fetch(
      getFetch({0, 0}, {0, 1}), std::move(consumer));
  EXPECT_TRUE(res.hasValue());
  co_await folly::coro::sleep(std::chrono::milliseconds(400));

  EXPECT_TRUE(consumerReset);
  EXPECT_TRUE(serverSawFetchCancel);
  EXPECT_FALSE(clientSession_->isClosed());
  if (serverHandle) {
    testing::Mock::VerifyAndClearExpectations(serverHandle.get());
  }
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

CO_TEST_P_X(MoQSessionTest, FetchWhoseDataKeepsArrivingIsNotReset) {
  co_await setupMoQSession();
  MoQSettings moqSettings;
  moqSettings.requestTimeout = std::chrono::milliseconds(500);
  clientSession_->setMoqSettings(moqSettings);

  constexpr uint64_t kObjects = 30;
  std::shared_ptr<FetchConsumer> serverPub;
  expectFetch([&serverPub](Fetch fetch, auto pub) -> TaskFetchResult {
    serverPub = std::move(pub);
    co_return makeFetchOkResult(fetch, AbsoluteLocation{0, kObjects - 1});
  });
  expectFetchSuccess();
  EXPECT_CALL(*clientSubscriberStatsCallback_, recordFetchLatency(_));
  EXPECT_CALL(*fetchCallback_, object(0, 0, _, _, _, _, _))
      .Times(kObjects)
      .WillRepeatedly(testing::Return(folly::unit));

  auto res = co_await clientSession_->fetch(
      getFetch({0, 0}, {0, kObjects}), fetchCallback_);
  EXPECT_TRUE(res.hasValue());
  for (uint64_t objectID = 0; objectID < kObjects; ++objectID) {
    co_await folly::coro::sleep(std::chrono::milliseconds(20));
    serverPub->object(
        0,
        0,
        objectID,
        moxygen::test::makeBuf(10),
        noExtensions(),
        /*finFetch=*/objectID == kObjects - 1);
  }
  co_await folly::coro::sleep(std::chrono::milliseconds(600));

  EXPECT_FALSE(clientSession_->isClosed());
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

// Fetch Y's caller is cancelled from inside subscribe X's SUBSCRIBE_OK
// processing while Y's FETCH_OK is already queued behind it. Y's FETCH_OK is
// then processed before Y's coroutine resumes. The cancellation must still end
// Y at the peer and reset Y's consumer.
CO_TEST_P_X(MoQSessionTest, FetchCancelledWhileFetchOkQueued) {
  co_await setupMoQSession();

  folly::coro::Baton serverSawX;
  folly::coro::Baton serverSawY;
  folly::coro::Baton releaseX;
  folly::coro::Baton releaseY;
  std::shared_ptr<MockFetchHandle> serverHandleY;
  bool serverSawFetchCancelY = false;
  EXPECT_CALL(*serverPublisher, subscribe(_, _))
      .WillOnce([&](auto sub, auto /*pub*/) -> TaskSubscribeResult {
        serverSawX.post();
        co_await releaseX;
        auto handle = makeSubscribeOkResult(sub);
        EXPECT_CALL(*handle, unsubscribe()).Times(testing::AnyNumber());
        co_return handle;
      });
  EXPECT_CALL(*serverPublisher, fetch(_, _))
      .WillOnce([&](Fetch fetch, auto /*pub*/) -> TaskFetchResult {
        serverSawY.post();
        co_await releaseY;
        serverHandleY = makeFetchOkResult(fetch, AbsoluteLocation{0, 0});
        EXPECT_CALL(*serverHandleY, fetchCancel()).WillRepeatedly([&] {
          serverSawFetchCancelY = true;
        });
        co_return serverHandleY;
      });

  std::string trace;
  folly::CancellationSource cancelY;
  bool yCallerResumed = false;
  bool cancelledWithYOkSent = false;
  auto consumerX = std::make_shared<testing::NiceMock<MockTrackConsumer>>();
  ON_CALL(*consumerX, setTrackAlias(_)).WillByDefault([&](TrackAlias) {
    // The server writes FETCH_OK right after its handler returns.
    cancelledWithYOkSent = serverHandleY != nullptr && !yCallerResumed;
    trace += folly::to<std::string>(
        "[X SUBSCRIBE_OK: Y OK sent=",
        serverHandleY != nullptr,
        ", cancel Y] ");
    cancelY.requestCancellation();
    return folly::Expected<folly::Unit, MoQPublishError>(folly::unit);
  });
  ON_CALL(*consumerX, publishDone(_))
      .WillByDefault(
          testing::Return(
              folly::Expected<folly::Unit, MoQPublishError>(folly::unit)));
  auto consumerY = std::make_shared<testing::StrictMock<MockFetchConsumer>>();
  EXPECT_CALL(*consumerY, reset(ResetStreamErrorCode::CANCELLED));

  auto subscribeX =
      folly::coro::co_withExecutor(
          &eventBase_,
          clientSession_->subscribe(getSubscribe(kTestTrackName), consumerX))
          .start()
          .via(&eventBase_);
  auto fetchY = folly::coro::co_withExecutor(
                    &eventBase_,
                    folly::coro::co_withCancellation(
                        cancelY.getToken(),
                        folly::coro::co_invoke(
                            [&]() -> folly::coro::Task<Publisher::FetchResult> {
                              auto res = co_await folly::coro::co_awaitTry(
                                  clientSession_->fetch(
                                      getFetch({0, 0}, {0, 1}), consumerY));
                              yCallerResumed = true;
                              trace += "[Y caller resumed] ";
                              co_return std::move(res).value();
                            })))
                    .start()
                    .via(&eventBase_);
  co_await serverSawX;
  co_await serverSawY;

  // Both handlers resume, in this order, before the client reads either reply.
  releaseX.post();
  releaseY.post();
  auto resultX = co_await folly::coro::co_awaitTry(std::move(subscribeX));
  EXPECT_TRUE(resultX.hasValue() && resultX->hasValue());
  auto resultY = co_await folly::coro::co_awaitTry(std::move(fetchY));
  EXPECT_TRUE(resultY.hasException<folly::OperationCancelled>()) << trace;
  EXPECT_TRUE(cancelledWithYOkSent) << trace;
  co_await folly::coro::sleep(std::chrono::milliseconds(200));

  EXPECT_TRUE(serverSawFetchCancelY) << trace;
  if (serverHandleY) {
    testing::Mock::VerifyAndClearExpectations(serverHandleY.get());
  }
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

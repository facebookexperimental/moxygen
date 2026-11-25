/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/lane_manager/CatalogReader.h"

#include "moxygen/test/MockMoQSession.h"
#include "moxygen/test/Mocks.h"

#include <folly/CancellationToken.h>
#include <folly/coro/BlockingWait.h>
#include <folly/coro/Collect.h>
#include <folly/coro/Invoke.h>
#include <folly/coro/SharedPromise.h>
#include <folly/coro/Timeout.h>
#include <folly/coro/WithCancellation.h>
#include <folly/io/async/EventBase.h>
#include <folly/portability/GMock.h>
#include <folly/portability/GTest.h>

namespace moxygen::lane_manager { namespace {

using namespace std::chrono_literals;
using testing::_;
using testing::Invoke;
using testing::NiceMock;

const TrackNamespace kBroadcast{std::vector<std::string>{"live", "1"}};
const RequestID kRequestID{7};

// FBMS-shaped: audio in altGroup 1, one video lane in altGroup 2.
const std::string kFbmsCatalog = R"({"tracks":[
{"name":"live-md-a_0","role":"audio","altGroup":1,"bitrate":64000,"initRef":"a"},
{"name":"live-md-v_0","role":"video","altGroup":2,"bitrate":2000000,"initRef":"v"}],
"initDataList":[{"id":"a","data":"YXVkaW8="},{"id":"v","data":"dmlkZW8="}]})";

// BBB-shaped, high lane first: video in altGroup 0, audio without one.
const std::string kBbbCatalog = R"({"tracks":[
{"name":"video1","role":"video","altGroup":0,"width":1920,"height":1080},
{"name":"video0","role":"video","altGroup":0,"width":320,"height":176},
{"name":"audio0","role":"audio","samplerate":48000}]})";

std::vector<std::string> names(const SwitchingSet* set) {
  std::vector<std::string> result;
  result.reserve(set->lanes.size());
  for (const auto& lane : set->lanes) {
    result.push_back(lane.trackName);
  }
  return result;
}

class CatalogReaderTest : public testing::Test {
 protected:
  template <typename T>
  T run(folly::coro::Task<T> task) {
    folly::RequestContextScopeGuard guard;
    folly::RequestContext::get()->setContextData(
        folly::RequestToken("moq_session"),
        std::make_unique<MoQSession::MoQSessionRequestData>(session_));
    auto result = folly::coro::blockingWait(std::move(task), &evb_);
    for (size_t i = 0; i < 4; ++i) {
      evb_.loopOnce(EVLOOP_NONBLOCK);
    }
    return result;
  }

  void recordAttempt(const SubscribeRequest& request) {
    EXPECT_EQ(request.fullTrackName.trackName, "catalog");
    EXPECT_EQ(request.requestID, kRequestID);
    subscribeSessions_.push_back(MoQSession::getRequestContext().sessionId);
    if (!attemptStarted_.isFulfilled()) {
      attemptStarted_.setValue();
    }
  }

  void expectSubscribe() {
    EXPECT_CALL(*publisher_, subscribe(_, _))
        .WillOnce([this](
                      const SubscribeRequest& request,
                      std::shared_ptr<TrackConsumer> consumer) {
          recordAttempt(request);
          live_ = std::move(consumer);
          SubscribeOk ok;
          ok.requestID = request.requestID;
          subscription_ = std::make_shared<MockSubscriptionHandle>(ok);
          return folly::coro::makeTask<Publisher::SubscribeResult>(
              subscription_);
        });
  }

  // Answers the joining FETCH with `catalog`, or fails it.
  void expectFetch(const std::optional<std::string>& catalog) {
    EXPECT_CALL(*publisher_, fetch(_, _))
        .WillOnce([catalog](
                      const Fetch& fetch,
                      std::shared_ptr<FetchConsumer> consumer) {
          EXPECT_EQ(
              std::get<JoiningFetch>(fetch.args).joiningRequestID, kRequestID);
          if (!catalog) {
            return folly::coro::makeTask<Publisher::FetchResult>(
                folly::makeUnexpected(
                    FetchError{
                        fetch.requestID,
                        FetchErrorCode::INVALID_RANGE,
                        "none"}));
          }
          consumer->object(0, 0, 0, folly::IOBuf::copyBuffer(*catalog));
          consumer->endOfFetch();
          return folly::coro::makeTask<Publisher::FetchResult>(
              std::make_shared<MockFetchHandle>(FetchOk{}));
        });
  }

  void publish(const std::string& catalog) {
    live_->objectStream(
        ObjectHeader(++group_, 0, 0), folly::IOBuf::copyBuffer(catalog));
  }

  folly::EventBase evb_;
  std::shared_ptr<MoQFollyExecutorImpl> executor_{
      std::make_shared<MoQFollyExecutorImpl>(&evb_)};
  std::shared_ptr<NiceMock<test::MockMoQSession>> session_{
      std::make_shared<NiceMock<test::MockMoQSession>>(executor_)};
  std::shared_ptr<MockPublisher> publisher_{std::make_shared<MockPublisher>()};
  CatalogReader reader_{publisher_, kBroadcast};
  std::shared_ptr<TrackConsumer> live_;
  std::shared_ptr<MockSubscriptionHandle> subscription_;
  std::vector<SessionId> subscribeSessions_;
  folly::coro::SharedPromise<void> attemptStarted_;
  uint64_t group_{0};
};

TEST_F(CatalogReaderTest, ImmutableSnapshotDoesNotSubscribeToTheClientCatalog) {
  CatalogReader reader{
      publisher_,
      kBroadcast,
      std::chrono::seconds(5),
      CatalogReader::Mode::ImmutableSnapshot};
  auto handle = std::make_shared<MockFetchHandle>(FetchOk{});
  EXPECT_CALL(*publisher_, subscribe(_, _)).Times(0);
  EXPECT_CALL(*publisher_, fetch(_, _))
      .WillOnce([handle](
                    const Fetch& fetch,
                    std::shared_ptr<FetchConsumer> consumer) {
        EXPECT_TRUE(std::holds_alternative<StandaloneFetch>(fetch.args));
        EXPECT_EQ(fetch.fullTrackName, (FullTrackName{kBroadcast, "catalog"}));
        consumer->object(0, 0, 0, folly::IOBuf::copyBuffer(kFbmsCatalog));
        consumer->endOfFetch();
        return folly::coro::makeTask<Publisher::FetchResult>(handle);
      });
  EXPECT_TRUE(folly::coro::blockingWait(reader.subscribe(kRequestID)));
  EXPECT_TRUE(folly::coro::blockingWait(reader.subscribe(kRequestID)));
  ASSERT_NE(reader.switchingSet(Role::Video), nullptr);
  EXPECT_EQ(
      reader.switchingSet(Role::Video)->lanes[0].init->to<std::string>(),
      "video");
  EXPECT_CALL(*handle, fetchCancel());
  reader.close();
}

TEST_F(CatalogReaderTest, FailedSnapshotDoesNotOpenALiveSubscription) {
  CatalogReader reader{
      publisher_,
      kBroadcast,
      std::chrono::seconds(5),
      CatalogReader::Mode::ImmutableSnapshot};
  EXPECT_CALL(*publisher_, subscribe(_, _)).Times(0);
  EXPECT_CALL(*publisher_, fetch(_, _)).WillOnce([](const Fetch& fetch, auto) {
    return folly::coro::makeTask<Publisher::FetchResult>(folly::makeUnexpected(
        FetchError{
            fetch.requestID, FetchErrorCode::DOES_NOT_EXIST, "no catalog"}));
  });
  const auto results =
      run(folly::coro::timeout(
          folly::coro::collectAll(
              reader.subscribe(kRequestID), reader.subscribe(RequestID(8))),
          1s));
  EXPECT_FALSE(std::get<0>(results));
  EXPECT_FALSE(std::get<1>(results));
  EXPECT_FALSE(run(reader.subscribe(RequestID(9))));
  ASSERT_TRUE(reader.error().has_value());
  EXPECT_EQ(reader.error()->errorCode, FetchErrorCode::DOES_NOT_EXIST);
  EXPECT_EQ(reader.error()->reasonPhrase, "no catalog");
}

class CatalogReaderSnapshotTest : public CatalogReaderTest {
 protected:
  void TearDown() override {
    snapshot_.close();
    if (!fetchReleased_.isFulfilled()) {
      fetchReleased_.setValue();
    }
    if (fetchStarted_.isFulfilled()) {
      (void)run(folly::coro::timeout(waitForFetchFinished(), 5s));
    }
  }

  void expectSnapshot(
      bool acceptImmediately = false,
      bool ignoreCancellation = false) {
    acceptImmediately_ = acceptImmediately;
    ignoreFetchCancellation_ = ignoreCancellation;
    EXPECT_CALL(*publisher_, subscribe(_, _)).Times(0);
    EXPECT_CALL(*publisher_, fetch(_, _))
        .WillOnce(Invoke(this, &CatalogReaderSnapshotTest::fetchSnapshot));
    EXPECT_CALL(*fetchHandle_, fetchCancel())
        .WillOnce(Invoke(this, &CatalogReaderSnapshotTest::recordFetchCancel));
  }

  folly::coro::Task<Publisher::FetchResult> fetchSnapshot(
      Fetch fetch,
      std::shared_ptr<FetchConsumer> consumer) {
    EXPECT_TRUE(std::holds_alternative<StandaloneFetch>(fetch.args));
    fetchConsumer_ = std::move(consumer);
    fetchStarted_.setValue();
    if (!acceptImmediately_) {
      auto token = co_await folly::coro::co_current_cancellation_token;
      if (ignoreFetchCancellation_) {
        token = folly::CancellationToken{};
      }
      auto released = co_await folly::coro::co_awaitTry(
          folly::coro::co_withCancellation(token, fetchReleased_.getFuture()));
      fetchWasCancelled_ = released.hasException<folly::OperationCancelled>();
      fetchFinished_.setValue();
      if (released.hasException()) {
        co_yield folly::coro::co_error(std::move(released).exception());
      }
    } else {
      fetchFinished_.setValue();
    }
    co_return fetchHandle_;
  }

  folly::coro::Task<bool> waitForFetchFinished() {
    co_await fetchFinished_.getFuture();
    co_return true;
  }

  void recordFetchCancel() {
    fetchCancelled_.setValue();
  }

  folly::coro::Task<bool> waitForFetchCancel() {
    co_await fetchCancelled_.getFuture();
    co_return true;
  }

  folly::coro::Task<bool> waitUntilCancelled(folly::CancellationToken token) {
    auto result = co_await folly::coro::co_awaitTry(
        folly::coro::co_withCancellation(
            token, snapshot_.subscribe(kRequestID)));
    initiatorFinished_.setValue();
    co_return result.hasException<folly::OperationCancelled>();
  }

  folly::coro::Task<void> cancelAndPublish(folly::CancellationSource& source) {
    co_await fetchStarted_.getFuture();
    source.requestCancellation();
    co_await initiatorFinished_.getFuture();
    EXPECT_FALSE(fetchWasCancelled_);
    publishSnapshot();
  }

  void publishSnapshot() {
    EXPECT_TRUE(
        fetchConsumer_->object(0, 0, 0, folly::IOBuf::copyBuffer(kFbmsCatalog))
            .hasValue());
    if (!fetchReleased_.isFulfilled()) {
      fetchReleased_.setValue();
    }
  }

  void checkCancelledInitiator(bool acceptImmediately) {
    expectSnapshot(acceptImmediately);
    folly::CancellationSource cancelled;
    const auto results =
        run(folly::coro::timeout(
            folly::coro::collectAll(
                waitUntilCancelled(cancelled.getToken()),
                snapshot_.subscribe(RequestID(8)),
                cancelAndPublish(cancelled)),
            5s));
    EXPECT_TRUE(std::get<0>(results));
    EXPECT_TRUE(std::get<1>(results));
    EXPECT_FALSE(fetchWasCancelled_);
    EXPECT_TRUE(run(snapshot_.subscribe(RequestID(9))));
    EXPECT_NE(snapshot_.switchingSet(Role::Video), nullptr);
    EXPECT_FALSE(snapshot_.error().has_value());
  }

  CatalogReader snapshot_{
      publisher_,
      kBroadcast,
      1s,
      CatalogReader::Mode::ImmutableSnapshot};
  std::shared_ptr<MockFetchHandle> fetchHandle_{
      std::make_shared<MockFetchHandle>(FetchOk{})};
  std::shared_ptr<FetchConsumer> fetchConsumer_;
  folly::coro::SharedPromise<void> fetchStarted_;
  folly::coro::SharedPromise<void> fetchReleased_;
  folly::coro::SharedPromise<void> fetchFinished_;
  folly::coro::SharedPromise<void> fetchCancelled_;
  folly::coro::SharedPromise<void> initiatorFinished_;
  bool acceptImmediately_{false};
  bool ignoreFetchCancellation_{false};
  bool fetchWasCancelled_{false};
};

TEST_F(CatalogReaderSnapshotTest, InitiatorCancellationBeforeFetchAcceptance) {
  checkCancelledInitiator(false);
}

TEST_F(CatalogReaderSnapshotTest, InitiatorCancellationAfterFetchAcceptance) {
  checkCancelledInitiator(true);
}

TEST_F(CatalogReaderSnapshotTest, InitiatorTimeoutDoesNotCloseSharedSnapshot) {
  expectSnapshot();

  EXPECT_THROW(run(snapshot_.subscribe(kRequestID)), folly::FutureTimeout);

  EXPECT_FALSE(fetchWasCancelled_);
  EXPECT_FALSE(fetchFinished_.isFulfilled());
  publishSnapshot();
  EXPECT_TRUE(run(snapshot_.subscribe(RequestID(8))));
  EXPECT_NE(snapshot_.switchingSet(Role::Video), nullptr);
  EXPECT_FALSE(snapshot_.error().has_value());
}

TEST_F(CatalogReaderTest, SnapshotAcquisitionTimeoutCancelsAcceptedFetch) {
  CatalogReader reader{
      publisher_, kBroadcast, 10s, CatalogReader::Mode::ImmutableSnapshot};
  auto handle = std::make_shared<MockFetchHandle>(FetchOk{});
  bool cancelled{false};
  EXPECT_CALL(*publisher_, subscribe(_, _)).Times(0);
  EXPECT_CALL(*publisher_, fetch(_, _)).WillOnce([handle](const Fetch&, auto) {
    return folly::coro::makeTask<Publisher::FetchResult>(handle);
  });
  EXPECT_CALL(*handle, fetchCancel()).WillOnce([&cancelled] {
    cancelled = true;
  });

  EXPECT_FALSE(run(folly::coro::timeout(reader.subscribe(kRequestID), 10s)));

  EXPECT_TRUE(cancelled);
  ASSERT_TRUE(reader.error().has_value());
  EXPECT_EQ(reader.error()->errorCode, FetchErrorCode::TIMEOUT);
  EXPECT_EQ(
      reader.error()->reasonPhrase, "catalog snapshot acquisition timed out");
  EXPECT_FALSE(run(reader.subscribe(RequestID(8))));
  reader.close();
}

TEST_F(
    CatalogReaderSnapshotTest,
    AcquisitionTimeoutCancelsLateSuccessfulFetch) {
  auto reader = std::make_unique<CatalogReader>(
      publisher_, kBroadcast, 10s, CatalogReader::Mode::ImmutableSnapshot);
  expectSnapshot(false, true);

  EXPECT_FALSE(run(folly::coro::timeout(reader->subscribe(kRequestID), 10s)));

  ASSERT_TRUE(reader->error().has_value());
  EXPECT_EQ(reader->error()->errorCode, FetchErrorCode::TIMEOUT);
  EXPECT_EQ(
      reader->error()->reasonPhrase, "catalog snapshot acquisition timed out");
  EXPECT_FALSE(fetchFinished_.isFulfilled());
  EXPECT_FALSE(run(reader->subscribe(RequestID(8))));
  reader.reset();
  publishSnapshot();
  EXPECT_TRUE(run(folly::coro::timeout(waitForFetchCancel(), 1s)));
}

TEST_F(CatalogReaderTest, ReadsTheCatalogFromTheJoiningFetch) {
  expectSubscribe();
  expectFetch(kFbmsCatalog);
  EXPECT_TRUE(run(reader_.subscribe(kRequestID)));

  const auto* video = reader_.switchingSet(Role::Video);
  ASSERT_NE(video, nullptr);
  EXPECT_EQ(names(video), std::vector<std::string>{"live-md-v_0"});
  EXPECT_EQ(video->id, 2);
  EXPECT_EQ(video->audioAllowanceBps, 64000);
  EXPECT_EQ(video->lanes[0].init->to<std::string>(), "video");
  EXPECT_EQ(
      names(reader_.switchingSet(Role::Audio)),
      std::vector<std::string>{"live-md-a_0"});

  EXPECT_CALL(*subscription_, unsubscribe());
  reader_.close();
}

TEST_F(CatalogReaderTest, StartsJoiningFetchWhileSubscribeIsPending) {
  folly::coro::SharedPromise<void> fetchStarted;
  bool subscribeCompleted{false};
  EXPECT_CALL(*publisher_, subscribe(_, _))
      .WillOnce(
          [this, &fetchStarted, &subscribeCompleted](
              SubscribeRequest request, std::shared_ptr<TrackConsumer> consumer)
              -> folly::coro::Task<Publisher::SubscribeResult> {
            recordAttempt(request);
            live_ = std::move(consumer);
            EXPECT_TRUE(live_->setTrackAlias(TrackAlias(1)).hasValue());
            co_await fetchStarted.getFuture();
            SubscribeOk ok;
            ok.requestID = request.requestID;
            subscription_ = std::make_shared<MockSubscriptionHandle>(ok);
            subscribeCompleted = true;
            co_return subscription_;
          });
  EXPECT_CALL(*publisher_, fetch(_, _))
      .WillOnce([this, &fetchStarted, &subscribeCompleted](
                    Fetch fetch, std::shared_ptr<FetchConsumer> consumer) {
        EXPECT_EQ(subscribeSessions_.size(), 1);
        EXPECT_FALSE(subscribeCompleted);
        EXPECT_EQ(
            std::get<JoiningFetch>(fetch.args).joiningRequestID, kRequestID);
        fetchStarted.setValue();
        consumer->object(0, 0, 0, folly::IOBuf::copyBuffer(kFbmsCatalog));
        return folly::coro::makeTask<Publisher::FetchResult>(
            std::make_shared<MockFetchHandle>(FetchOk{}));
      });

  EXPECT_TRUE(run(reader_.subscribe(kRequestID)));
}

TEST_F(CatalogReaderTest, ConcurrentSubscribersShareTheCatalogSubscription) {
  expectSubscribe();
  expectFetch(kFbmsCatalog);

  auto results =
      run(folly::coro::collectAll(
          reader_.subscribe(kRequestID), reader_.subscribe(kRequestID)));

  EXPECT_TRUE(std::get<0>(results));
  EXPECT_TRUE(std::get<1>(results));
  EXPECT_EQ(subscribeSessions_.size(), 1);
}

class CatalogReaderLiveFallbackTest : public CatalogReaderTest {
 protected:
  void expectRejectedFetch() {
    SubscribeOk ok;
    ok.requestID = kRequestID;
    subscription_ = std::make_shared<MockSubscriptionHandle>(ok);
    EXPECT_CALL(*subscription_, unsubscribe()).Times(0);
    EXPECT_CALL(*publisher_, subscribe(_, _))
        .WillOnce([this](
                      const SubscribeRequest& request,
                      std::shared_ptr<TrackConsumer> consumer) {
          recordAttempt(request);
          live_ = std::move(consumer);
          return folly::coro::makeTask<Publisher::SubscribeResult>(
              subscription_);
        });
    EXPECT_CALL(*publisher_, fetch(_, _))
        .WillOnce(Invoke(this, &CatalogReaderLiveFallbackTest::rejectFetch));
  }

  folly::coro::Task<Publisher::FetchResult> rejectFetch(
      const Fetch& fetch,
      std::shared_ptr<FetchConsumer>) {
    EXPECT_EQ(fetch.fullTrackName.trackName, "catalog");
    auto error = rejection_;
    error.requestID = fetch.requestID;
    fetchRejected_.setValue();
    return folly::coro::makeTask<Publisher::FetchResult>(
        folly::makeUnexpected(std::move(error)));
  }

  folly::coro::Task<void> publishAfterFetchRejection() {
    co_await fetchRejected_.getFuture();
    co_await folly::coro::co_reschedule_on_current_executor;
    publish(kFbmsCatalog);
  }

  void exerciseLiveFallback(
      FetchErrorCode errorCode,
      std::optional<std::chrono::milliseconds> retryInterval = std::nullopt) {
    rejection_ =
        FetchError{kRequestID, errorCode, "fetch rejected", retryInterval};
    expectRejectedFetch();

    const auto results =
        run(folly::coro::timeout(
            folly::coro::collectAll(
                reader_.subscribe(kRequestID),
                reader_.subscribe(RequestID(8)),
                publishAfterFetchRejection()),
            1s));

    EXPECT_TRUE(std::get<0>(results));
    EXPECT_TRUE(std::get<1>(results));
    EXPECT_TRUE(run(reader_.subscribe(RequestID(9))));
    EXPECT_NE(reader_.switchingSet(Role::Video), nullptr);
    EXPECT_FALSE(reader_.error().has_value());
    EXPECT_EQ(subscribeSessions_.size(), 1);
    testing::Mock::VerifyAndClearExpectations(subscription_.get());
    EXPECT_CALL(*subscription_, unsubscribe()).Times(1);
    reader_.close();
  }

  FetchError rejection_{
      kRequestID,
      FetchErrorCode::NOT_SUPPORTED,
      "fetch rejected"};
  folly::coro::SharedPromise<void> fetchRejected_;
};

TEST_F(
    CatalogReaderLiveFallbackTest,
    UnsupportedFetchKeepsLiveCatalogAvailable) {
  exerciseLiveFallback(FetchErrorCode::NOT_SUPPORTED);
}

TEST_F(
    CatalogReaderLiveFallbackTest,
    UnauthorizedFetchKeepsLiveCatalogAvailable) {
  exerciseLiveFallback(FetchErrorCode::UNAUTHORIZED);
}

TEST_F(
    CatalogReaderLiveFallbackTest,
    InvalidFetchRangeKeepsLiveCatalogAvailable) {
  exerciseLiveFallback(FetchErrorCode::INVALID_RANGE);
}

TEST_F(
    CatalogReaderLiveFallbackTest,
    ZeroRetryIntervalKeepsLiveCatalogAvailable) {
  exerciseLiveFallback(FetchErrorCode::EXCESSIVE_LOAD, 0ms);
}

TEST_F(CatalogReaderTest, OrdersLanesByResolutionWithoutBitrates) {
  expectSubscribe();
  expectFetch(kBbbCatalog);
  EXPECT_TRUE(run(reader_.subscribe(kRequestID)));

  EXPECT_EQ(
      names(reader_.switchingSet(Role::Video)),
      (std::vector<std::string>{"video0", "video1"}));
  EXPECT_EQ(
      names(reader_.switchingSet(Role::Audio)),
      std::vector<std::string>{"audio0"});
}

TEST_F(CatalogReaderTest, LeavesThePassthroughLaneOut) {
  expectSubscribe();
  expectFetch(R"({"tracks":[
{"name":"live-pst-v_0","role":"video","altGroup":2,"width":504,"height":896},
{"name":"live-hd-v_0","role":"video","altGroup":2,"width":480,"height":852},
{"name":"live-pst-a_0","role":"audio","altGroup":1,"samplerate":44100},
{"name":"live-hd-a_0","role":"audio","altGroup":1,"samplerate":44100}]})");
  EXPECT_TRUE(run(reader_.subscribe(kRequestID)));

  EXPECT_EQ(
      names(reader_.switchingSet(Role::Video)),
      std::vector<std::string>{"live-hd-v_0"});
  EXPECT_EQ(
      names(reader_.switchingSet(Role::Audio)),
      std::vector<std::string>{"live-hd-a_0"});
}

TEST_F(CatalogReaderTest, TakesBitratesFromLaterCatalogs) {
  expectSubscribe();
  expectFetch(kBbbCatalog);
  EXPECT_TRUE(run(reader_.subscribe(kRequestID)));

  publish(R"({"tracks":[
{"name":"video1","role":"video","altGroup":0,"bitrate":4000000},
{"name":"video0","role":"video","altGroup":0,"bitrate":300000}]})");
  const auto* video = reader_.switchingSet(Role::Video);
  EXPECT_EQ(names(video), (std::vector<std::string>{"video0", "video1"}));
  EXPECT_EQ(video->lanes[0].bitrateBps, 300000);
}

TEST_F(CatalogReaderTest, WaitsForTheNextCatalogWhenTheFetchFails) {
  expectSubscribe();
  expectFetch(std::nullopt);
  auto results =
      run(folly::coro::collectAll(
          reader_.subscribe(kRequestID),
          folly::coro::co_invoke([&]() -> folly::coro::Task<void> {
            co_await attemptStarted_.getFuture();
            publish(kFbmsCatalog);
            co_return;
          })));
  EXPECT_TRUE(std::get<0>(results));
  EXPECT_NE(reader_.switchingSet(Role::Video), nullptr);
}

TEST_F(CatalogReaderTest, RetriesATransientSubscribeFailure) {
  EXPECT_CALL(*publisher_, subscribe(_, _))
      .WillOnce([this](
                    const SubscribeRequest& request,
                    std::shared_ptr<TrackConsumer> consumer) {
        recordAttempt(request);
        live_ = std::move(consumer);
        SubscribeError error{
            request.requestID, RequestErrorCode::EXCESSIVE_LOAD, "busy"};
        error.retryInterval = 1ms;
        return folly::coro::makeTask<Publisher::SubscribeResult>(
            folly::makeUnexpected(std::move(error)));
      })
      .WillOnce([this](
                    const SubscribeRequest& request,
                    std::shared_ptr<TrackConsumer> consumer) {
        recordAttempt(request);
        live_ = std::move(consumer);
        SubscribeOk ok;
        ok.requestID = request.requestID;
        subscription_ = std::make_shared<MockSubscriptionHandle>(ok);
        live_->objectStream(
            ObjectHeader(++group_, 0, 0),
            folly::IOBuf::copyBuffer(kFbmsCatalog));
        return folly::coro::makeTask<Publisher::SubscribeResult>(subscription_);
      });
  expectFetch(std::nullopt);

  EXPECT_TRUE(run(reader_.subscribe(kRequestID)));
  EXPECT_EQ(
      subscribeSessions_,
      (std::vector<SessionId>{session_->sessionId(), session_->sessionId()}));
}

TEST_F(CatalogReaderTest, TrackEndIsTerminal) {
  expectSubscribe();
  expectFetch(kFbmsCatalog);
  EXPECT_TRUE(run(reader_.subscribe(kRequestID)));

  live_->publishDone(
      PublishDone{
          kRequestID, PublishDoneStatusCode::TRACK_ENDED, 0, "complete"});
  evb_.loop();

  EXPECT_FALSE(run(reader_.subscribe(kRequestID)));
  EXPECT_EQ(subscribeSessions_.size(), 1);
}

TEST_F(CatalogReaderTest, GivesUpWhenNoCatalogArrives) {
  CatalogReader reader{publisher_, kBroadcast, std::chrono::milliseconds(10)};
  expectSubscribe();
  expectFetch(std::nullopt);

  EXPECT_THROW(run(reader.subscribe(kRequestID)), folly::FutureTimeout);
}

TEST_F(CatalogReaderTest, RetriesATransientJoiningFetchFailure) {
  expectSubscribe();
  auto handle = std::make_shared<MockFetchHandle>(FetchOk{});
  EXPECT_CALL(*publisher_, fetch(_, _))
      .WillOnce([](const Fetch& fetch, auto) {
        FetchError error{
            fetch.requestID, FetchErrorCode::EXCESSIVE_LOAD, "busy"};
        error.retryInterval = 1ms;
        return folly::coro::makeTask<Publisher::FetchResult>(
            folly::makeUnexpected(std::move(error)));
      })
      .WillOnce([handle](
                    const Fetch& fetch,
                    std::shared_ptr<FetchConsumer> consumer) {
        EXPECT_EQ(
            std::get<JoiningFetch>(fetch.args).joiningRequestID, kRequestID);
        consumer->object(0, 0, 0, folly::IOBuf::copyBuffer(kFbmsCatalog));
        consumer->endOfFetch();
        return folly::coro::makeTask<Publisher::FetchResult>(handle);
      });

  EXPECT_TRUE(run(folly::coro::timeout(reader_.subscribe(kRequestID), 1s)));
  ASSERT_NE(reader_.switchingSet(Role::Video), nullptr);
  EXPECT_EQ(
      names(reader_.switchingSet(Role::Video)),
      std::vector<std::string>{"live-md-v_0"});
  EXPECT_FALSE(reader_.error().has_value());
  EXPECT_EQ(subscribeSessions_.size(), 1);
  EXPECT_CALL(*handle, fetchCancel());
  EXPECT_CALL(*subscription_, unsubscribe());
  reader_.close();
}

TEST_F(CatalogReaderTest, RetriesAJoiningFetchTimeout) {
  expectSubscribe();
  auto handle = std::make_shared<MockFetchHandle>(FetchOk{});
  EXPECT_CALL(*publisher_, fetch(_, _))
      .WillOnce([](const Fetch&, auto) {
        return folly::coro::makeErrorTask<Publisher::FetchResult>(
            folly::FutureTimeout());
      })
      .WillOnce([handle](
                    const Fetch& fetch,
                    std::shared_ptr<FetchConsumer> consumer) {
        EXPECT_EQ(
            std::get<JoiningFetch>(fetch.args).joiningRequestID, kRequestID);
        consumer->object(0, 0, 0, folly::IOBuf::copyBuffer(kFbmsCatalog));
        consumer->endOfFetch();
        return folly::coro::makeTask<Publisher::FetchResult>(handle);
      });

  EXPECT_TRUE(run(folly::coro::timeout(reader_.subscribe(kRequestID), 1s)));
  EXPECT_NE(reader_.switchingSet(Role::Video), nullptr);
  EXPECT_FALSE(reader_.error().has_value());
  EXPECT_EQ(subscribeSessions_.size(), 1);
  EXPECT_CALL(*handle, fetchCancel());
  EXPECT_CALL(*subscription_, unsubscribe());
  reader_.close();
}

TEST_F(CatalogReaderTest, ASubscribeThatThrowsReleasesLaterCalls) {
  EXPECT_CALL(*publisher_, subscribe(_, _))
      .WillOnce([](const auto&, const auto&) {
        return folly::coro::makeErrorTask<Publisher::SubscribeResult>(
            folly::OperationCancelled());
      });
  EXPECT_FALSE(run(reader_.subscribe(kRequestID)));
  EXPECT_FALSE(run(reader_.subscribe(kRequestID)));
}

}} // namespace moxygen::lane_manager

/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/lane_manager/CatalogReader.h"

#include "moxygen/test/Mocks.h"

#include <folly/coro/BlockingWait.h>
#include <folly/coro/Collect.h>
#include <folly/coro/Invoke.h>
#include <folly/portability/GMock.h>
#include <folly/portability/GTest.h>

namespace moxygen::lane_manager { namespace {

using testing::_;

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
  void expectSubscribe() {
    EXPECT_CALL(*publisher_, subscribe(_, _))
        .WillOnce([this](
                      const SubscribeRequest& request,
                      std::shared_ptr<TrackConsumer> consumer) {
          EXPECT_EQ(request.fullTrackName.trackName, "catalog");
          EXPECT_EQ(request.requestID, kRequestID);
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
                      Fetch fetch, std::shared_ptr<FetchConsumer> consumer) {
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

  std::shared_ptr<MockPublisher> publisher_{std::make_shared<MockPublisher>()};
  CatalogReader reader_{publisher_, kBroadcast};
  std::shared_ptr<TrackConsumer> live_;
  std::shared_ptr<MockSubscriptionHandle> subscription_;
  uint64_t group_{0};
};

TEST_F(CatalogReaderTest, ReadsTheCatalogFromTheJoiningFetch) {
  expectSubscribe();
  expectFetch(kFbmsCatalog);
  EXPECT_TRUE(folly::coro::blockingWait(reader_.open(kRequestID)));

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

TEST_F(CatalogReaderTest, OrdersLanesByResolutionWithoutBitrates) {
  expectSubscribe();
  expectFetch(kBbbCatalog);
  EXPECT_TRUE(folly::coro::blockingWait(reader_.open(kRequestID)));

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
  EXPECT_TRUE(folly::coro::blockingWait(reader_.open(kRequestID)));

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
  EXPECT_TRUE(folly::coro::blockingWait(reader_.open(kRequestID)));

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
  auto results = folly::coro::blockingWait(
      folly::coro::collectAll(
          reader_.open(kRequestID),
          folly::coro::co_invoke([&]() -> folly::coro::Task<void> {
            publish(kFbmsCatalog);
            co_return;
          })));
  EXPECT_TRUE(std::get<0>(results));
  EXPECT_NE(reader_.switchingSet(Role::Video), nullptr);
}

TEST_F(CatalogReaderTest, FailsWhenTheSubscribeFails) {
  EXPECT_CALL(*publisher_, subscribe(_, _))
      .WillOnce([](const SubscribeRequest& request, auto) {
        return folly::coro::makeTask<Publisher::SubscribeResult>(
            folly::makeUnexpected(
                SubscribeError{
                    request.requestID,
                    SubscribeErrorCode::DOES_NOT_EXIST,
                    ""}));
      });
  EXPECT_FALSE(folly::coro::blockingWait(reader_.open(kRequestID)));
}

TEST_F(CatalogReaderTest, GivesUpWhenNoCatalogArrives) {
  CatalogReader reader{publisher_, kBroadcast, std::chrono::milliseconds(10)};
  expectSubscribe();
  expectFetch(std::nullopt);

  EXPECT_THROW(
      folly::coro::blockingWait(reader.open(kRequestID)), folly::FutureTimeout);
}

TEST_F(CatalogReaderTest, ASubscribeThatThrowsReleasesLaterCalls) {
  EXPECT_CALL(*publisher_, subscribe(_, _))
      .WillOnce([](const auto&, const auto&) {
        return folly::coro::makeErrorTask<Publisher::SubscribeResult>(
            folly::OperationCancelled());
      });
  EXPECT_THROW(
      folly::coro::blockingWait(reader_.open(kRequestID)),
      folly::OperationCancelled);
  EXPECT_FALSE(folly::coro::blockingWait(reader_.open(kRequestID)));
}

}} // namespace moxygen::lane_manager

/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <quic/priority/HTTPPriorityQueue.h>
#include "moxygen/test/MoQSessionTestCommon.h"

using namespace moxygen;
using namespace moxygen::test;
using testing::_;

namespace {

using Subgroups = std::vector<std::pair<uint64_t, uint64_t>>;

constexpr Priority kPubPriority = 128;
constexpr uint64_t kMaxGroupBits = 0x3FFF;
const std::vector<uint64_t> kWrapGroups{12288, 16383, 16384, 24576};
const std::vector<uint64_t> kSaturatedGroups{16383, 16384, 16385};

uint64_t order(uint64_t groupBits, uint64_t subgroupBits = 0) {
  return (uint64_t{kPubPriority} << 19) | (groupBits << 5) | subgroupBits;
}

std::vector<uint64_t> orders(std::initializer_list<uint64_t> groupBits) {
  std::vector<uint64_t> result;
  for (auto bits : groupBits) {
    result.push_back(order(bits));
  }
  return result;
}

Subgroups firstSubgroups(const std::vector<uint64_t>& groups) {
  Subgroups subgroups;
  for (auto group : groups) {
    subgroups.emplace_back(group, 0);
  }
  return subgroups;
}

class GroupRankTest : public MoQSessionTest {
 protected:
  void rankGroups() {
    auto settings = serverSession_->getMoqSettings();
    settings.rankOldestFirstGroups = true;
    serverSession_->setMoqSettings(settings);
  }

  uint64_t streamOrder(uint64_t n) const {
    return std::get<1>(
        *serverWt_->writeHandles.at(serverObjectStreamId(n))->pri);
  }

  std::shared_ptr<MockSubscriptionHandle> subscribeOk(
      const SubscribeRequest& sub) {
    SubscribeOk ok;
    ok.requestID = sub.requestID;
    ok.trackAlias = TrackAlias(sub.requestID.value);
    ok.expires = std::chrono::milliseconds(0);
    ok.groupOrder = sub.groupOrder.value_or(GroupOrder::OldestFirst);
    return std::make_shared<MockSubscriptionHandle>(std::move(ok));
  }

  void acceptSubgroups() {
    auto subgroup = std::make_shared<testing::NiceMock<MockSubgroupConsumer>>();
    ON_CALL(*subgroup, object(_, _, _, _))
        .WillByDefault(testing::Return(folly::unit));
    EXPECT_CALL(*subscribeCallback_, beginSubgroup(_, _, _, _))
        .WillRepeatedly(testing::Return(subgroup));
  }

  void publish(TrackConsumer& pub, const Subgroups& subgroups) {
    for (auto [group, subgroup] : subgroups) {
      auto sgp = pub.beginSubgroup(group, subgroup, kPubPriority);
      EXPECT_TRUE(sgp.hasValue());
      if (sgp.hasValue()) {
        EXPECT_TRUE(sgp.value()
                        ->object(0, makeBuf(10), noExtensions(), true)
                        .hasValue());
      }
    }
  }

  folly::coro::Task<std::vector<uint64_t>> streamOrders(
      Subgroups subgroups,
      GroupOrder groupOrder = GroupOrder::OldestFirst) {
    folly::coro::Baton published;
    std::shared_ptr<TrackConsumer> serverPub;
    RequestID requestID;
    std::vector<uint64_t> result;
    expectSubscribe([&](auto sub, auto pub) -> TaskSubscribeResult {
      serverPub = pub;
      requestID = sub.requestID;
      eventBase_.add([&, pub] {
        publish(*pub, subgroups);
        for (size_t n = 0; n < subgroups.size(); ++n) {
          result.push_back(streamOrder(n));
        }
        published.post();
      });
      co_return subscribeOk(sub);
    });
    acceptSubgroups();
    auto sub = getSubscribe(kTestTrackName);
    sub.groupOrder = groupOrder;
    auto res = co_await clientSession_->subscribe(sub, subscribeCallback_);
    EXPECT_FALSE(res.hasError());
    co_await published;
    expectPublishDone();
    serverPub->publishDone(getTrackEndedPublishDone(requestID));
    co_await publishDone_;
    co_return result;
  }
};

} // namespace

INSTANTIATE_TEST_SUITE_P(
    GroupRankTest,
    GroupRankTest,
    testing::Values(VersionParams{{kVersionDraft18}, kVersionDraft18}));

CO_TEST_P_X(GroupRankTest, RankedGroupsStayOldestFirstAcrossTheWrap) {
  co_await setupMoQSession();
  rankGroups();
  auto result = co_await streamOrders(firstSubgroups(kWrapGroups));
  EXPECT_EQ(result, orders({0, 1, 2, 3}));
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

CO_TEST_P_X(GroupRankTest, TruncatedGroupBitsWithoutTheSetting) {
  co_await setupMoQSession();
  auto result = co_await streamOrders(firstSubgroups(kWrapGroups));
  EXPECT_EQ(result, orders({12288, 16383, 0, 8192}));
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

CO_TEST_P_X(GroupRankTest, NewestFirstIgnoresTheSetting) {
  co_await setupMoQSession();
  rankGroups();
  auto result = co_await streamOrders(
      firstSubgroups(kWrapGroups), GroupOrder::NewestFirst);
  EXPECT_EQ(
      result,
      orders({kMaxGroupBits - 12288, 0, kMaxGroupBits, kMaxGroupBits - 8192}));
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

CO_TEST_P_X(GroupRankTest, LateGroupTakesTheNearestOlderRank) {
  co_await setupMoQSession();
  rankGroups();
  Subgroups subgroups{{10, 0}, {12, 0}, {11, 0}, {10, 1}};
  auto result = co_await streamOrders(subgroups);
  EXPECT_EQ(
      result,
      (std::vector<uint64_t>{order(0), order(1), order(0), order(0, 1)}));
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

CO_TEST_P_X(GroupRankTest, GroupOlderThanTheWindowTakesItsOldestRank) {
  co_await setupMoQSession();
  rankGroups();
  std::vector<uint64_t> groups;
  for (uint64_t group = 1; group <= 65; ++group) {
    groups.push_back(group);
  }
  groups.push_back(0);
  auto result = co_await streamOrders(firstSubgroups(groups));
  EXPECT_EQ(
      (std::vector<uint64_t>{result[1], result[65], result[2]}),
      orders({1, 1, 2}));
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

CO_TEST_P_X(GroupRankTest, RanksAreSharedAcrossTracks) {
  co_await setupMoQSession();
  rankGroups();
  std::vector<std::shared_ptr<TrackConsumer>> pubs;
  std::vector<RequestID> requestIDs;
  auto onSubscribe = [&](auto sub, auto pub) -> TaskSubscribeResult {
    pubs.push_back(pub);
    requestIDs.push_back(sub.requestID);
    co_return subscribeOk(sub);
  };
  acceptSubgroups();
  auto otherTrack = kTestTrackName;
  otherTrack.trackName = "other";
  for (const auto& track :
       std::vector<FullTrackName>{kTestTrackName, otherTrack}) {
    expectSubscribe(onSubscribe);
    auto res = co_await clientSession_->subscribe(
        getSubscribe(track), subscribeCallback_);
    EXPECT_FALSE(res.hasError());
  }

  publish(*pubs[0], firstSubgroups({1000}));
  publish(*pubs[1], firstSubgroups({5}));
  publish(*pubs[0], firstSubgroups({1001}));
  publish(*pubs[1], firstSubgroups({6}));
  EXPECT_EQ(
      (std::vector<uint64_t>{
          streamOrder(0), streamOrder(1), streamOrder(2), streamOrder(3)}),
      orders({0, 1, 2, 3}));

  for (size_t i = 0; i < pubs.size(); ++i) {
    expectPublishDone();
    pubs[i]->publishDone(getTrackEndedPublishDone(requestIDs[i]));
    co_await publishDone_;
    publishDone_.reset();
  }
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

CO_TEST_P_X(GroupRankTest, SaturatedRanksFallBackToCreationOrder) {
  co_await setupMoQSession();
  rankGroups();
  serverSession_->setNextGroupRankForTesting(kMaxGroupBits - 1);
  Subgroups subgroups{{16383, 0}, {16384, 3}, {16385, 0}};
  auto result = co_await streamOrders(subgroups);
  EXPECT_EQ(result, orders({kMaxGroupBits - 1, kMaxGroupBits, kMaxGroupBits}));

  quic::HTTPPriorityQueue queue;
  for (uint64_t n = 3; n-- > 0;) {
    queue.insertOrUpdate(
        quic::PriorityQueue::Identifier::fromStreamID(serverObjectStreamId(n)),
        quic::HTTPPriorityQueue::Priority(
            0, false, static_cast<uint32_t>(result[n])));
  }
  std::vector<uint64_t> scheduled;
  while (!queue.empty()) {
    auto id = queue.peekNextScheduledID();
    scheduled.push_back(id.asStreamID());
    queue.erase(id);
  }
  EXPECT_EQ(
      scheduled,
      (std::vector<uint64_t>{
          serverObjectStreamId(0),
          serverObjectStreamId(1),
          serverObjectStreamId(2)}));
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

CO_TEST_P_X(GroupRankTest, SaturatedSubgroupsGetCreditInCreationOrder) {
  co_await setupMoQSession();
  rankGroups();
  serverSession_->setNextGroupRankForTesting(kMaxGroupBits);
  const auto openUni = serverWt_->openLocalUniStreams().size();
  serverWt_->setMaxLocalUniStreams(openUni);
  folly::coro::Baton published;
  std::shared_ptr<TrackConsumer> serverPub;
  RequestID requestID;
  expectSubscribe([&](auto sub, auto pub) -> TaskSubscribeResult {
    serverPub = pub;
    requestID = sub.requestID;
    eventBase_.add([&, pub] {
      publish(*pub, firstSubgroups(kSaturatedGroups));
      published.post();
    });
    co_return makeSubscribeOkResult(sub);
  });
  folly::coro::Baton received;
  {
    testing::InSequence inOrder;
    for (auto group : kSaturatedGroups) {
      auto sg = std::make_shared<testing::StrictMock<MockSubgroupConsumer>>();
      EXPECT_CALL(*subscribeCallback_, beginSubgroup(group, 0, _, _))
          .WillOnce(testing::Return(sg));
      EXPECT_CALL(*sg, object(0, _, _, true)).WillOnce([&, group] {
        if (group == kSaturatedGroups.back()) {
          received.post();
        }
        return folly::unit;
      });
    }
  }

  auto res = co_await clientSession_->subscribe(
      getSubscribe(kTestTrackName), subscribeCallback_);
  EXPECT_FALSE(res.hasError());
  co_await published;
  serverWt_->setMaxLocalUniStreams(openUni + 1);
  co_await received;
  expectPublishDone();
  serverPub->publishDone(getTrackEndedPublishDone(requestID));
  co_await publishDone_;
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

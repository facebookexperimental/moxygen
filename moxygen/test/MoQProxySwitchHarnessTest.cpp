/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <folly/coro/BlockingWait.h>
#include <folly/io/async/EventBase.h>
#include <folly/portability/GMock.h>
#include <folly/portability/GTest.h>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "moxygen/events/MoQFollyExecutorImpl.h"
#include "moxygen/test/MoQProxySwitchHarness.h"
#include "moxygen/test/MockMoQSession.h"

namespace moxygen::test { namespace {

using namespace testing;

const FullTrackName kTrack{TrackNamespace{{"live"}}, "video"};

Extensions fromSource(uint64_t hub, uint64_t subscription = 0) {
  Extensions extensions;
  extensions.insertMutableExtension(Extension(kHubIdExtensionType, hub));
  extensions.insertMutableExtension(
      Extension(kSubscriptionIdExtensionType, subscription));
  return extensions;
}

Payload originPayload(AbsoluteLocation location) {
  return folly::IOBuf::copyBuffer(SwitchOrigin::payload(kTrack, location));
}

class CheckingViewerTest : public Test {
 protected:
  // Delivers objects of one group on one subgroup stream, as subscription
  // `subscription` on hub `hub` publishes them, and records them as published.
  void deliverSubgroup(
      uint64_t group,
      const std::vector<uint64_t>& objects,
      uint64_t hub = 0,
      uint64_t subscription = 0) {
    auto stream = viewer_->beginSubgroup(group, 0, kDefaultPriority, {});
    ASSERT_TRUE(stream.hasValue());
    for (auto objectID : objects) {
      origin_->notePublished(kTrack, {group, objectID});
      (*stream)->object(
          objectID,
          originPayload({group, objectID}),
          fromSource(hub, subscription),
          false);
    }
  }

  void deliverDatagram(AbsoluteLocation location, uint64_t hub) {
    origin_->notePublished(kTrack, location);
    viewer_->datagram(
        ObjectHeader(
            location.group,
            0,
            location.object,
            kDefaultPriority,
            ObjectStatus::NORMAL,
            fromSource(hub)),
        originPayload(location),
        false);
  }

  std::shared_ptr<SwitchOrigin> origin_{std::make_shared<SwitchOrigin>()};
  std::shared_ptr<CheckingViewer> viewer_{
      std::make_shared<CheckingViewer>(origin_, kTrack)};
};

TEST_F(CheckingViewerTest, AcceptsTheOriginsObjects) {
  deliverSubgroup(0, {0, 1, 2, 3});

  EXPECT_THAT(viewer_->violations(), IsEmpty());
  EXPECT_THAT(viewer_->missing(), IsEmpty());
}

TEST_F(CheckingViewerTest, ReassemblesAnObjectSentInParts) {
  const auto payload = SwitchOrigin::payload(kTrack, {0, 0});
  origin_->notePublished(kTrack, {0, 0});
  auto stream = viewer_->beginSubgroup(0, 0, kDefaultPriority, {});

  (*stream)->beginObject(
      0,
      payload.size(),
      folly::IOBuf::copyBuffer(payload.substr(0, 3)),
      fromSource(0));
  EXPECT_THAT(viewer_->received(), IsEmpty());
  (*stream)->objectPayload(folly::IOBuf::copyBuffer(payload.substr(3)), false);

  const std::vector<AbsoluteLocation> received{{0, 0}};
  EXPECT_EQ(viewer_->received(), received);
  EXPECT_THAT(viewer_->violations(), IsEmpty());
}

TEST_F(CheckingViewerTest, FlagsACorruptPayload) {
  auto stream = viewer_->beginSubgroup(0, 0, kDefaultPriority, {});
  (*stream)->object(
      0, folly::IOBuf::copyBuffer("garbage"), fromSource(0), false);

  EXPECT_THAT(
      viewer_->violations(), ElementsAre("object 0/0 has payload 'garbage'"));
}

TEST_F(CheckingViewerTest, FlagsAnObjectReceivedTwice) {
  deliverSubgroup(0, {0, 1});
  deliverSubgroup(0, {1});

  EXPECT_THAT(viewer_->violations(), ElementsAre("object 0/1 received twice"));
}

TEST_F(CheckingViewerTest, FlagsAnObjectOnTheWrongSubgroup) {
  auto stream = viewer_->beginSubgroup(0, 1, kDefaultPriority, {});
  (*stream)->object(0, originPayload({0, 0}), fromSource(0), false);

  EXPECT_THAT(viewer_->violations(), ElementsAre("object 0/0 on subgroup 1"));
}

TEST_F(CheckingViewerTest, FlagsObjectsOutOfOrderInASubgroup) {
  deliverSubgroup(0, {1, 0});

  EXPECT_THAT(
      viewer_->violations(),
      ElementsAre("object 0/0 after object 1 in its subgroup"));
}

TEST_F(CheckingViewerTest, ReportsObjectsPublishedButNotReceived) {
  origin_->notePublished(kTrack, {0, 0});
  deliverSubgroup(1, {0, 2});
  origin_->notePublished(kTrack, {1, 1});
  origin_->notePublished(kTrack, {2, 0});

  const std::vector<AbsoluteLocation> missing{{1, 1}, {2, 0}};
  EXPECT_EQ(viewer_->missing(), missing);
}

TEST_F(CheckingViewerTest, ReportsAMissingStartOfTheExpectedRange) {
  viewer_->expectedStart = AbsoluteLocation{0, 2};
  for (uint64_t object = 1; object < 4; object++) {
    origin_->notePublished(kTrack, {0, object});
  }
  deliverSubgroup(1, {0, 1, 2, 3});

  const std::vector<AbsoluteLocation> missing{{0, 2}, {0, 3}};
  EXPECT_EQ(viewer_->missing(), missing);
}

TEST_F(CheckingViewerTest, ReportsTheExpectedRangeWhenNothingArrives) {
  viewer_->expectedStart = AbsoluteLocation{1, 0};
  origin_->notePublished(kTrack, {0, 3});
  origin_->notePublished(kTrack, {1, 0});

  const std::vector<AbsoluteLocation> missing{{1, 0}};
  EXPECT_EQ(viewer_->missing(), missing);
}

TEST_F(CheckingViewerTest, FlagsAnObjectBeforeTheExpectedStart) {
  viewer_->expectedStart = AbsoluteLocation{0, 2};
  deliverSubgroup(0, {1, 2, 3});

  EXPECT_THAT(
      viewer_->violations(),
      ElementsAre("object 0/1 before the expected start 0/2"));
}

TEST_F(CheckingViewerTest, FlagsAnObjectStreamOnTheWrongSubgroup) {
  origin_->notePublished(kTrack, {0, 0});
  viewer_->objectStream(
      ObjectHeader(
          0, 1, 0, kDefaultPriority, ObjectStatus::NORMAL, fromSource(0)),
      originPayload({0, 0}),
      false);

  EXPECT_THAT(viewer_->violations(), ElementsAre("object 0/0 on subgroup 1"));
}

TEST_F(CheckingViewerTest, FlagsAGroupFromTwoHubs) {
  deliverSubgroup(0, {0}, /*hub=*/1);
  deliverSubgroup(0, {1}, /*hub=*/2);

  EXPECT_THAT(
      viewer_->violations(), ElementsAre("group 0 came from hubs 1 and 2"));
}

TEST_F(CheckingViewerTest, FlagsAGroupFromTwoSubscriptionsOnOneHub) {
  deliverSubgroup(0, {0}, /*hub=*/1, /*subscription=*/0);
  deliverSubgroup(0, {1}, /*hub=*/1, /*subscription=*/1);

  EXPECT_THAT(
      viewer_->violations(),
      ElementsAre("group 0 came from subscriptions 0 and 1 on hub 1"));
}

TEST_F(CheckingViewerTest, FlagsAnObjectWithoutASource) {
  viewer_->datagram(ObjectHeader(0, 0, 0), originPayload({0, 0}), false);

  EXPECT_THAT(viewer_->violations(), ElementsAre("object 0/0 has no source"));
}

TEST_F(CheckingViewerTest, FlagsAnObjectOfAnotherTrackWithTheSameName) {
  const FullTrackName otherTrack{TrackNamespace{{"other"}}, kTrack.trackName};
  origin_->notePublished(kTrack, {0, 0});

  viewer_->datagram(
      ObjectHeader(
          0, 0, 0, kDefaultPriority, ObjectStatus::NORMAL, fromSource(0)),
      folly::IOBuf::copyBuffer(SwitchOrigin::payload(otherTrack, {0, 0})),
      false);

  EXPECT_THAT(
      viewer_->violations(), ElementsAre(HasSubstr("object 0/0 has payload")));
}

TEST_F(CheckingViewerTest, AcceptsAViewerJoiningMidGroup) {
  deliverSubgroup(0, {2, 3});

  EXPECT_THAT(viewer_->violations(), IsEmpty());
}

TEST_F(CheckingViewerTest, FlagsALaterSourceThatStartsMidGroup) {
  deliverSubgroup(0, {0, 1, 2, 3}, /*hub=*/1);
  deliverSubgroup(1, {2, 3}, /*hub=*/2);

  EXPECT_THAT(
      viewer_->violations(),
      ElementsAre("hub 2 subscription 0 started in group 1 without object 0"));
}

// The viewer was owed group 1 from object 0, though nothing arrived before it.
TEST_F(CheckingViewerTest, FlagsAFirstSourceThatStartsMidGroupAfterTheStart) {
  viewer_->expectedStart = AbsoluteLocation{0, 2};
  deliverSubgroup(1, {2, 3}, /*hub=*/2);

  EXPECT_THAT(
      viewer_->violations(),
      ElementsAre("hub 2 subscription 0 started in group 1 without object 0"));
}

TEST_F(CheckingViewerTest, ChecksSourcesByGroupNotByArrival) {
  deliverSubgroup(1, {0, 1, 2, 3}, /*hub=*/2);
  deliverSubgroup(0, {2, 3}, /*hub=*/1);

  EXPECT_THAT(viewer_->violations(), IsEmpty());
}

TEST_F(CheckingViewerTest, FlagsALaterSourceThatStartsMidGroupAndArrivesFirst) {
  deliverSubgroup(1, {2, 3}, /*hub=*/2);
  deliverSubgroup(0, {2, 3}, /*hub=*/1);

  EXPECT_THAT(
      viewer_->violations(),
      ElementsAre("hub 2 subscription 0 started in group 1 without object 0"));
}

TEST_F(CheckingViewerTest, AcceptsALaterSourceWhoseFirstObjectArrivesLast) {
  deliverSubgroup(0, {0, 1, 2, 3}, /*hub=*/1);
  deliverDatagram({1, 1}, /*hub=*/2);
  EXPECT_THAT(viewer_->violations(), SizeIs(1));

  deliverDatagram({1, 0}, /*hub=*/2);

  EXPECT_THAT(viewer_->violations(), IsEmpty());
}

TEST_F(CheckingViewerTest, FlagsAnUnexpectedPublishDone) {
  viewer_->publishDone(
      PublishDone{
          RequestID(1), PublishDoneStatusCode::TRACK_ENDED, 0, "ended"});

  EXPECT_THAT(
      viewer_->violations(), ElementsAre("unexpected PUBLISH_DONE 2: ended"));
}

TEST_F(CheckingViewerTest, AcceptsTheExpectedPublishDoneOnceAndFlagsASecond) {
  viewer_->expectedEnd = PublishDoneStatusCode::GOING_AWAY;
  viewer_->publishDone(
      PublishDone{RequestID(1), PublishDoneStatusCode::GOING_AWAY, 0, "away"});
  EXPECT_THAT(viewer_->violations(), IsEmpty());

  viewer_->publishDone(
      PublishDone{RequestID(1), PublishDoneStatusCode::GOING_AWAY, 0, "again"});

  EXPECT_THAT(
      viewer_->violations(), ElementsAre("second PUBLISH_DONE 4: again"));
  EXPECT_EQ(viewer_->done()->reasonPhrase, "away");
}

TEST_F(CheckingViewerTest, FlagsAnExpectedPublishDoneThatNeverArrives) {
  viewer_->expectedEnd = PublishDoneStatusCode::GOING_AWAY;

  EXPECT_THAT(
      viewer_->violations(),
      ElementsAre("expected PUBLISH_DONE 4 never arrived"));
}

TEST_F(CheckingViewerTest, RecordsResetsAndFlagsOnlyUnexpectedOnes) {
  (*viewer_->beginSubgroup(3, 0, kDefaultPriority, {}))
      ->reset(ResetStreamErrorCode::CANCELLED);
  viewer_->expectedReset = ResetStreamErrorCode::CANCELLED;
  (*viewer_->beginSubgroup(4, 0, kDefaultPriority, {}))
      ->reset(ResetStreamErrorCode::CANCELLED);
  (*viewer_->beginSubgroup(5, 0, kDefaultPriority, {}))
      ->reset(ResetStreamErrorCode::INTERNAL_ERROR);

  EXPECT_THAT(
      viewer_->violations(),
      ElementsAre("subgroup 3/0 reset with 1", "subgroup 5/0 reset with 0"));
  const std::vector<CheckingViewer::Reset> resets{
      {3, 0, ResetStreamErrorCode::CANCELLED},
      {4, 0, ResetStreamErrorCode::CANCELLED},
      {5, 0, ResetStreamErrorCode::INTERNAL_ERROR}};
  EXPECT_EQ(viewer_->resets(), resets);
  EXPECT_THAT(viewer_->unfinishedSubgroups(), IsEmpty());
}

TEST_F(CheckingViewerTest, ReportsSubgroupsThatNeverEnd) {
  deliverSubgroup(0, {0, 1});
  auto finished = viewer_->beginSubgroup(1, 0, kDefaultPriority, {});
  (*finished)->object(0, originPayload({1, 0}), fromSource(0), true);
  auto ended = viewer_->beginSubgroup(2, 0, kDefaultPriority, {});
  (*ended)->endOfSubgroup();
  auto groupEnded = viewer_->beginSubgroup(3, 0, kDefaultPriority, {});
  (*groupEnded)->endOfGroup(0);

  const std::vector<std::pair<uint64_t, uint64_t>> unfinished{{0, 0}};
  EXPECT_EQ(viewer_->unfinishedSubgroups(), unfinished);
}

TEST_F(CheckingViewerTest, FlagsDataOrASecondEndAfterASubgroupEnds) {
  auto stream = viewer_->beginSubgroup(0, 0, kDefaultPriority, {});
  (*stream)->object(0, originPayload({0, 0}), fromSource(0), true);
  (*stream)->object(1, originPayload({0, 1}), fromSource(0), false);
  (*stream)->endOfSubgroup();

  EXPECT_THAT(
      viewer_->violations(),
      ElementsAre(
          "object 0/1 after its subgroup ended", "subgroup 0/0 ended twice"));
}

class SwitchTestHubTest : public Test {
 protected:
  std::shared_ptr<Publisher::SubscriptionHandle> subscribe(
      const std::shared_ptr<TrackConsumer>& consumer,
      LocationFilter filter = LocationFilter::nextObject()) {
    auto request = SubscribeRequest::make(kTrack);
    request.filter = filter;
    auto result = folly::coro::blockingWait(
        hub_->subscribe(std::move(request), consumer));
    EXPECT_TRUE(result.hasValue());
    return result.hasValue() ? result.value() : nullptr;
  }

  std::shared_ptr<SwitchOrigin> origin_{std::make_shared<SwitchOrigin>()};
  std::shared_ptr<SwitchTestHub> hub_{
      std::make_shared<SwitchTestHub>(7, origin_)};
  std::shared_ptr<CheckingViewer> viewer_{
      std::make_shared<CheckingViewer>(origin_, kTrack)};
};

TEST_F(SwitchTestHubTest, SubscribeOkCarriesTheLargestPublished) {
  hub_->publishGroup(kTrack, 3);

  auto handle = subscribe(viewer_);

  EXPECT_EQ(handle->subscribeOk().largest, AbsoluteLocation(3, 3));
}

TEST_F(SwitchTestHubTest, PublishContinuesAPartialGroup) {
  auto handle = subscribe(viewer_);

  hub_->publish(kTrack, 0, 2);
  hub_->publishGroup(kTrack, 0);

  const std::vector<AbsoluteLocation> received{{0, 0}, {0, 1}, {0, 2}, {0, 3}};
  EXPECT_EQ(viewer_->received(), received);
  EXPECT_THAT(viewer_->violations(), IsEmpty());
}

TEST_F(SwitchTestHubTest, NextObjectStartsAfterTheLargestPublished) {
  hub_->publish(kTrack, 0, 2);
  auto handle = subscribe(viewer_, LocationFilter::nextObject());

  hub_->publishGroup(kTrack, 0);

  const std::vector<AbsoluteLocation> received{{0, 2}, {0, 3}};
  EXPECT_EQ(viewer_->received(), received);
}

TEST_F(SwitchTestHubTest, NextGroupStartSkipsTheGroupInProgress) {
  hub_->publish(kTrack, 0, 2);
  auto handle = subscribe(viewer_, LocationFilter::relativeStart(0));

  hub_->publishGroup(kTrack, 0);
  hub_->publishGroup(kTrack, 1);

  const std::vector<AbsoluteLocation> received{{1, 0}, {1, 1}, {1, 2}, {1, 3}};
  EXPECT_EQ(viewer_->received(), received);
  EXPECT_THAT(viewer_->violations(), IsEmpty());
}

TEST_F(SwitchTestHubTest, RecordsEverySubscribeFilter) {
  auto first = subscribe(viewer_, LocationFilter::nextObject());
  first->unsubscribe();
  auto second = subscribe(viewer_, LocationFilter::relativeStart(0));

  EXPECT_THAT(
      hub_->subscribeFilters(kTrack),
      ElementsAre(
          LocationFilter::nextObject(), LocationFilter::relativeStart(0)));
}

TEST_F(SwitchTestHubTest, StreamsOpenedMidSubgroupDoNotClaimItsFirstObject) {
  auto origin = std::make_shared<SwitchOrigin>(
      SwitchTrackShape{.objectsPerGroup = 5, .subgroupsPerGroup = 3});
  auto hub = std::make_shared<SwitchTestHub>(1, origin);
  auto viewer = std::make_shared<CheckingViewer>(origin, kTrack);
  hub->publish(kTrack, 0, 2);
  auto request = SubscribeRequest::make(kTrack);
  request.filter = LocationFilter::nextObject();
  auto handle =
      folly::coro::blockingWait(hub->subscribe(std::move(request), viewer));
  ASSERT_TRUE(handle.hasValue());

  hub->publishGroup(kTrack, 0);
  hub->publishGroup(kTrack, 1);

  // Objects 0 and 1 went out before the subscription.
  const std::vector<CheckingViewer::OpenedSubgroup> opened{
      {0, 2, true},
      {0, 0, false},
      {0, 1, false},
      {1, 0, true},
      {1, 1, true},
      {1, 2, true}};
  EXPECT_EQ(viewer->openedSubgroups(), opened);
  EXPECT_THAT(viewer->violations(), IsEmpty());
}

TEST_F(SwitchTestHubTest, EachSubscriptionStampsItsOwnId) {
  auto otherViewer = std::make_shared<CheckingViewer>(origin_, kTrack);
  auto first = subscribe(viewer_);
  auto second = subscribe(otherViewer);

  hub_->publishGroup(kTrack, 0);

  const std::map<uint64_t, CheckingViewer::Source> firstSources{{0, {7, 0}}};
  const std::map<uint64_t, CheckingViewer::Source> secondSources{{0, {7, 1}}};
  EXPECT_EQ(viewer_->sourceOfGroup(), firstSources);
  EXPECT_EQ(otherViewer->sourceOfGroup(), secondSources);
}

TEST_F(SwitchTestHubTest, EndingSubscriptionsResetsOpenStreamsThenEndsThem) {
  auto handle = subscribe(viewer_);
  hub_->publish(kTrack, 0, 2);
  viewer_->expectedEnd = PublishDoneStatusCode::GOING_AWAY;
  viewer_->expectedReset = ResetStreamErrorCode::INTERNAL_ERROR;

  hub_->endSubscriptions(
      kTrack,
      PublishDoneStatusCode::GOING_AWAY,
      ResetStreamErrorCode::INTERNAL_ERROR);

  ASSERT_TRUE(viewer_->done().has_value());
  EXPECT_THAT(viewer_->violations(), IsEmpty());
  const std::vector<CheckingViewer::Reset> resets{
      {0, 0, ResetStreamErrorCode::INTERNAL_ERROR}};
  EXPECT_EQ(viewer_->resets(), resets);
  EXPECT_THAT(viewer_->unfinishedSubgroups(), IsEmpty());
  EXPECT_EQ(hub_->subscriptions(kTrack), 0);
}

TEST_F(SwitchTestHubTest, UnsubscribedViewersGetNothing) {
  auto handle = subscribe(viewer_);

  handle->unsubscribe();
  hub_->publishGroup(kTrack, 0);

  EXPECT_THAT(viewer_->received(), IsEmpty());
  EXPECT_EQ(hub_->subscriptions(kTrack), 0);
}

struct ShapeCase {
  std::string name;
  SwitchTrackShape shape;
};

class SwitchTestHubShapeTest : public TestWithParam<ShapeCase> {};

// Whole groups and a partial one, in every layout, pass the viewer's checks.
TEST_P(SwitchTestHubShapeTest, ViewerAcceptsWhatTheHubPublishes) {
  auto origin = std::make_shared<SwitchOrigin>(GetParam().shape);
  auto hub = std::make_shared<SwitchTestHub>(1, origin);
  auto viewer = std::make_shared<CheckingViewer>(origin, kTrack);
  auto request = SubscribeRequest::make(kTrack);
  request.filter = LocationFilter::nextObject();
  auto handle =
      folly::coro::blockingWait(hub->subscribe(std::move(request), viewer));
  ASSERT_TRUE(handle.hasValue());

  hub->publishGroup(kTrack, 0);
  hub->publishGroup(kTrack, 2);
  hub->publish(kTrack, 5, 2);

  EXPECT_THAT(viewer->violations(), IsEmpty());
  EXPECT_THAT(viewer->missing(), IsEmpty());
  EXPECT_EQ(
      viewer->received().size(), 2 * GetParam().shape.objectsPerGroup + 2);
  const std::map<uint64_t, uint64_t> hubOfGroup{{0, 1}, {2, 1}, {5, 1}};
  EXPECT_EQ(viewer->hubOfGroup(), hubOfGroup);
  hub->publishGroup(kTrack, 5);
  EXPECT_THAT(viewer->unfinishedSubgroups(), IsEmpty());
}

INSTANTIATE_TEST_SUITE_P(
    Shapes,
    SwitchTestHubShapeTest,
    Values(
        ShapeCase{"OneSubgroup", {}},
        ShapeCase{
            "ThreeSubgroups",
            {.objectsPerGroup = 5, .subgroupsPerGroup = 3}},
        ShapeCase{
            "ObjectStreams",
            {.delivery = SwitchTrackShape::Delivery::ObjectStreams}},
        ShapeCase{
            "Datagrams",
            {.delivery = SwitchTrackShape::Delivery::Datagrams}}),
    [](const TestParamInfo<ShapeCase>& info) { return info.param.name; });

class ScriptedUpstreamProviderTest : public Test {
 protected:
  std::shared_ptr<MoQSession> makeSession() {
    return std::make_shared<NiceMock<MockMoQSession>>(executor_);
  }

  MoQUpstreamSessionResult getSession() {
    return folly::coro::blockingWait(provider_.getSession(
        kTrack,
        TrackRequestParameters{FrameType::SUBSCRIBE},
        /*hasFallbackProvider=*/false));
  }

  std::vector<std::optional<size_t>> attemptedUpstreams() const {
    std::vector<std::optional<size_t>> upstreams;
    for (const auto& attempt : provider_.attempts()) {
      upstreams.push_back(attempt.upstream);
    }
    return upstreams;
  }

  folly::EventBase eventBase_;
  std::shared_ptr<MoQFollyExecutorImpl> executor_{
      std::make_shared<MoQFollyExecutorImpl>(&eventBase_)};
  ScriptedUpstreamProvider provider_;
};

TEST_F(ScriptedUpstreamProviderTest, RoutesToTheFirstAvailableUpstream) {
  auto first = makeSession();
  auto second = makeSession();
  provider_.addUpstream(first);
  provider_.addUpstream(second);

  auto beforeDrain = getSession();
  provider_.setAvailable(0, false);
  auto afterDrain = getSession();
  provider_.setAvailable(1, false);
  auto noneLeft = getSession();

  EXPECT_EQ(beforeDrain.value(), first);
  EXPECT_EQ(afterDrain.value(), second);
  EXPECT_TRUE(noneLeft.hasError());
  const std::vector<std::optional<size_t>> upstreams{0, 1, std::nullopt};
  EXPECT_EQ(attemptedUpstreams(), upstreams);
}

TEST_F(ScriptedUpstreamProviderTest, FailsTheNextAttempts) {
  provider_.addUpstream(makeSession());
  provider_.failNextAttempts(2);

  auto first = getSession();
  auto second = getSession();
  auto third = getSession();

  EXPECT_TRUE(first.hasError());
  EXPECT_TRUE(second.hasError());
  EXPECT_TRUE(third.hasValue());
  const std::vector<std::optional<size_t>> upstreams{
      std::nullopt, std::nullopt, 0};
  EXPECT_EQ(attemptedUpstreams(), upstreams);
}

}} // namespace moxygen::test

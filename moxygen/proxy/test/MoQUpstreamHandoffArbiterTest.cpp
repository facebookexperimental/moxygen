/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <folly/portability/GTest.h>
#include <algorithm>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "moxygen/proxy/MoQUpstreamHandoffArbiter.h"

namespace moxygen { namespace {

using Admit = MoQUpstreamHandoffArbiter::Admit;
using Source = MoQUpstreamHandoffArbiter::Source;

constexpr auto kAccept = Admit::Accept;
constexpr auto kRefuse = Admit::Refuse;

class MoQUpstreamHandoffArbiterTest : public testing::Test {
 protected:
  Admit oldOpens(uint64_t group, uint64_t subgroup = 0) {
    return arbiter_.onBeginSubgroup(Source::Old, group, subgroup);
  }
  Admit replacementOpens(uint64_t group, uint64_t subgroup = 0) {
    return arbiter_.onBeginSubgroup(Source::Replacement, group, subgroup);
  }
  void oldFinishes(uint64_t group, uint64_t subgroup = 0) {
    arbiter_.onSubgroupEnded(Source::Old, group, subgroup);
  }
  Admit oldDatagram(uint64_t group, bool lastInGroup = false) {
    return arbiter_.onDatagram(Source::Old, group, lastInGroup);
  }
  void replacementObject(uint64_t group) {
    arbiter_.onObject(Source::Replacement, group);
  }
  Admit replacementDatagram(uint64_t group) {
    return arbiter_.onDatagram(Source::Replacement, group, false);
  }

  MoQUpstreamHandoffArbiter arbiter_;
};

TEST_F(MoQUpstreamHandoffArbiterTest, OldForwardsAsBeforeUntilTheReplacement) {
  EXPECT_EQ(oldOpens(10), kAccept);
  EXPECT_EQ(oldOpens(11), kAccept);

  EXPECT_FALSE(arbiter_.split().has_value());
  EXPECT_FALSE(arbiter_.oldCaughtUp());
}

// The replacement starts at a group start, so any of its subgroups may arrive
// first.
TEST_F(
    MoQUpstreamHandoffArbiterTest,
    ReplacementsFirstSubgroupFreezesTheSplit) {
  oldOpens(12);

  EXPECT_EQ(replacementOpens(13, /*subgroup=*/2), kAccept);
  EXPECT_EQ(replacementOpens(13, /*subgroup=*/0), kAccept);

  EXPECT_EQ(arbiter_.split()->replacementFirstGroup, 13);
  EXPECT_EQ(arbiter_.split()->oldLastGroup, 12);
}

TEST_F(MoQUpstreamHandoffArbiterTest, GroupsForwardedBeforeTheSwitchAreOlds) {
  MoQUpstreamHandoffArbiter arbiter({.highestGroup = 20});

  EXPECT_EQ(arbiter.onBeginSubgroup(Source::Replacement, 20, 0), kRefuse);
  EXPECT_EQ(arbiter.onBeginSubgroup(Source::Replacement, 21, 0), kAccept);
}

// Old opens some groups, then the replacement delivers its groups in order,
// then old opens more.
struct SplitCase {
  std::string name;
  std::vector<uint64_t> oldOpensFirst;
  std::vector<std::pair<uint64_t, Admit>> replacementOpens;
  std::vector<std::pair<uint64_t, Admit>> oldOpensAfter;
};

class MoQUpstreamHandoffArbiterSplitTest
    : public MoQUpstreamHandoffArbiterTest,
      public testing::WithParamInterface<SplitCase> {};

// Each group comes from exactly one attempt, and old has caught up once it
// reaches a replacement group and finishes its own.
TEST_P(MoQUpstreamHandoffArbiterSplitTest, EachGroupHasOneOwner) {
  const auto& splitCase = GetParam();
  std::vector<uint64_t> oldGroups;
  for (auto group : splitCase.oldOpensFirst) {
    EXPECT_EQ(oldOpens(group), kAccept) << "old group " << group;
    oldGroups.push_back(group);
  }

  for (const auto& [group, admit] : splitCase.replacementOpens) {
    EXPECT_EQ(replacementOpens(group), admit) << "replacement group " << group;
  }
  for (const auto& [group, admit] : splitCase.oldOpensAfter) {
    EXPECT_EQ(oldOpens(group), admit) << "old group " << group;
    if (admit == kAccept) {
      oldGroups.push_back(group);
    }
  }
  EXPECT_FALSE(arbiter_.oldCaughtUp());
  for (auto group : oldGroups) {
    oldFinishes(group);
  }

  EXPECT_TRUE(arbiter_.oldCaughtUp());
  EXPECT_EQ(
      arbiter_.split()->replacementFirstGroup,
      splitCase.replacementOpens.front().first);
  EXPECT_EQ(
      arbiter_.split()->oldLastGroup,
      *std::max_element(
          splitCase.oldOpensFirst.begin(), splitCase.oldOpensFirst.end()));
}

INSTANTIATE_TEST_SUITE_P(
    Splits,
    MoQUpstreamHandoffArbiterSplitTest,
    testing::Values(
        SplitCase{
            "ReplacementBehindByMoreThanAGroup",
            {10, 11, 12},
            {{10, kRefuse}, {11, kRefuse}, {12, kRefuse}, {13, kAccept}},
            {{13, kRefuse}}},
        SplitCase{
            "ReplacementBehindByLessThanAGroup",
            {11, 12},
            {{12, kRefuse}, {13, kAccept}},
            {{13, kRefuse}}},
        SplitCase{
            "ReplacementStartsRightAfterOld",
            {11, 12},
            {{13, kAccept}, {14, kAccept}},
            {{13, kRefuse}}},
        SplitCase{
            "ReplacementAheadByMoreThanAGroup",
            {11, 12},
            {{14, kAccept}, {15, kAccept}},
            {{13, kAccept}, {14, kRefuse}}},
        SplitCase{
            "SkippingGroupIdsReplacementBehind",
            {10, 12},
            {{12, kRefuse}, {15, kAccept}},
            {{15, kRefuse}}},
        SplitCase{
            "SkippingGroupIdsReplacementAhead",
            {10, 12},
            {{15, kAccept}},
            {{13, kAccept}, {14, kAccept}, {15, kRefuse}}}),
    [](const testing::TestParamInfo<SplitCase>& info) {
      return info.param.name;
    });

TEST_F(MoQUpstreamHandoffArbiterTest, AllSubgroupsOfAGroupHaveOneOwner) {
  oldOpens(12, 0);
  oldOpens(12, 1);

  EXPECT_EQ(replacementOpens(12, 0), kRefuse);
  EXPECT_EQ(replacementOpens(12, 1), kRefuse);
  EXPECT_EQ(replacementOpens(13, 0), kAccept);
  EXPECT_EQ(replacementOpens(13, 1), kAccept);
  EXPECT_EQ(oldOpens(13, 0), kRefuse);
  oldFinishes(12, 0);
  EXPECT_FALSE(arbiter_.oldCaughtUp());
  oldFinishes(12, 1);

  EXPECT_TRUE(arbiter_.oldCaughtUp());
}

// Old may still hold subgroups it opened before the switch, and open more
// before the split: it has caught up only once all of them finish.
TEST_F(MoQUpstreamHandoffArbiterTest, OldFinishesTheSubgroupsOpenAtTheSwitch) {
  MoQUpstreamHandoffArbiter arbiter(
      {.highestGroup = 12, .openSubgroups = {{12, 0}}});

  EXPECT_EQ(arbiter.onBeginSubgroup(Source::Old, 13, 0), kAccept);
  EXPECT_EQ(arbiter.onBeginSubgroup(Source::Replacement, 14, 0), kAccept);
  arbiter.onObject(Source::Replacement, 14);
  EXPECT_EQ(arbiter.split()->oldLastGroup, 13);
  EXPECT_EQ(arbiter.onBeginSubgroup(Source::Old, 14, 0), kRefuse);
  arbiter.onSubgroupEnded(Source::Old, 13, 0);
  EXPECT_FALSE(arbiter.oldCaughtUp());
  EXPECT_TRUE(arbiter.oldBlocksReplacement());
  arbiter.onSubgroupEnded(Source::Old, 12, 0);

  EXPECT_TRUE(arbiter.oldCaughtUp());
}

TEST_F(MoQUpstreamHandoffArbiterTest, DatagramsFollowGroupOwnership) {
  oldDatagram(12);
  oldDatagram(12);

  EXPECT_EQ(replacementDatagram(13), kAccept);
  EXPECT_EQ(replacementDatagram(12), kRefuse);
  EXPECT_EQ(replacementDatagram(13), kAccept);
  EXPECT_EQ(oldDatagram(12), kAccept);
  EXPECT_EQ(oldDatagram(13), kRefuse);
  EXPECT_EQ(arbiter_.split()->replacementFirstGroup, 13);
}

// An old datagram group is open until its last datagram arrives.
TEST_F(MoQUpstreamHandoffArbiterTest, OldDatagramGroupIsOpenUntilItsLast) {
  oldDatagram(12);
  replacementDatagram(13);
  EXPECT_TRUE(arbiter_.oldBlocksReplacement());

  EXPECT_EQ(oldDatagram(12, /*lastInGroup=*/true), kAccept);

  EXPECT_FALSE(arbiter_.oldBlocksReplacement());
}

// Publishers needn't flag a group's last datagram, and it can be lost, so old
// moving on to a later group closes the earlier one too. Datagrams arrive out
// of order, so a late one for that group is still old's.
TEST_F(MoQUpstreamHandoffArbiterTest, OldDatagramGroupClosesWhenOldMovesOn) {
  oldDatagram(11);
  oldDatagram(12);
  replacementDatagram(13);

  EXPECT_EQ(oldDatagram(13), kRefuse);

  EXPECT_TRUE(arbiter_.oldCaughtUp());
  EXPECT_EQ(oldDatagram(12), kAccept);
  EXPECT_EQ(oldDatagram(11), kAccept);
  EXPECT_TRUE(arbiter_.oldCaughtUp());
}

TEST_F(MoQUpstreamHandoffArbiterTest, LateDatagramDoesNotReopenAFinishedGroup) {
  oldDatagram(12, /*lastInGroup=*/true);
  EXPECT_EQ(oldDatagram(12), kAccept);

  replacementDatagram(13);

  EXPECT_FALSE(arbiter_.oldBlocksReplacement());
}

TEST_F(
    MoQUpstreamHandoffArbiterTest,
    OldFinishesTheDatagramGroupOpenAtTheSwitch) {
  MoQUpstreamHandoffArbiter arbiter(
      {.highestGroup = 12, .openDatagramGroup = 12});

  EXPECT_EQ(arbiter.onDatagram(Source::Replacement, 13, false), kAccept);
  EXPECT_TRUE(arbiter.oldBlocksReplacement());
  EXPECT_EQ(arbiter.onDatagram(Source::Old, 12, true), kAccept);

  EXPECT_FALSE(arbiter.oldBlocksReplacement());
}

// Whatever old had open, it had opened, even past the highest group the caller
// reported.
TEST_F(MoQUpstreamHandoffArbiterTest, OpenGroupsAtTheSwitchCountAsOpened) {
  MoQUpstreamHandoffArbiter subgroups(
      {.highestGroup = 11, .openSubgroups = {{12, 0}}});
  MoQUpstreamHandoffArbiter datagrams({.openDatagramGroup = 12});

  EXPECT_EQ(subgroups.onBeginSubgroup(Source::Replacement, 12, 0), kRefuse);
  EXPECT_EQ(subgroups.split()->oldLastGroup, 12);
  EXPECT_EQ(datagrams.onDatagram(Source::Replacement, 12, false), kRefuse);
  EXPECT_EQ(datagrams.split()->oldLastGroup, 12);
}

// Draft 18+ says when a stream starts mid-subgroup. The replacement is then
// missing that group's first objects, so neither it nor an earlier group can
// be the replacement's.
TEST_F(
    MoQUpstreamHandoffArbiterTest,
    StreamStartingMidSubgroupCantSetTheSplit) {
  oldOpens(12);

  EXPECT_EQ(
      arbiter_.onBeginSubgroup(
          Source::Replacement, 13, 0, /*startsMidSubgroup=*/true),
      kRefuse);
  EXPECT_FALSE(arbiter_.split().has_value());
  EXPECT_EQ(replacementOpens(13, /*subgroup=*/1), kRefuse);
  EXPECT_EQ(replacementOpens(14), kAccept);
  EXPECT_EQ(arbiter_.split()->replacementFirstGroup, 14);
  EXPECT_EQ(oldOpens(13), kAccept);
  EXPECT_EQ(
      arbiter_.onBeginSubgroup(Source::Old, 13, 1, /*startsMidSubgroup=*/true),
      kAccept);
}

// A mid-subgroup stream that arrives after its group became the
// replacement's can't be undone: the group reaches viewers without it.
TEST_F(MoQUpstreamHandoffArbiterTest, StreamStartingMidSubgroupLaterIsAGap) {
  oldOpens(12);
  EXPECT_EQ(replacementOpens(13, /*subgroup=*/0), kAccept);

  EXPECT_EQ(
      arbiter_.onBeginSubgroup(
          Source::Replacement, 13, 1, /*startsMidSubgroup=*/true),
      Admit::RefuseWithGap);
  EXPECT_EQ(
      arbiter_.onBeginSubgroup(
          Source::Replacement, 14, 0, /*startsMidSubgroup=*/true),
      Admit::RefuseWithGap);
  EXPECT_EQ(replacementOpens(13, /*subgroup=*/2), kAccept);
  arbiter_.onOldEnded();
  EXPECT_EQ(replacementOpens(13, /*subgroup=*/3), kAccept);
  EXPECT_EQ(replacementOpens(14, /*subgroup=*/1), kAccept);
}

// Old's last group counts as delivered even if old ended inside it, so the
// replacement never repeats a group viewers saw part of. Every group old never
// opened is the replacement's, whichever arrives first.
TEST_F(MoQUpstreamHandoffArbiterTest, OldEndingBeforeTheSplitKeepsItsGroups) {
  oldOpens(12);
  arbiter_.onOldEnded();

  EXPECT_EQ(oldOpens(13), kRefuse);
  EXPECT_EQ(replacementOpens(16), kAccept);
  EXPECT_EQ(replacementOpens(15), kAccept);
  EXPECT_EQ(replacementOpens(12), kRefuse);
  EXPECT_FALSE(arbiter_.split().has_value());
}

// A replacement that starts right after the track's Largest, in group 12,
// continues that group once old has ended with nothing left to send.
TEST_F(MoQUpstreamHandoffArbiterTest, AReplacementCanContinueOldsLastGroup) {
  oldOpens(12);
  oldFinishes(12);
  arbiter_.onOldEnded(/*continuedGroup=*/12);

  EXPECT_EQ(replacementOpens(12, /*subgroup=*/1), kAccept);
  EXPECT_EQ(replacementOpens(11), kRefuse);
  EXPECT_EQ(replacementOpens(13), kAccept);
}

// Viewers have the first objects of group 12's subgroups, so a replacement
// stream there that starts mid-subgroup goes on from them, even as the
// replacement's first data. In a later group it's missing them.
TEST_F(
    MoQUpstreamHandoffArbiterTest,
    AContinuedGroupTakesStreamsThatStartMidSubgroup) {
  oldOpens(12);
  oldFinishes(12);
  arbiter_.onOldEnded(/*continuedGroup=*/12);

  EXPECT_EQ(
      arbiter_.onBeginSubgroup(
          Source::Replacement, 12, 0, /*startsMidSubgroup=*/true),
      kAccept);
  EXPECT_EQ(
      arbiter_.onBeginSubgroup(
          Source::Replacement, 13, 0, /*startsMidSubgroup=*/true),
      Admit::RefuseWithGap);
  EXPECT_EQ(replacementOpens(13, /*subgroup=*/1), kAccept);
}

// Not when old opened a later group, or replacement data for that group was
// refused.
TEST_F(
    MoQUpstreamHandoffArbiterTest,
    AContinuedGroupMustBeOldsLastAndUnrefused) {
  oldOpens(12);
  oldOpens(13);
  arbiter_.onOldEnded(/*continuedGroup=*/12);
  EXPECT_EQ(
      arbiter_.onBeginSubgroup(
          Source::Replacement, 12, 0, /*startsMidSubgroup=*/true),
      kRefuse);
  EXPECT_EQ(replacementOpens(12, /*subgroup=*/1), kRefuse);
  EXPECT_EQ(replacementOpens(13, /*subgroup=*/1), kRefuse);
  EXPECT_EQ(replacementOpens(14), kAccept);

  MoQUpstreamHandoffArbiter refused;
  EXPECT_EQ(refused.onBeginSubgroup(Source::Old, 12, 0), kAccept);
  EXPECT_EQ(
      refused.onBeginSubgroup(
          Source::Replacement, 12, 0, /*startsMidSubgroup=*/true),
      kRefuse);
  refused.onOldEnded(/*continuedGroup=*/12);
  EXPECT_EQ(refused.onBeginSubgroup(Source::Replacement, 12, 1), kRefuse);
  EXPECT_EQ(refused.onBeginSubgroup(Source::Replacement, 13, 0), kAccept);
}

// Refused replacement data shows, whether its stream started mid-subgroup or
// its group was old's. Accepted data doesn't count.
TEST_F(MoQUpstreamHandoffArbiterTest, ItTellsWhetherReplacementDataWasRefused) {
  oldOpens(10);
  EXPECT_FALSE(arbiter_.replacementRefused());
  EXPECT_EQ(
      arbiter_.onBeginSubgroup(
          Source::Replacement, 10, 0, /*startsMidSubgroup=*/true),
      kRefuse);
  EXPECT_TRUE(arbiter_.replacementRefused());

  MoQUpstreamHandoffArbiter oldsGroup;
  EXPECT_EQ(oldsGroup.onBeginSubgroup(Source::Old, 10, 0), kAccept);
  EXPECT_EQ(oldsGroup.onBeginSubgroup(Source::Replacement, 11, 0), kAccept);
  EXPECT_FALSE(oldsGroup.replacementRefused());
  EXPECT_EQ(oldsGroup.onBeginSubgroup(Source::Replacement, 10, 1), kRefuse);
  EXPECT_TRUE(oldsGroup.replacementRefused());
}

// Once old ends it can't deliver the groups it never opened, so the
// replacement takes those too, even below the split.
TEST_F(MoQUpstreamHandoffArbiterTest, OldEndingHandsOverGroupsItNeverOpened) {
  oldOpens(12);
  replacementOpens(16);
  EXPECT_EQ(oldOpens(13), kAccept);

  arbiter_.onOldEnded();

  EXPECT_EQ(replacementOpens(15), kAccept);
  EXPECT_EQ(replacementOpens(14), kAccept);
  EXPECT_EQ(replacementOpens(13), kRefuse);
  EXPECT_EQ(replacementOpens(17), kAccept);
  // The split stays as it froze.
  EXPECT_EQ(arbiter_.split()->replacementFirstGroup, 16);
  EXPECT_EQ(arbiter_.split()->oldLastGroup, 12);
}

// The replacement's refused data never reached viewers, so its group can't
// become the replacement's when old ends, nor can a group below it.
TEST_F(
    MoQUpstreamHandoffArbiterTest,
    OldEndingKeepsGroupsWithRefusedReplacementDataOffLimits) {
  oldOpens(12);
  replacementOpens(16);
  EXPECT_EQ(replacementOpens(15, /*subgroup=*/0), kRefuse);
  oldOpens(13);

  arbiter_.onOldEnded();

  EXPECT_EQ(replacementOpens(15, /*subgroup=*/1), kRefuse);
  EXPECT_EQ(replacementOpens(14), kRefuse);
  EXPECT_EQ(replacementOpens(17), kAccept);
}

TEST_F(
    MoQUpstreamHandoffArbiterTest,
    OldEndingKeepsGroupsWithRefusedReplacementDatagramsOffLimits) {
  oldOpens(12);
  replacementDatagram(16);
  EXPECT_EQ(replacementDatagram(15), kRefuse);

  arbiter_.onOldEnded();

  EXPECT_EQ(replacementDatagram(15), kRefuse);
  EXPECT_EQ(replacementDatagram(16), kAccept);
}

// A group the replacement started mid-subgroup stays off limits after old
// ends.
TEST_F(MoQUpstreamHandoffArbiterTest, OldEndingKeepsPartialGroupsOffLimits) {
  oldOpens(12);
  arbiter_.onBeginSubgroup(
      Source::Replacement, 14, 0, /*startsMidSubgroup=*/true);

  arbiter_.onOldEnded();

  EXPECT_EQ(replacementOpens(14, /*subgroup=*/1), kRefuse);
  EXPECT_EQ(replacementOpens(13), kRefuse);
  EXPECT_EQ(replacementOpens(15), kAccept);
}

TEST_F(MoQUpstreamHandoffArbiterTest, OldsUnfinishedGroupsAreItsOpenOnes) {
  MoQUpstreamHandoffArbiter arbiter(
      {.highestGroup = 12, .openSubgroups = {{11, 0}}});
  arbiter.onBeginSubgroup(Source::Old, 12, 0);
  arbiter.onBeginSubgroup(Source::Old, 12, 1);
  arbiter.onDatagram(Source::Old, 13, false);
  arbiter.onSubgroupEnded(Source::Old, 11, 0);

  EXPECT_EQ(arbiter.oldUnfinishedGroups(), (std::set<uint64_t>{12, 13}));
}

// Old ending, or the caller tearing it down on a timer, stops it even with an
// unfinished group.
TEST_F(MoQUpstreamHandoffArbiterTest, OldEndingAfterTheSplitStopsItMidGroup) {
  oldOpens(12);
  replacementOpens(13);
  replacementObject(13);
  EXPECT_TRUE(arbiter_.oldBlocksReplacement());

  arbiter_.onOldEnded();

  EXPECT_FALSE(arbiter_.oldBlocksReplacement());
  EXPECT_EQ(oldOpens(12, 1), kRefuse);
  EXPECT_EQ(oldDatagram(12), kRefuse);
  EXPECT_EQ(replacementOpens(14), kAccept);
}

// Streams start out of order, so catching up doesn't prove old has opened
// every stream of its groups.
TEST_F(MoQUpstreamHandoffArbiterTest, LateStreamOfAnOldGroupIsStillOlds) {
  oldOpens(12, 0);
  replacementOpens(13);
  oldOpens(13);
  oldFinishes(12, 0);
  ASSERT_TRUE(arbiter_.oldCaughtUp());

  EXPECT_EQ(oldOpens(12, 1), kAccept);
  EXPECT_FALSE(arbiter_.oldCaughtUp());
  EXPECT_EQ(replacementOpens(12, 1), kRefuse);
  arbiter_.onSubgroupEnded(Source::Old, 12, 1);

  EXPECT_TRUE(arbiter_.oldCaughtUp());
}

TEST_F(MoQUpstreamHandoffArbiterTest, DataFromOldAfterItEndsIsRefused) {
  oldOpens(12);
  replacementOpens(13);
  oldOpens(13);
  oldFinishes(12);
  arbiter_.onOldEnded();

  EXPECT_EQ(oldOpens(12, 1), kRefuse);
  EXPECT_EQ(oldDatagram(12), kRefuse);
  EXPECT_EQ(replacementOpens(14), kAccept);
}

// Only replacement objects count: a stream header alone may be all a stalled
// replacement sends.
TEST_F(MoQUpstreamHandoffArbiterTest, OldBlocksTheReplacementWhileUnfinished) {
  oldOpens(11);
  oldOpens(12);
  EXPECT_EQ(replacementOpens(12), kRefuse);
  replacementObject(12);
  EXPECT_FALSE(arbiter_.oldBlocksReplacement());

  replacementOpens(13);
  EXPECT_FALSE(arbiter_.oldBlocksReplacement());
  replacementObject(13);
  EXPECT_TRUE(arbiter_.oldBlocksReplacement());
  oldFinishes(11);
  EXPECT_TRUE(arbiter_.oldBlocksReplacement());
  oldFinishes(12);

  EXPECT_FALSE(arbiter_.oldBlocksReplacement());
}

// The replacement that froze the split at 13 ends after the track opened
// group 14. Old keeps its groups, also 12, which it hadn't opened, and takes
// back those after 14. The next replacement gets only groups after 14 too.
TEST_F(
    MoQUpstreamHandoffArbiterTest,
    AReplacementEndingFirstHandsOldBackItsGroupsAfterTheTracks) {
  oldOpens(11);
  oldDatagram(11);
  replacementOpens(13);
  replacementOpens(14);

  auto renewed = arbiter_.afterReplacementEnded(
      {.highestGroup = 14,
       .openSubgroups = {{11, 0}},
       .openDatagramGroup = 11});

  EXPECT_FALSE(renewed.split().has_value());
  EXPECT_EQ(renewed.onDatagram(Source::Old, 11, /*lastInGroup=*/true), kAccept);
  EXPECT_EQ(renewed.onBeginSubgroup(Source::Old, 11, 1), kAccept);
  EXPECT_EQ(renewed.onBeginSubgroup(Source::Old, 12, 0), kAccept);
  EXPECT_EQ(renewed.onBeginSubgroup(Source::Old, 13, 1), kRefuse);
  EXPECT_EQ(renewed.onDatagram(Source::Old, 14, false), kRefuse);
  auto next = renewed;
  EXPECT_EQ(next.onBeginSubgroup(Source::Replacement, 14, 1), kRefuse);
  EXPECT_EQ(next.onBeginSubgroup(Source::Replacement, 15, 0), kAccept);
  EXPECT_EQ(renewed.onBeginSubgroup(Source::Old, 15, 0), kAccept);
  EXPECT_EQ(renewed.onBeginSubgroup(Source::Replacement, 16, 0), kAccept);
  EXPECT_EQ(renewed.split()->replacementFirstGroup, 16);
  EXPECT_EQ(renewed.split()->oldLastGroup, 15);
  // Refused for an ended replacement's group, old hasn't reached the next
  // replacement's.
  EXPECT_FALSE(renewed.oldCaughtUp());
}

// Each replacement that ended keeps its groups from old.
TEST_F(
    MoQUpstreamHandoffArbiterTest,
    EveryEndedReplacementKeepsItsGroupsFromOld) {
  oldOpens(11);
  replacementOpens(13);
  auto second = arbiter_.afterReplacementEnded(
      {.highestGroup = 13, .openSubgroups = {{11, 0}}});
  ASSERT_EQ(second.onBeginSubgroup(Source::Old, 14, 0), kAccept);
  ASSERT_EQ(second.onBeginSubgroup(Source::Replacement, 16, 0), kAccept);

  auto third = second.afterReplacementEnded(
      {.highestGroup = 16, .openSubgroups = {{11, 0}, {14, 0}}});

  EXPECT_EQ(third.onBeginSubgroup(Source::Old, 11, 1), kAccept);
  EXPECT_EQ(third.onBeginSubgroup(Source::Old, 13, 0), kRefuse);
  EXPECT_EQ(third.onBeginSubgroup(Source::Old, 15, 0), kAccept);
  EXPECT_EQ(third.onBeginSubgroup(Source::Old, 16, 0), kRefuse);
  EXPECT_EQ(third.onBeginSubgroup(Source::Old, 17, 0), kAccept);
}

}} // namespace moxygen

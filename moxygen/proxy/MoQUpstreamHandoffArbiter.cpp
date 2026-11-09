/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/proxy/MoQUpstreamHandoffArbiter.h"

#include <algorithm>

namespace moxygen {

MoQUpstreamHandoffArbiter::MoQUpstreamHandoffArbiter(OldProgress oldProgress)
    : highestOldGroup_(oldProgress.highestGroup),
      oldOpenSubgroups_(std::move(oldProgress.openSubgroups)) {
  if (oldProgress.openDatagramGroup) {
    oldDatagramGroup_ = DatagramGroup{*oldProgress.openDatagramGroup, true};
    raiseHighestOldGroup(oldDatagramGroup_->group);
  }
  for (const auto& [group, _] : oldOpenSubgroups_) {
    raiseHighestOldGroup(group);
  }
}

MoQUpstreamHandoffArbiter MoQUpstreamHandoffArbiter::afterReplacementEnded(
    OldProgress progress) const {
  const auto highestGroup = progress.highestGroup;
  MoQUpstreamHandoffArbiter renewed(std::move(progress));
  renewed.endedReplacements_ = endedReplacements_;
  if (split_ && highestGroup) {
    renewed.endedReplacements_.push_back(
        EndedReplacement{.split = *split_, .lastGroup = *highestGroup});
  }
  return renewed;
}

MoQUpstreamHandoffArbiter::Admit MoQUpstreamHandoffArbiter::onBeginSubgroup(
    Source source,
    uint64_t group,
    uint64_t subgroup,
    bool startsMidSubgroup) {
  if (source == Source::Replacement && startsMidSubgroup) {
    // The replacement goes on from the track's Largest, so in the group it
    // continues, viewers already have the subgroup's first objects.
    if (group == continuedGroup_) {
      return admit(source, group);
    }
    if (ownership_ && ownership_->ownedByReplacement(group)) {
      return Admit::RefuseWithGap;
    }
    raiseReplacementIncompleteGroup(group);
    return Admit::Refuse;
  }
  const auto admitted = admit(source, group);
  if (source == Source::Old && admitted == Admit::Accept) {
    oldOpenSubgroups_.emplace(group, subgroup);
  }
  return admitted;
}

MoQUpstreamHandoffArbiter::Admit MoQUpstreamHandoffArbiter::onDatagram(
    Source source,
    uint64_t group,
    bool lastInGroup) {
  const auto admitted = admit(source, group);
  if (source == Source::Old && !oldEnded_) {
    noteOldDatagram(group, lastInGroup, admitted == Admit::Accept);
  }
  if (source == Source::Replacement && admitted == Admit::Accept) {
    replacementDelivering_ = true;
  }
  return admitted;
}

void MoQUpstreamHandoffArbiter::onObject(Source source, uint64_t group) {
  if (source == Source::Replacement && ownership_ &&
      ownership_->ownedByReplacement(group)) {
    replacementDelivering_ = true;
  }
}

void MoQUpstreamHandoffArbiter::noteOldDatagram(
    uint64_t group,
    bool lastInGroup,
    bool accepted) {
  if (oldDatagramGroup_ && group < oldDatagramGroup_->group) {
    // Old has moved past it.
    return;
  }
  if (!oldDatagramGroup_ || group > oldDatagramGroup_->group) {
    oldDatagramGroup_ = DatagramGroup{group, accepted && !lastInGroup};
  } else if (lastInGroup) {
    oldDatagramGroup_->open = false;
  }
}

void MoQUpstreamHandoffArbiter::onSubgroupEnded(
    Source source,
    uint64_t group,
    uint64_t subgroup) {
  if (source == Source::Old) {
    oldOpenSubgroups_.erase({group, subgroup});
  }
}

void MoQUpstreamHandoffArbiter::onOldEnded(
    std::optional<uint64_t> continuedGroup) {
  oldEnded_ = true;
  continuedGroup_ = continuedGroup;
  if (ownership_) {
    ownership_ = splitAfterOldEnded();
  }
}

MoQUpstreamHandoffArbiter::Split MoQUpstreamHandoffArbiter::splitAfterOldEnded()
    const {
  // The replacement continues that group unless old opened a later one, and
  // still never takes a group whose replacement data was refused.
  if (continuedGroup_ &&
      highestOldGroup_.value_or(*continuedGroup_) <= *continuedGroup_) {
    return Split{*continuedGroup_, replacementIncompleteGroup_};
  }
  auto oldLastGroup = highestOldGroup_;
  if (replacementIncompleteGroup_) {
    oldLastGroup = std::max(
        oldLastGroup.value_or(*replacementIncompleteGroup_),
        *replacementIncompleteGroup_);
  }
  return Split{0, oldLastGroup};
}

bool MoQUpstreamHandoffArbiter::oldCaughtUp() const {
  return oldReachedReplacement_ && !oldHasOpenGroups();
}

std::set<uint64_t> MoQUpstreamHandoffArbiter::oldUnfinishedGroups() const {
  std::set<uint64_t> groups;
  for (const auto& [group, _] : oldOpenSubgroups_) {
    groups.insert(group);
  }
  if (oldDatagramGroup_ && oldDatagramGroup_->open) {
    groups.insert(oldDatagramGroup_->group);
  }
  return groups;
}

bool MoQUpstreamHandoffArbiter::oldBlocksReplacement() const {
  return replacementDelivering_ && !oldEnded_ && oldHasOpenGroups();
}

bool MoQUpstreamHandoffArbiter::oldHasOpenGroups() const {
  return !oldOpenSubgroups_.empty() ||
      (oldDatagramGroup_ && oldDatagramGroup_->open);
}

bool MoQUpstreamHandoffArbiter::endedReplacementOwned(uint64_t group) const {
  return std::any_of(
      endedReplacements_.begin(),
      endedReplacements_.end(),
      [group](const EndedReplacement& ended) { return ended.owned(group); });
}

void MoQUpstreamHandoffArbiter::raiseHighestOldGroup(uint64_t group) {
  highestOldGroup_ = std::max(highestOldGroup_.value_or(group), group);
}

void MoQUpstreamHandoffArbiter::raiseReplacementIncompleteGroup(
    uint64_t group) {
  replacementIncompleteGroup_ =
      std::max(replacementIncompleteGroup_.value_or(group), group);
}

MoQUpstreamHandoffArbiter::Admit MoQUpstreamHandoffArbiter::admit(
    Source source,
    uint64_t group) {
  if (source == Source::Old) {
    if (oldEnded_) {
      return Admit::Refuse;
    }
    if (ownership_ && ownership_->ownedByReplacement(group)) {
      oldReachedReplacement_ = true;
      return Admit::Refuse;
    }
    if (endedReplacementOwned(group)) {
      return Admit::Refuse;
    }
    raiseHighestOldGroup(group);
    return Admit::Accept;
  }
  if (!ownership_) {
    if (oldEnded_) {
      ownership_ = splitAfterOldEnded();
    } else if (
        replacementIncompleteGroup_ && group <= *replacementIncompleteGroup_) {
      // N's first objects must be on the replacement.
      return Admit::Refuse;
    } else {
      split_ = Split{group, highestOldGroup_};
      ownership_ = split_;
    }
  }
  if (ownership_->ownedByReplacement(group)) {
    return Admit::Accept;
  }
  raiseReplacementIncompleteGroup(group);
  return Admit::Refuse;
}

} // namespace moxygen

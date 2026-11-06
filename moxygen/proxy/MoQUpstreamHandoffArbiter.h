/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstdint>
#include <optional>
#include <set>
#include <utility>

namespace moxygen {

// Decides which upstream attempt owns each group while a track moves from an
// old upstream to its replacement, so every group reaches viewers whole from
// one attempt and none twice.
//
// Old forwards as before until the replacement delivers its first data. The
// replacement subscribes from the next group start, so whichever of its
// objects arrives first, that object's group, N, should be whole on the
// replacement. N freezes the split: the replacement owns groups at or after N
// that are also after the highest group the track had opened by then, H. Old
// owns, and finishes, every group before that. Data from the attempt that
// doesn't own its group is refused, before it reaches the cache or the
// forwarder. Once old has ended, the replacement owns every group after both
// the highest group old opened and the highest group whose replacement data
// was refused, which can no longer reach viewers whole from the replacement.
//
// Draft 18+ says on the wire when a stream starts mid-subgroup, so the
// replacement is missing that subgroup's first objects; the arbiter refuses
// such a stream. If no other replacement data for its group came first,
// neither that group nor an earlier one can become the replacement's. If its
// group is already the replacement's, that group reaches viewers without the
// subgroup, and the arbiter says so with RefuseWithGap. Draft 16 doesn't say,
// so nothing catches it there.
//
// Old has caught up once it tries to open a replacement group and has
// finished every subgroup and datagram group it opened before them. A datagram
// group is open until its last datagram, or until old sends datagrams for a
// later group. Streams and datagrams can arrive out of order, so data for one
// of old's groups may still arrive after that; it is accepted, and a late
// stream reopens its group. The arbiter has no timers: the caller tears old
// down once it has stayed caught up for a short reorder grace, or when it ends
// or a tail or old-stall timer fires, and reports that with onOldEnded().
// Everything from old is refused after that.
//
// The caller also keeps track aliases hop-local (the replacement's
// setTrackAlias is not passed on) and lets only a group's owner advance the
// track's Largest.
class MoQUpstreamHandoffArbiter {
 public:
  enum class Source { Old, Replacement };
  // RefuseWithGap: refused, and its group, already the replacement's, reaches
  // viewers without it.
  enum class Admit { Accept, Refuse, RefuseWithGap };

  // What old had open when the switch began.
  struct OldProgress {
    std::optional<uint64_t> highestGroup;
    std::set<std::pair<uint64_t, uint64_t>> openSubgroups;
    // The group old was sending datagrams for, if it hadn't finished.
    std::optional<uint64_t> openDatagramGroup;
  };

  // Kept as its two bounds, never as H + 1: group IDs can skip.
  struct Split {
    uint64_t replacementFirstGroup;
    std::optional<uint64_t> oldLastGroup;

    bool ownedByReplacement(uint64_t group) const {
      return group >= replacementFirstGroup &&
          (!oldLastGroup || group > *oldLastGroup);
    }
  };

  explicit MoQUpstreamHandoffArbiter(OldProgress oldProgress = {});

  // A received object stream is a subgroup of its own, so it arrives here
  // too. startsMidSubgroup is true only when the stream is known not to begin
  // with its subgroup's first object; draft 16 doesn't say.
  Admit onBeginSubgroup(
      Source source,
      uint64_t group,
      uint64_t subgroup,
      bool startsMidSubgroup = false);
  // An object arrived on an accepted subgroup stream.
  void onObject(Source source, uint64_t group);
  Admit onDatagram(Source source, uint64_t group, bool lastInGroup);
  // An accepted subgroup finished or was reset. Only old's matter, to tell
  // when it has caught up; the replacement's are ignored.
  void onSubgroupEnded(Source source, uint64_t group, uint64_t subgroup);
  void onOldEnded();

  // The split as the replacement's first data froze it while old was still
  // running. Old ending hands the replacement more groups but leaves this as
  // it was; a replacement whose first data comes after old ended has none.
  const std::optional<Split>& split() const {
    return split_;
  }
  // Old has reached the replacement's groups and finished every group it
  // opened before them. A late stream for one of those groups makes this false
  // again until it finishes.
  bool oldCaughtUp() const;
  // The replacement has delivered objects of its groups while old still has
  // unfinished groups that viewers may be waiting on.
  bool oldBlocksReplacement() const;

 private:
  struct DatagramGroup {
    uint64_t group;
    bool open;
  };

  Admit admit(Source source, uint64_t group);
  Split splitAfterOldEnded() const;
  void raiseHighestOldGroup(uint64_t group);
  void raiseReplacementIncompleteGroup(uint64_t group);
  void noteOldDatagram(uint64_t group, bool lastInGroup, bool accepted);
  bool oldHasOpenGroups() const;

  std::optional<uint64_t> highestOldGroup_;
  std::optional<Split> split_;
  // Which groups the replacement owns: the split, until old ends; then every
  // group after old's last (replacementFirstGroup 0).
  std::optional<Split> ownership_;
  std::set<std::pair<uint64_t, uint64_t>> oldOpenSubgroups_;
  // The latest group old sent datagrams for.
  std::optional<DatagramGroup> oldDatagramGroup_;
  // The highest group not yet the replacement's whose replacement data was
  // refused: a stream that started mid-subgroup, or data for a group old
  // owned.
  std::optional<uint64_t> replacementIncompleteGroup_;
  bool oldReachedReplacement_{false};
  bool oldEnded_{false};
  bool replacementDelivering_{false};
};

} // namespace moxygen

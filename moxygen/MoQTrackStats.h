/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>

#include <moxygen/util/RecentGroupWindow.h>

namespace moxygen {

// The data of one subscription, PUBLISH or FETCH, counted by MoQSession. Only
// code on the session's executor may read or update it.
struct MoQTrackStats {
  // Data objects and their payload bytes, as sent by the app or delivered to
  // it. Status objects do not count.
  uint64_t objects{0};
  uint64_t bytes{0};
  uint64_t groups{0};
  uint64_t subgroups{0};
  // Data datagrams. A status datagram counts in statusObjects.
  uint64_t datagrams{0};
  // Status objects, such as end of group.
  uint64_t statusObjects{0};
  // Subgroup and fetch streams that either side reset, and the last code.
  uint64_t resets{0};
  std::optional<uint64_t> lastResetCode;
  uint64_t openSubgroups{0};
  // Stream bytes when sending, headers included.
  uint64_t bytesQueued{0};
  // The payload within bytesQueued.
  uint64_t payloadBytesQueued{0};
  uint64_t bytesWritten{0};
  uint64_t bytesAcked{0};
  // Queued bytes discarded by a reset or a write error before being written.
  uint64_t bytesDropped{0};
  // Written bytes cancelled by a reset before the peer acked them.
  uint64_t bytesCancelled{0};
  // Datagram bytes when sending, headers included.
  uint64_t datagramBytes{0};
  // Data objects written when sending, datagrams included.
  uint64_t objectsWritten{0};
  // Acked data objects, and the time from queueing each one to its ack. These
  // count only if the track has a delivery callback or a delivery timeout, or
  // the session has an object-ack stats callback. Datagrams are never acked.
  uint64_t objectsAcked{0};
  uint64_t ackLatencyUsSum{0};
  uint64_t ackLatencyUsMax{0};
  std::optional<std::chrono::steady_clock::time_point> firstObject;
  RecentGroupWindow recentGroups;

  uint64_t bytesWaitingForCredit() const {
    return bytesQueued - bytesWritten - bytesDropped;
  }
  uint64_t bytesInFlight() const {
    return bytesWritten - bytesAcked - bytesCancelled;
  }

  // The session calls this once per stream for each group on the stream, and
  // once per datagram, so that objects do not age groups out.
  void onGroup(uint64_t group);
  void onObject(uint64_t payloadBytes);
  // A data object on a stream. streamGroup is the last group counted on it.
  void onStreamObject(
      std::optional<uint64_t>& streamGroup,
      uint64_t group,
      uint64_t payloadBytes);
  void onDatagram(uint64_t group, uint64_t payloadBytes);
  void onSubgroupOpened();
  void onSubgroupClosed();
  void onQueued(uint64_t wireBytes, uint64_t payloadBytes);
  void onWritten(uint64_t wireBytes, uint64_t dataObjects);
  void onAcked(uint64_t latencyUs);
  void onReset(uint64_t code);
};

// Implemented by the consumers and handles that MoQSession provides, but not
// by the app's own implementations of those interfaces.
class MoQTrackStatsSource {
 public:
  virtual ~MoQTrackStatsSource() = default;
  virtual std::shared_ptr<const MoQTrackStats> getTrackStats() const = 0;
};

// The session's counts for obj, or null when the session did not provide obj.
template <typename T>
std::shared_ptr<const MoQTrackStats> trackStatsOf(const T& obj) {
  auto* source = dynamic_cast<const MoQTrackStatsSource*>(&obj);
  return source ? source->getTrackStats() : nullptr;
}

} // namespace moxygen

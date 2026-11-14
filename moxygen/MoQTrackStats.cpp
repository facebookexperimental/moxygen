/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <moxygen/MoQTrackStats.h>

#include <algorithm>

#include <folly/logging/xlog.h>

namespace moxygen {

void MoQTrackStats::onGroup(uint64_t group) {
  if (recentGroups.admit(group)) {
    ++groups;
  }
}

void MoQTrackStats::onObject(uint64_t payloadBytes) {
  if (!firstObject) {
    firstObject = std::chrono::steady_clock::now();
  }
  ++objects;
  bytes += payloadBytes;
}

void MoQTrackStats::onStreamObject(
    std::optional<uint64_t>& streamGroup,
    uint64_t group,
    uint64_t payloadBytes) {
  if (streamGroup != group) {
    streamGroup = group;
    onGroup(group);
  }
  onObject(payloadBytes);
}

void MoQTrackStats::onDatagram(uint64_t group, uint64_t payloadBytes) {
  ++datagrams;
  onGroup(group);
  onObject(payloadBytes);
}

void MoQTrackStats::onSubgroupOpened() {
  ++subgroups;
  ++openSubgroups;
}

void MoQTrackStats::onSubgroupClosed() {
  XDCHECK_GT(openSubgroups, 0u);
  if (openSubgroups > 0) {
    --openSubgroups;
  }
}

void MoQTrackStats::onQueued(uint64_t wireBytes, uint64_t payloadBytes) {
  bytesQueued += wireBytes;
  payloadBytesQueued += payloadBytes;
}

void MoQTrackStats::onWritten(uint64_t wireBytes, uint64_t dataObjects) {
  bytesWritten += wireBytes;
  objectsWritten += dataObjects;
}

void MoQTrackStats::onAcked(uint64_t latencyUs) {
  ++objectsAcked;
  ackLatencyUsSum += latencyUs;
  ackLatencyUsMax = std::max(ackLatencyUsMax, latencyUs);
}

void MoQTrackStats::onReset(uint64_t code) {
  ++resets;
  lastResetCode = code;
}

} // namespace moxygen

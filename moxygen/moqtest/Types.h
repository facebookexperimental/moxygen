/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/Expected.h>
#include <cstdint>

namespace moxygen {

enum class ForwardingPreference : int {
  ONE_SUBGROUP_PER_GROUP = 0,
  ONE_SUBGROUP_PER_OBJECT = 1,
  TWO_SUBGROUPS_PER_GROUP = 2,
  DATAGRAM = 3
};

const uint64_t kDefaultObjectsPerGroup = 10;
const uint64_t kDefaultSizeOfObjectZero = 1024;
const uint64_t kDefaultSizeOfObjectGreaterThanZero = 100;
const uint64_t kDefaultObjectFrequency = 50;
const uint64_t kDefaultLastGroupInTrack = (1ULL << 62) - 1;
const uint64_t kDefaultStart = 0;
const uint64_t kDefaultIncrement = 1;
const uint64_t kDefaultPublisherDeliveryTimeout = 0;

struct MoQTestParameters {
  ForwardingPreference forwardingPreference =
      ForwardingPreference::ONE_SUBGROUP_PER_GROUP; // Tuple Field 1 Between 0 -
                                                    // 3
  uint64_t startGroup = kDefaultStart;              // Tuple Field 2
  uint64_t startObject = kDefaultStart;             // Tuple Field 3
  uint64_t lastGroupInTrack = kDefaultLastGroupInTrack; // Tuple Field 4
  uint64_t objectsPerGroup = kDefaultObjectsPerGroup;   // Tuple Field 6
  uint64_t sizeOfObjectZero = kDefaultSizeOfObjectZero; // Tuple Field 7
  uint64_t sizeOfObjectGreaterThanZero =
      kDefaultSizeOfObjectGreaterThanZero;            // Tuple Field 8
  uint64_t objectFrequency = kDefaultObjectFrequency; // Tuple Field 9
  uint64_t groupIncrement = kDefaultIncrement;        // Tuple Field 10
  uint64_t objectIncrement = kDefaultIncrement;       // Tuple Field 11
  bool sendEndOfGroupMarkers = false; // Tuple Field 12 0: DO NOT SEND, 1: SEND
  int testIntegerExtension = -1;  // Tuple Field 13 <= 0 -> True, <0 -> False
  int testVariableExtension = -1; // Tuple Field 14
  uint64_t publisherDeliveryTimeout =
      kDefaultPublisherDeliveryTimeout; // Tuple Field 15
  uint64_t deliveryTimeout =
      0; // Tuple Field 16 - Delivery timeout in milliseconds (0 = disabled)

  uint64_t lastObjectInTrack = this->objectsPerGroup +
      this->sendEndOfGroupMarkers; // Tuple Field 5 (Out of Order to Ensure
                                   // this->sendEndOfGroupMarkers is
                                   // initialized)
};

} // namespace moxygen

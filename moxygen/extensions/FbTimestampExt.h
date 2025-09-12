/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <chrono>
#include <cstdint>

#include <moxygen/MoQFramer.h>

namespace moxygen { namespace fbext {

// Vendor extension type IDs (even => intValue)
// These IDs are reserved for experimentation in moxygen and are not part of the
// spec. Values chosen from a high range to reduce collision risk.
constexpr uint64_t kExtFbTsClientSendUs = 0xFACE0000ULL; // t0
constexpr uint64_t kExtFbTsServerRecvUs = 0xFACE0002ULL; // t1
constexpr uint64_t kExtFbTsServerSendUs = 0xFACE0004ULL; // t2

inline uint64_t nowUsMono() {
  using namespace std::chrono;
  return duration_cast<microseconds>(steady_clock::now().time_since_epoch())
      .count();
}

inline bool hasIntExt(const Extensions& exts, uint64_t type) {
  for (const auto& e : exts) {
    if (e.type == type) {
      return true;
    }
  }
  return false;
}

inline void
appendIntExtIfMissing(Extensions& exts, uint64_t type, uint64_t value) {
  if (!hasIntExt(exts, type)) {
    exts.emplace_back(type, value);
  }
}

}} // namespace moxygen::fbext

/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstdint>

namespace moxygen {

// Called on the proxy's executor. An implementation shared by proxies on
// different executors must support concurrent callbacks.
class MoQProxyStatsCallback {
 public:
  virtual ~MoQProxyStatsCallback() = default;

  // Once for each track that acts on an upstream GOAWAY.
  virtual void onUpstreamGoaway() = 0;
  // Once per received GOAWAY on a session the proxy watches, before its tracks.
  virtual void onUpstreamSessionGoaway() = 0;
  // Once per received upstream request-stream GOAWAY, including late ones.
  virtual void onUpstreamRequestStreamGoaway() = 0;
  virtual void onUpstreamLost() = 0;

  virtual void onLostSwitchStarted() = 0;
  virtual void onLostSwitchCompleted(uint64_t durationMs) = 0;
  virtual void onLostSwitchFailed() = 0;
  virtual void onLostSwitchStopped() = 0;
  virtual void onGracefulSwitchStarted() = 0;
  virtual void onGracefulSwitchCompleted(uint64_t durationMs) = 0;
  virtual void onGracefulSwitchFailed() = 0;
  virtual void onGracefulSwitchStopped() = 0;

  virtual void onHandoffBehind() = 0;
  virtual void onHandoffEqual() = 0;
  virtual void onHandoffAhead() = 0;
  virtual void onLostCutoff(uint64_t unfinishedGroups) = 0;
  virtual void onTailCutoff(uint64_t unfinishedGroups) = 0;
  virtual void onStreamRefused() = 0;
  virtual void onDatagramRefused() = 0;
  virtual void onLateReplacementGap(uint64_t groups) = 0;
  virtual void onLostGap(uint64_t groups) = 0;
  virtual void onMidSubgroupGap() = 0;
  virtual void recordLostResumeTime(uint64_t durationMs) = 0;
};

} // namespace moxygen

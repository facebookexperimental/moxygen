/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/io/async/EventBase.h>
#include <quic/common/events/FollyQuicEventBase.h>
#include <moxygen/events/MoQExecutor.h>

namespace moxygen {

class MoQFollyExecutorImpl : public MoQExecutor,
                             public quic::FollyQuicEventBase {
 public:
  explicit MoQFollyExecutorImpl(folly::EventBase* evb)
      : quic::FollyQuicEventBase(evb) {}

  void add(folly::Func func) override;
};

} // namespace moxygen

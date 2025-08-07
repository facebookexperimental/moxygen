/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <quic/common/events/LibevQuicEventBase.h>
#include <moxygen/events/MoQExecutor.h>

#include <folly/IntrusiveList.h>

namespace moxygen {

class MoQLibevExecutorImpl : public MoQExecutor,
                             public quic::LibevQuicEventBase {
 public:
  explicit MoQLibevExecutorImpl(
      std::unique_ptr<quic::LibevQuicEventBase::EvLoopWeak> loop);

  // Implementation of folly::Executor::add
  void add(folly::Func func) override;
};

} // namespace moxygen

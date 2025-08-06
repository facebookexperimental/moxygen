// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

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

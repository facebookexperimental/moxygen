// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#include <moxygen/events/MoQLibevExecutorImpl.h>

namespace moxygen {

MoQLibevExecutorImpl::MoQLibevExecutorImpl(
    std::unique_ptr<quic::LibevQuicEventBase::EvLoopWeak> loop)
    : quic::LibevQuicEventBase(std::move(loop)) {}

void MoQLibevExecutorImpl::add(folly::Func func) {
  runInLoop(std::move(func).asStdFunction(), /*thisIteration=*/true);
}

} // namespace moxygen

// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#include <moxygen/events/MoQFollyExecutorImpl.h>

namespace moxygen {

void MoQFollyExecutorImpl::add(folly::Func func) {
  getBackingEventBase()->add(std::move(func));
}

} // namespace moxygen

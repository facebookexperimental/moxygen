// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#pragma once

#include <folly/Executor.h>

namespace moxygen {

class MoQExecutor : public folly::Executor {
 public:
  virtual ~MoQExecutor() = default;

  template <
      typename T,
      typename = std::enable_if_t<std::is_base_of_v<MoQExecutor, T>>>
  T* getTypedExecutor() {
    auto exec = dynamic_cast<T*>(this);
    if (exec) {
      return exec;
    } else {
      return nullptr;
    }
  }
};

} // namespace moxygen

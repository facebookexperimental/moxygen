/*
 *  Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 *  This source code is licensed under the MIT license found in the LICENSE
 *  file in the root directory of this source tree.
 *
 */

#pragma once

#include <memory>
#include "moxygen/mlog/MLogger.h"

namespace moxygen {

class MLoggerFactory {
 public:
  virtual ~MLoggerFactory() = default;

  virtual std::shared_ptr<MLogger> createMLogger() = 0;
};

} // namespace moxygen

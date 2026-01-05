/*
 *  Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 *  This source code is licensed under the MIT license found in the LICENSE
 *  file in the root directory of this source tree.
 *
 */

#pragma once

#include "moxygen/mlog/MLogger.h"

namespace moxygen {

/**
 * FileMLogger is a concrete MLogger implementation that outputs logs to a file.
 * It inherits from the abstract MLogger base class and implements outputLogs()
 * to write formatted JSON logs to the path specified via setPath().
 */
class FileMLogger : public MLogger {
 public:
  explicit FileMLogger(VantagePoint vantagePoint) : MLogger(vantagePoint) {}
  ~FileMLogger() override = default;

  /**
   * Outputs all accumulated logs to the file specified by path_.
   * Logs are formatted as pretty-printed JSON.
   */
  void outputLogs() override;
};

} // namespace moxygen

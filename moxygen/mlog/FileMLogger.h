/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/Executor.h>
#include <memory>
#include <optional>
#include <string>
#include "moxygen/mlog/MLogger.h"

namespace moxygen {

/**
 * FileMLogger is a concrete MLogger implementation that outputs logs to a file.
 * It inherits from the abstract MLogger base class and implements outputLogs()
 * to write formatted JSON logs to the path specified via setPath().
 *
 * If a write executor is set via setWriteExecutor(), outputLogs() will
 * pre-format logs on the calling thread and schedule the file write
 * asynchronously on that executor.
 *
 * If a directory is set via setDir(), setPath() will prepend it automatically.
 */
class FileMLogger : public MLogger {
 public:
  explicit FileMLogger(VantagePoint vantagePoint) : MLogger(vantagePoint) {}
  ~FileMLogger() override = default;

  /**
   * Set an executor for asynchronous file writes.
   */
  void setWriteExecutor(std::shared_ptr<folly::Executor> executor) {
    writeExecutor_ = std::move(executor);
  }

  /**
   * Set an output directory
   */
  void setDir(std::string dir) {
    dir_ = std::move(dir);
  }

  /**
   * Outputs all accumulated logs to the file specified by path_ or
   * auto-derived path (if dir is set).
   * Logs are formatted as pretty-printed JSON.
   */
  void outputLogs() override;

 private:
  /**
     * Derives the output file path. If dir is set, dcid with a non-empty hex
     * representation is required and the path is {dir}/{dcid_hex}.mlog.
     * Otherwise returns the explicit path_. Returns nullopt when logging should
     * be skipped.
   */
    std::optional<std::string> derivePath() const;

  std::shared_ptr<folly::Executor> writeExecutor_;
  std::optional<std::string> dir_;
};

} // namespace moxygen

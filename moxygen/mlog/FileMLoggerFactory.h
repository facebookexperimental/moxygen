/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/Executor.h>
#include <moxygen/mlog/FileMLogger.h>
#include <memory>
#include <optional>
#include <string>
#include "moxygen/mlog/MLoggerFactory.h"

namespace moxygen {

// Creates FileMLogger instances per session.
// Optionally forwards a write executor and/or output directory to each created
// logger.
class FileMLoggerFactory : public MLoggerFactory {
 public:
  explicit FileMLoggerFactory(VantagePoint vantagePoint = VantagePoint::SERVER)
      : vantagePoint_(vantagePoint) {}

  explicit FileMLoggerFactory(
      std::string path,
      VantagePoint vantagePoint = VantagePoint::SERVER)
      : vantagePoint_(vantagePoint), path_(std::move(path)) {}

  std::shared_ptr<MLogger> createMLogger() override {
    auto logger = std::make_shared<FileMLogger>(vantagePoint_);
    if (writeExecutor_) {
      logger->setWriteExecutor(writeExecutor_);
    }
    if (path_) {
      logger->setPath(*path_);
    }
    if (dir_) {
      logger->setDir(*dir_);
    }
    return logger;
  }

  void setWriteExecutor(std::shared_ptr<folly::Executor> executor) {
    writeExecutor_ = std::move(executor);
  }

  void setDir(std::string dir) {
    dir_ = std::move(dir);
  }

 private:
  VantagePoint vantagePoint_;
  std::shared_ptr<folly::Executor> writeExecutor_;
  std::optional<std::string> path_;
  std::optional<std::string> dir_;
};

} // namespace moxygen

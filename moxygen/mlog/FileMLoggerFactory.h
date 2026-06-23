/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/Executor.h>
#include <moxygen/mlog/FileMLogger.h>
#include <memory>
#include <string>
#include "moxygen/mlog/MLoggerFactory.h"

namespace moxygen {

// Common base for FileMLogger-producing factories. Holds the bits every
// variant shares (vantage point, optional async write executor) and defers
// actual logger creation to subclasses.
class FileMLoggerFactoryBase : public MLoggerFactory {
 public:
  explicit FileMLoggerFactoryBase(VantagePoint vantagePoint)
      : vantagePoint_(vantagePoint) {}

  std::shared_ptr<MLogger> createMLogger() final {
    auto logger = makeFileLogger();
    if (writeExecutor_) {
      logger->setWriteExecutor(writeExecutor_);
    }
    return logger;
  }

  void setWriteExecutor(std::shared_ptr<folly::Executor> executor) {
    writeExecutor_ = std::move(executor);
  }

 protected:
  VantagePoint vantagePoint() const {
    return vantagePoint_;
  }

  virtual std::shared_ptr<FileMLogger> makeFileLogger() = 0;

 private:
  VantagePoint vantagePoint_;
  std::shared_ptr<folly::Executor> writeExecutor_;
};

// Creates FileMLogger instances that all write to the same file `path`.
class FileMLoggerFactory : public FileMLoggerFactoryBase {
 public:
  explicit FileMLoggerFactory(
      std::string path,
      VantagePoint vantagePoint = VantagePoint::SERVER)
      : FileMLoggerFactoryBase(vantagePoint), path_(std::move(path)) {}

 protected:
  std::shared_ptr<FileMLogger> makeFileLogger() override {
    return std::make_shared<FileMLogger>(vantagePoint(), path_);
  }

 private:
  std::string path_;
};

// Creates FileMLogger instances that derive their filename from each
// session's dcid: {dir}/{dcid_hex}.mlog.
class DirMLoggerFactory : public FileMLoggerFactoryBase {
 public:
  explicit DirMLoggerFactory(
      std::string dir,
      VantagePoint vantagePoint = VantagePoint::SERVER)
      : FileMLoggerFactoryBase(vantagePoint), dir_(std::move(dir)) {}

 protected:
  std::shared_ptr<FileMLogger> makeFileLogger() override {
    return std::make_shared<FileMLogger>(
        vantagePoint(), FileMLogger::InDir, dir_);
  }

 private:
  std::string dir_;
};

} // namespace moxygen

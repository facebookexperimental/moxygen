/*
 *  Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 *  This source code is licensed under the MIT license found in the LICENSE
 *  file in the root directory of this source tree.
 *
 */

#pragma once

#include <moxygen/mlog/FileMLogger.h>
#include <string>
#include "moxygen/mlog/MLoggerFactory.h"

namespace moxygen {

// Creates an MLogger that writes to a file
class FileMLoggerFactory : public MLoggerFactory {
 public:
  FileMLoggerFactory(const std::string& path, VantagePoint vantagePoint)
      : path_(path), vantagePoint_(vantagePoint) {}

  std::shared_ptr<MLogger> createMLogger() override {
    auto logger = std::make_shared<FileMLogger>(vantagePoint_);
    logger->setPath(path_);
    return logger;
  }

 private:
  std::string path_;
  VantagePoint vantagePoint_;
};

} // namespace moxygen

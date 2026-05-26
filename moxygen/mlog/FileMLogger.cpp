/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/mlog/FileMLogger.h"
#include <folly/Random.h>
#include <folly/json/json.h>
#include <folly/logging/xlog.h>
#include <fstream>

namespace moxygen {

std::optional<std::string> FileMLogger::derivePath() const {
  if (dir_) {
    if (!dcid_) {
      XLOG(WARN) << "Skipping mlog output: dcid must be set when dir is configured";
      return std::nullopt;
    }
    const auto dcidHex = dcid_->hex();
    if (dcidHex.empty()) {
      XLOG(WARN)
          << "Skipping mlog output: dcid must have a non-empty hex representation when dir is configured";
      return std::nullopt;
    }
    return *dir_ + "/" + dcidHex + ".mlog";
  }
  return path_;
}

void FileMLogger::outputLogs() {
  auto path = derivePath();
  if (!path) {
    return;
  }

  // Pre-format logs on the calling thread (where formatLog() is safe to
  // call), then execute the write lambda either inline or via the executor.
  std::vector<std::string> serialized;
  serialized.reserve(logs_.size());
  for (const auto& log : logs_) {
    serialized.push_back(folly::toPrettyJson(formatLog(log)));
  }

  auto write = [p = std::move(*path), lines = std::move(serialized)]() {
    std::ofstream fileObj(p);
    for (const auto& line : lines) {
      fileObj << line << '\n';
    }
  };

  if (writeExecutor_) {
    writeExecutor_->add(std::move(write));
  } else {
    write();
  }
}

} // namespace moxygen

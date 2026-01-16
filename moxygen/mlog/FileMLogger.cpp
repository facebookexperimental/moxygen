/*
 *  Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 *  This source code is licensed under the MIT license found in the LICENSE
 *  file in the root directory of this source tree.
 *
 */

#include "moxygen/mlog/FileMLogger.h"
#include <folly/json/json.h>
#include <fstream>

namespace moxygen {

void FileMLogger::outputLogs() {
  std::ofstream fileObj(path_);
  for (const auto& log : logs_) {
    auto obj = formatLog(log);
    std::string jsonLog = folly::toPrettyJson(obj);
    fileObj << jsonLog << std::endl;
  }
  fileObj.close();
}

} // namespace moxygen

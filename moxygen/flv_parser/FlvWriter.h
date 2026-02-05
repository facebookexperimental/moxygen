/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/io/IOBuf.h>
#include <fstream> // std::ofstream
#include "moxygen/flv_parser/FlvCoder.h"
#include "moxygen/flv_parser/FlvCommon.h"

namespace moxygen::flv {

class FlvWriter : public FlvCoder {
 public:
  explicit FlvWriter(const std::string& filename)
      : f_(filename, std::ofstream::binary) {}

  ~FlvWriter() {
    f_.close();
  }

  bool writeTag(FlvTag tag);

 private:
  std::ofstream f_;
};

} // namespace moxygen::flv

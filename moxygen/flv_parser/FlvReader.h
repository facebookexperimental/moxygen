/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/io/IOBuf.h>
#include <fstream> // std::ifstream
#include "FlvCommon.h"

namespace moxygen::flv {

class FlvReader {
 public:
  explicit FlvReader(const std::string& filename)
      : f_(filename, std::ifstream::binary) {}

  ~FlvReader() {
    f_.close();
  }

  flv::FlvTag readNextTag();

 private:
  uint8_t read1Byte();
  uint32_t read3Bytes();
  uint32_t read4Bytes();
  std::unique_ptr<folly::IOBuf> readBytes(size_t n);

  std::ifstream f_;

  std::unique_ptr<folly::IOBuf> header_;
};

} // namespace moxygen::flv

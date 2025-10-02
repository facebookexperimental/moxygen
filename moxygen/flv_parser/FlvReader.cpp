/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/flv_parser/FlvReader.h"
#include <netinet/in.h>

namespace moxygen::flv {

std::unique_ptr<FlvStreamParser::MediaItem> FlvReader::getNextItem() {
  std::unique_ptr<MediaItem> ret;
  XLOG(DBG1) << __func__;

  while (!ret) {
    try {
      auto tag = readNextTag();
      ret = parser_.parse(tag);
    } catch (std::exception& ex) {
      XLOG(ERR) << "Error processing tag. Ex: " << folly::exceptionStr(ex);
      ret = nullptr;
      break;
    }
  }

  return ret;
}

FlvTag FlvReader::readNextTag() {
  if (!header_) {
    // Read header
    header_ = readBytes(9);
  }

  // Prev tag size
  readBytes(4);

  if (f_.peek() == EOF) {
    // Clean end of file
    return FlvReadCmd::FLV_EOF;
  }

  // Read 11 bytes for the FLV tag header
  // tag type (1 byte), data size (3 bytes), timestamp (3 bytes),
  // timestamp msb (1 byte) stream id (3 bytes)
  auto header = readBytes(11);
  auto cursor = folly::io::Cursor(header.get());
  auto tag = flvParseTagBase(cursor);
  auto data = readBytes(tag->size);
  return createTag(std::move(tag), std::move(data));
}

std::unique_ptr<folly::IOBuf> FlvReader::readBytes(size_t n) {
  std::unique_ptr<folly::IOBuf> ret;
  if (n > std::numeric_limits<int32_t>::max()) {
    throw std::runtime_error(
        fmt::format("Cannot read more than int64_t::max {}", n));
  }
  const int64_t bytesToRead = n;
  uint8_t tmp[bytesToRead];
  f_.read((char*)tmp, bytesToRead);
  if (f_.gcount() == bytesToRead && f_.rdstate() == std::ios::goodbit) {
    ret = folly::IOBuf::copyBuffer(tmp, bytesToRead);
  } else {
    throw std::runtime_error(fmt::format(
        "Failed to read {} bytes at offset {}",
        n,
        static_cast<int>(f_.tellg())));
  }
  return ret;
}

} // namespace moxygen::flv

/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/flv_parser/FlvReader.h"
#include <folly/io/IOBufQueue.h>
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
  if (n > std::numeric_limits<int32_t>::max()) {
    throw std::runtime_error(
        fmt::format("Cannot read more than int32_t::max {}", n));
  }

  folly::IOBufQueue queue;
  size_t remaining = n;
  constexpr size_t kChunkSize = 4096; // 4KB chunks

  while (remaining > 0) {
    size_t toRead = std::min(remaining, kChunkSize);
    auto buf = queue.preallocate(toRead, toRead);
    f_.read(reinterpret_cast<char*>(buf.first), toRead);

    if (f_.gcount() != static_cast<std::streamsize>(toRead) ||
        f_.rdstate() != std::ios::goodbit) {
      throw std::runtime_error(
          fmt::format(
              "Failed to read {} bytes at offset {} "
              "(got {} bytes, state: 0x{:x})",
              n,
              static_cast<int>(f_.tellg()),
              f_.gcount(),
              static_cast<unsigned int>(f_.rdstate())));
    }

    queue.postallocate(toRead);
    remaining -= toRead;
  }

  auto b = queue.move();
  // This funcion is expected to return a single IOBuff
  b->coalesce();
  return b;
}

} // namespace moxygen::flv

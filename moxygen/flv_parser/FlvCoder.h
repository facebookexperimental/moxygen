/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/io/IOBuf.h>
#include "moxygen/flv_parser/FlvCommon.h"

namespace moxygen::flv {

class FlvCoder {
 public:
  explicit FlvCoder() = default;
  ~FlvCoder() = default;

  std::unique_ptr<folly::IOBuf> writeTag(FlvTag tag);

 private:
  static constexpr size_t TAG_HEADER_SIZE = 11;
  static constexpr size_t VIDEO_TAG_HEADER_SIZE = 5;
  // Writes 2 but we need to return 1
  static constexpr size_t AUDIO_TAG_HEADER_SIZE = 1;

  std::unique_ptr<folly::IOBuf> writeTagHeader(const FlvTagBase& tagBase);
  std::unique_ptr<folly::IOBuf> writeVideoTagHeader(
      const FlvVideoTag& tagVideo);
  std::unique_ptr<folly::IOBuf> writeAudioTagHeader(
      const FlvAudioTag& tagAudio);
  std::unique_ptr<folly::IOBuf> writeIoBuf(std::unique_ptr<folly::IOBuf> buf);

  std::unique_ptr<folly::IOBuf> write3Bytes(uint32_t v);
  std::unique_ptr<folly::IOBuf> write4Bytes(uint32_t v);
  std::unique_ptr<folly::IOBuf> writeByte(std::byte b);

  const char flvHeader_[9] =
      {'F', 'L', 'V', 0x1, 0b00000101, 0x00, 0x00, 0x00, 0x09};

  bool headerWrote_{false};
};

} // namespace moxygen::flv

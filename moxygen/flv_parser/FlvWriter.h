// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#pragma once

#include <folly/io/IOBuf.h>
#include <fstream> // std::ofstream
#include "moxygen/flv_parser/FlvCommon.h"

namespace moxygen::flv {

class FlvWriter {
 public:
  explicit FlvWriter(const std::string& filename)
      : f_(filename, std::ofstream::binary) {}

  ~FlvWriter() {
    f_.close();
  }

  bool writeTag(FlvTag tag);

 private:
  size_t writeTagHeader(const FlvTagBase& tagBase);
  size_t writeVideoTagHeader(const FlvVideoTag& tagVideo);
  size_t writeAudioTagHeader(const FlvAudioTag& tagAudio);
  size_t writeIoBuf(std::unique_ptr<folly::IOBuf> buf);

  void write3Bytes(uint32_t v);
  void write4Bytes(uint32_t v);
  void writeByte(std::byte b);

  const char flvHeader_[9] =
      {'F', 'L', 'V', 0x1, 0b00000101, 0x00, 0x00, 0x00, 0x09};

  std::ofstream f_;
  bool headerWrote_{false};
  bool audioHeaderWritten_{false};
  bool videoHeaderWritten_{false};
};

} // namespace moxygen::flv

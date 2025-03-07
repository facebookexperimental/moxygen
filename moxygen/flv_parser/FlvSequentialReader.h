/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/Optional.h>
#include <folly/io/IOBuf.h>
#include <folly/logging/xlog.h>
#include <moxygen/flv_parser/FlvReader.h>
#include <moxygen/moq_mi/MediaItem.h>

namespace moxygen::flv {

class FlvSequentialReader {
 public:
  // FLV timebase
  const uint32_t kFlvTimeScale = 1000;

  explicit FlvSequentialReader(const std::string& file_path)
      : reader_(file_path),
        file_path_(file_path),
        videoFrameId_(0),
        audioFrameId_(0) {
    XLOG(INFO) << __func__;
  }
  ~FlvSequentialReader() {
    XLOG(INFO) << __func__;
  }

  using MediaType = moxygen::MediaType;
  using MediaItem = moxygen::MediaItem;

  std::unique_ptr<MediaItem> getNextItem();

 private:
  FlvReader reader_;
  std::string file_path_;

  std::unique_ptr<folly::IOBuf> avcDecoderRecord_;

  AscHeaderData ascHeader_;

  uint64_t videoFrameId_;
  uint64_t audioFrameId_;

  folly::Optional<uint64_t> lastVideoPts_;
  folly::Optional<uint64_t> lastAudioPts_;
};

} // namespace moxygen::flv

/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/Optional.h>
#include <folly/io/IOBuf.h>
#include <folly/logging/xlog.h>
#include <moxygen/flv_parser/FlvCommon.h>
#include <moxygen/moq_mi/MediaItem.h>

namespace moxygen::flv {

class FlvStreamParser {
 public:
  // FLV timebase
  const uint32_t kFlvTimeScale = 1000;

  FlvStreamParser() : videoFrameId_(0), audioFrameId_(0) {}

  using MediaType = moxygen::MediaType;
  using MediaItem = moxygen::MediaItem;

  std::unique_ptr<MediaItem> parse(std::unique_ptr<folly::IOBuf> data);
  std::unique_ptr<MediaItem> parse(FlvTag& tag);

 private:
  std::unique_ptr<folly::IOBuf> avcDecoderRecord_;
  AscHeaderData ascHeader_;

  uint64_t videoFrameId_;
  uint64_t audioFrameId_;

  folly::Optional<uint64_t> lastVideoPts_;
  folly::Optional<uint64_t> lastAudioPts_;
};

} // namespace moxygen::flv

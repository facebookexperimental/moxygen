/*
 *  Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 *  This source code is licensed under the MIT license found in the LICENSE
 *  file in the root directory of this source tree.
 *
 */

#pragma once

#include <folly/io/async/EventBase.h>
#include <moxygen/MoQFramer.h>
#include <moxygen/MoQSession.h>
#include <moxygen/Publisher.h>
#include <memory>
#include <string>

namespace moxygen {

// Image publisher that reuses an existing MoQ session
// (shares the same session as MoQAudioPublisher for the image0 track)
class MoQImagePublisher {
 public:
  MoQImagePublisher(
      std::shared_ptr<MoQSession> session,
      folly::Executor::KeepAlive<folly::EventBase> executor,
      FullTrackName fullImageTrackName);

  // Publish a single image as one MoQ object
  // mimeType: e.g., "image/jpeg", "image/png", "image/webp"
  // payload: image bytes
  void publishImage(const std::string& mimeType, Payload payload);

 private:
  std::shared_ptr<MoQSession> session_;
  folly::Executor::KeepAlive<folly::EventBase> executor_;
  FullTrackName fullImageTrackName_;
  std::shared_ptr<TrackConsumer> imageTrackPublisher_;
  uint64_t imageSeqId_{0};
  bool imagePublishReady_{false};
};

} // namespace moxygen

/*
 *  Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 *  This source code is licensed under the MIT license found in the LICENSE
 *  file in the root directory of this source tree.
 *
 */

#include "moxygen/samples/hack/MoQImagePublisher.h"
#include "moxygen/samples/facebook/ai_audio_server/AppImageExt.h"

#include <folly/logging/xlog.h>

// Android-specific logging that outputs to logcat
#ifdef __ANDROID__
#include <android/log.h> // @manual
#define ALOG_TAG "MoQImagePublisher"
#define ALOG_INFO(...) \
  __android_log_print(ANDROID_LOG_INFO, ALOG_TAG, __VA_ARGS__)
#define ALOG_ERR(...) \
  __android_log_print(ANDROID_LOG_ERROR, ALOG_TAG, __VA_ARGS__)
#else
// Non-Android fallback: use XLOG
#define ALOG_INFO(...) XLOG(INFO) << __VA_ARGS__
#define ALOG_ERR(...) XLOG(ERR) << __VA_ARGS__
#endif

namespace {
const uint8_t IMAGE_STREAM_PRIORITY = 120; /* Lower priority than audio */
} // namespace

namespace moxygen {

class LocalSubscriptionHandle : public SubscriptionHandle {
 public:
  explicit LocalSubscriptionHandle(RequestID rid, TrackAlias alias) {
    setSubscribeOk(
        SubscribeOk{
            rid,
            alias,
            std::chrono::milliseconds(0),
            GroupOrder::OldestFirst,
            std::nullopt});
  }
  void unsubscribe() override {}
  folly::coro::Task<RequestUpdateResult> requestUpdate(
      RequestUpdate reqUpdate) override {
    co_return folly::makeUnexpected(
        RequestError{
            reqUpdate.requestID,
            RequestErrorCode::NOT_SUPPORTED,
            "Request update not implemented"});
  }
};

MoQImagePublisher::MoQImagePublisher(
    std::shared_ptr<MoQSession> session,
    folly::Executor::KeepAlive<folly::EventBase> executor,
    FullTrackName fullImageTrackName)
    : session_(std::move(session)),
      executor_(std::move(executor)),
      fullImageTrackName_(std::move(fullImageTrackName)) {
  if (!session_) {
    XLOG(ERR) << "MoQImagePublisher: null session provided";
    return;
  }

  // Initialize publish on the image track
  // This must be called on the executor's event base
  executor_.get()->add([this]() {
    PublishRequest pub;
    pub.fullTrackName = fullImageTrackName_;
    pub.groupOrder = GroupOrder::OldestFirst;
    pub.forward = true; // Allow subgroup creation, aligned with audio

    auto handle =
        std::make_shared<LocalSubscriptionHandle>(RequestID(1), TrackAlias(1));
    auto res = session_->publish(std::move(pub), handle);
    if (!res) {
      XLOG(ERR) << "MoQImagePublisher: PUBLISH(image0) failed: code="
                << folly::to_underlying(res.error().errorCode)
                << " reason=" << res.error().reasonPhrase;
      return;
    }
    imageTrackPublisher_ = std::move(res->consumer);
    imagePublishReady_ = true;
    XLOG(INFO) << "MoQImagePublisher: image0 track publish ready";
  });
}

void MoQImagePublisher::publishImage(
    const std::string& mimeType,
    Payload payload) {
  if (!session_ || !executor_) {
    ALOG_ERR("MoQImagePublisher: null session or executor");
    return;
  }

  if (!payload) {
    ALOG_ERR("MoQImagePublisher: null payload");
    return;
  }

  ALOG_INFO(
      "MoQImagePublisher: publishing image, mime=%s size=%zu",
      mimeType.c_str(),
      payload->computeChainDataLength());

  // Capture by value to avoid lifetime issues
  auto session = session_;
  auto seqId = imageSeqId_++;

  executor_.get()->add(
      [this, mimeType, payload = std::move(payload), seqId]() mutable {
        if (!imagePublishReady_ || !imageTrackPublisher_) {
          ALOG_ERR("MoQImagePublisher: track not ready");
          return;
        }

        // Build object header with MIME type extension
        Extensions exts;
        moxygen::appext::appendImageMimeType(exts, mimeType);

        ObjectHeader objHeader = ObjectHeader{/*groupIn=*/seqId,
                                              /*subgroupIn=*/0,
                                              /*idIn=*/0,
                                              IMAGE_STREAM_PRIORITY,
                                              ObjectStatus::NORMAL,
                                              std::move(exts)};

        if (auto res = imageTrackPublisher_->objectStream(
                objHeader, std::move(payload));
            !res) {
          ALOG_ERR(
              "MoQImagePublisher: objectStream failed: %s",
              res.error().describe().c_str());
        }
      });
}

} // namespace moxygen

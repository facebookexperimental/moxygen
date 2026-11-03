/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/Function.h>
#include <folly/io/IOBuf.h>
#include <folly/logging/xlog.h>
#include <proxygen/lib/http/webtransport/WebTransport.h>
#include <cstdint>
#include <memory>

namespace moxygen {

// Counts a session's reliable writes that are not yet acked or cancelled, so
// that a draining session can close once everything it wrote has arrived.
// The transport holds raw pointers to this, so it stays alive while any write
// is pending.
class EgressAckTracker : public proxygen::WebTransport::ByteEventCallback,
                         public std::enable_shared_from_this<EgressAckTracker> {
 public:
  folly::Expected<
      proxygen::WebTransport::FCState,
      proxygen::WebTransport::ErrorCode>
  write(
      proxygen::WebTransport::StreamWriteHandle& handle,
      std::unique_ptr<folly::IOBuf> data,
      bool fin) {
    if (!data && !fin) {
      return handle.writeStreamData(nullptr, false, nullptr);
    }
    onWrite();
    auto res = handle.writeStreamData(std::move(data), fin, this);
    if (!res) {
      onWriteComplete();
    }
    return res;
  }

  // For writers that register their own byte event callback.
  void onWrite() {
    if (pending_++ == 0) {
      keepalive_ = shared_from_this();
    }
  }

  void onWriteComplete() {
    // A failed write can be both cancelled by the transport and completed by
    // the writer.
    if (pending_ == 0) {
      return;
    }
    if (--pending_ != 0) {
      return;
    }
    auto keepalive = std::move(keepalive_);
    if (onIdle_) {
      onIdle_();
    }
  }

  uint64_t pending() const {
    return pending_;
  }

  void setOnIdle(folly::Function<void()> onIdle) {
    onIdle_ = std::move(onIdle);
  }

  void onByteEvent(quic::StreamId /*id*/, uint64_t /*offset*/) noexcept
      override {
    onWriteComplete();
  }

  void onByteEventCanceled(quic::StreamId /*id*/, uint64_t /*offset*/) noexcept
      override {
    onWriteComplete();
  }

 private:
  uint64_t pending_{0};
  std::shared_ptr<EgressAckTracker> keepalive_;
  folly::Function<void()> onIdle_;
};

} // namespace moxygen

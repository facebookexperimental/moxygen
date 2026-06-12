/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/CancellationToken.h>
#include <folly/Function.h>
#include <folly/io/IOBufQueue.h>
#include <proxygen/lib/http/webtransport/WebTransport.h>
#include <moxygen/MoQTypes.h>
#include <moxygen/ReplyContext.h>
#include <cstdint>
#include <memory>
#include <optional>

namespace moxygen {

// Per-bidi-stream control shared by the read loop, the reply context, and
// the application handle. Tracks write-handle validity, owns a
// read-cancel source, and holds a fire-once peer-termination callback
// dispatched on FIN/RST/STOP_SENDING (and suppressible via
// disarmOnPeerTermination()).
class BidiStreamControl {
 public:
  // finIsCancellation: true => peer FIN also fires the peer-termination
  // callback (responder semantics, e.g. SUBSCRIBE_NAMESPACE). False => only
  // peer RST.
  explicit BidiStreamControl(
      proxygen::WebTransport::StreamWriteHandle* writeHandle,
      folly::CancellationToken sessionShutdownToken,
      bool finIsCancellation = true);

  ~BidiStreamControl() = default;
  BidiStreamControl(const BidiStreamControl&) = delete;
  BidiStreamControl& operator=(const BidiStreamControl&) = delete;
  BidiStreamControl(BidiStreamControl&&) = delete;
  BidiStreamControl& operator=(BidiStreamControl&&) = delete;

  // null after peer STOP_SENDING or local cancel(); callers must null-check.
  proxygen::WebTransport::StreamWriteHandle* writeHandle() const {
    return writeHandle_;
  }

  folly::CancellationToken getReadCancelToken() const {
    return readCancelSource_.getToken();
  }

  // Set once per stream (sender: before read loop; responder: on first
  // frame). Required before firePeerTermination() can dispatch.
  void setRequestID(RequestID id) {
    requestID_ = id;
  }

  // Fires at most once on peer-initiated close. Requires setRequestID().
  void setOnPeerTermination(folly::Function<void(RequestID)> fn) {
    onPeerTerminationFn_ = std::move(fn);
  }

  std::optional<RequestID> requestID() const {
    return requestID_;
  }

  // Suppress the peer-termination callback (terminal reply arrived; any further
  // peer close is informational).
  void disarmOnPeerTermination() {
    onPeerTerminationFn_ = nullptr;
  }

  // Invoked by the read loop on FIN/RST exit. Idempotent.
  void firePeerTermination();

  // Local cancel: RST our write half, cancel the read source (the read
  // loop's exit guard STOP_SENDINGs the read half), and clear the
  // peer-termination callback.
  void cancel(ResetStreamErrorCode code);

  // FIN the write half and release its bookkeeping. Does not touch the
  // peer-termination callback — the read half stays open for the response.
  void writeFin() {
    write(nullptr, /*fin=*/true);
  }

  // Write to the write half; if fin, also release write-side bookkeeping.
  // No-op if the write handle is already null.
  void write(std::unique_ptr<folly::IOBuf> data, bool fin);

  // Code the exit guard uses for STOP_SENDING (0 unless cancel() set it).
  uint32_t readCancelCode() const {
    return readCancelCode_;
  }

  bool finIsCancellation() const {
    return finIsCancellation_;
  }

 private:
  void onPeerStopSending();
  // Null the write handle and drop its cancel callback after we close it.
  void onLocalWriteClose();

  proxygen::WebTransport::StreamWriteHandle* writeHandle_{nullptr};
  folly::CancellationSource readCancelSource_;
  // Used to suppress firePeerTermination() during shutdown; cleanup() can't
  // always reach this control to clear it directly.
  folly::CancellationToken sessionShutdownToken_;
  uint32_t readCancelCode_{0};
  folly::Function<void(RequestID)> onPeerTerminationFn_;
  std::optional<RequestID> requestID_;
  std::optional<folly::CancellationCallback> writeCancelCb_;
  bool finIsCancellation_{true};
};

// ReplyContext that writes to the bidi reply stream wrapped by a
// BidiStreamControl. flush(fin=true) FINs the stream and releases the
// control's write half; cancel() RST+STOP_SENDINGs via the control.
class BidiStreamReplyContext : public ReplyContext {
 public:
  BidiStreamReplyContext(
      std::shared_ptr<BidiStreamControl> control,
      folly::CancellationToken token);

  folly::IOBufQueue& writeBuf() override {
    return writeBuf_;
  }
  void flush(bool fin = false) override;
  void cancel(ResetStreamErrorCode code) override;

 private:
  folly::IOBufQueue writeBuf_{folly::IOBufQueue::cacheChainLength()};
  std::shared_ptr<BidiStreamControl> control_;
};

} // namespace moxygen

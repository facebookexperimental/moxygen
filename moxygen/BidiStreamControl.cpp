/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/BidiStreamControl.h"

#include <folly/Utility.h>
#include <folly/logging/xlog.h>

namespace moxygen {

BidiStreamControl::BidiStreamControl(
    proxygen::WebTransport::StreamWriteHandle* writeHandle,
    folly::CancellationToken sessionShutdownToken,
    uint64_t negotiatedVersion,
    bool finIsCancellation,
    std::shared_ptr<EgressAckTracker> egressTracker)
    : writeHandle_(writeHandle),
      streamId_(writeHandle ? writeHandle->getID() : 0),
      sessionShutdownToken_(std::move(sessionShutdownToken)),
      negotiatedVersion_(negotiatedVersion),
      finIsCancellation_(finIsCancellation),
      egressTracker_(std::move(egressTracker)) {
  if (writeHandle_) {
    writeCancelCb_.emplace(
        writeHandle_->getCancelToken(), [this] { onPeerStopSending(); });
  }
}

void BidiStreamControl::onPeerStopSending() {
  // Cancel token also fires on local close; an exception means peer
  // STOP_SENDING, which is the only case we act on here.
  if (!writeHandle_ || !writeHandle_->exception()) {
    return;
  }
  auto* ex = writeHandle_->exception();
  uint32_t code = ex->error;
  XLOG(DBG1) << "STOP_SENDING received on bidi stream id="
             << writeHandle_->getID() << " code=" << code;
  writeHandle_->resetStream(code);
  writeHandle_ = nullptr;
  setPeerClose(PeerClose::StopSending);
  readCancelCode_ = toWireResetStreamErrorCode(
      ResetStreamErrorCode::CANCELLED, negotiatedVersion_);
  readCancelSource_.requestCancellation();
  // Skip during shutdown: cleanup() delivers the canonical error, and
  // post-OK controls (e.g. PUBLISH_NAMESPACE) live only in user handles
  // where cleanup can't reach them to disarm.
  if (!sessionShutdownToken_.isCancellationRequested()) {
    firePeerTermination(fromWireResetStreamErrorCode(code, negotiatedVersion_));
  }
}

void BidiStreamControl::firePeerTermination(
    std::optional<ResetStreamErrorCode> errorCode) {
  peerCancelSource_.requestCancellation();
  if (!onPeerTerminationFn_ || !requestID_) {
    return;
  }
  auto fn = std::move(onPeerTerminationFn_);
  fn(*requestID_, errorCode);
}

void BidiStreamControl::onLocalWriteClose() {
  // Drop the cancel cb so the dead handle's token can't fire into us.
  writeCancelCb_.reset();
  writeHandle_ = nullptr;
}

void BidiStreamControl::write(std::unique_ptr<folly::IOBuf> data, bool fin) {
  if (!writeHandle_) {
    return;
  }
  egressTracker_->write(*writeHandle_, std::move(data), fin);
  if (fin) {
    onLocalWriteClose();
  }
}

void BidiStreamControl::cancel(ResetStreamErrorCode code) {
  onPeerTerminationFn_ = nullptr;
  readCancelCode_ = toWireResetStreamErrorCode(code, negotiatedVersion_);
  if (auto* wh = writeHandle_) {
    // Before resetStream: its cancellation would re-enter onPeerStopSending.
    onLocalWriteClose();
    wh->resetStream(readCancelCode_);
  }
  readCancelSource_.requestCancellation();
}

BidiStreamReplyContext::BidiStreamReplyContext(
    std::shared_ptr<BidiStreamControl> control,
    folly::CancellationToken token)
    : ReplyContext(std::move(token)), control_(std::move(control)) {}

void BidiStreamReplyContext::flush(bool fin) {
  if (fin && control_) {
    control_->disarmOnPeerTermination();
  }
  if (!cancelled() && control_) {
    control_->write(writeBuf_.move(), fin);
  } else {
    writeBuf_.move();
  }
}

void BidiStreamReplyContext::cancel(ResetStreamErrorCode code) {
  if (control_) {
    control_->cancel(code);
  }
}

} // namespace moxygen

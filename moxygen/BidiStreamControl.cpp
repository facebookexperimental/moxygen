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
    bool finIsCancellation)
    : writeHandle_(writeHandle),
      sessionShutdownToken_(std::move(sessionShutdownToken)),
      negotiatedVersion_(negotiatedVersion),
      finIsCancellation_(finIsCancellation) {
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
  readCancelSource_.requestCancellation();
  // Skip during shutdown: cleanup() delivers the canonical error, and
  // post-OK controls (e.g. PUBLISH_NAMESPACE) live only in user handles
  // where cleanup can't reach them to disarm.
  if (!sessionShutdownToken_.isCancellationRequested()) {
    firePeerTermination();
  }
}

void BidiStreamControl::firePeerTermination() {
  if (!onPeerTerminationFn_ || !requestID_) {
    return;
  }
  auto fn = std::move(onPeerTerminationFn_);
  fn(*requestID_);
}

void BidiStreamControl::onLocalWriteClose() {
  // Drop the cancel cb so the dead handle's token can't fire into us.
  writeCancelCb_.reset();
  writeHandle_ = nullptr;
}

void BidiStreamControl::write(
    std::unique_ptr<folly::IOBuf> data,
    bool fin,
    proxygen::WebTransport::ByteEventCallback* deliveryCallback) {
  if (!writeHandle_) {
    if (deliveryCallback) {
      deliveryCallback->onByteEventCanceled(0, 0);
    }
    return;
  }
  auto streamID = writeHandle_->getID();
  auto writeResult =
      writeHandle_->writeStreamData(std::move(data), fin, deliveryCallback);
  if (!writeResult && deliveryCallback) {
    deliveryCallback->onByteEventCanceled(streamID, 0);
  }
  if (fin) {
    onLocalWriteClose();
  }
}

void BidiStreamControl::cancel(ResetStreamErrorCode code) {
  onPeerTerminationFn_ = nullptr;
  readCancelCode_ = toWireResetStreamErrorCode(code, negotiatedVersion_);
  if (writeHandle_) {
    writeHandle_->resetStream(readCancelCode_);
    onLocalWriteClose();
  }
  readCancelSource_.requestCancellation();
}

BidiStreamReplyContext::BidiStreamReplyContext(
    std::shared_ptr<BidiStreamControl> control,
    folly::CancellationToken token)
    : ReplyContext(std::move(token)), control_(std::move(control)) {}

void BidiStreamReplyContext::flush(
    bool fin,
    proxygen::WebTransport::ByteEventCallback* deliveryCallback) {
  if (fin && control_) {
    control_->disarmOnPeerTermination();
  }
  if (!cancelled() && control_) {
    control_->write(writeBuf_.move(), fin, deliveryCallback);
  } else {
    writeBuf_.move();
    if (deliveryCallback) {
      deliveryCallback->onByteEventCanceled(0, 0);
    }
  }
}

void BidiStreamReplyContext::cancel(ResetStreamErrorCode code) {
  if (control_) {
    control_->cancel(code);
  }
}

} // namespace moxygen

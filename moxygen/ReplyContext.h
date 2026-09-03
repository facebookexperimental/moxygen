/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/CancellationToken.h>
#include <folly/io/IOBufQueue.h>
#include <moxygen/MoQTypes.h>

namespace moxygen {

// Generic write destination for request responses.
// Abstracts control stream vs bidi stream writing.
class ReplyContext {
 public:
  explicit ReplyContext(folly::CancellationToken token)
      : cancelToken_(std::move(token)) {}
  virtual ~ReplyContext() = default;

  // Get the buffer to serialize frames into
  virtual folly::IOBufQueue& writeBuf() = 0;

  // Flush serialized data to the transport, optionally with FIN
  virtual void flush(bool fin = false) = 0;

  // FIN the bidi reply stream; no-op for control-stream contexts.
  void flushFinal() {
    flush(/*fin=*/true);
  }

  // STOP_SENDING + RESET the bidi reply stream; no-op for control-stream
  // contexts. Idempotent.
  virtual void cancel(ResetStreamErrorCode /*code*/) {}

  bool cancelled() const {
    return cancelToken_.isCancellationRequested();
  }

 protected:
  folly::CancellationToken cancelToken_;
};

} // namespace moxygen

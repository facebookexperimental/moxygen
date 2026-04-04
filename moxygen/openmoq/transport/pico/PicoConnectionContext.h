/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <moxygen/openmoq/transport/pico/PicoQuicWebTransport.h>
#include <picoquic.h>
#include <memory>

namespace moxygen {

class MoQSession;

// Per-connection callback context stored via picoquic_set_callback() once the
// connection is ready. The magic field lets callers distinguish this struct
// from their owner pointer during the pre-ready phase.
struct PicoConnectionContext {
  static constexpr uint32_t kMagic = 0xC0EC0001;
  uint32_t magic{kMagic};

  std::shared_ptr<PicoQuicWebTransport> webTransport;
  // Keepalive: holds the session alive until this ctx is deleted on close.
  std::shared_ptr<MoQSession> moqSession;
};

// Handles the post-connection section of a picoquic callback. Routes close
// events through webTransport (which notifies the session and clears state),
// then frees ctx. All other events are forwarded to webTransport directly.
inline int dispatchConnectionEvent(
    PicoConnectionContext* ctx,
    picoquic_cnx_t* cnx,
    uint64_t stream_id,
    uint8_t* bytes,
    size_t length,
    picoquic_call_back_event_t fin_or_event,
    void* v_stream_ctx) {
  if (!ctx->webTransport) {
    return -1;
  }
  int ret = ctx->webTransport->handlePicoEvent(
      cnx, stream_id, bytes, length, static_cast<int>(fin_or_event),
      v_stream_ctx);
  switch (fin_or_event) {
    case picoquic_callback_close:
    case picoquic_callback_application_close:
    case picoquic_callback_stateless_reset:
      ctx->magic = 0xDEADBEEF;
      delete ctx;
      return 0;
    default:
      return ret;
  }
}

} // namespace moxygen

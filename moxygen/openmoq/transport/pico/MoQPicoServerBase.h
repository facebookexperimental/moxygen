/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <memory>
#include <moxygen/MoQServerBase.h>
#include <string>

// Forward declaration — avoids exposing picoquic.h to consumers
typedef struct st_picoquic_quic_t picoquic_quic_t;
typedef struct st_picoquic_cnx_t picoquic_cnx_t;

namespace moxygen {

class MoQExecutor;

/**
 * MoQPicoServerBase - shared picoquic machinery for MoQ servers.
 *
 * Holds the picoquic_quic_t context plus the picoCallback, ConnectionContext,
 * and onNewConnection logic shared by MoQPicoQuicServer (thread-based) and
 * MoQPicoQuicEventBaseServer (EventBase-based).
 *
 * Subclasses must set executor_ before calling createQuicContext().
 */
class MoQPicoServerBase : public MoQServerBase {
 public:
  MoQPicoServerBase(std::string cert,
                    std::string key,
                    std::string endpoint,
                    std::string versions = "");
  ~MoQPicoServerBase() override;

 protected:
  /**
   * Creates and configures the picoquic_quic_t context:
   *   picoquic_create + ALPN selection callback + cookie mode + BBR.
   * Sets quic_. executor_ must already be set.
   * Returns true on success, false on failure (logs error).
   */
  bool createQuicContext();

  /**
   * Frees quic_ and sets it to nullptr.
   */
  void destroyQuicContext();

  std::string cert_;
  std::string key_;
  std::string versions_;
  picoquic_quic_t* quic_{nullptr};
  // Shared ownership so it can be passed directly to createSession().
  // MoQPicoQuicServer stores PicoQuicExecutor here.
  // MoQPicoQuicEventBaseServer stores MoQFollyExecutorImpl with a no-op
  // deleter (caller retains ownership).
  std::shared_ptr<MoQExecutor> executor_;

 private:
  // Called from picoCallback via MoQPicoServerBaseCallbacks (friend).
  // Takes void* to keep picoquic_cnx_t out of this header.
  void onNewConnectionImpl(void* cnx);

  friend struct MoQPicoServerBaseCallbacks;
};

} // namespace moxygen

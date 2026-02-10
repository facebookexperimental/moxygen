/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <moxygen/MoQSession.h>
#include <moxygen/mlog/MLoggerFactory.h>
#include <memory>
#include <string>

namespace moxygen {

/**
 * MoQServerBase - Base class for MoQ servers
 */
class MoQServerBase : public MoQSession::ServerSetupCallback {
 public:
  explicit MoQServerBase(std::string endpoint);

  ~MoQServerBase() override = default;

  MoQServerBase(const MoQServerBase&) = delete;
  MoQServerBase(MoQServerBase&&) = delete;
  MoQServerBase& operator=(const MoQServerBase&) = delete;
  MoQServerBase& operator=(MoQServerBase&&) = delete;

  /**
   * Callback for new MoQ sessions.
   * Applications should override this to handle new sessions.
   */
  virtual void onNewSession(std::shared_ptr<MoQSession> session) = 0;

  /**
   * Callback when a session is terminated.
   * Applications can override this to clean up session state.
   */
  virtual void terminateClientSession(std::shared_ptr<MoQSession> /*session*/) {
  }

  /**
   * Start the server on the specified address.
   * Must be implemented by derived classes.
   */
  virtual void start(const folly::SocketAddress& addr) = 0;

  /**
   * Stop the server.
   * Must be implemented by derived classes.
   */
  virtual void stop() = 0;

  /**
   * Set the logger factory for creating per-session loggers.
   */
  void setMLoggerFactory(std::shared_ptr<MLoggerFactory> factory);

  // ServerSetupCallback overrides
  folly::Try<ServerSetup> onClientSetup(
      ClientSetup clientSetup,
      const std::shared_ptr<MoQSession>& session) override;

  folly::Expected<folly::Unit, SessionCloseErrorCode> validateAuthority(
      const ClientSetup& clientSetup,
      uint64_t negotiatedVersion,
      std::shared_ptr<MoQSession> session) override;

 protected:
  virtual std::shared_ptr<MoQSession> createSession(
      folly::MaybeManagedPtr<proxygen::WebTransport> wt,
      std::shared_ptr<MoQExecutor> executor);

  // Coroutine to handle client session lifecycle
  folly::coro::Task<void> handleClientSession(
      std::shared_ptr<MoQSession> clientSession);

  // Create a logger from the factory if one is set
  std::shared_ptr<MLogger> createLogger() const;

  [[nodiscard]] const std::string& getEndpoint() const {
    return endpoint_;
  }

  // AUTHORITY parameter validation methods
  bool isValidAuthorityFormat(const std::string& authority);
  bool isSupportedAuthority(const std::string& authority);

  std::string endpoint_;
  std::shared_ptr<MLoggerFactory> mLoggerFactory_;
};

} // namespace moxygen

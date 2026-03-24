/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <moxygen/MoQSession.h>
#include <moxygen/mlog/MLoggerFactory.h>
#include <memory>
#include <string>
#include <unordered_set>

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
   * Get the address the server is listening on.
   * Derived classes that support address binding should override this.
   */
  [[nodiscard]] virtual folly::SocketAddress getAddress() const {
    return folly::SocketAddress{};
  }

  /**
   * Set the logger factory for creating per-session loggers.
   */
  void setMLoggerFactory(std::shared_ptr<MLoggerFactory> factory);

  // ServerSetupCallback overrides
  folly::Try<Setup> onClientSetup(
      Setup clientSetup,
      const std::shared_ptr<MoQSession>& session) override;

  folly::Expected<folly::Unit, SessionCloseErrorCode> validateAuthority(
      const Setup& clientSetup,
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

  void addEndpoint(std::string endpoint) {
    endpoints_.insert(std::move(endpoint));
  }
  bool isAcceptedEndpoint(folly::StringPiece path) const {
    return endpoints_.count(std::string(path)) > 0;
  }

  // AUTHORITY parameter validation methods
  bool isValidAuthorityFormat(const std::string& authority);
  bool isSupportedAuthority(const std::string& authority);

  std::unordered_set<std::string> endpoints_;
  std::shared_ptr<MLoggerFactory> mLoggerFactory_;
};

} // namespace moxygen

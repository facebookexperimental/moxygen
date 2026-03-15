/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <folly/coro/Baton.h>
#include <folly/logging/xlog.h>
#include <moxygen/MoQServerBase.h>

namespace moxygen {

MoQServerBase::MoQServerBase(std::string endpoint) {
  endpoints_.insert(std::move(endpoint));
}

void MoQServerBase::setMLoggerFactory(std::shared_ptr<MLoggerFactory> factory) {
  mLoggerFactory_ = std::move(factory);
}

std::shared_ptr<MLogger> MoQServerBase::createLogger() const {
  if (mLoggerFactory_) {
    return mLoggerFactory_->createMLogger();
  }
  return nullptr;
}

std::shared_ptr<MoQSession> MoQServerBase::createSession(
    folly::MaybeManagedPtr<proxygen::WebTransport> wt,
    std::shared_ptr<MoQExecutor> executor) {
  return std::make_shared<MoQSession>(
      std::move(wt), *this, std::move(executor));
}

folly::coro::Task<void> MoQServerBase::handleClientSession(
    std::shared_ptr<MoQSession> clientSession) {
  onNewSession(clientSession);
  clientSession->start();

  // The clientSession will cancel this token when the app calls close() or
  // the underlying transport invokes onSessionEnd
  folly::coro::Baton baton;
  folly::CancellationCallback cb(
      clientSession->getCancelToken(), [&baton] { baton.post(); });
  co_await baton;
  terminateClientSession(std::move(clientSession));
}

folly::Try<Setup> MoQServerBase::onClientSetup(
    Setup setup,
    const std::shared_ptr<MoQSession>& session) {
  XLOG(DBG1) << "MoQServerBase::ClientSetup";

  // Version is either negotiated via ALPN or defaults to draft-14
  auto sessionVersion = session->getNegotiatedVersion();
  if (sessionVersion) {
    XLOG(DBG1)
        << "MoQServerBase::ClientSetup: Using ALPN-negotiated version: moqt-"
        << getDraftMajorVersion(*sessionVersion);
  } else {
    XLOG(DBG1) << "MoQServerBase::ClientSetup: No ALPN, using draft-14";
  }

  // TODO: Make the default MAX_REQUEST_ID configurable and
  // take in the value from ClientSetup
  static constexpr size_t kDefaultMaxRequestID = 100;
  static constexpr size_t kMaxAuthTokenCacheSize = 1024;
  Setup serverSetup;
  serverSetup.params.insertParam(
      Parameter{
          folly::to_underlying(SetupKey::MAX_REQUEST_ID),
          kDefaultMaxRequestID});
  serverSetup.params.insertParam(
      Parameter{
          folly::to_underlying(SetupKey::MAX_AUTH_TOKEN_CACHE_SIZE),
          kMaxAuthTokenCacheSize});

  // Log Server Setup
  if (auto logger = session->getLogger()) {
    logger->logServerSetup(
        serverSetup, sessionVersion.value_or(kVersionDraft14));
  }

  return folly::Try<Setup>(serverSetup);
}

folly::Expected<folly::Unit, SessionCloseErrorCode>
MoQServerBase::validateAuthority(
    const Setup& setup,
    uint64_t negotiatedVersion,
    std::shared_ptr<MoQSession>) {
  if (getDraftMajorVersion(negotiatedVersion) >= 14) {
    // Find and validate AUTHORITY
    auto authorityParam = std::find_if(
        setup.params.begin(), setup.params.end(), [](const SetupParameter& p) {
          return p.key == folly::to_underlying(SetupKey::AUTHORITY);
        });

    bool hasAuthority = (authorityParam != setup.params.end());

    if (hasAuthority) {
      const std::string& authority = authorityParam->asString;

      // MALFORMED_AUTHORITY validation
      if (!isValidAuthorityFormat(authority)) {
        XLOG(ERR) << "Malformed authority format: " << authority;
        return folly::makeUnexpected(
            SessionCloseErrorCode::MALFORMED_AUTHORITY);
      }

      // INVALID_AUTHORITY validation
      if (!isSupportedAuthority(authority)) {
        XLOG(ERR) << "Unsupported authority: " << authority;
        return folly::makeUnexpected(SessionCloseErrorCode::INVALID_AUTHORITY);
      }
    }
  }

  return folly::unit;
}

bool MoQServerBase::isValidAuthorityFormat(const std::string& authority) {
  // TODO: Implement RFC 3986 authority format validation
  return !authority.empty();
}

bool MoQServerBase::isSupportedAuthority(const std::string& /*authority*/) {
  // TODO: Implement server-side authority support validation
  return true;
}

} // namespace moxygen

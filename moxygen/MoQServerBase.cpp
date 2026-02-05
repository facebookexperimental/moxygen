/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <folly/coro/Baton.h>
#include <folly/logging/xlog.h>
#include <moxygen/MoQServerBase.h>

namespace moxygen {

MoQServerBase::MoQServerBase(std::string endpoint)
    : endpoint_(std::move(endpoint)) {}

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
    MoQExecutor::KeepAlive executor) {
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

folly::Try<ServerSetup> MoQServerBase::onClientSetup(
    ClientSetup setup,
    const std::shared_ptr<MoQSession>& session) {
  XLOG(DBG1) << "MoQServerBase::ClientSetup";

  uint64_t negotiatedVersion = 0;

  // Check if version was negotiated via ALPN first (takes precedence)
  auto sessionVersion = session->getNegotiatedVersion();
  if (sessionVersion) {
    // ALPN mode: use the ALPN-negotiated version
    negotiatedVersion = *sessionVersion;
    XLOG(DBG1)
        << "MoQServerBase::ClientSetup: Using ALPN-negotiated version: moqt-"
        << getDraftMajorVersion(negotiatedVersion);
  } else if (!setup.supportedVersions.empty()) {
    // Legacy mode: negotiate from version array in CLIENT_SETUP
    // Iterate over supported versions and set the highest version within the
    // range
    uint64_t highestVersion = 0;
    for (const auto& version : setup.supportedVersions) {
      if (getDraftMajorVersion(version) >= 15) {
        XLOG(WARN) << "MoQServerBase::ClientSetup: Skiping version " << version
                   << " (which needs alpn negotiation), to attempt fallback.";
        continue;
      }
      if (isSupportedVersion(version)) {
        highestVersion = std::max(highestVersion, version);
      }
    }
    if (highestVersion == 0) {
      std::string errorMessage = folly::to<std::string>(
          "The only supported versions in client_setup are ",
          getSupportedVersionsString());
      return folly::Try<ServerSetup>(std::runtime_error(errorMessage));
    }
    negotiatedVersion = highestVersion;
  } else {
    // No version available from either ALPN or CLIENT_SETUP
    return folly::Try<ServerSetup>(
        std::runtime_error("No version negotiated via ALPN or CLIENT_SETUP"));
  }

  // TODO: Make the default MAX_REQUEST_ID configurable and
  // take in the value from ClientSetup
  static constexpr size_t kDefaultMaxRequestID = 100;
  static constexpr size_t kMaxAuthTokenCacheSize = 1024;
  ServerSetup serverSetup{.selectedVersion = negotiatedVersion};
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
    logger->logServerSetup(serverSetup);
  }

  return folly::Try<ServerSetup>(serverSetup);
}

folly::Expected<folly::Unit, SessionCloseErrorCode>
MoQServerBase::validateAuthority(
    const ClientSetup& setup,
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

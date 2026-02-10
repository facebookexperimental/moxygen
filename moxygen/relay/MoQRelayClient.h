/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/coro/Sleep.h>
#include <folly/logging/xlog.h>
#include <quic/state/TransportSettings.h>
#include <moxygen/MoQClient.h>
#include <moxygen/MoQRelaySession.h>

namespace moxygen {

class MoQRelayClient {
 public:
  explicit MoQRelayClient(std::unique_ptr<MoQClient> moqClient)
      : moqClient_(std::move(moqClient)) {}

  // Convenience constructor for QUIC transport with relay session
  MoQRelayClient(
      std::shared_ptr<MoQExecutor> exec,
      proxygen::URL url,
      std::shared_ptr<fizz::CertificateVerifier> verifier = nullptr)
      : moqClient_(
            std::make_unique<MoQClient>(
                std::move(exec),
                std::move(url),
                MoQRelaySession::createRelaySessionFactory(),
                std::move(verifier))) {}

  void setLogger(std::shared_ptr<MLogger> logger) {
    if (moqClient_ && logger) {
      moqClient_->setLogger(logger);
    }
  }

  folly::coro::Task<void> setup(
      std::shared_ptr<Publisher> publisher,
      std::shared_ptr<Subscriber> subscriber,
      std::chrono::milliseconds connectTimeout = std::chrono::seconds(5),
      std::chrono::milliseconds transactionTimeout = std::chrono::seconds(60),
      const quic::TransportSettings& transportSettings =
          quic::TransportSettings(),
      const std::vector<std::string>& alpns = {}) {
    co_await moqClient_->setupMoQSession(
        connectTimeout,
        transactionTimeout,
        std::move(publisher),
        std::move(subscriber),
        transportSettings,
        alpns);
  }

  folly::coro::Task<void> run(
      std::shared_ptr<Publisher> publisher,
      std::vector<TrackNamespace> namespaces) {
    try {
      bool isPublisher = bool(publisher);
      // could parallelize
      if (!moqClient_->moqSession_) {
        XLOG(ERR) << "Session is dead now #sad";
        co_return;
      }
      for (auto& ns : namespaces) {
        PublishNamespace ann;
        ann.trackNamespace = std::move(ns);
        auto res =
            co_await moqClient_->moqSession_->publishNamespace(std::move(ann));
        if (!res) {
          XLOG(ERR) << "PublishNamespaceError reqID="
                    << res.error().requestID.value
                    << " code=" << folly::to_underlying(res.error().errorCode)
                    << " reason=" << res.error().reasonPhrase;
        } else {
          publishNamespaceHandles_.emplace_back(std::move(res.value()));
        }
      }
      if (isPublisher) {
        while (moqClient_->moqSession_) {
          co_await folly::coro::sleep(std::chrono::seconds(30));
          if (!moqClient_->moqSession_) {
            break;
          }
          PublishNamespace ann;
          ann.trackNamespace.trackNamespace.emplace_back("ping");
          auto handle = co_await moqClient_->moqSession_->publishNamespace(ann);
          if (handle.hasError()) {
            break;
          }
          handle.value()->publishNamespaceDone();
        }
      }
    } catch (const std::exception& ex) {
      XLOG(ERR) << folly::exceptionStr(ex);
      co_return;
    }
  }

  std::shared_ptr<MoQSession> getSession() const {
    return moqClient_->moqSession_;
  }

  MoQExecutor* getEventBase() const {
    return moqClient_->getEventBase().get();
  }

  void shutdown() {
    for (auto& handle : publishNamespaceHandles_) {
      if (handle) {
        handle->publishNamespaceDone();
      }
    }
    publishNamespaceHandles_.clear();
    // Flush mLogs before closing the session, since closeSession() does not
    // trigger onSessionEnd callback (where logs would normally be flushed)
    if (moqClient_ && moqClient_->logger_) {
      moqClient_->logger_->outputLogs();
    }
    if (moqClient_ && moqClient_->moqSession_) {
      // Break strong references held by the session to the Publisher/Subscriber
      moqClient_->moqSession_->setPublishHandler(nullptr);
      moqClient_->moqSession_->setSubscribeHandler(nullptr);
      // Close the session cleanly after relay run has stopped
      moqClient_->moqSession_->close(SessionCloseErrorCode::NO_ERROR);
      moqClient_->moqSession_.reset();
    }
  }

 private:
  std::unique_ptr<MoQClient> moqClient_;
  std::vector<std::shared_ptr<Subscriber::PublishNamespaceHandle>>
      publishNamespaceHandles_;
};

} // namespace moxygen

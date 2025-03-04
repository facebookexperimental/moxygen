/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/logging/xlog.h>
#include "moxygen/MoQClient.h"

namespace moxygen {

class MoQRelayClient {
 public:
  MoQRelayClient(
      folly::EventBase* evb,
      proxygen::URL url,
      MoQClient::TransportType ttype =
          MoQClient::TransportType::H3_WEBTRANSPORT)
      : moqClient_(evb, std::move(url), ttype) {}

  folly::coro::Task<void> run(
      std::shared_ptr<Publisher> publisher,
      std::shared_ptr<Subscriber> subscriber,
      std::vector<TrackNamespace> namespaces,
      std::chrono::milliseconds connectTimeout = std::chrono::seconds(5),
      std::chrono::milliseconds transactionTimeout = std::chrono::seconds(60)) {
    try {
      bool isPublisher = bool(publisher);
      co_await moqClient_.setupMoQSession(
          connectTimeout,
          transactionTimeout,
          std::move(publisher),
          std::move(subscriber));
      // could parallelize
      if (!moqClient_.moqSession_) {
        XLOG(ERR) << "Session is dead now #sad";
        co_return;
      }
      for (auto& ns : namespaces) {
        Announce ann;
        ann.trackNamespace = std::move(ns);
        auto res = co_await moqClient_.moqSession_->announce(std::move(ann));
        if (!res) {
          XLOG(ERR) << "AnnounceError namespace=" << res.error().trackNamespace
                    << " code=" << folly::to_underlying(res.error().errorCode)
                    << " reason=" << res.error().reasonPhrase;
        } else {
          announceHandles_.emplace_back(std::move(res.value()));
        }
      }
      if (isPublisher) {
        while (moqClient_.moqSession_) {
          co_await folly::coro::sleep(std::chrono::seconds(30));
          if (!moqClient_.moqSession_) {
            break;
          }
          Announce ann;
          ann.trackNamespace.trackNamespace.push_back("ping");
          auto handle = co_await moqClient_.moqSession_->announce(ann);
          if (handle.hasError()) {
            break;
          }
          handle.value()->unannounce();
        }
      }
    } catch (const std::exception& ex) {
      XLOG(ERR) << folly::exceptionStr(ex);
      co_return;
    }
  }

  std::shared_ptr<MoQSession> getSession() const {
    return moqClient_.moqSession_;
  }

  void shutdown() {
    for (auto& handle : announceHandles_) {
      handle->unannounce();
    }
    announceHandles_.clear();
  }

 private:
  MoQClient moqClient_;
  std::vector<std::shared_ptr<Subscriber::AnnounceHandle>> announceHandles_;
};

} // namespace moxygen

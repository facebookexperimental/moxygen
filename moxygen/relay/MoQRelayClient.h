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
  MoQRelayClient(folly::EventBase* evb, proxygen::URL url)
      : moqClient_(evb, std::move(url)) {}

  folly::coro::Task<void> run(
      std::shared_ptr<Publisher> publisher,
      std::shared_ptr<Subscriber> subscriber,
      std::vector<TrackNamespace> namespaces,
      std::chrono::milliseconds connectTimeout = std::chrono::seconds(5),
      std::chrono::milliseconds transactionTimeout = std::chrono::seconds(60)) {
    try {
      co_await moqClient_.setupMoQSession(
          connectTimeout,
          transactionTimeout,
          std::move(publisher),
          std::move(subscriber));
      auto exec = co_await folly::coro::co_current_executor;
      // could parallelize
      if (!moqClient_.moqSession_) {
        XLOG(ERR) << "Session is dead now #sad";
        co_return;
      }
      for (auto& ns : namespaces) {
        auto res =
            co_await moqClient_.moqSession_->announce({std::move(ns), {}});
        if (!res) {
          XLOG(ERR) << "AnnounceError namespace=" << res.error().trackNamespace
                    << " code=" << res.error().errorCode
                    << " reason=" << res.error().reasonPhrase;
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

 private:
  MoQClient moqClient_;
};

} // namespace moxygen

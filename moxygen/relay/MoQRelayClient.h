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
      std::function<std::unique_ptr<MoQSession::ControlVisitor>(
          std::shared_ptr<MoQSession>)> controllerFn)
      : moqClient_(evb, url), controllerFn_(controllerFn) {}

  folly::coro::Task<void> run(
      Role role,
      std::vector<TrackNamespace> namespaces,
      std::chrono::milliseconds connectTimeout = std::chrono::seconds(5),
      std::chrono::milliseconds transactionTimeout = std::chrono::seconds(60)) {
    try {
      co_await moqClient_.setupMoQSession(
          connectTimeout, transactionTimeout, role);
      auto exec = co_await folly::coro::co_current_executor;
      auto controller = controllerFn_(moqClient_.moqSession_);
      if (!controller) {
        XLOG(ERR) << "Failed to make controller";
        co_return;
      }
      controlReadLoop(std::move(controller)).scheduleOn(exec).start();
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

 private:
  folly::coro::Task<void> controlReadLoop(
      std::unique_ptr<MoQSession::ControlVisitor> controller) {
    while (moqClient_.moqSession_) {
      auto msg = co_await moqClient_.moqSession_->controlMessages().next();
      if (!msg) {
        break;
      }
      boost::apply_visitor(*controller, msg.value());
    }
  }
  MoQClient moqClient_;
  std::function<std::unique_ptr<MoQSession::ControlVisitor>(
      std::shared_ptr<MoQSession>)>
      controllerFn_;
};

} // namespace moxygen

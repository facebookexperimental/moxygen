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
      std::vector<std::string> namespaces,
      std::chrono::milliseconds connectTimeout = std::chrono::seconds(5),
      std::chrono::milliseconds transactionTimeout = std::chrono::seconds(60)) {
    try {
      co_await moqClient_.setupMoQSession(connectTimeout, transactionTimeout);
      auto exec = co_await folly::coro::co_current_executor;
      auto controller = controllerFn_(moqClient_.moqSession_);
      if (!controller) {
        XLOG(ERR) << "Failed to make controller";
        co_return;
      }
      controlReadLoop(std::move(controller)).scheduleOn(exec).start();
      // could parallelize
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
      XLOG(ERR) << ex.what();
      co_return;
    }
  }

 private:
  folly::coro::Task<void> controlReadLoop(
      std::unique_ptr<MoQSession::ControlVisitor> controller) {
    while (auto msg =
               co_await moqClient_.moqSession_->controlMessages().next()) {
      boost::apply_visitor(*controller, msg.value());
    }
  }
  MoQClient moqClient_;
  std::function<std::unique_ptr<MoQSession::ControlVisitor>(
      std::shared_ptr<MoQSession>)>
      controllerFn_;
};

} // namespace moxygen

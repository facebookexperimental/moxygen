/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <moxygen/MoQSession.h>

#include <folly/coro/Promise.h>
#include <proxygen/lib/http/webtransport/QuicWebTransport.h>
#include <proxygen/lib/http/webtransport/WebTransport.h>
#include <proxygen/lib/utils/URL.h>
#include "moxygen/mlog/MLogger.h"

namespace moxygen {

const std::string kDefaultClientFilePath = "./mlog_client.txt";

class Subscriber;

class MoQClient : public proxygen::WebTransportHandler {
 public:
  MoQClient(folly::EventBase* evb, proxygen::URL url)
      : evb_(evb), url_(std::move(url)) {}

  folly::EventBase* getEventBase() {
    return evb_;
  }

  std::shared_ptr<MoQSession> moqSession_;
  virtual folly::coro::Task<void> setupMoQSession(
      std::chrono::milliseconds connect_timeout,
      std::chrono::milliseconds transaction_timeout,
      std::shared_ptr<Publisher> publishHandler,
      std::shared_ptr<Subscriber> subscribeHandler) noexcept;

  void setLogger(std::shared_ptr<MLogger> logger);

 protected:
  folly::coro::Task<ServerSetup> completeSetupMoQSession(
      proxygen::WebTransport* wt,
      const folly::Optional<std::string>& pathParam,
      std::shared_ptr<Publisher> publishHandler,
      std::shared_ptr<Subscriber> subscribeHandler);
  ClientSetup getClientSetup(const folly::Optional<std::string>& path);

  void onSessionEnd(folly::Optional<uint32_t>) override;
  void onNewBidiStream(
      proxygen::WebTransport::BidiStreamHandle handle) override;
  void onNewUniStream(
      proxygen::WebTransport::StreamReadHandle* handle) override;
  void onDatagram(std::unique_ptr<folly::IOBuf>) override;

  folly::EventBase* evb_{nullptr};
  proxygen::URL url_;
  std::shared_ptr<proxygen::QuicWebTransport> quicWebTransport_;
  std::shared_ptr<MLogger> logger_ = nullptr;
};

} // namespace moxygen

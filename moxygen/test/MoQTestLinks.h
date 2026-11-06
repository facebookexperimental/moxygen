/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

// FakeSharedWebTransport.h uses xlog without including it.
#include <folly/logging/xlog.h>
#include <proxygen/lib/http/webtransport/test/FakeSharedWebTransport.h>
#include <memory>
#include <vector>

#include "moxygen/MoQSession.h"

namespace moxygen::test {

// A client and a server MoQSession over a FakeSharedWebTransport pair.
struct MoQTestLink {
  std::unique_ptr<proxygen::test::FakeSharedWebTransport> clientWt;
  std::unique_ptr<proxygen::test::FakeSharedWebTransport> serverWt;
  std::shared_ptr<MoQSession> client;
  std::shared_ptr<MoQSession> server;
};

// Connects session pairs at one MoQT version. The server side accepts setup
// as a relay server does.
class MoQTestLinks : public MoQSession::ServerSetupCallback {
 public:
  MoQTestLinks(uint64_t version, std::shared_ptr<MoQExecutor> executor)
      : version_(version), executor_(std::move(executor)) {}

  folly::coro::Task<MoQTestLink*> connect(
      std::shared_ptr<Publisher> serverPublisher,
      std::shared_ptr<Subscriber> serverSubscriber = nullptr);
  void closeClients();
  // Drops every link; close them first.
  void clear();

  folly::Expected<folly::Unit, SessionCloseErrorCode> validateAuthority(
      const Setup&,
      uint64_t,
      std::shared_ptr<MoQSession>) override {
    return folly::unit;
  }
  folly::Try<Setup> onClientSetup(Setup, const std::shared_ptr<MoQSession>&)
      override;

 private:
  uint64_t version_;
  std::shared_ptr<MoQExecutor> executor_;
  std::vector<std::unique_ptr<MoQTestLink>> links_;
};

} // namespace moxygen::test

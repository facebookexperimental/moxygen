/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/proxy/MoQWebTransportBootstrapHandler.h"

#include <folly/coro/BlockingWait.h>
#include <folly/io/async/EventBase.h>
#include <folly/portability/GTest.h>
#include <proxygen/lib/http/webtransport/test/FakeSharedWebTransport.h>

#include <memory>
#include <stdexcept>

#include "moxygen/events/MoQFollyExecutorImpl.h"

namespace moxygen::test {

TEST(MoQWebTransportBootstrapHandlerTest, CreatesSessionAndOwnsTransport) {
  folly::EventBase eventBase;
  auto executor = std::make_shared<MoQFollyExecutorImpl>(&eventBase);
  auto pendingSession = makeMoQWebTransportSession(std::move(executor));
  auto [clientTransport, serverTransport] =
      proxygen::test::FakeSharedWebTransport::makeSharedWebTransport();
  auto transport = std::shared_ptr<proxygen::test::FakeSharedWebTransport>(
      std::move(clientTransport));

  pendingSession.handler->onWebTransportSession(transport);
  auto session =
      folly::coro::blockingWait(std::move(pendingSession.session), &eventBase);

  ASSERT_NE(session, nullptr);
  EXPECT_FALSE(transport->isSessionClosed());
  session.reset();
  EXPECT_TRUE(transport->isSessionClosed());
}

TEST(MoQWebTransportBootstrapHandlerTest, RejectsNullExecutor) {
  EXPECT_THROW(makeMoQWebTransportSession(nullptr), std::invalid_argument);
}

} // namespace moxygen::test

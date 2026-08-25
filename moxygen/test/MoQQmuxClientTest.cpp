/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <moxygen/MoQQmuxClient.h>

#include <folly/coro/Error.h>
#include <folly/portability/GTest.h>

#undef EV_READ
#undef EV_WRITE
#undef EV_TIMEOUT
#undef EV_SIGNAL
#undef EVLOOP_NONBLOCK

#include <quic/common/events/QuicLibevExecutorImpl.h>

#include <stdexcept>

namespace moxygen { namespace {

class EvLoop : public quic::LibevQuicEventBase::EvLoopHolder {
 public:
  EvLoop() : loop_(ev_loop_new(0)) {}

  ~EvLoop() override {
    ev_loop_destroy(loop_);
  }

  struct ev_loop* get() override {
    return loop_;
  }

  std::optional<pthread_t> getEventLoopThread() override {
    return pthread_self();
  }

 private:
  struct ev_loop* loop_;
};

class TestQmuxTransportFactory final : public QmuxTransportFactory {
 public:
  folly::coro::Task<QmuxTransportConnectResult> createQmuxTransport(
      const proxygen::URL& url,
      std::chrono::milliseconds connectTimeout,
      const std::vector<std::string>& alpns) override {
    called = true;
    observedHost = url.getHost();
    observedPort = url.getPort();
    observedTimeout = connectTimeout;
    observedAlpns = alpns;
    co_yield folly::coro::co_error(
        std::runtime_error("expected transport factory failure"));
  }

  bool called{false};
  std::string observedHost;
  uint16_t observedPort{0};
  std::chrono::milliseconds observedTimeout{0};
  std::vector<std::string> observedAlpns;
};

folly::coro::Task<void> setupAndCapture(
    std::shared_ptr<MoQQmuxClient> client,
    std::chrono::milliseconds connectTimeout,
    std::vector<std::string> alpns,
    folly::exception_wrapper* error,
    bool* done) {
  auto result = co_await folly::coro::co_awaitTry(client->setupMoQSession(
      connectTimeout,
      std::chrono::milliseconds(0),
      nullptr,
      nullptr,
      quic::TransportSettings{},
      std::move(alpns)));
  if (result.hasException()) {
    *error = result.exception();
  }
  *done = true;
}

TEST(MoQQmuxClientTest, UsesInjectedTransportFactoryOnLibevExecutor) {
  auto executor =
      std::make_shared<quic::QuicLibevExecutorImpl>(std::make_unique<EvLoop>());
  auto transportFactory = std::make_shared<TestQmuxTransportFactory>();

  auto client = std::make_shared<MoQQmuxClient>(
      executor,
      proxygen::URL("moqt://example.com:4443/path"),
      transportFactory);
  const auto expectedTimeout = std::chrono::milliseconds(321);
  const std::vector<std::string> expectedAlpns{"moq-00", "moq-01"};
  folly::exception_wrapper error;
  bool done = false;

  folly::coro::co_withExecutor(
      executor.get(),
      setupAndCapture(client, expectedTimeout, expectedAlpns, &error, &done))
      .start();
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(1);
  while (!done && std::chrono::steady_clock::now() < deadline) {
    executor->loop();
  }

  ASSERT_TRUE(done);
  EXPECT_TRUE(transportFactory->called);
  EXPECT_EQ(transportFactory->observedHost, "example.com");
  EXPECT_EQ(transportFactory->observedPort, 4443);
  EXPECT_EQ(transportFactory->observedTimeout, expectedTimeout);
  EXPECT_EQ(transportFactory->observedAlpns, expectedAlpns);
  EXPECT_TRUE(error.is_compatible_with<std::runtime_error>());
}

}} // namespace moxygen

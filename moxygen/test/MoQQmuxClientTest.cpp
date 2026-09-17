/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <moxygen/MoQQmuxClient.h>

#include <folly/CancellationToken.h>
#include <folly/coro/Error.h>
#include <folly/coro/WithCancellation.h>
#include <folly/io/async/AsyncServerSocket.h>
#include <folly/io/async/AsyncSocket.h>
#include <folly/io/async/AsyncTransport.h>
#include <folly/io/async/ScopedEventBaseThread.h>
#include <folly/portability/GTest.h>
#include <folly/synchronization/SaturatingSemaphore.h>
#include <moxygen/MoQFollyQmuxTransportFactory.h>
#include <moxygen/events/MoQFollyExecutorImpl.h>
#include <moxygen/util/InsecureVerifierDangerousDoNotUseInProduction.h>

#undef EV_READ
#undef EV_WRITE
#undef EV_TIMEOUT
#undef EV_SIGNAL
#undef EVLOOP_NONBLOCK

#include <quic/common/events/QuicLibevExecutorImpl.h>

#include <stdexcept>

namespace moxygen { namespace {

using namespace std::chrono_literals;

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

// Enough of a QmuxTransport to be adopted. Reads and writes fail, so setup
// goes no further -- the point is only that the factory was bypassed.
class StubQmuxTransport final : public proxygen::qmux::QmuxTransport {
 public:
  folly::coro::Task<size_t> read(
      folly::IOBufQueue& /*readBuf*/,
      size_t /*minReadSize*/,
      size_t /*newAllocationSize*/,
      std::chrono::milliseconds /*timeout*/) override {
    co_yield folly::coro::co_error(std::runtime_error("stub transport read"));
  }

  folly::coro::Task<folly::Unit> write(
      folly::IOBufQueue& /*writeBuf*/,
      std::chrono::milliseconds /*timeout*/) override {
    co_yield folly::coro::co_error(std::runtime_error("stub transport write"));
  }

  void shutdownWrite() override {}

  [[nodiscard]] folly::SocketAddress getLocalAddress() const noexcept override {
    return {};
  }

  [[nodiscard]] folly::SocketAddress getPeerAddress() const noexcept override {
    return {};
  }

  [[nodiscard]] folly::AsyncTransport* getUnderlyingTransport()
      const noexcept override {
    return nullptr;
  }
};

class IdleTcpServer : public folly::AsyncServerSocket::AcceptCallback,
                      public folly::AsyncTransport::ReadCallback {
 public:
  IdleTcpServer() : evb_(thread_.getEventBase()) {
    evb_->runInEventBaseThreadAndWait([&] {
      serverSocket_ = folly::AsyncServerSocket::newSocket(evb_);
      serverSocket_->bind(folly::SocketAddress("127.0.0.1", 0));
      serverSocket_->listen(1);
      serverSocket_->addAcceptCallback(this, nullptr);
      serverSocket_->startAccepting();
      serverSocket_->getAddress(&address_);
    });
  }

  ~IdleTcpServer() override {
    evb_->runInEventBaseThreadAndWait([&] {
      if (acceptedSocket_) {
        acceptedSocket_->setReadCB(nullptr);
        acceptedSocket_->closeNow();
        acceptedSocket_.reset();
      }
      serverSocket_->stopAccepting();
      serverSocket_.reset();
    });
  }

  const folly::SocketAddress& getAddress() const {
    return address_;
  }

  bool waitForClientHello(std::chrono::milliseconds timeout) {
    return clientHelloReceived_.try_wait_for(timeout);
  }

  void closeAcceptedConnection() {
    evb_->runInEventBaseThreadAndWait([&] {
      if (acceptedSocket_) {
        acceptedSocket_->setReadCB(nullptr);
        acceptedSocket_->closeNow();
        acceptedSocket_.reset();
      }
    });
  }

  void connectionAccepted(
      folly::NetworkSocket fd,
      const folly::SocketAddress& /* clientAddr */,
      AcceptInfo /* info */) noexcept override {
    acceptedSocket_ = folly::AsyncSocket::newSocket(evb_, fd);
    acceptedSocket_->setReadCB(this);
  }

  void acceptError(folly::exception_wrapper ex) noexcept override {
    ADD_FAILURE() << "TCP accept failed: " << ex.what();
  }

  void getReadBuffer(void** bufReturn, size_t* lenReturn) override {
    *bufReturn = readBuffer_;
    *lenReturn = sizeof(readBuffer_);
  }

  void readDataAvailable(size_t len) noexcept override {
    if (len > 0) {
      clientHelloReceived_.post();
    }
  }

  void readEOF() noexcept override {}

  void readErr(const folly::AsyncSocketException& /* ex */) noexcept override {}

 private:
  folly::ScopedEventBaseThread thread_{"idle-qmux-server"};
  folly::EventBase* evb_;
  std::shared_ptr<folly::AsyncServerSocket> serverSocket_;
  folly::AsyncSocket::UniquePtr acceptedSocket_;
  folly::SocketAddress address_;
  folly::SaturatingSemaphore<true> clientHelloReceived_;
  char readBuffer_[4096]{};
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
      alpns));
  if (result.hasException()) {
    *error = result.exception();
  }
  *done = true;
}

struct QmuxConnectState {
  folly::SaturatingSemaphore<true> done;
  folly::exception_wrapper exception;
};

folly::coro::Task<void> connectQmuxAndSignal(
    std::shared_ptr<QmuxTransportFactory> transportFactory,
    proxygen::URL url,
    std::shared_ptr<QmuxConnectState> state) {
  auto result =
      co_await folly::coro::co_awaitTry(transportFactory->createQmuxTransport(
          url, 60s, std::vector<std::string>{"moqt-16"}));
  if (result.hasException()) {
    state->exception = std::move(result.exception());
  }
  state->done.post();
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

// A caller-supplied transport is adopted, so the factory is never consulted.
TEST(MoQQmuxClientTest, AdoptedTransportSkipsTransportFactory) {
  auto executor =
      std::make_shared<quic::QuicLibevExecutorImpl>(std::make_unique<EvLoop>());
  auto transportFactory = std::make_shared<TestQmuxTransportFactory>();

  auto client = std::make_shared<MoQQmuxClient>(
      executor,
      proxygen::URL("moqt://example.com:4443/path"),
      transportFactory);
  client->setQmuxTransport(std::make_unique<StubQmuxTransport>(), "moq-00");

  folly::exception_wrapper error;
  bool done = false;
  folly::coro::co_withExecutor(
      executor.get(),
      setupAndCapture(
          client, std::chrono::milliseconds(321), {"moq-00"}, &error, &done))
      .start();
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(1);
  while (!done && std::chrono::steady_clock::now() < deadline) {
    executor->loop();
  }

  ASSERT_TRUE(done);
  EXPECT_FALSE(transportFactory->called);
}

void testCancellationDrainsParkedFizzHandshake(bool cancelOnEventBase) {
  IdleTcpServer server;
  folly::ScopedEventBaseThread clientThread("qmux-client");
  auto executor =
      std::make_shared<MoQFollyExecutorImpl>(clientThread.getEventBase());
  auto transportFactory = makeFollyQmuxTransportFactory(
      executor,
      std::make_shared<test::InsecureVerifierDangerousDoNotUseInProduction>());
  const auto& serverAddress = server.getAddress();
  proxygen::URL url(
      "moqt", serverAddress.getAddressStr(), serverAddress.getPort(), "/");
  folly::CancellationSource cancellationSource;
  auto state = std::make_shared<QmuxConnectState>();

  folly::coro::co_withExecutor(
      executor.get(),
      folly::coro::co_withCancellation(
          cancellationSource.getToken(),
          connectQmuxAndSignal(transportFactory, std::move(url), state)))
      .start();

  const bool sawClientHello = server.waitForClientHello(5s);
  const bool completedBeforeCancellation = state->done.try_wait_for(0ms);
  if (cancelOnEventBase) {
    clientThread.getEventBase()->runInEventBaseThreadAndWait(
        [&] { cancellationSource.requestCancellation(); });
  } else {
    cancellationSource.requestCancellation();
  }

  const bool completedPromptly = state->done.try_wait_for(5s);

  server.closeAcceptedConnection();
  const bool drainedAfterPeerClose = completedBeforeCancellation ||
      completedPromptly || state->done.try_wait_for(5s);

  EXPECT_TRUE(sawClientHello) << "QMUX client did not start the Fizz handshake";
  EXPECT_FALSE(completedBeforeCancellation)
      << "QMUX connection completed before cancellation";
  ASSERT_TRUE(drainedAfterPeerClose)
      << "QMUX client did not finish after the peer connection was closed";
  EXPECT_TRUE(completedPromptly)
      << "QMUX client waited for the Fizz handshake after cancellation";
  EXPECT_TRUE(state->exception.is_compatible_with<folly::OperationCancelled>());
  clientThread.getEventBase()->runInEventBaseThreadAndWait([] {});
}

TEST(MoQQmuxClientTest, CancellationDrainsParkedFizzHandshake) {
  testCancellationDrainsParkedFizzHandshake(true);
}

TEST(MoQQmuxClientTest, OffThreadCancellationDrainsParkedFizzHandshake) {
  testCancellationDrainsParkedFizzHandshake(false);
}

}} // namespace moxygen

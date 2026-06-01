/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <fizz/server/FizzServerContext.h>
#include <folly/io/async/AsyncSocket.h>
#include <folly/io/async/AsyncTransport.h>
#include <folly/io/async/EventBase.h>
#include <folly/io/async/ScopedEventBaseThread.h>
#include <folly/portability/GTest.h>
#include <folly/synchronization/Baton.h>
#include <proxygen/httpserver/samples/hq/FizzContext.h>
#include <moxygen/MoQQmuxServer.h>
#include <moxygen/MoQVersions.h>

#include <memory>
#include <thread>
#include <utility>

using namespace std::chrono_literals;

namespace moxygen::test {
namespace {

// MoQServerBase::onNewSession is pure-virtual. The Fizz handshake never
// completes in these tests, so the override never runs — it just satisfies
// the abstract-class requirement.
class NopMoQQmuxServer : public MoQQmuxServer {
 public:
  using MoQQmuxServer::MoQQmuxServer;
  void onNewSession(std::shared_ptr<MoQSession> /*session*/) override {}
};

std::shared_ptr<const fizz::server::FizzServerContext> makeTestFizzContext() {
  return quic::samples::createFizzServerContextWithInsecureDefault(
      getDefaultMoqtProtocols(
          /*includeExperimental=*/true, /*useStandard=*/false),
      fizz::server::ClientAuthMode::None,
      /*certificateFilePath=*/"",
      /*keyFilePath=*/"");
}

class ConnectCallback : public folly::AsyncSocket::ConnectCallback {
 public:
  void connectSuccess() noexcept override {
    success = true;
    baton.post();
  }
  void connectErr(const folly::AsyncSocketException& ex) noexcept override {
    error = ex.what();
    baton.post();
  }
  folly::Baton<> baton;
  bool success{false};
  std::string error;
};

// Posts `baton` on EOF or read error. Records if any data was received so the
// test can assert that an idle handshake didn't unexpectedly produce bytes.
class EofReadCallback : public folly::AsyncTransport::ReadCallback {
 public:
  void getReadBuffer(void** bufReturn, size_t* lenReturn) override {
    *bufReturn = buf;
    *lenReturn = sizeof(buf);
  }
  void readDataAvailable(size_t /*len*/) noexcept override {
    dataReceived = true;
    baton.post();
  }
  void readEOF() noexcept override {
    baton.post();
  }
  void readErr(const folly::AsyncSocketException& /*ex*/) noexcept override {
    baton.post();
  }
  folly::Baton<> baton;
  bool dataReceived{false};
  char buf[1024];
};

// Run `server.stop()` on a helper thread with a test-failure deadline. If
// stop() truly hangs we leak the thread rather than abort the test process.
void expectStopReturnsWithin(
    MoQQmuxServer& server,
    std::chrono::milliseconds deadline) {
  auto done = std::make_shared<folly::Baton<>>();
  std::thread stopThread([&server, done] {
    server.stop();
    done->post();
  });
  if (done->try_wait_for(deadline)) {
    stopThread.join();
  } else {
    ADD_FAILURE() << "server->stop() did not return within " << deadline.count()
                  << "ms";
    stopThread.detach();
  }
}

} // namespace

// A TCP peer that completes the SYN/SYN-ACK handshake but never sends any TLS
// bytes must not park the server's handleAccept indefinitely.
// config_.handshakeTimeout has to bound the wait.
TEST(MoQQmuxServerTest, IdleConnectionHandshakeTimesOut) {
  MoQQmuxServer::Config config;
  config.handshakeTimeout = 100ms;

  auto server = std::make_unique<NopMoQQmuxServer>(
      "/test", makeTestFizzContext(), std::move(config));
  server->start(folly::SocketAddress("127.0.0.1", 0));
  auto serverAddr = server->getAddress();

  folly::ScopedEventBaseThread clientThread("test-client");
  auto* clientEvb = clientThread.getEventBase();

  folly::AsyncSocket::UniquePtr clientSocket;
  ConnectCallback connectCb;
  clientEvb->runInEventBaseThreadAndWait([&] {
    clientSocket.reset(new folly::AsyncSocket(clientEvb));
    clientSocket->connect(&connectCb, serverAddr);
  });
  ASSERT_TRUE(connectCb.baton.try_wait_for(5s))
      << "TCP connect to test server hung";
  ASSERT_TRUE(connectCb.success) << "TCP connect failed: " << connectCb.error;

  EofReadCallback readCb;
  clientEvb->runInEventBaseThreadAndWait(
      [&] { clientSocket->setReadCB(&readCb); });

  // The server's Fizz handshake should time out after ~100ms and close the
  // connection. The 5s wait is a test-failure deadline, not a polling delay —
  // a regression to an unbounded wait would fail loudly here instead of
  // hanging.
  EXPECT_TRUE(readCb.baton.try_wait_for(5s))
      << "Server did not close idle TLS handshake within deadline";
  EXPECT_FALSE(readCb.dataReceived)
      << "Server unexpectedly sent bytes on an idle handshake";

  clientEvb->runInEventBaseThreadAndWait([&] {
    if (clientSocket) {
      clientSocket->setReadCB(nullptr);
      clientSocket.reset();
    }
  });

  // handleAccept already finished above; stop() should just clean up workers.
  expectStopReturnsWithin(*server, 5s);
}

// A stop() initiated while handleAccept is parked in the Fizz handshake must
// drain quickly via the cancellation bridge, not wait for handshakeTimeout.
TEST(MoQQmuxServerTest, StopDrainsParkedHandshake) {
  MoQQmuxServer::Config config;
  // Long handshakeTimeout so a 5s test deadline conclusively measures the
  // cancellation/drain path, not incidental timeout firing.
  config.handshakeTimeout = 60s;

  auto server = std::make_unique<NopMoQQmuxServer>(
      "/test", makeTestFizzContext(), std::move(config));
  server->start(folly::SocketAddress("127.0.0.1", 0));
  auto serverAddr = server->getAddress();

  folly::ScopedEventBaseThread clientThread("test-client");
  auto* clientEvb = clientThread.getEventBase();

  folly::AsyncSocket::UniquePtr clientSocket;
  ConnectCallback connectCb;
  clientEvb->runInEventBaseThreadAndWait([&] {
    clientSocket.reset(new folly::AsyncSocket(clientEvb));
    clientSocket->connect(&connectCb, serverAddr);
  });
  ASSERT_TRUE(connectCb.baton.try_wait_for(5s))
      << "TCP connect to test server hung";
  ASSERT_TRUE(connectCb.success) << "TCP connect failed: " << connectCb.error;

  // Do NOT send any TLS bytes. The server's first-loop EB round-trip in
  // stop() is a FIFO barrier: any queued connectionAccepted callback runs
  // before the socket close, so handleAccept is in flight when the second
  // loop drains. stop() must drive the cancellation bridge to wake the Fizz
  // wait; otherwise we'd block ~60s on the handshakeTimeout.
  expectStopReturnsWithin(*server, 5s);

  clientEvb->runInEventBaseThreadAndWait([&] {
    if (clientSocket) {
      clientSocket.reset();
    }
  });
}

} // namespace moxygen::test

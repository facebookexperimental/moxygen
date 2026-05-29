/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <moxygen/MoQQmuxServer.h>

#include <folly/ScopeGuard.h>
#include <folly/coro/WithCancellation.h>
#include <folly/io/async/AsyncSocket.h>
#include <folly/io/coro/Transport.h>
#include <folly/logging/xlog.h>
#include <proxygen/lib/transport/qmux/QmuxConnector.h>
#include <moxygen/events/MoQFollyExecutorImpl.h>

namespace moxygen {

class MoQQmuxServer::WorkerAcceptCallback
    : public folly::AsyncServerSocket::AcceptCallback {
 public:
  WorkerAcceptCallback(
      MoQQmuxServer& server,
      folly::EventBase* workerEvb,
      MoQQmuxServer::WorkerShutdownState* shutdownState)
      : server_(server),
        workerEvb_(workerEvb),
        shutdownState_(shutdownState),
        executor_(std::make_shared<MoQFollyExecutorImpl>(workerEvb)) {}

  void connectionAccepted(
      folly::NetworkSocket fd,
      const folly::SocketAddress& clientAddr,
      AcceptInfo /*info*/) noexcept override {
    XLOG(DBG1) << "MoQQmuxServer: accepted connection from "
               << clientAddr.describe();
    folly::AsyncTransport::UniquePtr asyncSocket(
        new folly::AsyncSocket(workerEvb_, fd));
    co_withExecutor(
        workerEvb_,
        server_.handleAccept(
            workerEvb_, executor_, std::move(asyncSocket), shutdownState_))
        .start();
  }

  void acceptError(folly::exception_wrapper ex) noexcept override {
    XLOG(ERR) << "MoQQmuxServer: accept error: " << ex.what();
  }

 private:
  MoQQmuxServer& server_;
  folly::EventBase* const workerEvb_;
  MoQQmuxServer::WorkerShutdownState* const shutdownState_;
  const std::shared_ptr<MoQExecutor> executor_;
};

MoQQmuxServer::MoQQmuxServer(std::string endpoint, Config config)
    : MoQServerBase(std::move(endpoint)), config_(std::move(config)) {}

MoQQmuxServer::~MoQQmuxServer() {
  if (!serverSockets_.empty()) {
    stop();
  }
}

void MoQQmuxServer::start(
    const folly::SocketAddress& addr,
    std::vector<folly::EventBase*> evbs) {
  CHECK(!stopped_) << "MoQQmuxServer::start called after stop()";
  CHECK(serverSockets_.empty()) << "MoQQmuxServer::start called twice";

  if (evbs.empty()) {
    // No caller-supplied pool — spin up our own. Joined in stop().
    CHECK_GT(config_.serverThreads, 0u)
        << "Config::serverThreads must be > 0 when start() is called "
           "without a caller-supplied EventBase pool";
    ownedWorkers_.reserve(config_.serverThreads);
    workerEvbs_.reserve(config_.serverThreads);
    for (size_t i = 0; i < config_.serverThreads; ++i) {
      ownedWorkers_.push_back(
          std::make_unique<folly::ScopedEventBaseThread>("MoQQmuxServer"));
      workerEvbs_.push_back(ownedWorkers_.back()->getEventBase());
    }
  } else {
    for (auto* evb : evbs) {
      CHECK(evb) << "null EventBase in MoQQmuxServer worker pool";
    }
    workerEvbs_ = std::move(evbs);
  }

  serverSockets_.reserve(workerEvbs_.size());
  workerCallbacks_.reserve(workerEvbs_.size());
  workerShutdownState_.reserve(workerEvbs_.size());
  for (size_t i = 0; i < workerEvbs_.size(); ++i) {
    workerShutdownState_.push_back(std::make_unique<WorkerShutdownState>());
  }

  // If `addr` has port 0 the first bind picks an ephemeral port; the
  // remaining workers must bind to that concrete port to join the same
  // reuseport group.
  folly::SocketAddress bindAddr = addr;

  for (size_t i = 0; i < workerEvbs_.size(); ++i) {
    auto* workerEvb = workerEvbs_[i];
    auto callback = std::make_unique<WorkerAcceptCallback>(
        *this, workerEvb, workerShutdownState_[i].get());
    std::shared_ptr<folly::AsyncServerSocket> socket;
    // All AsyncServerSocket lifecycle (construct, bind, listen, callback
    // registration, startAccepting) must run on the socket's owning evb.
    workerEvb->runImmediatelyOrRunInEventBaseThreadAndWait(
        [&, callbackPtr = callback.get()] {
          socket = folly::AsyncServerSocket::newSocket(workerEvb);
          socket->setReusePortEnabled(true);
          socket->bind(bindAddr);
          socket->listen(/*backlog=*/128);
          // nullptr eventBase => callback runs on the socket's own evb
          // (== workerEvb)
          socket->addAcceptCallback(callbackPtr, /*eventBase=*/nullptr);
          socket->startAccepting();
          if (bindAddr.getPort() == 0) {
            // Resolve the kernel-assigned ephemeral port from the first
            // bind so the rest of the workers can join the reuseport
            // group on that exact port.
            socket->getAddress(&bindAddr);
          }
        });
    serverSockets_.push_back(std::move(socket));
    workerCallbacks_.push_back(std::move(callback));
  }

  boundAddr_ = bindAddr;
  XLOG(DBG1) << "MoQQmuxServer listening on " << boundAddr_.describe()
             << " across " << workerEvbs_.size()
             << " worker evb(s) (SO_REUSEPORT)";
}

void MoQQmuxServer::stop() {
  CHECK(!isInWorkerPool())
      << "MoQQmuxServer::stop must not be called from a worker EB thread";
  if (serverSockets_.empty()) {
    return;
  }
  stopped_ = true;
  for (size_t i = 0; i < serverSockets_.size(); ++i) {
    auto* workerEvb = workerEvbs_[i];
    auto& socket = serverSockets_[i];
    workerEvb->runImmediatelyOrRunInEventBaseThreadAndWait([&] {
      socket->stopAccepting();
      socket.reset();
    });
  }
  serverSockets_.clear();
  workerCallbacks_.clear();
  cancelSource_.requestCancellation();

  for (size_t i = 0; i < workerEvbs_.size(); ++i) {
    auto* workerEvb = workerEvbs_[i];
    auto& state = workerShutdownState_[i];
    workerEvb->runImmediatelyOrRunInEventBaseThreadAndWait([&] {
      state->draining = true;
      for (auto* session : state->liveSessions) {
        session->close(SessionCloseErrorCode::NO_ERROR);
      }
      if (state->inflightAccepts == 0) {
        // No handleAccept will post for us; do it ourselves.
        state->done.post();
      }
    });
    state->done.wait();
  }

  workerShutdownState_.clear();
  workerEvbs_.clear();
  ownedWorkers_.clear();
}

folly::coro::Task<void> MoQQmuxServer::handleAccept(
    folly::EventBase* workerEvb,
    std::shared_ptr<MoQExecutor> executor,
    folly::AsyncTransport::UniquePtr asyncSocket,
    WorkerShutdownState* state) {
  ++state->inflightAccepts;
  SCOPE_EXIT {
    if (--state->inflightAccepts == 0 && state->draining) {
      state->done.post();
    }
  };

  co_await folly::coro::co_withCancellation(
      cancelSource_.getToken(), [&]() -> folly::coro::Task<void> {
        auto transport = std::make_unique<folly::coro::Transport>(
            workerEvb, std::move(asyncSocket));

        auto sessionResult = co_await folly::coro::co_awaitTry(
            proxygen::qmux::QmuxConnector::connect(
                workerEvb,
                proxygen::qmux::WtDir::Server,
                config_.selfTransportParams,
                std::move(transport),
                config_.handshakeTimeout,
                config_.sessionConfig));
        if (sessionResult.hasException()) {
          XLOG(WARN) << "MoQQmuxServer: QMUX handshake failed: "
                     << sessionResult.exception().what();
          co_return;
        }

        auto qmuxSession = std::move(*sessionResult);
        auto moqSession = createSession(qmuxSession, std::move(executor));
        qmuxSession->setHandler(moqSession.get());
        qmuxSession->start(qmuxSession);

        auto* moqSessionPtr = moqSession.get();
        state->liveSessions.insert(moqSessionPtr);
        SCOPE_EXIT {
          state->liveSessions.erase(moqSessionPtr);
        };

        co_await handleClientSession(std::move(moqSession));
      }());
}

} // namespace moxygen

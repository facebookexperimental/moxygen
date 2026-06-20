/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <moxygen/MoQQmuxServer.h>

#include <fizz/server/AsyncFizzServer.h>
#include <folly/ScopeGuard.h>
#include <folly/coro/Baton.h>
#include <folly/coro/CurrentExecutor.h>
#include <folly/coro/Error.h>
#include <folly/coro/Timeout.h>
#include <folly/coro/WithCancellation.h>
#include <folly/futures/ThreadWheelTimekeeper.h>
#include <folly/io/async/AsyncSocket.h>
#include <folly/io/async/EventBaseManager.h>
#include <folly/io/coro/Transport.h>
#include <folly/logging/xlog.h>
#include <proxygen/lib/transport/qmux/QmuxConnector.h>
#include <moxygen/MoQVersions.h>
#include <moxygen/events/MoQFollyExecutorImpl.h>
#include <algorithm>

namespace moxygen {

namespace {

// Bridges AsyncFizzServer's callback-based handshake into a coroutine.
class FizzAcceptCb : public fizz::server::AsyncFizzServer::HandshakeCallback {
 public:
  void fizzHandshakeSuccess(
      fizz::server::AsyncFizzServer* /*transport*/) noexcept override {
    baton.post();
  }
  void fizzHandshakeError(
      fizz::server::AsyncFizzServer* /*transport*/,
      folly::exception_wrapper ex) noexcept override {
    exception = std::move(ex);
    baton.post();
  }
  // QMUX-over-TLS is TLS 1.3 only; refuse the SSLv2-style fallback path.
  void fizzHandshakeAttemptFallback(
      fizz::server::AttemptVersionFallback /*fallback*/) override {
    exception = folly::make_exception_wrapper<std::runtime_error>(
        "MoQQmuxServer: TLS version fallback not supported");
    baton.post();
  }

  folly::coro::Baton baton;
  folly::exception_wrapper exception;
};

} // namespace

class MoQQmuxServer::WorkerAcceptCallback
    : public folly::AsyncServerSocket::AcceptCallback {
 public:
  WorkerAcceptCallback(
      MoQQmuxServer& server,
      folly::EventBase* workerEvb,
      MoQQmuxServer::WorkerShutdownState* shutdownState)
      : server_(server), workerEvb_(workerEvb), shutdownState_(shutdownState) {}

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
            workerEvb_, std::move(asyncSocket), shutdownState_))
        .start();
  }

  void acceptError(folly::exception_wrapper ex) noexcept override {
    XLOG(ERR) << "MoQQmuxServer: accept error: " << ex.what();
  }

 private:
  MoQQmuxServer& server_;
  folly::EventBase* const workerEvb_;
  MoQQmuxServer::WorkerShutdownState* const shutdownState_;
};

MoQQmuxServer::MoQQmuxServer(
    std::string endpoint,
    std::shared_ptr<const fizz::server::FizzServerContext> fizzContext,
    Config config)
    : MoQServerBase(std::move(endpoint)),
      config_(std::move(config)),
      fizzContext_(std::move(fizzContext)) {
  CHECK(fizzContext_) << "MoQQmuxServer requires a non-null FizzServerContext";
  const auto& alpns = fizzContext_->getSupportedAlpns();
  CHECK(std::any_of(alpns.begin(), alpns.end(), [](const std::string& a) {
    return isLegacyAlpn(a) || getVersionFromAlpn(a).has_value();
  })) << "FizzServerContext must advertise at least one MoQ ALPN";
}

MoQQmuxServer::~MoQQmuxServer() {
  if (started_ && !stopped_) {
    stop();
  }
}

void MoQQmuxServer::start(
    const folly::SocketAddress& addr,
    std::vector<folly::EventBase*> evbs) {
  CHECK(!stopped_) << "MoQQmuxServer::start called after stop()";
  CHECK(!started_)
      << "MoQQmuxServer::start called twice / after initExternallyFed";

  // Rollback guard to ensure that we cleanup if start() throws.
  auto rollback = folly::makeGuard([this] { teardown(); });

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
  workerExecutors_.reserve(workerEvbs_.size());
  for (size_t i = 0; i < workerEvbs_.size(); ++i) {
    workerShutdownState_.push_back(std::make_unique<WorkerShutdownState>());
    workerExecutors_.push_back(
        std::make_shared<MoQFollyExecutorImpl>(workerEvbs_[i]));
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
  started_ = true;
  rollback.dismiss();
  XLOG(DBG1) << "MoQQmuxServer listening on " << boundAddr_.describe()
             << " across " << workerEvbs_.size()
             << " worker evb(s) (SO_REUSEPORT)";
}

void MoQQmuxServer::initExternallyFed(
    std::vector<folly::EventBase*> workerEvbs) {
  CHECK(!stopped_) << "MoQQmuxServer::initExternallyFed called after stop()";
  CHECK(!started_)
      << "MoQQmuxServer::initExternallyFed called twice / after start()";
  CHECK(!workerEvbs.empty())
      << "MoQQmuxServer::initExternallyFed requires a non-empty worker pool";
  for (auto* evb : workerEvbs) {
    CHECK(evb) << "null EventBase in MoQQmuxServer worker pool";
  }
  workerEvbs_ = std::move(workerEvbs);
  workerShutdownState_.reserve(workerEvbs_.size());
  workerExecutors_.reserve(workerEvbs_.size());
  for (size_t i = 0; i < workerEvbs_.size(); ++i) {
    workerShutdownState_.push_back(std::make_unique<WorkerShutdownState>());
    workerExecutors_.push_back(
        std::make_shared<MoQFollyExecutorImpl>(workerEvbs_[i]));
  }
  started_ = true;
}

MoQQmuxServer::WorkerShutdownState* MoQQmuxServer::findStateFor(
    folly::EventBase* workerEvb) noexcept {
  for (size_t i = 0; i < workerEvbs_.size(); ++i) {
    if (workerEvbs_[i] == workerEvb) {
      return workerShutdownState_[i].get();
    }
  }
  return nullptr;
}

std::shared_ptr<MoQExecutor> MoQQmuxServer::findExecutorFor(
    folly::EventBase* workerEvb) noexcept {
  for (size_t i = 0; i < workerEvbs_.size(); ++i) {
    if (workerEvbs_[i] == workerEvb) {
      return workerExecutors_[i];
    }
  }
  return nullptr;
}

bool MoQQmuxServer::dispatchExternallyFedSession(
    folly::AsyncTransport::UniquePtr fizzCompletedTransport,
    std::string negotiatedAlpn,
    folly::Function<void(MoQSession*)> postCreateHook) {
  auto* workerEvb = folly::EventBaseManager::get()->getExistingEventBase();
  CHECK(workerEvb && workerEvb->isInEventBaseThread())
      << "dispatchExternallyFedSession must be called on a worker EB thread";
  WorkerShutdownState* state = findStateFor(workerEvb);
  if (!state || state->draining) {
    XLOG(WARN) << "MoQQmuxServer::dispatchExternallyFedSession: rejecting "
                  "session (ALPN '"
               << negotiatedAlpn
               << "') — server is stopped or this worker has begun draining";
    return false;
  }
  ++state->inflightAccepts;
  co_withExecutor(
      workerEvb,
      handleExternallyFedSession(
          state,
          std::move(fizzCompletedTransport),
          std::move(negotiatedAlpn),
          std::move(postCreateHook)))
      .start();
  return true;
}

folly::coro::Task<void> MoQQmuxServer::handleExternallyFedSession(
    WorkerShutdownState* state,
    folly::AsyncTransport::UniquePtr fizzCompletedTransport,
    std::string negotiatedAlpn,
    folly::Function<void(MoQSession*)> postCreateHook) {
  auto* workerEvb = folly::EventBaseManager::get()->getExistingEventBase();
  CHECK(workerEvb) << "handleExternallyFedSession must run on a thread "
                      "that owns an EventBase";
  CHECK(state) << "handleExternallyFedSession: state must be non-null "
                  "(caller dispatchExternallyFedSession owns the increment)";
  SCOPE_EXIT {
    if (--state->inflightAccepts == 0 && state->draining) {
      state->done.post();
    }
  };

  co_await folly::coro::co_withCancellation(
      cancelSource_.getToken(), [&]() -> folly::coro::Task<void> {
        co_await runQmuxAndSession(
            workerEvb,
            std::move(fizzCompletedTransport),
            std::move(negotiatedAlpn),
            config_.handshakeTimeout,
            state,
            std::move(postCreateHook));
      }());
}

void MoQQmuxServer::stop() {
  CHECK(!isInWorkerPool())
      << "MoQQmuxServer::stop must not be called from a worker EB thread";
  if (!started_ || stopped_) {
    return;
  }
  stopped_ = true;
  teardown();
}

void MoQQmuxServer::teardown() {
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
  workerExecutors_.clear();
  workerEvbs_.clear();
  ownedWorkers_.clear();
}

folly::coro::Task<void> MoQQmuxServer::handleAccept(
    folly::EventBase* workerEvb,
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
        auto handshakeStart = std::chrono::steady_clock::now();

        // Step 1: Fizz handshake on the accepted socket, bounded by
        // config_.handshakeTimeout. AsyncFizzServer::accept() registers
        // the callback but arms no deadline of its own, so without an
        // outer timeout a peer that completes the TCP handshake and
        // then sends nothing would park this coroutine forever.
        FizzAcceptCb fizzCb;
        fizz::server::AsyncFizzServer::UniquePtr fizzServer(
            new fizz::server::AsyncFizzServer(
                folly::AsyncTransportWrapper::UniquePtr(std::move(asyncSocket)),
                fizzContext_));
        fizzServer->accept(&fizzCb);
        folly::EventBaseThreadTimekeeper tk{*workerEvb};
        auto fizzWaitResult = co_await folly::coro::co_awaitTry(
            folly::coro::timeout(
                [&]() -> folly::coro::Task<void> {
                  auto token =
                      co_await folly::coro::co_current_cancellation_token;
                  folly::CancellationCallback cb(
                      token, [&] { fizzCb.baton.post(); });
                  co_await fizzCb.baton;
                  co_await folly::coro::co_reschedule_on_current_executor;
                  if (token.isCancellationRequested()) {
                    co_yield folly::coro::co_stopped_may_throw;
                  }
                }(),
                config_.handshakeTimeout,
                &tk));
        if (fizzWaitResult.hasException()) {
          XLOG(WARN) << "MoQQmuxServer: Fizz handshake timed out or failed: "
                     << fizzWaitResult.exception().what();
          co_return;
        }
        if (fizzCb.exception) {
          XLOG(WARN) << "MoQQmuxServer: Fizz handshake failed: "
                     << fizzCb.exception.what();
          co_return;
        }

        // Grab the negotiated ALPN before fizzServer is moved away. The
        // MoQ draft version selected during the TLS handshake is
        // announced here as e.g. "moqt-16"; we hand it to the MoQSession
        // below so the server doesn't silently fall back to draft-14 in
        // onClientSetup.
        std::string alpn = fizzServer->getApplicationProtocol();

        // Step 2: QMUX handshake.
        auto elapsedSoFar =
            std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now() - handshakeStart);
        if (elapsedSoFar >= config_.handshakeTimeout) {
          XLOG(WARN) << "MoQQmuxServer: handshake budget exhausted by Fizz; "
                     << "rejecting connection (elapsed=" << elapsedSoFar.count()
                     << "ms, budget=" << config_.handshakeTimeout.count()
                     << "ms)";
          co_return;
        }
        auto qmuxTimeout = config_.handshakeTimeout - elapsedSoFar;

        // Delegate the post-Fizz body to the shared helper.
        co_await runQmuxAndSession(
            workerEvb,
            folly::AsyncTransport::UniquePtr(std::move(fizzServer)),
            std::move(alpn),
            qmuxTimeout,
            state);
      }());
}

folly::coro::Task<void> MoQQmuxServer::runQmuxAndSession(
    folly::EventBase* workerEvb,
    folly::AsyncTransport::UniquePtr fizzCompletedTransport,
    std::string negotiatedAlpn,
    std::chrono::milliseconds qmuxTimeout,
    WorkerShutdownState* state,
    folly::Function<void(MoQSession*)> postCreateHook) {
  if (!isLegacyAlpn(negotiatedAlpn) &&
      !getVersionFromAlpn(negotiatedAlpn).has_value()) {
    XLOG(WARN) << "MoQQmuxServer: rejecting session; "
               << (negotiatedAlpn.empty()
                       ? "no negotiated ALPN"
                       : "ALPN '" + negotiatedAlpn + "' is not a MoQ protocol");
    co_return;
  }

  auto executor = findExecutorFor(workerEvb);
  CHECK(executor)
      << "runQmuxAndSession: workerEvb is not in this server's pool";
  auto transport = std::make_unique<folly::coro::Transport>(
      workerEvb, std::move(fizzCompletedTransport));

  auto sessionResult = co_await folly::coro::co_awaitTry(
      proxygen::qmux::QmuxConnector::connect(
          workerEvb,
          proxygen::qmux::WtDir::Server,
          config_.selfTransportParams,
          std::move(transport),
          qmuxTimeout,
          config_.sessionConfig));
  if (sessionResult.hasException()) {
    XLOG(WARN) << "MoQQmuxServer: QMUX handshake failed: "
               << sessionResult.exception().what();
    co_return;
  }

  auto qmuxSession = std::move(*sessionResult);
  auto moqSession = createSession(qmuxSession, std::move(executor));
  moqSession->validateAndSetVersionFromAlpn(negotiatedAlpn);
  qmuxSession->setHandler(moqSession.get());
  qmuxSession->start(qmuxSession);

  auto* moqSessionPtr = moqSession.get();
  state->liveSessions.insert(moqSessionPtr);
  SCOPE_EXIT {
    state->liveSessions.erase(moqSessionPtr);
  };

  if (postCreateHook) {
    postCreateHook(moqSessionPtr);
  }

  // qmux loops keep a raw handler pointer and can run past session teardown, so
  // the session must outlive them and the handler must be detached first.
  SCOPE_EXIT {
    qmuxSession->setHandler(nullptr);
  };
  co_await handleClientSession(moqSession);
}

} // namespace moxygen

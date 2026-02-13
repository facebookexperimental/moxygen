/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/moqtest/interop/MoQInteropClient.h"

#include <folly/coro/BlockingWait.h>
#include <folly/coro/Collect.h>
#include <folly/coro/Sleep.h>
#include <folly/coro/Timeout.h>
#include "moxygen/MoQRelaySession.h"
#include "moxygen/MoQWebTransportClient.h"
#include "moxygen/util/InsecureVerifierDangerousDoNotUseInProduction.h"

namespace {
// Minimal TrackConsumer for subscribe calls where we don't expect data.
class NoopTrackConsumer : public moxygen::TrackConsumer {
 public:
  folly::Expected<folly::Unit, moxygen::MoQPublishError> setTrackAlias(
      moxygen::TrackAlias /*alias*/) override {
    return folly::unit;
  }

  folly::Expected<
      std::shared_ptr<moxygen::SubgroupConsumer>,
      moxygen::MoQPublishError>
  beginSubgroup(
      uint64_t /*groupID*/,
      uint64_t /*subgroupID*/,
      moxygen::Priority /*priority*/,
      bool /*containsLastInGroup*/) override {
    return folly::makeUnexpected(
        moxygen::MoQPublishError(
            moxygen::MoQPublishError::API_ERROR, "NoopTrackConsumer"));
  }

  folly::Expected<folly::SemiFuture<folly::Unit>, moxygen::MoQPublishError>
  awaitStreamCredit() override {
    return folly::makeSemiFuture(folly::unit);
  }

  folly::Expected<folly::Unit, moxygen::MoQPublishError> objectStream(
      const moxygen::ObjectHeader& /*header*/,
      moxygen::Payload /*payload*/,
      bool /*lastInGroup*/) override {
    return folly::unit;
  }

  folly::Expected<folly::Unit, moxygen::MoQPublishError> datagram(
      const moxygen::ObjectHeader& /*header*/,
      moxygen::Payload /*payload*/,
      bool /*lastInGroup*/) override {
    return folly::unit;
  }

  folly::Expected<folly::Unit, moxygen::MoQPublishError> publishDone(
      moxygen::PublishDone /*pubDone*/) override {
    return folly::unit;
  }
};

// Simple SubscriptionHandle that stores the SubscribeOk
class SimpleSubscriptionHandle : public moxygen::SubscriptionHandle {
 public:
  explicit SimpleSubscriptionHandle(moxygen::SubscribeOk ok)
      : SubscriptionHandle(std::move(ok)) {}

  void unsubscribe() override {}

  folly::coro::Task<RequestUpdateResult> requestUpdate(
      moxygen::RequestUpdate /*reqUpdate*/) override {
    co_return folly::makeUnexpected(
        moxygen::RequestError{
            moxygen::RequestID(0),
            moxygen::RequestErrorCode::NOT_SUPPORTED,
            "Not implemented"});
  }
};

// Simple Publisher that accepts all subscriptions
class SimplePublisher : public moxygen::Publisher {
 public:
  folly::coro::Task<SubscribeResult> subscribe(
      moxygen::SubscribeRequest sub,
      std::shared_ptr<moxygen::TrackConsumer> /*callback*/) override {
    moxygen::SubscribeOk ok;
    ok.requestID = sub.requestID;
    ok.expires = std::chrono::milliseconds(0);
    // GroupOrder::Default (0x0) is invalid in SUBSCRIBE_OK wire format.
    // Use OldestFirst as the resolved value when request has Default.
    ok.groupOrder = (sub.groupOrder == moxygen::GroupOrder::Default)
        ? moxygen::GroupOrder::OldestFirst
        : sub.groupOrder;
    ok.largest = std::nullopt;
    co_return std::make_shared<SimpleSubscriptionHandle>(std::move(ok));
  }
};

} // namespace

namespace moxygen {

const std::vector<std::string> kInteropAlpns = {"moqt-14", "moqt-16"};

MoQInteropClient::MoQInteropClient(
    folly::EventBase* evb,
    proxygen::URL url,
    bool useQuicTransport,
    bool tlsDisableVerify,
    std::chrono::milliseconds connectTimeout,
    std::chrono::milliseconds transactionTimeout)
    : evb_(evb),
      url_(std::move(url)),
      useQuicTransport_(useQuicTransport),
      tlsDisableVerify_(tlsDisableVerify),
      connectTimeout_(connectTimeout),
      transactionTimeout_(transactionTimeout),
      moqExecutor_(std::make_unique<MoQFollyExecutorImpl>(evb)) {}

folly::coro::Task<std::unique_ptr<MoQClient>>
MoQInteropClient::createAndSetupSession() {
  std::shared_ptr<fizz::CertificateVerifier> verifier;
  if (tlsDisableVerify_) {
    verifier =
        std::make_shared<test::InsecureVerifierDangerousDoNotUseInProduction>();
  }

  auto moqClient = useQuicTransport_
      ? std::make_unique<MoQClient>(moqExecutor_, url_, std::move(verifier))
      : std::make_unique<MoQWebTransportClient>(
            moqExecutor_, url_, std::move(verifier));

  quic::TransportSettings ts;
  ts.orderedReadCallbacks = true;

  auto totalTimeout = connectTimeout_ + transactionTimeout_;
  co_await folly::coro::timeout(
      moqClient->setupMoQSession(
          connectTimeout_,
          transactionTimeout_,
          nullptr,
          nullptr,
          ts,
          kInteropAlpns),
      totalTimeout);

  co_return std::move(moqClient);
}

folly::coro::Task<std::unique_ptr<MoQClient>>
MoQInteropClient::createAndSetupRelaySession() {
  std::shared_ptr<fizz::CertificateVerifier> verifier;
  if (tlsDisableVerify_) {
    verifier =
        std::make_shared<test::InsecureVerifierDangerousDoNotUseInProduction>();
  }

  // Use MoQRelaySession for publishNamespace support
  auto sessionFactory = MoQRelaySession::createRelaySessionFactory();

  auto moqClient = useQuicTransport_
      ? std::make_unique<MoQClient>(
            moqExecutor_, url_, sessionFactory, std::move(verifier))
      : std::make_unique<MoQWebTransportClient>(
            moqExecutor_, url_, sessionFactory, std::move(verifier));

  quic::TransportSettings ts;
  ts.orderedReadCallbacks = true;

  // Create a publisher to handle subscribe requests forwarded by the relay
  auto publishHandler = std::make_shared<SimplePublisher>();

  auto totalTimeout = connectTimeout_ + transactionTimeout_;
  co_await folly::coro::timeout(
      moqClient->setupMoQSession(
          connectTimeout_,
          transactionTimeout_,
          publishHandler,
          nullptr,
          ts,
          kInteropAlpns),
      totalTimeout);

  co_return std::move(moqClient);
}

folly::coro::Task<InteropTestResult> MoQInteropClient::testSetupOnly() {
  InteropTestResult result;
  result.testName = "setup-only";
  auto start = std::chrono::steady_clock::now();

  try {
    std::shared_ptr<fizz::CertificateVerifier> verifier;
    if (tlsDisableVerify_) {
      verifier = std::make_shared<
          test::InsecureVerifierDangerousDoNotUseInProduction>();
    }

    auto moqClient = useQuicTransport_
        ? std::make_unique<MoQClient>(moqExecutor_, url_, std::move(verifier))
        : std::make_unique<MoQWebTransportClient>(
              moqExecutor_, url_, std::move(verifier));

    quic::TransportSettings ts;
    ts.orderedReadCallbacks = true;

    // Wrap setupMoQSession in a timeout to prevent indefinite hangs
    auto totalTimeout = connectTimeout_ + transactionTimeout_;
    co_await folly::coro::timeout(
        moqClient->setupMoQSession(
            connectTimeout_,
            transactionTimeout_,
            nullptr, // no publish handler
            nullptr, // no subscribe handler
            ts,
            kInteropAlpns),
        totalTimeout);

    // SETUP exchange completed successfully - close gracefully
    if (moqClient->moqSession_) {
      moqClient->moqSession_->close(SessionCloseErrorCode::NO_ERROR);
    }

    auto end = std::chrono::steady_clock::now();
    result.passed = true;
    result.duration =
        std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    result.message = "SETUP exchange completed successfully";
  } catch (const folly::FutureTimeout&) {
    auto end = std::chrono::steady_clock::now();
    result.passed = false;
    result.duration =
        std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    result.message = "Timed out waiting for SETUP exchange";
  } catch (const std::exception& ex) {
    auto end = std::chrono::steady_clock::now();
    result.passed = false;
    result.duration =
        std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    result.message = ex.what();
  }

  co_return result;
}

folly::coro::Task<InteropTestResult> MoQInteropClient::testAnnounceOnly() {
  InteropTestResult result;
  result.testName = "announce-only";
  auto start = std::chrono::steady_clock::now();

  try {
    auto moqClient = co_await createAndSetupRelaySession();

    PublishNamespace pn;
    pn.trackNamespace = TrackNamespace({"moq-test"}, {"interop"});

    auto annResult = co_await folly::coro::timeout(
        moqClient->moqSession_->publishNamespace(std::move(pn)),
        transactionTimeout_);

    if (annResult.hasValue()) {
      result.passed = true;
      result.message = "PUBLISH_NAMESPACE completed successfully";
    } else {
      result.passed = false;
      result.message = annResult.error().reasonPhrase;
    }

    if (moqClient->moqSession_) {
      moqClient->moqSession_->close(SessionCloseErrorCode::NO_ERROR);
    }
  } catch (const folly::FutureTimeout&) {
    result.passed = false;
    result.message = "Timed out waiting for PUBLISH_NAMESPACE response";
  } catch (const std::exception& ex) {
    result.passed = false;
    result.message = ex.what();
  }

  auto end = std::chrono::steady_clock::now();
  result.duration =
      std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
  co_return result;
}

folly::coro::Task<InteropTestResult> MoQInteropClient::testSubscribeError() {
  InteropTestResult result;
  result.testName = "subscribe-error";
  auto start = std::chrono::steady_clock::now();

  try {
    auto moqClient = co_await createAndSetupSession();

    FullTrackName ftn{TrackNamespace({"nonexistent"}), "no-such-track"};
    auto sub = SubscribeRequest::make(ftn);
    auto consumer = std::make_shared<NoopTrackConsumer>();

    auto subResult = co_await folly::coro::timeout(
        moqClient->moqSession_->subscribe(std::move(sub), consumer),
        transactionTimeout_);

    if (subResult.hasError()) {
      result.passed = true;
      result.message = "Subscribe correctly returned error: " +
          subResult.error().reasonPhrase;
    } else {
      // We expected an error but got success.
      // Subscribing to a non-existent track should fail.
      result.passed = false;
      result.message =
          "Subscribe succeeded (relay accepted non-existent track)";
      subResult.value()->unsubscribe();
    }

    if (moqClient->moqSession_) {
      moqClient->moqSession_->close(SessionCloseErrorCode::NO_ERROR);
    }
  } catch (const folly::FutureTimeout&) {
    result.passed = false;
    result.message = "Timed out waiting for SUBSCRIBE response";
  } catch (const std::exception& ex) {
    result.passed = false;
    result.message = ex.what();
  }

  auto end = std::chrono::steady_clock::now();
  result.duration =
      std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
  co_return result;
}

folly::coro::Task<InteropTestResult> MoQInteropClient::testAnnounceSubscribe() {
  InteropTestResult result;
  result.testName = "announce-subscribe";
  auto start = std::chrono::steady_clock::now();

  std::unique_ptr<MoQClient> publisher;
  std::unique_ptr<MoQClient> subscriber;
  // Keep the publish namespace handle alive until test completes
  std::shared_ptr<Subscriber::PublishNamespaceHandle> publishNamespaceHandle;

  try {
    // Set up publisher and announce
    publisher = co_await createAndSetupRelaySession();

    PublishNamespace pn;
    pn.trackNamespace = TrackNamespace({"moq-interop-test"});

    auto annResult = co_await folly::coro::timeout(
        publisher->moqSession_->publishNamespace(std::move(pn)),
        transactionTimeout_);

    if (annResult.hasError()) {
      result.passed = false;
      result.message =
          "PUBLISH_NAMESPACE failed: " + annResult.error().reasonPhrase;
      if (publisher->moqSession_) {
        publisher->moqSession_->close(SessionCloseErrorCode::NO_ERROR);
      }
      auto end = std::chrono::steady_clock::now();
      result.duration =
          std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
      co_return result;
    }

    // Store handle to keep the publish namespace active
    publishNamespaceHandle = std::move(annResult.value());

    // Set up subscriber and subscribe
    subscriber = co_await createAndSetupSession();

    FullTrackName ftn{TrackNamespace({"moq-interop-test"}), "interop-track"};
    auto sub = SubscribeRequest::make(ftn);
    auto consumer = std::make_shared<NoopTrackConsumer>();

    auto subResult = co_await folly::coro::timeout(
        subscriber->moqSession_->subscribe(std::move(sub), consumer),
        transactionTimeout_);

    if (subResult.hasValue()) {
      result.passed = true;
      result.message =
          "Publisher announced, subscriber subscribed successfully";
      subResult.value()->unsubscribe();
    } else {
      result.passed = false;
      result.message = "Subscribe failed: " + subResult.error().reasonPhrase;
    }
  } catch (const folly::FutureTimeout&) {
    result.passed = false;
    result.message = "Timed out during announce-subscribe test";
  } catch (const std::exception& ex) {
    result.passed = false;
    result.message = ex.what();
  }

  if (publishNamespaceHandle) {
    publishNamespaceHandle->publishNamespaceDone();
    publishNamespaceHandle.reset();
  }
  if (subscriber && subscriber->moqSession_) {
    subscriber->moqSession_->close(SessionCloseErrorCode::NO_ERROR);
  }
  if (publisher && publisher->moqSession_) {
    publisher->moqSession_->close(SessionCloseErrorCode::NO_ERROR);
  }

  auto end = std::chrono::steady_clock::now();
  result.duration =
      std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
  co_return result;
}

folly::coro::Task<InteropTestResult>
MoQInteropClient::testPublishNamespaceDone() {
  InteropTestResult result;
  result.testName = "publish-namespace-done";
  auto start = std::chrono::steady_clock::now();

  try {
    auto moqClient = co_await createAndSetupRelaySession();

    PublishNamespace pn;
    pn.trackNamespace =
        TrackNamespace(std::vector<std::string>{"moq-test", "interop"});

    auto annResult = co_await folly::coro::timeout(
        moqClient->moqSession_->publishNamespace(std::move(pn)),
        transactionTimeout_);

    if (annResult.hasError()) {
      result.passed = false;
      result.message =
          "PUBLISH_NAMESPACE failed: " + annResult.error().reasonPhrase;
    } else {
      // PUBLISH_NAMESPACE_OK received, now send PUBLISH_NAMESPACE_DONE
      annResult.value()->publishNamespaceDone();
      result.passed = true;
      result.message =
          "PUBLISH_NAMESPACE_OK received, PUBLISH_NAMESPACE_DONE sent";
    }

    if (moqClient->moqSession_) {
      moqClient->moqSession_->close(SessionCloseErrorCode::NO_ERROR);
    }
  } catch (const folly::FutureTimeout&) {
    result.passed = false;
    result.message = "Timed out waiting for PUBLISH_NAMESPACE response";
  } catch (const std::exception& ex) {
    result.passed = false;
    result.message = ex.what();
  }

  auto end = std::chrono::steady_clock::now();
  result.duration =
      std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
  co_return result;
}

folly::coro::Task<InteropTestResult>
MoQInteropClient::testSubscribeBeforeAnnounce() {
  InteropTestResult result;
  result.testName = "subscribe-before-announce";
  auto start = std::chrono::steady_clock::now();

  std::unique_ptr<MoQClient> subscriber;
  std::unique_ptr<MoQClient> publisher;
  std::shared_ptr<Subscriber::PublishNamespaceHandle> publishNamespaceHandle;

  try {
    // Step 1: Connect subscriber first
    subscriber = co_await createAndSetupSession();

    FullTrackName ftn{
        TrackNamespace(std::vector<std::string>{"moq-test", "interop"}),
        "test-track"};
    auto sub = SubscribeRequest::make(ftn);
    auto consumer = std::make_shared<NoopTrackConsumer>();

    // Step 2: Run subscribe and delayed publisher announce concurrently.
    // Publisher starts 500ms after subscriber's subscribe is sent.
    auto overallTimeout =
        std::chrono::milliseconds(500) + connectTimeout_ + transactionTimeout_;

    auto [subTry, pubTry] = co_await folly::coro::timeout(
        folly::coro::collectAllTry(
            subscriber->moqSession_->subscribe(std::move(sub), consumer),
            [this, &publisher, &publishNamespaceHandle]()
                -> folly::coro::Task<void> {
              co_await folly::coro::sleep(std::chrono::milliseconds(500));
              publisher = co_await createAndSetupRelaySession();
              PublishNamespace pn;
              pn.trackNamespace = TrackNamespace(
                  std::vector<std::string>{"moq-test", "interop"});
              auto annResult =
                  co_await publisher->moqSession_->publishNamespace(
                      std::move(pn));
              if (annResult.hasValue()) {
                publishNamespaceHandle = std::move(annResult.value());
              } else {
                throw std::runtime_error(
                    "PUBLISH_NAMESPACE failed: " +
                    annResult.error().reasonPhrase);
              }
            }()),
        overallTimeout);

    if (pubTry.hasException()) {
      result.passed = false;
      result.message = "Publisher failed: " +
          std::string(pubTry.exception().what().toStdString());
    } else if (subTry.hasException()) {
      result.passed = false;
      result.message = "Subscribe threw exception: " +
          std::string(subTry.exception().what().toStdString());
    } else {
      auto& subResult = subTry.value();
      if (subResult.hasValue()) {
        result.passed = true;
        result.message =
            "Subscribe succeeded after publisher announced"
            " (relay buffers pending subscriptions)";
        subResult.value()->unsubscribe();
      } else {
        // Subscribe error is also valid per the spec
        result.passed = true;
        result.message =
            "Subscribe returned error (relay doesn't buffer pending"
            " subscriptions): " +
            subResult.error().reasonPhrase;
      }
    }
  } catch (const folly::FutureTimeout&) {
    result.passed = false;
    result.message = "Timed out during subscribe-before-announce test";
  } catch (const std::exception& ex) {
    result.passed = false;
    result.message = ex.what();
  }

  if (publishNamespaceHandle) {
    publishNamespaceHandle->publishNamespaceDone();
    publishNamespaceHandle.reset();
  }
  if (subscriber && subscriber->moqSession_) {
    subscriber->moqSession_->close(SessionCloseErrorCode::NO_ERROR);
  }
  if (publisher && publisher->moqSession_) {
    publisher->moqSession_->close(SessionCloseErrorCode::NO_ERROR);
  }

  auto end = std::chrono::steady_clock::now();
  result.duration =
      std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
  co_return result;
}

} // namespace moxygen

/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <folly/coro/Sleep.h>
#include <folly/init/Init.h>
#include <folly/io/async/EventBase.h>
#include <folly/portability/GFlags.h>
#include <moxygen/MoQClient.h>
#include <moxygen/MoQWebTransportClient.h>
#include <moxygen/relay/MoQRelayClient.h>
#include <moxygen/util/InsecureVerifierDangerousDoNotUseInProduction.h>

DEFINE_string(connect_url, "", "URL for webtransport server");
DEFINE_string(track_namespace, "", "Track Namespace");
DEFINE_string(track_namespace_delimiter, "/", "Track Namespace Delimiter");
DEFINE_string(track_name, "", "Track Name");
DEFINE_string(message, "hello", "Message content to send");
DEFINE_int32(count, 1, "Number of messages to send");
DEFINE_int32(interval_ms, 100, "Interval between messages in milliseconds");
DEFINE_int32(connect_timeout, 1000, "Connect timeout (ms)");
DEFINE_int32(transaction_timeout, 120, "Transaction timeout (s)");
DEFINE_bool(quic_transport, false, "Use raw QUIC transport");
DEFINE_bool(
    insecure,
    false,
    "Use insecure verifier (skip certificate validation)");
DEFINE_bool(
    use_legacy_setup,
    false,
    "If true, use only moq-00 ALPN (legacy). If false, use latest draft ALPN with fallback to legacy");

namespace {
using namespace moxygen;

// Simple SubscriptionHandle for the publish request
class SimpleSubscriptionHandle : public SubscriptionHandle {
 public:
  SimpleSubscriptionHandle() = default;

  void unsubscribe() override {
    XLOG(INFO) << "unsubscribe called";
  }

  folly::coro::Task<RequestUpdateResult> requestUpdate(RequestUpdate) override {
    co_return folly::makeUnexpected(
        RequestError{
            RequestID(0), RequestErrorCode::INTERNAL_ERROR, "Not supported"});
  }
};

// Simple publisher that sends PUBLISH message to server
class MoQSimplePublisher
    : public Subscriber,
      public std::enable_shared_from_this<MoQSimplePublisher> {
 public:
  MoQSimplePublisher(
      std::shared_ptr<MoQFollyExecutorImpl> evb,
      proxygen::URL url,
      TrackNamespace trackNamespace,
      std::string trackName,
      std::string message,
      int count,
      int intervalMs,
      std::shared_ptr<fizz::CertificateVerifier> verifier = nullptr)
      : moqClient_(
            FLAGS_quic_transport
                ? std::make_unique<MoQClient>(
                      evb,
                      std::move(url),
                      MoQRelaySession::createRelaySessionFactory(),
                      verifier)
                : std::make_unique<MoQWebTransportClient>(
                      evb,
                      std::move(url),
                      MoQRelaySession::createRelaySessionFactory(),
                      verifier)),
        trackNamespace_(std::move(trackNamespace)),
        trackName_(std::move(trackName)),
        message_(std::move(message)),
        count_(count),
        intervalMs_(intervalMs) {}

  folly::coro::Task<void> run() noexcept {
    XLOG(INFO) << __func__;
    auto g =
        folly::makeGuard([func = __func__] { XLOG(INFO) << "exit " << func; });
    try {
      // Default to experimental protocols, override to legacy if flag set
      std::vector<std::string> alpns =
          getDefaultMoqtProtocols(!FLAGS_use_legacy_setup);
      // Register as subscriber (to receive PUBLISH response)
      co_await moqClient_.setup(
          /*publisher=*/nullptr,
          /*subscriber=*/shared_from_this(),
          std::chrono::milliseconds(FLAGS_connect_timeout),
          std::chrono::seconds(FLAGS_transaction_timeout),
          quic::TransportSettings(),
          alpns);

      // Create the PUBLISH request
      PublishRequest pubReq;
      pubReq.fullTrackName = FullTrackName({trackNamespace_, trackName_});
      pubReq.groupOrder = GroupOrder::OldestFirst;

      // Create a subscription handle
      auto subHandle = std::make_shared<SimpleSubscriptionHandle>();

      XLOG(INFO) << "Sending PUBLISH request for: " << trackNamespace_ << "/"
                 << trackName_;

      // Send PUBLISH to server
      auto publishResult = moqClient_.getSession()->publish(pubReq, subHandle);
      if (publishResult.hasError()) {
        XLOG(ERR) << "PUBLISH failed: " << publishResult.error().reasonPhrase;
        moqClient_.shutdown();
        co_return;
      }

      // Get the consumer from the result
      auto& pubResponse = publishResult.value();
      publisher_ = pubResponse.consumer;

      // Wait for PUBLISH_OK
      auto pubOkResult = co_await std::move(pubResponse.reply);
      if (pubOkResult.hasError()) {
        XLOG(ERR) << "PUBLISH_OK error: " << pubOkResult.error().reasonPhrase;
        publisher_.reset();
        moqClient_.shutdown();
        co_return;
      }

      XLOG(INFO) << "PUBLISH_OK received, sending " << count_ << " messages";

      // Send messages
      for (int i = 0; i < count_; i++) {
        auto res = publisher_->objectStream(
            ObjectHeader{
                nextGroup_++,
                /*subgroupIn=*/0,
                /*idIn=*/0,
                /*priorityIn=*/kDefaultPriority,
                ObjectStatus::NORMAL},
            folly::IOBuf::copyBuffer(message_));
        if (res.hasError()) {
          XLOG(ERR) << "objectStream failed: " << res.error().describe();
          break;
        }
        XLOG(INFO) << "Sent message " << (i + 1) << "/" << count_ << ": "
                   << message_;
        if (i < count_ - 1 && intervalMs_ > 0) {
          co_await folly::coro::sleep(std::chrono::milliseconds(intervalMs_));
        }
      }

      // Send publishDone to signal end
      XLOG(INFO) << "Sending publishDone";
      publisher_->publishDone(
          PublishDone{
              pubOkResult.value().requestID,
              PublishDoneStatusCode::TRACK_ENDED,
              0,
              "Track ended"});

      // Give time for the data to be sent
      co_await folly::coro::sleep(std::chrono::milliseconds(100));

      // Cleanup
      publisher_.reset();
      moqClient_.getSession()->close(SessionCloseErrorCode::NO_ERROR);
      moqClient_.shutdown();

    } catch (const std::exception& ex) {
      XLOG(ERR) << folly::exceptionStr(ex);
      publisher_.reset();
      moqClient_.shutdown();
      co_return;
    }
    XLOG(INFO) << __func__ << " done";
  }

  // ---- Subscriber interface ----

  // Called when server publishes namespace to us (we don't care)
  folly::coro::Task<PublishNamespaceResult> publishNamespace(
      PublishNamespace ann,
      std::shared_ptr<PublishNamespaceCallback>) override {
    XLOG(INFO) << "Received PublishNamespace ns=" << ann.trackNamespace;
    co_return std::make_shared<PublishNamespaceHandle>(PublishNamespaceOk{
        .requestID = ann.requestID, .requestSpecificParams = {}});
  }

  void goaway(Goaway goaway) override {
    XLOG(INFO) << "Goaway uri=" << goaway.newSessionUri;
    moqClient_.shutdown();
  }

  MoQRelayClient moqClient_;
  TrackNamespace trackNamespace_;
  std::string trackName_;
  std::string message_;
  int count_;
  int intervalMs_;
  uint64_t nextGroup_{0};
  std::shared_ptr<TrackConsumer> publisher_;
};

} // namespace

using namespace moxygen;

int main(int argc, char* argv[]) {
  folly::Init init(&argc, &argv, false);
  folly::EventBase eventBase;
  proxygen::URL url(FLAGS_connect_url);

  if (!url.isValid() || !url.hasHost()) {
    XLOG(ERR) << "Invalid url: " << FLAGS_connect_url;
    return 1;
  }

  if (FLAGS_track_namespace.empty() || FLAGS_track_name.empty()) {
    XLOG(ERR) << "track_namespace and track_name are required";
    return 1;
  }

  TrackNamespace ns =
      TrackNamespace(FLAGS_track_namespace, FLAGS_track_namespace_delimiter);
  std::shared_ptr<MoQFollyExecutorImpl> moqEvb =
      std::make_shared<MoQFollyExecutorImpl>(&eventBase);
  std::shared_ptr<fizz::CertificateVerifier> verifier = nullptr;
  if (FLAGS_insecure) {
    verifier = std::make_shared<
        moxygen::test::InsecureVerifierDangerousDoNotUseInProduction>();
  }

  auto publisher = std::make_shared<MoQSimplePublisher>(
      moqEvb,
      std::move(url),
      std::move(ns),
      FLAGS_track_name,
      FLAGS_message,
      FLAGS_count,
      FLAGS_interval_ms,
      verifier);

  co_withExecutor(&eventBase, publisher->run())
      .start()
      .via(&eventBase)
      .thenTry([](const auto&) {});
  eventBase.loop();
  return 0;
}

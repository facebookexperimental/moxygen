// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include <folly/coro/Sleep.h>
#include <folly/io/async/EventBase.h>
#include <moxygen/MoQClient.h>
#include <moxygen/MoQWebTransportClient.h>
#include <moxygen/relay/MoQRelayClient.h>
#include <moxygen/util/InsecureVerifierDangerousDoNotUseInProduction.h>

#include <iostream>
#include <memory>
#include <string>

namespace facebook::moxygen {

using namespace ::moxygen;

namespace {
struct MoQPublisherOptions {
  // Required
  std::string connectUrl;
  std::string trackNamespace;
  std::string trackName;

  // Optional
  std::string trackNamespaceDelimiter = "/";
  std::string message = "hello";
  int32_t count = 1;
  int32_t intervalMs = 100;
  int32_t connectTimeout = 1000;
  int32_t transactionTimeout = 120;
  bool quicTransport = false;
  bool useLegacySetup = false;
  bool insecure = false;
};

// Custom exception for MoQ publisher errors
class MoQPublisherError : public std::exception {
 public:
  MoQPublisherError(int code, std::string message)
      : code_(code), message_(std::move(message)) {}

  int code() const {
    return code_;
  }
  const std::string& message() const {
    return message_;
  }
  const char* what() const noexcept override {
    return message_.c_str();
  }

 private:
  int code_;
  std::string message_;
};

// Helper function to get error message from code
std::string getPublisherErrorMessage(int code) {
  switch (code) {
    case -1:
      return "Invalid URL";
    case -2:
      return "Exception occurred during execution";
    case -3:
      return "PUBLISH failed";
    case -4:
      return "PUBLISH_OK error";
    case -5:
      return "objectStream failed";
    case -6:
      return "Other exception occurred";
    default:
      return "Unknown error";
  }
}

// Simple SubscriptionHandle for the publish request
class SimpleSubscriptionHandle : public SubscriptionHandle {
 public:
  SimpleSubscriptionHandle() = default;

  void unsubscribe() override {}

  folly::coro::Task<RequestUpdateResult> requestUpdate(RequestUpdate) override {
    co_return folly::makeUnexpected(
        RequestError{
            RequestID(0), RequestErrorCode::INTERNAL_ERROR, "Not supported"});
  }
};

// Publisher implementation - sends PUBLISH message to server
class PyMoQPublisher : public Subscriber,
                       public std::enable_shared_from_this<PyMoQPublisher> {
 public:
  PyMoQPublisher(
      std::shared_ptr<MoQFollyExecutorImpl> evb,
      proxygen::URL url,
      TrackNamespace trackNamespace,
      std::string trackName,
      const MoQPublisherOptions& options,
      std::shared_ptr<fizz::CertificateVerifier> verifier = nullptr)
      : moqClient_(
            options.quicTransport
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
        options_(options) {}

  folly::coro::Task<int> run() noexcept {
    try {
      std::vector<std::string> alpns =
          getDefaultMoqtProtocols(!options_.useLegacySetup);
      co_await moqClient_.setup(
          /*publisher=*/nullptr,
          /*subscriber=*/shared_from_this(),
          std::chrono::milliseconds(options_.connectTimeout),
          std::chrono::seconds(options_.transactionTimeout),
          quic::TransportSettings(),
          alpns);

      // Create the PUBLISH request
      PublishRequest pubReq;
      pubReq.fullTrackName = FullTrackName({trackNamespace_, trackName_});
      pubReq.groupOrder = GroupOrder::OldestFirst;

      auto subHandle = std::make_shared<SimpleSubscriptionHandle>();

      std::cout << "Sending PUBLISH request for: " << trackNamespace_ << "/"
                << trackName_ << std::endl;

      // Send PUBLISH to server
      auto publishResult = moqClient_.getSession()->publish(pubReq, subHandle);
      if (publishResult.hasError()) {
        std::cerr << "PUBLISH failed: " << publishResult.error().reasonPhrase
                  << std::endl;
        moqClient_.shutdown();
        co_return -3;
      }

      auto& pubResponse = publishResult.value();
      publisher_ = pubResponse.consumer;

      // Wait for PUBLISH_OK
      auto pubOkResult = co_await std::move(pubResponse.reply);
      if (pubOkResult.hasError()) {
        std::cerr << "PUBLISH_OK error: " << pubOkResult.error().reasonPhrase
                  << std::endl;
        publisher_.reset();
        moqClient_.shutdown();
        co_return -4;
      }

      std::cout << "PUBLISH_OK received, sending " << options_.count
                << " messages" << std::endl;

      // Send messages
      for (int i = 0; i < options_.count; i++) {
        auto res = publisher_->objectStream(
            ObjectHeader{
                nextGroup_++,
                /*subgroupIn=*/0,
                /*idIn=*/0,
                /*priorityIn=*/kDefaultPriority,
                ObjectStatus::NORMAL},
            folly::IOBuf::copyBuffer(options_.message));
        if (res.hasError()) {
          std::cerr << "objectStream failed: " << res.error().describe()
                    << std::endl;
          publisher_.reset();
          moqClient_.shutdown();
          co_return -5;
        }
        std::cout << "Sent message " << (i + 1) << "/" << options_.count
                  << std::endl;
        if (i < options_.count - 1 && options_.intervalMs > 0) {
          co_await folly::coro::sleep(
              std::chrono::milliseconds(options_.intervalMs));
        }
      }

      // Send publishDone
      std::cout << "Sending publishDone" << std::endl;
      publisher_->publishDone(
          PublishDone{
              pubOkResult.value().requestID,
              PublishDoneStatusCode::TRACK_ENDED,
              0,
              "Track ended"});

      co_await folly::coro::sleep(std::chrono::milliseconds(100));

      publisher_.reset();
      moqClient_.getSession()->close(SessionCloseErrorCode::NO_ERROR);
      moqClient_.shutdown();

      co_return 0;
    } catch (const std::exception& ex) {
      std::cerr << "Exception: " << ex.what() << std::endl;
      publisher_.reset();
      moqClient_.shutdown();
      co_return -6;
    }
  }

  folly::coro::Task<PublishNamespaceResult> publishNamespace(
      PublishNamespace ann,
      std::shared_ptr<PublishNamespaceCallback>) override {
    co_return std::make_shared<PublishNamespaceHandle>(PublishNamespaceOk{
        .requestID = ann.requestID, .requestSpecificParams = {}});
  }

  void goaway(Goaway) override {
    moqClient_.shutdown();
  }

  MoQRelayClient moqClient_;
  TrackNamespace trackNamespace_;
  std::string trackName_;
  MoQPublisherOptions options_;
  uint64_t nextGroup_{0};
  std::shared_ptr<TrackConsumer> publisher_;
};

// Python wrapper class
class PyMoQSimplePublisher {
 public:
  explicit PyMoQSimplePublisher(MoQPublisherOptions options)
      : options_(std::move(options)) {}

  void run() {
    int result = runInternal();
    if (result != 0) {
      throw MoQPublisherError(result, getPublisherErrorMessage(result));
    }
  }

 private:
  int runInternal() {
    try {
      folly::EventBase eventBase;
      proxygen::URL parsedUrl(options_.connectUrl);

      if (!parsedUrl.isValid() || !parsedUrl.hasHost()) {
        std::cerr << "Invalid URL" << std::endl;
        return -1;
      }

      TrackNamespace ns = TrackNamespace(
          options_.trackNamespace, options_.trackNamespaceDelimiter);
      auto moqEvb = std::make_shared<MoQFollyExecutorImpl>(&eventBase);

      std::shared_ptr<fizz::CertificateVerifier> verifier = nullptr;
      if (options_.insecure) {
        verifier = std::make_shared<
            moxygen::test::InsecureVerifierDangerousDoNotUseInProduction>();
      }

      auto publisher = std::make_shared<PyMoQPublisher>(
          moqEvb,
          std::move(parsedUrl),
          std::move(ns),
          options_.trackName,
          options_,
          verifier);

      auto resultPtr = std::make_shared<int>(-999);

      co_withExecutor(&eventBase, publisher->run())
          .start()
          .via(&eventBase)
          .thenValue([resultPtr, &eventBase](int res) {
            *resultPtr = res;
            eventBase.terminateLoopSoon();
          })
          .thenError(
              [resultPtr, &eventBase](const folly::exception_wrapper& ew) {
                std::cerr << "Error: " << ew.what() << std::endl;
                *resultPtr = -2;
                eventBase.terminateLoopSoon();
              });

      eventBase.loop();

      return *resultPtr;
    } catch (const MoQPublisherError& e) {
      return e.code();
    } catch (const std::exception& ex) {
      std::cerr << "Exception in runInternal: " << ex.what() << std::endl;
      return -2;
    }
  }

  MoQPublisherOptions options_;
};

} // namespace

// Python bindings
PYBIND11_MODULE(moq_simple_publisher_pybinding, m) {
  m.doc() = "MoQ Simple Publisher Python Bindings";

  // Register custom exception
  // NOLINTNEXTLINE(facebook-hte-ContextDependentStaticInit)
  static pybind11::exception<MoQPublisherError> exc(m, "MoQPublisherError");
  pybind11::register_exception_translator([](std::exception_ptr p) {
    try {
      if (p) {
        std::rethrow_exception(p);
      }
    } catch (const MoQPublisherError& e) {
      PyErr_SetString(
          exc.ptr(), (std::to_string(e.code()) + ": " + e.message()).c_str());
    }
  });

  pybind11::class_<PyMoQSimplePublisher>(m, "PyMoQSimplePublisher")
      .def(pybind11::init([](const pybind11::kwargs& kwargs) {
        MoQPublisherOptions options;

        // Required params
        if (!kwargs.contains("connect_url") ||
            !kwargs.contains("track_namespace") ||
            !kwargs.contains("track_name")) {
          throw std::invalid_argument(
              "connect_url, track_namespace, and track_name are required");
        }
        options.connectUrl = kwargs["connect_url"].cast<std::string>();
        options.trackNamespace = kwargs["track_namespace"].cast<std::string>();
        options.trackName = kwargs["track_name"].cast<std::string>();

        // Optional params
        if (kwargs.contains("track_namespace_delimiter")) {
          options.trackNamespaceDelimiter =
              kwargs["track_namespace_delimiter"].cast<std::string>();
        }
        if (kwargs.contains("message")) {
          options.message = kwargs["message"].cast<std::string>();
        }
        if (kwargs.contains("count")) {
          options.count = kwargs["count"].cast<int32_t>();
        }
        if (kwargs.contains("interval_ms")) {
          options.intervalMs = kwargs["interval_ms"].cast<int32_t>();
        }
        if (kwargs.contains("connect_timeout")) {
          options.connectTimeout = kwargs["connect_timeout"].cast<int32_t>();
        }
        if (kwargs.contains("transaction_timeout")) {
          options.transactionTimeout =
              kwargs["transaction_timeout"].cast<int32_t>();
        }
        if (kwargs.contains("quic_transport")) {
          options.quicTransport = kwargs["quic_transport"].cast<bool>();
        }
        if (kwargs.contains("use_legacy_setup")) {
          options.useLegacySetup = kwargs["use_legacy_setup"].cast<bool>();
        }
        if (kwargs.contains("insecure")) {
          options.insecure = kwargs["insecure"].cast<bool>();
        }

        return std::make_unique<PyMoQSimplePublisher>(std::move(options));
      }))
      .def(
          "run",
          &PyMoQSimplePublisher::run,
          "Run the publisher. Blocks until all messages are sent or error occurs.");
}

} // namespace facebook::moxygen

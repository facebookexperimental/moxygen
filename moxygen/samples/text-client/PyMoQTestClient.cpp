// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include <folly/coro/Sleep.h>
#include <folly/io/async/EventBase.h>
#include <proxygen/lib/http/HTTPCommonHeaders.h>
#include <moxygen/MoQClient.h>
#include <moxygen/MoQWebTransportClient.h>
#include <moxygen/ObjectReceiver.h>
#include <moxygen/relay/MoQRelayClient.h>
#include <moxygen/util/InsecureVerifierDangerousDoNotUseInProduction.h>

#include <iostream>
#include <memory>
#include <string>

namespace facebook::moxygen {

using namespace ::moxygen;

// Options struct - 1:1 mapping to C++ FLAGS
struct MoQClientOptions {
  // Required (maps to --connect_url, --track_namespace, --track_name)
  std::string connectUrl;
  std::string trackNamespace;
  std::string trackName;

  // Maps to --track_namespace_delimiter
  std::string trackNamespaceDelimiter = "/";

  // Maps to --sg, --so, --eg (using string to match C++, empty means unset)
  std::string sg; // start group
  std::string so; // start object
  std::string eg; // end group

  // Maps to --connect_timeout, --transaction_timeout
  int32_t connectTimeout = 1000;
  int32_t transactionTimeout = 120;

  // Maps to --quic_transport, --use_legacy_setup, --insecure
  bool quicTransport = false;
  bool useLegacySetup = false;
  bool insecure = false;

  // Maps to --forward, --delivery_timeout
  bool forward = true;
  uint64_t deliveryTimeout = 0;

  // Maps to --fetch, --jrfetch, --jafetch, --join_start (mutually exclusive)
  bool fetch = false;
  bool jrfetch = false; // joining relative fetch
  bool jafetch = false; // joining absolute fetch
  int32_t joinStart = 0;

  // Maps to --publish, --unsubscribe, --unsubscribe_time
  bool publish = false;
  bool unsubscribe = false;
  uint64_t unsubscribeTime = 30;

  // Maps to --mlog_path
  std::string mlogPath;
};

// Custom exception for MoQ client errors
class MoQClientError : public std::exception {
 public:
  MoQClientError(int code, std::string message)
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
std::string getErrorMessage(int code) {
  switch (code) {
    case -1:
      return "Invalid URL";
    case -2:
      return "Exception occurred during execution";
    case -3:
      return "Fetch failed - server returned FetchError";
    case -4:
      return "Subscribe failed - server returned SubscribeError";
    case -5:
      return "Stream error received";
    case -6:
      return "Other exception occurred";
    case -7:
      return "SubscribeNamespace failed";
    default:
      return "Unknown error";
  }
}

// Helper function to convert RequestErrorCode to string
const char* requestErrorCodeToString(RequestErrorCode code) {
  switch (code) {
    case RequestErrorCode::INTERNAL_ERROR:
      return "INTERNAL_ERROR";
    case RequestErrorCode::UNAUTHORIZED:
      return "UNAUTHORIZED";
    case RequestErrorCode::TIMEOUT:
      return "TIMEOUT";
    case RequestErrorCode::NOT_SUPPORTED:
      return "NOT_SUPPORTED";
    case RequestErrorCode::TRACK_NOT_EXIST:
      return "TRACK_NOT_EXIST/NAMESPACE_PREFIX_UNKNOWN/UNINTERESTED";
    case RequestErrorCode::INVALID_RANGE:
      return "INVALID_RANGE";
    case RequestErrorCode::CANCELLED:
      return "CANCELLED";
    case RequestErrorCode::GOING_AWAY:
      return "GOING_AWAY";
    default:
      return "UNKNOWN";
  }
}

// Helper struct to parse location params (similar to flags2params in C++)
struct SubParams {
  LocationType locType{};
  std::optional<AbsoluteLocation> start;
  uint64_t endGroup{};
};

SubParams parseLocationParams(const MoQClientOptions& options) {
  SubParams result;
  std::string soStr = options.so;

  if (options.sg.empty()) {
    if (soStr.empty()) {
      result.locType = LocationType::LargestObject;
      return result;
    } else {
      throw MoQClientError(-1, "Invalid: sg blank, so=" + soStr);
    }
  } else if (soStr.empty()) {
    soStr = "0";
  }

  if (options.jafetch || options.jrfetch) {
    throw MoQClientError(-1, "Joining fetch requires empty sg");
  }

  result.start.emplace(
      folly::to<uint64_t>(options.sg), folly::to<uint64_t>(soStr));

  if (options.eg.empty()) {
    result.locType = LocationType::AbsoluteStart;
    return result;
  } else {
    result.locType = LocationType::AbsoluteRange;
    result.endGroup = folly::to<uint64_t>(options.eg);
    return result;
  }
}

// Simple handler for Python binding
class PyObjectHandler : public ObjectReceiverCallback {
 public:
  explicit PyObjectHandler(bool isFetch) : isFetch_(isFetch) {}
  ~PyObjectHandler() override = default;

  FlowControlState onObject(
      std::optional<TrackAlias> /*trackAlias*/,
      const ObjectHeader& header,
      Payload payload) override {
    objectCount_++;
    if (payload) {
      bytesReceived_ += payload->computeChainDataLength();
    }
    std::cout << "Object: group=" << header.group << " id=" << header.id;
    if (header.length.has_value()) {
      std::cout << " length=" << header.length.value();
    }
    std::cout << " status=" << folly::to_underlying(header.status) << std::endl;
    return FlowControlState::UNBLOCKED;
  }

  void onObjectStatus(
      std::optional<TrackAlias> /*trackAlias*/,
      const ObjectHeader& objHeader) override {
    statusCount_++;
    std::cout << "ObjectStatus=" << uint32_t(objHeader.status) << std::endl;
  }

  void onEndOfStream() override {
    if (isFetch_) {
      std::cout << "onEndOfStream" << std::endl;
      baton.post();
    }
  }

  void onError(ResetStreamErrorCode error) override {
    std::cout << "Stream Error=" << folly::to_underlying(error) << std::endl;
    hasError_ = true;
    baton.post();
  }

  void onPublishDone(PublishDone) override {
    if (!isFetch_) {
      std::cout << "onPublishDone" << std::endl;
      baton.post();
    }
  }

  folly::coro::Baton baton;
  uint64_t objectCount_{0};
  uint64_t statusCount_{0};
  uint64_t bytesReceived_{0};
  bool hasError_{false};

 private:
  bool isFetch_{false};
};

// Subscriber implementation (similar to MoQTextClient)
class PyMoQSubscriber : public Subscriber,
                        public std::enable_shared_from_this<PyMoQSubscriber> {
 public:
  PyMoQSubscriber(
      std::shared_ptr<MoQFollyExecutorImpl> evb,
      proxygen::URL url,
      FullTrackName ftn,
      const MoQClientOptions& options,
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
        fullTrackName_(std::move(ftn)),
        options_(options),
        handler_(
            std::make_shared<PyObjectHandler>(
                options.fetch || options.jafetch || options.jrfetch)),
        subReceiver_(
            std::make_shared<ObjectReceiver>(
                ObjectReceiver::SUBSCRIBE,
                handler_)) {}

  PublishResult publish(
      PublishRequest pub,
      std::shared_ptr<SubscriptionHandle> handle) override {
    moxygen::PublishOk publishOk;
    publishOk.requestID = pub.requestID;
    publishOk.forward = true;
    publishOk.subscriberPriority = kDefaultPriority;
    publishOk.groupOrder = moxygen::GroupOrder::Default;
    publishOk.locType = moxygen::LocationType::AbsoluteStart;
    publishOk.start = moxygen::AbsoluteLocation(0, 0);

    if (handle) {
      subHandles_.push_back(handle);
    }

    moxygen::Subscriber::PublishConsumerAndReplyTask publishResponse{
        subReceiver_,
        folly::coro::makeTask(
            folly::Expected<moxygen::PublishOk, moxygen::PublishError>(
                publishOk))};

    return publishResponse;
  }

  folly::coro::Task<int> run(SubscribeRequest sub) noexcept {
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

      if (options_.publish) {
        // Publish mode - SubscribeNamespace
        SubscribeNamespace subAnn{
            sub.requestID,
            sub.fullTrackName.trackNamespace,
            true /* forward */,
            sub.params,
            {} /* options */};
        auto res = co_await moqClient_.getSession()->subscribeNamespace(
            subAnn, nullptr);
        if (!res.hasValue()) {
          auto& error = res.error();
          std::cerr << "SubscribeNamespace failed with error:" << std::endl;
          std::cerr << "  - Error Code: "
                    << folly::to_underlying(error.errorCode) << " ("
                    << requestErrorCodeToString(error.errorCode) << ")"
                    << std::endl;
          std::cerr << "  - Reason: " << error.reasonPhrase << std::endl;
          std::cerr << "  - Request ID: " << error.requestID.value << std::endl;
          moqClient_.shutdown();
          co_return -7;
        }
        subscribeNamespaceHandle_ = res.value();

        if (options_.unsubscribe) {
          co_await folly::coro::sleep(
              std::chrono::seconds(options_.unsubscribeTime));
          for (auto& handle : subHandles_) {
            handle->unsubscribe();
          }
        }

        co_await handler_->baton;
      } else if (options_.jafetch || options_.jrfetch) {
        // Joining fetch mode
        FetchType fetchType = options_.jafetch ? FetchType::ABSOLUTE_JOINING
                                               : FetchType::RELATIVE_JOINING;
        auto fetchReceiver = std::make_shared<ObjectReceiver>(
            ObjectReceiver::FETCH, std::make_shared<PyObjectHandler>(true));

        auto joinResult = co_await moqClient_.getSession()->join(
            sub,
            subReceiver_,
            options_.joinStart,
            sub.priority,
            sub.groupOrder,
            {},
            fetchReceiver,
            fetchType);

        if (joinResult.subscribeResult.hasError()) {
          auto& error = joinResult.subscribeResult.error();
          std::cerr << "Join/Subscribe failed with error:" << std::endl;
          std::cerr << "  - Error Code: "
                    << folly::to_underlying(error.errorCode) << std::endl;
          std::cerr << "  - Reason: " << error.reasonPhrase << std::endl;
          moqClient_.shutdown();
          co_return -4;
        }

        co_await handler_->baton;
      } else if (options_.fetch) {
        // Pure fetch mode
        if (!sub.start.has_value()) {
          std::cerr << "Fetch requires start location" << std::endl;
          moqClient_.shutdown();
          co_return -1;
        }
        AbsoluteLocation fetchEnd{sub.endGroup + 1, 0};
        auto fetchReq = Fetch(
            RequestID(0),
            sub.fullTrackName,
            *sub.start,
            fetchEnd,
            sub.priority,
            sub.groupOrder);
        auto res =
            co_await moqClient_.getSession()->fetch(fetchReq, subReceiver_);
        if (!res.hasValue()) {
          auto& error = res.error();
          std::cerr << "Fetch failed with error:" << std::endl;
          std::cerr << "  - Error Code: "
                    << folly::to_underlying(error.errorCode) << " ("
                    << requestErrorCodeToString(error.errorCode) << ")"
                    << std::endl;
          std::cerr << "  - Reason: " << error.reasonPhrase << std::endl;
          std::cerr << "  - Request ID: " << error.requestID.value << std::endl;
          moqClient_.shutdown();
          co_return -3;
        }
        co_await handler_->baton;
      } else {
        // Default: Subscribe mode
        auto res =
            co_await moqClient_.getSession()->subscribe(sub, subReceiver_);
        if (!res.hasValue()) {
          auto& error = res.error();
          std::cerr << "Subscribe failed with error:" << std::endl;
          std::cerr << "  - Error Code: "
                    << folly::to_underlying(error.errorCode) << " ("
                    << requestErrorCodeToString(error.errorCode) << ")"
                    << std::endl;
          std::cerr << "  - Reason: " << error.reasonPhrase << std::endl;
          std::cerr << "  - Request ID: " << error.requestID.value << std::endl;
          moqClient_.shutdown();
          co_return -4;
        }
        co_await handler_->baton;
      }

      moqClient_.shutdown();

      if (handler_->hasError_) {
        co_return -5;
      }

      co_return 0; // Success
    } catch (const std::exception& ex) {
      std::cerr << "Exception: " << ex.what() << std::endl;
      co_return -6;
    }
  }

  void stop() {
    moqClient_.shutdown();
  }

  MoQRelayClient moqClient_;
  FullTrackName fullTrackName_;
  MoQClientOptions options_;
  std::shared_ptr<PyObjectHandler> handler_;
  std::shared_ptr<ObjectReceiver> subReceiver_;
  std::shared_ptr<Publisher::SubscribeNamespaceHandle>
      subscribeNamespaceHandle_;
  std::vector<std::shared_ptr<Publisher::SubscriptionHandle>> subHandles_;
};

// Python wrapper class
class PyMoQTestClient {
 public:
  explicit PyMoQTestClient(MoQClientOptions options)
      : options_(std::move(options)) {}

  void run() {
    int result = runInternal();
    if (result != 0) {
      throw MoQClientError(result, getErrorMessage(result));
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

      // Parse location params
      SubParams subParams;
      bool isFetch = options_.fetch || options_.jafetch || options_.jrfetch;
      if (isFetch && !options_.sg.empty()) {
        subParams = parseLocationParams(options_);
      } else if (isFetch) {
        // Default fetch params
        subParams.locType = LocationType::AbsoluteRange;
        subParams.start.emplace(AbsoluteLocation(0, 0));
        subParams.endGroup = 1;
      } else if (!options_.sg.empty()) {
        subParams = parseLocationParams(options_);
      } else {
        subParams.locType = LocationType::LargestObject;
      }

      TrackNamespace ns = TrackNamespace(
          options_.trackNamespace, options_.trackNamespaceDelimiter);
      auto moqEvb = std::make_shared<MoQFollyExecutorImpl>(&eventBase);

      std::shared_ptr<fizz::CertificateVerifier> verifier = nullptr;
      if (options_.insecure) {
        verifier = std::make_shared<
            moxygen::test::InsecureVerifierDangerousDoNotUseInProduction>();
      }

      auto client = std::make_shared<PyMoQSubscriber>(
          moqEvb,
          std::move(parsedUrl),
          FullTrackName({ns, options_.trackName}),
          options_,
          verifier);

      // Build subscribe request
      std::vector<Parameter> params;
      if (options_.deliveryTimeout > 0) {
        params.emplace_back(
            folly::to_underlying(TrackRequestParamKey::DELIVERY_TIMEOUT),
            options_.deliveryTimeout);
      }

      auto sub = SubscribeRequest::make(
          FullTrackName({ns, options_.trackName}),
          kDefaultPriority,
          GroupOrder::OldestFirst,
          options_.forward,
          subParams.locType,
          subParams.start,
          subParams.endGroup,
          params);

      // Use shared_ptr for result to avoid reference capture issues
      auto resultPtr = std::make_shared<int>(-999);

      co_withExecutor(&eventBase, client->run(sub))
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
    } catch (const MoQClientError& e) {
      return e.code();
    } catch (const std::exception& ex) {
      std::cerr << "Exception in runInternal: " << ex.what() << std::endl;
      return -2;
    }
  }

  MoQClientOptions options_;
};

// Helper to get supported parameters with defaults (for Python testing)
// Returns default values from MoQClientOptions struct - single source of truth
pybind11::dict getSupportedParams() {
  MoQClientOptions defaults; // Uses struct default values
  pybind11::dict params;

  // Required params (empty string in C++ means required)
  params["connect_url"] = defaults.connectUrl.empty()
      ? pybind11::none()
      : pybind11::cast(defaults.connectUrl);
  params["track_namespace"] = defaults.trackNamespace.empty()
      ? pybind11::none()
      : pybind11::cast(defaults.trackNamespace);
  params["track_name"] = defaults.trackName.empty()
      ? pybind11::none()
      : pybind11::cast(defaults.trackName);

  // Optional params - directly from struct defaults
  params["track_namespace_delimiter"] = defaults.trackNamespaceDelimiter;
  params["sg"] = defaults.sg;
  params["so"] = defaults.so;
  params["eg"] = defaults.eg;
  params["connect_timeout"] = defaults.connectTimeout;
  params["transaction_timeout"] = defaults.transactionTimeout;
  params["quic_transport"] = defaults.quicTransport;
  params["use_legacy_setup"] = defaults.useLegacySetup;
  params["insecure"] = defaults.insecure;
  params["forward"] = defaults.forward;
  params["delivery_timeout"] = defaults.deliveryTimeout;
  params["fetch"] = defaults.fetch;
  params["jrfetch"] = defaults.jrfetch;
  params["jafetch"] = defaults.jafetch;
  params["join_start"] = defaults.joinStart;
  params["publish"] = defaults.publish;
  params["unsubscribe"] = defaults.unsubscribe;
  params["unsubscribe_time"] = defaults.unsubscribeTime;
  params["mlog_path"] = defaults.mlogPath;

  return params;
}

// Python bindings with kwargs pattern (matching Meta internal style like
// GLBClient)
PYBIND11_MODULE(moq_client_pybinding, m) {
  m.doc() = "MoQ Test Client Python Bindings";

  // Expose supported params for testing
  m.def(
      "get_supported_params",
      &getSupportedParams,
      "Get dict of supported params with their default values. "
      "Required params have None as default.");

  // Register custom exception
  // NOLINTNEXTLINE(facebook-hte-ContextDependentStaticInit)
  static pybind11::exception<MoQClientError> exc(m, "MoQClientError");
  pybind11::register_exception_translator([](std::exception_ptr p) {
    try {
      if (p) {
        std::rethrow_exception(p);
      }
    } catch (const MoQClientError& e) {
      PyErr_SetString(
          exc.ptr(), (std::to_string(e.code()) + ": " + e.message()).c_str());
    }
  });

  pybind11::class_<PyMoQTestClient>(m, "PyMoQTestClient")
      .def(pybind11::init([](const pybind11::kwargs& kwargs) {
        MoQClientOptions options;

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

        // Optional params - mapped exactly to C++ FLAGS names
        if (kwargs.contains("track_namespace_delimiter")) {
          options.trackNamespaceDelimiter =
              kwargs["track_namespace_delimiter"].cast<std::string>();
        }
        if (kwargs.contains("sg")) {
          options.sg = kwargs["sg"].cast<std::string>();
        }
        if (kwargs.contains("so")) {
          options.so = kwargs["so"].cast<std::string>();
        }
        if (kwargs.contains("eg")) {
          options.eg = kwargs["eg"].cast<std::string>();
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
        if (kwargs.contains("forward")) {
          options.forward = kwargs["forward"].cast<bool>();
        }
        if (kwargs.contains("delivery_timeout")) {
          options.deliveryTimeout = kwargs["delivery_timeout"].cast<uint64_t>();
        }
        if (kwargs.contains("fetch")) {
          options.fetch = kwargs["fetch"].cast<bool>();
        }
        if (kwargs.contains("jrfetch")) {
          options.jrfetch = kwargs["jrfetch"].cast<bool>();
        }
        if (kwargs.contains("jafetch")) {
          options.jafetch = kwargs["jafetch"].cast<bool>();
        }
        if (kwargs.contains("join_start")) {
          options.joinStart = kwargs["join_start"].cast<int32_t>();
        }
        if (kwargs.contains("publish")) {
          options.publish = kwargs["publish"].cast<bool>();
        }
        if (kwargs.contains("unsubscribe")) {
          options.unsubscribe = kwargs["unsubscribe"].cast<bool>();
        }
        if (kwargs.contains("unsubscribe_time")) {
          options.unsubscribeTime = kwargs["unsubscribe_time"].cast<uint64_t>();
        }
        if (kwargs.contains("mlog_path")) {
          options.mlogPath = kwargs["mlog_path"].cast<std::string>();
        }

        // Validate mutually exclusive params (same as C++ main())
        int sumFetchCount =
            int(options.fetch) + int(options.jafetch) + int(options.jrfetch);
        if (sumFetchCount > 1) {
          throw std::invalid_argument(
              "Can specify at most one of fetch, jafetch, or jrfetch");
        }

        return std::make_unique<PyMoQTestClient>(std::move(options));
      }))
      .def(
          "run",
          &PyMoQTestClient::run,
          "Run the MoQ client. Behavior determined by options. Raises MoQClientError on failure.");
}

} // namespace facebook::moxygen

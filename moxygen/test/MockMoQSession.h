#pragma once

#include <thread>

#include <moxygen/MoQSession.h>
#include <moxygen/events/MoQFollyExecutorImpl.h>

namespace moxygen::test {

// Mock MoQSession for testing relay behavior
class MockMoQSession : public MoQSession {
 public:
  // Constructor that accepts an external executor
  explicit MockMoQSession(std::shared_ptr<MoQExecutor> exec)
      : MoQSession(
            folly::MaybeManagedPtr<proxygen::WebTransport>(nullptr),
            exec),
        ownsEventBase_(false) {}

  // Default constructor that creates and manages its own executor
  MockMoQSession()
      : MoQSession(
            folly::MaybeManagedPtr<proxygen::WebTransport>(nullptr),
            std::make_shared<MoQFollyExecutorImpl>(&evb_)),
        ownsEventBase_(true) {
    // Start EventBase in background thread for async operations
    evbThread_ = std::thread([this]() { evb_.loopForever(); });
  }

  ~MockMoQSession() {
    if (ownsEventBase_) {
      // Stop EventBase and wait for thread
      evb_.terminateLoopSoon();
      if (evbThread_.joinable()) {
        evbThread_.join();
      }
    }
  }

  MOCK_METHOD(
      folly::coro::Task<PublishNamespaceResult>,
      publishNamespace,
      (PublishNamespace, std::shared_ptr<Subscriber::PublishNamespaceCallback>),
      (override));

  MOCK_METHOD(
      PublishResult,
      publish,
      (PublishRequest, std::shared_ptr<Publisher::SubscriptionHandle>),
      (override));

  MOCK_METHOD(
      folly::coro::Task<SubscribeResult>,
      subscribe,
      (SubscribeRequest, std::shared_ptr<TrackConsumer>),
      (override));

  MOCK_METHOD(
      std::optional<uint64_t>,
      getNegotiatedVersion,
      (),
      (const, override));

  RequestID peekNextRequestID() {
    return RequestID(nextRequestID_++);
  }

 private:
  folly::EventBase evb_;
  std::thread evbThread_;
  bool ownsEventBase_;
  uint64_t nextRequestID_{1};
};

} // namespace moxygen::test

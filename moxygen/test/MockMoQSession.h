#pragma once

#include <thread>

#include <moxygen/MoQSession.h>
#include <moxygen/events/MoQFollyExecutorImpl.h>

namespace moxygen::test {

// Helper class to create an executor before passing to base class
class MockMoQSessionExecutorHelper {
 protected:
  folly::EventBase evb_;
  std::unique_ptr<MoQFollyExecutorImpl> ownedExecutor_{
      std::make_unique<MoQFollyExecutorImpl>(&evb_)};
};

// Mock MoQSession for testing relay behavior
class MockMoQSession : private MockMoQSessionExecutorHelper, public MoQSession {
 public:
  // Constructor that accepts an external executor
  explicit MockMoQSession(MoQExecutor::KeepAlive exec)
      : MoQSession(
            folly::MaybeManagedPtr<proxygen::WebTransport>(nullptr),
            exec),
        ownsEventBase_(false) {}

  // Default constructor that creates and manages its own executor
  MockMoQSession()
      : MoQSession(
            folly::MaybeManagedPtr<proxygen::WebTransport>(nullptr),
            ownedExecutor_->keepAlive()),
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
  std::thread evbThread_;
  bool ownsEventBase_;
  uint64_t nextRequestID_{1};
};

} // namespace moxygen::test

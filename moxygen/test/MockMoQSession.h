#pragma once

#include <moxygen/MoQSession.h>

namespace moxygen::test {

// Mock MoQSession for testing relay behavior
class MockMoQSession : public MoQSession {
 public:
  MockMoQSession()
      : MoQSession(
            folly::MaybeManagedPtr<proxygen::WebTransport>(nullptr),
            nullptr) {
    evb_ = std::make_shared<folly::EventBase>();
  }

  folly::Executor* getExecutor() const {
    return evb_.get();
  }

  MOCK_METHOD(
      folly::coro::Task<AnnounceResult>,
      announce,
      (Announce, std::shared_ptr<Subscriber::AnnounceCallback>),
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
      folly::Optional<uint64_t>,
      getNegotiatedVersion,
      (),
      (const, override));

  RequestID peekNextRequestID() {
    return RequestID(nextRequestID_++);
  }

 private:
  std::shared_ptr<folly::EventBase> evb_;
  uint64_t nextRequestID_{1};
};

} // namespace moxygen::test

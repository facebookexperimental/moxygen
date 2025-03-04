#pragma once

#include <folly/coro/Task.h>
#include <moxygen/MoQFramer.h>

// MoQ Publisher interface
//
// This class is symmetric for the caller and callee.  MoQSession will implement
// Publisher, and an application will optionally set a Publisher callback.
//
// The caller will invoke:
//
//   auto subscribeResult = co_await session->subscribe(...);
//
//   subscribeResult.value() can be used for subscribeUpdate and unsubscribe
//
// And the remote peer will receive a callback
//
// folly::coro::Task<SubscribeResult> subscribe(...) {
//   verify subscribe
//   create SubscriptionHandle
//   Fill in subscribeOK
//   Current session can be obtained from folly::RequestContext
//   co_return handle;
// }

namespace moxygen {

class FetchConsumer;
class TrackConsumer;

// Represents a publisher on which the caller can invoke TRACK_STATUS_REQUEST,
// SUBSCRIBE, FETCH and SUBSCRIBE_ANNOUNCES.
class Publisher {
 public:
  virtual ~Publisher() = default;

  // Send/respond to TRACK_STATUS_REQUEST
  using TrackStatusResult = TrackStatus;
  virtual folly::coro::Task<TrackStatusResult> trackStatus(
      TrackStatusRequest trackStatusRequest) {
    return folly::coro::makeTask<TrackStatusResult>(TrackStatus{
        trackStatusRequest.fullTrackName,
        TrackStatusCode::UNKNOWN,
        folly::none});
  }

  // On successful SUBSCRIBE, a SubscriptionHandle is returned, which the
  // caller can use to UNSUBSCRIBE or SUBSCRIBE_UPDATE.
  class SubscriptionHandle {
   public:
    SubscriptionHandle() = default;
    explicit SubscriptionHandle(SubscribeOk ok) : subscribeOk_(std::move(ok)) {}
    virtual ~SubscriptionHandle() = default;

    virtual void unsubscribe() = 0;
    virtual void subscribeUpdate(SubscribeUpdate subUpdate) = 0;

    const SubscribeOk& subscribeOk() const {
      return *subscribeOk_;
    }

   protected:
    void setSubscribeOk(SubscribeOk subOk) {
      subscribeOk_ = std::move(subOk);
    }
    folly::Optional<SubscribeOk> subscribeOk_;
  };

  // Send/respond to a SUBSCRIBE
  using SubscribeResult =
      folly::Expected<std::shared_ptr<SubscriptionHandle>, SubscribeError>;
  virtual folly::coro::Task<SubscribeResult> subscribe(
      SubscribeRequest sub,
      std::shared_ptr<TrackConsumer> callback) {
    return folly::coro::makeTask<SubscribeResult>(
        folly::makeUnexpected(SubscribeError{
            sub.subscribeID,
            SubscribeErrorCode::NOT_SUPPORTED,
            "unimplemented",
            folly::none}));
  }

  // On successful FETCH, a FetchHandle is returned, which the caller can use
  // to FETCH_CANCEL.
  class FetchHandle {
   public:
    FetchHandle() = default;
    explicit FetchHandle(FetchOk ok) : fetchOk_(std::move(ok)) {}
    virtual ~FetchHandle() = default;

    virtual void fetchCancel() = 0;

    const FetchOk& fetchOk() const {
      return *fetchOk_;
    }

   protected:
    void setFetchOk(FetchOk fOk) {
      fetchOk_ = std::move(fOk);
    }

    folly::Optional<FetchOk> fetchOk_;
  };

  // Send/respond to a FETCH
  using FetchResult = folly::Expected<std::shared_ptr<FetchHandle>, FetchError>;
  virtual folly::coro::Task<FetchResult> fetch(
      Fetch fetch,
      std::shared_ptr<FetchConsumer> fetchCallback) {
    return folly::coro::makeTask<FetchResult>(folly::makeUnexpected(FetchError{
        fetch.subscribeID, FetchErrorCode::NOT_SUPPORTED, "unimplemented"}));
  }

  // On successful SUBSCRIBE_ANNOUNCES, a SubscribeAnnouncesHandle is returned,
  // which the caller can use to UNSUBSCRIBE_ANNOUNCES
  class SubscribeAnnouncesHandle {
   public:
    SubscribeAnnouncesHandle() = default;
    explicit SubscribeAnnouncesHandle(SubscribeAnnouncesOk ok)
        : subscribeAnnouncesOk_(std::move(ok)) {}
    virtual ~SubscribeAnnouncesHandle() = default;

    virtual void unsubscribeAnnounces() = 0;

    const SubscribeAnnouncesOk& subscribeAnnouncesOk() const {
      return *subscribeAnnouncesOk_;
    }

   protected:
    void setSubscribeAnnouncesOk(SubscribeAnnouncesOk ok) {
      subscribeAnnouncesOk_ = std::move(ok);
    }

    folly::Optional<SubscribeAnnouncesOk> subscribeAnnouncesOk_;
  };

  // Send/respond to SUBSCRIBE_ANNOUNCES
  using SubscribeAnnouncesResult = folly::Expected<
      std::shared_ptr<SubscribeAnnouncesHandle>,
      SubscribeAnnouncesError>;
  virtual folly::coro::Task<SubscribeAnnouncesResult> subscribeAnnounces(
      SubscribeAnnounces subAnn) {
    return folly::coro::makeTask<SubscribeAnnouncesResult>(
        folly::makeUnexpected(SubscribeAnnouncesError{
            subAnn.trackNamespacePrefix,
            SubscribeAnnouncesErrorCode::NOT_SUPPORTED,
            "unimplemented"}));
  }

  virtual void goaway(Goaway /*goaway*/) {}
};

} // namespace moxygen

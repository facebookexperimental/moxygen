/*
 *  Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 *  This source code is licensed under the MIT license found in the LICENSE
 *  file in the root directory of this source tree.
 *
 */

#pragma once

#include <folly/coro/Task.h>
#include <moxygen/MoQTypes.h>

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

class SubscriptionHandle {
 public:
  SubscriptionHandle() = default;
  explicit SubscriptionHandle(SubscribeOk ok) : subscribeOk_(std::move(ok)) {}
  virtual ~SubscriptionHandle() = default;

  virtual void unsubscribe() = 0;
  using SubscribeUpdateResult =
      folly::Expected<SubscribeUpdateOk, SubscribeUpdateError>;
  // Updates subscription parameters (start/end locations, priority, forward).
  // This is a coroutine because it may do async work, such as forwarding the
  // update to the upstream publisher in a relay scenario.
  virtual folly::coro::Task<SubscribeUpdateResult> subscribeUpdate(
      SubscribeUpdate subUpdate) = 0;

  const SubscribeOk& subscribeOk() const {
    return *subscribeOk_;
  }

 protected:
  void setSubscribeOk(SubscribeOk subOk) {
    subscribeOk_ = std::move(subOk);
  }
  std::optional<SubscribeOk> subscribeOk_;
};

// Represents a publisher on which the caller can invoke TRACK_STATUS_REQUEST,
// SUBSCRIBE, FETCH and SUBSCRIBE_NAMESPACE.
class Publisher {
 public:
  using SubscriptionHandle = moxygen::SubscriptionHandle;
  virtual ~Publisher() = default;

  // Send/respond to TRACK_STATUS_REQUEST
  using TrackStatusResult = folly::Expected<TrackStatusOk, TrackStatusError>;
  virtual folly::coro::Task<TrackStatusResult> trackStatus(
      const TrackStatus trackStatus) {
    return folly::coro::makeTask<TrackStatusResult>(
        folly::makeUnexpected<TrackStatusError>(TrackStatusError{
            trackStatus.requestID,
            TrackStatusErrorCode::NOT_SUPPORTED,
            "Track status not implemented"}));
  }

  // On successful SUBSCRIBE, a SubscriptionHandle is returned, which the
  // caller can use to UNSUBSCRIBE or SUBSCRIBE_UPDATE.
  // Send/respond to a SUBSCRIBE
  using SubscribeResult =
      folly::Expected<std::shared_ptr<SubscriptionHandle>, SubscribeError>;
  virtual folly::coro::Task<SubscribeResult> subscribe(
      SubscribeRequest sub,
      std::shared_ptr<TrackConsumer> callback) {
    return folly::coro::makeTask<SubscribeResult>(folly::makeUnexpected(
        SubscribeError{
            sub.requestID,
            SubscribeErrorCode::NOT_SUPPORTED,
            "unimplemented"}));
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

    std::optional<FetchOk> fetchOk_;
  };

  // Send/respond to a FETCH
  using FetchResult = folly::Expected<std::shared_ptr<FetchHandle>, FetchError>;
  virtual folly::coro::Task<FetchResult> fetch(
      Fetch fetch,
      std::shared_ptr<FetchConsumer> fetchCallback) {
    return folly::coro::makeTask<FetchResult>(folly::makeUnexpected(
        FetchError{
            fetch.requestID, FetchErrorCode::NOT_SUPPORTED, "unimplemented"}));
  }

  // On successful SUBSCRIBE_NAMESPACE, a SubscribeNamespaceHandle is returned,
  // which the caller can use to UNSUBSCRIBE_NAMESPACE
  class SubscribeNamespaceHandle {
   public:
    SubscribeNamespaceHandle() = default;
    explicit SubscribeNamespaceHandle(SubscribeNamespaceOk ok)
        : subscribeNamespaceOk_(std::move(ok)) {}
    virtual ~SubscribeNamespaceHandle() = default;

    virtual void unsubscribeNamespace() = 0;

    const SubscribeNamespaceOk& subscribeNamespaceOk() const {
      return *subscribeNamespaceOk_;
    }

   protected:
    void setSubscribeNamespaceOk(SubscribeNamespaceOk ok) {
      subscribeNamespaceOk_ = std::move(ok);
    }

    std::optional<SubscribeNamespaceOk> subscribeNamespaceOk_;
  };

  // Send/respond to SUBSCRIBE_NAMESPACE
  using SubscribeNamespaceResult = folly::Expected<
      std::shared_ptr<SubscribeNamespaceHandle>,
      SubscribeNamespaceError>;
  virtual folly::coro::Task<SubscribeNamespaceResult> subscribeNamespace(
      SubscribeNamespace subAnn) {
    return folly::coro::makeTask<SubscribeNamespaceResult>(
        folly::makeUnexpected(
            SubscribeNamespaceError{
                subAnn.requestID,
                SubscribeNamespaceErrorCode::NOT_SUPPORTED,
                "unimplemented"}));
  }

  virtual void goaway(Goaway /*goaway*/) {}
};

} // namespace moxygen

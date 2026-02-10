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

// MoQ Subscriber interface
//
// This class is symmetric for the caller and callee.  MoQSession will implement
// Subscriber, and an application will optionally set Subbscriber callback on
// session.
//
// The caller will invoke:
//
//   auto publishNamespaceResult = co_await session->publishNamespace(...);
//
//   publishNamespaceResult.value() can be used for unnanounce
//
// And the remote peer will receive a callback
//
// folly::coro::Task<PublishNamespaceResult> publishNamespace(...) {
//   verify publishNamespace
//   create PublishNamespaceHandle
//   Fill in publishNamespaceOk
//   Current session can be obtained from folly::RequestContext
//   co_return handle;
// }

namespace moxygen {

class TrackConsumer;
class SubscriptionHandle;

// Represents a subscriber on which the caller can invoke PUBLISH_NAMESPACE
class Subscriber {
 public:
  using SubscriptionHandle = moxygen::SubscriptionHandle;
  virtual ~Subscriber() = default;

  // On successful PUBLISH_NAMESPACE, an PublishNamespaceHandle is returned,
  // which the caller can use to later PUBLISH_NAMESPACE_DONE.
  class PublishNamespaceHandle {
   public:
    PublishNamespaceHandle() = default;
    explicit PublishNamespaceHandle(PublishNamespaceOk annOk)
        : publishNamespaceOk_(std::move(annOk)) {}
    virtual ~PublishNamespaceHandle() = default;
    // Providing a default implementation of publishNamespaceDone, because it
    // can be an uninteresting message
    virtual void publishNamespaceDone() {}

    using RequestUpdateResult = folly::Expected<RequestOk, RequestError>;
    virtual folly::coro::Task<RequestUpdateResult> requestUpdate(
        RequestUpdate reqUpdate) {
      co_return folly::makeUnexpected(
          RequestError{
              reqUpdate.requestID,
              RequestErrorCode::NOT_SUPPORTED,
              "unimplemented"});
    }

    const PublishNamespaceOk& publishNamespaceOk() const {
      return *publishNamespaceOk_;
    }

   protected:
    void setPublishNamespaceOk(PublishNamespaceOk ok) {
      publishNamespaceOk_ = std::move(ok);
    };

    std::optional<PublishNamespaceOk> publishNamespaceOk_;
  };

  // A subscribe receives an PublishNamespaceCallback in publishNamespace, which
  // can be used to issue PUBLISH_NAMESPACE_CANCEL at some point after
  // PUBLISH_NAMESPACE_Ok.
  class PublishNamespaceCallback {
   public:
    virtual ~PublishNamespaceCallback() = default;

    virtual void publishNamespaceCancel(
        PublishNamespaceErrorCode errorCode,
        std::string reasonPhrase) = 0;
  };

  // Send/respond to PUBLISH_NAMESPACE
  using PublishNamespaceResult = folly::
      Expected<std::shared_ptr<PublishNamespaceHandle>, PublishNamespaceError>;
  virtual folly::coro::Task<PublishNamespaceResult> publishNamespace(
      PublishNamespace ann,
      std::shared_ptr<PublishNamespaceCallback> = nullptr) {
    return folly::coro::makeTask<PublishNamespaceResult>(folly::makeUnexpected(
        PublishNamespaceError{
            ann.requestID,
            PublishNamespaceErrorCode::NOT_SUPPORTED,
            "unimplemented"}));
  }

  // Result of a PUBLISH request containing consumer and async reply
  struct PublishConsumerAndReplyTask {
    std::shared_ptr<TrackConsumer> consumer;
    folly::coro::Task<folly::Expected<PublishOk, PublishError>> reply;
  };

  // Send/respond to a PUBLISH - synchronous API o that the publisher can
  // immediately start sending data instead of waiting for a PUBLISH_OK from the
  // peer
  using PublishResult =
      folly::Expected<PublishConsumerAndReplyTask, PublishError>;
  virtual PublishResult publish(
      PublishRequest pub,
      std::shared_ptr<SubscriptionHandle> /*handle*/ = nullptr) {
    return folly::makeUnexpected(
        PublishError{
            pub.requestID, PublishErrorCode::NOT_SUPPORTED, "unimplemented"});
  }

  virtual void goaway(Goaway /*goaway*/) {}
};

} // namespace moxygen

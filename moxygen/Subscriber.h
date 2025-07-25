/*
 *  Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 *  This source code is licensed under the MIT license found in the LICENSE
 *  file in the root directory of this source tree.
 *
 */

#pragma once

#include <folly/coro/Task.h>
#include <moxygen/MoQFramer.h>

// MoQ Subscriber interface
//
// This class is symmetric for the caller and callee.  MoQSession will implement
// Subscriber, and an application will optionally set Subbscriber callback on
// session.
//
// The caller will invoke:
//
//   auto announceResult = co_await session->announce(...);
//
//   announceResult.value() can be used for unnanounce
//
// And the remote peer will receive a callback
//
// folly::coro::Task<AnnounceResult> announce(...) {
//   verify announce
//   create AnnounceHandle
//   Fill in announceOk
//   Current session can be obtained from folly::RequestContext
//   co_return handle;
// }

namespace moxygen {

class TrackConsumer;
class SubscriptionHandle;

// Represents a subscriber on which the caller can invoke ANNOUNCE
class Subscriber {
 public:
  using SubscriptionHandle = moxygen::SubscriptionHandle;
  virtual ~Subscriber() = default;

  // On successful ANNOUNCE, an AnnounceHandle is returned, which the caller
  // can use to later UNANNOUNCE.
  class AnnounceHandle {
   public:
    AnnounceHandle() = default;
    explicit AnnounceHandle(AnnounceOk annOk) : announceOk_(std::move(annOk)) {}
    virtual ~AnnounceHandle() = default;
    // Providing a default implementation of unannounce, because it can be
    // an uninteresting message
    virtual void unannounce() {};

    const AnnounceOk& announceOk() const {
      return *announceOk_;
    }

   protected:
    void setAnnounceOk(AnnounceOk ok) {
      announceOk_ = std::move(ok);
    };

    folly::Optional<AnnounceOk> announceOk_;
  };

  // A subscribe receives an AnnounceCallback in announce, which can be used
  // to issue ANNOUNCE_CANCEL at some point after ANNOUNCE_Ok.
  class AnnounceCallback {
   public:
    virtual ~AnnounceCallback() = default;

    virtual void announceCancel(
        AnnounceErrorCode errorCode,
        std::string reasonPhrase) = 0;
  };

  // Send/respond to ANNOUNCE
  using AnnounceResult =
      folly::Expected<std::shared_ptr<AnnounceHandle>, AnnounceError>;
  virtual folly::coro::Task<AnnounceResult> announce(
      Announce ann,
      std::shared_ptr<AnnounceCallback> = nullptr) {
    return folly::coro::makeTask<AnnounceResult>(
        folly::makeUnexpected(AnnounceError{
            ann.requestID,
            ann.trackNamespace,
            AnnounceErrorCode::NOT_SUPPORTED,
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
    return folly::makeUnexpected(PublishError{
        pub.requestID, PublishErrorCode::NOT_SUPPORTED, "unimplemented"});
  }

  virtual void goaway(Goaway /*goaway*/) {}
};

} // namespace moxygen

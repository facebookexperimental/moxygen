/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <moxygen/MoQFramer.h>

namespace moxygen {

/*
 * The stats in the MoQStatsCallback are common to both the publisher
 * and subscriber. The comments above each function describe when they're
 * called for each of the two roles.
 */
class MoQStatsCallback {
 public:
  virtual ~MoQStatsCallback() = default;

  /*
   * Publisher: Responded to a SUBSCRIBE request with a SUBSCRIBE_OK
   * Subscriber: Received a SUBSCRIBE_OK from the publisher
   */
  virtual void onSubscribeSuccess() = 0;

  /*
   * Publisher: Responded to a SUBSCRIBE request with a SUBSCRIBE_ERROR
   * Subscriber: Received a SUBSCRIBE_ERROR from the publisher
   */
  virtual void onSubscribeError(SubscribeErrorCode errorCode) = 0;

  /*
   * Publisher: Responded to a FETCH request with a FETCH_OK
   * Subscriber: Received a FETCH_OK from the publisher
   */
  virtual void onFetchSuccess() = 0;

  /*
   * Publisher: Responded to a FETCH request with a FETCH_ERROR
   * Subscriber: Received a FETCH_ERROR from the publisher
   */
  virtual void onFetchError(FetchErrorCode errorCode) = 0;

  /*
   * Publisher: Received an PUBLISH_NAMESPACE_OK from the subscriber
   * Subscriber: Responded to an PUBLISH_NAMESPACE request with a
   * PUBLISH_NAMESPACE_OK
   */
  virtual void onPublishNamespaceSuccess() = 0;

  /*
   * Publisher: Received an PUBLISH_NAMESPACE_ERROR from the subscriber
   * Subscriber: Responded to an PUBLISH_NAMESPACE request with a
   * PUBLISH_NAMESPACE_ERROR
   */
  virtual void onPublishNamespaceError(PublishNamespaceErrorCode errorCode) = 0;

  /*
   * Publisher: Sent an PUBLISH_NAMESPACE_DONE
   * Subscriber: Received an PUBLISH_NAMESPACE_DONE
   */
  virtual void onPublishNamespaceDone() = 0;

  /*
   * Publisher: Received an PUBLISH_NAMESPACE_CANCEL
   * Subscriber: Sent an PUBLISH_NAMESPACE_CANCEL
   */
  virtual void onPublishNamespaceCancel() = 0;

  /*
   * Publisher: Responded to a SUBSCRIBE_NAMESPACE request with a
   *   SUBSCRIBE_NAMESPACE_OK
   * Subscriber: Received a SUBSCRIBE_NAMESPACE_OK from the publisher
   */
  virtual void onSubscribeNamespaceSuccess() = 0;

  /*
   * Publisher: Responded to a SUBSCRIBE_NAMESPACE request with a
   *   SUBSCRIBE_NAMESPACE_ERROR
   * Subscriber: Received a SUBSCRIBE_NAMESPACE_ERROR from the publisher
   */
  virtual void onSubscribeNamespaceError(
      SubscribeNamespaceErrorCode errorCode) = 0;

  /*
   * Publisher: Received an UNSUBSCRIBE_NAMESPACE
   * Subscriber: Sent an UNSUBSCRIBE_NAMESPACE
   */
  virtual void onUnsubscribeNamespace() = 0;

  /*
   * Publisher: Responded to a TRACK_STATUS request
   * Subscriber: Sent a TRACK_STATUS request
   */
  virtual void onTrackStatus() = 0;

  /*
   * Publisher: Received to an UNSUBSCRIBE
   * Subscriber: Sent an UNSUBSCRIBE
   */
  virtual void onUnsubscribe() = 0;

  /*
   * Publisher: Sent a SUBSCRIBE_DONE
   * Subscriber: Received a SUBSCRIBE_DONE
   */
  virtual void onSubscribeDone(SubscribeDoneStatusCode statusCode) = 0;

  /*
   * Publisher: Received a SUBSCRIBE_UPDATE
   * Subscriber: Sent a SUBSCRIBE_UPDATE
   */
  virtual void onSubscribeUpdate() = 0;

  /*
   * Publisher: Opened a subscription stream to the subscriber
   * Subscriber: The publisher opened a subscription stream to us
   */
  virtual void onSubscriptionStreamOpened() = 0;

  /*
   * Publisher: Closed a subscription stream to the subscriber
   * Subscriber: The publisher closed a subscription stream to us
   */
  virtual void onSubscriptionStreamClosed() = 0;
};

class MoQPublisherStatsCallback : public MoQStatsCallback {
 public:
  // Record the time it takes from request to response for an PUBLISH_NAMESPACE
  virtual void recordPublishNamespaceLatency(uint64_t latencyMsec) = 0;

  // Record the time it takes from request to response for a PUBLISH
  virtual void recordPublishLatency(uint64_t latencyMsec) = 0;

  // Called when PUBLISH receives PUBLISH_ERROR response
  virtual void onPublishError(PublishErrorCode errorCode) = 0;

  // Called when PUBLISH receives PUBLISH_OK response
  virtual void onPublishSuccess() = 0;
};

class MoQSubscriberStatsCallback : public MoQStatsCallback {
 public:
  // Record the time it takes from request to response for a SUBSCRIBE
  virtual void recordSubscribeLatency(uint64_t latencyMsec) = 0;

  // Record the time it takes from request to response for a FETCH
  virtual void recordFetchLatency(uint64_t latencyMsec) = 0;

  // Called when receiving a PUBLISH message
  virtual void onPublish() = 0;

  // Called when sending PUBLISH_OK response
  virtual void onPublishOk() = 0;

  // Called when sending PUBLISH_ERROR response
  virtual void onPublishError(PublishErrorCode errorCode) = 0;
};

#define MOQ_PUBLISHER_STATS(publisherStatsCallback, method, ...) \
  if (publisherStatsCallback) {                                  \
    folly::invoke(                                               \
        &MoQPublisherStatsCallback::method,                      \
        publisherStatsCallback,                                  \
        ##__VA_ARGS__);                                          \
  }                                                              \
  static_assert(true, "semicolon required")

#define MOQ_SUBSCRIBER_STATS(subscriberStatsCallback, method, ...) \
  if (subscriberStatsCallback) {                                   \
    folly::invoke(                                                 \
        &MoQSubscriberStatsCallback::method,                       \
        subscriberStatsCallback,                                   \
        ##__VA_ARGS__);                                            \
  }                                                                \
  static_assert(true, "semicolon required")

} // namespace moxygen

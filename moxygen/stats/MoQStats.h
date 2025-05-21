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
   * Publisher: Received an ANNOUNCE_OK from the subscriber
   * Subscriber: Responded to an ANNOUNCE request with a ANNOUNCE_OK
   */
  virtual void onAnnounceSuccess() = 0;

  /*
   * Publisher: Received an ANNOUNCE_ERROR from the subscriber
   * Subscriber: Responded to an ANNOUNCE request with a ANNOUNCE_ERROR
   */
  virtual void onAnnounceError(AnnounceErrorCode errorCode) = 0;

  /*
   * Publisher: Sent an UNANNOUNCE
   * Subscriber: Received an UNANNOUNCE
   */
  virtual void onUnannounce() = 0;

  /*
   * Publisher: Received an ANNOUNCE_CANCEL
   * Subscriber: Sent an ANNOUNCE_CANCEL
   */
  virtual void onAnnounceCancel() = 0;

  /*
   * Publisher: Responded to a SUBSCRIBE_ANNOUNCES request with a
   *   SUBSCRIBE_ANNOUNCES_OK
   * Subscriber: Received a SUBSCRIBE_ANNOUNCES_OK from the publisher
   */
  virtual void onSubscribeAnnouncesSuccess() = 0;

  /*
   * Publisher: Responded to a SUBSCRIBE_ANNOUNCES request with a
   *   SUBSCRIBE_ANNOUNCES_ERROR
   * Subscriber: Received a SUBSCRIBE_ANNOUNCES_ERROR from the publisher
   */
  virtual void onSubscribeAnnouncesError(
      SubscribeAnnouncesErrorCode errorCode) = 0;

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
};

class MoQPublisherStatsCallback : public MoQStatsCallback {};

class MoQSubscriberStatsCallback : public MoQStatsCallback {};

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

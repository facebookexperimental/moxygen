/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/test/MoQSessionTestCommon.h"

using namespace moxygen;
using namespace moxygen::test;
using testing::_;

// === TRACK STATUS tests ===

CO_TEST_P_X(MoQSessionTest, TrackStatusOk) {
  co_await setupMoQSession();
  EXPECT_CALL(*serverPublisherStatsCallback_, onTrackStatus());
  EXPECT_CALL(*clientSubscriberStatsCallback_, onTrackStatus());
  EXPECT_CALL(*serverPublisher, trackStatus(_))
      .WillOnce(
          testing::Invoke(
              [](TrackStatus request)
                  -> folly::coro::Task<Publisher::TrackStatusResult> {
                co_return makeTrackStatusOkResult(
                    request, AbsoluteLocation{0, 0});
              }));
  auto res = co_await clientSession_->trackStatus(getTrackStatus());
  EXPECT_FALSE(res.hasError());
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}
CO_TEST_P_X(MoQSessionTest, TrackStatusExceptionReleasesRequestID) {
  co_await setupMoQSession();
  auto initialMaxRequestID = serverSession_->maxRequestID();
  EXPECT_CALL(*serverPublisherStatsCallback_, onTrackStatus());
  EXPECT_CALL(*clientSubscriberStatsCallback_, onTrackStatus());
  // Publisher throws an exception — handleTrackStatus must not crash and
  // must still retire the request ID.
  EXPECT_CALL(*serverPublisher, trackStatus(_))
      .WillOnce(
          testing::Invoke(
              [](TrackStatus)
                  -> folly::coro::Task<Publisher::TrackStatusResult> {
                co_yield folly::coro::co_error(
                    std::runtime_error("trackStatus exploded"));
              }));
  auto res = co_await clientSession_->trackStatus(getTrackStatus());
  EXPECT_TRUE(res.hasError());
  co_await folly::coro::co_reschedule_on_current_executor;
  // The server should have retired the request ID despite the exception.
  EXPECT_GT(serverSession_->maxRequestID(), initialMaxRequestID);
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}
CO_TEST_P_X(MoQSessionTest, TrackStatusWithAuthorizationToken) {
  co_await setupMoQSession();
  EXPECT_CALL(*serverPublisherStatsCallback_, onTrackStatus());
  EXPECT_CALL(*clientSubscriberStatsCallback_, onTrackStatus());
  EXPECT_CALL(*serverPublisher, trackStatus(_))
      .WillOnce(
          testing::Invoke(
              [](TrackStatus request)
                  -> folly::coro::Task<Publisher::TrackStatusResult> {
                EXPECT_EQ(request.params.size(), 5);
                auto verifyParam = [](const auto& param,
                                      const std::string& expectedTokenValue) {
                  return param.key ==
                      folly::to_underlying(
                             TrackRequestParamKey::AUTHORIZATION_TOKEN) &&
                      param.asAuthToken.tokenType == 0 &&
                      param.asAuthToken.tokenValue == expectedTokenValue;
                };

                EXPECT_TRUE(verifyParam(request.params.at(0), "abc"));
                EXPECT_TRUE(
                    verifyParam(request.params.at(1), std::string(20, 'x')));
                EXPECT_TRUE(verifyParam(request.params.at(2), "abcd"));
                EXPECT_TRUE(verifyParam(request.params.at(3), "abcd"));
                EXPECT_TRUE(verifyParam(request.params.at(4), "xyzw"))
                    << "'" << request.params.at(4).asAuthToken.tokenValue;

                co_return makeTrackStatusOkResult(
                    request, AbsoluteLocation{0, 0});
              }));
  TrackStatus request = getTrackStatus();
  auto addAuthToken = [](auto& params, const AuthToken& token) {
    params.insertParam(Parameter(
        folly::to_underlying(TrackRequestParamKey::AUTHORIZATION_TOKEN),
        token));
  };

  addAuthToken(request.params, {0, "abc", AuthToken::DontRegister});
  addAuthToken(request.params, {0, std::string(20, 'x'), AuthToken::Register});
  addAuthToken(request.params, {0, "abcd", AuthToken::Register});
  addAuthToken(request.params, {0, "abcd", AuthToken::Register});
  addAuthToken(request.params, {0, "xyzw", AuthToken::Register});
  auto res = co_await clientSession_->trackStatus(request);
  EXPECT_FALSE(res.hasError());
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

CO_TEST_P_X(MoQSessionTest, TrackStatusPublisherException) {
  co_await setupMoQSession();
  // The server has initialMaxRequestID_=4 (kTestMaxRequestID *
  // getRequestIDMultiplier). Each trackStatus uses one request ID. If
  // retireRequestID is not called after the exception, request IDs leak and
  // eventually the client cannot send more requests.
  auto numRequests = initialMaxRequestID_ + 1;
  EXPECT_CALL(*serverPublisherStatsCallback_, onTrackStatus())
      .Times(numRequests);
  EXPECT_CALL(*clientSubscriberStatsCallback_, onTrackStatus())
      .Times(numRequests);
  EXPECT_CALL(*serverPublisher, trackStatus(_))
      .WillRepeatedly(
          testing::Invoke(
              [](TrackStatus)
                  -> folly::coro::Task<Publisher::TrackStatusResult> {
                throw std::runtime_error("publisher exploded");
                co_return makeTrackStatusOkResult({}, AbsoluteLocation{0, 0});
              }));
  // Send enough requests to exhaust request IDs if they are leaked
  for (uint64_t i = 0; i < numRequests; i++) {
    auto res = co_await clientSession_->trackStatus(getTrackStatus());
    EXPECT_TRUE(res.hasError())
        << "Request " << i << " should have returned an error";
    EXPECT_EQ(res.error().errorCode, TrackStatusErrorCode::INTERNAL_ERROR);
    // Allow the server-side fire-and-forget coroutine to complete
    co_await folly::coro::co_reschedule_on_current_executor;
    co_await folly::coro::co_reschedule_on_current_executor;
  }
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
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

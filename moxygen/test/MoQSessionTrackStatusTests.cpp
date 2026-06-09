/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/MoQTrackProperties.h"
#include "moxygen/test/MoQSessionTestCommon.h"
#include "moxygen/test/MockMoQSession.h"

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

// === Track Properties validation tests for REQUEST_OK (draft 18+) ===

namespace {

// Test fixture exposes the protected helper so we can directly exercise the
// draft-18 Track Properties validation that BOTH MoQSession::onRequestOk and
// MoQRelaySession::onRequestOk delegate to. Testing the shared helper covers
// both code paths.
class ValidatorSession : public MockMoQSession {
 public:
  using MockMoQSession::MockMoQSession;
  using MoQSession::validateRequestOkParams;
  using MoQSession::validateRequestOkTrackProperties;
};

class CloseCallbackRecorder : public MoQSession::MoQSessionCloseCallback {
 public:
  void onMoQSessionClosed(
      SessionCloseErrorCode error,
      folly::Optional<uint32_t> /*wtError*/) override {
    closed = true;
    lastError = error;
  }
  bool closed{false};
  SessionCloseErrorCode lastError{SessionCloseErrorCode::NO_ERROR};
};

std::shared_ptr<testing::NiceMock<ValidatorSession>> makeValidatorSession(
    uint64_t version) {
  auto session = std::make_shared<testing::NiceMock<ValidatorSession>>();
  ON_CALL(*session, getNegotiatedVersion())
      .WillByDefault(testing::Return(std::optional<uint64_t>(version)));
  return session;
}

RequestOk makeRequestOkWithProperty() {
  RequestOk requestOk;
  requestOk.requestID = RequestID(1);
  requestOk.trackProperties.insertMutableExtension(
      Extension{kPublisherPriorityExtensionType, 50});
  return requestOk;
}

} // namespace

TEST(MoQSessionRequestOkTrackPropertiesValidation, AllowsTrackStatusOk) {
  auto session = makeValidatorSession(kVersionDraft18);
  auto requestOk = makeRequestOkWithProperty();
  EXPECT_TRUE(session->validateRequestOkTrackProperties(
      requestOk, FrameType::TRACK_STATUS_OK));
}

TEST(MoQSessionRequestOkTrackPropertiesValidation, AllowsEmptyTrackProperties) {
  auto session = makeValidatorSession(kVersionDraft18);
  RequestOk requestOk;
  requestOk.requestID = RequestID(1);
  // trackProperties intentionally empty.

  for (auto frameType :
       {FrameType::REQUEST_OK,
        FrameType::PUBLISH_NAMESPACE_OK,
        FrameType::SUBSCRIBE_NAMESPACE_OK,
        FrameType::PUBLISH_OK}) {
    EXPECT_TRUE(
        session->validateRequestOkTrackProperties(requestOk, frameType));
  }
}

TEST(
    MoQSessionRequestOkTrackPropertiesValidation,
    RejectsTrackPropertiesForNonTrackStatusOk) {
  // Each non-TRACK_STATUS_OK frame type with non-empty trackProperties must
  // cause the session to close with PROTOCOL_VIOLATION. This is the rule that
  // MoQRelaySession::onRequestOk previously bypassed; covered here via the
  // shared helper that both onRequestOk implementations now call.
  for (auto frameType :
       {FrameType::REQUEST_OK,
        FrameType::PUBLISH_NAMESPACE_OK,
        FrameType::SUBSCRIBE_NAMESPACE_OK,
        FrameType::PUBLISH_OK}) {
    auto session = makeValidatorSession(kVersionDraft18);
    CloseCallbackRecorder recorder;
    session->setSessionCloseCallback(&recorder);
    auto requestOk = makeRequestOkWithProperty();

    EXPECT_FALSE(
        session->validateRequestOkTrackProperties(requestOk, frameType))
        << "frameType=" << folly::to_underlying(frameType);
    EXPECT_TRUE(session->isClosed())
        << "frameType=" << folly::to_underlying(frameType);
    EXPECT_TRUE(recorder.closed)
        << "frameType=" << folly::to_underlying(frameType);
    EXPECT_EQ(recorder.lastError, SessionCloseErrorCode::PROTOCOL_VIOLATION)
        << "frameType=" << folly::to_underlying(frameType);
  }
}

TEST(
    MoQSessionRequestOkTrackPropertiesValidation,
    PreDraft18AllowsTrackPropertiesForAnyFrameType) {
  // The validator is a no-op for drafts that don't have Track Properties on
  // REQUEST_OK. The helper accepts whatever is on the struct; it is the
  // wire-level writer/parser that drops the field for those versions.
  auto session = makeValidatorSession(kVersionDraft17);
  auto requestOk = makeRequestOkWithProperty();

  EXPECT_TRUE(session->validateRequestOkTrackProperties(
      requestOk, FrameType::REQUEST_OK));
  EXPECT_FALSE(session->isClosed());
}

// === Param validation tests for REQUEST_OK shorthands (draft 18+) ===
//
// REQUEST_OK is parsed against the union of params allowed for any shorthand
// (e.g. OBJECT_DELIVERY_TIMEOUT lists REQUEST_OK so PUBLISH_OK passes). Once
// the shorthand resolves, generic params not allowed for it must be rejected.
// Both MoQSession::onRequestOk and MoQRelaySession::onRequestOk delegate to the
// shared validateRequestOkParams helper.

namespace {
RequestOk makeRequestOkWithObjectDeliveryTimeout() {
  RequestOk requestOk;
  requestOk.requestID = RequestID(1);
  requestOk.params.setMajorVersion(getDraftMajorVersion(kVersionDraft18));
  requestOk.params.insertParam(Parameter(
      folly::to_underlying(TrackRequestParamKey::OBJECT_DELIVERY_TIMEOUT),
      uint64_t(1000)));
  return requestOk;
}
} // namespace

TEST(
    MoQSessionRequestOkParamsValidation,
    AllowsObjectDeliveryTimeoutForPublishOk) {
  auto session = makeValidatorSession(kVersionDraft18);
  auto requestOk = makeRequestOkWithObjectDeliveryTimeout();
  EXPECT_TRUE(
      session->validateRequestOkParams(requestOk, FrameType::PUBLISH_OK));
  EXPECT_FALSE(session->isClosed());
}

TEST(
    MoQSessionRequestOkParamsValidation,
    RejectsObjectDeliveryTimeoutForNonPublishOkShorthands) {
  // OBJECT_DELIVERY_TIMEOUT is valid for PUBLISH_OK but not for these
  // REQUEST_OK shorthands (spec 10.2.4). The parser accepts the superset;
  // resolution must reject it here with PROTOCOL_VIOLATION.
  for (auto frameType :
       {FrameType::TRACK_STATUS_OK,
        FrameType::SUBSCRIBE_NAMESPACE_OK,
        FrameType::PUBLISH_NAMESPACE_OK}) {
    auto session = makeValidatorSession(kVersionDraft18);
    CloseCallbackRecorder recorder;
    session->setSessionCloseCallback(&recorder);
    auto requestOk = makeRequestOkWithObjectDeliveryTimeout();

    EXPECT_FALSE(session->validateRequestOkParams(requestOk, frameType))
        << "frameType=" << folly::to_underlying(frameType);
    EXPECT_TRUE(session->isClosed())
        << "frameType=" << folly::to_underlying(frameType);
    EXPECT_EQ(recorder.lastError, SessionCloseErrorCode::PROTOCOL_VIOLATION)
        << "frameType=" << folly::to_underlying(frameType);
  }
}

TEST(MoQSessionRequestOkParamsValidation, PreDraft18SkipsParamValidation) {
  // Pre-18 each shorthand had its own frame type, so there is no superset to
  // re-check; the validator is a no-op.
  auto session = makeValidatorSession(kVersionDraft17);
  RequestOk requestOk;
  requestOk.requestID = RequestID(1);
  EXPECT_TRUE(
      session->validateRequestOkParams(requestOk, FrameType::TRACK_STATUS_OK));
  EXPECT_FALSE(session->isClosed());
}

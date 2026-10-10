/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/MoQVersions.h"
#include "moxygen/ReplyContext.h"
#include "moxygen/test/MoQSessionTestCommon.h"

#include <optional>
#include <vector>

using namespace moxygen;
using namespace moxygen::test;
using testing::_;

namespace {

class CapturingReplyContext : public ReplyContext {
 public:
  CapturingReplyContext() : ReplyContext(folly::CancellationToken()) {}

  folly::IOBufQueue& writeBuf() override {
    return writeBuf_;
  }

  void flush(bool fin = false) override {
    flushedBuf_.append(writeBuf_.move());
    fin_ = fin;
  }

  bool empty() const {
    return flushedBuf_.empty();
  }

  bool fin() const {
    return fin_;
  }

  std::unique_ptr<folly::IOBuf> moveFlushed() {
    return flushedBuf_.move();
  }

 private:
  folly::IOBufQueue writeBuf_{folly::IOBufQueue::cacheChainLength()};
  folly::IOBufQueue flushedBuf_{folly::IOBufQueue::cacheChainLength()};
  bool fin_{false};
};

class RecordingControlCallback : public MoQControlCodec::ControlCallback {
 public:
  void onRequestOk(RequestOk ok, FrameType frameType) override {
    requestOk = std::move(ok);
    requestOkFrameType = frameType;
    callbacks.push_back(FrameType::REQUEST_OK);
  }

  void onPublishBlocked(PublishBlocked blocked) override {
    this->publishBlocked = std::move(blocked);
    callbacks.push_back(FrameType::PUBLISH_BLOCKED);
  }

  void onConnectionError(ErrorCode error) override {
    connectionError = error;
  }

  std::vector<FrameType> callbacks;
  std::optional<RequestOk> requestOk;
  std::optional<FrameType> requestOkFrameType;
  std::optional<PublishBlocked> publishBlocked;
  std::optional<ErrorCode> connectionError;
};

} // namespace

TEST(SubscribeTracksReplyTest, PublishBlockedBeforeOkIsSentAfterOk) {
  MoQFrameWriter writer;
  writer.initializeVersion(kVersionDraft18);
  auto replyContext = std::make_shared<CapturingReplyContext>();
  SubscribeTracksReply reply(writer, replyContext);
  const TrackNamespace suffix(std::vector<std::string>{"sports"});

  reply.publishBlocked(suffix, "highlights");
  EXPECT_TRUE(replyContext->empty());

  ASSERT_TRUE(reply.ok(RequestOk{.requestID = RequestID(7)}).hasValue());
  EXPECT_FALSE(replyContext->fin());

  auto serialized = replyContext->moveFlushed();
  ASSERT_NE(serialized, nullptr);

  RecordingControlCallback callback;
  MoQBidiStreamCodec bidiCodec(
      &callback,
      /*allowedFrames=*/{FrameType::PUBLISH_BLOCKED},
      /*requestID=*/RequestID(7),
      /*okType=*/FrameType::REQUEST_OK);
  bidiCodec.initializeVersion(kVersionDraft18);

  auto res = bidiCodec.onIngress(std::move(serialized), false);
  ASSERT_EQ(res, MoQCodec::ParseResult::CONTINUE);
  EXPECT_FALSE(callback.connectionError.has_value());

  ASSERT_EQ(callback.callbacks.size(), 2);
  EXPECT_EQ(callback.callbacks[0], FrameType::REQUEST_OK);
  EXPECT_EQ(callback.callbacks[1], FrameType::PUBLISH_BLOCKED);

  ASSERT_TRUE(callback.requestOk.has_value());
  EXPECT_EQ(callback.requestOk->requestID, RequestID(7));
  EXPECT_EQ(callback.requestOkFrameType, FrameType::REQUEST_OK);

  ASSERT_TRUE(callback.publishBlocked.has_value());
  EXPECT_EQ(callback.publishBlocked->trackNamespaceSuffix, suffix);
  EXPECT_EQ(callback.publishBlocked->trackName, "highlights");
}

TEST(SubscribeTracksReplyTest, ErrorAfterErrorIsRejected) {
  MoQFrameWriter writer;
  writer.initializeVersion(kVersionDraft18);
  auto replyContext = std::make_shared<CapturingReplyContext>();
  SubscribeTracksReply reply(writer, replyContext);
  const SubscribeTracksError error{
      RequestID(7), SubscribeTracksErrorCode::INTERNAL_ERROR, "boom"};

  ASSERT_TRUE(reply.error(error).hasValue());
  EXPECT_TRUE(replyContext->fin());

  auto secondError = reply.error(error);
  ASSERT_TRUE(secondError.hasError());
  EXPECT_EQ(secondError.error(), quic::TransportErrorCode::PROTOCOL_VIOLATION);
}

// Draft 18+ only: SUBSCRIBE_TRACKS is a new message split out of
// SUBSCRIBE_NAMESPACE in draft 18.
using V18PlusSubscribeTracksTest = MoQSessionTest;

CO_TEST_P_X(Draft22Test, SubscribeTracksCarriesIncludePropertiesFalse) {
  co_await setupMoQSession();

  std::shared_ptr<MockSubscribeTracksHandle> mockHandle;
  EXPECT_CALL(*serverPublisher, subscribeTracks(_, _))
      .WillOnce(
          [&mockHandle](
              SubscribeTracks subTracks, auto /*publishBlockedHandle*/)
              -> folly::coro::Task<Publisher::SubscribeTracksResult> {
            EXPECT_FALSE(subTracks.includeProperties);
            mockHandle = std::make_shared<MockSubscribeTracksHandle>(
                SubscribeTracksOk({.requestID = subTracks.requestID}));
            co_return mockHandle;
          });

  auto request = getSubscribeTracks();
  request.includeProperties = false;
  auto result = co_await clientSession_->subscribeTracks(request);
  EXPECT_TRUE(result.hasValue());
  if (result.hasError()) {
    clientSession_->close(SessionCloseErrorCode::NO_ERROR);
    co_return;
  }

  folly::coro::Baton unsubscribed;
  EXPECT_CALL(*mockHandle, unsubscribeTracks()).WillOnce([&unsubscribed]() {
    unsubscribed.post();
  });
  result.value()->unsubscribeTracks();
  co_await unsubscribed;
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

CO_TEST_P_X(V18PlusSubscribeTracksTest, SubscribeAndUnsubscribeTracks) {
  co_await setupMoQSession();

  std::shared_ptr<MockSubscribeTracksHandle> mockSubscribeTracksHandle;
  EXPECT_CALL(*serverPublisher, subscribeTracks(_, _))
      .WillOnce(
          testing::Invoke(
              [&mockSubscribeTracksHandle](
                  auto subTracks, auto publishBlockedHandle)
                  -> folly::coro::Task<Publisher::SubscribeTracksResult> {
                EXPECT_NE(publishBlockedHandle, nullptr);
                if (publishBlockedHandle) {
                  publishBlockedHandle->publishBlocked(
                      TrackNamespace(std::vector<std::string>{"sports"}),
                      "highlights");
                }
                mockSubscribeTracksHandle =
                    std::make_shared<MockSubscribeTracksHandle>(
                        SubscribeTracksOk(
                            {.requestID = subTracks.requestID,
                             .requestSpecificParams = {}}));
                co_return mockSubscribeTracksHandle;
              }));

  auto result = co_await clientSession_->subscribeTracks(getSubscribeTracks());
  EXPECT_FALSE(result.hasError());

  folly::coro::Baton barricade;
  EXPECT_CALL(*mockSubscribeTracksHandle, unsubscribeTracks())
      .WillOnce(testing::Invoke([&barricade]() { barricade.post(); }));
  result.value()->unsubscribeTracks();
  co_await barricade;
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

class MockPublishBlockedHandle : public Publisher::PublishBlockedHandle {
 public:
  MOCK_METHOD(
      void,
      publishBlocked,
      (const TrackNamespace&, const std::string&),
      (override));
};

// Unsubscribing must release the caller's PublishBlockedHandle without
// waiting for the peer to close its half of the stream.
CO_TEST_P_X(
    V18PlusSubscribeTracksTest,
    UnsubscribeTracksReleasesPublishBlockedHandle) {
  co_await setupMoQSession();

  folly::coro::Baton serverSawUnsubscribe;
  std::shared_ptr<MockSubscribeTracksHandle> serverHandle;
  EXPECT_CALL(*serverPublisher, subscribeTracks(_, _))
      .WillOnce(
          [&](auto subTracks, auto /* publishBlockedHandle */)
              -> folly::coro::Task<Publisher::SubscribeTracksResult> {
            serverHandle =
                std::make_shared<MockSubscribeTracksHandle>(SubscribeTracksOk(
                    {.requestID = subTracks.requestID,
                     .requestSpecificParams = {}}));
            EXPECT_CALL(*serverHandle, unsubscribeTracks()).WillOnce([&] {
              serverSawUnsubscribe.post();
            });
            co_return serverHandle;
          });

  auto clientHandle =
      std::make_shared<testing::StrictMock<MockPublishBlockedHandle>>();
  std::weak_ptr<Publisher::PublishBlockedHandle> weakClientHandle =
      clientHandle;
  auto result = co_await clientSession_->subscribeTracks(
      getSubscribeTracks(), std::move(clientHandle));
  EXPECT_FALSE(result.hasError());
  if (result.hasValue()) {
    result.value()->unsubscribeTracks();
  }
  co_await serverSawUnsubscribe;
  co_await folly::coro::sleep(std::chrono::milliseconds(200));

  EXPECT_TRUE(weakClientHandle.expired());
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

CO_TEST_P_X(
    V18PlusSubscribeTracksTest,
    SubscribeTracksNotSupportedNoPublisher) {
  // Don't install a publish handler on the server side — the relay should
  // reply with NOT_SUPPORTED.
  co_await setupMoQSession();
  serverSession_->setPublishHandler(nullptr);

  auto result = co_await clientSession_->subscribeTracks(getSubscribeTracks());
  EXPECT_TRUE(result.hasError());
  if (result.hasError()) {
    EXPECT_EQ(
        result.error().errorCode, SubscribeTracksErrorCode::NOT_SUPPORTED);
  }

  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

CO_TEST_P_X(V18PlusSubscribeTracksTest, SubscribeTracksAppReturnsError) {
  co_await setupMoQSession();

  EXPECT_CALL(*serverPublisher, subscribeTracks(_, _))
      .WillOnce(
          testing::Invoke(
              [](auto subTracks, auto /*publishBlockedHandle*/)
                  -> folly::coro::Task<Publisher::SubscribeTracksResult> {
                co_return folly::makeUnexpected(
                    SubscribeTracksError{
                        subTracks.requestID,
                        SubscribeTracksErrorCode::INTERNAL_ERROR,
                        "boom"});
              }));

  auto result = co_await clientSession_->subscribeTracks(getSubscribeTracks());
  EXPECT_TRUE(result.hasError());
  if (result.hasError()) {
    EXPECT_EQ(
        result.error().errorCode, SubscribeTracksErrorCode::INTERNAL_ERROR);
    EXPECT_EQ(result.error().reasonPhrase, "boom");
  }

  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

CO_TEST_P_X(V18PlusSubscribeTracksTest, SubscribeTracksAppThrows) {
  co_await setupMoQSession();

  EXPECT_CALL(*serverPublisher, subscribeTracks(_, _))
      .WillOnce(
          testing::Invoke(
              [](auto /*subTracks*/, auto /*publishBlockedHandle*/)
                  -> folly::coro::Task<Publisher::SubscribeTracksResult> {
                throw std::runtime_error("kaboom");
                co_return folly::makeUnexpected(SubscribeTracksError{});
              }));

  auto result = co_await clientSession_->subscribeTracks(getSubscribeTracks());
  EXPECT_TRUE(result.hasError());
  if (result.hasError()) {
    EXPECT_EQ(
        result.error().errorCode, SubscribeTracksErrorCode::INTERNAL_ERROR);
  }

  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

CO_TEST_P_X(V18PlusSubscribeTracksTest, UnsubscribeTracksAfterSessionClosed) {
  co_await setupMoQSession();

  EXPECT_CALL(*serverPublisher, subscribeTracks(_, _))
      .WillOnce(
          testing::Invoke(
              [](auto subTracks, auto /*publishBlockedHandle*/)
                  -> folly::coro::Task<Publisher::SubscribeTracksResult> {
                co_return std::make_shared<MockSubscribeTracksHandle>(
                    SubscribeTracksOk(
                        {.requestID = subTracks.requestID,
                         .requestSpecificParams = {}}));
              }));

  auto result = co_await clientSession_->subscribeTracks(getSubscribeTracks());
  EXPECT_FALSE(result.hasError());

  // Closing the session first then unsubscribing should be a no-op (not a
  // crash).
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
  EXPECT_NO_THROW(result.value()->unsubscribeTracks());
}

// A subscribeTracks whose caller is cancelled before the reply must release
// the caller's handle and end the subscription at the peer.
CO_TEST_P_X(
    V18PlusSubscribeTracksTest,
    SubscribeTracksCallerCancelledBeforeOk) {
  co_await setupMoQSession();

  folly::coro::Baton serverSawSubscribeTracks;
  folly::coro::Baton releaseHandler;
  bool serverSawUnsubscribe = false;
  std::shared_ptr<MockSubscribeTracksHandle> serverHandle;
  EXPECT_CALL(*serverPublisher, subscribeTracks(_, _))
      .WillOnce(
          [&](auto subTracks, auto /* publishBlockedHandle */)
              -> folly::coro::Task<Publisher::SubscribeTracksResult> {
            serverSawSubscribeTracks.post();
            co_await releaseHandler;
            serverHandle =
                std::make_shared<MockSubscribeTracksHandle>(SubscribeTracksOk(
                    {.requestID = subTracks.requestID,
                     .requestSpecificParams = {}}));
            EXPECT_CALL(*serverHandle, unsubscribeTracks()).WillRepeatedly([&] {
              serverSawUnsubscribe = true;
            });
            co_return serverHandle;
          });

  auto clientHandle =
      std::make_shared<testing::StrictMock<MockPublishBlockedHandle>>();
  std::weak_ptr<Publisher::PublishBlockedHandle> weakClientHandle =
      clientHandle;
  folly::CancellationSource cancelSource;
  auto subscribeTracksFut =
      folly::coro::co_withExecutor(
          &eventBase_,
          folly::coro::co_withCancellation(
              cancelSource.getToken(),
              clientSession_->subscribeTracks(
                  getSubscribeTracks(), std::move(clientHandle))))
          .start()
          .via(&eventBase_);
  co_await serverSawSubscribeTracks;

  cancelSource.requestCancellation();
  EXPECT_THROW(
      co_await std::move(subscribeTracksFut), folly::OperationCancelled);
  releaseHandler.post();
  co_await folly::coro::sleep(std::chrono::milliseconds(200));

  EXPECT_TRUE(weakClientHandle.expired());
  EXPECT_TRUE(serverSawUnsubscribe);
  if (serverHandle) {
    testing::Mock::VerifyAndClearExpectations(serverHandle.get());
  }
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

// The SUBSCRIBE_TRACKS reply is read in the same event-loop turn in which the
// caller is cancelled, so it is processed before the caller resumes. The
// cancellation must still release the caller's handle and end the
// subscription at the peer.
CO_TEST_P_X(V18PlusSubscribeTracksTest, SubscribeTracksCancelledWhileOkQueued) {
  co_await setupMoQSession();

  folly::coro::Baton serverSawSubscribeTracks;
  folly::coro::Baton releaseHandler;
  bool serverSawUnsubscribe = false;
  std::shared_ptr<MockSubscribeTracksHandle> serverHandle;
  EXPECT_CALL(*serverPublisher, subscribeTracks(_, _))
      .WillOnce(
          [&](auto subTracks, auto /* publishBlockedHandle */)
              -> folly::coro::Task<Publisher::SubscribeTracksResult> {
            serverSawSubscribeTracks.post();
            co_await releaseHandler;
            serverHandle =
                std::make_shared<MockSubscribeTracksHandle>(SubscribeTracksOk(
                    {.requestID = subTracks.requestID,
                     .requestSpecificParams = {}}));
            EXPECT_CALL(*serverHandle, unsubscribeTracks()).WillRepeatedly([&] {
              serverSawUnsubscribe = true;
            });
            co_return serverHandle;
          });

  auto clientHandle =
      std::make_shared<testing::StrictMock<MockPublishBlockedHandle>>();
  std::weak_ptr<Publisher::PublishBlockedHandle> weakClientHandle =
      clientHandle;
  folly::CancellationSource cancelSource;
  auto subscribeTracksFut =
      folly::coro::co_withExecutor(
          &eventBase_,
          folly::coro::co_withCancellation(
              cancelSource.getToken(),
              clientSession_->subscribeTracks(
                  getSubscribeTracks(), std::move(clientHandle))))
          .start()
          .via(&eventBase_);
  co_await serverSawSubscribeTracks;

  // The request's own bidi is the only client-initiated one on draft 18.
  std::shared_ptr<proxygen::test::FakeStreamHandle> replyStream;
  for (const auto& [id, handle] : serverWt_->writeHandles) {
    if (id % 4 == 0) {
      replyStream = handle;
    }
  }
  EXPECT_NE(replyStream, nullptr);
  if (!replyStream) {
    clientSession_->close(SessionCloseErrorCode::NO_ERROR);
    co_return;
  }
  replyStream->setImmediateDelivery(false);
  releaseHandler.post();
  for (int i = 0; i < 10 && replyStream->inflightBuf_.empty(); ++i) {
    co_await folly::coro::co_reschedule_on_current_executor;
  }
  EXPECT_FALSE(replyStream->inflightBuf_.empty());
  replyStream->setImmediateDelivery(true);
  replyStream->deliverInflightData();
  cancelSource.requestCancellation();
  EXPECT_THROW(
      co_await std::move(subscribeTracksFut), folly::OperationCancelled);
  co_await folly::coro::sleep(std::chrono::milliseconds(200));

  EXPECT_TRUE(weakClientHandle.expired());
  EXPECT_TRUE(serverSawUnsubscribe);
  if (serverHandle) {
    testing::Mock::VerifyAndClearExpectations(serverHandle.get());
  }
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

INSTANTIATE_TEST_SUITE_P(
    V18PlusSubscribeTracksTest,
    V18PlusSubscribeTracksTest,
    testing::Values(VersionParams{{kVersionDraft18}, kVersionDraft18}));

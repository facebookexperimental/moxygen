/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/mlog/MLogger.h"
#include "moxygen/test/MoQSessionTestCommon.h"

using namespace moxygen;
using namespace moxygen::test;
using testing::_;

// === MLOG CONTROL MESSAGE RECORDS ===
//
// The frame hooks in MoQFrameWriter and the control codecs produce these
// records. Each side records each control message once: Sent when it is
// serialized, Received when it is parsed.
namespace {
class RecordingMLogger : public MLogger {
 public:
  explicit RecordingMLogger(VantagePoint vp) : MLogger(vp) {}
  void outputLogs() override {}

  struct StreamSignal {
    bool reset; // false: STOP_SENDING
    Direction dir;
    uint64_t streamID;
    uint64_t errorCode;
  };
  std::vector<StreamSignal> signals;

  void onResetStream(Direction dir, uint64_t streamID, uint64_t code) override {
    signals.push_back({true, dir, streamID, code});
  }
  void onStopSending(Direction dir, uint64_t streamID, uint64_t code) override {
    signals.push_back({false, dir, streamID, code});
  }

  std::vector<std::pair<Direction, uint64_t>> fins;
  void onStreamFin(Direction dir, uint64_t streamID) override {
    fins.emplace_back(dir, streamID);
  }
  bool hasFin(Direction dir, uint64_t streamID) const {
    return std::find(fins.begin(), fins.end(), std::make_pair(dir, streamID)) !=
        fins.end();
  }

  std::vector<Owner> controlStreamOwners(uint64_t streamID) const {
    std::vector<Owner> owners;
    for (const auto& event : logs_) {
      if (const auto* rec = std::get_if<MOQTStreamTypeSet>(&event.data_); rec &&
          rec->streamType == MOQTStreamType::CONTROL &&
          rec->streamId == streamID && rec->owner) {
        owners.push_back(*rec->owner);
      }
    }
    return owners;
  }

  const StreamSignal* findSignal(bool reset, Direction dir) const {
    for (const auto& s : signals) {
      if (s.reset == reset && s.dir == dir) {
        return &s;
      }
    }
    return nullptr;
  }

  size_t countControlMessages(const std::string& type) const {
    size_t count = 0;
    for (const auto& event : logs_) {
      const auto* rec = std::get_if<MOQTControlMessage>(&event.data_);
      if (rec && rec->message && rec->message->type == type) {
        count++;
      }
    }
    return count;
  }

  std::optional<uint64_t> streamOf(const std::string& type, bool created)
      const {
    const auto name =
        created ? kControlMessageCreatedName : kControlMessageParsedName;
    for (const auto& event : logs_) {
      const auto* rec = std::get_if<MOQTControlMessage>(&event.data_);
      if (rec && event.name_ == name && rec->message &&
          rec->message->type == type) {
        return rec->streamId;
      }
    }
    return std::nullopt;
  }
};
} // namespace

CO_TEST_P_X(MoQSessionTest, MLogRecordsControlMessagesExactlyOnce) {
  auto clientLogger = std::make_shared<RecordingMLogger>(VantagePoint::CLIENT);
  auto serverLogger = std::make_shared<RecordingMLogger>(VantagePoint::SERVER);

  clientSession_->setLogger(clientLogger);
  serverSession_->setLogger(serverLogger);
  co_await setupMoQSession();

  std::shared_ptr<MockPublishNamespaceHandle> mockPublishNamespaceHandle;
  std::shared_ptr<moxygen::Subscriber::PublishNamespaceCallback>
      publishNamespaceCallback;
  EXPECT_CALL(*serverSubscriber, publishNamespace(_, _))
      .WillOnce(
          [&mockPublishNamespaceHandle, &publishNamespaceCallback](
              auto ann, auto publishNamespaceCallbackIn)
              -> folly::coro::Task<Subscriber::PublishNamespaceResult> {
            publishNamespaceCallback = publishNamespaceCallbackIn;
            mockPublishNamespaceHandle =
                std::make_shared<MockPublishNamespaceHandle>(PublishNamespaceOk(
                    {.requestID = ann.requestID, .requestSpecificParams = {}}));
            co_return Subscriber::PublishNamespaceResult(
                mockPublishNamespaceHandle);
          });

  EXPECT_CALL(*clientPublisherStatsCallback_, onPublishNamespaceSuccess());
  EXPECT_CALL(*serverSubscriberStatsCallback_, onPublishNamespaceSuccess());
  auto mockPublishNamespaceCallback =
      std::make_shared<MockPublishNamespaceCallback>();
  auto publishNamespaceResult = co_await clientSession_->publishNamespace(
      getPublishNamespace(), mockPublishNamespaceCallback);
  EXPECT_FALSE(publishNamespaceResult.hasError());

  EXPECT_CALL(*clientPublisherStatsCallback_, onPublishNamespaceCancel());
  EXPECT_CALL(*serverSubscriberStatsCallback_, onPublishNamespaceCancel());
  folly::coro::Baton barricade;
  EXPECT_CALL(*mockPublishNamespaceCallback, publishNamespaceCancel(_, _))
      .WillOnce(
          [&barricade](moxygen::PublishNamespaceErrorCode, const std::string&) {
            barricade.post();
          });
  publishNamespaceCallback->publishNamespaceCancel(
      PublishNamespaceErrorCode::UNINTERESTED, "Not interested!");
  co_await barricade;

  // MLogTypes.h mixes naming styles ("publish_namespace" vs
  // "publishNamespace_cancel"). A misspelled type does not match any record.
  // Draft 18 cancels by resetting the request stream, which mlog does not
  // record.
  EXPECT_EQ(clientLogger->countControlMessages("publish_namespace"), 1u);
  EXPECT_EQ(serverLogger->countControlMessages("publish_namespace"), 1u);
  // A reply is recorded on the stream that carried its request: the control
  // stream before draft 18, the request's bidi stream after.
  const auto controlStream = clientLogger->streamOf("setup", true);
  const auto requestStream = clientLogger->streamOf("publish_namespace", true);
  EXPECT_TRUE(controlStream.has_value());
  EXPECT_TRUE(requestStream.has_value());
  if (getDraftMajorVersion(getServerSelectedVersion()) < 18) {
    EXPECT_EQ(requestStream, controlStream);
  } else {
    EXPECT_NE(requestStream, controlStream);
  }
  EXPECT_EQ(
      clientLogger->streamOf("request_ok", false),
      clientLogger->streamOf("publish_namespace", true));
  EXPECT_TRUE(serverLogger->streamOf("publish_namespace", false).has_value());
  EXPECT_EQ(
      serverLogger->streamOf("request_ok", true),
      serverLogger->streamOf("publish_namespace", false));
  const size_t expectedCancels =
      getDraftMajorVersion(getServerSelectedVersion()) < 18 ? 1 : 0;
  EXPECT_EQ(
      clientLogger->countControlMessages("publishNamespace_cancel"),
      expectedCancels);
  EXPECT_EQ(
      serverLogger->countControlMessages("publishNamespace_cancel"),
      expectedCancels);

  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

// === MLOG REQUEST REPLIES ===
//
// Each test runs one request on a fresh session, so each record type appears
// at most once per side. Both sides record the reply on the stream that
// carried its request.
class MLogReplyTest : public MoQSessionTest {
 protected:
  using Exchange = folly::Function<folly::coro::Task<void>()>;

  // Sets up a session with recording loggers, runs `exchange`, and checks the
  // request and reply records on both sides.
  folly::coro::Task<void> expectReply(
      std::string request,
      std::string reply,
      Exchange exchange,
      bool publishSession = false) {
    clientSession_->setLogger(clientLogger_);
    serverSession_->setLogger(serverLogger_);
    if (publishSession) {
      co_await setupMoQSessionForPublish(initialMaxRequestID_);
    } else {
      co_await setupMoQSession();
    }
    co_await exchange();
    expectRecords(*clientLogger_, *serverLogger_, request, reply);
    clientSession_->close(SessionCloseErrorCode::NO_ERROR);
  }

  bool isDraft18OrLater() {
    return getDraftMajorVersion(getServerSelectedVersion()) >= 18;
  }

  // Each exchange below sends one request, which the peer accepts or rejects.

  folly::coro::Task<void> subscribe(bool accept) {
    std::shared_ptr<TrackConsumer> trackConsumer;
    expectSubscribe(
        [accept, &trackConsumer](auto sub, auto pub) -> TaskSubscribeResult {
          if (!accept) {
            co_return folly::makeUnexpected(
                SubscribeError{
                    sub.requestID, SubscribeErrorCode::UNAUTHORIZED, "no"});
          }
          trackConsumer = pub;
          co_return makeSubscribeOkResult(sub, AbsoluteLocation{0, 0});
        },
        MoQControlCodec::Direction::SERVER,
        accept ? std::nullopt
               : std::optional(SubscribeErrorCode::UNAUTHORIZED));
    auto subscribeRequest = getSubscribe(kTestTrackName);
    auto res = co_await clientSession_->subscribe(
        subscribeRequest, subscribeCallback_);
    EXPECT_EQ(res.hasValue(), accept);
    if (trackConsumer) {
      expectPublishDone();
      trackConsumer->publishDone(
          getTrackEndedPublishDone(subscribeRequest.requestID));
      co_await publishDone_;
    }
  }

  folly::coro::Task<void> fetch(bool accept) {
    folly::coro::Baton objectReceived;
    if (accept) {
      expectFetch([](Fetch fetch, auto fetchPub) -> TaskFetchResult {
        auto* standalone = std::get_if<StandaloneFetch>(&fetch.args);
        EXPECT_NE(standalone, nullptr);
        fetchPub->object(
            standalone->start.group,
            /*subgroupID=*/0,
            standalone->start.object,
            moxygen::test::makeBuf(10),
            noExtensions(),
            /*finFetch=*/true);
        co_return makeFetchOkResult(fetch, AbsoluteLocation{100, 100});
      });
      EXPECT_CALL(*fetchCallback_, object(0, 0, 0, _, _, true, _))
          .WillOnce([&] {
            objectReceived.post();
            return folly::unit;
          });
    } else {
      expectFetch(
          [](Fetch fetch, auto) -> TaskFetchResult {
            co_return folly::makeUnexpected(
                FetchError{
                    fetch.requestID, FetchErrorCode::DOES_NOT_EXIST, "no"});
          },
          FetchErrorCode::DOES_NOT_EXIST);
    }
    auto res = co_await clientSession_->fetch(
        getFetch({0, 0}, {0, 1}), fetchCallback_);
    EXPECT_EQ(res.hasValue(), accept);
    if (accept) {
      co_await objectReceived;
    }
  }

  folly::coro::Task<void> publish(bool accept) {
    EXPECT_CALL(*serverSubscriber, publish(_, _))
        .WillOnce(
            [accept](
                const PublishRequest& pub, std::shared_ptr<SubscriptionHandle>)
                -> Subscriber::PublishResult {
              if (!accept) {
                return folly::makeUnexpected(
                    PublishError{
                        pub.requestID, PublishErrorCode::UNAUTHORIZED, "no"});
              }
              return makePublishOkResult(pub);
            });
    PublishRequest pub{
        RequestID(0),
        FullTrackName{TrackNamespace{{"test"}}, "test-track"},
        TrackAlias(100),
        GroupOrder::OldestFirst,
        AbsoluteLocation{0, 100},
        true,
        Extensions{},
    };
    auto result = clientSession_->publish(std::move(pub), makePublishHandle());
    EXPECT_TRUE(result.hasValue());
    if (result.hasValue()) {
      auto reply = co_await std::move(result.value().reply);
      EXPECT_EQ(reply.hasValue(), accept);
    }
  }

  folly::coro::Task<void> trackStatus(bool accept) {
    EXPECT_CALL(*serverPublisher, trackStatus(_))
        .WillOnce(
            [accept](TrackStatus request)
                -> folly::coro::Task<Publisher::TrackStatusResult> {
              if (!accept) {
                co_return folly::makeUnexpected(
                    TrackStatusError{
                        request.requestID,
                        TrackStatusErrorCode::DOES_NOT_EXIST,
                        "no"});
              }
              co_return makeTrackStatusOkResult(
                  request, AbsoluteLocation{0, 0});
            });
    auto res = co_await clientSession_->trackStatus(getTrackStatus());
    EXPECT_EQ(res.hasValue(), accept);
  }

  // Subscribes, then sends one REQUEST_UPDATE. A rejected update ends the
  // subscription.
  folly::coro::Task<void> requestUpdate(bool accept) {
    std::shared_ptr<MockSubscriptionHandle> handle;
    std::shared_ptr<TrackConsumer> trackConsumer;
    expectSubscribe(
        [&handle, &trackConsumer](auto sub, auto pub) -> TaskSubscribeResult {
          trackConsumer = pub;
          handle = makeSubscribeOkResult(sub, AbsoluteLocation{15, 20});
          co_return handle;
        });
    auto subscribeRequest = getSubscribe(kTestTrackName);
    auto res = co_await clientSession_->subscribe(
        subscribeRequest, subscribeCallback_);
    EXPECT_FALSE(res.hasError());
    if (res.hasError()) {
      co_return;
    }

    folly::coro::Baton updateInvoked;
    EXPECT_CALL(*handle, requestUpdateCalled).WillOnce([&](const auto&) {
      updateInvoked.post();
    });
    folly::Expected<RequestOk, RequestError> result =
        RequestOk{.requestID = subscribeRequest.requestID};
    if (!accept) {
      result = folly::makeUnexpected(
          RequestError{
              subscribeRequest.requestID,
              RequestErrorCode::NOT_SUPPORTED,
              "no"});
    }
    EXPECT_CALL(*handle, requestUpdateResult).WillOnce(testing::Return(result));
    folly::coro::Baton subscriptionEnded;
    if (accept) {
      expectPublishDone();
    } else {
      EXPECT_CALL(*subscribeCallback_, publishDone(_))
          .WillOnce([&subscriptionEnded](const PublishDone&) {
            subscriptionEnded.post();
            return folly::Expected<folly::Unit, MoQPublishError>(folly::unit);
          });
    }

    co_await res.value()->requestUpdate(
        SubscribeUpdate{
            subscribeRequest.requestID,
            RequestID(0),
            LocationFilter::range({20, 10}, {100, 0}),
            kDefaultPriority + 1,
            true,
            /*fill=*/std::nullopt});
    co_await updateInvoked;
    if (accept) {
      trackConsumer->publishDone(
          getTrackEndedPublishDone(subscribeRequest.requestID));
      co_await publishDone_;
    } else {
      co_await subscriptionEnded;
    }
  }

  folly::coro::Task<void> publishNamespaceRejected() {
    EXPECT_CALL(*serverSubscriber, publishNamespace(_, _))
        .WillOnce(
            [](auto pubNs,
               auto) -> folly::coro::Task<Subscriber::PublishNamespaceResult> {
              co_return folly::makeUnexpected(
                  PublishNamespaceError{
                      pubNs.requestID,
                      PublishNamespaceErrorCode::UNAUTHORIZED,
                      "no"});
            });
    auto res = co_await clientSession_->publishNamespace(getPublishNamespace());
    EXPECT_TRUE(res.hasError());
  }

  folly::coro::Task<void> subscribeNamespace(bool accept) {
    EXPECT_CALL(*serverPublisher, subscribeNamespace(_, _))
        .WillOnce(
            [accept](auto subNs, auto)
                -> folly::coro::Task<Publisher::SubscribeNamespaceResult> {
              if (!accept) {
                co_return folly::makeUnexpected(
                    SubscribeNamespaceError{
                        subNs.requestID,
                        SubscribeNamespaceErrorCode::NOT_SUPPORTED,
                        "no"});
              }
              co_return std::make_shared<MockSubscribeNamespaceHandle>(
                  SubscribeNamespaceOk(
                      {.requestID = subNs.requestID,
                       .requestSpecificParams = {}}));
            });
    auto res = co_await clientSession_->subscribeNamespace(
        getSubscribeNamespace(), nullptr);
    EXPECT_EQ(res.hasValue(), accept);
  }

  folly::coro::Task<void> subscribeTracks(bool accept) {
    EXPECT_CALL(*serverPublisher, subscribeTracks(_, _))
        .WillOnce(
            [accept](
                auto subTracks,
                auto) -> folly::coro::Task<Publisher::SubscribeTracksResult> {
              if (!accept) {
                co_return folly::makeUnexpected(
                    SubscribeTracksError{
                        subTracks.requestID,
                        SubscribeTracksErrorCode::INTERNAL_ERROR,
                        "no"});
              }
              co_return std::make_shared<MockSubscribeTracksHandle>(
                  SubscribeTracksOk(
                      {.requestID = subTracks.requestID,
                       .requestSpecificParams = {}}));
            });
    auto res = co_await clientSession_->subscribeTracks(getSubscribeTracks());
    EXPECT_EQ(res.hasValue(), accept);
  }

 private:
  static void expectRecords(
      const RecordingMLogger& requester,
      const RecordingMLogger& responder,
      const std::string& request,
      const std::string& reply) {
    EXPECT_EQ(requester.countControlMessages(request), 1u) << request;
    EXPECT_EQ(requester.countControlMessages(reply), 1u) << reply;
    EXPECT_EQ(responder.countControlMessages(request), 1u) << request;
    EXPECT_EQ(responder.countControlMessages(reply), 1u) << reply;

    const auto sentRequest = requester.streamOf(request, /*created=*/true);
    const auto parsedRequest = responder.streamOf(request, /*created=*/false);
    ASSERT_TRUE(sentRequest.has_value()) << request;
    ASSERT_TRUE(parsedRequest.has_value()) << request;
    EXPECT_EQ(requester.streamOf(reply, /*created=*/false), sentRequest)
        << reply;
    EXPECT_EQ(responder.streamOf(reply, /*created=*/true), parsedRequest)
        << reply;
  }

  std::shared_ptr<RecordingMLogger> clientLogger_{
      std::make_shared<RecordingMLogger>(VantagePoint::CLIENT)};
  std::shared_ptr<RecordingMLogger> serverLogger_{
      std::make_shared<RecordingMLogger>(VantagePoint::SERVER)};
};

CO_TEST_P_X(MLogReplyTest, SubscribeOk) {
  co_await expectReply(
      "subscribe", "subscribe_ok", [this] { return subscribe(true); });
}

CO_TEST_P_X(MLogReplyTest, SubscribeError) {
  co_await expectReply(
      "subscribe", "request_error", [this] { return subscribe(false); });
}

CO_TEST_P_X(MLogReplyTest, FetchOk) {
  co_await expectReply("fetch", "fetch_ok", [this] { return fetch(true); });
}

CO_TEST_P_X(MLogReplyTest, FetchError) {
  co_await expectReply(
      "fetch", "request_error", [this] { return fetch(false); });
}

// mlog records PUBLISH_OK as a request_ok on every draft.
CO_TEST_P_X(MLogReplyTest, PublishOk) {
  co_await expectReply(
      "publish",
      "request_ok",
      [this] { return publish(true); },
      /*publishSession=*/true);
}

CO_TEST_P_X(MLogReplyTest, PublishError) {
  co_await expectReply(
      "publish",
      "request_error",
      [this] { return publish(false); },
      /*publishSession=*/true);
}

CO_TEST_P_X(MLogReplyTest, TrackStatusOk) {
  co_await expectReply(
      "track_status", "request_ok", [this] { return trackStatus(true); });
}

// Before draft 18 the error is a TRACK_STATUS_ERROR frame. mlog records it
// as a request_error.
CO_TEST_P_X(MLogReplyTest, TrackStatusError) {
  co_await expectReply(
      "track_status", "request_error", [this] { return trackStatus(false); });
}

CO_TEST_P_X(MLogReplyTest, RequestUpdateOk) {
  co_await expectReply(
      "request_update", "request_ok", [this] { return requestUpdate(true); });
}

CO_TEST_P_X(MLogReplyTest, RequestUpdateError) {
  co_await expectReply("request_update", "request_error", [this] {
    return requestUpdate(false);
  });
}

// MLogRecordsControlMessagesExactlyOnce covers PUBLISH_NAMESPACE's OK.
CO_TEST_P_X(MLogReplyTest, PublishNamespaceError) {
  co_await expectReply("publish_namespace", "request_error", [this] {
    return publishNamespaceRejected();
  });
}

CO_TEST_P_X(MLogReplyTest, SubscribeNamespaceOk) {
  co_await expectReply("subscribe_namespace", "request_ok", [this] {
    return subscribeNamespace(true);
  });
}

CO_TEST_P_X(MLogReplyTest, SubscribeNamespaceError) {
  co_await expectReply("subscribe_namespace", "request_error", [this] {
    return subscribeNamespace(false);
  });
}

// SUBSCRIBE_TRACKS exists from draft 18.
CO_TEST_P_X(MLogReplyTest, SubscribeTracksOk) {
  if (isDraft18OrLater()) {
    co_await expectReply("subscribe_tracks", "request_ok", [this] {
      return subscribeTracks(true);
    });
  }
}

CO_TEST_P_X(MLogReplyTest, SubscribeTracksError) {
  if (isDraft18OrLater()) {
    co_await expectReply("subscribe_tracks", "request_error", [this] {
      return subscribeTracks(false);
    });
  }
}

INSTANTIATE_TEST_SUITE_P(
    MLogReplyTest,
    MLogReplyTest,
    testing::ValuesIn(getSupportedVersionParams()));

// === STREAM SIGNALS ===

CO_TEST_P_X(MoQSessionTest, DataStreamResetIsObservedOnBothSides) {
  auto clientLogger = std::make_shared<RecordingMLogger>(VantagePoint::CLIENT);
  auto serverLogger = std::make_shared<RecordingMLogger>(VantagePoint::SERVER);
  clientSession_->setLogger(clientLogger);
  serverSession_->setLogger(serverLogger);
  co_await publishValidationTest(
      [](const auto& sub, auto pub, auto sgp, auto sgc) {
        EXPECT_CALL(*sgc, beginObject(1, 100, _, _))
            .WillOnce(
                testing::Return(
                    folly::Expected<folly::Unit, MoQPublishError>(
                        folly::unit)));
        EXPECT_TRUE(sgp->beginObject(1, 100, test::makeBuf(10)));
        sgp->reset(ResetStreamErrorCode::INTERNAL_ERROR);
        pub->publishDone(getTrackEndedPublishDone(sub.requestID));
      });

  using Dir = MoQFrameObserver::Direction;
  const auto* sent = serverLogger->findSignal(true, Dir::Sent);
  const auto* received = clientLogger->findSignal(true, Dir::Received);
  CO_ASSERT_NE(sent, nullptr);
  CO_ASSERT_NE(received, nullptr);
  EXPECT_EQ(sent->streamID, serverObjectStreamId());
  EXPECT_EQ(received->streamID, sent->streamID);
  EXPECT_EQ(received->errorCode, sent->errorCode);
}

CO_TEST_P_X(Draft18Test, UnsubscribeResetsTheRequestStream) {
  auto clientLogger = std::make_shared<RecordingMLogger>(VantagePoint::CLIENT);
  auto serverLogger = std::make_shared<RecordingMLogger>(VantagePoint::SERVER);
  clientSession_->setLogger(clientLogger);
  serverSession_->setLogger(serverLogger);
  co_await setupMoQSession();
  std::shared_ptr<TrackConsumer> publisher;
  expectSubscribe([&publisher](auto sub, auto pub) -> TaskSubscribeResult {
    publisher = pub;
    co_return makeSubscribeOkResult(sub);
  });
  auto handle = co_await clientSession_->subscribe(
      getSubscribe(kTestTrackName), subscribeCallback_);
  CO_ASSERT_TRUE(handle.hasValue());
  auto requestStream = clientLogger->streamOf("subscribe", /*created=*/true);
  CO_ASSERT_TRUE(requestStream.has_value());

  handle.value()->unsubscribe();
  co_await rescheduleN(4);

  using Dir = MoQFrameObserver::Direction;
  auto expectOnRequestStream = [&](const auto* signal, const char* what) {
    EXPECT_NE(signal, nullptr) << what;
    if (signal) {
      EXPECT_EQ(signal->streamID, *requestStream) << what;
    }
  };
  expectOnRequestStream(
      clientLogger->findSignal(true, Dir::Sent), "client sent reset");
  expectOnRequestStream(
      serverLogger->findSignal(true, Dir::Received), "server received reset");
  expectOnRequestStream(
      clientLogger->findSignal(false, Dir::Sent), "client sent STOP_SENDING");
  expectOnRequestStream(
      serverLogger->findSignal(false, Dir::Received),
      "server received STOP_SENDING");
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

CO_TEST_P_X(Draft18Test, ReceivedPaddingDatagramReportsItsLength) {
  struct PaddingLogger : RecordingMLogger {
    using RecordingMLogger::RecordingMLogger;
    std::vector<uint64_t> lengths;
    void onPaddingDatagram(Direction dir, uint64_t paddingLength) override {
      if (dir == Direction::Received) {
        lengths.push_back(paddingLength);
      }
    }
  };
  auto observer = std::make_shared<PaddingLogger>(VantagePoint::CLIENT);
  clientSession_->setLogger(observer);
  co_await setupMoQSession();
  MoQFrameWriter writer;
  writer.initializeVersion(kVersionDraft18);
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  CO_ASSERT_TRUE(writer.writePaddingDatagram(writeBuf, 8).hasValue());

  clientSession_->onDatagram(writeBuf.move());

  EXPECT_EQ(observer->lengths, std::vector<uint64_t>{8});
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

CO_TEST_P_X(Draft18Test, TrackStatusFinIsObservedOnBothSides) {
  auto clientLogger = std::make_shared<RecordingMLogger>(VantagePoint::CLIENT);
  auto serverLogger = std::make_shared<RecordingMLogger>(VantagePoint::SERVER);
  clientSession_->setLogger(clientLogger);
  serverSession_->setLogger(serverLogger);
  co_await setupMoQSession();
  EXPECT_CALL(*serverPublisher, trackStatus(_))
      .WillOnce(
          [](TrackStatus request)
              -> folly::coro::Task<Publisher::TrackStatusResult> {
            co_return makeTrackStatusOkResult(request, AbsoluteLocation{0, 0});
          });
  auto res = co_await clientSession_->trackStatus(getTrackStatus());
  EXPECT_FALSE(res.hasError());
  co_await rescheduleN(2);

  // The client FINs after TRACK_STATUS, and the server after its reply.
  using Dir = MoQFrameObserver::Direction;
  auto stream = clientLogger->streamOf("track_status", /*created=*/true);
  CO_ASSERT_TRUE(stream.has_value());
  EXPECT_TRUE(clientLogger->hasFin(Dir::Sent, *stream));
  EXPECT_TRUE(serverLogger->hasFin(Dir::Received, *stream));
  EXPECT_TRUE(serverLogger->hasFin(Dir::Sent, *stream));
  EXPECT_TRUE(clientLogger->hasFin(Dir::Received, *stream));
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

CO_TEST_P_X(MoQSessionTest, FetchDataFinIsObservedOnBothSides) {
  auto clientLogger = std::make_shared<RecordingMLogger>(VantagePoint::CLIENT);
  auto serverLogger = std::make_shared<RecordingMLogger>(VantagePoint::SERVER);
  clientSession_->setLogger(clientLogger);
  serverSession_->setLogger(serverLogger);
  co_await setupMoQSession();
  expectFetch([](Fetch fetch, auto fetchPub) -> TaskFetchResult {
    fetchPub->object(
        0, 0, 0, test::makeBuf(10), noExtensions(), /*finFetch=*/true);
    co_return makeFetchOkResult(fetch, AbsoluteLocation{0, 0});
  });
  EXPECT_CALL(*fetchCallback_, object(0, 0, 0, _, _, true, _))
      .WillOnce(testing::Return(folly::unit));
  auto res =
      co_await clientSession_->fetch(getFetch({0, 0}, {0, 1}), fetchCallback_);
  EXPECT_FALSE(res.hasError());
  co_await rescheduleN(2);

  using Dir = MoQFrameObserver::Direction;
  auto stream = serverObjectStreamId();
  EXPECT_TRUE(serverLogger->hasFin(Dir::Sent, stream));
  EXPECT_TRUE(clientLogger->hasFin(Dir::Received, stream));
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

CO_TEST_P_X(Draft18Test, RequestStreamTypeIsLoggedOnceBySide) {
  auto clientLogger = std::make_shared<RecordingMLogger>(VantagePoint::CLIENT);
  auto serverLogger = std::make_shared<RecordingMLogger>(VantagePoint::SERVER);
  clientSession_->setLogger(clientLogger);
  serverSession_->setLogger(serverLogger);
  co_await setupMoQSession();
  expectSubscribe([](auto sub, auto) -> TaskSubscribeResult {
    co_return makeSubscribeOkResult(sub);
  });
  auto handle = co_await clientSession_->subscribe(
      getSubscribe(kTestTrackName), subscribeCallback_);
  CO_ASSERT_TRUE(handle.hasValue());
  auto requestStream = clientLogger->streamOf("subscribe", /*created=*/true);
  CO_ASSERT_TRUE(requestStream.has_value());

  EXPECT_EQ(
      clientLogger->controlStreamOwners(*requestStream),
      std::vector<Owner>{Owner::LOCAL});
  EXPECT_EQ(
      serverLogger->controlStreamOwners(*requestStream),
      std::vector<Owner>{Owner::REMOTE});
  EXPECT_CALL(*subscribeCallback_, publishDone(_))
      .WillRepeatedly(testing::Return(folly::unit));
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

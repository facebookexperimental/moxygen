/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <folly/Singleton.h>
#include <folly/coro/BlockingWait.h>
#include <folly/coro/Task.h>
#include <folly/coro/Timeout.h>
#include <folly/io/async/EventBase.h>
#include <folly/io/async/ScopedEventBaseThread.h>
#include <folly/portability/GTest.h>
#include <proxygen/httpserver/samples/hq/FizzContext.h>
#include <moxygen/MoQClient.h>
#include <moxygen/MoQConsumers.h>
#include <moxygen/MoQServer.h>
#include <moxygen/MoQSession.h>
#include <moxygen/MoQVersions.h>
#include <moxygen/ObjectReceiver.h>
#include <moxygen/Publisher.h>
#include <moxygen/Subscriber.h>
#include <moxygen/events/MoQFollyExecutorImpl.h>
#include <moxygen/util/InsecureVerifierDangerousDoNotUseInProduction.h>

#include <map>
#include <set>

using namespace std::chrono_literals;

namespace moxygen::test {

namespace {

const std::string kTestEndpoint = "/test";
const std::string kTestPayload = "hello-moq";
const size_t kDefaultObjectCount = 3;
const FullTrackName kIntegrationTestTrackName{
    TrackNamespace({{"integration-test"}}),
    "test-track"};
const FullTrackName kSecondTrackName{
    TrackNamespace({{"integration-test"}}),
    "second-track"};

std::unique_ptr<folly::IOBuf> makePayload(const std::string& data) {
  return folly::IOBuf::copyBuffer(data);
}

std::string payloadToString(const Payload& payload) {
  if (!payload) {
    return "";
  }
  // Coalesce handles chained IOBufs from multi-frame object delivery
  auto coalesced = payload->clone();
  coalesced->coalesce();
  return std::string(
      reinterpret_cast<const char*>(coalesced->data()), coalesced->length());
}

struct ReceivedObject {
  uint64_t group;
  uint64_t subgroup;
  uint64_t id;
  std::string payload;
};

class TestObjectCallback : public ObjectReceiverCallback {
 public:
  FlowControlState onObject(
      std::optional<TrackAlias> /*trackAlias*/,
      const ObjectHeader& objHeader,
      Payload payload) override {
    std::lock_guard<std::mutex> lock(mutex_);
    objects_.push_back(
        ReceivedObject{
            objHeader.group,
            objHeader.subgroup,
            objHeader.id,
            payloadToString(payload)});
    objectBaton_.post();
    return FlowControlState::UNBLOCKED;
  }

  void onObjectStatus(
      std::optional<TrackAlias> /*trackAlias*/,
      const ObjectHeader& objHeader) override {
    std::lock_guard<std::mutex> lock(mutex_);
    lastObjectStatus_ = objHeader.status;
    objectStatusBaton_.post();
  }

  void onEndOfStream() override {
    endOfStream_ = true;
  }

  void onError(ResetStreamErrorCode error) override {
    std::lock_guard<std::mutex> lock(mutex_);
    error_ = error;
  }

  void onPublishDone(PublishDone done) override {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      publishDone_ = std::move(done);
    }
    publishDoneBaton_.post();
  }

  void onAllDataReceived() override {
    allDataReceived_ = true;
    allDataReceivedBaton_.post();
  }

  std::vector<ReceivedObject> getObjects() {
    std::lock_guard<std::mutex> lock(mutex_);
    return objects_;
  }

  folly::coro::Task<void> waitForObjects(size_t count) {
    while (true) {
      objectBaton_.reset();
      {
        std::lock_guard<std::mutex> lock(mutex_);
        if (objects_.size() >= count) {
          co_return;
        }
      }
      co_await objectBaton_;
    }
  }

  folly::coro::Task<void> waitForPublishDone() {
    co_await publishDoneBaton_;
  }

  folly::coro::Task<void> waitForAllDataReceived() {
    co_await allDataReceivedBaton_;
  }

  folly::coro::Task<void> waitForObjectStatus() {
    co_await objectStatusBaton_;
  }

  std::optional<ObjectStatus> getLastObjectStatus() {
    std::lock_guard<std::mutex> lock(mutex_);
    return lastObjectStatus_;
  }

  std::optional<PublishDone> getPublishDone() {
    std::lock_guard<std::mutex> lock(mutex_);
    return publishDone_;
  }

  bool isAllDataReceived() const {
    return allDataReceived_.load();
  }

 private:
  std::mutex mutex_;
  std::optional<PublishDone> publishDone_;
  std::optional<ResetStreamErrorCode> error_;
  std::vector<ReceivedObject> objects_;
  std::optional<ObjectStatus> lastObjectStatus_;
  std::atomic<bool> endOfStream_{false};
  std::atomic<bool> allDataReceived_{false};
  folly::coro::Baton objectBaton_;
  folly::coro::Baton publishDoneBaton_;
  folly::coro::Baton allDataReceivedBaton_;
  folly::coro::Baton objectStatusBaton_;
};

class TestSubscriptionHandle : public Publisher::SubscriptionHandle {
 public:
  explicit TestSubscriptionHandle(SubscribeOk ok)
      : Publisher::SubscriptionHandle(std::move(ok)) {}

  void unsubscribe() override {
    cancelSource_.requestCancellation();
  }

  folly::coro::Task<RequestUpdateResult> requestUpdate(
      RequestUpdate reqUpdate) override {
    co_return folly::makeUnexpected(
        RequestError{
            reqUpdate.requestID,
            RequestErrorCode::NOT_SUPPORTED,
            "not implemented"});
  }

 private:
  folly::CancellationSource cancelSource_;
};

class TestFetchHandle : public Publisher::FetchHandle {
 public:
  explicit TestFetchHandle(FetchOk ok)
      : Publisher::FetchHandle(std::move(ok)) {}

  void fetchCancel() override {
    cancelSource_.requestCancellation();
  }

  folly::coro::Task<RequestUpdateResult> requestUpdate(
      RequestUpdate reqUpdate) override {
    co_return folly::makeUnexpected(
        RequestError{
            reqUpdate.requestID,
            RequestErrorCode::NOT_SUPPORTED,
            "not implemented"});
  }

 private:
  folly::CancellationSource cancelSource_;
};

void sendPublishDone(
    std::shared_ptr<TrackConsumer>& cb,
    RequestID reqID,
    const std::string& reason = "done") {
  PublishDone pubDone;
  pubDone.requestID = reqID;
  pubDone.statusCode = PublishDoneStatusCode::TRACK_ENDED;
  pubDone.reasonPhrase = reason;
  cb->publishDone(std::move(pubDone));
}

// Helper to create a subscribe handler that accepts the subscribe, then
// defers data sending to the next EventBase loop iteration. This is needed
// because trackAlias_ is set by subscribeOkSent() synchronously on the same
// EventBase thread right after this handler's coroutine returns. Using
// runInEventBaseThread schedules the send callback for the next iteration,
// which is guaranteed to execute after subscribeOkSent() completes.
using DataSender =
    std::function<void(std::shared_ptr<TrackConsumer>, RequestID)>;

namespace {
std::atomic<uint64_t> nextTrackAlias{1};
} // namespace

folly::coro::Task<Publisher::SubscribeResult> acceptAndSendDelayed(
    SubscribeRequest sub,
    std::shared_ptr<TrackConsumer> callback,
    DataSender sendFn) {
  SubscribeOk ok;
  ok.requestID = sub.requestID;
  ok.trackAlias = TrackAlias(nextTrackAlias.fetch_add(1));
  ok.expires = 0ms;
  ok.groupOrder = GroupOrder::OldestFirst;

  auto handle = std::make_shared<TestSubscriptionHandle>(ok);

  auto evb = folly::EventBaseManager::get()->getEventBase();
  auto requestID = sub.requestID;
  evb->runInLoop(
      [callback = std::move(callback),
       requestID,
       sendFn = std::move(sendFn)]() { sendFn(callback, requestID); });

  co_return handle;
}

// Server-side Publisher that handles subscribe and fetch requests.
// Uses configurable lambdas; defaults send kDefaultObjectCount objects
// via a single subgroup followed by PublishDone.
class TestPublisher : public Publisher {
 public:
  using SubscribeHandler = std::function<folly::coro::Task<SubscribeResult>(
      SubscribeRequest,
      std::shared_ptr<TrackConsumer>)>;
  using FetchHandler = std::function<
      folly::coro::Task<FetchResult>(Fetch, std::shared_ptr<FetchConsumer>)>;

  void setSubscribeHandler(SubscribeHandler handler) {
    subscribeHandler_ = std::move(handler);
  }

  void setFetchHandler(FetchHandler handler) {
    fetchHandler_ = std::move(handler);
  }

  folly::coro::Task<SubscribeResult> subscribe(
      SubscribeRequest sub,
      std::shared_ptr<TrackConsumer> callback) override {
    if (subscribeHandler_) {
      co_return co_await subscribeHandler_(std::move(sub), std::move(callback));
    }
    co_return co_await defaultSubscribe(std::move(sub), std::move(callback));
  }

  folly::coro::Task<FetchResult> fetch(
      Fetch fetch,
      std::shared_ptr<FetchConsumer> fetchCallback) override {
    if (fetchHandler_) {
      co_return co_await fetchHandler_(
          std::move(fetch), std::move(fetchCallback));
    }
    co_return co_await defaultFetch(std::move(fetch), std::move(fetchCallback));
  }

 private:
  folly::coro::Task<SubscribeResult> defaultSubscribe(
      SubscribeRequest sub,
      std::shared_ptr<TrackConsumer> callback) {
    co_return co_await acceptAndSendDelayed(
        std::move(sub),
        std::move(callback),
        [](std::shared_ptr<TrackConsumer> cb, RequestID reqID) {
          auto sgResult = cb->beginSubgroup(0, 0, kDefaultPriority);
          if (!sgResult.hasError()) {
            auto sg = sgResult.value();
            for (uint64_t i = 0; i < kDefaultObjectCount; ++i) {
              sg->object(i, makePayload(kTestPayload + std::to_string(i)));
            }
            sg->endOfSubgroup();
          }

          sendPublishDone(cb, reqID);
        });
  }

  folly::coro::Task<FetchResult> defaultFetch(
      Fetch fetch,
      std::shared_ptr<FetchConsumer> fetchCallback) {
    FetchOk ok;
    ok.requestID = fetch.requestID;
    ok.groupOrder = GroupOrder::OldestFirst;
    ok.endOfTrack = 0;
    ok.endLocation = AbsoluteLocation{0, kDefaultObjectCount - 1};

    auto handle = std::make_shared<TestFetchHandle>(ok);

    for (uint64_t i = 0; i < kDefaultObjectCount; ++i) {
      auto payload = makePayload(kTestPayload + std::to_string(i));
      fetchCallback->object(0, 0, i, std::move(payload));
    }
    fetchCallback->endOfFetch();

    co_return handle;
  }

  SubscribeHandler subscribeHandler_;
  FetchHandler fetchHandler_;
};

// Server-side Subscriber that handles client publish requests.
// Collects received objects via an ObjectReceiver + TestObjectCallback
// for verification in client-publish tests.
class TestSubscriber : public Subscriber {
 public:
  PublishResult publish(
      PublishRequest pub,
      std::shared_ptr<SubscriptionHandle> /*handle*/) override {
    return defaultPublish(std::move(pub));
  }

  std::shared_ptr<TestObjectCallback> getReceiverCallback() {
    return receiverCallback_;
  }

 private:
  PublishResult defaultPublish(const PublishRequest& pub) {
    receiverCallback_ = std::make_shared<TestObjectCallback>();
    receiver_ = std::make_shared<ObjectReceiver>(
        ObjectReceiver::SUBSCRIBE, receiverCallback_);

    PublishOk publishOk;
    publishOk.requestID = pub.requestID;
    publishOk.forward = true;
    publishOk.subscriberPriority = kDefaultPriority;
    publishOk.groupOrder = GroupOrder::Default;
    publishOk.locType = LocationType::LargestObject;

    auto replyTask =
        folly::coro::makeTask<folly::Expected<PublishOk, PublishError>>(
            std::move(publishOk));

    return PublishConsumerAndReplyTask{
        std::static_pointer_cast<TrackConsumer>(receiver_),
        std::move(replyTask)};
  }

  std::shared_ptr<ObjectReceiver> receiver_;
  std::shared_ptr<TestObjectCallback> receiverCallback_;
};

class TestServer : public MoQServer {
 public:
  TestServer(
      std::shared_ptr<TestPublisher> publisher,
      std::shared_ptr<TestSubscriber> subscriber,
      bool rejectSetup = false)
      : MoQServer(
            quic::samples::createFizzServerContextWithInsecureDefault(
                []() {
                  std::vector<std::string> alpns = {"h3"};
                  auto moqt = getDefaultMoqtProtocols(true);
                  alpns.insert(alpns.end(), moqt.begin(), moqt.end());
                  return alpns;
                }(),
                fizz::server::ClientAuthMode::None,
                "",
                ""),
            kTestEndpoint),
        publisher_(std::move(publisher)),
        subscriber_(std::move(subscriber)),
        rejectSetup_(rejectSetup) {}

  void onNewSession(std::shared_ptr<MoQSession> clientSession) override {
    if (publisher_) {
      clientSession->setPublishHandler(publisher_);
    }
    if (subscriber_) {
      clientSession->setSubscribeHandler(subscriber_);
    }
  }

  folly::Try<Setup> onClientSetup(
      Setup setup,
      const std::shared_ptr<MoQSession>& session) override {
    if (rejectSetup_) {
      return folly::makeTryWith(
          []() -> Setup { throw std::runtime_error("rejected by server"); });
    }
    return MoQServer::onClientSetup(std::move(setup), session);
  }

 private:
  std::shared_ptr<TestPublisher> publisher_;
  std::shared_ptr<TestSubscriber> subscriber_;
  bool rejectSetup_{false};
};

// Helper to build a SubscribeOk for client-side publish tests.
// trackAlias must be unique within the session to avoid collisions.
std::shared_ptr<TestSubscriptionHandle> makeClientPublishHandle(
    TrackAlias alias = TrackAlias(1)) {
  SubscribeOk subOk;
  subOk.requestID = RequestID(0);
  subOk.trackAlias = alias;
  subOk.expires = 0ms;
  subOk.groupOrder = GroupOrder::OldestFirst;
  return std::make_shared<TestSubscriptionHandle>(std::move(subOk));
}

// Helper to build a SubscribeRequest
SubscribeRequest makeSubscribe() {
  SubscribeRequest sub;
  sub.fullTrackName = kIntegrationTestTrackName;
  sub.priority = kDefaultPriority;
  sub.groupOrder = GroupOrder::OldestFirst;
  sub.forward = true;
  sub.locType = LocationType::LargestObject;
  return sub;
}

} // namespace

class MoQIntegrationTest : public ::testing::TestWithParam<uint64_t> {
 public:
  void SetUp() override {
    folly::SingletonVault::singleton()->registrationComplete();

    publisher_ = std::make_shared<TestPublisher>();
    subscriber_ = std::make_shared<TestSubscriber>();

    server_ = std::make_shared<TestServer>(publisher_, subscriber_);
    server_->start(folly::SocketAddress("::", 0));
    server_->waitUntilInitialized();

    auto fds = server_->getAllListeningSocketFDs();
    ASSERT_FALSE(fds.empty());
    folly::SocketAddress boundAddr;
    boundAddr.setFromLocalAddress(folly::NetworkSocket::fromFd(fds[0]));
    serverPort_ = boundAddr.getPort();
  }

  void TearDown() override {
    try {
      if (client_ && client_->moqSession_) {
        clientEvbThread_.getEventBase()->runInEventBaseThreadAndWait([this] {
          client_->moqSession_->close(SessionCloseErrorCode::NO_ERROR);
        });
      }
    } catch (...) {
      // Session may be in a bad state after test failure; ignore close errors.
    }
    client_.reset();
    if (server_) {
      server_->stop();
      server_.reset();
    }
  }

 protected:
  // Run a test coroutine on the client's EventBase thread with a timeout.
  void runTest(folly::coro::Task<void> testBody) {
    auto result = folly::coro::blockingWait(
        folly::coro::co_withExecutor(
            clientEvbThread_.getEventBase(),
            folly::coro::co_invoke(
                [body = std::move(
                     testBody)]() mutable -> folly::coro::Task<std::string> {
                  auto res = co_await folly::coro::co_awaitTry(
                      folly::coro::timeout(std::move(body), 10s));
                  if (res.hasException<folly::FutureTimeout>()) {
                    co_return "Test timed out after 10 seconds";
                  }
                  if (res.hasException()) {
                    co_return res.exception().what().toStdString();
                  }
                  co_return "";
                })));
    EXPECT_TRUE(result.empty()) << result;
  }

  // Subscribe to the test track and return the callback for assertions.
  // Fails the test and returns nullptr if the subscribe is rejected.
  folly::coro::Task<std::shared_ptr<TestObjectCallback>> subscribeAndReceive() {
    auto callback = std::make_shared<TestObjectCallback>();
    auto receiver =
        std::make_shared<ObjectReceiver>(ObjectReceiver::SUBSCRIBE, callback);

    auto result =
        co_await client_->moqSession_->subscribe(makeSubscribe(), receiver);
    EXPECT_FALSE(result.hasError());
    if (result.hasError()) {
      co_return nullptr;
    }
    co_return callback;
  }

  folly::coro::Task<void> connectClient() {
    auto evb = clientEvbThread_.getEventBase();
    auto exec = std::make_shared<MoQFollyExecutorImpl>(evb);

    auto url = proxygen::URL(
        fmt::format("https://localhost:{}{}", serverPort_, kTestEndpoint));

    client_ = std::make_unique<MoQClient>(
        exec,
        std::move(url),
        std::make_shared<InsecureVerifierDangerousDoNotUseInProduction>());

    quic::TransportSettings ts;
    ts.advertisedInitialConnectionFlowControlWindow = 1024 * 1024;
    ts.advertisedInitialBidiLocalStreamFlowControlWindow = 1024 * 1024;
    ts.advertisedInitialBidiRemoteStreamFlowControlWindow = 1024 * 1024;
    ts.advertisedInitialUniStreamFlowControlWindow = 1024 * 1024;

    // Use the parameterized version's ALPN to force a specific protocol version
    auto alpn = getAlpnFromVersion(GetParam());
    std::vector<std::string> alpns;
    if (alpn) {
      alpns.push_back(*alpn);
    }

    co_await client_->setupMoQSession(5s, 5s, nullptr, nullptr, ts, alpns);
  }

  std::shared_ptr<TestPublisher> publisher_;
  std::shared_ptr<TestSubscriber> subscriber_;
  std::shared_ptr<TestServer> server_;
  std::unique_ptr<MoQClient> client_;
  uint16_t serverPort_{0};
  folly::ScopedEventBaseThread clientEvbThread_{"MoQIntegrationTestClient"};
};

// ============================================================================
// Core Publish/Subscribe Flow Tests
// ============================================================================

TEST_P(MoQIntegrationTest, PublishAndSubscribe_BasicDataFlow) {
  runTest(folly::coro::co_invoke([this]() -> folly::coro::Task<void> {
    co_await connectClient();

    auto callback = co_await subscribeAndReceive();
    if (!callback) {
      co_return;
    }

    co_await callback->waitForObjects(kDefaultObjectCount);

    auto objects = callback->getObjects();
    EXPECT_EQ(objects.size(), kDefaultObjectCount);
    for (size_t i = 0; i < std::min(objects.size(), kDefaultObjectCount); ++i) {
      EXPECT_EQ(objects[i].group, 0u);
      EXPECT_EQ(objects[i].subgroup, 0u);
      EXPECT_EQ(objects[i].id, i);
      EXPECT_EQ(objects[i].payload, kTestPayload + std::to_string(i));
    }
  }));
}

TEST_P(MoQIntegrationTest, PublishAndSubscribe_MultipleGroups) {
  publisher_->setSubscribeHandler(
      [](SubscribeRequest sub, std::shared_ptr<TrackConsumer> callback)
          -> folly::coro::Task<Publisher::SubscribeResult> {
        co_return co_await acceptAndSendDelayed(
            std::move(sub),
            std::move(callback),
            [](std::shared_ptr<TrackConsumer> cb, RequestID reqID) {
              for (uint64_t g = 0; g < kDefaultObjectCount; ++g) {
                auto sgResult = cb->beginSubgroup(g, 0, kDefaultPriority);
                if (sgResult.hasError()) {
                  return;
                }
                auto sg = sgResult.value();
                sg->object(0, makePayload("group" + std::to_string(g)));
                sg->endOfGroup(1);
              }

              sendPublishDone(cb, reqID);
            });
      });

  runTest(folly::coro::co_invoke([this]() -> folly::coro::Task<void> {
    co_await connectClient();

    auto callback = co_await subscribeAndReceive();
    if (!callback) {
      co_return;
    }

    co_await callback->waitForObjects(kDefaultObjectCount);

    auto objects = callback->getObjects();
    EXPECT_EQ(objects.size(), kDefaultObjectCount);
    // QUIC streams may arrive in any order, so check all groups
    // are present without assuming order
    std::set<uint64_t> groups;
    std::map<uint64_t, std::string> payloadByGroup;
    for (const auto& obj : objects) {
      groups.insert(obj.group);
      payloadByGroup[obj.group] = obj.payload;
    }
    EXPECT_EQ(groups.size(), kDefaultObjectCount);
    for (uint64_t g = 0; g < kDefaultObjectCount; ++g) {
      EXPECT_TRUE(groups.count(g)) << "Missing group " << g;
      EXPECT_EQ(payloadByGroup[g], "group" + std::to_string(g));
    }
  }));
}

TEST_P(MoQIntegrationTest, PublishAndSubscribe_MultipleSubgroups) {
  publisher_->setSubscribeHandler(
      [](SubscribeRequest sub, std::shared_ptr<TrackConsumer> callback)
          -> folly::coro::Task<Publisher::SubscribeResult> {
        co_return co_await acceptAndSendDelayed(
            std::move(sub),
            std::move(callback),
            [](std::shared_ptr<TrackConsumer> cb, RequestID reqID) {
              for (uint64_t sg = 0; sg < 2; ++sg) {
                auto sgResult = cb->beginSubgroup(0, sg, kDefaultPriority);
                if (sgResult.hasError()) {
                  return;
                }
                auto sgConsumer = sgResult.value();
                sgConsumer->object(0, makePayload("sg" + std::to_string(sg)));
                sgConsumer->endOfSubgroup();
              }

              sendPublishDone(cb, reqID);
            });
      });

  runTest(folly::coro::co_invoke([this]() -> folly::coro::Task<void> {
    co_await connectClient();

    auto callback = co_await subscribeAndReceive();
    if (!callback) {
      co_return;
    }

    co_await callback->waitForObjects(2);

    auto objects = callback->getObjects();
    EXPECT_EQ(objects.size(), 2u);
    // QUIC streams may arrive in any order
    std::map<uint64_t, std::string> payloadBySg;
    for (const auto& obj : objects) {
      payloadBySg[obj.subgroup] = obj.payload;
    }
    EXPECT_EQ(payloadBySg.size(), 2u);
    EXPECT_EQ(payloadBySg[0], "sg0");
    EXPECT_EQ(payloadBySg[1], "sg1");
  }));
}

TEST_P(MoQIntegrationTest, PublishAndSubscribe_PublishDone) {
  runTest(folly::coro::co_invoke([this]() -> folly::coro::Task<void> {
    co_await connectClient();

    auto callback = co_await subscribeAndReceive();
    if (!callback) {
      co_return;
    }

    co_await callback->waitForObjects(kDefaultObjectCount);
    co_await callback->waitForPublishDone();

    auto pubDone = callback->getPublishDone();
    EXPECT_TRUE(pubDone.has_value());
    if (pubDone) {
      EXPECT_EQ(pubDone->statusCode, PublishDoneStatusCode::TRACK_ENDED);
    }
  }));
}

TEST_P(MoQIntegrationTest, PublishAndSubscribe_Datagram) {
  publisher_->setSubscribeHandler(
      [](SubscribeRequest sub, std::shared_ptr<TrackConsumer> callback)
          -> folly::coro::Task<Publisher::SubscribeResult> {
        co_return co_await acceptAndSendDelayed(
            std::move(sub),
            std::move(callback),
            [](std::shared_ptr<TrackConsumer> cb, RequestID reqID) {
              ObjectHeader header(0, 0, 0, kDefaultPriority);
              cb->datagram(header, makePayload("datagram-payload"));

              sendPublishDone(cb, reqID);
            });
      });

  runTest(folly::coro::co_invoke([this]() -> folly::coro::Task<void> {
    co_await connectClient();

    auto callback = co_await subscribeAndReceive();
    if (!callback) {
      co_return;
    }

    co_await callback->waitForObjects(1);

    auto objects = callback->getObjects();
    EXPECT_GE(objects.size(), 1u);
    if (!objects.empty()) {
      EXPECT_EQ(objects[0].group, 0u);
      EXPECT_EQ(objects[0].subgroup, 0u);
      EXPECT_EQ(objects[0].id, 0u);
      EXPECT_EQ(objects[0].payload, "datagram-payload");
    }
  }));
}

// Verify that omitting `.forward` (relying on the struct default of false)
// causes beginSubgroup() to fail — callers must always set forward=true.
TEST_P(MoQIntegrationTest, ClientPublish_ForwardDefaultBreaksSubgroup) {
  runTest(folly::coro::co_invoke([this]() -> folly::coro::Task<void> {
    co_await connectClient();

    PublishRequest publishRequest{.fullTrackName = kIntegrationTestTrackName};
    // forward defaults to false in the struct definition

    auto subHandle = makeClientPublishHandle();

    auto publishResult =
        client_->moqSession_->publish(publishRequest, subHandle);
    EXPECT_FALSE(publishResult.hasError());
    if (publishResult.hasError()) {
      co_return;
    }

    auto trackConsumer = publishResult.value().consumer;
    auto sgResult = trackConsumer->beginSubgroup(0, 0, kDefaultPriority);
    // With forward=false, beginSubgroup must fail
    EXPECT_TRUE(sgResult.hasError())
        << "beginSubgroup should fail when forward=false";
  }));
}

// Verify that the full client publish flow works when forward is explicitly
// set to true.
TEST_P(MoQIntegrationTest, ClientPublish_BasicDataFlow) {
  runTest(folly::coro::co_invoke([this]() -> folly::coro::Task<void> {
    co_await connectClient();

    // forward must be explicitly set to true for beginSubgroup() to succeed.
    // See ClientPublish_ForwardDefaultBreaksSubgroup for the negative test.
    PublishRequest publishRequest{
        .fullTrackName = kIntegrationTestTrackName, .forward = true};

    auto subHandle = makeClientPublishHandle();

    auto publishResult =
        client_->moqSession_->publish(publishRequest, subHandle);
    EXPECT_FALSE(publishResult.hasError())
        << "Publish failed: " << publishResult.error().reasonPhrase;
    if (publishResult.hasError()) {
      co_return;
    }

    auto trackConsumer = publishResult.value().consumer;

    // With forward=false, beginSubgroup() returns an error.
    auto sgResult = trackConsumer->beginSubgroup(0, 0, kDefaultPriority);
    EXPECT_FALSE(sgResult.hasError())
        << "beginSubgroup failed — PublishRequest.forward may be false";
    if (sgResult.hasError()) {
      co_return;
    }

    auto sg = sgResult.value();

    for (uint64_t i = 0; i < kDefaultObjectCount; ++i) {
      auto payload = makePayload(kTestPayload + std::to_string(i));
      auto objResult = sg->object(i, std::move(payload));
      EXPECT_FALSE(objResult.hasError());
    }
    sg->endOfSubgroup();

    // Wait for PublishOk from server
    auto publishOk = co_await std::move(publishResult.value().reply);
    EXPECT_FALSE(publishOk.hasError());
    if (!publishOk.hasError()) {
      EXPECT_TRUE(publishOk.value().forward);
    }

    // Verify server received the data
    auto receiverCallback = subscriber_->getReceiverCallback();
    EXPECT_NE(receiverCallback, nullptr);
    if (!receiverCallback) {
      co_return;
    }

    co_await receiverCallback->waitForObjects(kDefaultObjectCount);
    auto objects = receiverCallback->getObjects();
    EXPECT_EQ(objects.size(), kDefaultObjectCount);
    for (size_t i = 0; i < std::min(objects.size(), kDefaultObjectCount); ++i) {
      EXPECT_EQ(objects[i].id, i);
      EXPECT_EQ(objects[i].payload, kTestPayload + std::to_string(i));
    }
  }));
}

// ============================================================================
// Fetch Tests
// ============================================================================

TEST_P(MoQIntegrationTest, Fetch_BasicDataFlow) {
  runTest(folly::coro::co_invoke([this]() -> folly::coro::Task<void> {
    co_await connectClient();

    auto callback = std::make_shared<TestObjectCallback>();
    auto receiver =
        std::make_shared<ObjectReceiver>(ObjectReceiver::FETCH, callback);

    Fetch fetch(
        RequestID(0),
        kIntegrationTestTrackName,
        AbsoluteLocation{0, 0},
        AbsoluteLocation{0, kDefaultObjectCount},
        kDefaultPriority,
        GroupOrder::OldestFirst);

    auto result =
        co_await client_->moqSession_->fetch(std::move(fetch), receiver);
    EXPECT_FALSE(result.hasError());
    if (result.hasError()) {
      co_return;
    }

    co_await callback->waitForObjects(kDefaultObjectCount);

    auto objects = callback->getObjects();
    EXPECT_EQ(objects.size(), kDefaultObjectCount);
    for (size_t i = 0; i < std::min(objects.size(), kDefaultObjectCount); ++i) {
      EXPECT_EQ(objects[i].id, i);
      EXPECT_EQ(objects[i].payload, kTestPayload + std::to_string(i));
    }
  }));
}

TEST_P(MoQIntegrationTest, Fetch_EmptyResult) {
  publisher_->setFetchHandler(
      [](Fetch fetch, std::shared_ptr<FetchConsumer> fetchCallback)
          -> folly::coro::Task<Publisher::FetchResult> {
        FetchOk ok;
        ok.requestID = fetch.requestID;
        ok.groupOrder = GroupOrder::OldestFirst;
        ok.endOfTrack = 0;
        ok.endLocation = AbsoluteLocation{0, 0};

        auto handle = std::make_shared<TestFetchHandle>(ok);
        fetchCallback->endOfFetch();

        co_return handle;
      });

  runTest(folly::coro::co_invoke([this]() -> folly::coro::Task<void> {
    co_await connectClient();

    auto callback = std::make_shared<TestObjectCallback>();
    auto receiver =
        std::make_shared<ObjectReceiver>(ObjectReceiver::FETCH, callback);

    Fetch fetch(
        RequestID(0),
        kIntegrationTestTrackName,
        AbsoluteLocation{0, 0},
        AbsoluteLocation{0, 0},
        kDefaultPriority,
        GroupOrder::OldestFirst);

    auto result =
        co_await client_->moqSession_->fetch(std::move(fetch), receiver);
    EXPECT_FALSE(result.hasError());
    if (result.hasError()) {
      co_return;
    }

    co_await callback->waitForAllDataReceived();
    EXPECT_TRUE(callback->isAllDataReceived());
    EXPECT_EQ(callback->getObjects().size(), 0u);
  }));
}

// ============================================================================
// Session Setup Tests
// ============================================================================

TEST_P(MoQIntegrationTest, SessionSetup_ConnectAndDisconnect) {
  runTest(folly::coro::co_invoke([this]() -> folly::coro::Task<void> {
    co_await connectClient();
    EXPECT_NE(client_->moqSession_, nullptr);
    EXPECT_TRUE(client_->moqSession_->getNegotiatedVersion().has_value());

    // Verify data actually flows over the connection before closing
    auto callback = co_await subscribeAndReceive();
    if (!callback) {
      co_return;
    }
    co_await callback->waitForObjects(kDefaultObjectCount);
    EXPECT_EQ(callback->getObjects().size(), kDefaultObjectCount);

    client_->moqSession_->close(SessionCloseErrorCode::NO_ERROR);
  }));
}

TEST_P(MoQIntegrationTest, SessionSetup_VersionNegotiation) {
  runTest(folly::coro::co_invoke([this]() -> folly::coro::Task<void> {
    co_await connectClient();
    auto version = client_->moqSession_->getNegotiatedVersion();
    EXPECT_TRUE(version.has_value());
    if (version) {
      EXPECT_TRUE(isSupportedVersion(*version));
    }

    // Verify the negotiated version actually works for data exchange
    auto callback = co_await subscribeAndReceive();
    if (!callback) {
      co_return;
    }
    co_await callback->waitForObjects(kDefaultObjectCount);
    EXPECT_EQ(callback->getObjects().size(), kDefaultObjectCount);
  }));
}

TEST_P(MoQIntegrationTest, PublishAndSubscribe_EndOfGroup) {
  publisher_->setSubscribeHandler(
      [](SubscribeRequest sub, std::shared_ptr<TrackConsumer> callback)
          -> folly::coro::Task<Publisher::SubscribeResult> {
        co_return co_await acceptAndSendDelayed(
            std::move(sub),
            std::move(callback),
            [](std::shared_ptr<TrackConsumer> cb, RequestID reqID) {
              auto sgResult = cb->beginSubgroup(0, 0, kDefaultPriority);
              if (sgResult.hasError()) {
                return;
              }
              auto sg = sgResult.value();
              sg->object(0, makePayload("obj-before-eog"));
              sg->endOfGroup(1);

              sendPublishDone(cb, reqID);
            });
      });

  runTest(folly::coro::co_invoke([this]() -> folly::coro::Task<void> {
    co_await connectClient();

    auto callback = co_await subscribeAndReceive();
    if (!callback) {
      co_return;
    }

    co_await callback->waitForObjects(1);
    auto objects = callback->getObjects();
    EXPECT_EQ(objects.size(), 1u);
    if (!objects.empty()) {
      EXPECT_EQ(objects[0].payload, "obj-before-eog");
    }

    co_await callback->waitForObjectStatus();
    auto status = callback->getLastObjectStatus();
    EXPECT_TRUE(status.has_value());
    if (status) {
      EXPECT_EQ(*status, ObjectStatus::END_OF_GROUP);
    }
  }));
}

// ============================================================================
// Error Handling Tests
// ============================================================================

TEST_P(MoQIntegrationTest, Subscribe_ServerRejects) {
  publisher_->setSubscribeHandler(
      [](SubscribeRequest sub, std::shared_ptr<TrackConsumer> /*callback*/)
          -> folly::coro::Task<Publisher::SubscribeResult> {
        SubscribeError error;
        error.requestID = sub.requestID;
        error.errorCode = SubscribeErrorCode::NOT_SUPPORTED;
        error.reasonPhrase = "rejected for testing";
        co_return folly::makeUnexpected(error);
      });

  runTest(folly::coro::co_invoke([this]() -> folly::coro::Task<void> {
    co_await connectClient();

    auto callback = std::make_shared<TestObjectCallback>();
    auto receiver =
        std::make_shared<ObjectReceiver>(ObjectReceiver::SUBSCRIBE, callback);

    auto result =
        co_await client_->moqSession_->subscribe(makeSubscribe(), receiver);
    EXPECT_TRUE(result.hasError());
    if (result.hasError()) {
      EXPECT_EQ(result.error().errorCode, SubscribeErrorCode::NOT_SUPPORTED);
      EXPECT_EQ(result.error().reasonPhrase, "rejected for testing");
    }
  }));
}

TEST_P(MoQIntegrationTest, Fetch_ServerRejects) {
  publisher_->setFetchHandler(
      [](Fetch fetch, std::shared_ptr<FetchConsumer> /*callback*/)
          -> folly::coro::Task<Publisher::FetchResult> {
        FetchError error;
        error.requestID = fetch.requestID;
        error.errorCode = FetchErrorCode::NOT_SUPPORTED;
        error.reasonPhrase = "fetch rejected";
        co_return folly::makeUnexpected(error);
      });

  runTest(folly::coro::co_invoke([this]() -> folly::coro::Task<void> {
    co_await connectClient();

    auto callback = std::make_shared<TestObjectCallback>();
    auto receiver =
        std::make_shared<ObjectReceiver>(ObjectReceiver::FETCH, callback);

    Fetch fetch(
        RequestID(0),
        kIntegrationTestTrackName,
        AbsoluteLocation{0, 0},
        AbsoluteLocation{0, kDefaultObjectCount},
        kDefaultPriority,
        GroupOrder::OldestFirst);

    auto result =
        co_await client_->moqSession_->fetch(std::move(fetch), receiver);
    EXPECT_TRUE(result.hasError());
    if (result.hasError()) {
      EXPECT_EQ(result.error().errorCode, FetchErrorCode::NOT_SUPPORTED);
    }
  }));
}

TEST_P(MoQIntegrationTest, SessionSetup_ServerRejectsClient) {
  // Stop the normal server and start a rejecting one.
  // Note: server setup runs outside runTest() so it lacks the 10s timeout,
  // but local server init is fast and deterministic.
  server_->stop();
  server_.reset();

  server_ = std::make_shared<TestServer>(
      publisher_, subscriber_, /*rejectSetup=*/true);
  server_->start(folly::SocketAddress("::", 0));
  server_->waitUntilInitialized();

  auto fds = server_->getAllListeningSocketFDs();
  ASSERT_FALSE(fds.empty());
  folly::SocketAddress boundAddr;
  boundAddr.setFromLocalAddress(folly::NetworkSocket::fromFd(fds[0]));
  serverPort_ = boundAddr.getPort();

  runTest(folly::coro::co_invoke([this]() -> folly::coro::Task<void> {
    auto res = co_await folly::coro::co_awaitTry(connectClient());
    EXPECT_TRUE(res.hasException())
        << "Expected setupMoQSession to fail when server rejects setup";
  }));
}

// ============================================================================
// Bidirectional and Scale Tests
// ============================================================================

// Concurrent subscribe (server→client) and publish (client→server) on the
// same session. Both directions are initiated before either is awaited to
// ensure they overlap on the transport.
TEST_P(MoQIntegrationTest, BidirectionalFlow) {
  runTest(folly::coro::co_invoke([this]() -> folly::coro::Task<void> {
    co_await connectClient();

    // Initiate Direction 1: Subscribe (server sends data to client)
    auto subCallback = std::make_shared<TestObjectCallback>();
    auto receiver = std::make_shared<ObjectReceiver>(
        ObjectReceiver::SUBSCRIBE, subCallback);
    auto subResult =
        co_await client_->moqSession_->subscribe(makeSubscribe(), receiver);
    EXPECT_FALSE(subResult.hasError());
    if (subResult.hasError()) {
      co_return;
    }

    // Initiate Direction 2: Publish (client sends data to server)
    PublishRequest publishRequest{
        .fullTrackName = kSecondTrackName, .forward = true};
    auto subHandle = makeClientPublishHandle();

    auto publishResult =
        client_->moqSession_->publish(publishRequest, subHandle);
    EXPECT_FALSE(publishResult.hasError());
    if (publishResult.hasError()) {
      co_return;
    }

    // Wait for server to accept the publish before sending data
    auto publishOk = co_await std::move(publishResult.value().reply);
    EXPECT_FALSE(publishOk.hasError());
    if (publishOk.hasError()) {
      co_return;
    }

    // Now send client→server data while server→client is already in flight
    auto trackConsumer = publishResult.value().consumer;
    auto sgResult = trackConsumer->beginSubgroup(0, 0, kDefaultPriority);
    EXPECT_FALSE(sgResult.hasError());
    if (sgResult.hasError()) {
      co_return;
    }

    auto sg = sgResult.value();
    for (uint64_t i = 0; i < kDefaultObjectCount; ++i) {
      sg->object(i, makePayload("pub" + std::to_string(i)));
    }
    sg->endOfSubgroup();

    // Verify both directions delivered data correctly
    co_await subCallback->waitForObjects(kDefaultObjectCount);
    auto subObjects = subCallback->getObjects();
    EXPECT_EQ(subObjects.size(), kDefaultObjectCount);
    for (size_t i = 0; i < std::min(subObjects.size(), kDefaultObjectCount);
         ++i) {
      EXPECT_EQ(subObjects[i].id, i);
      EXPECT_EQ(subObjects[i].payload, kTestPayload + std::to_string(i));
    }

    auto receiverCallback = subscriber_->getReceiverCallback();
    EXPECT_NE(receiverCallback, nullptr);
    if (!receiverCallback) {
      co_return;
    }
    co_await receiverCallback->waitForObjects(kDefaultObjectCount);
    auto pubObjects = receiverCallback->getObjects();
    EXPECT_EQ(pubObjects.size(), kDefaultObjectCount);
    for (size_t i = 0; i < std::min(pubObjects.size(), kDefaultObjectCount);
         ++i) {
      EXPECT_EQ(pubObjects[i].id, i);
      EXPECT_EQ(pubObjects[i].payload, "pub" + std::to_string(i));
    }
  }));
}

// Many objects that exercise flow control. Default flow control windows
// are 1MB; sending 2048 × 1KB = 2MB total pushes past the window.
TEST_P(MoQIntegrationTest, PublishAndSubscribe_ManyObjects) {
  const size_t kPayloadSize = 1024; // 1KB per object (fits in single frame)
  const size_t kObjectCount = 2048; // 2MB total > 1MB window

  publisher_->setSubscribeHandler(
      [](SubscribeRequest sub, std::shared_ptr<TrackConsumer> callback)
          -> folly::coro::Task<Publisher::SubscribeResult> {
        co_return co_await acceptAndSendDelayed(
            std::move(sub),
            std::move(callback),
            [](std::shared_ptr<TrackConsumer> cb, RequestID reqID) {
              auto sgResult = cb->beginSubgroup(0, 0, kDefaultPriority);
              if (sgResult.hasError()) {
                return;
              }
              auto sg = sgResult.value();
              for (uint64_t i = 0; i < kObjectCount; ++i) {
                std::string payload(kPayloadSize, 'A' + (char)(i % 26));
                sg->object(i, makePayload(payload));
              }
              sg->endOfSubgroup();

              sendPublishDone(cb, reqID);
            });
      });

  runTest(
      folly::coro::co_invoke(
          [this, kPayloadSize, kObjectCount]() -> folly::coro::Task<void> {
            co_await connectClient();

            auto callback = co_await subscribeAndReceive();
            if (!callback) {
              co_return;
            }

            co_await callback->waitForObjects(kObjectCount);
            auto objects = callback->getObjects();
            EXPECT_EQ(objects.size(), kObjectCount);
            for (size_t i = 0; i < std::min(objects.size(), kObjectCount);
                 ++i) {
              EXPECT_EQ(objects[i].payload.size(), kPayloadSize);
              char expected = 'A' + (char)(i % 26);
              EXPECT_EQ(objects[i].payload.front(), expected);
              EXPECT_EQ(objects[i].payload.back(), expected);
            }
          }));
}

// Large objects that span multiple QUIC frames exercise the
// beginObject()/objectPayload() chunked delivery path in ObjectReceiver,
// which is distinct from the single-frame object() path.
TEST_P(MoQIntegrationTest, PublishAndSubscribe_LargePayload) {
  const size_t kLargePayloadSize = 512 * 1024; // 512KB per object
  const size_t kLargeObjectCount = 4;          // 2MB total

  publisher_->setSubscribeHandler(
      [](SubscribeRequest sub, std::shared_ptr<TrackConsumer> callback)
          -> folly::coro::Task<Publisher::SubscribeResult> {
        co_return co_await acceptAndSendDelayed(
            std::move(sub),
            std::move(callback),
            [](std::shared_ptr<TrackConsumer> cb, RequestID reqID) {
              auto sgResult = cb->beginSubgroup(0, 0, kDefaultPriority);
              if (sgResult.hasError()) {
                return;
              }
              auto sg = sgResult.value();
              for (uint64_t i = 0; i < kLargeObjectCount; ++i) {
                std::string payload(kLargePayloadSize, 'A' + (char)(i % 26));
                sg->object(i, makePayload(payload));
              }
              sg->endOfSubgroup();

              sendPublishDone(cb, reqID);
            });
      });

  runTest(
      folly::coro::co_invoke(
          [this,
           kLargePayloadSize,
           kLargeObjectCount]() -> folly::coro::Task<void> {
            co_await connectClient();

            auto callback = co_await subscribeAndReceive();
            if (!callback) {
              co_return;
            }

            co_await callback->waitForObjects(kLargeObjectCount);
            auto objects = callback->getObjects();
            EXPECT_EQ(objects.size(), kLargeObjectCount);
            for (size_t i = 0; i < std::min(objects.size(), kLargeObjectCount);
                 ++i) {
              EXPECT_EQ(objects[i].payload.size(), kLargePayloadSize);
              char expected = 'A' + (char)(i % 26);
              EXPECT_EQ(objects[i].payload.front(), expected);
              EXPECT_EQ(objects[i].payload.back(), expected);
            }
          }));
}

// Multiple tracks on one session. Verifies track routing doesn't mix up
// data between tracks (e.g., trackAlias collisions).
TEST_P(MoQIntegrationTest, PublishAndSubscribe_MultipleTracks) {
  publisher_->setSubscribeHandler(
      [](SubscribeRequest sub, std::shared_ptr<TrackConsumer> callback)
          -> folly::coro::Task<Publisher::SubscribeResult> {
        auto trackName = sub.fullTrackName.trackName;
        co_return co_await acceptAndSendDelayed(
            std::move(sub),
            std::move(callback),
            [trackName](std::shared_ptr<TrackConsumer> cb, RequestID reqID) {
              auto sgResult = cb->beginSubgroup(0, 0, kDefaultPriority);
              if (sgResult.hasError()) {
                return;
              }
              auto sg = sgResult.value();
              // Tag each object with the track name so we can verify routing
              sg->object(0, makePayload("track:" + trackName));
              sg->endOfSubgroup();

              sendPublishDone(cb, reqID);
            });
      });

  runTest(folly::coro::co_invoke([this]() -> folly::coro::Task<void> {
    co_await connectClient();

    // Subscribe to track 1
    auto callback1 = std::make_shared<TestObjectCallback>();
    auto receiver1 =
        std::make_shared<ObjectReceiver>(ObjectReceiver::SUBSCRIBE, callback1);
    auto result1 =
        co_await client_->moqSession_->subscribe(makeSubscribe(), receiver1);
    EXPECT_FALSE(result1.hasError());
    if (result1.hasError()) {
      co_return;
    }

    // Subscribe to track 2
    auto callback2 = std::make_shared<TestObjectCallback>();
    auto receiver2 =
        std::make_shared<ObjectReceiver>(ObjectReceiver::SUBSCRIBE, callback2);
    SubscribeRequest sub2 = makeSubscribe();
    sub2.fullTrackName = kSecondTrackName;
    auto result2 =
        co_await client_->moqSession_->subscribe(std::move(sub2), receiver2);
    EXPECT_FALSE(result2.hasError());
    if (result2.hasError()) {
      co_return;
    }

    // Wait for both tracks to deliver
    co_await callback1->waitForObjects(1);
    co_await callback2->waitForObjects(1);

    auto objects1 = callback1->getObjects();
    auto objects2 = callback2->getObjects();
    EXPECT_EQ(objects1.size(), 1u);
    EXPECT_EQ(objects2.size(), 1u);
    if (!objects1.empty()) {
      EXPECT_EQ(objects1[0].payload, "track:test-track");
    }
    if (!objects2.empty()) {
      EXPECT_EQ(objects2[0].payload, "track:second-track");
    }
  }));
}

// Mid-stream unsubscribe. Verifies the client can unsubscribe after
// receiving partial data without crashing or hanging.
TEST_P(MoQIntegrationTest, Subscribe_MidStreamUnsubscribe) {
  publisher_->setSubscribeHandler(
      [](SubscribeRequest sub, std::shared_ptr<TrackConsumer> callback)
          -> folly::coro::Task<Publisher::SubscribeResult> {
        co_return co_await acceptAndSendDelayed(
            std::move(sub),
            std::move(callback),
            [](std::shared_ptr<TrackConsumer> cb, RequestID /*reqID*/) {
              auto sgResult = cb->beginSubgroup(0, 0, kDefaultPriority);
              if (sgResult.hasError()) {
                return;
              }
              auto sg = sgResult.value();
              sg->object(0, makePayload("obj0"));
              sg->object(1, makePayload("obj1"));
              // Intentionally leave the subgroup open (no endOfSubgroup)
              // to simulate an ongoing stream that gets unsubscribed.
            });
      });

  runTest(folly::coro::co_invoke([this]() -> folly::coro::Task<void> {
    co_await connectClient();

    auto callback = std::make_shared<TestObjectCallback>();
    auto receiver =
        std::make_shared<ObjectReceiver>(ObjectReceiver::SUBSCRIBE, callback);

    auto result =
        co_await client_->moqSession_->subscribe(makeSubscribe(), receiver);
    EXPECT_FALSE(result.hasError());
    if (result.hasError()) {
      co_return;
    }

    // Wait for first batch of objects
    co_await callback->waitForObjects(2);
    auto objects = callback->getObjects();
    EXPECT_GE(objects.size(), 2u);
    if (objects.size() >= 2) {
      EXPECT_EQ(objects[0].payload, "obj0");
      EXPECT_EQ(objects[1].payload, "obj1");
    }

    // Unsubscribe mid-stream — must not crash or hang
    result.value()->unsubscribe();
  }));
}

// In-order delivery within a single subgroup. MoQ protocol guarantees
// objects within a subgroup arrive in send order (they share a QUIC stream).
// Uses varying payload sizes to stress moxygen's framing/reassembly.
TEST_P(MoQIntegrationTest, PublishAndSubscribe_InOrderDelivery) {
  const size_t kOrderTestCount = 20;

  publisher_->setSubscribeHandler(
      [](SubscribeRequest sub, std::shared_ptr<TrackConsumer> callback)
          -> folly::coro::Task<Publisher::SubscribeResult> {
        co_return co_await acceptAndSendDelayed(
            std::move(sub),
            std::move(callback),
            [](std::shared_ptr<TrackConsumer> cb, RequestID reqID) {
              auto sgResult = cb->beginSubgroup(0, 0, kDefaultPriority);
              if (sgResult.hasError()) {
                return;
              }
              auto sg = sgResult.value();
              for (uint64_t i = 0; i < kOrderTestCount; ++i) {
                // Vary payload sizes: 10B, 100B, 1KB, 10KB cycle
                size_t sizes[] = {10, 100, 1024, 10240};
                size_t payloadSize = sizes[i % 4];
                std::string payload(payloadSize, 'A' + (char)(i % 26));
                sg->object(i, makePayload(payload));
              }
              sg->endOfSubgroup();

              sendPublishDone(cb, reqID);
            });
      });

  runTest(
      folly::coro::co_invoke(
          [this, kOrderTestCount]() -> folly::coro::Task<void> {
            co_await connectClient();

            auto callback = co_await subscribeAndReceive();
            if (!callback) {
              co_return;
            }

            co_await callback->waitForObjects(kOrderTestCount);
            auto objects = callback->getObjects();
            EXPECT_EQ(objects.size(), kOrderTestCount);

            // Objects must arrive in order within a subgroup
            size_t sizes[] = {10, 100, 1024, 10240};
            for (size_t i = 0; i < std::min(objects.size(), kOrderTestCount);
                 ++i) {
              EXPECT_EQ(objects[i].id, i) << "Object " << i << " out of order";
              EXPECT_EQ(objects[i].payload.size(), sizes[i % 4]);
              char expected = 'A' + (char)(i % 26);
              EXPECT_EQ(objects[i].payload.front(), expected);
              EXPECT_EQ(objects[i].payload.back(), expected);
            }
          }));
}

INSTANTIATE_TEST_SUITE_P(
    MoQIntegrationTest,
    MoQIntegrationTest,
    testing::ValuesIn(kSupportedVersions),
    [](const testing::TestParamInfo<uint64_t>& info) {
      return "Draft" + std::to_string(getDraftMajorVersion(info.param));
    });

} // namespace moxygen::test

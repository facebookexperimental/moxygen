/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <folly/coro/BlockingWait.h>
#include <folly/io/async/EventBase.h>
#include <folly/portability/GMock.h>
#include <folly/portability/GTest.h>
#include <moxygen/MoQTrackProperties.h>
#include <moxygen/events/MoQFollyExecutorImpl.h>
#include <moxygen/relay/MoQForwarder.h>
#include <moxygen/relay/MoQRelay.h>
#include <moxygen/test/MockMoQSession.h>
#include <moxygen/test/Mocks.h>
#include <moxygen/test/TestUtils.h>

using namespace testing;

namespace moxygen::test {

const TrackNamespace kTestNamespace{{"test", "namespace"}};
const TrackNamespace kAllowedPrefix{{"test"}};
const FullTrackName kTestTrackName{kTestNamespace, "track1"};

// TestMoQExecutor that can be driven for tests
class TestMoQExecutor : public MoQFollyExecutorImpl,
                        public folly::DrivableExecutor {
 public:
  explicit TestMoQExecutor() : MoQFollyExecutorImpl(&evb_) {}
  ~TestMoQExecutor() override = default;

  void add(folly::Func func) override {
    MoQFollyExecutorImpl::add(std::move(func));
  }

  // Implements DrivableExec::drive
  void drive() override {
    // Run the event loop until there is nothing left to do
    // (simulate a "tick" for test event loop)
    if (auto* evb = getBackingEventBase()) {
      evb->loopOnce();
    }
  }

 private:
  folly::EventBase evb_;
};

// Test fixture for MoQRelay tests
// This provides a skeleton for testing MoQRelay functionality.
// Full integration tests with real sessions will be added when implementing
// multi-publisher support.
class MoQRelayTest : public ::testing::Test {
 protected:
  void SetUp() override {
    exec_ = std::make_shared<TestMoQExecutor>();
    relay_ = std::make_shared<MoQRelay>(/*enableCache=*/false);
    relay_->setAllowedNamespacePrefix(kAllowedPrefix);
  }

  void TearDown() override {
    relay_.reset();
  }

  // Helper to create a mock session
  std::shared_ptr<MockMoQSession> createMockSession() {
    auto session = std::make_shared<NiceMock<MockMoQSession>>(exec_);
    ON_CALL(*session, getNegotiatedVersion())
        .WillByDefault(Return(std::optional<uint64_t>(kVersionDraftCurrent)));
    auto state = getOrCreateMockState(session);
    return session;
  }

  // Helper to create a mock subscription handle for publish calls
  std::shared_ptr<Publisher::SubscriptionHandle>
  createMockSubscriptionHandle() {
    SubscribeOk ok;
    ok.requestID = RequestID(0);
    ok.trackAlias = TrackAlias(0);
    ok.expires = std::chrono::milliseconds(0);
    ok.groupOrder = GroupOrder::Default;
    auto handle =
        std::make_shared<NiceMock<MockSubscriptionHandle>>(std::move(ok));
    return handle;
  }

  // Helper to remove a session from the relay and clean up mock state
  void removeSession(std::shared_ptr<MoQSession> sess) {
    cleanupMockSession(std::move(sess));
  }

  // Helper to create a mock consumer with default actions
  std::shared_ptr<MockTrackConsumer> createMockConsumer() {
    auto consumer = std::make_shared<NiceMock<MockTrackConsumer>>();
    ON_CALL(*consumer, setTrackAlias(_))
        .WillByDefault(
            Return(folly::makeExpected<MoQPublishError>(folly::unit)));
    ON_CALL(*consumer, publishDone(_))
        .WillByDefault(
            Return(folly::makeExpected<MoQPublishError>(folly::unit)));
    return consumer;
  }

  // Helper to subscribe a session to a track
  std::shared_ptr<SubscriptionHandle> subscribeToTrack(
      std::shared_ptr<MoQSession> session,
      const FullTrackName& trackName,
      std::shared_ptr<TrackConsumer> consumer,
      RequestID requestID = RequestID(0),
      bool addToState = true,
      folly::Optional<SubscribeErrorCode> expectedError = folly::none) {
    SubscribeRequest sub;
    sub.fullTrackName = trackName;
    sub.requestID = requestID;
    sub.locType = LocationType::LargestObject;
    std::shared_ptr<SubscriptionHandle> handle{nullptr};
    withSessionContext(session, [&]() {
      auto task = relay_->subscribe(std::move(sub), consumer);
      auto res = folly::coro::blockingWait(std::move(task), exec_.get());
      if (expectedError.has_value()) {
        // Expect an error and, if present, verify the error code matches
        EXPECT_FALSE(res.hasValue());
        // res has an error; compare error code when available
        const auto& err = res.error();
        // If SubscribeError exposes a code accessor or public member, use it.
        // Assuming err.code exists or err.errorCode() accessor:
        // Adjust the line below to match actual error API.
        EXPECT_EQ(err.errorCode, *expectedError);
      } else {
        EXPECT_TRUE(res.hasValue());
        handle = *res;
        if (addToState) {
          getOrCreateMockState(session)->subscribeHandles.push_back(handle);
        }
      }
    });
    return handle;
  }

  // Helper to set up RequestContext for a session (simulates incoming request)
  template <typename Func>
  auto withSessionContext(std::shared_ptr<MoQSession> session, Func&& func)
      -> decltype(func()) {
    folly::RequestContextScopeGuard guard;
    folly::RequestContext::get()->setContextData(
        sessionRequestToken(),
        std::make_unique<MoQSession::MoQSessionRequestData>(
            std::move(session)));
    return func();
  }

  static const folly::RequestToken& sessionRequestToken() {
    static folly::RequestToken token("moq_session");
    return token;
  }

  // Helper to simulate session cleanup for mock sessions
  // Real MoQRelaySession calls cleanup() which invokes callbacks on stored
  // state. For mock sessions, we need to manually track and clean up.
  struct MockSessionState {
    std::shared_ptr<MoQSession> session;
    std::vector<std::shared_ptr<TrackConsumer>> publishConsumers;
    // Handles for results from publishNamespace, subscribeNamespace,
    // and subscribe
    std::vector<std::shared_ptr<Subscriber::PublishNamespaceHandle>>
        publishNamespaceHandles;
    std::vector<std::shared_ptr<Publisher::SubscribeNamespaceHandle>>
        subscribeNamespaceHandles;
    std::vector<std::shared_ptr<Publisher::SubscriptionHandle>>
        subscribeHandles;

    void cleanup() {
      // Simulate MoQSession::cleanup() for publish tracks
      // This calls publishDone on all tracked consumers, which triggers
      // FilterConsumer callbacks that properly clean up relay state
      for (auto& consumer : publishConsumers) {
        consumer->publishDone(
            {RequestID(0),
             PublishDoneStatusCode::SESSION_CLOSED,
             0,
             "mock session cleanup"});
      }
      publishConsumers.clear();

      // Clean up publishNamespaceHandles
      for (auto& handle : publishNamespaceHandles) {
        if (handle) {
          handle->publishNamespaceDone();
        }
      }
      publishNamespaceHandles.clear();

      // Clean up subscribeNamespaceHandles
      for (auto& handle : subscribeNamespaceHandles) {
        if (handle) {
          handle->unsubscribeNamespace();
        }
      }
      subscribeNamespaceHandles.clear();

      // Clean up subscribeHandles
      for (auto& handle : subscribeHandles) {
        if (handle) {
          handle->unsubscribe();
        }
      }
      subscribeHandles.clear();
    }
  };

  std::map<MoQSession*, std::shared_ptr<MockSessionState>> mockSessions_;

  std::shared_ptr<MockSessionState> getOrCreateMockState(
      std::shared_ptr<MoQSession> session) {
    auto it = mockSessions_.find(session.get());
    if (it == mockSessions_.end()) {
      auto state = std::make_shared<MockSessionState>();
      state->session = session;
      mockSessions_[session.get()] = state;
      return state;
    }
    return it->second;
  }

  void cleanupMockSession(std::shared_ptr<MoQSession> session) {
    auto it = mockSessions_.find(session.get());
    if (it != mockSessions_.end()) {
      // Use withSessionContext to ensure session context is set during cleanup
      withSessionContext(it->second->session, [&]() { it->second->cleanup(); });
      mockSessions_.erase(it);
    }
  }

  // Helper to publish a namespace
  // Returns the PublishNamespaceHandle so tests can use it for manual cleanup
  // if needed If addToState is true, the handle is automatically saved for
  // cleanup
  std::shared_ptr<Subscriber::PublishNamespaceHandle> doPublishNamespace(
      std::shared_ptr<MoQSession> session,
      const TrackNamespace& ns,
      bool addToState = true) {
    PublishNamespace ann;
    ann.trackNamespace = ns;
    return withSessionContext(session, [&]() {
      auto task = relay_->publishNamespace(std::move(ann), nullptr);
      auto res = folly::coro::blockingWait(std::move(task), exec_.get());
      EXPECT_TRUE(res.hasValue());
      if (res.hasValue()) {
        if (addToState) {
          getOrCreateMockState(session)->publishNamespaceHandles.push_back(
              *res);
        }
        return *res;
      }
      return std::shared_ptr<Subscriber::PublishNamespaceHandle>(nullptr);
    });
  }

  // Helper to publish a track
  // Returns the TrackConsumer so tests can use it for manual operations if
  // needed If addToState is true, the consumer is automatically saved for
  // cleanup
  std::shared_ptr<TrackConsumer> doPublish(
      std::shared_ptr<MoQSession> session,
      const FullTrackName& trackName,
      bool addToState = true) {
    PublishRequest pub;
    pub.fullTrackName = trackName;
    return withSessionContext(session, [&]() {
      auto res =
          relay_->publish(std::move(pub), createMockSubscriptionHandle());
      EXPECT_TRUE(res.hasValue());
      if (res.hasValue()) {
        if (addToState) {
          getOrCreateMockState(session)->publishConsumers.push_back(
              res->consumer);
        }
        return res->consumer;
      }
      return std::shared_ptr<TrackConsumer>(nullptr);
    });
  }

  // Helper to subscribe to namespace publishes
  // Returns the SubscribeNamespaceHandle so tests can use it for manual cleanup
  // if needed If addToState is true, the handle is automatically saved for
  // cleanup
  std::shared_ptr<Publisher::SubscribeNamespaceHandle> doSubscribeNamespace(
      std::shared_ptr<MoQSession> session,
      const TrackNamespace& nsPrefix,
      bool addToState = true) {
    SubscribeNamespace subNs;
    subNs.trackNamespacePrefix = nsPrefix;
    return withSessionContext(session, [&]() {
      auto task = relay_->subscribeNamespace(std::move(subNs), nullptr);
      auto res = folly::coro::blockingWait(std::move(task), exec_.get());
      EXPECT_TRUE(res.hasValue());
      if (res.hasValue()) {
        if (addToState) {
          getOrCreateMockState(session)->subscribeNamespaceHandles.push_back(
              *res);
        }
        return *res;
      }
      return std::shared_ptr<Publisher::SubscribeNamespaceHandle>(nullptr);
    });
  }

  // Helper to set up a mock subgroup consumer with default expectations
  std::shared_ptr<MockSubgroupConsumer> createMockSubgroupConsumer() {
    auto sg = std::make_shared<NiceMock<MockSubgroupConsumer>>();
    ON_CALL(*sg, beginObject(_, _, _, _))
        .WillByDefault(
            Return(folly::makeExpected<MoQPublishError>(folly::unit)));
    ON_CALL(*sg, objectPayload(_, _))
        .WillByDefault(Return(
            folly::makeExpected<MoQPublishError>(
                ObjectPublishStatus::IN_PROGRESS)));
    ON_CALL(*sg, endOfSubgroup())
        .WillByDefault(
            Return(folly::makeExpected<MoQPublishError>(folly::unit)));
    ON_CALL(*sg, endOfGroup(_))
        .WillByDefault(
            Return(folly::makeExpected<MoQPublishError>(folly::unit)));
    ON_CALL(*sg, endOfTrackAndGroup(_))
        .WillByDefault(
            Return(folly::makeExpected<MoQPublishError>(folly::unit)));
    return sg;
  }

  // Publish a track with a caller-supplied handle so tests can set expectations
  // on requestUpdateCalled.
  std::shared_ptr<TrackConsumer> doPublishWithHandle(
      std::shared_ptr<MoQSession> session,
      const FullTrackName& trackName,
      std::shared_ptr<Publisher::SubscriptionHandle> handle) {
    return withSessionContext(session, [&]() -> std::shared_ptr<TrackConsumer> {
      PublishRequest pub;
      pub.fullTrackName = trackName;
      auto res = relay_->publish(std::move(pub), std::move(handle));
      EXPECT_TRUE(res.hasValue());
      if (!res.hasValue()) {
        return nullptr;
      }
      auto consumer = res->consumer;
      getOrCreateMockState(session)->publishConsumers.push_back(consumer);
      return consumer;
    });
  }

  // Subscribe to a namespace with an explicit forward flag.
  std::shared_ptr<Publisher::SubscribeNamespaceHandle>
  doSubscribeNamespaceWithForward(
      std::shared_ptr<MoQSession> session,
      const TrackNamespace& nsPrefix,
      bool forward) {
    SubscribeNamespace subNs;
    subNs.trackNamespacePrefix = nsPrefix;
    subNs.forward = forward;
    return withSessionContext(session, [&]() {
      auto task = relay_->subscribeNamespace(std::move(subNs), nullptr);
      auto res = folly::coro::blockingWait(std::move(task), exec_.get());
      EXPECT_TRUE(res.hasValue());
      if (!res.hasValue()) {
        return std::shared_ptr<Publisher::SubscribeNamespaceHandle>(nullptr);
      }
      getOrCreateMockState(session)->subscribeNamespaceHandles.push_back(*res);
      return *res;
    });
  }

  // Set up session->publish() to succeed with a mock consumer and immediate
  // PublishOk.
  void setupPublishSucceeds(std::shared_ptr<MockMoQSession> session) {
    ON_CALL(*session, publish(_, _))
        .WillByDefault(Invoke(
            [this](PublishRequest pub, auto) -> Subscriber::PublishResult {
              PublishOk ok{
                  pub.requestID,
                  /*forward=*/pub.forward,
                  /*priority=*/128,
                  GroupOrder::Default,
                  LocationType::LargestObject,
                  /*start=*/kLocationMin,
                  /*endGroup=*/std::make_optional(uint64_t(0))};
              return Subscriber::PublishConsumerAndReplyTask{
                  createMockConsumer(),
                  folly::coro::makeTask<
                      folly::Expected<PublishOk, PublishError>>(std::move(ok))};
            }));
  }

  // Build a NiceMock SubscriptionHandle suitable for publish() calls.
  std::shared_ptr<NiceMock<MockSubscriptionHandle>> makePublishHandle() {
    SubscribeOk ok;
    ok.requestID = RequestID(0);
    ok.trackAlias = TrackAlias(0);
    ok.expires = std::chrono::milliseconds(0);
    ok.groupOrder = GroupOrder::Default;
    auto handle =
        std::make_shared<NiceMock<MockSubscriptionHandle>>(std::move(ok));
    ON_CALL(*handle, requestUpdateResult())
        .WillByDefault(Return(folly::makeExpected<RequestError>(RequestOk{})));
    return handle;
  }

  std::shared_ptr<TestMoQExecutor> exec_;
  std::shared_ptr<MoQRelay> relay_;
};

// Test: Basic relay construction
TEST_F(MoQRelayTest, Construction) {
  EXPECT_NE(relay_, nullptr);
}

// Test: Verify allowed namespace prefix is set correctly
TEST_F(MoQRelayTest, AllowedNamespacePrefix) {
  // This just verifies the relay can be constructed with a namespace prefix
  // More detailed testing requires full session setup
  auto relay2 = std::make_shared<MoQRelay>(/*enableCache=*/true);
  relay2->setAllowedNamespacePrefix(kTestNamespace);
  EXPECT_NE(relay2, nullptr);
}

// Test: MockMoQSession can be created
TEST_F(MoQRelayTest, MockSessionCreation) {
  auto mockSession = createMockSession();
  EXPECT_NE(mockSession, nullptr);
  EXPECT_NE(mockSession->getExecutor(), nullptr);
}

// Test: Publish a track through the relay
TEST_F(MoQRelayTest, PublishSuccess) {
  auto publisherSession = createMockSession();

  // Publish the namespace
  doPublishNamespace(publisherSession, kTestNamespace);

  // Publish the track
  doPublish(publisherSession, kTestTrackName);

  // Cleanup: remove the session from relay to avoid mock leak warning
  removeSession(publisherSession);
}

// Test: Tree pruning when leaf node is removed
// Scenario: test/A/B/C and test/A/D exist. Remove C should prune B but keep A
// and D
TEST_F(MoQRelayTest, PruneLeafKeepSiblings) {
  auto publisherABC = createMockSession();
  auto publisherAD = createMockSession();

  // PublishNamespace test/A/B/C (don't add to state because we
  // publishNamespaceDone manually)
  TrackNamespace nsABC{{"test", "A", "B", "C"}};
  auto handleABC =
      doPublishNamespace(publisherABC, nsABC, /*addToState=*/false);

  // PublishNamespace test/A/D
  TrackNamespace nsAD{{"test", "A", "D"}};
  doPublishNamespace(publisherAD, nsAD);

  // PublishNamespaceDone test/A/B/C - should prune B (and C) but keep A and D
  withSessionContext(
      publisherABC, [&]() { handleABC->publishNamespaceDone(); });

  // Verify test/A/D still exists using findPublishNamespaceSessions
  auto sessions = relay_->findPublishNamespaceSessions(nsAD);
  EXPECT_EQ(sessions.size(), 1);
  EXPECT_EQ(sessions[0], publisherAD);

  removeSession(publisherABC);
  removeSession(publisherAD);
}

// Test: Tree pruning removes highest empty ancestor
// Scenario: test/A/B/C only. Remove C should prune A (highest empty after test)
TEST_F(MoQRelayTest, PruneHighestEmptyAncestor) {
  auto publisher = createMockSession();

  // PublishNamespace test/A/B/C (don't add to state because we
  // publishNamespaceDone manually)
  TrackNamespace nsABC{{"test", "A", "B", "C"}};
  auto handle = doPublishNamespace(publisher, nsABC, /*addToState=*/false);

  // PublishNamespaceDone test/A/B/C - should prune A (highest empty ancestor)
  withSessionContext(publisher, [&]() { handle->publishNamespaceDone(); });

  // Try to publishNamespace test/A/B/C again with a new session - should create
  // fresh tree
  auto publisher2 = createMockSession();
  doPublishNamespace(publisher2, nsABC);

  removeSession(publisher);
  removeSession(publisher2);
}

// Test: Pruning happens automatically on removeSession
TEST_F(MoQRelayTest, PruneOnRemoveSession) {
  auto publisher = createMockSession();

  // PublishNamespace deep tree test/A/B/C/D
  TrackNamespace nsABCD{{"test", "A", "B", "C", "D"}};
  doPublishNamespace(publisher, nsABCD);

  // Remove session - should prune entire tree test/A/B/C/D
  removeSession(publisher);

  // Verify we can create test/A/B/C/D again (tree was pruned)
  auto publisher2 = createMockSession();
  doPublishNamespace(publisher2, nsABCD);

  removeSession(publisher2);
}

// Test: No pruning when node still has content (multiple publishers)
TEST_F(MoQRelayTest, NoPruneWhenNodeHasContent) {
  auto publisher1 = createMockSession();
  auto publisher2 = createMockSession();

  // Both publishNamespace test/A/B
  TrackNamespace nsAB{{"test", "A", "B"}};
  auto handle1 = doPublishNamespace(publisher1, nsAB, /*addToState=*/false);
  doPublishNamespace(publisher2, nsAB);

  // PublishNamespaceDone from publisher1 - should NOT prune because publisher2
  // still there
  withSessionContext(publisher1, [&]() { handle1->publishNamespaceDone(); });

  // Verify test/A/B still exists by publishing a track through publisher2
  doPublish(publisher2, FullTrackName{nsAB, "track1"});

  removeSession(publisher1);
  removeSession(publisher2);
}

// Test: EXPOSES BUG - onPublishDone should trigger pruning but doesn't
// This test FAILS because onPublishDone removes the publish from the map
// but doesn't call tryPruneChild to clean up empty nodes
TEST_F(MoQRelayTest, PruneOnPublishDoneBug) {
  auto publisher = createMockSession();

  // Create deep tree test/A/B/C with only a publish (no publishNamespace)
  TrackNamespace nsABC{{"test", "A", "B", "C"}};

  // First publishNamespace so we can publish
  doPublishNamespace(publisher, nsABC);

  // Publish a track
  auto consumer = doPublish(publisher, FullTrackName{nsABC, "track1"});

  // Verify publish exists in the tree
  auto state = relay_->findPublishState(FullTrackName{nsABC, "track1"});
  EXPECT_TRUE(state.nodeExists);
  EXPECT_EQ(state.session, publisher);

  // PublishNamespaceDone - node should stay because publish is still active
  withSessionContext(publisher, [&]() {
    getOrCreateMockState(publisher)
        ->publishNamespaceHandles[0]
        ->publishNamespaceDone();
    getOrCreateMockState(publisher)->publishNamespaceHandles.clear();
  });

  // Publish should still be there, node still exists
  state = relay_->findPublishState(FullTrackName{nsABC, "track1"});
  EXPECT_TRUE(state.nodeExists);
  EXPECT_EQ(state.session, publisher);

  // End the publish - onPublishDone gets called
  withSessionContext(publisher, [&]() {
    consumer->publishDone(
        {RequestID(0),
         PublishDoneStatusCode::SUBSCRIPTION_ENDED,
         0,
         "publisher done"});
  });

  // BUG EXPOSED: The publish is removed from the map but the node still exists
  state = relay_->findPublishState(FullTrackName{nsABC, "track1"});
  EXPECT_EQ(state.session, nullptr); // No session - PASS

  // THIS FAILS: Node should have been pruned but still exists (memory leak)
  EXPECT_FALSE(state.nodeExists)
      << "BUG: Node test/A/B/C still exists after publish ended and was the "
         "only content. "
         "onPublishDone should have called tryPruneChild to clean up empty "
         "nodes.";

  removeSession(publisher);
}

// Test: Mixed content types - node with publishNamespace + publish
TEST_F(MoQRelayTest, MixedContentPublishNamespaceAndPublish) {
  auto publisher = createMockSession();

  // PublishNamespace test/A/B
  TrackNamespace nsAB{{"test", "A", "B"}};
  doPublishNamespace(publisher, nsAB);

  // Publish a track in test/A/B
  doPublish(publisher, FullTrackName{nsAB, "track1"});

  // PublishNamespaceDone - should NOT prune because publish still exists
  withSessionContext(publisher, [&]() {
    getOrCreateMockState(publisher)
        ->publishNamespaceHandles[0]
        ->publishNamespaceDone();
    getOrCreateMockState(publisher)->publishNamespaceHandles.clear();
  });

  // Verify node still exists by publishing another track
  doPublish(publisher, FullTrackName{nsAB, "track2"});

  removeSession(publisher);
}

// Test: Mixed content types - node with publishNamespace + sessions
// (subscribers)
TEST_F(MoQRelayTest, MixedContentPublishNamespaceAndSessions) {
  auto publisher = createMockSession();
  auto subscriber = createMockSession();

  // PublishNamespace test/A/B
  TrackNamespace nsAB{{"test", "A", "B"}};
  doPublishNamespace(publisher, nsAB);

  // Subscribe to namespace from another session
  doSubscribeNamespace(subscriber, nsAB);

  // PublishNamespaceDone from publisher - should NOT prune because subscriber
  // still there
  std::shared_ptr<Subscriber::PublishNamespaceHandle> handle =
      getOrCreateMockState(publisher)->publishNamespaceHandles[0];
  getOrCreateMockState(publisher)->publishNamespaceHandles.clear();
  withSessionContext(publisher, [&]() { handle->publishNamespaceDone(); });

  // PublishNamespace again from a different publisher - should work (node still
  // exists)
  auto publisher2 = createMockSession();
  doPublishNamespace(publisher2, nsAB);

  removeSession(publisher);
  removeSession(publisher2);
  removeSession(subscriber);
}

// Test: UnsubscribeNamespace triggers pruning
TEST_F(MoQRelayTest, PruneOnUnsubscribeNamespace) {
  auto subscriber = createMockSession();

  // Subscribe to test/A/B/C namespace (creates tree without publishNamespace)
  TrackNamespace nsABC{{"test", "A", "B", "C"}};
  auto handle = doSubscribeNamespace(subscriber, nsABC, /*addToState=*/false);

  // Unsubscribe - should prune the entire test/A/B/C tree
  withSessionContext(subscriber, [&]() { handle->unsubscribeNamespace(); });

  // Verify tree was pruned by subscribing again - should create fresh tree
  doSubscribeNamespace(subscriber, nsABC);

  removeSession(subscriber);
}

// Test: Middle empty nodes in deep tree
// Scenario: test/A (has publishNamespace), test/A/B (empty), test/A/B/C (has
// publish) Remove C should prune B but keep A
TEST_F(MoQRelayTest, PruneMiddleEmptyNode) {
  auto publisherA = createMockSession();
  auto publisherC = createMockSession();

  // PublishNamespace test/A
  TrackNamespace nsA{{"test", "A"}};
  doPublishNamespace(publisherA, nsA);

  // PublishNamespace test/A/B/C (this creates B as empty intermediate node)
  TrackNamespace nsABC{{"test", "A", "B", "C"}};
  auto handleC = doPublishNamespace(publisherC, nsABC, /*addToState=*/false);

  // PublishNamespaceDone test/A/B/C - should prune B and C but keep A
  withSessionContext(publisherC, [&]() { handleC->publishNamespaceDone(); });

  // Verify test/A still exists
  auto sessionsA = relay_->findPublishNamespaceSessions(nsA);
  EXPECT_EQ(sessionsA.size(), 1);
  EXPECT_EQ(sessionsA[0], publisherA);

  // Verify test/A/B/C was pruned - should be able to publishNamespace it again
  auto publisherC2 = createMockSession();
  doPublishNamespace(publisherC2, nsABC);

  removeSession(publisherA);
  removeSession(publisherC);
  removeSession(publisherC2);
}

// Test: Double publishNamespaceDone doesn't crash or corrupt state
TEST_F(MoQRelayTest, DoublePublishNamespaceDone) {
  auto publisher = createMockSession();

  // PublishNamespace test/A/B
  TrackNamespace nsAB{{"test", "A", "B"}};
  auto handle = doPublishNamespace(publisher, nsAB, /*addToState=*/false);

  // PublishNamespaceDone once
  withSessionContext(publisher, [&]() { handle->publishNamespaceDone(); });

  // PublishNamespaceDone again - should not crash (code handles this
  // gracefully)
  withSessionContext(publisher, [&]() { handle->publishNamespaceDone(); });

  // Verify we can still use the relay
  auto publisher2 = createMockSession();
  doPublishNamespace(publisher2, nsAB);

  removeSession(publisher);
  removeSession(publisher2);
}

// Test: Ownership check in publishNamespaceDone prevents non-owner from
// clearing state When a session calls publishNamespaceDone but is not the owner
// of the namespace, the publishNamespaceDone should be ignored and the real
// owner should remain.
TEST_F(MoQRelayTest, StalePublishNamespaceDoneDoesNotAffectNewOwner) {
  auto publisher1 = createMockSession();
  auto publisher2 = createMockSession();

  // Publisher1 publishNamespaces test/A/B
  TrackNamespace nsAB{{"test", "A", "B"}};
  auto handle1 = doPublishNamespace(publisher1, nsAB, /*addToState=*/false);

  // Publisher2 publishNamespaces a child namespace test/A/B/C
  TrackNamespace nsABC{{"test", "A", "B", "C"}};
  doPublishNamespace(publisher2, nsABC);

  // Publisher2 tries to publishNamespaceDone Publisher1's namespace test/A/B
  // using Publisher1's handle but with Publisher2's session context - should be
  // ignored because publisher2 is not the owner of test/A/B
  withSessionContext(publisher2, [&]() { handle1->publishNamespaceDone(); });

  // Verify publisher1 is STILL the owner by checking
  // findPublishNamespaceSessions returns publisher1 for the namespace. If the
  // ownership check didn't work, sourceSession would be null and
  // findPublishNamespaceSessions would return empty.
  auto sessions = relay_->findPublishNamespaceSessions(nsAB);
  EXPECT_EQ(sessions.size(), 1)
      << "Ownership check failed: findPublishNamespaceSessions returned wrong count";
  if (!sessions.empty()) {
    EXPECT_EQ(sessions[0], publisher1)
        << "Ownership check failed: wrong session returned";
  }

  // Publisher1 should still be able to properly publishNamespaceDone its own
  // namespace
  withSessionContext(publisher1, [&]() { handle1->publishNamespaceDone(); });

  // Clean up - should not crash
  removeSession(publisher1);
  removeSession(publisher2);
}

// Test: Pruning with multiple children at same level
// Scenario: test/A has children B, C, D. Only B has content.
// Remove B should prune B but keep A, C, D structure intact
TEST_F(MoQRelayTest, PruneOneOfMultipleChildren) {
  auto publisherB = createMockSession();
  auto subscriberC = createMockSession();
  auto subscriberD = createMockSession();

  // PublishNamespace test/A/B
  TrackNamespace nsAB{{"test", "A", "B"}};
  auto handleB = doPublishNamespace(publisherB, nsAB, /*addToState=*/false);

  // Subscribe to test/A/C (creates C as empty node with session)
  TrackNamespace nsAC{{"test", "A", "C"}};
  doSubscribeNamespace(subscriberC, nsAC);

  // Subscribe to test/A/D
  TrackNamespace nsAD{{"test", "A", "D"}};
  doSubscribeNamespace(subscriberD, nsAD);

  // PublishNamespaceDone test/A/B - should prune only B
  withSessionContext(publisherB, [&]() { handleB->publishNamespaceDone(); });

  // Verify test/A still exists (has children C and D)
  // Try to publishNamespace at test/A
  auto publisherA = createMockSession();
  TrackNamespace nsA{{"test", "A"}};
  doPublishNamespace(publisherA, nsA);

  removeSession(publisherB);
  removeSession(publisherA);
  removeSession(subscriberC);
  removeSession(subscriberD);
}

// Test: Empty namespace edge case
TEST_F(MoQRelayTest, EmptyNamespacePublishNamespaceDone) {
  auto publisher = createMockSession();

  // Try to publishNamespace empty namespace (edge case)
  TrackNamespace emptyNs{{}};

  // This might fail or succeed depending on implementation
  // Just verify it doesn't crash
  PublishNamespace ann;
  ann.trackNamespace = emptyNs;
  withSessionContext(publisher, [&]() {
    auto task = relay_->publishNamespace(std::move(ann), nullptr);
    auto res = folly::coro::blockingWait(std::move(task), exec_.get());
    // Don't assert on success/failure, just verify no crash
    if (res.hasValue()) {
      getOrCreateMockState(publisher)->publishNamespaceHandles.push_back(
          res.value());
    }
  });

  removeSession(publisher);
}

// Test: Verify activeChildCount consistency after complex operations
TEST_F(MoQRelayTest, ActiveChildCountConsistency) {
  auto pub1 = createMockSession();
  auto pub2 = createMockSession();
  auto sub1 = createMockSession();

  // Build tree: test/A/B and test/A/C with different content types
  TrackNamespace nsAB{{"test", "A", "B"}};
  doPublishNamespace(pub1, nsAB);

  TrackNamespace nsAC{{"test", "A", "C"}};
  doSubscribeNamespace(sub1, nsAC);

  // At this point, test/A should have activeChildCount_ == 2 (B and C)
  // We can't directly access private members, but we can verify behavior

  // Remove pub1 (which should remove B and decrement A's count)
  removeSession(pub1);

  // A should still exist (C is still active)
  // Verify by announcing at test/A
  TrackNamespace nsA{{"test", "A"}};
  doPublishNamespace(pub2, nsA);

  // Remove sub1 (which should remove C)
  removeSession(sub1);

  // A should still exist because pub2 published at A
  auto sessions = relay_->findPublishNamespaceSessions(nsA);
  EXPECT_EQ(sessions.size(), 1);
  EXPECT_EQ(sessions[0], pub2);

  removeSession(pub2);
}

// Test: Publish then publishNamespaceDone shouldn't prune while publish active
TEST_F(MoQRelayTest, PublishKeepsNodeAliveAfterPublishNamespaceDone) {
  auto publisher = createMockSession();

  // PublishNamespace test/A/B
  TrackNamespace nsAB{{"test", "A", "B"}};
  doPublishNamespace(publisher, nsAB);

  // Publish track
  doPublish(publisher, FullTrackName{nsAB, "track1"});

  // PublishNamespaceDone (but publish is still active)
  std::shared_ptr<Subscriber::PublishNamespaceHandle> handle =
      getOrCreateMockState(publisher)->publishNamespaceHandles[0];
  getOrCreateMockState(publisher)->publishNamespaceHandles.clear();
  withSessionContext(publisher, [&]() { handle->publishNamespaceDone(); });

  // Try to publish another track - should work (node still exists)
  doPublish(publisher, FullTrackName{nsAB, "track2"});

  removeSession(publisher);
}

// Test: SubscribeNamespace only receives publishes while active
// Sequence: subscribeNamespace (sub1), publish (sub1 gets it),
// beginSubgroup, publishDone, subscribeNamespace (sub2), new publish
// (only sub2 gets it)
TEST_F(MoQRelayTest, SubscribeNamespaceDoesntAddDrainingPublish) {
  auto publisherSession = createMockSession();
  auto subscriber1 = createMockSession();
  auto subscriber2 = createMockSession();

  // Subscriber 1 subscribes to publishNamespaces
  auto handle1 =
      doSubscribeNamespace(subscriber1, kTestNamespace, /*addToState=*/false);

  // Publish first track - subscriber 1 should receive it
  auto mockConsumer1 = createMockConsumer();
  EXPECT_CALL(*subscriber1, publish(testing::_, testing::_))
      .WillOnce([mockConsumer1](const auto& /*pubReq*/, auto /*subHandle*/) {
        return Subscriber::PublishResult(
            Subscriber::PublishConsumerAndReplyTask{
                mockConsumer1,
                []() -> folly::coro::Task<
                         folly::Expected<PublishOk, PublishError>> {
                  co_return PublishOk{/*requestID=*/RequestID(1),
                                      /*forward=*/true,
                                      /*subscriberPriority=*/0,
                                      /*groupOrder=*/GroupOrder::OldestFirst,
                                      /*locType=*/LocationType::LargestObject,
                                      /*start=*/std::nullopt,
                                      /*endGroup=*/std::nullopt};
                }()});
      });

  EXPECT_CALL(*mockConsumer1, beginSubgroup(_, _, _, _))
      .WillOnce([&](uint64_t, uint64_t, uint8_t, bool) {
        auto sg = std::make_shared<NiceMock<MockSubgroupConsumer>>();
        EXPECT_CALL(*sg, endOfSubgroup())
            .WillOnce(testing::Return(folly::unit));
        return folly::
            makeExpected<MoQPublishError, std::shared_ptr<SubgroupConsumer>>(
                sg);
      });

  // Begin a subgroup for ongoing publish activity
  auto pubConsumer = doPublish(
      publisherSession,
      FullTrackName{kTestNamespace, "track_stream"},
      /*addToState=*/false);
  // TODO: bug subscriber not added until next loop?
  exec_->drive();
  auto subgroupRes = pubConsumer->beginSubgroup(0, 0, 0);
  EXPECT_TRUE(subgroupRes.hasValue());
  auto subgroup = *subgroupRes;

  // publisher ends subscription
  EXPECT_CALL(*mockConsumer1, publishDone(testing::_));
  EXPECT_TRUE(pubConsumer
                  ->publishDone(
                      {RequestID(1),
                       PublishDoneStatusCode::TRACK_ENDED,
                       0,
                       "track ended"})
                  .hasValue());
  subgroup->endOfSubgroup();

  // Subscriber 2 subscribes to publishNamespaces but doesn't get finished track
  doSubscribeNamespace(subscriber2, kTestNamespace);

  // First publish (existing context handles initial publish), now publish a
  // second track
  // Expect publish calls on both subscribers, just fail them.
  EXPECT_CALL(*subscriber1, publish(testing::_, testing::_))
      .WillOnce([](const auto& /*pubReq*/, auto /*subHandle*/) {
        return folly::makeUnexpected(PublishError{});
      });

  EXPECT_CALL(*subscriber2, publish(testing::_, testing::_))
      .WillOnce([](const auto& /*pubReq*/, auto /*subHandle*/) {
        return folly::makeUnexpected(PublishError{});
      });

  auto pubConsumer2 = doPublish(
      publisherSession, FullTrackName{kTestNamespace, "track_stream_2"});
  exec_->drive();

  removeSession(publisherSession);
  removeSession(subscriber1);
  removeSession(subscriber2);
}

TEST_F(MoQRelayTest, SubscribeNamespaceEmptyPrefixRejectedPreV16) {
  // Default session uses kVersionDraftCurrent (draft-14, which is < 16)
  auto session = createMockSession();

  TrackNamespace emptyNs{{}};
  SubscribeNamespace subNs;
  subNs.trackNamespacePrefix = emptyNs;

  withSessionContext(session, [&]() {
    auto task = relay_->subscribeNamespace(std::move(subNs), nullptr);
    auto res = folly::coro::blockingWait(std::move(task), exec_.get());
    ASSERT_FALSE(res.hasValue())
        << "Empty namespace prefix should be rejected for pre-v16 sessions";
    EXPECT_EQ(
        res.error().errorCode,
        SubscribeNamespaceErrorCode::NAMESPACE_PREFIX_UNKNOWN);
    EXPECT_EQ(res.error().reasonPhrase, "empty");
  });

  removeSession(session);
}

TEST_F(MoQRelayTest, SubscribeNamespaceEmptyPrefixAllowedV16) {
  auto session = createMockSession();
  // Override the negotiated version to draft-16
  ON_CALL(*session, getNegotiatedVersion())
      .WillByDefault(Return(std::optional<uint64_t>(kVersionDraft16)));

  TrackNamespace emptyNs{{}};
  doSubscribeNamespace(session, emptyNs);

  removeSession(session);
}

// ============================================================
// Extensions Tests
// ============================================================

// Test: Extensions from publish are forwarded to subscribers via
// subscribeNamespace
TEST_F(MoQRelayTest, PublishExtensionsForwardedToSubscribers) {
  auto publisherSession = createMockSession();
  auto subscriber = createMockSession();

  // Subscribe to namespace first
  auto mockConsumer = createMockConsumer();
  Extensions receivedExtensions;
  EXPECT_CALL(*subscriber, publish(testing::_, testing::_))
      .WillOnce([&mockConsumer, &receivedExtensions](
                    const PublishRequest& pubReq, auto /*subHandle*/) {
        receivedExtensions = pubReq.extensions;
        return Subscriber::PublishResult(
            Subscriber::PublishConsumerAndReplyTask{
                mockConsumer,
                []() -> folly::coro::Task<
                         folly::Expected<PublishOk, PublishError>> {
                  co_return PublishOk{
                      RequestID(1),
                      true,
                      0,
                      GroupOrder::OldestFirst,
                      LocationType::LargestObject,
                      std::nullopt,
                      std::nullopt};
                }()});
      });

  doSubscribeNamespace(subscriber, kTestNamespace);

  // Publish with extensions (both known and unknown)
  PublishRequest pub;
  pub.fullTrackName = kTestTrackName;
  pub.extensions.insertMutableExtension(
      Extension{kDeliveryTimeoutExtensionType, 5000});
  pub.extensions.insertMutableExtension(Extension{0xBEEF'0000, 42});

  withSessionContext(publisherSession, [&]() {
    auto res = relay_->publish(std::move(pub), createMockSubscriptionHandle());
    EXPECT_TRUE(res.hasValue());
    if (res.hasValue()) {
      getOrCreateMockState(publisherSession)
          ->publishConsumers.push_back(res->consumer);
    }
  });
  exec_->drive();

  // Verify extensions were forwarded
  EXPECT_EQ(
      receivedExtensions.getIntExtension(kDeliveryTimeoutExtensionType), 5000);
  EXPECT_EQ(receivedExtensions.getIntExtension(0xBEEF'0000), 42);

  removeSession(publisherSession);
  removeSession(subscriber);
}

// ============================================================
// Dynamic Groups Extension Tests
// ============================================================

// Test: relay PUBLISH path – dynamic groups from PublishRequest extensions
// is stored in the forwarder and forwarded to every downstream subscriber
TEST_F(MoQRelayTest, RelayPublishPropagatesDynamicGroupsToSubscribers) {
  auto publisherSession = createMockSession();
  auto subscriberSession = createMockSession();

  // Build a PublishRequest with DYNAMIC_GROUPS enabled
  PublishRequest pub;
  pub.fullTrackName = kTestTrackName;
  setPublisherDynamicGroups(pub, true);

  withSessionContext(publisherSession, [&]() {
    auto res = relay_->publish(std::move(pub), createMockSubscriptionHandle());
    ASSERT_TRUE(res.hasValue());
    getOrCreateMockState(publisherSession)
        ->publishConsumers.push_back(res->consumer);
  });

  auto consumer = createMockConsumer();
  auto handle = subscribeToTrack(
      subscriberSession, kTestTrackName, consumer, RequestID(1));
  ASSERT_NE(handle, nullptr);

  auto dynGroups = getPublisherDynamicGroups(handle->subscribeOk());
  ASSERT_TRUE(dynGroups.has_value());
  EXPECT_TRUE(*dynGroups);

  removeSession(subscriberSession);
  exec_->drive();
  removeSession(publisherSession);
}

// Test: Extensions from publish are forwarded to late-joining subscribers
TEST_F(MoQRelayTest, PublishExtensionsForwardedToLateJoiners) {
  auto publisherSession = createMockSession();
  auto subscriber1 = createMockSession();
  auto subscriber2 = createMockSession();

  // Subscriber 1 subscribes first
  auto mockConsumer1 = createMockConsumer();
  EXPECT_CALL(*subscriber1, publish(testing::_, testing::_))
      .WillOnce([&mockConsumer1](const auto&, auto) {
        return Subscriber::PublishResult(
            Subscriber::PublishConsumerAndReplyTask{
                mockConsumer1,
                []() -> folly::coro::Task<
                         folly::Expected<PublishOk, PublishError>> {
                  co_return PublishOk{
                      RequestID(1),
                      true,
                      0,
                      GroupOrder::OldestFirst,
                      LocationType::LargestObject,
                      std::nullopt,
                      std::nullopt};
                }()});
      });

  doSubscribeNamespace(subscriber1, kTestNamespace);

  // Publish with extensions
  PublishRequest pub;
  pub.fullTrackName = kTestTrackName;
  pub.extensions.insertMutableExtension(
      Extension{kDeliveryTimeoutExtensionType, 3000});
  pub.extensions.insertMutableExtension(Extension{0xCAFE'0000, 99});

  withSessionContext(publisherSession, [&]() {
    auto res = relay_->publish(std::move(pub), createMockSubscriptionHandle());
    EXPECT_TRUE(res.hasValue());
    if (res.hasValue()) {
      getOrCreateMockState(publisherSession)
          ->publishConsumers.push_back(res->consumer);
    }
  });
  exec_->drive();

  // Late-joining subscriber 2 should also get extensions
  Extensions receivedExtensions;
  auto mockConsumer2 = createMockConsumer();
  EXPECT_CALL(*subscriber2, publish(testing::_, testing::_))
      .WillOnce([&mockConsumer2, &receivedExtensions](
                    const PublishRequest& pubReq, auto) {
        receivedExtensions = pubReq.extensions;
        return Subscriber::PublishResult(
            Subscriber::PublishConsumerAndReplyTask{
                mockConsumer2,
                []() -> folly::coro::Task<
                         folly::Expected<PublishOk, PublishError>> {
                  co_return PublishOk{
                      RequestID(2),
                      true,
                      0,
                      GroupOrder::OldestFirst,
                      LocationType::LargestObject,
                      std::nullopt,
                      std::nullopt};
                }()});
      });

  doSubscribeNamespace(subscriber2, kTestNamespace);
  exec_->drive();

  // Verify late-joiner received extensions
  EXPECT_EQ(
      receivedExtensions.getIntExtension(kDeliveryTimeoutExtensionType), 3000);
  EXPECT_EQ(receivedExtensions.getIntExtension(0xCAFE'0000), 99);

  removeSession(publisherSession);
  removeSession(subscriber1);
  removeSession(subscriber2);
}

// Test: relay SUBSCRIBE path – dynamic groups from the upstream SubscribeOk is
// stored in the forwarder and forwarded to both the first and late-joining
// downstream subscribers
TEST_F(MoQRelayTest, RelaySubscribePropagatesDynamicGroupsToAllSubscribers) {
  auto publisherSession = createMockSession();
  auto subscriber1 = createMockSession();
  auto subscriber2 = createMockSession();

  doPublishNamespace(publisherSession, kTestNamespace);

  // Upstream returns a SubscribeOk with DYNAMIC_GROUPS = true
  SubscribeOk upstreamOk;
  upstreamOk.requestID = RequestID(1);
  upstreamOk.trackAlias = TrackAlias(1);
  upstreamOk.expires = std::chrono::milliseconds(0);
  upstreamOk.groupOrder = GroupOrder::OldestFirst;
  setPublisherDynamicGroups(upstreamOk, true);

  EXPECT_CALL(*publisherSession, subscribe(_, _))
      .WillOnce([upstreamOk](const auto& /*req*/, auto /*consumer*/) {
        auto handle =
            std::make_shared<NiceMock<MockSubscriptionHandle>>(upstreamOk);
        return folly::coro::makeTask<Publisher::SubscribeResult>(
            folly::
                Expected<std::shared_ptr<SubscriptionHandle>, SubscribeError>(
                    handle));
      });

  // First subscriber
  auto consumer1 = createMockConsumer();
  auto handle1 =
      subscribeToTrack(subscriber1, kTestTrackName, consumer1, RequestID(1));
  ASSERT_NE(handle1, nullptr);
  auto dynGroups1 = getPublisherDynamicGroups(handle1->subscribeOk());
  ASSERT_TRUE(dynGroups1.has_value());
  EXPECT_TRUE(*dynGroups1);

  // Late-joining second subscriber – forwarder should propagate the stored
  // dynamic groups value without another upstream roundtrip
  auto consumer2 = createMockConsumer();
  auto handle2 =
      subscribeToTrack(subscriber2, kTestTrackName, consumer2, RequestID(2));
  ASSERT_NE(handle2, nullptr);
  auto dynGroups2 = getPublisherDynamicGroups(handle2->subscribeOk());
  ASSERT_TRUE(dynGroups2.has_value());
  EXPECT_TRUE(*dynGroups2);

  removeSession(publisherSession);
  removeSession(subscriber1);
  removeSession(subscriber2);
}

TEST_F(MoQRelayTest, ExactNamespaceSubscriberReceivesPublishNamespace) {
  auto subscriber = createMockSession();
  auto publisher = createMockSession();

  // Subscriber subscribes to exact namespace {"test", "namespace"}
  doSubscribeNamespace(subscriber, kTestNamespace);

  // Expect the subscriber to receive a publishNamespace forwarding when
  // the publisher announces the same exact namespace
  EXPECT_CALL(*subscriber, publishNamespace(_, _))
      .WillOnce(
          [](PublishNamespace ann,
             auto) -> folly::coro::Task<Subscriber::PublishNamespaceResult> {
            EXPECT_EQ(ann.trackNamespace, kTestNamespace);
            co_return folly::makeUnexpected(
                PublishNamespaceError{
                    ann.requestID,
                    PublishNamespaceErrorCode::UNINTERESTED,
                    "test"});
          });

  // Publisher announces the same exact namespace
  doPublishNamespace(publisher, kTestNamespace);

  // Drive the executor so the async publishNamespace forwarding runs
  exec_->drive();

  removeSession(publisher);
  removeSession(subscriber);
}

// Test: TrackStatus on non-existent track
TEST_F(MoQRelayTest, TrackStatusNonExistentTrack) {
  auto clientSession = createMockSession();

  // Request trackStatus for a track that doesn't exist
  TrackStatus trackStatus;
  trackStatus.fullTrackName = kTestTrackName;
  trackStatus.requestID = RequestID(1);

  withSessionContext(clientSession, [&]() {
    auto task = relay_->trackStatus(trackStatus);
    auto res = folly::coro::blockingWait(std::move(task), exec_.get());

    // Should return error indicating track not found
    EXPECT_FALSE(res.hasValue());
    EXPECT_EQ(res.error().errorCode, TrackStatusErrorCode::TRACK_NOT_EXIST);
    EXPECT_FALSE(res.error().reasonPhrase.empty());
  });

  removeSession(clientSession);
}

// Test: TrackStatus on existing track - returns forwarder state (no upstream
// call)
TEST_F(MoQRelayTest, TrackStatusSuccessfulForward) {
  auto publisherSession = createMockSession();
  auto clientSession = createMockSession();

  doPublish(publisherSession, kTestTrackName);

  auto consumer = createMockConsumer();
  subscribeToTrack(clientSession, kTestTrackName, consumer, RequestID(1));

  TrackStatus trackStatus;
  trackStatus.fullTrackName = kTestTrackName;
  trackStatus.requestID = RequestID(2);

  withSessionContext(clientSession, [&]() {
    auto task = relay_->trackStatus(trackStatus);
    auto res = folly::coro::blockingWait(std::move(task), exec_.get());

    // Should return status from local forwarder
    // Since no data was sent, statusCode should be TRACK_NOT_STARTED
    EXPECT_TRUE(res.hasValue());
    EXPECT_EQ(res.value().statusCode, TrackStatusCode::TRACK_NOT_STARTED);
    EXPECT_EQ(res.value().fullTrackName, kTestTrackName);
  });

  removeSession(clientSession);
  exec_->drive();
  removeSession(publisherSession);
}

// Test: TrackStatus using namespace prefix matching (no exact subscription)
// Verifies that when there's no exact subscription but a publisher has
// published a matching namespace prefix, the relay correctly routes
// TRACK_STATUS upstream using prefix matching
TEST_F(MoQRelayTest, TrackStatusViaPrefixMatching) {
  auto publisher = createMockSession();
  auto requester = createMockSession();

  // Publisher publishes namespace but NOT the specific track
  doPublishNamespace(publisher, kTestNamespace);

  // No exact subscription exists for kTestTrackName, so trackStatus should
  // use prefix matching to find the publisher

  // Mock the upstream trackStatus call
  TrackStatusOk statusOk;
  statusOk.requestID = RequestID(1);
  statusOk.trackAlias = TrackAlias(0);
  statusOk.largest = AbsoluteLocation{50, 25};

  EXPECT_CALL(*publisher, trackStatus(_))
      .WillOnce([statusOk](const auto& /*ts*/) {
        return folly::coro::makeTask<Publisher::TrackStatusResult>(statusOk);
      });

  // Execute trackStatus from requester's perspective
  TrackStatus trackStatus;
  trackStatus.requestID = RequestID(1);
  trackStatus.fullTrackName = kTestTrackName;

  withSessionContext(requester, [&]() {
    auto task = relay_->trackStatus(trackStatus);
    auto result = folly::coro::blockingWait(std::move(task), exec_.get());

    // Should successfully forward via prefix matching and return the result
    EXPECT_TRUE(result.hasValue())
        << "TrackStatus via namespace prefix matching should succeed";
    EXPECT_EQ(result.value().requestID, RequestID(1));
    EXPECT_TRUE(result.value().largest.has_value());
    EXPECT_EQ(result.value().largest->group, 50);
    EXPECT_EQ(result.value().largest->object, 25);
  });

  removeSession(publisher);
  removeSession(requester);
}

// ============================================================
// New Group Request (NGR) Tests
// ============================================================

namespace {
// Simple callback that records every newGroupRequested call.
struct TestNGRCallback : public MoQForwarder::Callback {
  void onEmpty(MoQForwarder*) override {}
  void newGroupRequested(MoQForwarder*, uint64_t group) override {
    calls.push_back(group);
  }
  std::vector<uint64_t> calls;
};

// Build a minimal params object carrying NEW_GROUP_REQUEST=val.
auto makeNGRParams(uint64_t val) {
  RequestUpdate upd;
  upd.params.insertParam(Parameter(
      folly::to_underlying(TrackRequestParamKey::NEW_GROUP_REQUEST), val));
  return upd.params;
}
} // namespace

// Relay test: When a late-joining subscriber sends NEW_GROUP_REQUEST in its
// SUBSCRIBE, the relay forwards it upstream via REQUEST_UPDATE
TEST_F(MoQRelayTest, RelaySubscribeLateJoinerNGRForwardedUpstream) {
  auto publisherSession = createMockSession();
  auto subscriber1 = createMockSession();
  auto subscriber2 = createMockSession();

  doPublishNamespace(publisherSession, kTestNamespace);

  // Upstream SubscribeOk advertises DYNAMIC_GROUPS = true
  SubscribeOk upstreamOk;
  upstreamOk.requestID = RequestID(100);
  upstreamOk.trackAlias = TrackAlias(1);
  upstreamOk.expires = std::chrono::milliseconds(0);
  upstreamOk.groupOrder = GroupOrder::OldestFirst;
  setPublisherDynamicGroups(upstreamOk, true);

  auto upstreamHandle =
      std::make_shared<NiceMock<MockSubscriptionHandle>>(upstreamOk);
  ON_CALL(*upstreamHandle, requestUpdateResult())
      .WillByDefault(Return(
          folly::makeExpected<RequestError>(RequestOk{
              RequestID(0),
              TrackRequestParameters(FrameType::REQUEST_OK),
              {}})));

  EXPECT_CALL(*publisherSession, subscribe(_, _))
      .WillOnce([&upstreamHandle](const auto& /*req*/, auto /*consumer*/) {
        return folly::coro::makeTask<Publisher::SubscribeResult>(
            folly::
                Expected<std::shared_ptr<SubscriptionHandle>, SubscribeError>(
                    upstreamHandle));
      });

  // First subscriber establishes the upstream subscription (no NGR)
  auto consumer1 = createMockConsumer();
  auto handle1 =
      subscribeToTrack(subscriber1, kTestTrackName, consumer1, RequestID(1));
  ASSERT_NE(handle1, nullptr);

  // Second subscriber includes NEW_GROUP_REQUEST=8
  auto consumer2 = createMockConsumer();
  SubscribeRequest sub2;
  sub2.fullTrackName = kTestTrackName;
  sub2.requestID = RequestID(2);
  sub2.locType = LocationType::LargestObject;
  sub2.params.insertParam(Parameter(
      folly::to_underlying(TrackRequestParamKey::NEW_GROUP_REQUEST),
      uint64_t(8)));

  // The relay must forward NGR=8 upstream via REQUEST_UPDATE
  EXPECT_CALL(*upstreamHandle, requestUpdateCalled(_))
      .WillOnce([](const RequestUpdate& update) {
        auto ngrValue = getFirstIntParam(
            update.params, TrackRequestParamKey::NEW_GROUP_REQUEST);
        ASSERT_TRUE(ngrValue.has_value());
        EXPECT_EQ(*ngrValue, 8);
      });

  std::shared_ptr<SubscriptionHandle> handle2{nullptr};
  withSessionContext(subscriber2, [&]() {
    auto task = relay_->subscribe(std::move(sub2), consumer2);
    auto res = folly::coro::blockingWait(std::move(task), exec_.get());
    ASSERT_TRUE(res.hasValue());
    handle2 = *res;
  });
  exec_->drive();

  // Register handle2 so removeSession(subscriber2) will unsubscribe it,
  // allowing the forwarder to become empty and release the upstream handle.
  getOrCreateMockState(subscriber2)->subscribeHandles.push_back(handle2);

  removeSession(publisherSession);
  removeSession(subscriber1);
  removeSession(subscriber2);
  exec_->drive();
}

// Relay test: A downstream subscriber sending REQUEST_UPDATE with
// NEW_GROUP_REQUEST causes the relay to cascade the NGR upstream
TEST_F(MoQRelayTest, RelayRequestUpdateNGRCascadedUpstream) {
  auto publisherSession = createMockSession();
  auto subscriberSession = createMockSession();

  doPublishNamespace(publisherSession, kTestNamespace);

  // Upstream SubscribeOk advertises DYNAMIC_GROUPS = true
  SubscribeOk upstreamOk;
  upstreamOk.requestID = RequestID(100);
  upstreamOk.trackAlias = TrackAlias(1);
  upstreamOk.expires = std::chrono::milliseconds(0);
  upstreamOk.groupOrder = GroupOrder::OldestFirst;
  setPublisherDynamicGroups(upstreamOk, true);

  auto upstreamHandle =
      std::make_shared<NiceMock<MockSubscriptionHandle>>(upstreamOk);
  ON_CALL(*upstreamHandle, requestUpdateResult())
      .WillByDefault(Return(
          folly::makeExpected<RequestError>(RequestOk{
              RequestID(0),
              TrackRequestParameters(FrameType::REQUEST_OK),
              {}})));

  EXPECT_CALL(*publisherSession, subscribe(_, _))
      .WillOnce([&upstreamHandle](const auto& /*req*/, auto /*consumer*/) {
        return folly::coro::makeTask<Publisher::SubscribeResult>(
            folly::
                Expected<std::shared_ptr<SubscriptionHandle>, SubscribeError>(
                    upstreamHandle));
      });

  // Subscribe downstream session - triggers upstream subscribe
  auto consumer = createMockConsumer();
  auto handle = subscribeToTrack(
      subscriberSession, kTestTrackName, consumer, RequestID(1));
  ASSERT_NE(handle, nullptr);

  auto* subscriber = dynamic_cast<MoQForwarder::Subscriber*>(handle.get());
  ASSERT_NE(subscriber, nullptr);

  // The relay must cascade NGR=9 upstream via REQUEST_UPDATE
  EXPECT_CALL(*upstreamHandle, requestUpdateCalled(_))
      .WillOnce([](const RequestUpdate& update) {
        auto ngrValue = getFirstIntParam(
            update.params, TrackRequestParamKey::NEW_GROUP_REQUEST);
        ASSERT_TRUE(ngrValue.has_value());
        EXPECT_EQ(*ngrValue, 9);
      });

  // Downstream subscriber sends REQUEST_UPDATE carrying NEW_GROUP_REQUEST=9
  RequestUpdate update;
  update.requestID = RequestID(2);
  update.existingRequestID = RequestID(1);
  update.params.insertParam(Parameter(
      folly::to_underlying(TrackRequestParamKey::NEW_GROUP_REQUEST),
      uint64_t(9)));
  folly::coro::blockingWait(subscriber->requestUpdate(std::move(update)));
  exec_->drive();

  removeSession(publisherSession);
  removeSession(subscriberSession);
}

// Unit test: Subscriber::onPublishOk postprocessing
// Verifies that onPublishOk correctly updates:
// 1. Subscriber range based on PublishOk fields
// 2. shouldForward flag
// 3. NEW_GROUP_REQUEST forwarding when it passes gating checks
TEST_F(MoQRelayTest, SubscriberOnPublishOkPostprocessing) {
  auto publisherSession = createMockSession();
  auto subscriberSession = createMockSession();

  // Publish track and subscribe
  auto publishConsumer = doPublish(publisherSession, kTestTrackName);
  auto consumer = createMockConsumer();
  auto handle = subscribeToTrack(
      subscriberSession, kTestTrackName, consumer, RequestID(1));
  ASSERT_NE(handle, nullptr);

  // Cast to access Subscriber internals
  auto* subscriber = dynamic_cast<MoQForwarder::Subscriber*>(handle.get());
  ASSERT_NE(subscriber, nullptr);

  // Test 1: Range update from PublishOk
  // Create a PublishOk with specific start/end locations
  PublishOk pubOk1{
      RequestID(1),                // requestID
      true,                        // forward
      0,                           // subscriberPriority
      GroupOrder::OldestFirst,     // groupOrder
      LocationType::AbsoluteStart, // locType
      AbsoluteLocation{5, 0},      // start
      uint64_t(15),                // endGroup
      TrackRequestParameters(FrameType::PUBLISH_OK)};

  // Apply the postprocessing
  subscriber->onPublishOk(pubOk1);

  // Verify range was updated
  EXPECT_EQ(subscriber->range.start.group, 5);

  // Test 2: Forward flag update
  subscriber->shouldForward = true;
  PublishOk pubOk2{
      RequestID(2),
      false, // forward = false
      0,
      GroupOrder::OldestFirst,
      LocationType::AbsoluteStart,
      AbsoluteLocation{5, 0},
      uint64_t(15),
      TrackRequestParameters(FrameType::PUBLISH_OK)};
  subscriber->onPublishOk(pubOk2);
  EXPECT_FALSE(subscriber->shouldForward)
      << "Forward flag should be updated to false";

  subscriber->shouldForward = false;
  PublishOk pubOk3{
      RequestID(3),
      true, // forward = true
      0,
      GroupOrder::OldestFirst,
      LocationType::AbsoluteStart,
      AbsoluteLocation{5, 0},
      uint64_t(15),
      TrackRequestParameters(FrameType::PUBLISH_OK)};
  subscriber->onPublishOk(pubOk3);
  EXPECT_TRUE(subscriber->shouldForward)
      << "Forward flag should be updated to true";

  // Test 3: NEW_GROUP_REQUEST forwarding via onPublishOk
  // Enable dynamic groups and attach a callback to observe NGR fires
  PublishRequest pub;
  setPublisherDynamicGroups(pub, true);
  subscriber->forwarder.setExtensions(pub.extensions);

  auto cb = std::make_shared<TestNGRCallback>();
  subscriber->forwarder.setCallback(cb);

  // Build a PublishOk carrying NEW_GROUP_REQUEST=20
  TrackRequestParameters ngrParams(FrameType::PUBLISH_OK);
  ngrParams.insertParam(Parameter(
      folly::to_underlying(TrackRequestParamKey::NEW_GROUP_REQUEST),
      uint64_t(20)));
  PublishOk pubOk4{
      RequestID(4),
      true,
      0,
      GroupOrder::OldestFirst,
      LocationType::AbsoluteStart,
      AbsoluteLocation{10, 0},
      std::nullopt,
      std::move(ngrParams)};

  // onPublishOk should fire the NGR callback for group 20
  subscriber->onPublishOk(pubOk4);
  ASSERT_EQ(cb->calls.size(), 1u);
  EXPECT_EQ(cb->calls[0], 20u);
  cb->calls.clear();

  // outstanding=20: re-requesting group 20 is a no-op; group 21 fires
  subscriber->forwarder.tryProcessNewGroupRequest(makeNGRParams(20));
  EXPECT_TRUE(cb->calls.empty()) << "Group 20 already outstanding";
  subscriber->forwarder.tryProcessNewGroupRequest(makeNGRParams(21));
  ASSERT_EQ(cb->calls.size(), 1u);
  EXPECT_EQ(cb->calls[0], 21u);

  removeSession(publisherSession);
  removeSession(subscriberSession);
}

// Repro for moxygen#168: a PUBLISH_OK arriving after beginObject has already
// fanned out must not orphan the subscriber's open SubgroupConsumer.
// Sequence:
//   1. Subscribe (LargestObject -> range.start resolves to {0,0} with no data).
//   2. Publisher begins multi-chunk object 5; forwarder creates sg and
//      delivers the initial payload.
//   3. PUBLISH_OK arrives late (LargestObject) -> range.start becomes {0,6}.
//   4. Publisher sends the object 5 continuation. Before the fix, the
//      forwarder re-checked range.start on the existing sg and silently
//      skipped it, stranding it with a partial object and tripping
//      validatePublish on the next beginObject downstream.
TEST_F(MoQRelayTest, SubscriberOnPublishOkDoesNotStrandPartialObject) {
  auto publisherSession = createMockSession();
  auto subscriberSession = createMockSession();

  auto mockConsumer = createMockConsumer();
  std::shared_ptr<NiceMock<MockSubgroupConsumer>> sg;

  EXPECT_CALL(*mockConsumer, beginSubgroup(_, _, _, _))
      .WillOnce([&](uint64_t, uint64_t, uint8_t, bool) {
        sg = std::make_shared<NiceMock<MockSubgroupConsumer>>();
        ON_CALL(*sg, beginObject(_, _, _, _))
            .WillByDefault(
                Return(folly::makeExpected<MoQPublishError>(folly::unit)));
        ON_CALL(*sg, objectPayload(_, _))
            .WillByDefault(
                Return(folly::makeExpected<MoQPublishError>(
                    ObjectPublishStatus::IN_PROGRESS)));
        return folly::
            makeExpected<MoQPublishError, std::shared_ptr<SubgroupConsumer>>(
                sg);
      });

  auto publishConsumer = doPublish(publisherSession, kTestTrackName);

  auto handle = subscribeToTrack(
      subscriberSession, kTestTrackName, mockConsumer, RequestID(1));
  ASSERT_NE(handle, nullptr);
  auto* subscriber = dynamic_cast<MoQForwarder::Subscriber*>(handle.get());
  ASSERT_NE(subscriber, nullptr);

  auto subgroupRes = publishConsumer->beginSubgroup(0, 0, 0);
  ASSERT_TRUE(subgroupRes.hasValue());
  auto subgroup = *subgroupRes;

  constexpr uint64_t kObjectID = 5;
  constexpr uint64_t kObjectLength = 100;
  constexpr uint64_t kInitialLength = 20;
  auto initial = folly::IOBuf::copyBuffer(std::string(kInitialLength, 'a'));
  ASSERT_TRUE(
      subgroup
          ->beginObject(kObjectID, kObjectLength, std::move(initial), {})
          .hasValue());
  ASSERT_NE(sg, nullptr);

  // Late PUBLISH_OK with LargestObject: range.start becomes largest.object + 1.
  PublishOk pubOk{
      RequestID(1),
      true, // forward
      0,    // subscriberPriority
      GroupOrder::OldestFirst,
      LocationType::LargestObject,
      std::nullopt, // start
      std::nullopt, // endGroup
      TrackRequestParameters(FrameType::PUBLISH_OK)};
  subscriber->onPublishOk(pubOk);
  EXPECT_EQ(subscriber->range.start.object, kObjectID + 1);

  // The object 5 continuation must still reach sg; without the fix the
  // forwarder skips sg on range.start and leaves it with a partial object.
  EXPECT_CALL(*sg, objectPayload(_, _)).Times(1);

  auto continuation =
      folly::IOBuf::copyBuffer(std::string(kObjectLength - kInitialLength, 'b'));
  ASSERT_TRUE(
      subgroup->objectPayload(std::move(continuation), /*finStream=*/false)
          .hasValue());

  removeSession(publisherSession);
  removeSession(subscriberSession);
}

// Test: Duplicate beginSubgroup with active consumers resets them and creates
// new ones.
// Sequence: publish, 2 subscribers, beginSubgroup, beginSubgroup again ->
// first consumers get reset, both subscribers get new consumers.
TEST_F(MoQRelayTest, DuplicateSubgroupReplacesActiveConsumers) {
  auto publisherSession = createMockSession();
  auto sub1 = createMockSession();
  auto sub2 = createMockSession();

  auto mockConsumer1 = createMockConsumer();
  auto mockConsumer2 = createMockConsumer();

  auto sg1v1 = createMockSubgroupConsumer();
  auto sg2v1 = createMockSubgroupConsumer();
  auto sg1v2 = createMockSubgroupConsumer();
  auto sg2v2 = createMockSubgroupConsumer();

  // First beginSubgroup gives v1 consumers; second call gives v2 consumers
  EXPECT_CALL(*mockConsumer1, beginSubgroup(0, 0, _, _))
      .WillOnce([&](uint64_t, uint64_t, uint8_t, bool) {
        return folly::
            makeExpected<MoQPublishError, std::shared_ptr<SubgroupConsumer>>(
                sg1v1);
      })
      .WillOnce([&](uint64_t, uint64_t, uint8_t, bool) {
        return folly::
            makeExpected<MoQPublishError, std::shared_ptr<SubgroupConsumer>>(
                sg1v2);
      });
  EXPECT_CALL(*mockConsumer2, beginSubgroup(0, 0, _, _))
      .WillOnce([&](uint64_t, uint64_t, uint8_t, bool) {
        return folly::
            makeExpected<MoQPublishError, std::shared_ptr<SubgroupConsumer>>(
                sg2v1);
      })
      .WillOnce([&](uint64_t, uint64_t, uint8_t, bool) {
        return folly::
            makeExpected<MoQPublishError, std::shared_ptr<SubgroupConsumer>>(
                sg2v2);
      });

  // v1 consumers should be reset when duplicate arrives
  EXPECT_CALL(*sg1v1, reset(ResetStreamErrorCode::CANCELLED)).Times(1);
  EXPECT_CALL(*sg2v1, reset(ResetStreamErrorCode::CANCELLED)).Times(1);
  // v2 consumers should not be reset during duplicate handling; they will be
  // closed cleanly via endOfSubgroup before teardown
  EXPECT_CALL(*sg1v2, reset(_)).Times(0);
  EXPECT_CALL(*sg2v2, reset(_)).Times(0);

  auto publishConsumer = doPublish(publisherSession, kTestTrackName);
  subscribeToTrack(sub1, kTestTrackName, mockConsumer1, RequestID(1));
  subscribeToTrack(sub2, kTestTrackName, mockConsumer2, RequestID(2));

  auto sgForwarder1 = publishConsumer->beginSubgroup(0, 0, 0);
  EXPECT_TRUE(sgForwarder1.hasValue());

  // Duplicate beginSubgroup - should reset v1 consumers and return new
  // forwarder
  auto sgForwarder2 = publishConsumer->beginSubgroup(0, 0, 0);
  EXPECT_TRUE(sgForwarder2.hasValue());
  EXPECT_NE(sgForwarder1.value(), sgForwarder2.value());

  // Close the new subgroup cleanly before teardown to avoid reset during
  // cleanup
  EXPECT_TRUE(sgForwarder2.value()->endOfSubgroup().hasValue());

  removeSession(publisherSession);
  removeSession(sub1);
  removeSession(sub2);
}

// Test: Duplicate beginSubgroup after all subscribers have stop_sending'd
// returns CANCELLED to propagate the signal back to the publisher.
TEST_F(MoQRelayTest, DuplicateSubgroupCancelledWhenNoActiveConsumers) {
  auto publisherSession = createMockSession();
  auto subscriber = createMockSession();

  auto mockConsumer = createMockConsumer();
  auto mockSg = std::make_shared<NiceMock<MockSubgroupConsumer>>();

  EXPECT_CALL(*mockConsumer, beginSubgroup(0, 0, _, _))
      .WillOnce([&](uint64_t, uint64_t, uint8_t, bool) {
        return folly::
            makeExpected<MoQPublishError, std::shared_ptr<SubgroupConsumer>>(
                mockSg);
      });

  // Subscriber's object() returns CANCELLED to simulate stop_sending
  EXPECT_CALL(*mockSg, object(_, _, _, _))
      .WillOnce(Return(
          folly::makeUnexpected(
              MoQPublishError(MoQPublishError::CANCELLED, "stop sending"))));

  auto publishConsumer = doPublish(publisherSession, kTestTrackName);
  subscribeToTrack(subscriber, kTestTrackName, mockConsumer, RequestID(1));

  auto sgRes = publishConsumer->beginSubgroup(0, 0, 0);
  ASSERT_TRUE(sgRes.hasValue());
  auto sg = sgRes.value();

  // Trigger stop_sending tombstone via CANCELLED error from object()
  sg->object(0, nullptr, {}, false);

  // Duplicate beginSubgroup - all consumers tombstoned, should return CANCELLED
  auto dupRes = publishConsumer->beginSubgroup(0, 0, 0);
  EXPECT_TRUE(dupRes.hasError());
  EXPECT_EQ(dupRes.error().code, MoQPublishError::CANCELLED);

  removeSession(publisherSession);
  removeSession(subscriber);
}

// Test: Duplicate beginSubgroup with partial stop_sending - active subscriber
// gets reset and new consumer; tombstoned subscriber is skipped.
TEST_F(MoQRelayTest, DuplicateSubgroupSkipsTombstonedSubscriber) {
  auto publisherSession = createMockSession();
  auto subA = createMockSession();
  auto subB = createMockSession();

  auto consumerA = createMockConsumer();
  auto consumerB = createMockConsumer();

  auto sgAv1 = createMockSubgroupConsumer();
  auto sgBv1 = createMockSubgroupConsumer();
  auto sgAv2 = createMockSubgroupConsumer();

  // First beginSubgroup: both A and B get consumers
  EXPECT_CALL(*consumerA, beginSubgroup(0, 0, _, _))
      .WillOnce([&](uint64_t, uint64_t, uint8_t, bool) {
        return folly::
            makeExpected<MoQPublishError, std::shared_ptr<SubgroupConsumer>>(
                sgAv1);
      })
      .WillOnce([&](uint64_t, uint64_t, uint8_t, bool) {
        return folly::
            makeExpected<MoQPublishError, std::shared_ptr<SubgroupConsumer>>(
                sgAv2);
      });
  EXPECT_CALL(*consumerB, beginSubgroup(0, 0, _, _))
      .WillOnce([&](uint64_t, uint64_t, uint8_t, bool) {
        return folly::
            makeExpected<MoQPublishError, std::shared_ptr<SubgroupConsumer>>(
                sgBv1);
      });

  // object() is forwarded to both A and B; sub A succeeds, sub B returns
  // CANCELLED to simulate stop_sending
  EXPECT_CALL(*sgAv1, object(_, _, _, _))
      .WillOnce(Return(folly::makeExpected<MoQPublishError>(folly::unit)));
  EXPECT_CALL(*sgBv1, object(_, _, _, _))
      .WillOnce(Return(
          folly::makeUnexpected(
              MoQPublishError(MoQPublishError::CANCELLED, "stop sending"))));

  // On duplicate: sub A's v1 consumer gets reset; sub B is tombstoned (no
  // reset)
  EXPECT_CALL(*sgAv1, reset(ResetStreamErrorCode::CANCELLED)).Times(1);
  EXPECT_CALL(*sgBv1, reset(_)).Times(0);
  EXPECT_CALL(*sgAv2, reset(_)).Times(0);

  auto publishConsumer = doPublish(publisherSession, kTestTrackName);
  subscribeToTrack(subA, kTestTrackName, consumerA, RequestID(1));
  subscribeToTrack(subB, kTestTrackName, consumerB, RequestID(2));

  auto sgForwarder1 = publishConsumer->beginSubgroup(0, 0, 0);
  ASSERT_TRUE(sgForwarder1.hasValue());

  // Trigger tombstone for sub B via CANCELLED from object()
  sgForwarder1.value()->object(0, nullptr, {}, false);

  // Duplicate beginSubgroup: sub A gets reset+new, sub B is skipped
  // (tombstoned)
  auto sgForwarder2 = publishConsumer->beginSubgroup(0, 0, 0);
  EXPECT_TRUE(sgForwarder2.hasValue());
  EXPECT_NE(sgForwarder1.value(), sgForwarder2.value());

  // Close the new subgroup cleanly before teardown
  EXPECT_TRUE(sgForwarder2.value()->endOfSubgroup().hasValue());

  removeSession(publisherSession);
  removeSession(subA);
  removeSession(subB);
}

// Test: forwardChanged must not crash when called after the publisher has
// terminated (onPublishDone clears handle/upstream). We trigger forwardChanged
// via Subscriber::requestUpdate changing forward from true→false (1→0
// transition). The subscriber survives drain because it has an open subgroup.
TEST_F(MoQRelayTest, ForwardChangedAfterPublisherTermination) {
  auto publisherSession = createMockSession();
  auto subSession = createMockSession();

  doPublishNamespace(publisherSession, kTestNamespace);
  auto publishConsumer = doPublish(publisherSession, kTestTrackName);

  // Subscriber with forward=true (default)
  auto consumer = createMockConsumer();
  auto handle =
      subscribeToTrack(subSession, kTestTrackName, consumer, RequestID(0));
  ASSERT_NE(handle, nullptr);

  // Begin a subgroup so the subscriber has open subgroups and survives drain
  auto sg = createMockSubgroupConsumer();
  EXPECT_CALL(*consumer, beginSubgroup(0, 0, _, _))
      .WillOnce([&sg](uint64_t, uint64_t, uint8_t, bool) {
        return folly::
            makeExpected<MoQPublishError, std::shared_ptr<SubgroupConsumer>>(
                sg);
      });
  auto subgroupRes = publishConsumer->beginSubgroup(0, 0, 0);
  ASSERT_TRUE(subgroupRes.hasValue());

  // Publisher terminates — onPublishDone clears handle/upstream.
  // forwarder->publishDone sets draining and calls drainSubscriber, but the
  // subscriber has an open subgroup so it stays (receivedPublishDone_=true).
  EXPECT_CALL(*consumer, publishDone(_))
      .WillOnce(Return(folly::makeExpected<MoQPublishError>(folly::unit)));
  publishConsumer->publishDone(
      {RequestID(0),
       PublishDoneStatusCode::SUBSCRIPTION_ENDED,
       0,
       "publisher ended"});

  // Subscriber sends requestUpdate changing forward from true→false.
  // This calls removeForwardingSubscriber → forwardingSubscribers_ 1→0 →
  // forwardChanged on relay callback. forwardChanged accesses
  // subscription.upstream which was nulled by onPublishDone → crash.
  RequestUpdate update;
  update.requestID = RequestID(0);
  update.forward = false;
  auto task = handle->requestUpdate(std::move(update));
  auto res = folly::coro::blockingWait(std::move(task), exec_.get());
  EXPECT_TRUE(res.hasValue());

  // Clean up: reset the subgroup so subscriber can be fully removed
  EXPECT_CALL(*sg, reset(_)).Times(1);
  subgroupRes.value()->reset(ResetStreamErrorCode::CANCELLED);

  removeSession(publisherSession);
  removeSession(subSession);
}

// Test: fetch fallback to subscriptions_ after publisher termination must not
// crash. When findPublishNamespaceSession returns null (no publishNamespace),
// fetch falls back to subscriptions_. After onPublishDone, upstream is null
// but the subscription entry remains if the forwarder has subscribers.
TEST_F(MoQRelayTest, FetchAfterPublisherTermination) {
  auto publisherSession = createMockSession();
  auto subSession = createMockSession();
  auto fetchSession = createMockSession();

  // Publish WITHOUT publishNamespace so findPublishNamespaceSession returns
  // null and fetch falls back to subscriptions_
  auto publishConsumer =
      doPublish(publisherSession, kTestTrackName, /*addToState=*/false);

  // Subscriber with open subgroup so subscription survives publisher drain
  auto consumer = createMockConsumer();
  auto sg = createMockSubgroupConsumer();
  EXPECT_CALL(*consumer, beginSubgroup(0, 0, _, _))
      .WillOnce([&sg](uint64_t, uint64_t, uint8_t, bool) {
        return folly::
            makeExpected<MoQPublishError, std::shared_ptr<SubgroupConsumer>>(
                sg);
      });
  auto handle =
      subscribeToTrack(subSession, kTestTrackName, consumer, RequestID(0));
  ASSERT_NE(handle, nullptr);

  // Begin subgroup to keep subscriber alive
  auto subgroupRes = publishConsumer->beginSubgroup(0, 0, 0);
  ASSERT_TRUE(subgroupRes.hasValue());

  // Publisher terminates — clears upstream but subscription stays
  EXPECT_CALL(*consumer, publishDone(_))
      .WillOnce(Return(folly::makeExpected<MoQPublishError>(folly::unit)));
  publishConsumer->publishDone(
      {RequestID(0),
       PublishDoneStatusCode::SUBSCRIPTION_ENDED,
       0,
       "publisher ended"});

  // Fetch from a different session — falls back to subscriptions_, gets null
  // upstream, then crashes at line 1011 dereferencing null upstreamSession
  Fetch fetch(
      RequestID(0),
      kTestTrackName,
      AbsoluteLocation{0, 0},
      AbsoluteLocation{1, 0});
  auto fetchConsumer = std::make_shared<NiceMock<MockFetchConsumer>>();
  withSessionContext(fetchSession, [&]() {
    auto task = relay_->fetch(std::move(fetch), fetchConsumer);
    auto res = folly::coro::blockingWait(std::move(task), exec_.get());
    // Should return an error, not crash
    EXPECT_FALSE(res.hasValue());
    EXPECT_EQ(res.error().errorCode, FetchErrorCode::TRACK_NOT_EXIST);
  });

  // Clean up
  EXPECT_CALL(*sg, reset(_)).Times(1);
  subgroupRes.value()->reset(ResetStreamErrorCode::CANCELLED);
  handle->unsubscribe();
  removeSession(publisherSession);
  removeSession(subSession);
  removeSession(fetchSession);
}

// ============================================================
// Publish Replaces Subscribe Tests
// ============================================================

// Regression test: When a PUBLISH replaces a subscribe-path subscription, the
// old forwarder's subscribers must receive publishDone, and the new
// publish-path subscription must be fully functional (accepting data from the
// new publisher).
TEST_F(MoQRelayTest, PublishReplacesSubscribeDrainsOldAndServesNew) {
  auto publisherSession = createMockSession();
  auto subscriberSession = createMockSession();

  doPublishNamespace(publisherSession, kTestNamespace);

  // Set up upstream subscribe that succeeds
  SubscribeOk upstreamOk;
  upstreamOk.requestID = RequestID(1);
  upstreamOk.trackAlias = TrackAlias(1);
  upstreamOk.expires = std::chrono::milliseconds(0);
  upstreamOk.groupOrder = GroupOrder::OldestFirst;

  EXPECT_CALL(*publisherSession, subscribe(_, _))
      .WillOnce([upstreamOk](const auto& /*req*/, auto /*consumer*/) {
        auto handle =
            std::make_shared<NiceMock<MockSubscriptionHandle>>(upstreamOk);
        return folly::coro::makeTask<Publisher::SubscribeResult>(
            folly::
                Expected<std::shared_ptr<SubscriptionHandle>, SubscribeError>(
                    handle));
      });

  // Subscribe to the track (creates subscribe-path subscription)
  auto oldConsumer = createMockConsumer();
  bool publishDoneReceived = false;
  EXPECT_CALL(*oldConsumer, publishDone(_))
      .WillOnce([&publishDoneReceived](const PublishDone&) {
        publishDoneReceived = true;
        return folly::makeExpected<MoQPublishError>(folly::unit);
      });
  auto handle = subscribeToTrack(
      subscriberSession,
      kTestTrackName,
      oldConsumer,
      RequestID(1),
      /*addToState=*/false);
  ASSERT_NE(handle, nullptr);

  // PUBLISH arrives for the same track — replaces subscribe-path subscription
  auto publishConsumer = doPublish(publisherSession, kTestTrackName);
  ASSERT_NE(publishConsumer, nullptr);

  // Old subscriber must have been drained
  EXPECT_TRUE(publishDoneReceived)
      << "Old subscribe-path subscriber should receive publishDone";

  // New publish-path subscription should be functional: subscribe a new
  // downstream consumer and verify it receives data from the publisher
  auto newConsumer = createMockConsumer();
  auto sg = createMockSubgroupConsumer();
  EXPECT_CALL(*newConsumer, beginSubgroup(0, 0, _, _))
      .WillOnce([&sg](uint64_t, uint64_t, uint8_t, bool) {
        return folly::
            makeExpected<MoQPublishError, std::shared_ptr<SubgroupConsumer>>(
                sg);
      });
  EXPECT_CALL(*sg, endOfSubgroup()).WillOnce(Return(folly::unit));

  subscribeToTrack(
      subscriberSession, kTestTrackName, newConsumer, RequestID(2));

  auto sgRes = publishConsumer->beginSubgroup(0, 0, 0);
  ASSERT_TRUE(sgRes.hasValue());
  EXPECT_TRUE(sgRes.value()->endOfSubgroup().hasValue());

  removeSession(publisherSession);
  removeSession(subscriberSession);
}

// ---------------------------------------------------------------------------
// MoQForwarder double-add tests
//
// When addSubscriber(session, subReq, consumer) is called for a session that
// already has a subscriber in the forwarder (e.g. from a prior
// addSubscriber(session, forward) call), the emplace into subscribers_ is a
// no-op. Before the fix:
//   - A new orphaned subscriber (not in the map) was returned.
//   - addForwardingSubscriber() was called unconditionally, inflating the
//   count.
// After the fix:
//   - The existing in-map subscriber is returned.
//   - addForwardingSubscriber() is guarded by whether insertion actually
//   occurred.
// ---------------------------------------------------------------------------

// Calling addSubscriber(subReq, consumer) for a session that was previously
// added via addSubscriber(forward) must return the in-map subscriber (not an
// orphan) and must not inflate the forwarding count.
TEST_F(
    MoQRelayTest,
    ForwarderDoubleAdd_ReturnsExistingSubscriberAndNoCountInflation) {
  auto session = createMockSession();
  auto forwarder = std::make_shared<MoQForwarder>(kTestTrackName, std::nullopt);

  // First add via the publishToSession path.
  auto first = forwarder->addSubscriber(session, /*forward=*/true);
  ASSERT_NE(first, nullptr);
  EXPECT_FALSE(forwarder->empty());
  EXPECT_EQ(forwarder->numForwardingSubscribers(), 1);

  // Second add via the direct-subscribe path — same session, already in map.
  SubscribeRequest subReq;
  subReq.fullTrackName = kTestTrackName;
  subReq.requestID = RequestID(1);
  subReq.forward = true;
  auto consumer = createMockConsumer();

  auto second = forwarder->addSubscriber(session, subReq, std::move(consumer));
  ASSERT_NE(second, nullptr);

  // Must return the existing in-map entry, not a new orphaned subscriber.
  EXPECT_EQ(first.get(), second.get());

  // Forwarding count must not be inflated — still 1, not 2.
  EXPECT_EQ(forwarder->numForwardingSubscribers(), 1);

  // Exactly one subscriber is in the map — one removal empties the forwarder.
  forwarder->removeSubscriber(session, std::nullopt, "test");
  EXPECT_TRUE(forwarder->empty());
  EXPECT_EQ(forwarder->numForwardingSubscribers(), 0);
}

// Calling addSubscriber(session, forward) twice for the same session must
// return the in-map subscriber and must not inflate the forwarding count.
TEST_F(
    MoQRelayTest,
    ForwarderDoubleAdd_ForwardOverload_ReturnsExistingSubscriberAndNoCountInflation) {
  auto session = createMockSession();
  auto forwarder = std::make_shared<MoQForwarder>(kTestTrackName, std::nullopt);

  auto first = forwarder->addSubscriber(session, /*forward=*/true);
  ASSERT_NE(first, nullptr);
  EXPECT_FALSE(forwarder->empty());
  EXPECT_EQ(forwarder->numForwardingSubscribers(), 1);

  auto second = forwarder->addSubscriber(session, /*forward=*/true);
  ASSERT_NE(second, nullptr);

  EXPECT_EQ(first.get(), second.get());
  EXPECT_EQ(forwarder->numForwardingSubscribers(), 1);

  forwarder->removeSubscriber(session, std::nullopt, "test");
  EXPECT_TRUE(forwarder->empty());
  EXPECT_EQ(forwarder->numForwardingSubscribers(), 0);
}

// A late-joining subscriber must receive containsLastInGroup=true when the
// original beginSubgroup had it set. Previously SubgroupForwarder always
// passed containsLastInGroup=false (defaulted) in the late-joiner path.
//
// Timeline: publish → early sub → beginSubgroup(containsLastInGroup=true) →
//   object(0) [early only] → late sub → object(1) [both; late gets
//   beginSubgroup with correct containsLastInGroup=true]
TEST_F(MoQRelayTest, ForwarderLateJoiner_ContainsLastInGroupPropagated) {
  auto publisherSession = createMockSession();
  auto earlySubscriber = createMockSession();
  auto lateSubscriber = createMockSession();

  auto earlyConsumer = createMockConsumer();
  auto lateConsumer = createMockConsumer();

  // Early subscriber gets beginSubgroup for group 0 with
  // containsLastInGroup=true
  auto earlySubgroupConsumer = createMockSubgroupConsumer();
  EXPECT_CALL(
      *earlyConsumer, beginSubgroup(0, 0, _, /*containsLastInGroup=*/true))
      .WillOnce(Return(
          folly::
              makeExpected<MoQPublishError, std::shared_ptr<SubgroupConsumer>>(
                  earlySubgroupConsumer)));

  // Late subscriber must also see containsLastInGroup=true (not the false
  // default)
  auto lateSubgroupConsumer = createMockSubgroupConsumer();
  EXPECT_CALL(
      *lateConsumer, beginSubgroup(0, 0, _, /*containsLastInGroup=*/true))
      .WillOnce(Return(
          folly::
              makeExpected<MoQPublishError, std::shared_ptr<SubgroupConsumer>>(
                  lateSubgroupConsumer)));

  auto publishConsumer = doPublish(publisherSession, kTestTrackName);
  subscribeToTrack(
      earlySubscriber, kTestTrackName, earlyConsumer, RequestID(1));

  // Publisher opens subgroup with containsLastInGroup=true
  auto sgRes =
      publishConsumer->beginSubgroup(0, 0, 0, /*containsLastInGroup=*/true);
  ASSERT_TRUE(sgRes.hasValue());
  auto sg = *sgRes;

  // Object 0 goes only to the early subscriber; advances largest_ to {0,0}
  EXPECT_CALL(*earlySubgroupConsumer, object(0, _, _, _))
      .WillOnce(Return(folly::makeExpected<MoQPublishError>(folly::unit)));
  EXPECT_TRUE(
      sg->object(0, folly::IOBuf::copyBuffer("hi"), {}, false).hasValue());

  // Late subscriber joins; range starts at {0,1} (LargestObject after {0,0})
  subscribeToTrack(lateSubscriber, kTestTrackName, lateConsumer, RequestID(2));

  // Object 1: early gets it on existing subgroup, late triggers late-joiner
  // path (beginSubgroup with containsLastInGroup from SubgroupForwarder)
  EXPECT_CALL(*earlySubgroupConsumer, object(1, _, _, _))
      .WillOnce(Return(folly::makeExpected<MoQPublishError>(folly::unit)));
  EXPECT_CALL(*lateSubgroupConsumer, object(1, _, _, _))
      .WillOnce(Return(folly::makeExpected<MoQPublishError>(folly::unit)));
  EXPECT_TRUE(
      sg->object(1, folly::IOBuf::copyBuffer("world"), {}, false).hasValue());

  EXPECT_TRUE(sg->endOfSubgroup().hasValue());
  removeSession(publisherSession);
  removeSession(earlySubscriber);
  removeSession(lateSubscriber);
}

// onPublishOk must update forwardingSubscribers_ (not just shouldForward) so
// that forwardChanged fires and the publisher receives a corrective
// REQUEST_UPDATE when the peer says fwd=false.
TEST_F(MoQRelayTest, OnPublishOkUpdatesForwardingCount) {
  auto session = createMockSession();
  auto forwarder =
      std::make_shared<MoQForwarder>(kTestTrackName, AbsoluteLocation{0, 0});

  struct ForwardCallback : MoQForwarder::Callback {
    void onEmpty(MoQForwarder*) override {}
    void forwardChanged(MoQForwarder* f) override {
      calls.push_back(f->numForwardingSubscribers());
    }
    std::vector<uint64_t> calls;
  };
  auto cb = std::make_shared<ForwardCallback>();
  forwarder->setCallback(cb);

  // Adding a forwarding subscriber fires forwardChanged (0->1).
  auto subscriber = forwarder->addSubscriber(session, /*forward=*/true);
  ASSERT_NE(subscriber, nullptr);
  EXPECT_EQ(forwarder->numForwardingSubscribers(), 1u);
  ASSERT_EQ(cb->calls.size(), 1u);
  EXPECT_EQ(cb->calls[0], 1u);
  cb->calls.clear();

  // onPublishOk(fwd=false) must decrement the count and fire forwardChanged.
  PublishOk pubOkFalse{
      RequestID(0),
      /*forward=*/false,
      0,
      GroupOrder::OldestFirst,
      LocationType::AbsoluteStart,
      AbsoluteLocation{0, 0},
      std::nullopt,
      TrackRequestParameters(FrameType::PUBLISH_OK)};
  subscriber->onPublishOk(pubOkFalse);

  EXPECT_FALSE(subscriber->shouldForward);
  EXPECT_EQ(forwarder->numForwardingSubscribers(), 0u)
      << "forwardingSubscribers_ must be decremented by onPublishOk(fwd=false)";
  ASSERT_EQ(cb->calls.size(), 1u)
      << "forwardChanged must fire when count drops to 0";
  EXPECT_EQ(cb->calls[0], 0u);
  cb->calls.clear();

  // onPublishOk(fwd=true) must increment and fire forwardChanged again.
  PublishOk pubOkTrue{
      RequestID(0),
      /*forward=*/true,
      0,
      GroupOrder::OldestFirst,
      LocationType::AbsoluteStart,
      AbsoluteLocation{0, 0},
      std::nullopt,
      TrackRequestParameters(FrameType::PUBLISH_OK)};
  subscriber->onPublishOk(pubOkTrue);

  EXPECT_TRUE(subscriber->shouldForward);
  EXPECT_EQ(forwarder->numForwardingSubscribers(), 1u)
      << "forwardingSubscribers_ must be incremented by onPublishOk(fwd=true)";
  ASSERT_EQ(cb->calls.size(), 1u)
      << "forwardChanged must fire when count rises to 1";
  EXPECT_EQ(cb->calls[0], 1u);
}

// When a subscriber with forward=true joins a namespace whose track forwarder
// is empty, the relay should fire exactly one REQUEST_UPDATE(forward=true) —
// from forwardChanged(), not a redundant explicit doSubscribeUpdate.
TEST_F(MoQRelayTest, SubscribeNsForwardTrueEmptyForwarderSingleRequestUpdate) {
  auto pubSession = createMockSession();
  doPublishNamespace(pubSession, kTestNamespace);
  auto mockHandle = makePublishHandle();
  doPublishWithHandle(pubSession, kTestTrackName, mockHandle);

  EXPECT_CALL(*mockHandle, requestUpdateCalled(_))
      .Times(1)
      .WillOnce([](const RequestUpdate& u) {
        ASSERT_TRUE(u.forward.has_value());
        EXPECT_TRUE(*u.forward);
      });

  auto subSession = createMockSession();
  setupPublishSucceeds(subSession);
  doSubscribeNamespaceWithForward(subSession, kTestNamespace, /*forward=*/true);

  for (int i = 0; i < 5; i++) {
    exec_->drive();
  }

  ASSERT_TRUE(testing::Mock::VerifyAndClearExpectations(mockHandle.get()));

  removeSession(subSession);
  removeSession(pubSession);
  for (int i = 0; i < 3; i++) {
    exec_->drive();
  }
}

// When a subscriber with forward=false joins a namespace whose track forwarder
// is empty, the relay should not send any REQUEST_UPDATE — the upstream is
// already at forward=false (set by publish() which found no subscribers).
TEST_F(MoQRelayTest, SubscribeNsForwardFalseEmptyForwarderNoRequestUpdate) {
  auto pubSession = createMockSession();
  doPublishNamespace(pubSession, kTestNamespace);
  auto mockHandle = makePublishHandle();
  doPublishWithHandle(pubSession, kTestTrackName, mockHandle);

  EXPECT_CALL(*mockHandle, requestUpdateCalled(_)).Times(0);

  auto subSession = createMockSession();
  setupPublishSucceeds(subSession);
  doSubscribeNamespaceWithForward(
      subSession, kTestNamespace, /*forward=*/false);

  for (int i = 0; i < 5; i++) {
    exec_->drive();
  }

  ASSERT_TRUE(testing::Mock::VerifyAndClearExpectations(mockHandle.get()));

  removeSession(subSession);
  removeSession(pubSession);
  for (int i = 0; i < 3; i++) {
    exec_->drive();
  }
}

// When a second subscriber with forward=true joins an existing subscription
// (causing a 0->1 forwarding transition), the relay should fire exactly one
// REQUEST_UPDATE — from forwardChanged(), not a redundant explicit block.
TEST_F(MoQRelayTest, SubscribeSecondForwardingSubscriberSingleRequestUpdate) {
  auto pubSession = createMockSession();
  doPublishNamespace(pubSession, kTestNamespace);
  auto mockHandle = makePublishHandle();
  doPublishWithHandle(pubSession, kTestTrackName, mockHandle);

  // S1 joins with forward=false — no REQUEST_UPDATE expected.
  auto s1 = createMockSession();
  setupPublishSucceeds(s1);
  {
    SubscribeRequest sub;
    sub.fullTrackName = kTestTrackName;
    sub.requestID = RequestID(1);
    sub.locType = LocationType::LargestObject;
    sub.forward = false;
    withSessionContext(s1, [&]() {
      auto res = folly::coro::blockingWait(
          relay_->subscribe(std::move(sub), createMockConsumer()), exec_.get());
      EXPECT_TRUE(res.hasValue());
      if (res.hasValue()) {
        getOrCreateMockState(s1)->subscribeHandles.push_back(*res);
      }
    });
  }
  for (int i = 0; i < 3; i++) {
    exec_->drive();
  }

  // Expect exactly one REQUEST_UPDATE(forward=true) when S2 joins.
  EXPECT_CALL(*mockHandle, requestUpdateCalled(_))
      .Times(1)
      .WillOnce([](const RequestUpdate& u) {
        ASSERT_TRUE(u.forward.has_value());
        EXPECT_TRUE(*u.forward);
      });

  auto s2 = createMockSession();
  setupPublishSucceeds(s2);
  {
    SubscribeRequest sub;
    sub.fullTrackName = kTestTrackName;
    sub.requestID = RequestID(2);
    sub.locType = LocationType::LargestObject;
    sub.forward = true;
    withSessionContext(s2, [&]() {
      auto res = folly::coro::blockingWait(
          relay_->subscribe(std::move(sub), createMockConsumer()), exec_.get());
      EXPECT_TRUE(res.hasValue());
      if (res.hasValue()) {
        getOrCreateMockState(s2)->subscribeHandles.push_back(*res);
      }
    });
  }
  for (int i = 0; i < 5; i++) {
    exec_->drive();
  }

  ASSERT_TRUE(testing::Mock::VerifyAndClearExpectations(mockHandle.get()));

  removeSession(s2);
  removeSession(s1);
  removeSession(pubSession);
  for (int i = 0; i < 3; i++) {
    exec_->drive();
  }
}

} // namespace moxygen::test

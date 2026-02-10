/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <folly/coro/BlockingWait.h>
#include <folly/io/async/EventBase.h>
#include <folly/portability/GMock.h>
#include <folly/portability/GTest.h>
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

// Test: Verify that subgroups are only created at appropriate times
// Sequence: publish, sub1 joins, beginSubgroup (->1), beginObject (->1),
// sub2 joins, beginObject (->1,2), sub3 joins, objectPayload (->1,2),
// endOfSubgroup (->1,2)
TEST_F(MoQRelayTest, ForwarderOnlyCreatesSubgroupsBeforeObjectData) {
  auto publisherSession = createMockSession();
  auto subscriber1 = createMockSession();
  auto subscriber2 = createMockSession();
  auto subscriber3 = createMockSession();

  // Set up mock consumers with expectations
  auto mockConsumer1 = createMockConsumer();
  auto mockConsumer2 = createMockConsumer();
  auto mockConsumer3 = createMockConsumer();

  auto setupSubgroupConsumer = [](auto& sg) {
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
  };

  // Subscriber 1 and 2 should get beginSubgroup
  EXPECT_CALL(*mockConsumer1, beginSubgroup(_, _, _, _))
      .WillOnce([&](uint64_t, uint64_t, uint8_t, bool) {
        auto sg = std::make_shared<NiceMock<MockSubgroupConsumer>>();
        setupSubgroupConsumer(sg);
        return folly::
            makeExpected<MoQPublishError, std::shared_ptr<SubgroupConsumer>>(
                sg);
      });

  EXPECT_CALL(*mockConsumer2, beginSubgroup(_, _, _, _))
      .WillOnce([&](uint64_t, uint64_t, uint8_t, bool) {
        auto sg = std::make_shared<NiceMock<MockSubgroupConsumer>>();
        setupSubgroupConsumer(sg);
        return folly::
            makeExpected<MoQPublishError, std::shared_ptr<SubgroupConsumer>>(
                sg);
      });

  // Subscriber 3 joins mid-object and should NOT get beginSubgroup
  EXPECT_CALL(*mockConsumer3, beginSubgroup(_, _, _, _)).Times(0);

  // Setup: publish track
  auto publishConsumer = doPublish(publisherSession, kTestTrackName);

  // Subscriber 1 joins before any data
  subscribeToTrack(subscriber1, kTestTrackName, mockConsumer1, RequestID(1));

  // Publisher sends data with subscribers joining at different points
  auto sgRes = publishConsumer->beginSubgroup(0, 0, 0);
  EXPECT_TRUE(sgRes.hasValue());
  auto subgroup = *sgRes;

  // First object -> goes to subscriber 1
  EXPECT_TRUE(subgroup->beginObject(0, 4, 0).hasValue());
  EXPECT_TRUE(subgroup->objectPayload(folly::IOBuf::copyBuffer("test"), false)
                  .hasValue());

  // Subscriber 2 joins after first object
  subscribeToTrack(subscriber2, kTestTrackName, mockConsumer2, RequestID(2));

  // Second object -> goes to subscribers 1, 2
  EXPECT_TRUE(subgroup->beginObject(1, 9, 0).hasValue());

  // Subscriber 3 joins mid-object
  subscribeToTrack(subscriber3, kTestTrackName, mockConsumer3, RequestID(3));

  // Payload and endOfSubgroup -> only to subscribers 1, 2 (NOT 3)
  EXPECT_TRUE(
      subgroup->objectPayload(folly::IOBuf::copyBuffer("more data"), false)
          .hasValue());
  EXPECT_TRUE(subgroup->endOfSubgroup().hasValue());

  removeSession(publisherSession);
  removeSession(subscriber1);
  removeSession(subscriber2);
  removeSession(subscriber3);
}

// Test: Graceful session draining - comprehensive test of draining behavior
// This test verifies that when a publisher calls publishDone, subscribers
// are drained (receive publishDone) but their open subgroups are NOT reset.
// Draining subscribers should not receive new subgroups and are removed when
// their last subgroup closes.
TEST_F(MoQRelayTest, GracefulSessionDraining) {
  auto publisherSession = createMockSession();
  std::array<std::shared_ptr<MoQSession>, 3> subscribers;
  for (auto& s : subscribers) {
    s = createMockSession();
  }

  // Setup: Publish track
  auto publishConsumer = doPublish(publisherSession, kTestTrackName);

  // Create subscribers with different subgroup states
  std::array<std::shared_ptr<MockSubgroupConsumer>, 2> sub0_sgs;
  std::shared_ptr<MockSubgroupConsumer> sub1_sg0;

  std::array<std::shared_ptr<MockTrackConsumer>, 3> consumers;
  for (auto& c : consumers) {
    c = createMockConsumer();
  }

  // Subscriber1: will have 2 open subgroups (0, 1)
  for (uint64_t i = 0; i < 2; ++i) {
    EXPECT_CALL(*consumers[0], beginSubgroup(i, 0, _, _))
        .WillOnce([this, i, &sub0_sgs](uint64_t, uint64_t, uint8_t, bool) {
          sub0_sgs[i] = createMockSubgroupConsumer();
          return folly::
              makeExpected<MoQPublishError, std::shared_ptr<SubgroupConsumer>>(
                  sub0_sgs[i]);
        });
  }

  // Subscriber2: will have 1 open subgroup (1)
  EXPECT_CALL(*consumers[1], beginSubgroup(1, 0, _, _))
      .WillOnce([this, &sub1_sg0](uint64_t, uint64_t, uint8_t, bool) {
        sub1_sg0 = createMockSubgroupConsumer();
        return folly::
            makeExpected<MoQPublishError, std::shared_ptr<SubgroupConsumer>>(
                sub1_sg0);
      });

  std::array<std::shared_ptr<SubgroupConsumer>, 2> sgs;
  subscribeToTrack(subscribers[0], kTestTrackName, consumers[0], RequestID(1));
  sgs[0] = publishConsumer->beginSubgroup(0, 0, 0).value();
  subscribeToTrack(subscribers[1], kTestTrackName, consumers[1], RequestID(2));
  sgs[1] = publishConsumer->beginSubgroup(1, 0, 0).value();
  subscribeToTrack(subscribers[2], kTestTrackName, consumers[2], RequestID(3));

  // Trigger draining by calling publishDone from publisher
  // All subscribers should receive publishDone but subgroups should NOT reset
  // Note: publishDone may be called multiple times (during drain and cleanup)
  for (auto& consumer : consumers) {
    EXPECT_CALL(*consumer, publishDone(_))
        .WillOnce(Return(folly::makeExpected<MoQPublishError>(folly::unit)));
  }

  // Verify subgroups are NOT reset (no reset() calls expected)
  EXPECT_CALL(*sub0_sgs[0], reset(_)).Times(0);
  EXPECT_CALL(*sub0_sgs[1], reset(_)).Times(0);
  EXPECT_CALL(*sub1_sg0, reset(_)).Times(0);

  publishConsumer->publishDone(
      PublishDone{
          RequestID(0),
          PublishDoneStatusCode::SUBSCRIPTION_ENDED,
          0,
          "publisher ended"});

  // At this point:
  // - subscriber3 removed (had no subgroups)
  // - subscriber1 still exists (has 2 open subgroups)
  // - subscriber2 still exists (has 1 open subgroup)

  // Close subgroup 0 -> subscriber2 removed (was its last subgroup)
  // subscriber1 still has subgroup 1
  EXPECT_TRUE(sgs[0]->endOfSubgroup().hasValue());

  // Close subgroup 1 -> subscriber1 removed (was its last subgroup)
  EXPECT_TRUE(sgs[1]->endOfSubgroup().hasValue());

  removeSession(publisherSession);
}

// Test: removeSession (via unsubscribe) immediately resets all open subgroups
TEST_F(MoQRelayTest, RemoveSessionResetsOpenSubgroups) {
  auto publisherSession = createMockSession();
  auto subscriber = createMockSession();

  // Setup: Publish track
  auto publishConsumer = doPublish(publisherSession, kTestTrackName);

  // Create subscriber with 2 open subgroups
  std::array<std::shared_ptr<MockSubgroupConsumer>, 2> sgs;
  auto consumer = createMockConsumer();

  for (uint64_t i = 0; i < sgs.size(); ++i) {
    EXPECT_CALL(*consumer, beginSubgroup(i, 0, _, _))
        .WillOnce([this, i, &sgs](uint64_t, uint64_t, uint8_t, bool) {
          sgs[i] = createMockSubgroupConsumer();
          EXPECT_CALL(*sgs[i], object(0, testing::_, testing::_, false))
              .WillOnce(testing::Return(folly::unit));
          return folly::
              makeExpected<MoQPublishError, std::shared_ptr<SubgroupConsumer>>(
                  sgs[i]);
        });
  }

  auto subHandle = subscribeToTrack(
      subscriber, kTestTrackName, consumer, RequestID(1), /*addToState=*/false);

  // Publish data to create subgroups and first objects
  std::array<std::shared_ptr<SubgroupConsumer>, 2> pubSgs;
  for (uint64_t i = 0; i < pubSgs.size(); ++i) {
    auto sg = publishConsumer->beginSubgroup(i, 0, 0);
    ASSERT_TRUE(sg.hasValue());
    EXPECT_TRUE((*sg)->object(0, test::makeBuf(10)).hasValue());
    pubSgs[i] = *sg;
  }

  // Expect both subgroups to be reset when subscriber unsubscribes
  for (auto& sg : sgs) {
    EXPECT_CALL(*sg, reset(ResetStreamErrorCode::CANCELLED));
  }

  // Unsubscribe triggers removeSession which resets all subgroups
  withSessionContext(subscriber, [&]() { subHandle->unsubscribe(); });

  for (uint64_t i = 0; i < pubSgs.size(); ++i) {
    // Ensure each published subgroup is properly closed after unsubscribe
    EXPECT_TRUE(pubSgs[i]->endOfSubgroup().hasValue());
  }
  removeSession(subscriber);
  removeSession(publisherSession);
}

// Test: Draining subscriber removed when subgroup closes with error
TEST_F(MoQRelayTest, DrainingSubscriberRemovedOnSubgroupError) {
  auto publisherSession = createMockSession();
  auto subscriber = createMockSession();

  // Setup: Publish track
  auto publishConsumer = doPublish(publisherSession, kTestTrackName);

  // Create subscriber with 3 open subgroups (0, 1, 2)
  std::array<std::shared_ptr<MockSubgroupConsumer>, 3> sgs;
  auto consumer = createMockConsumer();

  for (uint64_t i = 0; i < sgs.size(); ++i) {
    EXPECT_CALL(*consumer, beginSubgroup(i, 0, _, _))
        .WillOnce([this, i, &sgs](uint64_t, uint64_t, uint8_t, bool) {
          sgs[i] = createMockSubgroupConsumer();
          return folly::
              makeExpected<MoQPublishError, std::shared_ptr<SubgroupConsumer>>(
                  sgs[i]);
        });
  }

  subscribeToTrack(subscriber, kTestTrackName, consumer, RequestID(1));

  // Publish data to create all 3 subgroups
  std::array<std::shared_ptr<SubgroupConsumer>, 3> pubSgs;
  for (uint64_t i = 0; i < 3; ++i) {
    pubSgs[i] = publishConsumer->beginSubgroup(i, 0, 0).value();
    EXPECT_TRUE((pubSgs[i])->beginObject(0, 10, 0).hasValue());
  }

  // Drain the subscriber - should mark it draining but not remove
  // Note: publishDone may be called multiple times (during drain and cleanup)
  EXPECT_CALL(*consumer, publishDone(_))
      .Times(AtLeast(1))
      .WillRepeatedly(
          Return(folly::makeExpected<MoQPublishError>(folly::unit)));

  // Subgroups should NOT be reset during drain
  EXPECT_CALL(*sgs[0], reset(_)).Times(0);
  EXPECT_CALL(*sgs[1], reset(_)).Times(0);
  EXPECT_CALL(*sgs[2], reset(_)).Times(0);

  publishConsumer->publishDone(
      PublishDone{
          RequestID(0),
          PublishDoneStatusCode::SUBSCRIPTION_ENDED,
          0,
          "publisher ended"});

  // Now close subgroups one by one
  // Test 1: Send error on subgroup 0 -> subscriber NOT removed (still has 1, 2)
  EXPECT_CALL(*sgs[0], reset(ResetStreamErrorCode::INTERNAL_ERROR));
  pubSgs[0]->reset(ResetStreamErrorCode::INTERNAL_ERROR);

  // Test 2: Close subgroup 1 normally -> subscriber NOT removed (still has 2)
  EXPECT_CALL(*sgs[1], objectPayload(testing::_, true));
  EXPECT_TRUE(pubSgs[1]->objectPayload(test::makeBuf(10), true));

  // Test 3: Send error on subgroup 2 -> subscriber IS removed (was last)
  EXPECT_CALL(*sgs[2], reset(ResetStreamErrorCode::INTERNAL_ERROR));
  pubSgs[2]->reset(ResetStreamErrorCode::INTERNAL_ERROR);

  removeSession(publisherSession);
}

// Test: Subscriber that ends subscription doesn't receive subsequent objects
// Sequence: publish, sub1 subscribes, beginSubgroup, sub1 ends (publishDone),
// sub2 subscribes, send object -> only goes to sub1
TEST_F(MoQRelayTest, SubscriberUnsubscribeDoesNotReceiveNewObjects) {
  auto publisherSession = createMockSession();
  auto subscriber1 = createMockSession();
  auto subscriber2 = createMockSession();

  // Set up mock consumers
  auto mockConsumer1 = createMockConsumer();
  auto mockConsumer2 = createMockConsumer();

  // Sub1 should get beginSubgroup
  EXPECT_CALL(*mockConsumer1, beginSubgroup(_, _, _, _))
      .WillOnce([&](uint64_t, uint64_t, uint8_t, bool) {
        auto sg = std::make_shared<NiceMock<MockSubgroupConsumer>>();
        EXPECT_CALL(*sg, beginObject(_, _, _, _)).WillOnce(Return(folly::unit));
        EXPECT_CALL(*sg, reset(_));
        return folly::
            makeExpected<MoQPublishError, std::shared_ptr<SubgroupConsumer>>(
                sg);
      });

  // Sub2 should also get beginSubgroup
  EXPECT_CALL(*mockConsumer2, beginSubgroup(_, _, _, _)).Times(0);

  // Publish track
  auto publishConsumer =
      doPublish(publisherSession, kTestTrackName, /*addToState=*/false);

  // Subscriber 1 joins
  subscribeToTrack(subscriber1, kTestTrackName, mockConsumer1, RequestID(1));

  // Begin subgroup - sub1 receives it
  auto sgRes = publishConsumer->beginSubgroup(0, 0, 0);
  EXPECT_TRUE(sgRes.hasValue());
  auto subgroup = *sgRes;

  // publisher ends subscription
  EXPECT_CALL(*mockConsumer1, publishDone(testing::_));
  EXPECT_TRUE(publishConsumer
                  ->publishDone(
                      {RequestID(1),
                       PublishDoneStatusCode::TRACK_ENDED,
                       0,
                       "track ended"})
                  .hasValue());

  // Subscriber 2 joins after publishDone
  subscribeToTrack(
      subscriber2,
      kTestTrackName,
      mockConsumer2,
      RequestID(2),
      /*addToState=*/false,
      SubscribeErrorCode::INTERNAL_ERROR);

  // Send object - should only go to subscriber 1 (sub2 is gone)
  EXPECT_TRUE(subgroup->beginObject(0, 4, 0).hasValue());
  subgroup->reset(ResetStreamErrorCode::SESSION_CLOSED);

  removeSession(publisherSession);
  removeSession(subscriber1);
  removeSession(subscriber2);
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
      .WillOnce([mockConsumer1](auto pubReq, auto subHandle) {
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
      .WillOnce([](auto /*pubReq*/, auto /*subHandle*/) {
        return folly::makeUnexpected(PublishError{});
      });

  EXPECT_CALL(*subscriber2, publish(testing::_, testing::_))
      .WillOnce([](auto /*pubReq*/, auto /*subHandle*/) {
        return folly::makeUnexpected(PublishError{});
      });

  auto pubConsumer2 = doPublish(
      publisherSession, FullTrackName{kTestNamespace, "track_stream_2"});
  exec_->drive();

  removeSession(publisherSession);
  removeSession(subscriber1);
  removeSession(subscriber2);
}

// Test: Data operations return CANCELLED when all subscribers fail and are
// removed. This verifies that when a data operation (objectPayload with
// fin=false) causes all subscribers to error out, the operation correctly
// detects that data went nowhere and returns CANCELLED.
TEST_F(MoQRelayTest, DataOperationCancelledWhenAllSubscribersFail) {
  auto publisherSession = createMockSession();
  std::array<std::shared_ptr<MoQSession>, 2> subscribers;
  std::array<std::shared_ptr<MockTrackConsumer>, 2> consumers;
  std::array<std::shared_ptr<MockSubgroupConsumer>, 2> sgs;

  for (auto& s : subscribers) {
    s = createMockSession();
  }
  for (auto& c : consumers) {
    c = createMockConsumer();
  }

  // Setup both subscribers to succeed on first payload, fail on second
  for (size_t i = 0; i < 2; ++i) {
    EXPECT_CALL(*consumers[i], beginSubgroup(0, 0, _, _))
        .WillOnce([this, i, &sgs](uint64_t, uint64_t, uint8_t, bool) {
          sgs[i] = createMockSubgroupConsumer();
          EXPECT_CALL(*sgs[i], objectPayload(_, false))
              .WillOnce(Return(
                  folly::makeExpected<MoQPublishError>(
                      ObjectPublishStatus::IN_PROGRESS)))
              .WillOnce(Return(
                  folly::makeUnexpected(MoQPublishError(
                      MoQPublishError::WRITE_ERROR, "write failed"))));
          return folly::
              makeExpected<MoQPublishError, std::shared_ptr<SubgroupConsumer>>(
                  sgs[i]);
        });
  }

  // Publish track and add subscribers
  auto publishConsumer = doPublish(publisherSession, kTestTrackName);
  for (size_t i = 0; i < 2; ++i) {
    subscribeToTrack(
        subscribers[i], kTestTrackName, consumers[i], RequestID(i + 1));
  }

  // Begin subgroup and object
  auto sgRes = publishConsumer->beginSubgroup(0, 0, 0);
  ASSERT_TRUE(sgRes.hasValue());
  auto subgroup = *sgRes;
  EXPECT_TRUE(subgroup->beginObject(0, 100, 0).hasValue());

  // First objectPayload succeeds - both subscribers receive it
  auto res1 = subgroup->objectPayload(folly::IOBuf::copyBuffer("data1"), false);
  EXPECT_TRUE(res1.hasValue());
  EXPECT_EQ(*res1, ObjectPublishStatus::IN_PROGRESS);

  // Second objectPayload causes both subscribers to fail and get removed
  // This should return CANCELLED because all subscribers are gone
  auto res2 = subgroup->objectPayload(folly::IOBuf::copyBuffer("data2"), false);
  EXPECT_FALSE(res2.hasValue());
  EXPECT_EQ(res2.error().code, MoQPublishError::CANCELLED);

  // Clean up
  subgroup->reset(ResetStreamErrorCode::INTERNAL_ERROR);
  removeSession(publisherSession);
  for (auto& s : subscribers) {
    removeSession(s);
  }
}

// Test: Partial subscriber failure does not cancel data operations.
// This verifies that when some subscribers fail but others succeed, the
// operation continues successfully and only returns CANCELLED when ALL
// subscribers are gone.
TEST_F(MoQRelayTest, PartialSubscriberFailureDoesNotCancelData) {
  auto publisherSession = createMockSession();
  std::array<std::shared_ptr<MoQSession>, 3> subscribers;
  std::array<std::shared_ptr<MockTrackConsumer>, 3> consumers;
  std::array<std::shared_ptr<MockSubgroupConsumer>, 3> sgs;

  for (auto& s : subscribers) {
    s = createMockSession();
  }
  for (auto& c : consumers) {
    c = createMockConsumer();
  }

  // Setup subscriber 0 - will fail on second objectPayload
  EXPECT_CALL(*consumers[0], beginSubgroup(0, 0, _, _))
      .WillOnce([this, &sgs](uint64_t, uint64_t, uint8_t, bool) {
        sgs[0] = createMockSubgroupConsumer();
        EXPECT_CALL(*sgs[0], objectPayload(_, false))
            .WillOnce(Return(
                folly::makeExpected<MoQPublishError>(
                    ObjectPublishStatus::IN_PROGRESS)))
            .WillOnce(Return(
                folly::makeUnexpected(MoQPublishError(
                    MoQPublishError::WRITE_ERROR, "write failed"))));
        // Expect reset when subscriber is removed
        EXPECT_CALL(*sgs[0], reset(_)).Times(1);
        return folly::
            makeExpected<MoQPublishError, std::shared_ptr<SubgroupConsumer>>(
                sgs[0]);
      });

  // Setup subscribers 1 and 2 - will succeed on objectPayload
  // They will get reset during cleanup when the test calls subgroup->reset()
  for (size_t i = 1; i < 3; ++i) {
    EXPECT_CALL(*consumers[i], beginSubgroup(0, 0, _, _))
        .WillOnce([this, i, &sgs](uint64_t, uint64_t, uint8_t, bool) {
          sgs[i] = createMockSubgroupConsumer();
          EXPECT_CALL(*sgs[i], objectPayload(_, false))
              .WillRepeatedly(Return(
                  folly::makeExpected<MoQPublishError>(
                      ObjectPublishStatus::IN_PROGRESS)));
          // Will be reset by cleanup code at end of test
          EXPECT_CALL(*sgs[i], reset(ResetStreamErrorCode::SESSION_CLOSED))
              .Times(1);
          return folly::
              makeExpected<MoQPublishError, std::shared_ptr<SubgroupConsumer>>(
                  sgs[i]);
        });
  }

  // Publish track and add subscribers
  auto publishConsumer = doPublish(publisherSession, kTestTrackName);
  for (size_t i = 0; i < 3; ++i) {
    subscribeToTrack(
        subscribers[i], kTestTrackName, consumers[i], RequestID(i + 1));
  }

  // Begin subgroup and object
  auto sgRes = publishConsumer->beginSubgroup(0, 0, 0);
  ASSERT_TRUE(sgRes.hasValue());
  auto subgroup = *sgRes;
  EXPECT_TRUE(subgroup->beginObject(0, 100, 0).hasValue());

  // First objectPayload succeeds - all 3 subscribers receive it
  auto res1 = subgroup->objectPayload(folly::IOBuf::copyBuffer("data1"), false);
  EXPECT_TRUE(res1.hasValue());
  EXPECT_EQ(*res1, ObjectPublishStatus::IN_PROGRESS);

  // Second objectPayload causes subscriber 0 to fail and get removed
  // But subscribers 1 and 2 still exist, so operation succeeds
  auto res2 = subgroup->objectPayload(folly::IOBuf::copyBuffer("data2"), false);
  EXPECT_TRUE(res2.hasValue());
  EXPECT_EQ(*res2, ObjectPublishStatus::IN_PROGRESS);

  // Clean up - reset the subgroup which resets remaining subscribers 1 and 2
  subgroup->reset(ResetStreamErrorCode::SESSION_CLOSED);
  removeSession(publisherSession);
  for (auto& s : subscribers) {
    removeSession(s);
  }
}

// Test: SubscribeUpdate can decrease start location
// Per spec, start location can be decreased. The subscriber should use FETCH
// to retrieve objects between the new start and the current largest location.
TEST_F(MoQRelayTest, SubscribeUpdateStartLocationCanDecrease) {
  auto publisherSession = createMockSession();
  auto subscriberSession = createMockSession();

  // Setup: Publish track
  auto publishConsumer = doPublish(publisherSession, kTestTrackName);

  // Subscribe with initial start location
  auto consumer = createMockConsumer();
  SubscribeRequest sub;
  sub.fullTrackName = kTestTrackName;
  sub.requestID = RequestID(1);
  sub.locType = LocationType::AbsoluteStart;
  sub.start = AbsoluteLocation{10, 0};
  sub.endGroup = 0; // Open-ended

  std::shared_ptr<SubscriptionHandle> handle{nullptr};
  withSessionContext(subscriberSession, [&]() {
    auto task = relay_->subscribe(std::move(sub), consumer);
    auto res = folly::coro::blockingWait(std::move(task), exec_.get());
    ASSERT_TRUE(res.hasValue());
    handle = *res;
  });

  // Cast to MoQForwarder::Subscriber to access range
  auto* subscriber = dynamic_cast<MoQForwarder::Subscriber*>(handle.get());
  ASSERT_NE(subscriber, nullptr);

  // Verify initial start location
  EXPECT_EQ(subscriber->range.start, (AbsoluteLocation{10, 0}));

  // Send SubscribeUpdate with decreased start location
  SubscribeUpdate subscribeUpdate{
      RequestID(2),           // requestID for the update
      sub.requestID,          // subscriptionRequestID
      AbsoluteLocation{5, 0}, // Start decreased from {10, 0} to {5, 0}
      0,                      // endGroup (open-ended)
      kDefaultPriority,
      true}; // forward

  auto updateRes =
      folly::coro::blockingWait(subscriber->requestUpdate(subscribeUpdate));
  EXPECT_TRUE(updateRes.hasValue())
      << "RequestUpdate with decreased start should succeed";

  // Verify start location was updated
  EXPECT_EQ(subscriber->range.start, (AbsoluteLocation{5, 0}))
      << "Start location should be updated to {5, 0}";

  removeSession(publisherSession);
  removeSession(subscriberSession);
}

// Test: Subgroup is tombstoned after CANCELLED error (soft error)
// Verifies that a CANCELLED error on a subgroup sets it to nullptr (tombstone)
// but keeps the subscription alive
TEST_F(MoQRelayTest, SubgroupTombstonedAfterCancelledError) {
  auto publisherSession = createMockSession();
  auto subscriber = createMockSession();

  // Setup: Publish track
  auto publishConsumer = doPublish(publisherSession, kTestTrackName);

  // Create subscriber with a mock consumer that will fail with CANCELLED
  auto consumer = createMockConsumer();
  std::shared_ptr<MockSubgroupConsumer> sg;

  EXPECT_CALL(*consumer, beginSubgroup(0, 0, _, _))
      .WillOnce([this, &sg](uint64_t, uint64_t, uint8_t, bool) {
        sg = createMockSubgroupConsumer();
        // First object succeeds, second fails with CANCELLED
        EXPECT_CALL(*sg, object(0, _, _, false)).WillOnce(Return(folly::unit));
        EXPECT_CALL(*sg, object(1, _, _, false))
            .WillOnce(Return(
                folly::makeUnexpected(MoQPublishError(
                    MoQPublishError::CANCELLED, "STOP_SENDING"))));
        // Per API contract, error implies implicit reset - no reset() call
        return folly::
            makeExpected<MoQPublishError, std::shared_ptr<SubgroupConsumer>>(
                sg);
      });

  // Second subgroup should still be created (subscription is alive)
  std::shared_ptr<MockSubgroupConsumer> sg2;
  EXPECT_CALL(*consumer, beginSubgroup(1, 0, _, _))
      .WillOnce([this, &sg2](uint64_t, uint64_t, uint8_t, bool) {
        sg2 = createMockSubgroupConsumer();
        EXPECT_CALL(*sg2, endOfSubgroup()).WillOnce(Return(folly::unit));
        return folly::
            makeExpected<MoQPublishError, std::shared_ptr<SubgroupConsumer>>(
                sg2);
      });

  subscribeToTrack(subscriber, kTestTrackName, consumer, RequestID(1));

  // Begin first subgroup and send objects
  auto subgroup1Res = publishConsumer->beginSubgroup(0, 0, 0);
  ASSERT_TRUE(subgroup1Res.hasValue());
  auto subgroup1 = *subgroup1Res;

  // First object succeeds
  EXPECT_TRUE(subgroup1->object(0, test::makeBuf(10)).hasValue());

  // Second object triggers CANCELLED error - subgroup gets tombstoned
  EXPECT_TRUE(subgroup1->object(1, test::makeBuf(10)).hasValue());

  // Begin second subgroup - should succeed because subscription is still alive
  auto subgroup2Res = publishConsumer->beginSubgroup(1, 0, 0);
  ASSERT_TRUE(subgroup2Res.hasValue());
  auto subgroup2 = *subgroup2Res;

  // End second subgroup
  EXPECT_TRUE(subgroup2->endOfSubgroup().hasValue());

  // End first subgroup (after tombstoning)
  EXPECT_TRUE(subgroup1->endOfSubgroup().hasValue());

  removeSession(publisherSession);
  removeSession(subscriber);
}

// Test: Objects published to tombstoned subgroup are skipped for that
// subscriber
TEST_F(MoQRelayTest, TombstonedSubgroupIgnoresSubsequentObjects) {
  auto publisherSession = createMockSession();
  auto subscriber = createMockSession();

  // Setup: Publish track
  auto publishConsumer = doPublish(publisherSession, kTestTrackName);

  // Create subscriber that fails with CANCELLED on first object
  auto consumer = createMockConsumer();
  std::shared_ptr<MockSubgroupConsumer> sg;

  EXPECT_CALL(*consumer, beginSubgroup(0, 0, _, _))
      .WillOnce([this, &sg](uint64_t, uint64_t, uint8_t, bool) {
        sg = createMockSubgroupConsumer();
        // First object fails with CANCELLED
        EXPECT_CALL(*sg, object(0, _, _, false))
            .WillOnce(Return(
                folly::makeUnexpected(MoQPublishError(
                    MoQPublishError::CANCELLED, "delivery timeout"))));
        // Per API contract, error implies implicit reset - no reset() call
        // After tombstoning, should NOT receive any more objects
        EXPECT_CALL(*sg, object(1, _, _, _)).Times(0);
        EXPECT_CALL(*sg, object(2, _, _, _)).Times(0);
        return folly::
            makeExpected<MoQPublishError, std::shared_ptr<SubgroupConsumer>>(
                sg);
      });

  subscribeToTrack(subscriber, kTestTrackName, consumer, RequestID(1));

  // Begin subgroup
  auto subgroupRes = publishConsumer->beginSubgroup(0, 0, 0);
  ASSERT_TRUE(subgroupRes.hasValue());
  auto subgroup = *subgroupRes;

  // First object triggers CANCELLED - subgroup gets tombstoned
  EXPECT_TRUE(subgroup->object(0, test::makeBuf(10)).hasValue());

  // Subsequent objects should be skipped for this subscriber (tombstoned)
  // These return CANCELLED because there are no more active subscribers
  auto res1 = subgroup->object(1, test::makeBuf(10));
  EXPECT_FALSE(res1.hasValue());
  EXPECT_EQ(res1.error().code, MoQPublishError::CANCELLED);

  auto res2 = subgroup->object(2, test::makeBuf(10));
  EXPECT_FALSE(res2.hasValue());
  EXPECT_EQ(res2.error().code, MoQPublishError::CANCELLED);

  subgroup->reset(ResetStreamErrorCode::SESSION_CLOSED);
  removeSession(publisherSession);
  removeSession(subscriber);
}

// Test: Late joiner gets subgroup even after another subscriber tombstoned it
TEST_F(MoQRelayTest, LateJoinerGetsSubgroupAfterTombstone) {
  auto publisherSession = createMockSession();
  auto subscriber1 = createMockSession();
  auto subscriber2 = createMockSession();

  // Setup: Publish track
  auto publishConsumer = doPublish(publisherSession, kTestTrackName);

  // First subscriber fails with CANCELLED (tombstones subgroup)
  auto consumer1 = createMockConsumer();
  std::shared_ptr<MockSubgroupConsumer> sg1;

  EXPECT_CALL(*consumer1, beginSubgroup(0, 0, _, _))
      .WillOnce([this, &sg1](uint64_t, uint64_t, uint8_t, bool) {
        sg1 = createMockSubgroupConsumer();
        EXPECT_CALL(*sg1, object(0, _, _, false))
            .WillOnce(Return(
                folly::makeUnexpected(MoQPublishError(
                    MoQPublishError::CANCELLED, "STOP_SENDING"))));
        // Per API contract, error implies implicit reset - no reset() call
        return folly::
            makeExpected<MoQPublishError, std::shared_ptr<SubgroupConsumer>>(
                sg1);
      });

  // Second subscriber (late joiner) should still get the subgroup
  auto consumer2 = createMockConsumer();
  std::shared_ptr<MockSubgroupConsumer> sg2;

  EXPECT_CALL(*consumer2, beginSubgroup(0, 0, _, _))
      .WillOnce([this, &sg2](uint64_t, uint64_t, uint8_t, bool) {
        sg2 = createMockSubgroupConsumer();
        EXPECT_CALL(*sg2, object(1, _, _, false)).WillOnce(Return(folly::unit));
        EXPECT_CALL(*sg2, endOfSubgroup()).WillOnce(Return(folly::unit));
        return folly::
            makeExpected<MoQPublishError, std::shared_ptr<SubgroupConsumer>>(
                sg2);
      });

  // Subscribe first subscriber
  subscribeToTrack(subscriber1, kTestTrackName, consumer1, RequestID(1));

  // Begin subgroup
  auto subgroupRes = publishConsumer->beginSubgroup(0, 0, 0);
  ASSERT_TRUE(subgroupRes.hasValue());
  auto subgroup = *subgroupRes;

  // First object triggers CANCELLED for subscriber1 - tombstones their subgroup
  EXPECT_TRUE(subgroup->object(0, test::makeBuf(10)).hasValue());

  // Late joiner subscribes after tombstone
  subscribeToTrack(subscriber2, kTestTrackName, consumer2, RequestID(2));

  // Next object should go to subscriber2 only (subscriber1 is tombstoned)
  EXPECT_TRUE(subgroup->object(1, test::makeBuf(10)).hasValue());

  // End subgroup
  EXPECT_TRUE(subgroup->endOfSubgroup().hasValue());

  removeSession(publisherSession);
  removeSession(subscriber1);
  removeSession(subscriber2);
}

// Test: Hard errors (WRITE_ERROR) remove the entire subscription
TEST_F(MoQRelayTest, HardErrorsRemoveSubscriber) {
  auto publisherSession = createMockSession();
  auto subscriber = createMockSession();

  // Setup: Publish track
  auto publishConsumer = doPublish(publisherSession, kTestTrackName);

  // Create subscriber that fails with WRITE_ERROR (hard error)
  auto consumer = createMockConsumer();
  std::shared_ptr<MockSubgroupConsumer> sg;

  EXPECT_CALL(*consumer, beginSubgroup(0, 0, _, _))
      .WillOnce([this, &sg](uint64_t, uint64_t, uint8_t, bool) {
        sg = createMockSubgroupConsumer();
        EXPECT_CALL(*sg, object(0, _, _, false))
            .WillOnce(Return(
                folly::makeUnexpected(MoQPublishError(
                    MoQPublishError::WRITE_ERROR, "transport broken"))));
        // Should be reset when subscriber is removed
        EXPECT_CALL(*sg, reset(_)).Times(1);
        return folly::
            makeExpected<MoQPublishError, std::shared_ptr<SubgroupConsumer>>(
                sg);
      });

  // publishDone should be called because subscription is being removed
  EXPECT_CALL(*consumer, publishDone(_))
      .WillOnce(Return(folly::makeExpected<MoQPublishError>(folly::unit)));

  // Second subgroup should NOT be created (subscriber removed)
  EXPECT_CALL(*consumer, beginSubgroup(1, _, _, _)).Times(0);

  subscribeToTrack(subscriber, kTestTrackName, consumer, RequestID(1));

  // Begin first subgroup
  auto subgroup1Res = publishConsumer->beginSubgroup(0, 0, 0);
  ASSERT_TRUE(subgroup1Res.hasValue());
  auto subgroup1 = *subgroup1Res;

  // Object triggers WRITE_ERROR - subscriber removed entirely
  // The call returns CANCELLED because no subscriber received the object
  auto res = subgroup1->object(0, test::makeBuf(10));
  EXPECT_FALSE(res.hasValue());
  EXPECT_EQ(res.error().code, MoQPublishError::CANCELLED);

  // Try to begin second subgroup - also returns CANCELLED (no subscribers)
  auto subgroup2Res = publishConsumer->beginSubgroup(1, 0, 0);
  EXPECT_FALSE(subgroup2Res.hasValue());
  EXPECT_EQ(subgroup2Res.error().code, MoQPublishError::CANCELLED);

  subgroup1->reset(ResetStreamErrorCode::SESSION_CLOSED);
  removeSession(publisherSession);
  removeSession(subscriber);
}

} // namespace moxygen::test

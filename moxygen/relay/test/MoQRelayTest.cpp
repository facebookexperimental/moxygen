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
        .WillByDefault(Return(folly::Optional<uint64_t>(kVersionDraftCurrent)));
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
    ON_CALL(*consumer, subscribeDone(_))
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
    // Handles for results from announce, subscribeAnnounces, and subscribe
    std::vector<std::shared_ptr<Subscriber::AnnounceHandle>> announceHandles;
    std::vector<std::shared_ptr<Publisher::SubscribeAnnouncesHandle>>
        subscribeAnnouncesHandles;
    std::vector<std::shared_ptr<Publisher::SubscriptionHandle>>
        subscribeHandles;

    void cleanup() {
      // Simulate MoQSession::cleanup() for publish tracks
      // This calls subscribeDone on all tracked consumers, which triggers
      // FilterConsumer callbacks that properly clean up relay state
      for (auto& consumer : publishConsumers) {
        consumer->subscribeDone(
            {RequestID(0),
             SubscribeDoneStatusCode::SESSION_CLOSED,
             0,
             "mock session cleanup"});
      }
      publishConsumers.clear();

      // Clean up announceHandles
      for (auto& handle : announceHandles) {
        if (handle) {
          handle->unannounce();
        }
      }
      announceHandles.clear();

      // Clean up subscribeAnnouncesHandles
      for (auto& handle : subscribeAnnouncesHandles) {
        if (handle) {
          handle->unsubscribeAnnounces();
        }
      }
      subscribeAnnouncesHandles.clear();

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

  // Helper to announce a namespace
  // Returns the AnnounceHandle so tests can use it for manual cleanup if needed
  // If addToState is true, the handle is automatically saved for cleanup
  std::shared_ptr<Subscriber::AnnounceHandle> doAnnounce(
      std::shared_ptr<MoQSession> session,
      const TrackNamespace& ns,
      bool addToState = true) {
    Announce ann;
    ann.trackNamespace = ns;
    return withSessionContext(session, [&]() {
      auto task = relay_->announce(std::move(ann), nullptr);
      auto res = folly::coro::blockingWait(std::move(task), exec_.get());
      EXPECT_TRUE(res.hasValue());
      if (res.hasValue()) {
        if (addToState) {
          getOrCreateMockState(session)->announceHandles.push_back(*res);
        }
        return *res;
      }
      return std::shared_ptr<Subscriber::AnnounceHandle>(nullptr);
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

  // Helper to subscribe to namespace announces
  // Returns the SubscribeAnnouncesHandle so tests can use it for manual cleanup
  // if needed If addToState is true, the handle is automatically saved for
  // cleanup
  std::shared_ptr<Publisher::SubscribeAnnouncesHandle> doSubscribeAnnounces(
      std::shared_ptr<MoQSession> session,
      const TrackNamespace& nsPrefix,
      bool addToState = true) {
    SubscribeAnnounces subNs;
    subNs.trackNamespacePrefix = nsPrefix;
    return withSessionContext(session, [&]() {
      auto task = relay_->subscribeAnnounces(std::move(subNs));
      auto res = folly::coro::blockingWait(std::move(task), exec_.get());
      EXPECT_TRUE(res.hasValue());
      if (res.hasValue()) {
        if (addToState) {
          getOrCreateMockState(session)->subscribeAnnouncesHandles.push_back(
              *res);
        }
        return *res;
      }
      return std::shared_ptr<Publisher::SubscribeAnnouncesHandle>(nullptr);
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
    ON_CALL(*sg, endOfGroup(_, _))
        .WillByDefault(
            Return(folly::makeExpected<MoQPublishError>(folly::unit)));
    ON_CALL(*sg, endOfTrackAndGroup(_, _))
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

  // Announce the namespace
  doAnnounce(publisherSession, kTestNamespace);

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

  // Announce test/A/B/C (don't add to state because we unannounce manually)
  TrackNamespace nsABC{{"test", "A", "B", "C"}};
  auto handleABC = doAnnounce(publisherABC, nsABC, /*addToState=*/false);

  // Announce test/A/D
  TrackNamespace nsAD{{"test", "A", "D"}};
  doAnnounce(publisherAD, nsAD);

  // Unannounce test/A/B/C - should prune B (and C) but keep A and D
  withSessionContext(publisherABC, [&]() { handleABC->unannounce(); });

  // Verify test/A/D still exists using findAnnounceSessions
  auto sessions = relay_->findAnnounceSessions(nsAD);
  EXPECT_EQ(sessions.size(), 1);
  EXPECT_EQ(sessions[0], publisherAD);

  removeSession(publisherABC);
  removeSession(publisherAD);
}

// Test: Tree pruning removes highest empty ancestor
// Scenario: test/A/B/C only. Remove C should prune A (highest empty after test)
TEST_F(MoQRelayTest, PruneHighestEmptyAncestor) {
  auto publisher = createMockSession();

  // Announce test/A/B/C (don't add to state because we unannounce manually)
  TrackNamespace nsABC{{"test", "A", "B", "C"}};
  auto handle = doAnnounce(publisher, nsABC, /*addToState=*/false);

  // Unannounce test/A/B/C - should prune A (highest empty ancestor)
  withSessionContext(publisher, [&]() { handle->unannounce(); });

  // Try to announce test/A/B/C again with a new session - should create fresh
  // tree
  auto publisher2 = createMockSession();
  doAnnounce(publisher2, nsABC);

  removeSession(publisher);
  removeSession(publisher2);
}

// Test: Pruning happens automatically on removeSession
TEST_F(MoQRelayTest, PruneOnRemoveSession) {
  auto publisher = createMockSession();

  // Announce deep tree test/A/B/C/D
  TrackNamespace nsABCD{{"test", "A", "B", "C", "D"}};
  doAnnounce(publisher, nsABCD);

  // Remove session - should prune entire tree test/A/B/C/D
  removeSession(publisher);

  // Verify we can create test/A/B/C/D again (tree was pruned)
  auto publisher2 = createMockSession();
  doAnnounce(publisher2, nsABCD);

  removeSession(publisher2);
}

// Test: No pruning when node still has content (multiple publishers)
TEST_F(MoQRelayTest, NoPruneWhenNodeHasContent) {
  auto publisher1 = createMockSession();
  auto publisher2 = createMockSession();

  // Both announce test/A/B
  TrackNamespace nsAB{{"test", "A", "B"}};
  auto handle1 = doAnnounce(publisher1, nsAB, /*addToState=*/false);
  doAnnounce(publisher2, nsAB);

  // Unannounce from publisher1 - should NOT prune because publisher2 still
  // there
  withSessionContext(publisher1, [&]() { handle1->unannounce(); });

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

  // Create deep tree test/A/B/C with only a publish (no announce)
  TrackNamespace nsABC{{"test", "A", "B", "C"}};

  // First announce so we can publish
  doAnnounce(publisher, nsABC);

  // Publish a track
  auto consumer = doPublish(publisher, FullTrackName{nsABC, "track1"});

  // Verify publish exists in the tree
  auto state = relay_->findPublishState(FullTrackName{nsABC, "track1"});
  EXPECT_TRUE(state.nodeExists);
  EXPECT_EQ(state.session, publisher);

  // Unannounce - node should stay because publish is still active
  withSessionContext(publisher, [&]() {
    getOrCreateMockState(publisher)->announceHandles[0]->unannounce();
    getOrCreateMockState(publisher)->announceHandles.clear();
  });

  // Publish should still be there, node still exists
  state = relay_->findPublishState(FullTrackName{nsABC, "track1"});
  EXPECT_TRUE(state.nodeExists);
  EXPECT_EQ(state.session, publisher);

  // End the publish - onPublishDone gets called
  withSessionContext(publisher, [&]() {
    consumer->subscribeDone(
        {RequestID(0),
         SubscribeDoneStatusCode::SUBSCRIPTION_ENDED,
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

// Test: Mixed content types - node with announce + publish
TEST_F(MoQRelayTest, MixedContentAnnounceAndPublish) {
  auto publisher = createMockSession();

  // Announce test/A/B
  TrackNamespace nsAB{{"test", "A", "B"}};
  doAnnounce(publisher, nsAB);

  // Publish a track in test/A/B
  doPublish(publisher, FullTrackName{nsAB, "track1"});

  // Unannounce - should NOT prune because publish still exists
  withSessionContext(publisher, [&]() {
    getOrCreateMockState(publisher)->announceHandles[0]->unannounce();
    getOrCreateMockState(publisher)->announceHandles.clear();
  });

  // Verify node still exists by publishing another track
  doPublish(publisher, FullTrackName{nsAB, "track2"});

  removeSession(publisher);
}

// Test: Mixed content types - node with announce + sessions (subscribers)
TEST_F(MoQRelayTest, MixedContentAnnounceAndSessions) {
  auto publisher = createMockSession();
  auto subscriber = createMockSession();

  // Announce test/A/B
  TrackNamespace nsAB{{"test", "A", "B"}};
  doAnnounce(publisher, nsAB);

  // Subscribe to namespace from another session
  doSubscribeAnnounces(subscriber, nsAB);

  // Unannounce from publisher - should NOT prune because subscriber still there
  std::shared_ptr<Subscriber::AnnounceHandle> handle =
      getOrCreateMockState(publisher)->announceHandles[0];
  getOrCreateMockState(publisher)->announceHandles.clear();
  withSessionContext(publisher, [&]() { handle->unannounce(); });

  // Announce again from a different publisher - should work (node still exists)
  auto publisher2 = createMockSession();
  doAnnounce(publisher2, nsAB);

  removeSession(publisher);
  removeSession(publisher2);
  removeSession(subscriber);
}

// Test: UnsubscribeNamespace triggers pruning
TEST_F(MoQRelayTest, PruneOnUnsubscribeNamespace) {
  auto subscriber = createMockSession();

  // Subscribe to test/A/B/C namespace (creates tree without announce)
  TrackNamespace nsABC{{"test", "A", "B", "C"}};
  auto handle = doSubscribeAnnounces(subscriber, nsABC, /*addToState=*/false);

  // Unsubscribe - should prune the entire test/A/B/C tree
  withSessionContext(subscriber, [&]() { handle->unsubscribeAnnounces(); });

  // Verify tree was pruned by subscribing again - should create fresh tree
  doSubscribeAnnounces(subscriber, nsABC);

  removeSession(subscriber);
}

// Test: Middle empty nodes in deep tree
// Scenario: test/A (has announce), test/A/B (empty), test/A/B/C (has publish)
// Remove C should prune B but keep A
TEST_F(MoQRelayTest, PruneMiddleEmptyNode) {
  auto publisherA = createMockSession();
  auto publisherC = createMockSession();

  // Announce test/A
  TrackNamespace nsA{{"test", "A"}};
  doAnnounce(publisherA, nsA);

  // Announce test/A/B/C (this creates B as empty intermediate node)
  TrackNamespace nsABC{{"test", "A", "B", "C"}};
  auto handleC = doAnnounce(publisherC, nsABC, /*addToState=*/false);

  // Unannounce test/A/B/C - should prune B and C but keep A
  withSessionContext(publisherC, [&]() { handleC->unannounce(); });

  // Verify test/A still exists
  auto sessionsA = relay_->findAnnounceSessions(nsA);
  EXPECT_EQ(sessionsA.size(), 1);
  EXPECT_EQ(sessionsA[0], publisherA);

  // Verify test/A/B/C was pruned - should be able to announce it again
  auto publisherC2 = createMockSession();
  doAnnounce(publisherC2, nsABC);

  removeSession(publisherA);
  removeSession(publisherC);
  removeSession(publisherC2);
}

// Test: Double unannounce doesn't crash or corrupt state
TEST_F(MoQRelayTest, DoubleUnannounce) {
  auto publisher = createMockSession();

  // Announce test/A/B
  TrackNamespace nsAB{{"test", "A", "B"}};
  auto handle = doAnnounce(publisher, nsAB, /*addToState=*/false);

  // Unannounce once
  withSessionContext(publisher, [&]() { handle->unannounce(); });

  // Unannounce again - should not crash (code handles this gracefully)
  withSessionContext(publisher, [&]() { handle->unannounce(); });

  // Verify we can still use the relay
  auto publisher2 = createMockSession();
  doAnnounce(publisher2, nsAB);

  removeSession(publisher);
  removeSession(publisher2);
}

// Test: Pruning with multiple children at same level
// Scenario: test/A has children B, C, D. Only B has content.
// Remove B should prune B but keep A, C, D structure intact
TEST_F(MoQRelayTest, PruneOneOfMultipleChildren) {
  auto publisherB = createMockSession();
  auto subscriberC = createMockSession();
  auto subscriberD = createMockSession();

  // Announce test/A/B
  TrackNamespace nsAB{{"test", "A", "B"}};
  auto handleB = doAnnounce(publisherB, nsAB, /*addToState=*/false);

  // Subscribe to test/A/C (creates C as empty node with session)
  TrackNamespace nsAC{{"test", "A", "C"}};
  doSubscribeAnnounces(subscriberC, nsAC);

  // Subscribe to test/A/D
  TrackNamespace nsAD{{"test", "A", "D"}};
  doSubscribeAnnounces(subscriberD, nsAD);

  // Unannounce test/A/B - should prune only B
  withSessionContext(publisherB, [&]() { handleB->unannounce(); });

  // Verify test/A still exists (has children C and D)
  // Try to announce at test/A
  auto publisherA = createMockSession();
  TrackNamespace nsA{{"test", "A"}};
  doAnnounce(publisherA, nsA);

  removeSession(publisherB);
  removeSession(publisherA);
  removeSession(subscriberC);
  removeSession(subscriberD);
}

// Test: Empty namespace edge case
TEST_F(MoQRelayTest, EmptyNamespaceUnannounce) {
  auto publisher = createMockSession();

  // Try to announce empty namespace (edge case)
  TrackNamespace emptyNs{{}};

  // This might fail or succeed depending on implementation
  // Just verify it doesn't crash
  Announce ann;
  ann.trackNamespace = emptyNs;
  withSessionContext(publisher, [&]() {
    auto task = relay_->announce(std::move(ann), nullptr);
    auto res = folly::coro::blockingWait(std::move(task), exec_.get());
    // Don't assert on success/failure, just verify no crash
    if (res.hasValue()) {
      getOrCreateMockState(publisher)->announceHandles.push_back(res.value());
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
  doAnnounce(pub1, nsAB);

  TrackNamespace nsAC{{"test", "A", "C"}};
  doSubscribeAnnounces(sub1, nsAC);

  // At this point, test/A should have activeChildCount_ == 2 (B and C)
  // We can't directly access private members, but we can verify behavior

  // Remove pub1 (which should remove B and decrement A's count)
  removeSession(pub1);

  // A should still exist (C is still active)
  // Verify by announcing at test/A
  TrackNamespace nsA{{"test", "A"}};
  doAnnounce(pub2, nsA);

  // Remove sub1 (which should remove C)
  removeSession(sub1);

  // A should still exist because pub2 announced at A
  auto sessions = relay_->findAnnounceSessions(nsA);
  EXPECT_EQ(sessions.size(), 1);
  EXPECT_EQ(sessions[0], pub2);

  removeSession(pub2);
}

// Test: Publish then unannounce shouldn't prune while publish active
TEST_F(MoQRelayTest, PublishKeepsNodeAliveAfterUnannounce) {
  auto publisher = createMockSession();

  // Announce test/A/B
  TrackNamespace nsAB{{"test", "A", "B"}};
  doAnnounce(publisher, nsAB);

  // Publish track
  doPublish(publisher, FullTrackName{nsAB, "track1"});

  // Unannounce (but publish is still active)
  std::shared_ptr<Subscriber::AnnounceHandle> handle =
      getOrCreateMockState(publisher)->announceHandles[0];
  getOrCreateMockState(publisher)->announceHandles.clear();
  withSessionContext(publisher, [&]() { handle->unannounce(); });

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
  EXPECT_CALL(*mockConsumer1, beginSubgroup(_, _, _))
      .WillOnce([&](uint64_t, uint64_t, uint8_t) {
        auto sg = std::make_shared<NiceMock<MockSubgroupConsumer>>();
        setupSubgroupConsumer(sg);
        return folly::
            makeExpected<MoQPublishError, std::shared_ptr<SubgroupConsumer>>(
                sg);
      });

  EXPECT_CALL(*mockConsumer2, beginSubgroup(_, _, _))
      .WillOnce([&](uint64_t, uint64_t, uint8_t) {
        auto sg = std::make_shared<NiceMock<MockSubgroupConsumer>>();
        setupSubgroupConsumer(sg);
        return folly::
            makeExpected<MoQPublishError, std::shared_ptr<SubgroupConsumer>>(
                sg);
      });

  // Subscriber 3 joins mid-object and should NOT get beginSubgroup
  EXPECT_CALL(*mockConsumer3, beginSubgroup(_, _, _)).Times(0);

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
// This test verifies that when a publisher calls subscribeDone, subscribers
// are drained (receive subscribeDone) but their open subgroups are NOT reset.
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
    EXPECT_CALL(*consumers[0], beginSubgroup(i, 0, _))
        .WillOnce([this, i, &sub0_sgs](uint64_t, uint64_t, uint8_t) {
          sub0_sgs[i] = createMockSubgroupConsumer();
          return folly::
              makeExpected<MoQPublishError, std::shared_ptr<SubgroupConsumer>>(
                  sub0_sgs[i]);
        });
  }

  // Subscriber2: will have 1 open subgroup (1)
  EXPECT_CALL(*consumers[1], beginSubgroup(1, 0, _))
      .WillOnce([this, &sub1_sg0](uint64_t, uint64_t, uint8_t) {
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

  // Trigger draining by calling subscribeDone from publisher
  // All subscribers should receive subscribeDone but subgroups should NOT reset
  // Note: subscribeDone may be called multiple times (during drain and cleanup)
  for (auto& consumer : consumers) {
    EXPECT_CALL(*consumer, subscribeDone(_))
        .WillOnce(Return(folly::makeExpected<MoQPublishError>(folly::unit)));
  }

  // Verify subgroups are NOT reset (no reset() calls expected)
  EXPECT_CALL(*sub0_sgs[0], reset(_)).Times(0);
  EXPECT_CALL(*sub0_sgs[1], reset(_)).Times(0);
  EXPECT_CALL(*sub1_sg0, reset(_)).Times(0);

  publishConsumer->subscribeDone(
      SubscribeDone{
          RequestID(0),
          SubscribeDoneStatusCode::SUBSCRIPTION_ENDED,
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
    EXPECT_CALL(*consumer, beginSubgroup(i, 0, _))
        .WillOnce([this, i, &sgs](uint64_t, uint64_t, uint8_t) {
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
    EXPECT_CALL(*consumer, beginSubgroup(i, 0, _))
        .WillOnce([this, i, &sgs](uint64_t, uint64_t, uint8_t) {
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
  // Note: subscribeDone may be called multiple times (during drain and cleanup)
  EXPECT_CALL(*consumer, subscribeDone(_))
      .Times(AtLeast(1))
      .WillRepeatedly(
          Return(folly::makeExpected<MoQPublishError>(folly::unit)));

  // Subgroups should NOT be reset during drain
  EXPECT_CALL(*sgs[0], reset(_)).Times(0);
  EXPECT_CALL(*sgs[1], reset(_)).Times(0);
  EXPECT_CALL(*sgs[2], reset(_)).Times(0);

  publishConsumer->subscribeDone(
      SubscribeDone{
          RequestID(0),
          SubscribeDoneStatusCode::SUBSCRIPTION_ENDED,
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
// Sequence: publish, sub1 subscribes, beginSubgroup, sub1 ends (subscribeDone),
// sub2 subscribes, send object -> only goes to sub1
TEST_F(MoQRelayTest, SubscriberUnsubscribeDoesNotReceiveNewObjects) {
  auto publisherSession = createMockSession();
  auto subscriber1 = createMockSession();
  auto subscriber2 = createMockSession();

  // Set up mock consumers
  auto mockConsumer1 = createMockConsumer();
  auto mockConsumer2 = createMockConsumer();

  // Sub1 should get beginSubgroup
  EXPECT_CALL(*mockConsumer1, beginSubgroup(_, _, _))
      .WillOnce([&](uint64_t, uint64_t, uint8_t) {
        auto sg = std::make_shared<NiceMock<MockSubgroupConsumer>>();
        EXPECT_CALL(*sg, beginObject(_, _, _, _)).WillOnce(Return(folly::unit));
        EXPECT_CALL(*sg, reset(_));
        return folly::
            makeExpected<MoQPublishError, std::shared_ptr<SubgroupConsumer>>(
                sg);
      });

  // Sub2 should also get beginSubgroup
  EXPECT_CALL(*mockConsumer2, beginSubgroup(_, _, _)).Times(0);

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
  EXPECT_CALL(*mockConsumer1, subscribeDone(testing::_));
  EXPECT_TRUE(publishConsumer
                  ->subscribeDone(
                      {RequestID(1),
                       SubscribeDoneStatusCode::TRACK_ENDED,
                       0,
                       "track ended"})
                  .hasValue());

  // Subscriber 2 joins after subscribeDone
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

// Test: SubscribeAnnounces only receives publishes while active
// Sequence: subscribeAnnounces (sub1), publish (sub1 gets it), beginSubgroup,
// subscribeDone, subscribeAnnounces (sub2), new publish (only sub2 gets it)
TEST_F(MoQRelayTest, SubscribeAnnouncesDoesntAddDrainingPublish) {
  auto publisherSession = createMockSession();
  auto subscriber1 = createMockSession();
  auto subscriber2 = createMockSession();

  // Subscriber 1 subscribes to announces
  auto handle1 =
      doSubscribeAnnounces(subscriber1, kTestNamespace, /*addToState=*/false);

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
                                      /*start=*/folly::none,
                                      /*endGroup=*/folly::none,
                                      /*params=*/{}};
                }()});
      });

  EXPECT_CALL(*mockConsumer1, beginSubgroup(_, _, _))
      .WillOnce([&](uint64_t, uint64_t, uint8_t) {
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
  EXPECT_CALL(*mockConsumer1, subscribeDone(testing::_));
  EXPECT_TRUE(pubConsumer
                  ->subscribeDone(
                      {RequestID(1),
                       SubscribeDoneStatusCode::TRACK_ENDED,
                       0,
                       "track ended"})
                  .hasValue());
  subgroup->endOfSubgroup();

  // Subscriber 2 subscribes to announces but doesn't get finished track
  doSubscribeAnnounces(subscriber2, kTestNamespace);

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
    EXPECT_CALL(*consumers[i], beginSubgroup(0, 0, _))
        .WillOnce([this, i, &sgs](uint64_t, uint64_t, uint8_t) {
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
  EXPECT_CALL(*consumers[0], beginSubgroup(0, 0, _))
      .WillOnce([this, &sgs](uint64_t, uint64_t, uint8_t) {
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
    EXPECT_CALL(*consumers[i], beginSubgroup(0, 0, _))
        .WillOnce([this, i, &sgs](uint64_t, uint64_t, uint8_t) {
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

} // namespace moxygen::test

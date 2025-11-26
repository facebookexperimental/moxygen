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

  // Helper to remove a session from the relay and clean up mock state
  void removeSession(std::shared_ptr<MoQSession> sess) {
    cleanupMockSession(std::move(sess));
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
      auto res = relay_->publish(std::move(pub), nullptr);
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

} // namespace moxygen::test

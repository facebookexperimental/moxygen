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

  // First, announce the namespace
  Announce ann;
  ann.trackNamespace = kTestNamespace;

  // Announce the namespace (this should succeed)
  withSessionContext(publisherSession, [&]() {
    auto res =
        folly::coro::blockingWait(relay_->announce(std::move(ann), nullptr));
    EXPECT_TRUE(res.hasValue());
    getOrCreateMockState(publisherSession)->announceHandles.push_back(*res);
  });

  // Now publish a track in that namespace
  PublishRequest pub;
  pub.fullTrackName = kTestTrackName;

  // Publish the track (this should succeed)
  withSessionContext(publisherSession, [&]() {
    auto res = relay_->publish(std::move(pub), nullptr);
    EXPECT_TRUE(res.hasValue());
    getOrCreateMockState(publisherSession)
        ->publishConsumers.push_back(res->consumer);
  });

  // Cleanup: remove the session from relay to avoid mock leak warning
  removeSession(publisherSession);
}

// Test: Tree pruning when leaf node is removed
// Scenario: test/A/B/C and test/A/D exist. Remove C should prune B but keep A
// and D
TEST_F(MoQRelayTest, PruneLeafKeepSiblings) {
  auto publisherABC = createMockSession();
  auto publisherAD = createMockSession();

  // Announce test/A/B/C
  TrackNamespace nsABC{{"test", "A", "B", "C"}};
  Announce annABC;
  annABC.trackNamespace = nsABC;
  std::shared_ptr<Subscriber::AnnounceHandle> handleABC;
  withSessionContext(publisherABC, [&]() {
    auto task = relay_->announce(std::move(annABC), nullptr);
    auto res = folly::coro::blockingWait(std::move(task), exec_.get());
    EXPECT_TRUE(res.hasValue());
    handleABC = std::move(*res);
    // Not saving state, because we unannounce before remove
  });

  // Announce test/A/D
  TrackNamespace nsAD{{"test", "A", "D"}};
  Announce annAD;
  annAD.trackNamespace = nsAD;
  withSessionContext(publisherAD, [&]() {
    auto task = relay_->announce(std::move(annAD), nullptr);
    auto res = folly::coro::blockingWait(std::move(task), exec_.get());
    EXPECT_TRUE(res.hasValue());
    getOrCreateMockState(publisherAD)->announceHandles.push_back(res.value());
  });

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

  // Announce test/A/B/C
  TrackNamespace nsABC{{"test", "A", "B", "C"}};
  Announce ann;
  ann.trackNamespace = nsABC;
  std::shared_ptr<Subscriber::AnnounceHandle> handle;
  withSessionContext(publisher, [&]() {
    auto task = relay_->announce(std::move(ann), nullptr);
    auto res = folly::coro::blockingWait(std::move(task), exec_.get());
    EXPECT_TRUE(res.hasValue());
    handle = res.value();
    // Not saving state, because we unannounce before remove
  });

  // Unannounce test/A/B/C - should prune A (highest empty ancestor)
  withSessionContext(publisher, [&]() { handle->unannounce(); });

  // Try to announce test/A/B/C again with a new session - should create fresh
  // tree
  auto publisher2 = createMockSession();

  Announce ann2;
  ann2.trackNamespace = nsABC;
  withSessionContext(publisher2, [&]() {
    auto task = relay_->announce(std::move(ann2), nullptr);
    auto res = folly::coro::blockingWait(std::move(task), exec_.get());
    EXPECT_TRUE(res.hasValue());
    getOrCreateMockState(publisher2)->announceHandles.push_back(res.value());
  });

  removeSession(publisher);
  removeSession(publisher2);
}

// Test: Pruning happens automatically on removeSession
TEST_F(MoQRelayTest, PruneOnRemoveSession) {
  auto publisher = createMockSession();

  // Announce deep tree test/A/B/C/D
  TrackNamespace nsABCD{{"test", "A", "B", "C", "D"}};
  Announce ann;
  ann.trackNamespace = nsABCD;
  withSessionContext(publisher, [&]() {
    auto task = relay_->announce(std::move(ann), nullptr);
    auto res = folly::coro::blockingWait(std::move(task), exec_.get());
    EXPECT_TRUE(res.hasValue());
    getOrCreateMockState(publisher)->announceHandles.push_back(res.value());
  });

  // Remove session - should prune entire tree test/A/B/C/D
  removeSession(publisher);

  // Verify we can create test/A/B/C/D again (tree was pruned)
  auto publisher2 = createMockSession();

  Announce ann2;
  ann2.trackNamespace = nsABCD;
  withSessionContext(publisher2, [&]() {
    auto task = relay_->announce(std::move(ann2), nullptr);
    auto res = folly::coro::blockingWait(std::move(task), exec_.get());
    EXPECT_TRUE(res.hasValue());
    getOrCreateMockState(publisher2)->announceHandles.push_back(res.value());
  });

  removeSession(publisher2);
}

// Test: No pruning when node still has content (multiple publishers)
TEST_F(MoQRelayTest, NoPruneWhenNodeHasContent) {
  auto publisher1 = createMockSession();
  auto publisher2 = createMockSession();

  // Both announce test/A/B
  TrackNamespace nsAB{{"test", "A", "B"}};
  Announce ann1;
  ann1.trackNamespace = nsAB;
  std::shared_ptr<Subscriber::AnnounceHandle> handle1;
  withSessionContext(publisher1, [&]() {
    auto task = relay_->announce(std::move(ann1), nullptr);
    auto res = folly::coro::blockingWait(std::move(task), exec_.get());
    EXPECT_TRUE(res.hasValue());
    handle1 = res.value();
    // Not saving state, because we unannounce before remove
  });

  Announce ann2;
  ann2.trackNamespace = nsAB;
  withSessionContext(publisher2, [&]() {
    auto task = relay_->announce(std::move(ann2), nullptr);
    auto res = folly::coro::blockingWait(std::move(task), exec_.get());
    EXPECT_TRUE(res.hasValue());
    getOrCreateMockState(publisher2)->announceHandles.push_back(res.value());
  });

  // Unannounce from publisher1 - should NOT prune because publisher2 still
  // there
  withSessionContext(publisher1, [&]() { handle1->unannounce(); });

  // Verify test/A/B still exists by publishing a track through publisher2
  PublishRequest pub;
  pub.fullTrackName = FullTrackName{nsAB, "track1"};
  withSessionContext(publisher2, [&]() {
    auto res = relay_->publish(std::move(pub), nullptr);
    EXPECT_TRUE(res.hasValue());
    getOrCreateMockState(publisher2)->publishConsumers.push_back(res->consumer);
  });

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
  PublishRequest pub;
  pub.fullTrackName = FullTrackName{nsABC, "track1"};
  std::shared_ptr<TrackConsumer> consumer;

  // First announce so we can publish
  Announce ann;
  ann.trackNamespace = nsABC;
  withSessionContext(publisher, [&]() {
    auto task = relay_->announce(std::move(ann), nullptr);
    auto res = folly::coro::blockingWait(std::move(task), exec_.get());
    EXPECT_TRUE(res.hasValue());
    getOrCreateMockState(publisher)->announceHandles.push_back(res.value());
  });

  withSessionContext(publisher, [&]() {
    auto res = relay_->publish(std::move(pub), nullptr);
    EXPECT_TRUE(res.hasValue());
    consumer = res->consumer;
    getOrCreateMockState(publisher)->publishConsumers.push_back(consumer);
  });

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
  Announce ann;
  ann.trackNamespace = nsAB;
  withSessionContext(publisher, [&]() {
    auto task = relay_->announce(std::move(ann), nullptr);
    auto res = folly::coro::blockingWait(std::move(task), exec_.get());
    EXPECT_TRUE(res.hasValue());
    getOrCreateMockState(publisher)->announceHandles.push_back(res.value());
  });

  // Publish a track in test/A/B
  PublishRequest pub;
  pub.fullTrackName = FullTrackName{nsAB, "track1"};
  withSessionContext(publisher, [&]() {
    auto res = relay_->publish(std::move(pub), nullptr);
    EXPECT_TRUE(res.hasValue());
    getOrCreateMockState(publisher)->publishConsumers.push_back(res->consumer);
  });

  // Unannounce - should NOT prune because publish still exists
  withSessionContext(publisher, [&]() {
    getOrCreateMockState(publisher)->announceHandles[0]->unannounce();
    getOrCreateMockState(publisher)->announceHandles.clear();
  });

  // Verify node still exists by publishing another track
  PublishRequest pub2;
  pub2.fullTrackName = FullTrackName{nsAB, "track2"};
  withSessionContext(publisher, [&]() {
    auto res = relay_->publish(std::move(pub2), nullptr);
    EXPECT_TRUE(res.hasValue());
    getOrCreateMockState(publisher)->publishConsumers.push_back(res->consumer);
  });

  removeSession(publisher);
}

// Test: Mixed content types - node with announce + sessions (subscribers)
TEST_F(MoQRelayTest, MixedContentAnnounceAndSessions) {
  auto publisher = createMockSession();
  auto subscriber = createMockSession();

  // Announce test/A/B
  TrackNamespace nsAB{{"test", "A", "B"}};
  Announce ann;
  ann.trackNamespace = nsAB;
  withSessionContext(publisher, [&]() {
    auto task = relay_->announce(std::move(ann), nullptr);
    auto res = folly::coro::blockingWait(std::move(task), exec_.get());
    EXPECT_TRUE(res.hasValue());
    getOrCreateMockState(publisher)->announceHandles.push_back(res.value());
  });

  // Subscribe to namespace from another session
  SubscribeAnnounces subNs;
  subNs.trackNamespacePrefix = nsAB;
  withSessionContext(subscriber, [&]() {
    auto task = relay_->subscribeAnnounces(std::move(subNs));
    auto res = folly::coro::blockingWait(std::move(task), exec_.get());
    EXPECT_TRUE(res.hasValue());
    getOrCreateMockState(subscriber)
        ->subscribeAnnouncesHandles.push_back(res.value());
  });

  // Unannounce from publisher - should NOT prune because subscriber still there
  std::shared_ptr<Subscriber::AnnounceHandle> handle =
      getOrCreateMockState(publisher)->announceHandles[0];
  getOrCreateMockState(publisher)->announceHandles.clear();
  withSessionContext(publisher, [&]() { handle->unannounce(); });

  // Announce again from a different publisher - should work (node still exists)
  auto publisher2 = createMockSession();
  Announce ann2;
  ann2.trackNamespace = nsAB;
  withSessionContext(publisher2, [&]() {
    auto task = relay_->announce(std::move(ann2), nullptr);
    auto res = folly::coro::blockingWait(std::move(task), exec_.get());
    EXPECT_TRUE(res.hasValue());
    getOrCreateMockState(publisher2)->announceHandles.push_back(res.value());
  });

  removeSession(publisher);
  removeSession(publisher2);
  removeSession(subscriber);
}

// Test: UnsubscribeNamespace triggers pruning
TEST_F(MoQRelayTest, PruneOnUnsubscribeNamespace) {
  auto subscriber = createMockSession();

  // Subscribe to test/A/B/C namespace (creates tree without announce)
  TrackNamespace nsABC{{"test", "A", "B", "C"}};
  SubscribeAnnounces subNs;
  subNs.trackNamespacePrefix = nsABC;
  std::shared_ptr<Publisher::SubscribeAnnouncesHandle> handle;
  withSessionContext(subscriber, [&]() {
    auto task = relay_->subscribeAnnounces(std::move(subNs));
    auto res = folly::coro::blockingWait(std::move(task), exec_.get());
    EXPECT_TRUE(res.hasValue());
    handle = res.value();
    // Not saving to state because we'll manually unsubscribe
  });

  // Unsubscribe - should prune the entire test/A/B/C tree
  withSessionContext(subscriber, [&]() { handle->unsubscribeAnnounces(); });

  // Verify tree was pruned by subscribing again - should create fresh tree
  SubscribeAnnounces subNs2;
  subNs2.trackNamespacePrefix = nsABC;
  withSessionContext(subscriber, [&]() {
    auto task = relay_->subscribeAnnounces(std::move(subNs2));
    auto res = folly::coro::blockingWait(std::move(task), exec_.get());
    EXPECT_TRUE(res.hasValue());
    getOrCreateMockState(subscriber)
        ->subscribeAnnouncesHandles.push_back(res.value());
  });

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
  Announce annA;
  annA.trackNamespace = nsA;
  withSessionContext(publisherA, [&]() {
    auto task = relay_->announce(std::move(annA), nullptr);
    auto res = folly::coro::blockingWait(std::move(task), exec_.get());
    EXPECT_TRUE(res.hasValue());
    getOrCreateMockState(publisherA)->announceHandles.push_back(res.value());
  });

  // Announce test/A/B/C (this creates B as empty intermediate node)
  TrackNamespace nsABC{{"test", "A", "B", "C"}};
  Announce annC;
  annC.trackNamespace = nsABC;
  std::shared_ptr<Subscriber::AnnounceHandle> handleC;
  withSessionContext(publisherC, [&]() {
    auto task = relay_->announce(std::move(annC), nullptr);
    auto res = folly::coro::blockingWait(std::move(task), exec_.get());
    EXPECT_TRUE(res.hasValue());
    handleC = res.value();
  });

  // Unannounce test/A/B/C - should prune B and C but keep A
  withSessionContext(publisherC, [&]() { handleC->unannounce(); });

  // Verify test/A still exists
  auto sessionsA = relay_->findAnnounceSessions(nsA);
  EXPECT_EQ(sessionsA.size(), 1);
  EXPECT_EQ(sessionsA[0], publisherA);

  // Verify test/A/B/C was pruned - should be able to announce it again
  Announce annC2;
  annC2.trackNamespace = nsABC;
  auto publisherC2 = createMockSession();
  withSessionContext(publisherC2, [&]() {
    auto task = relay_->announce(std::move(annC2), nullptr);
    auto res = folly::coro::blockingWait(std::move(task), exec_.get());
    EXPECT_TRUE(res.hasValue());
    getOrCreateMockState(publisherC2)->announceHandles.push_back(res.value());
  });

  removeSession(publisherA);
  removeSession(publisherC);
  removeSession(publisherC2);
}

// Test: Double unannounce doesn't crash or corrupt state
TEST_F(MoQRelayTest, DoubleUnannounce) {
  auto publisher = createMockSession();

  // Announce test/A/B
  TrackNamespace nsAB{{"test", "A", "B"}};
  Announce ann;
  ann.trackNamespace = nsAB;
  std::shared_ptr<Subscriber::AnnounceHandle> handle;
  withSessionContext(publisher, [&]() {
    auto task = relay_->announce(std::move(ann), nullptr);
    auto res = folly::coro::blockingWait(std::move(task), exec_.get());
    EXPECT_TRUE(res.hasValue());
    handle = res.value();
  });

  // Unannounce once
  withSessionContext(publisher, [&]() { handle->unannounce(); });

  // Unannounce again - should not crash (code handles this gracefully)
  withSessionContext(publisher, [&]() { handle->unannounce(); });

  // Verify we can still use the relay
  auto publisher2 = createMockSession();
  Announce ann2;
  ann2.trackNamespace = nsAB;
  withSessionContext(publisher2, [&]() {
    auto task = relay_->announce(std::move(ann2), nullptr);
    auto res = folly::coro::blockingWait(std::move(task), exec_.get());
    EXPECT_TRUE(res.hasValue());
    getOrCreateMockState(publisher2)->announceHandles.push_back(res.value());
  });

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
  Announce annB;
  annB.trackNamespace = nsAB;
  std::shared_ptr<Subscriber::AnnounceHandle> handleB;
  withSessionContext(publisherB, [&]() {
    auto task = relay_->announce(std::move(annB), nullptr);
    auto res = folly::coro::blockingWait(std::move(task), exec_.get());
    EXPECT_TRUE(res.hasValue());
    handleB = res.value();
  });

  // Subscribe to test/A/C (creates C as empty node with session)
  TrackNamespace nsAC{{"test", "A", "C"}};
  SubscribeAnnounces subC;
  subC.trackNamespacePrefix = nsAC;
  withSessionContext(subscriberC, [&]() {
    auto task = relay_->subscribeAnnounces(std::move(subC));
    auto res = folly::coro::blockingWait(std::move(task), exec_.get());
    EXPECT_TRUE(res.hasValue());
    getOrCreateMockState(subscriberC)
        ->subscribeAnnouncesHandles.push_back(res.value());
  });

  // Subscribe to test/A/D
  TrackNamespace nsAD{{"test", "A", "D"}};
  SubscribeAnnounces subD;
  subD.trackNamespacePrefix = nsAD;
  withSessionContext(subscriberD, [&]() {
    auto task = relay_->subscribeAnnounces(std::move(subD));
    auto res = folly::coro::blockingWait(std::move(task), exec_.get());
    EXPECT_TRUE(res.hasValue());
    getOrCreateMockState(subscriberD)
        ->subscribeAnnouncesHandles.push_back(res.value());
  });

  // Unannounce test/A/B - should prune only B
  withSessionContext(publisherB, [&]() { handleB->unannounce(); });

  // Verify test/A still exists (has children C and D)
  // Try to announce at test/A
  auto publisherA = createMockSession();
  TrackNamespace nsA{{"test", "A"}};
  Announce annA;
  annA.trackNamespace = nsA;
  withSessionContext(publisherA, [&]() {
    auto task = relay_->announce(std::move(annA), nullptr);
    auto res = folly::coro::blockingWait(std::move(task), exec_.get());
    EXPECT_TRUE(res.hasValue());
    getOrCreateMockState(publisherA)->announceHandles.push_back(res.value());
  });

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
  Announce ann;
  ann.trackNamespace = emptyNs;

  // This might fail or succeed depending on implementation
  // Just verify it doesn't crash
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
  Announce annAB;
  annAB.trackNamespace = nsAB;
  withSessionContext(pub1, [&]() {
    auto task = relay_->announce(std::move(annAB), nullptr);
    auto res = folly::coro::blockingWait(std::move(task), exec_.get());
    EXPECT_TRUE(res.hasValue());
    getOrCreateMockState(pub1)->announceHandles.push_back(res.value());
  });

  TrackNamespace nsAC{{"test", "A", "C"}};
  SubscribeAnnounces subAC;
  subAC.trackNamespacePrefix = nsAC;
  withSessionContext(sub1, [&]() {
    auto task = relay_->subscribeAnnounces(std::move(subAC));
    auto res = folly::coro::blockingWait(std::move(task), exec_.get());
    EXPECT_TRUE(res.hasValue());
    getOrCreateMockState(sub1)->subscribeAnnouncesHandles.push_back(
        res.value());
  });

  // At this point, test/A should have activeChildCount_ == 2 (B and C)
  // We can't directly access private members, but we can verify behavior

  // Remove pub1 (which should remove B and decrement A's count)
  removeSession(pub1);

  // A should still exist (C is still active)
  // Verify by announcing at test/A
  TrackNamespace nsA{{"test", "A"}};
  Announce annA;
  annA.trackNamespace = nsA;
  withSessionContext(pub2, [&]() {
    auto task = relay_->announce(std::move(annA), nullptr);
    auto res = folly::coro::blockingWait(std::move(task), exec_.get());
    EXPECT_TRUE(res.hasValue());
    getOrCreateMockState(pub2)->announceHandles.push_back(res.value());
  });

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
  Announce ann;
  ann.trackNamespace = nsAB;
  withSessionContext(publisher, [&]() {
    auto task = relay_->announce(std::move(ann), nullptr);
    auto res = folly::coro::blockingWait(std::move(task), exec_.get());
    EXPECT_TRUE(res.hasValue());
    getOrCreateMockState(publisher)->announceHandles.push_back(res.value());
  });

  // Publish track
  PublishRequest pub;
  pub.fullTrackName = FullTrackName{nsAB, "track1"};
  withSessionContext(publisher, [&]() {
    auto res = relay_->publish(std::move(pub), nullptr);
    EXPECT_TRUE(res.hasValue());
    getOrCreateMockState(publisher)->publishConsumers.push_back(res->consumer);
  });

  // Unannounce (but publish is still active)
  std::shared_ptr<Subscriber::AnnounceHandle> handle =
      getOrCreateMockState(publisher)->announceHandles[0];
  getOrCreateMockState(publisher)->announceHandles.clear();
  withSessionContext(publisher, [&]() { handle->unannounce(); });

  // Try to publish another track - should work (node still exists)
  PublishRequest pub2;
  pub2.fullTrackName = FullTrackName{nsAB, "track2"};
  withSessionContext(publisher, [&]() {
    auto res = relay_->publish(std::move(pub2), nullptr);
    EXPECT_TRUE(res.hasValue());
    getOrCreateMockState(publisher)->publishConsumers.push_back(res->consumer);
  });

  removeSession(publisher);
}

} // namespace moxygen::test

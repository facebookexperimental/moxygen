/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <folly/coro/BlockingWait.h>
#include <folly/io/async/EventBase.h>
#include <folly/portability/GMock.h>
#include <folly/portability/GTest.h>
#include <moxygen/relay/MoQRelay.h>
#include <moxygen/test/MockMoQSession.h>

using namespace testing;

namespace moxygen::test {

const TrackNamespace kTestNamespace{{"test", "namespace"}};
const TrackNamespace kAllowedPrefix{{"test"}};
const FullTrackName kTestTrackName{kTestNamespace, "track1"};

// Test fixture for MoQRelay tests
// This provides a skeleton for testing MoQRelay functionality.
// Full integration tests with real sessions will be added when implementing
// multi-publisher support.
class MoQRelayTest : public ::testing::Test {
 protected:
  void SetUp() override {
    relay_ = std::make_shared<MoQRelay>(/*enableCache=*/false);
    relay_->setAllowedNamespacePrefix(kAllowedPrefix);
  }

  void TearDown() override {
    relay_.reset();
  }

  // Helper to create a mock session
  std::shared_ptr<MockMoQSession> createMockSession() {
    auto session = std::make_shared<NiceMock<MockMoQSession>>();
    ON_CALL(*session, getNegotiatedVersion())
        .WillByDefault(Return(folly::Optional<uint64_t>(kVersionDraftCurrent)));
    auto state = getOrCreateMockState(session);
    return session;
  }

  // Helper to remove a session from the relay and clean up mock state
  void removeSession(std::shared_ptr<MoQSession> sess) {
    cleanupMockSession(sess);
    relay_->removeSession(std::move(sess));
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

} // namespace moxygen::test

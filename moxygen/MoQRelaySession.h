/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/container/F14Map.h>
#include <moxygen/MoQSession.h>

namespace moxygen {

class SeparateStreamSubNsReply : public SubNSReply {
 public:
  SeparateStreamSubNsReply(
      MoQFrameWriter& moqFrameWriter,
      std::shared_ptr<ReplyContext> replyContext)
      : SubNSReply(moqFrameWriter, std::move(replyContext)) {}

  ~SeparateStreamSubNsReply() = default;

  WriteResult ok(const SubscribeNamespaceOk&) override;
  WriteResult error(const SubscribeNamespaceError&) override;
  WriteResult namespaceMsg(const Namespace&) override;
  WriteResult namespaceDoneMsg(const NamespaceDone&) override;

 private:
  void flushPendingMessages();

  folly::IOBufQueue pendingBuf_{folly::IOBufQueue::cacheChainLength()};
  bool pendingFin_{false};
  bool okSent_{false};
  bool namespaceFrameSent_{false};
  bool errorSent_{false};
};

/**
 * MoQRelaySession extends MoQSession with full publishNamespace
 * functionality.
 *
 * This subclass provides real implementations of publishNamespace() and
 * subscribeNamespace() methods, along with proper publishNamespace
 * state management. It should be used in relay servers and any applications
 * that need to handle publishNamespaces.
 *
 * The base MoQSession returns NOT_SUPPORTED for publishNamespace
 * operations, making it suitable for simple clients that only subscribe to
 * tracks.
 */
class MoQRelaySession : public MoQSession {
 public:
  // Inherit all base constructors
  using MoQSession::MoQSession;

  // ~MoQSession calls cleanup(), but the vtable has reverted to MoQSession
  // at that point, so MoQRelaySession::cleanup() would be skipped.
  // Call cleanupRelayState() directly to avoid shared_from_this() issues
  // in the destructor (cleanup() calls setRequestSession() which needs
  // a live shared_ptr).
  ~MoQRelaySession() override {
    cleanupRelayState();
  }

  // Static factory for creating relay sessions in clients
  static std::function<std::shared_ptr<MoQSession>(
      folly::MaybeManagedPtr<proxygen::WebTransport>,
      std::shared_ptr<MoQExecutor>)>
  createRelaySessionFactory();

  // Override cleanup method for proper inheritance pattern
  void cleanup() override;

  // Override publishNamespace methods with real implementations
  folly::coro::Task<Subscriber::PublishNamespaceResult> publishNamespace(
      PublishNamespace pubNs,
      std::shared_ptr<PublishNamespaceCallback> publishNamespaceCallback =
          nullptr) override;

  folly::coro::Task<Publisher::SubscribeNamespaceResult> subscribeNamespace(
      SubscribeNamespace subNs,
      std::shared_ptr<NamespacePublishHandle> namespacePublishHandle) override;

  // Draft 18+
  folly::coro::Task<Publisher::SubscribeTracksResult> subscribeTracks(
      SubscribeTracks subTracks,
      std::shared_ptr<PublishBlockedHandle> publishBlockedHandle =
          nullptr) override;

 protected:
  void onSubscribeNamespaceImpl(
      const SubscribeNamespace& subscribeNamespace,
      std::shared_ptr<SubNSReply> subNsReply) override;

  // Draft 18+
  void onSubscribeTracksImpl(
      const SubscribeTracks& subscribeTracks,
      std::shared_ptr<SubscribeTracksReply> subTracksReply) override;

  void onSubscribeTracksStreamClosed(RequestID requestID) override;

  std::shared_ptr<SubNSReply> getSubNsReply(
      std::shared_ptr<ReplyContext> replyContext) override {
    return std::make_shared<SeparateStreamSubNsReply>(
        moqFrameWriter_, std::move(replyContext));
  }

  std::shared_ptr<SubscribeTracksReply> getSubTracksReply(
      std::shared_ptr<ReplyContext> replyContext) override {
    return std::make_shared<SubscribeTracksReply>(
        moqFrameWriter_, std::move(replyContext));
  }

 private:
  // Clean up relay-specific state without requiring shared_from_this().
  // Safe to call from the destructor (where the shared_ptr is expired).
  void cleanupRelayState();

  // Forward declarations for inner classes
  class SubscriberPublishNamespaceCallback;
  class PublisherPublishNamespaceHandle;
  class SubscribeNamespaceHandle;
  class SubscribeTracksHandle;

  // Override to handle ANNOUNCE and SUBSCRIBE_ANNOUNCES updates
  void onRequestUpdate(RequestUpdate requestUpdate) override;

  // Route REQUEST_UPDATE responses for namespace requests to their own bidi
  // reply context (draft 18+); other requests fall back to the base.
  ReplyContext* getRequestUpdateReplyContext(
      RequestID existingRequestID) override;

  // A failed SUBSCRIBE_NAMESPACE / PUBLISH_NAMESPACE update closes the
  // request's bidi stream; other request types fall back to the base.
  void terminateRequestUpdateOnError(
      RequestID existingRequestID,
      const SubscribeUpdateError& requestError) override;

  // REQUEST_UPDATE handlers for namespace requests - take handles directly
  void handlePublishNamespaceRequestUpdate(
      RequestUpdate requestUpdate,
      std::shared_ptr<Subscriber::PublishNamespaceHandle> pubNsHandle);
  void handleSubscribeNamespaceRequestUpdate(
      RequestUpdate requestUpdate,
      std::shared_ptr<Publisher::SubscribeNamespaceHandle>
          subscribeNamespaceHandle);
  void handleSubscribeTracksRequestUpdate(
      RequestUpdate requestUpdate,
      std::shared_ptr<Publisher::SubscribeTracksHandle> subscribeTracksHandle);

  // Draft 16+: send a REQUEST_UPDATE for a locally-initiated
  // SUBSCRIBE_NAMESPACE (or SUBSCRIBE_TRACKS) and await the REQUEST_OK /
  // REQUEST_ERROR. Draft 18+ sends it on the subscription's own bidi request
  // stream (correlated FIFO via the stream's responseIDQueue); pre-18 there are
  // no per-request bidi streams, so it rides the shared control stream and is
  // correlated by the on-wire requestID. The base MoQSession::requestUpdate
  // only targets SUBSCRIBE / FETCH requests, so namespace-style requests need
  // their own send path. Allocates the update's requestID and reuses the base
  // REQUEST_UPDATE response routing (onRequestOk ->
  // handleSubscribeUpdateOkFromRequestOk).
  folly::coro::Task<folly::Expected<RequestOk, RequestError>>
  sendRequestUpdateOnBidi(
      RequestUpdate reqUpdate,
      RequestID existingRequestID,
      std::shared_ptr<BidiStreamControl> control);

  // Internal publishNamespace handling methods
  folly::coro::Task<void> handleSubscribeNamespace(
      SubscribeNamespace sa,
      std::shared_ptr<SubNSReply> subNsReply);
  void subscribeNamespaceOk(
      const SubscribeNamespaceOk& saOk,
      std::shared_ptr<SubNSReply>&& subNsReply);
  void unsubscribeNamespace(const UnsubscribeNamespace& unsubNs);

  // Draft 18+: SUBSCRIBE_TRACKS handling.
  folly::coro::Task<void> handleSubscribeTracks(
      SubscribeTracks subTracks,
      std::shared_ptr<SubscribeTracksReply> subTracksReply);
  void subscribeTracksOk(
      const RequestOk& subTracksOk,
      std::shared_ptr<SubscribeTracksReply>&& subTracksReply);

  folly::coro::Task<void> handlePublishNamespace(
      PublishNamespace publishNamespace,
      std::shared_ptr<ReplyContext> replyContext);
  void publishNamespaceOk(
      const PublishNamespaceOk& pubNsOk,
      ReplyContext& replyContext);
  void publishNamespaceCancel(
      const PublishNamespaceCancel& pubNsCancel,
      std::shared_ptr<ReplyContext> replyContext);
  void publishNamespaceDone(
      const PublishNamespaceDone& publishNamespaceDone,
      std::shared_ptr<ReplyContext> replyCtx);

  // Override all incoming publishNamespace message handlers
  void onPublishNamespace(PublishNamespace pubNs) override;
  void onPublishNamespaceImpl(
      PublishNamespace pubNs,
      std::shared_ptr<ReplyContext> replyContext) override;
  void onPublishNamespaceCancel(
      PublishNamespaceCancel publishNamespaceCancel) override;
  void onPublishNamespaceDone(PublishNamespaceDone pubNsDone) override;
  void onRequestOk(RequestOk ok, FrameType frameType) override;
  void onUnsubscribeNamespace(UnsubscribeNamespace unsub) override;

  // Helper methods for handling RequestOk for different request types
  void handlePublishNamespaceOkFromRequestOk(
      const RequestOk& requestOk,
      PendingRequestIterator reqIt);
  void handleSubscribeNamespaceOkFromRequestOk(
      const RequestOk& requestOk,
      PendingRequestIterator reqIt);
  // Draft 18+
  void handleSubscribeTracksOkFromRequestOk(
      const RequestOk& requestOk,
      PendingRequestIterator reqIt);

  // PublishNamespace-specific types (moved from base class)
  struct PendingPublishNamespace {
    TrackNamespace trackNamespace;
    folly::coro::Promise<
        folly::Expected<PublishNamespaceOk, PublishNamespaceError>>
        promise;
    std::shared_ptr<PublishNamespaceCallback> callback;
  };

  // PublishNamespace state management
  // Primary maps keyed by RequestID
  folly::F14FastMap<
      RequestID,
      std::shared_ptr<Subscriber::PublishNamespaceHandle>,
      RequestID::hash>
      publishNamespaceHandles_;
  folly::F14FastMap<
      RequestID,
      std::shared_ptr<Subscriber::PublishNamespaceCallback>,
      RequestID::hash>
      publishNamespaceCallbacks_;
  folly::F14FastMap<
      RequestID,
      std::shared_ptr<Publisher::SubscribeNamespaceHandle>,
      RequestID::hash>
      subscribeNamespaceHandles_;
  // Draft 18+: reply context for each responder-side namespace or
  // SUBSCRIBE_TRACKS request's bidi stream, so a failed REQUEST_UPDATE can send
  // REQUEST_ERROR and close it.
  folly::F14FastMap<RequestID, std::shared_ptr<ReplyContext>, RequestID::hash>
      requestUpdateReplyContexts_;
  // Draft 18+
  folly::F14FastMap<
      RequestID,
      std::shared_ptr<Publisher::SubscribeTracksHandle>,
      RequestID::hash>
      subscribeTracksHandles_;

  // Legacy TrackNamespace → RequestID translation maps.
  // Remove these once we drop support for the respective legacy versions.
  // legacyPublisherPublishNamespaceNsToReqId_: v15- (publisher side)
  // legacySubscriberPublishNamespaceNsToReqId_: v15- (subscriber side)
  // legacySubscribeNamespaceNsToReqId_: v14- (subscribe namespace)
  folly::F14FastMap<TrackNamespace, RequestID, TrackNamespace::hash>
      legacyPublisherNamespaceToReqId_;
  folly::F14FastMap<TrackNamespace, RequestID, TrackNamespace::hash>
      legacySubscriberNamespaceToReqId_;
  folly::F14FastMap<TrackNamespace, RequestID, TrackNamespace::hash>
      legacySubscribeNamespaceToReqId_;

  // Extended PendingRequestState for publishNamespace support
  class MoQRelayPendingRequestState;
};

} // namespace moxygen

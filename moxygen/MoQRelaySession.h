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

  // Static factory for creating relay sessions in clients
  static std::function<std::shared_ptr<MoQSession>(
      folly::MaybeManagedPtr<proxygen::WebTransport>,
      std::shared_ptr<MoQExecutor>)>
  createRelaySessionFactory();

  // Override cleanup method for proper inheritance pattern
  void cleanup() override;

  // Override publishNamespace methods with real implementations
  folly::coro::Task<Subscriber::PublishNamespaceResult> publishNamespace(
      PublishNamespace ann,
      std::shared_ptr<PublishNamespaceCallback> publishNamespaceCallback =
          nullptr) override;

  folly::coro::Task<Publisher::SubscribeNamespaceResult> subscribeNamespace(
      SubscribeNamespace subAnn,
      std::shared_ptr<NamespacePublishHandle> namespacePublishHandle) override;

  folly::coro::Task<void> subscribeNamespaceSenderReadLoop(
      proxygen::WebTransport::StreamReadHandle* readHandle,
      std::shared_ptr<Publisher::NamespacePublishHandle>
          namespacePublishHandle);

 protected:
  void onSubscribeNamespaceImpl(
      const SubscribeNamespace& subscribeNamespace,
      std::shared_ptr<SubNSReply> subNsReply) override;

  std::shared_ptr<SubNSReply> getSubNsReply(
      std::shared_ptr<ReplyContext> replyContext) override {
    return std::make_shared<SeparateStreamSubNsReply>(
        moqFrameWriter_, std::move(replyContext));
  }

 private:
  // Forward declarations for inner classes
  class SubscriberPublishNamespaceCallback;
  class PublisherPublishNamespaceHandle;
  class SubscribeNamespaceHandle;

  // Override to handle ANNOUNCE and SUBSCRIBE_ANNOUNCES updates
  void onRequestUpdate(RequestUpdate requestUpdate) override;

  // REQUEST_UPDATE handlers for announcement types - take handles directly
  void handlePublishNamespaceRequestUpdate(
      RequestUpdate requestUpdate,
      std::shared_ptr<Subscriber::PublishNamespaceHandle> announceHandle);
  void handleSubscribeNamespaceRequestUpdate(
      RequestUpdate requestUpdate,
      std::shared_ptr<Publisher::SubscribeNamespaceHandle>
          subscribeNamespaceHandle);

  // Internal publishNamespace handling methods
  folly::coro::Task<void> handleSubscribeNamespace(
      SubscribeNamespace sa,
      std::shared_ptr<SubNSReply> subNsReply);
  void subscribeNamespaceOk(
      const SubscribeNamespaceOk& saOk,
      std::shared_ptr<SubNSReply>&& subNsReply);
  void unsubscribeNamespace(const UnsubscribeNamespace& unsubAnn);

  folly::coro::Task<void> handlePublishNamespace(
      PublishNamespace publishNamespace,
      std::shared_ptr<ReplyContext> replyContext);
  void publishNamespaceOk(
      const PublishNamespaceOk& annOk,
      ReplyContext& replyContext);
  void publishNamespaceCancel(const PublishNamespaceCancel& annCan);
  void publishNamespaceDone(const PublishNamespaceDone& publishNamespaceDone);

  // Override all incoming publishNamespace message handlers
  void onPublishNamespace(PublishNamespace ann) override;
  void onPublishNamespaceImpl(
      PublishNamespace ann,
      std::shared_ptr<ReplyContext> replyContext) override;
  void onPublishNamespaceCancel(
      PublishNamespaceCancel publishNamespaceCancel) override;
  void onPublishNamespaceDone(PublishNamespaceDone unAnn) override;
  void onRequestOk(RequestOk ok, FrameType frameType) override;
  void onUnsubscribeNamespace(UnsubscribeNamespace unsub) override;

  // Helper methods for handling RequestOk for different request types
  void handlePublishNamespaceOkFromRequestOk(
      const RequestOk& requestOk,
      PendingRequestIterator reqIt);
  void handleSubscribeNamespaceOkFromRequestOk(
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

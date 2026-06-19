/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <proxygen/lib/http/webtransport/WebTransport.h>
#include <moxygen/MoQCodec.h>
#include <moxygen/events/MoQDeliveryTimer.h>
#include <moxygen/events/MoQExecutor.h>
#include <chrono>

#include <folly/MaybeManagedPtr.h>
#include <folly/container/F14Map.h>
#include <folly/coro/Promise.h>
#include <folly/coro/Task.h>
#include <folly/logging/xlog.h>
#include <moxygen/MoQConsumers.h>
#include <moxygen/Publisher.h>
#include <moxygen/Subscriber.h>
#include <moxygen/stats/MoQStats.h>
#include <memory>
#include <optional>
#include "moxygen/mlog/MLogger.h"
#include "moxygen/util/TimedBaton.h"

namespace moxygen {

class BidiStreamControl;

namespace detail {
class ObjectStreamCallback;
} // namespace detail

struct BufferingThresholds {
  // A value of 0 means no threshold
  uint64_t perSubscription{0};
  uint64_t perSession{0}; // Currently unused
};

struct MoQSettings {
  BufferingThresholds bufferingThresholds{};
  // Timeout for waiting for setup to complete
  std::chrono::milliseconds setupTimeout{std::chrono::seconds(5)};
  // Timeout for waiting for version negotiation to complete
  std::chrono::milliseconds versionNegotiationTimeout{std::chrono::seconds(2)};
  // Timeout for waiting for unknown alias resolution
  std::chrono::milliseconds unknownAliasTimeout{std::chrono::seconds(2)};
  // Timeout for waiting for in-flight streams when PUBLISH_DONE is received
  std::chrono::milliseconds publishDoneStreamCountTimeout{
      std::chrono::seconds(2)};
};

class ReplyContext;

class SubNSReply {
 public:
  SubNSReply(
      MoQFrameWriter& moqFrameWriter,
      std::shared_ptr<ReplyContext> replyContext)
      : moqFrameWriter_(moqFrameWriter),
        replyContext_(std::move(replyContext)) {}

  virtual ~SubNSReply() = default;

  virtual WriteResult ok(const SubscribeNamespaceOk&);
  virtual WriteResult error(const SubscribeNamespaceError&);
  virtual WriteResult namespaceMsg(const Namespace&) {
    XLOG(FATAL) << "Unimplemented";
    folly::assume_unreachable();
  }
  virtual WriteResult namespaceDoneMsg(const NamespaceDone&) {
    XLOG(FATAL) << "Unimplemented";
    folly::assume_unreachable();
  }

 protected:
  MoQFrameWriter& moqFrameWriter_;
  std::shared_ptr<ReplyContext> replyContext_;
};

// Reply type that writes a generic REQUEST_OK / REQUEST_ERROR frame to a
// ReplyContext. Used as the response sink for request/reply messages that
// share the generic REQUEST_OK / REQUEST_ERROR envelope.
class MessageReply {
 public:
  MessageReply(
      MoQFrameWriter& moqFrameWriter,
      std::shared_ptr<ReplyContext> replyContext)
      : moqFrameWriter_(moqFrameWriter),
        replyContext_(std::move(replyContext)) {}

  virtual ~MessageReply() = default;

  virtual WriteResult ok(const RequestOk& okMsg);
  virtual WriteResult error(const SubscribeTracksError& errorMsg);

 protected:
  MoQFrameWriter& moqFrameWriter_;
  std::shared_ptr<ReplyContext> replyContext_;
};

class SubscribeTracksReply : public MessageReply,
                             public Publisher::PublishBlockedHandle {
 public:
  SubscribeTracksReply(
      MoQFrameWriter& moqFrameWriter,
      std::shared_ptr<ReplyContext> replyContext)
      : MessageReply(moqFrameWriter, std::move(replyContext)) {}

  ~SubscribeTracksReply() override = default;

  WriteResult ok(const RequestOk&) override;
  WriteResult error(const SubscribeTracksError&) override;
  void publishBlocked(
      const TrackNamespace& trackNamespaceSuffix,
      const std::string& trackName) override;

 private:
  void flushPendingMessages();

  folly::IOBufQueue pendingBuf_{folly::IOBufQueue::cacheChainLength()};
  bool okSent_{false};
  bool errorSent_{false};
};

class MoQSession : public Subscriber,
                   public Publisher,
                   public MoQControlCodec::ControlCallback,
                   public proxygen::WebTransportHandler,
                   public std::enable_shared_from_this<MoQSession> {
 public:
  struct MoQSessionRequestData : public folly::RequestData {
    explicit MoQSessionRequestData(std::shared_ptr<MoQSession> s)
        : session(std::move(s)) {}
    bool hasCallback() override {
      return false;
    }
    std::shared_ptr<MoQSession> session;
  };

  static std::shared_ptr<MoQSession> getRequestSession();

  void setServerMaxTokenCacheSizeGuess(size_t size);

  class ServerSetupCallback {
   public:
    virtual ~ServerSetupCallback() = default;
    virtual folly::Try<Setup> onClientSetup(
        Setup clientSetup,
        const std::shared_ptr<MoQSession>& session) = 0;

    // Authority validation callback - returns error code if validation fails
    virtual folly::Expected<folly::Unit, SessionCloseErrorCode>
    validateAuthority(
        const Setup& clientSetup,
        uint64_t negotiatedVersion,
        std::shared_ptr<MoQSession> session) = 0;
  };

  class MoQSessionCloseCallback {
   public:
    virtual ~MoQSessionCloseCallback() = default;
    virtual void onMoQSessionClosed(
        SessionCloseErrorCode /* error */,
        folly::Optional<uint32_t> /* wtError */) {
      XLOG(DBG1) << __func__ << " sess=" << this;
    }
  };

  void setSessionCloseCallback(MoQSessionCloseCallback* cb) {
    closeCallback_ = cb;
  }

  bool isClosed() const {
    return closed_;
  }

  void setAuthority(std::string a) {
    authority_ = std::move(a);
  }
  void setPath(std::string p) {
    path_ = std::move(p);
  }
  const std::string& getAuthority() const {
    return authority_;
  }
  const std::string& getPath() const {
    return path_;
  }

  explicit MoQSession(
      folly::MaybeManagedPtr<proxygen::WebTransport> wt,
      std::shared_ptr<MoQExecutor> exec);

  explicit MoQSession(
      folly::MaybeManagedPtr<proxygen::WebTransport> wt,
      ServerSetupCallback& serverSetupCallback,
      std::shared_ptr<MoQExecutor> exec);

  void setVersion(uint64_t version);
  void setMoqSettings(MoQSettings settings);
  // Accessor used by PublisherImpl and others
  const MoQSettings& getMoqSettings() const {
    return moqSettings_;
  }
  void setPublishHandler(std::shared_ptr<Publisher> publishHandler);
  void setSubscribeHandler(std::shared_ptr<Subscriber> subscribeHandler);

  Subscriber::PublishResult publish(
      PublishRequest pub,
      std::shared_ptr<Publisher::SubscriptionHandle> handle = nullptr) override;

  virtual std::optional<uint64_t> getNegotiatedVersion() const {
    return negotiatedVersion_;
  }

  virtual std::shared_ptr<SubNSReply> getSubNsReply(
      std::shared_ptr<ReplyContext> replyContext) {
    return std::make_shared<SubNSReply>(
        moqFrameWriter_, std::move(replyContext));
  }

  virtual std::shared_ptr<SubscribeTracksReply> getSubTracksReply(
      std::shared_ptr<ReplyContext> replyContext) {
    return std::make_shared<SubscribeTracksReply>(
        moqFrameWriter_, std::move(replyContext));
  }

  [[nodiscard]] MoQExecutor* getExecutor() const {
    return exec_.get();
  }

  folly::CancellationToken getCancelToken() const {
    return cancellationSource_.getToken();
  }

  folly::SocketAddress getPeerAddress() const {
    if (wt_) {
      return wt_->getPeerAddress();
    }
    return folly::SocketAddress();
  }

  [[nodiscard]] quic::TransportInfo getTransportInfo() const {
    if (!wt_) {
      return {};
    }

    // Rate limit getTransportInfo calls to at most once per second
    auto now = std::chrono::steady_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        now - lastTransportInfoUpdate_);

    if (elapsed >= std::chrono::seconds(1)) {
      cachedTransportInfo_ = wt_->getTransportInfo();
      lastTransportInfoUpdate_ = now;
    }

    return cachedTransportInfo_;
  }

  ~MoQSession() override;

  void start();
  void drain();
  void close(
      SessionCloseErrorCode error,
      folly::Optional<uint32_t> wtError = folly::none);

  void goaway(Goaway goaway) override;

  folly::coro::Task<Setup> setup(Setup setup);
  folly::Expected<folly::Unit, quic::TransportErrorCode> sendSetup(Setup setup);
  folly::coro::Task<Setup> awaitPeerSetup();

  void setMaxConcurrentRequests(uint64_t maxConcurrent);

  uint64_t maxRequestID() const {
    return maxRequestID_;
  }

  void validateAndSetVersionFromAlpn(const std::string& alpn);

  static GroupOrder resolveGroupOrder(GroupOrder pubOrder, GroupOrder subOrder);

  static std::string getMoQTImplementationString();

  folly::coro::Task<TrackStatusResult> trackStatus(
      TrackStatus trackStatus) override;

  folly::coro::Task<SubscribeResult> subscribe(
      SubscribeRequest sub,
      std::shared_ptr<TrackConsumer> callback) override;

  folly::coro::Task<FetchResult> fetch(
      Fetch fetch,
      std::shared_ptr<FetchConsumer> fetchy) override;

  struct JoinResult {
    SubscribeResult subscribeResult;
    FetchResult fetchResult;
  };
  folly::coro::Task<JoinResult> join(
      SubscribeRequest subscribe,
      std::shared_ptr<TrackConsumer> subscribeCallback,
      uint64_t joiningStart,
      uint8_t fetchPri,
      GroupOrder fetchOrder,
      std::vector<Parameter> fetchParams,
      std::shared_ptr<FetchConsumer> fetchCallback,
      FetchType fetchType);

  void setPublisherStatsCallback(
      std::shared_ptr<MoQPublisherStatsCallback> publisherStatsCallback) {
    publisherStatsCallback_ = publisherStatsCallback;
  }

  void setSubscriberStatsCallback(
      std::shared_ptr<MoQSubscriberStatsCallback> subscriberStatsCallback) {
    subscriberStatsCallback_ = subscriberStatsCallback;
  }

  void onSubscriptionStreamOpenedByPeer() {
    MOQ_SUBSCRIBER_STATS(subscriberStatsCallback_, onSubscriptionStreamOpened);
  }

  void onSubscriptionStreamClosedByPeer() {
    MOQ_SUBSCRIBER_STATS(subscriberStatsCallback_, onSubscriptionStreamClosed);
  }

  void onSubscriptionStreamOpened() {
    MOQ_PUBLISHER_STATS(publisherStatsCallback_, onSubscriptionStreamOpened);
  }

  void onSubscriptionStreamClosed() {
    MOQ_PUBLISHER_STATS(publisherStatsCallback_, onSubscriptionStreamClosed);
  }

  class PublisherImpl : public std::enable_shared_from_this<PublisherImpl> {
   public:
    PublisherImpl(
        MoQSession* session,
        FullTrackName ftn,
        RequestID requestID,
        Priority subPriority,
        GroupOrder groupOrder,
        uint64_t version,
        uint64_t bytesBufferedThreshold)
        : session_(session),
          fullTrackName_(std::move(ftn)),
          requestID_(requestID),
          subPriority_(subPriority),
          groupOrder_(groupOrder),
          version_(version),
          bytesBufferedThreshold_(bytesBufferedThreshold) {
      moqFrameWriter_.initializeVersion(version);
    }

    virtual ~PublisherImpl() = default;

    const FullTrackName& fullTrackName() const {
      return fullTrackName_;
    }
    RequestID requestID() const {
      return requestID_;
    }
    uint8_t subPriority() const {
      return subPriority_;
    }
    void setSubPriority(uint8_t subPriority) {
      subPriority_ = subPriority;
    }
    void setGroupOrder(GroupOrder groupOrder) {
      groupOrder_ = groupOrder;
    }

    GroupOrder getGroupOrder() const {
      return groupOrder_;
    }

    std::optional<uint8_t> getPublisherPriority() const {
      return publisherPriority_;
    }

    void setPublisherPriority(uint8_t priority) {
      publisherPriority_ = priority;
    }

    void setSession(MoQSession* session) {
      session_ = session;
    }

    virtual void terminatePublish(
        PublishDone pubDone,
        ResetStreamErrorCode error = ResetStreamErrorCode::INTERNAL_ERROR) = 0;

    virtual void onStreamCreated() {}

    virtual void onStreamComplete(const ObjectHeader& finalHeader) = 0;

    virtual void onTooManyBytesBuffered() = 0;

    bool canBufferBytes(uint64_t numBytes) {
      if (bytesBufferedThreshold_ == 0) {
        return true;
      }
      return (bytesBuffered_ + numBytes <= bytesBufferedThreshold_);
    }

    void fetchComplete();

    proxygen::WebTransport* getWebTransport() const {
      if (session_) {
        return session_->wt_;
      }
      return nullptr;
    }

    uint64_t getVersion() const {
      return version_;
    }

    void onBytesBuffered(uint64_t amount) {
      bytesBuffered_ += amount;
    }

    void onBytesUnbuffered(uint64_t amount) {
      bytesBuffered_ -= amount;
    }

    const MoQSettings* getMoqSettings() const {
      if (session_) {
        return &session_->getMoqSettings();
      }
      return nullptr;
    }

    std::shared_ptr<MoQExecutor> getExecutor() const {
      return session_ ? session_->exec_ : nullptr;
    }

    quic::TransportInfo getTransportInfo() const {
      return session_ ? session_->getTransportInfo() : quic::TransportInfo();
    }

    void setReplyContext(std::shared_ptr<ReplyContext> ctx) {
      replyContext_ = std::move(ctx);
    }

    ReplyContext* replyContext() const {
      return replyContext_.get();
    }

    // Lets cleanup() disarm the sender close callback before teardown.
    void setBidiControl(std::shared_ptr<BidiStreamControl> control) {
      bidiControl_ = std::move(control);
    }
    const std::shared_ptr<BidiStreamControl>& bidiControl() const {
      return bidiControl_;
    }

   protected:
    MoQSession* session_{nullptr};
    FullTrackName fullTrackName_;
    RequestID requestID_;
    uint8_t subPriority_;
    GroupOrder groupOrder_;
    MoQFrameWriter moqFrameWriter_;
    uint64_t version_;
    uint64_t bytesBuffered_{0};
    uint64_t bytesBufferedThreshold_{0};
    std::optional<uint8_t> publisherPriority_;
    std::shared_ptr<ReplyContext> replyContext_;
    std::shared_ptr<BidiStreamControl> bidiControl_;
  };

  void onNewUniStream(
      proxygen::WebTransport::StreamReadHandle* rh) noexcept override;
  void onNewBidiStream(
      proxygen::WebTransport::BidiStreamHandle bh) noexcept override;

  void handleClientSetup(
      proxygen::WebTransport::BidiStreamHandle bh,
      proxygen::WebTransport::StreamData initialData) noexcept;

  // Used to tease apart the control stream from subscribe namespace streams.
  // This is only called for draft >= 16.
  folly::coro::Task<void> bidiStreamDemuxer(
      proxygen::WebTransport::BidiStreamHandle bh) noexcept;

  struct BidiStreamConfig {
    std::vector<FrameType> allowedFrames;
    folly::Function<void(RequestID)> onPeerTermination;
    // If true, peer FIN fires onPeerTermination (FIN-cancels-the-request, per
    // SUBSCRIBE_NAMESPACE spec). If false, only peer RST fires it.
    bool finIsCancellation{false};
  };
  std::optional<BidiStreamConfig> getBidiStreamConfig(FrameType frameType);

  void onDatagram(std::unique_ptr<folly::IOBuf> datagram) noexcept override;
  void onSessionEnd(folly::Optional<uint32_t> err) noexcept override;
  void onSessionDrain() noexcept override {
    XLOG(DBG1) << __func__ << " sess=" << this;
  }

  class TrackReceiveStateBase;
  class SubscribeTrackReceiveState;
  class FetchTrackReceiveState;
  friend class FetchTrackReceiveState;
  friend class SubNsStreamCallback;
  friend class SubTracksStreamCallback;

  std::shared_ptr<SubscribeTrackReceiveState> getSubscribeTrackReceiveState(
      TrackAlias alias);
  std::shared_ptr<FetchTrackReceiveState> getFetchTrackReceiveState(
      RequestID requestID);

  void setLogger(const std::shared_ptr<MLogger>& logger);
  std::shared_ptr<MLogger> getLogger() const;
  RequestID peekNextRequestID() const {
    return nextRequestID_;
  }

  // Making this public temporarily until we have param management in a single
  // place
  static std::optional<uint64_t> getDeliveryTimeoutIfPresent(
      const TrackRequestParameters& params,
      uint64_t version);

  // Single callback class for all bidi stream request types.
  // Overrides all onXxx methods; the codec's allowedFrames filter ensures
  // only the matching one fires.
  class BidiRequestCallback : public MoQControlCodec::ControlCallback {
   public:
    BidiRequestCallback(
        MoQSession* session,
        std::shared_ptr<BidiStreamControl> control,
        folly::Function<void(RequestID)> onPeerTermination)
        : session_(session),
          control_(std::move(control)),
          onPeerTerminationFn_(std::move(onPeerTermination)) {}

    void onConnectionError(ErrorCode error) override;
    void onRequestUpdate(RequestUpdate requestUpdate) override;
    void onRequestOk(RequestOk requestOk, FrameType frameType) override;
    void onRequestError(RequestError requestError, FrameType frameType)
        override;
    void onSubscribe(SubscribeRequest sub) override;
    void onFetch(Fetch fetch) override;
    void onPublish(PublishRequest pub) override;
    void onPublishDone(PublishDone publishDone) override;
    void onPublishNamespace(PublishNamespace pubNs) override;
    void onTrackStatus(TrackStatus ts) override;
    void onSubscribeNamespace(SubscribeNamespace subNs) override;
    void onSubscribeTracks(SubscribeTracks subTracks) override;

    std::optional<RequestID> requestID() const {
      return requestID_;
    }

   private:
    bool handleFirstFrame(RequestID reqId);

    MoQSession* session_;
    std::shared_ptr<BidiStreamControl> control_;
    folly::Function<void(RequestID)> onPeerTerminationFn_;
    std::optional<RequestID> requestID_;
    std::shared_ptr<ReplyContext> replyContext_;
  };

  // Create a ReplyContext that wraps the given BidiStreamControl (draft 18+)
  // or returns the control stream reply context (null control / legacy).
  std::shared_ptr<ReplyContext> makeReplyContext(
      std::shared_ptr<BidiStreamControl> control);

  // Send a serialized request. In draft >= minBidiDraftVersion, opens a
  // bidi stream and starts a read loop; otherwise appends to the control
  // stream. Returns the control (null on the control-stream path).
  // onPeerTermination fires on peer-initiated close after the terminal reply;
  // early close (before terminal) is handled by failPendingRequestOnEarlyClose.
  // In draft 18+ every response stream's first frame MUST be a terminal: the
  // caller's typed `okType` or `REQUEST_ERROR`. `postTerminal` lists any
  // frames the peer may send after the terminal (e.g. PUBLISH_DONE on a
  // SUBSCRIBE).
  folly::Expected<std::shared_ptr<BidiStreamControl>, std::string> sendRequest(
      folly::IOBufQueue& writeBuf,
      FrameType okType,
      std::vector<FrameType> postTerminal,
      RequestID requestID,
      uint64_t minBidiDraftVersion = 18,
      std::unique_ptr<MoQControlCodec::ControlCallback> senderCallback =
          nullptr,
      folly::Function<void(RequestID)> onPeerTermination = nullptr);

  // Fail a pending sender request when its bidi closes before the terminal
  // reply. No-op if the entry is already gone.
  void failPendingRequestOnEarlyClose(RequestID requestID, bool wasReset);

 private:
  static const folly::RequestToken& sessionRequestToken();

  folly::coro::Task<void> controlWriteLoop(
      proxygen::WebTransport::StreamWriteHandle* writeHandle);

  folly::coro::Task<void> dataStreamReadLoop(
      std::shared_ptr<MoQSession> session,
      proxygen::WebTransport::StreamReadHandle* readHandle,
      proxygen::WebTransport::StreamData initialBufferedData = {
          nullptr,
          false});

  void startControlWriteLoop(
      proxygen::WebTransport::StreamWriteHandle* writeHandle);

  void replayBufferedUniStreams();

  folly::coro::Task<void> handlePreSetupUniStream(
      std::shared_ptr<MoQSession> session,
      proxygen::WebTransport::StreamReadHandle* readHandle);

  class TrackPublisherImpl;
  class FetchPublisherImpl;

  folly::coro::Task<void> handleTrackStatus(
      TrackStatus trackStatus,
      std::shared_ptr<ReplyContext> replyContext);
  void trackStatusOk(
      const TrackStatusOk& trackStatusOk,
      ReplyContext& replyContext);
  void trackStatusError(
      const TrackStatusError& trackStatusError,
      ReplyContext& replyContext);

  folly::coro::Task<void> handleSubscribe(
      SubscribeRequest sub,
      std::shared_ptr<TrackPublisherImpl> trackPublisher,
      std::shared_ptr<ReplyContext> replyContext);
  void sendSubscribeOk(const SubscribeOk& subOk, ReplyContext& replyContext);
  void subscribeError(const SubscribeError& subErr, ReplyContext& replyContext);
  void unsubscribe(
      const Unsubscribe& unsubscribe,
      const std::shared_ptr<BidiStreamControl>& control = nullptr);
  // Backward compatibility forwarders
  void subscribeUpdate(const SubscribeUpdate& subUpdate) {
    requestUpdate(subUpdate);
  }
  void subscribeUpdateOk(const RequestOk& reqOk, RequestID existingReqID) {
    requestUpdateOk(reqOk, existingReqID);
  }
  void subscribeUpdateError(
      const SubscribeUpdateError& reqError,
      RequestID existingReqID) {
    requestUpdateError(reqError, existingReqID);
  }
  void sendPublishDone(const PublishDone& pubDone);

  folly::coro::Task<void> handleFetch(
      Fetch fetch,
      std::shared_ptr<FetchPublisherImpl> fetchPublisher,
      std::shared_ptr<ReplyContext> replyContext);
  void fetchOk(const FetchOk& fetchOk, ReplyContext& replyContext);
  void fetchError(const FetchError& fetchError, ReplyContext& replyContext);
  void fetchCancel(
      const FetchCancel& fetchCancel,
      const std::shared_ptr<BidiStreamControl>& control = nullptr);

  folly::coro::Task<void> handlePublish(
      PublishRequest publish,
      std::shared_ptr<Publisher::SubscriptionHandle> publishHandle,
      std::shared_ptr<ReplyContext> replyContext);
  void publishOk(const PublishOk& pubOk, ReplyContext& replyContext);
  void publishError(
      const PublishError& publishError,
      ReplyContext& replyContext);

  class ReceiverSubscriptionHandle;
  class ReceiverFetchHandle;

  void onClientSetup(Setup clientSetup) override;
  void onServerSetup(Setup setup) override;
  void onSubscribe(SubscribeRequest subscribeRequest) override;
  void onSubscribeOk(SubscribeOk subscribeOk) override;
  void onRequestOk(RequestOk requestOk, FrameType frameType) override;
  void onRequestError(RequestError requestError, FrameType frameType) override;
  void onUnsubscribe(Unsubscribe unsubscribe) override;
  void onPublish(PublishRequest publish) override;
  void onPublishOk(PublishOk publishOk) override;
  void onPublishDone(PublishDone publishDone) override;
  void onMaxRequestID(MaxRequestID maxSubId) override;
  void onRequestsBlocked(RequestsBlocked requestsBlocked) override;
  void onFetch(Fetch fetch) override;
  void onFetchCancel(FetchCancel fetchCancel) override;
  void onFetchOk(FetchOk fetchOk) override;
  void onTrackStatus(TrackStatus trackStatus) override;
  void onTrackStatusOk(TrackStatusOk trackStatusOk) override;
  void onTrackStatusError(TrackStatusError trackStatusError) override;
  void onGoaway(Goaway goaway) override;
  void onConnectionError(ErrorCode error) override;

  // PublishNamespace callback methods - default implementations for simple
  // clients
  void onPublishNamespace(PublishNamespace publishNamespace) override;
  void onPublishNamespaceDone(
      PublishNamespaceDone publishNamespaceDone) override;
  void onPublishNamespaceCancel(
      PublishNamespaceCancel publishNamespaceCancel) override;
  void onSubscribeNamespace(SubscribeNamespace subscribeNamespace) override;
  void onUnsubscribeNamespace(
      UnsubscribeNamespace unsubscribeNamespace) override;
  void removeSubscriptionState(TrackAlias alias, RequestID id);
  void checkForCloseOnDrain();

  void sendMaxRequestID(bool signalWriteLoop);
  void fetchComplete(RequestID requestID);

  // Get the max requestID from the setup params. If MAX_REQUEST_ID key
  // is not present, we default to 0 as specified. 0 means that the peer
  // MUST NOT create any subscriptions
  static uint64_t getMaxRequestIDIfPresent(const SetupParameters& params);
  static uint64_t getMaxAuthTokenCacheSizeIfPresent(
      const SetupParameters& params,
      uint64_t version);
  static std::optional<std::string> getMoQTImplementationIfPresent(
      const SetupParameters& params);
  void setPublisherPriorityFromParams(
      const TrackRequestParameters& params,
      const std::shared_ptr<TrackPublisherImpl>& trackPublisher);
  void setPublisherPriorityFromParams(
      const TrackRequestParameters& params,
      const std::shared_ptr<SubscribeTrackReceiveState>& trackPublisher);

 protected:
  // Protected members and methods for MoQRelaySession subclass access

  // Returns the shared ReplyContext for the control stream
  std::shared_ptr<ReplyContext> controlStreamReplyContext();

  // Impl methods - take ReplyContext so bidi stream callbacks can call them
  void onSubscribeImpl(
      SubscribeRequest subscribeRequest,
      std::shared_ptr<ReplyContext> replyContext);
  void onFetchImpl(Fetch fetch, std::shared_ptr<ReplyContext> replyContext);
  void onTrackStatusImpl(
      TrackStatus trackStatus,
      std::shared_ptr<ReplyContext> replyContext);
  void onPublishImpl(
      PublishRequest publish,
      std::shared_ptr<ReplyContext> replyContext,
      std::shared_ptr<BidiStreamControl> control = nullptr);
  virtual void onPublishNamespaceImpl(
      PublishNamespace publishNamespace,
      std::shared_ptr<ReplyContext> replyContext);

  void requestUpdate(
      const RequestUpdate& reqUpdate,
      const std::shared_ptr<BidiStreamControl>& control = nullptr);

  folly::coro::Task<void> controlReadLoop(
      proxygen::WebTransport::StreamReadHandle* readHandle,
      proxygen::WebTransport::StreamData initialData,
      std::unique_ptr<MoQControlCodec> codec = nullptr,
      std::unique_ptr<BidiRequestCallback> bidiCallback = nullptr,
      std::shared_ptr<BidiStreamControl> control = nullptr,
      std::unique_ptr<MoQControlCodec::ControlCallback> senderCallback =
          nullptr);

  std::unique_ptr<MoQControlCodec> makeBidiCodec(
      MoQControlCodec::ControlCallback* callback,
      std::vector<FrameType> allowedFrames,
      std::optional<RequestID> requestID = std::nullopt,
      std::optional<FrameType> okType = std::nullopt,
      std::deque<RequestID>* responseIDQueue = nullptr);

  // Core session state
  MoQControlCodec::Direction dir_;
  folly::MaybeManagedPtr<proxygen::WebTransport> wt_;
  std::shared_ptr<MoQExecutor> exec_;
  std::shared_ptr<MLogger> logger_ = nullptr;

  // Control channel state
  folly::IOBufQueue controlWriteBuf_{folly::IOBufQueue::cacheChainLength()};
  moxygen::TimedBaton controlWriteEvent_;
  std::shared_ptr<ReplyContext> controlStreamReplyContext_;

  std::unique_ptr<MoQControlCodec> controlCodec_;

  // Track management maps
  folly::F14FastMap<
      TrackAlias,
      std::shared_ptr<SubscribeTrackReceiveState>,
      TrackAlias::hash>
      subTracks_;
  folly::F14FastMap<
      RequestID,
      std::shared_ptr<FetchTrackReceiveState>,
      RequestID::hash>
      fetches_;
  folly::F14FastMap<RequestID, TrackAlias, RequestID::hash> reqIdToTrackAlias_;

  // Protected utility methods
  bool closeSessionIfRequestIDInvalid(
      RequestID requestID,
      bool skipCheck,
      bool isNewRequest,
      bool parityMatters = true);
  bool shouldRejectNewPeerRequestDueToGoaway() const;
  bool shouldFailNewLocalRequestDueToGoaway() const;
  uint8_t getRequestIDMultiplier() const {
    return 2;
  }
  // Initialize request-ID flow control limits. On draft-18+ these are
  // unbounded (the QUIC bidi stream limit governs instead).
  void initLocalMaxRequestID(uint64_t fromParam);
  void initPeerMaxRequestID(const Parameters& peerParams);
  void deliverBufferedData(TrackAlias trackAlias);
  void aliasifyAuthTokens(
      Parameters& params,
      const std::optional<uint64_t>& forceVersion = std::nullopt);
  RequestID getNextRequestID();
  // Resolves joining.joiningRequestID (including the std::nullopt auto-resolve
  // case) and validates the resulting state against fullTrackName.  Sets
  // joining.joiningRequestID to the resolved value when std::nullopt is passed.
  folly::Expected<std::shared_ptr<SubscribeTrackReceiveState>, FetchError>
  resolveJoiningFetch(
      RequestID requestID,
      JoiningFetch& joining,
      const FullTrackName& fullTrackName);
  void setRequestSession() {
    folly::RequestContext::get()->setContextData(
        sessionRequestToken(),
        std::make_unique<MoQSessionRequestData>(shared_from_this()));
  }
  void retireRequestID(bool signalWriteLoop);

  // Virtual cleanup method for proper inheritance pattern (moved from private)
  virtual void cleanup();

  // PublishNamespace response methods - available for responding to
  // incoming publishNamespaces
  void publishNamespaceError(
      const PublishNamespaceError& publishNamespaceError,
      ReplyContext& replyContext);
  void subscribeNamespaceError(
      const SubscribeNamespaceError& subscribeNamespaceError,
      std::shared_ptr<SubNSReply>&& subNsReply);

  void subscribeTracksError(
      const SubscribeTracksError& subscribeTracksError,
      std::shared_ptr<SubscribeTracksReply>&& subTracksReply);

  virtual void onSubscribeNamespaceImpl(
      const SubscribeNamespace& subscribeNamespace,
      std::shared_ptr<SubNSReply> subNsReply);

  virtual void onSubscribeTracksImpl(
      const SubscribeTracks& subscribeTracks,
      std::shared_ptr<SubscribeTracksReply> subTracksReply);

  virtual void onSubscribeTracksStreamClosed(RequestID /*requestID*/) {}

  // REQUEST_UPDATE error response - available for subclass handlers
  void requestUpdateError(
      const SubscribeUpdateError& requestError,
      RequestID existingRequestID,
      bool terminateExistingRequest = true);

  // REQUEST_UPDATE ok response - available for subclass handlers
  void requestUpdateOk(const RequestOk& requestOk, RequestID existingRequestID);

  // Returns the reply context for REQUEST_UPDATE responses: the responder's
  // bidi reply context in draft 18+, otherwise the shared control stream.
  // Returns nullptr only when the request can no longer be found.
  ReplyContext* getRequestUpdateReplyContext(RequestID existingRequestID);

  // REQUEST_UPDATE handler (protected for subclass access)
  void onRequestUpdate(RequestUpdate requestUpdate) override;

  // Type-specific REQUEST_UPDATE handlers
  virtual void handleSubscribeRequestUpdate(
      RequestUpdate requestUpdate,
      std::shared_ptr<TrackPublisherImpl> trackPublisher);
  virtual void handleFetchRequestUpdate(
      const RequestUpdate& requestUpdate,
      const std::shared_ptr<FetchPublisherImpl>& fetchPublisher);

  // Core frame writer and stats callbacks needed by MoQRelaySession
  MoQFrameWriter moqFrameWriter_;
  std::shared_ptr<MoQPublisherStatsCallback> publisherStatsCallback_{nullptr};
  std::shared_ptr<MoQSubscriberStatsCallback> subscriberStatsCallback_{nullptr};

  // Handlers and cancellation needed by MoQRelaySession
  folly::CancellationSource cancellationSource_;
  std::shared_ptr<Subscriber> subscribeHandler_;
  std::shared_ptr<Publisher> publishHandler_;

  // Consolidated pending request state using bespoke discriminated union
  class PendingRequestState {
   public:
    enum class Type : uint8_t {
      SUBSCRIBE_TRACK,
      PUBLISH,
      TRACK_STATUS,
      FETCH,
      REQUEST_UPDATE,
      // PublishNamespace types - only handled by MoQRelaySession subclass
      PUBLISH_NAMESPACE,
      SUBSCRIBE_NAMESPACE,
      SUBSCRIBE_TRACKS // draft 18+
    };

    // Make polymorphic for subclassing - destructor implemented below

   protected:
    Type type_{Type::SUBSCRIBE_TRACK};
    union Storage {
      Storage() {}
      ~Storage() {}

      std::shared_ptr<SubscribeTrackReceiveState> subscribeTrack_;
      folly::coro::Promise<folly::Expected<PublishOk, PublishError>> publish_;
      folly::coro::Promise<folly::Expected<TrackStatusOk, TrackStatusError>>
          trackStatus_;
      std::shared_ptr<FetchTrackReceiveState> fetchTrack_;
      folly::coro::Promise<folly::Expected<RequestOk, RequestError>>
          requestUpdate_;
    } storage_;

   public:
    // Factory methods for type-safe construction returning unique_ptr
    static std::unique_ptr<PendingRequestState> makeSubscribeTrack(
        std::shared_ptr<SubscribeTrackReceiveState> state) {
      auto result = std::make_unique<PendingRequestState>();
      result->type_ = Type::SUBSCRIBE_TRACK;
      new (&result->storage_.subscribeTrack_) auto(std::move(state));
      return result;
    }

    static std::unique_ptr<PendingRequestState> makePublish(
        folly::coro::Promise<folly::Expected<PublishOk, PublishError>>
            promise) {
      auto result = std::make_unique<PendingRequestState>();
      result->type_ = Type::PUBLISH;
      new (&result->storage_.publish_) auto(std::move(promise));
      return result;
    }

    static std::unique_ptr<PendingRequestState> makeTrackStatus(
        folly::coro::Promise<folly::Expected<TrackStatusOk, TrackStatusError>>
            promise) {
      auto result = std::make_unique<PendingRequestState>();
      result->type_ = Type::TRACK_STATUS;
      new (&result->storage_.trackStatus_) auto(std::move(promise));
      return result;
    }

    static std::unique_ptr<PendingRequestState> makeFetch(
        std::shared_ptr<FetchTrackReceiveState> state) {
      auto result = std::make_unique<PendingRequestState>();
      result->type_ = Type::FETCH;
      new (&result->storage_.fetchTrack_) auto(std::move(state));
      return result;
    }

    static std::unique_ptr<PendingRequestState> makeRequestUpdate(
        folly::coro::Promise<folly::Expected<RequestOk, RequestError>>
            promise) {
      auto result = std::make_unique<PendingRequestState>();
      result->type_ = Type::REQUEST_UPDATE;
      new (&result->storage_.requestUpdate_) auto(std::move(promise));
      return result;
    }

    // Delete copy/move operations as this is held in unique_ptr
    PendingRequestState(const PendingRequestState&) = delete;
    PendingRequestState(PendingRequestState&&) = delete;
    PendingRequestState& operator=(const PendingRequestState&) = delete;
    PendingRequestState& operator=(PendingRequestState&&) = delete;

    // Virtual destructor implementation
    virtual ~PendingRequestState() {
      switch (type_) {
        case Type::SUBSCRIBE_TRACK:
          storage_.subscribeTrack_.~shared_ptr();
          break;
        case Type::PUBLISH:
          storage_.publish_.~Promise();
          break;
        case Type::TRACK_STATUS:
          storage_.trackStatus_.~Promise();
          break;
        case Type::FETCH:
          // If FETCH storage is added, destroy it here, e.g.:
          storage_.fetchTrack_.~shared_ptr<FetchTrackReceiveState>();
          break;
        case Type::REQUEST_UPDATE:
          storage_.requestUpdate_.~Promise();
          break;
        case Type::PUBLISH_NAMESPACE:
        case Type::SUBSCRIBE_NAMESPACE:
        case Type::SUBSCRIBE_TRACKS:
          // These types are handled by MoQRelaySession subclass destructor
          break;
      }
    }

    // Duck typing access - overloaded functions for each type
    std::shared_ptr<SubscribeTrackReceiveState>* tryGetSubscribeTrack() {
      return type_ == Type::SUBSCRIBE_TRACK ? &storage_.subscribeTrack_
                                            : nullptr;
    }

    FrameType getFrameType(bool ok) const {
      switch (type_) {
        case Type::SUBSCRIBE_TRACK:
          return ok ? FrameType::SUBSCRIBE_OK : FrameType::SUBSCRIBE_ERROR;
        case Type::PUBLISH:
          return ok ? FrameType::PUBLISH_OK : FrameType::PUBLISH_ERROR;
        case Type::TRACK_STATUS:
          return ok ? FrameType::TRACK_STATUS_OK : FrameType::TRACK_STATUS;
        case Type::FETCH:
          return ok ? FrameType::FETCH_OK : FrameType::FETCH_ERROR;
        case Type::REQUEST_UPDATE:
          return ok ? FrameType::REQUEST_OK : FrameType::REQUEST_ERROR;
        case Type::PUBLISH_NAMESPACE:
          return ok ? FrameType::PUBLISH_NAMESPACE_OK
                    : FrameType::PUBLISH_NAMESPACE_ERROR;
        case Type::SUBSCRIBE_NAMESPACE:
          return ok ? FrameType::SUBSCRIBE_NAMESPACE_OK
                    : FrameType::SUBSCRIBE_NAMESPACE_ERROR;
        case Type::SUBSCRIBE_TRACKS:
          // Draft 18 uses unified REQUEST_OK / REQUEST_ERROR.
          return ok ? FrameType::REQUEST_OK : FrameType::REQUEST_ERROR;
      }
      folly::assume_unreachable();
    }
    FrameType getOkFrameType() const {
      return getFrameType(true);
    }

    FrameType getErrorFrameType() const {
      return getFrameType(false);
    }

    Type type() const {
      return type_;
    }

    virtual folly::Expected<Type, folly::Unit> setError(
        RequestError error,
        FrameType frameType);

    const std::shared_ptr<SubscribeTrackReceiveState>* tryGetSubscribeTrack()
        const {
      return type_ == Type::SUBSCRIBE_TRACK ? &storage_.subscribeTrack_
                                            : nullptr;
    }

    folly::coro::Promise<folly::Expected<PublishOk, PublishError>>*
    tryGetPublish() {
      return type_ == Type::PUBLISH ? &storage_.publish_ : nullptr;
    }

    folly::coro::Promise<folly::Expected<TrackStatusOk, TrackStatusError>>*
    tryGetTrackStatus() {
      return type_ == Type::TRACK_STATUS ? &storage_.trackStatus_ : nullptr;
    }

    std::shared_ptr<FetchTrackReceiveState>* tryGetFetch() {
      return type_ == Type::FETCH ? &storage_.fetchTrack_ : nullptr;
    }

    folly::coro::Promise<folly::Expected<RequestOk, RequestError>>*
    tryGetRequestUpdate() {
      return type_ == Type::REQUEST_UPDATE ? &storage_.requestUpdate_ : nullptr;
    }

    Type getType() const {
      return type_;
    }

    // For SUBSCRIBE_TRACK / FETCH the held track owns the control; set on
    // the track instead — bidiControl() delegates.
    void setBidiControl(std::shared_ptr<BidiStreamControl> control) {
      bidiControl_ = std::move(control);
    }
    const std::shared_ptr<BidiStreamControl>& bidiControl() const;

   public:
    // Default constructor - only use via factory methods
    PendingRequestState() = default;

   private:
    std::shared_ptr<BidiStreamControl> bidiControl_;
  };

  // Pending requests map - declared after PendingRequestState class
  folly::F14FastMap<
      RequestID,
      std::unique_ptr<PendingRequestState>,
      RequestID::hash>
      pendingRequests_;

  // Type alias for pending request iterator
  using PendingRequestIterator = folly::F14FastMap<
      RequestID,
      std::unique_ptr<PendingRequestState>,
      RequestID::hash>::iterator;

  void handleTrackStatusOkFromRequestOk(const RequestOk& requestOk);
  void handlePublishOkFromRequestOk(const RequestOk& requestOk);
  void handleSubscribeUpdateOkFromRequestOk(
      const RequestOk& requestOk,
      PendingRequestIterator reqIt);

  // Draft 18+: Track Properties on REQUEST_OK are only permitted for
  // TRACK_STATUS_OK. For all other shorthands receiving Track Properties is a
  // PROTOCOL_VIOLATION (spec section 10.5). Returns true if the frame is
  // valid; if it returns false the session has already been closed.
  bool validateRequestOkTrackProperties(
      const RequestOk& requestOk,
      FrameType resolvedFrameType);

  // Draft 18+: the delivery-timeout params accept REQUEST_OK at parse time (so
  // PUBLISH_OK, sent as REQUEST_OK, is accepted), but among REQUEST_OK
  // responses they are valid only for PUBLISH_OK. Once the shorthand resolves,
  // reject them for any other response. Returns true if valid; if it returns
  // false the session has already been closed.
  bool validateRequestOkParams(
      const RequestOk& requestOk,
      FrameType resolvedFrameType);

 protected:
  std::optional<uint64_t> negotiatedVersion_;

  // Shared receive-side auth token cache. All codecs that parse auth tokens
  // (control stream, draft16+ SUBSCRIBE_NAMESPACE bidi streams, etc.) point to
  // this cache so that aliases registered on one stream are visible on all
  // others and the total budget is enforced once rather than per-codec.
  MoQTokenCache receiveTokenCache_;

 private:
  class GoawayTimeoutCallback;

  // Private implementation methods
  void initializeNegotiatedVersion(uint64_t negotiatedVersion);
  void removeBufferedSubgroupBaton(TrackAlias alias, TimedBaton* baton);
  void scheduleGoawayTimeout(uint64_t timeoutMs);
  void cancelGoawayTimeout();
  void onGoawayTimeoutExpired();
  bool hasOpenRequestsForDrain() const;
  bool hasOpenRequestsForGoaway() const;

  // Private session state
  folly::F14FastMap<RequestID, std::shared_ptr<PublisherImpl>, RequestID::hash>
      pubTracks_;
  folly::F14FastSet<FullTrackName, FullTrackName::hash> pendingPublishTracks_;
  folly::F14FastSet<FullTrackName, FullTrackName::hash> pendingSubscribeTracks_;
  folly::F14FastMap<TrackAlias, std::list<Payload>, TrackAlias::hash>
      bufferedDatagrams_;
  folly::F14FastMap<TrackAlias, std::list<TimedBaton*>, TrackAlias::hash>
      bufferedSubgroups_;
  std::list<std::shared_ptr<moxygen::TimedBaton>> subgroupsWaitingForVersion_;

  uint64_t closedRequests_{0};
  uint64_t maxConcurrentRequests_{100};
  uint64_t peerMaxRequestID_{0};

  folly::coro::Promise<Setup> setupPromise_;
  folly::coro::Future<Setup> setupFuture_;
  bool setupComplete_{false};
  bool peerControlStreamReceived_{false};
  struct BufferedUniStream {
    proxygen::WebTransport::StreamReadHandle* readHandle;
    proxygen::WebTransport::StreamData initialData;
  };
  std::vector<BufferedUniStream> bufferedPreSetupUniStreams_;
  bool draining_{false};
  bool receivedGoaway_{false};
  std::optional<RequestID> receivedGoawayRequestID_;

  uint64_t nextRequestID_{0};
  uint64_t nextExpectedPeerRequestID_{0};
  uint64_t nextPeerRequestIDForGoaway_{0};
  uint64_t maxRequestID_{0};

  ServerSetupCallback* serverSetupCallback_{nullptr};
  MoQSessionCloseCallback* closeCallback_{nullptr};
  MoQSettings moqSettings_;

  // Send-side auth token cache (for aliasifyAuthTokens).
  MoQTokenCache tokenCache_{1024};

  // Cached transport info to avoid expensive getTransportInfo calls
  mutable quic::TransportInfo cachedTransportInfo_;
  mutable std::chrono::steady_clock::time_point lastTransportInfoUpdate_{};
  std::unique_ptr<GoawayTimeoutCallback> goawayTimeout_;
  bool closed_{false};
  std::string authority_;
  std::string path_;
};
} // namespace moxygen

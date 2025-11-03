/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <proxygen/lib/http/webtransport/WebTransport.h>
#include <moxygen/MoQCodec.h>
#include <moxygen/events/MoQDeliveryTimer.h>
#include <moxygen/events/MoQExecutor.h>

#include <folly/MaybeManagedPtr.h>
#include <folly/Optional.h>
#include <folly/container/F14Map.h>
#include <folly/coro/Promise.h>
#include <folly/coro/Task.h>
#include <folly/logging/xlog.h>
#include <moxygen/MoQConsumers.h>
#include <moxygen/Publisher.h>
#include <moxygen/Subscriber.h>
#include <moxygen/stats/MoQStats.h>
#include <memory>
#include "moxygen/mlog/MLogger.h"
#include "moxygen/util/TimedBaton.h"

namespace moxygen {

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
    virtual folly::Try<ServerSetup> onClientSetup(
        ClientSetup clientSetup,
        const std::shared_ptr<MoQSession>& session) = 0;

    // Authority validation callback - returns error code if validation fails
    virtual folly::Expected<folly::Unit, SessionCloseErrorCode>
    validateAuthority(
        const ClientSetup& clientSetup,
        uint64_t negotiatedVersion,
        std::shared_ptr<MoQSession> session) = 0;
  };

  class MoQSessionCloseCallback {
   public:
    virtual ~MoQSessionCloseCallback() = default;
    virtual void onMoQSessionClosed() {
      XLOG(DBG1) << __func__ << " sess=" << this;
    }
  };

  void setSessionCloseCallback(MoQSessionCloseCallback* cb) {
    closeCallback_ = cb;
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

  folly::Optional<uint64_t> getNegotiatedVersion() const {
    return negotiatedVersion_;
  }

  [[nodiscard]] folly::Executor* getExecutor() const {
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
    if (wt_) {
      return wt_->getTransportInfo();
    }
    return {};
  }

  ~MoQSession() override;

  void start();
  void drain();
  void close(SessionCloseErrorCode error);

  void goaway(Goaway goaway) override;

  folly::coro::Task<ServerSetup> setup(ClientSetup setup);

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
      TrackRequestParameters fetchParams,
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

    folly::Optional<uint8_t> getPublisherPriority() const {
      return publisherPriority_;
    }

    void setPublisherPriority(uint8_t priority) {
      publisherPriority_ = priority;
    }

    void setSession(MoQSession* session) {
      session_ = session;
    }

    virtual void terminatePublish(
        SubscribeDone subDone,
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
    folly::Optional<uint8_t> publisherPriority_;
  };

  void onNewUniStream(proxygen::WebTransport::StreamReadHandle* rh) override;
  void onNewBidiStream(proxygen::WebTransport::BidiStreamHandle bh) override;
  void onDatagram(std::unique_ptr<folly::IOBuf> datagram) override;
  void onSessionEnd(folly::Optional<uint32_t> err) override {
    XLOG(DBG1) << __func__ << "err="
               << (err ? folly::to<std::string>(*err) : std::string("none"))
               << " sess=" << this;
    // The peer closed us, but we can close with NO_ERROR
    close(SessionCloseErrorCode::NO_ERROR);
  }

  class TrackReceiveStateBase;
  class SubscribeTrackReceiveState;
  class FetchTrackReceiveState;
  friend class FetchTrackReceiveState;

  std::shared_ptr<SubscribeTrackReceiveState> getSubscribeTrackReceiveState(
      TrackAlias alias);
  std::shared_ptr<FetchTrackReceiveState> getFetchTrackReceiveState(
      RequestID requestID);

  void setLogger(const std::shared_ptr<MLogger>& logger);
  RequestID peekNextRequestID() const {
    return nextRequestID_;
  }

  // Making this public temporarily until we have param management in a single
  // place
  static folly::Optional<uint64_t> getDeliveryTimeoutIfPresent(
      const TrackRequestParameters& params,
      uint64_t version);

 private:
  static const folly::RequestToken& sessionRequestToken();

  folly::coro::Task<void> controlWriteLoop(
      proxygen::WebTransport::StreamWriteHandle* writeHandle);
  folly::coro::Task<void> controlReadLoop(
      proxygen::WebTransport::StreamReadHandle* readHandle);
  folly::coro::Task<folly::Expected<bool, MoQPublishError>> headerParsed(
      MoQObjectStreamCodec& codec,
      detail::ObjectStreamCallback& callback,
      proxygen::WebTransport::StreamData& streamData);

  folly::coro::Task<void> unidirectionalReadLoop(
      std::shared_ptr<MoQSession> session,
      proxygen::WebTransport::StreamReadHandle* readHandle);

  class TrackPublisherImpl;
  class FetchPublisherImpl;

  folly::coro::Task<void> handleTrackStatus(TrackStatus trackStatus);
  void trackStatusOk(const TrackStatusOk& trackStatusOk);
  void trackStatusError(const TrackStatusError& trackStatusError);

  folly::coro::Task<void> handleSubscribe(
      SubscribeRequest sub,
      std::shared_ptr<TrackPublisherImpl> trackPublisher);
  void sendSubscribeOk(const SubscribeOk& subOk);
  void subscribeError(const SubscribeError& subErr);
  void unsubscribe(const Unsubscribe& unsubscribe);
  void subscribeUpdate(const SubscribeUpdate& subUpdate);
  void sendSubscribeDone(const SubscribeDone& subDone);

  folly::coro::Task<void> handleFetch(
      Fetch fetch,
      std::shared_ptr<FetchPublisherImpl> fetchPublisher);
  void fetchOk(const FetchOk& fetchOk);
  void fetchError(const FetchError& fetchError);
  void fetchCancel(const FetchCancel& fetchCancel);

  folly::coro::Task<void> handlePublish(
      PublishRequest publish,
      std::shared_ptr<Publisher::SubscriptionHandle> publishHandle);
  void publishOk(const PublishOk& pubOk);
  void publishError(const PublishError& publishError);

  class ReceiverSubscriptionHandle;
  class ReceiverFetchHandle;

  void onClientSetup(ClientSetup clientSetup) override;
  void onServerSetup(ServerSetup setup) override;
  void onSubscribe(SubscribeRequest subscribeRequest) override;
  void onSubscribeUpdate(SubscribeUpdate subscribeUpdate) override;
  void onSubscribeOk(SubscribeOk subscribeOk) override;
  void onRequestOk(RequestOk requestOk, FrameType frameType) override;
  void onRequestError(RequestError requestError, FrameType frameType) override;
  void onUnsubscribe(Unsubscribe unsubscribe) override;
  void onPublish(PublishRequest publish) override;
  void onPublishOk(PublishOk publishOk) override;
  void onSubscribeDone(SubscribeDone subscribeDone) override;
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

  // Announcement callback methods - default implementations for simple clients
  void onAnnounce(Announce announce) override;
  void onUnannounce(Unannounce unannounce) override;
  void onAnnounceCancel(AnnounceCancel announceCancel) override;
  void onSubscribeAnnounces(SubscribeAnnounces subscribeAnnounces) override;
  void onUnsubscribeAnnounces(
      UnsubscribeAnnounces unsubscribeAnnounces) override;
  void removeSubscriptionState(TrackAlias alias, RequestID id);
  void checkForCloseOnDrain();

  void sendMaxRequestID(bool signalWriteLoop);
  void fetchComplete(RequestID requestID);

  // Get the max requestID from the setup params. If MAX_REQUEST_ID key
  // is not present, we default to 0 as specified. 0 means that the peer
  // MUST NOT create any subscriptions
  static uint64_t getMaxRequestIDIfPresent(const SetupParameters& params);
  static uint64_t getMaxAuthTokenCacheSizeIfPresent(
      const SetupParameters& params);
  static folly::Optional<std::string> getMoQTImplementationIfPresent(
      const SetupParameters& params);
  static bool shouldIncludeMoqtImplementationParam(
      const std::vector<uint64_t>& supportedVersions);
  void setPublisherPriorityFromParams(
      const TrackRequestParameters& params,
      const std::shared_ptr<TrackPublisherImpl>& trackPublisher);
  void setPublisherPriorityFromParams(
      const TrackRequestParameters& params,
      const std::shared_ptr<SubscribeTrackReceiveState>& trackPublisher);

 protected:
  // Protected members and methods for MoQRelaySession subclass access

  // Core session state
  MoQControlCodec::Direction dir_;
  folly::MaybeManagedPtr<proxygen::WebTransport> wt_;
  std::shared_ptr<MoQExecutor> exec_;
  std::shared_ptr<MLogger> logger_ = nullptr;

  // Control channel state
  folly::IOBufQueue controlWriteBuf_{folly::IOBufQueue::cacheChainLength()};
  moxygen::TimedBaton controlWriteEvent_;

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
  uint8_t getRequestIDMultiplier() const {
    return 2;
  }
  void deliverBufferedData(TrackAlias trackAlias);
  void aliasifyAuthTokens(
      Parameters& params,
      const folly::Optional<uint64_t>& forceVersion = folly::none);
  RequestID getNextRequestID();
  void setRequestSession() {
    folly::RequestContext::get()->setContextData(
        sessionRequestToken(),
        std::make_unique<MoQSessionRequestData>(shared_from_this()));
  }
  void retireRequestID(bool signalWriteLoop);

  // Virtual cleanup method for proper inheritance pattern (moved from private)
  virtual void cleanup();

  // Announcement response methods - available for responding to incoming
  // announcements
  void announceError(const AnnounceError& announceError);
  void subscribeAnnouncesError(
      const SubscribeAnnouncesError& subscribeAnnouncesError);

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
      // Announcement types - only handled by MoQRelaySession subclass
      ANNOUNCE,
      SUBSCRIBE_ANNOUNCES
    };

    // Make polymorphic for subclassing - destructor implemented below

   protected:
    Type type_;
    union Storage {
      Storage() {}
      ~Storage() {}

      std::shared_ptr<SubscribeTrackReceiveState> subscribeTrack_;
      folly::coro::Promise<folly::Expected<PublishOk, PublishError>> publish_;
      folly::coro::Promise<folly::Expected<TrackStatusOk, TrackStatusError>>
          trackStatus_;
      std::shared_ptr<FetchTrackReceiveState> fetchTrack_;
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
        case Type::ANNOUNCE:
        case Type::SUBSCRIBE_ANNOUNCES:
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
        case Type::ANNOUNCE:
          return ok ? FrameType::ANNOUNCE_OK : FrameType::ANNOUNCE_ERROR;
        case Type::SUBSCRIBE_ANNOUNCES:
          return ok ? FrameType::SUBSCRIBE_ANNOUNCES_OK
                    : FrameType::SUBSCRIBE_ANNOUNCES_ERROR;
      }
      folly::assume_unreachable();
    }
    FrameType getOkFrameType() const {
      return getFrameType(true);
    }

    FrameType getErrorFrameType() const {
      return getFrameType(false);
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

    Type getType() const {
      return type_;
    }

   public:
    // Default constructor - only use via factory methods
    PendingRequestState() = default;
  };

  // Pending requests map - declared after PendingRequestState class
  folly::F14FastMap<
      RequestID,
      std::unique_ptr<PendingRequestState>,
      RequestID::hash>
      pendingRequests_;

 private:
  // Private implementation methods
  void initializeNegotiatedVersion(uint64_t negotiatedVersion);

  // Private session state
  folly::F14FastMap<RequestID, std::shared_ptr<PublisherImpl>, RequestID::hash>
      pubTracks_;
  folly::F14FastMap<TrackAlias, std::list<Payload>, TrackAlias::hash>
      bufferedDatagrams_;
  folly::F14FastMap<TrackAlias, std::list<TimedBaton*>, TrackAlias::hash>
      bufferedSubgroups_;
  std::list<std::shared_ptr<moxygen::TimedBaton>> subgroupsWaitingForVersion_;

  uint64_t closedRequests_{0};
  uint64_t maxConcurrentRequests_{100};
  uint64_t peerMaxRequestID_{0};

  folly::coro::Promise<ServerSetup> setupPromise_;
  bool setupComplete_{false};
  bool draining_{false};
  bool receivedGoaway_{false};

  uint64_t nextRequestID_{0};
  uint64_t nextExpectedPeerRequestID_{0};
  uint64_t maxRequestID_{0};

  ServerSetupCallback* serverSetupCallback_{nullptr};
  MoQSessionCloseCallback* closeCallback_{nullptr};
  MoQSettings moqSettings_;

  folly::Optional<uint64_t> negotiatedVersion_;
  MoQControlCodec controlCodec_;
  MoQTokenCache tokenCache_{1024};
};
} // namespace moxygen

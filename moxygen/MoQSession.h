/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <proxygen/lib/http/webtransport/WebTransport.h>
#include <moxygen/MoQCodec.h>
#include <moxygen/events/MoQExecutor.h>

#include <folly/MaybeManagedPtr.h>
#include <folly/container/F14Set.h>
#include <folly/coro/AsyncGenerator.h>
#include <folly/coro/Promise.h>
#include <folly/coro/Task.h>
#include <folly/coro/UnboundedQueue.h>
#include <folly/logging/xlog.h>
#include <moxygen/MoQConsumers.h>
#include <moxygen/Publisher.h>
#include <moxygen/Subscriber.h>
#include <moxygen/stats/MoQStats.h>
#include "moxygen/mlog/MLogger.h"
#include "moxygen/util/TimedBaton.h"

#include <boost/variant.hpp>

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

  static std::shared_ptr<MoQSession> getRequestSession() {
    auto reqData =
        folly::RequestContext::get()->getContextData(sessionRequestToken());
    XCHECK(reqData);
    auto sessionData = dynamic_cast<MoQSessionRequestData*>(reqData);
    XCHECK(sessionData);
    XCHECK(sessionData->session);
    return sessionData->session;
  }

  void setServerMaxTokenCacheSizeGuess(size_t size) {
    if (!setupComplete_ && dir_ == MoQControlCodec::Direction::CLIENT) {
      tokenCache_.setMaxSize(size);
    }
  }

  class ServerSetupCallback {
   public:
    virtual ~ServerSetupCallback() = default;
    virtual folly::Try<ServerSetup> onClientSetup(
        ClientSetup clientSetup,
        std::shared_ptr<MoQSession> session) = 0;
  };

  explicit MoQSession(
      folly::MaybeManagedPtr<proxygen::WebTransport> wt,
      MoQExecutor* exec)
      : dir_(MoQControlCodec::Direction::CLIENT),
        wt_(wt),
        exec_(exec),
        nextRequestID_(0),
        nextExpectedPeerRequestID_(1),
        controlCodec_(dir_, this) {}

  explicit MoQSession(
      folly::MaybeManagedPtr<proxygen::WebTransport> wt,
      ServerSetupCallback& serverSetupCallback,
      MoQExecutor* exec)
      : dir_(MoQControlCodec::Direction::SERVER),
        wt_(wt),
        exec_(exec),
        nextRequestID_(1),
        nextExpectedPeerRequestID_(0),
        serverSetupCallback_(&serverSetupCallback),
        controlCodec_(dir_, this) {}

  void setMoqSettings(MoQSettings settings) {
    moqSettings_ = settings;
  }

  void setPublishHandler(std::shared_ptr<Publisher> publishHandler) {
    publishHandler_ = std::move(publishHandler);
  }

  void setSubscribeHandler(std::shared_ptr<Subscriber> subscribeHandler) {
    subscribeHandler_ = std::move(subscribeHandler);
  }

  Subscriber::PublishResult publish(
      PublishRequest pub,
      std::shared_ptr<SubscriptionHandle> handle = nullptr) override;

  folly::Optional<uint64_t> getNegotiatedVersion() const {
    return negotiatedVersion_;
  }

  [[nodiscard]] folly::Executor* getExecutor() const {
    return exec_;
  }

  folly::CancellationToken getCancelToken() const {
    return cancellationSource_.getToken();
  }

  ~MoQSession() override;

  void start();
  void drain();
  void close(SessionCloseErrorCode error);

  void goaway(Goaway goaway) override;

  folly::coro::Task<ServerSetup> setup(ClientSetup setup);

  void setMaxConcurrentRequests(uint64_t maxConcurrent) {
    if (maxConcurrent > maxConcurrentRequests_) {
      auto delta = maxConcurrent - maxConcurrentRequests_;
      maxRequestID_ += delta;
      sendMaxRequestID(/*signalWriteLoop=*/true);
    }
  }

  uint64_t maxRequestID() const {
    return maxRequestID_;
  }

  static GroupOrder resolveGroupOrder(
      GroupOrder pubOrder,
      GroupOrder subOrder) {
    return subOrder == GroupOrder::Default ? pubOrder : subOrder;
  }

  folly::coro::Task<TrackStatusResult> trackStatus(
      TrackStatusRequest trackStatusRequest) override;

  folly::coro::Task<SubscribeResult> subscribe(
      SubscribeRequest sub,
      std::shared_ptr<TrackConsumer> callback) override;

  folly::coro::Task<FetchResult> fetch(
      Fetch fetch,
      std::shared_ptr<FetchConsumer> fetchy) override;

  folly::coro::Task<Publisher::SubscribeAnnouncesResult> subscribeAnnounces(
      SubscribeAnnounces subAnn) override;

  folly::coro::Task<Subscriber::AnnounceResult> announce(
      Announce ann,
      std::shared_ptr<AnnounceCallback> announceCallback = nullptr) override;

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
      std::vector<TrackRequestParameter> fetchParams,
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

    folly::Expected<folly::Unit, MoQPublishError> subscribeDone(
        SubscribeDone subDone);

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

 private:
  static const folly::RequestToken& sessionRequestToken();
  std::shared_ptr<MLogger> logger_ = nullptr;
  void setRequestSession() {
    folly::RequestContext::get()->setContextData(
        sessionRequestToken(),
        std::make_unique<MoQSessionRequestData>(shared_from_this()));
  }

  void cleanup();

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

  folly::coro::Task<void> handleTrackStatus(TrackStatusRequest trackStatusReq);
  void writeTrackStatus(const TrackStatus& trackStatus);

  folly::coro::Task<void> handleSubscribe(
      SubscribeRequest sub,
      std::shared_ptr<TrackPublisherImpl> trackPublisher);
  std::shared_ptr<TrackConsumer> subscribeOk(const SubscribeOk& subOk);
  void subscribeError(const SubscribeError& subErr);
  void unsubscribe(const Unsubscribe& unsubscribe);
  void subscribeUpdate(const SubscribeUpdate& subUpdate);
  void subscribeDone(const SubscribeDone& subDone);

  folly::coro::Task<void> handleFetch(
      Fetch fetch,
      std::shared_ptr<FetchPublisherImpl> fetchPublisher);
  void fetchOk(const FetchOk& fetchOk);
  void fetchError(const FetchError& fetchError);
  void fetchCancel(const FetchCancel& fetchCancel);

  folly::coro::Task<void> handleSubscribeAnnounces(SubscribeAnnounces sa);
  void subscribeAnnouncesOk(const SubscribeAnnouncesOk& saOk);
  void subscribeAnnouncesError(
      const SubscribeAnnouncesError& subscribeAnnouncesError);
  void unsubscribeAnnounces(const UnsubscribeAnnounces& unsubscribeAnnounces);

  folly::coro::Task<void> handleAnnounce(Announce announce);
  void announceOk(const AnnounceOk& annOk);
  void announceError(const AnnounceError& announceError);
  void announceCancel(const AnnounceCancel& annCan);
  void unannounce(const Unannounce& unannounce);

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
  void onSubscribeError(SubscribeError subscribeError) override;
  void onUnsubscribe(Unsubscribe unsubscribe) override;
  void onPublish(PublishRequest publish) override;
  void onPublishOk(PublishOk publishOk) override;
  void onPublishError(PublishError publishError) override;
  void onSubscribeDone(SubscribeDone subscribeDone) override;
  void onMaxRequestID(MaxRequestID maxSubId) override;
  void onRequestsBlocked(RequestsBlocked requestsBlocked) override;
  void onFetch(Fetch fetch) override;
  void onFetchCancel(FetchCancel fetchCancel) override;
  void onFetchOk(FetchOk fetchOk) override;
  void onFetchError(FetchError fetchError) override;
  void onAnnounce(Announce announce) override;
  void onAnnounceOk(AnnounceOk announceOk) override;
  void onAnnounceError(AnnounceError announceError) override;
  void onUnannounce(Unannounce unannounce) override;
  void onAnnounceCancel(AnnounceCancel announceCancel) override;
  void onSubscribeAnnounces(SubscribeAnnounces subscribeAnnounces) override;
  void onSubscribeAnnouncesOk(
      SubscribeAnnouncesOk subscribeAnnouncesOk) override;
  void onSubscribeAnnouncesError(
      SubscribeAnnouncesError announceError) override;
  void onUnsubscribeAnnounces(
      UnsubscribeAnnounces unsubscribeAnnounces) override;
  void onTrackStatusRequest(TrackStatusRequest trackStatusRequest) override;
  void onTrackStatus(TrackStatus trackStatus) override;
  void onGoaway(Goaway goaway) override;
  void onConnectionError(ErrorCode error) override;
  void removeSubscriptionState(TrackAlias alias, RequestID id);
  void checkForCloseOnDrain();

  void retireRequestID(bool signalWriteLoop);
  void sendMaxRequestID(bool signalWriteLoop);
  void fetchComplete(RequestID requestID);

  // Get the max requestID from the setup params. If MAX_REQUEST_ID key
  // is not present, we default to 0 as specified. 0 means that the peer
  // MUST NOT create any subscriptions
  static uint64_t getMaxRequestIDIfPresent(
      const std::vector<SetupParameter>& params);
  static uint64_t getMaxAuthTokenCacheSizeIfPresent(
      const std::vector<SetupParameter>& params);

  //  Closes the session if the requestID is invalid, that is,
  //  requestID <= maxRequestID_;
  bool closeSessionIfRequestIDInvalid(
      RequestID requestID,
      bool skipCheck,
      bool isNewRequest,
      bool parityMatters = true);

  void initializeNegotiatedVersion(uint64_t negotiatedVersion);
  void aliasifyAuthTokens(
      std::vector<Parameter>& params,
      const folly::Optional<uint64_t>& forceVersion = folly::none);
  RequestID getRequestID(RequestID id, const FullTrackName& ftn);
  RequestID getNextRequestID(bool legacyAction = false);
  void maybeAddLegacyRequestIDMapping(const FullTrackName& ftn, RequestID id);
  uint8_t getRequestIDMultiplier() const {
    return (getDraftMajorVersion(*getNegotiatedVersion()) >= 11) ? 2 : 1;
  }

  MoQControlCodec::Direction dir_;
  folly::MaybeManagedPtr<proxygen::WebTransport> wt_;
  MoQExecutor* exec_{nullptr};
  folly::IOBufQueue controlWriteBuf_{folly::IOBufQueue::cacheChainLength()};
  moxygen::TimedBaton controlWriteEvent_;

  // Track Alias -> Receive State
  folly::F14FastMap<
      TrackAlias,
      std::shared_ptr<SubscribeTrackReceiveState>,
      TrackAlias::hash>
      subTracks_;
  folly::F14FastMap<
      RequestID,
      std::shared_ptr<SubscribeTrackReceiveState>,
      RequestID::hash>
      pendingSubscribeTracks_;

  folly::F14FastMap<
      RequestID,
      std::shared_ptr<FetchTrackReceiveState>,
      RequestID::hash>
      fetches_;
  folly::F14FastMap<RequestID, TrackAlias, RequestID::hash> reqIdToTrackAlias_;

  struct PendingAnnounce {
    TrackNamespace trackNamespace;
    folly::coro::Promise<folly::Expected<AnnounceOk, AnnounceError>> promise;
    std::shared_ptr<AnnounceCallback> callback;
  };

  // Publisher State
  // Track Namespace -> Promise<AnnounceOK>
  folly::F14FastMap<RequestID, PendingAnnounce, RequestID::hash>
      pendingAnnounce_;

  folly::F14FastMap<
      RequestID,
      folly::coro::Promise<
          folly::Expected<SubscribeAnnouncesOk, SubscribeAnnouncesError>>,
      RequestID::hash>
      pendingSubscribeAnnounces_;

  folly::F14FastMap<
      RequestID,
      folly::coro::Promise<folly::Expected<PublishOk, PublishError>>,
      RequestID::hash>
      pendingPublish_;

  // Track Status
  folly::
      F14FastMap<RequestID, folly::coro::Promise<TrackStatus>, RequestID::hash>
          trackStatuses_;

  // For <= v10
  folly::F14FastMap<FullTrackName, RequestID, FullTrackName::hash>
      fullTrackNameToRequestID_;

  // Subscriber ID -> metadata about a publish track
  folly::F14FastMap<RequestID, std::shared_ptr<PublisherImpl>, RequestID::hash>
      pubTracks_;

  class SubscriberAnnounceCallback;
  class PublisherAnnounceHandle;
  class SubscribeAnnouncesHandle;
  folly::F14FastMap<
      TrackNamespace,
      std::shared_ptr<Subscriber::AnnounceHandle>,
      TrackNamespace::hash>
      subscriberAnnounces_;
  folly::F14FastMap<
      TrackNamespace,
      std::shared_ptr<Subscriber::AnnounceCallback>,
      TrackNamespace::hash>
      publisherAnnounces_;
  folly::F14FastMap<
      TrackNamespace,
      std::shared_ptr<Publisher::SubscribeAnnouncesHandle>,
      TrackNamespace::hash>
      subscribeAnnounces_;
  folly::F14FastMap<TrackAlias, std::list<Payload>, TrackAlias::hash>
      bufferedDatagrams_;
  folly::F14FastMap<TrackAlias, std::list<TimedBaton*>, TrackAlias::hash>
      bufferedSubgroups_;
  // TODO: The reason we have multiple TimedBatons here is that the TimedBaton
  // doesn't support multiple waiters. Would be better to make a primitive that
  // supports multiple waiters. A "TimedBarrier"?
  std::list<std::shared_ptr<moxygen::TimedBaton>> subgroupsWaitingForVersion_;

  uint64_t closedRequests_{0};
  // TODO: Make this value configurable. maxConcurrentRequests_ represents
  // the maximum number of concurrent subscriptions to a given sessions, set
  // to the initial MAX_REQUEST_ID
  uint64_t maxConcurrentRequests_{100};
  uint64_t peerMaxRequestID_{0};

  folly::coro::Promise<ServerSetup> setupPromise_;
  bool setupComplete_{false};
  bool draining_{false};
  bool receivedGoaway_{false};
  folly::CancellationSource cancellationSource_;

  // RequestID must be a unique monotonically increasing number that is
  // less than maxRequestID.
  uint64_t nextRequestID_{0};
  uint64_t nextExpectedPeerRequestID_{0};
  // For request IDs for messages that didn't use subscribe ID in v<11
  uint64_t legacyNextRequestID_{quic::kEightByteLimit + 1};
  uint64_t maxRequestID_{0};

  ServerSetupCallback* serverSetupCallback_{nullptr};
  MoQSettings moqSettings_;
  std::shared_ptr<Publisher> publishHandler_;
  std::shared_ptr<Subscriber> subscribeHandler_;

  std::shared_ptr<MoQPublisherStatsCallback> publisherStatsCallback_{nullptr};
  std::shared_ptr<MoQSubscriberStatsCallback> subscriberStatsCallback_{nullptr};

  MoQFrameWriter moqFrameWriter_;
  folly::Optional<uint64_t> negotiatedVersion_{0};
  MoQControlCodec controlCodec_;
  MoQTokenCache tokenCache_{1024}; // sending tokens
};
} // namespace moxygen

/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <proxygen/lib/http/webtransport/WebTransport.h>
#include "moxygen/MoQCodec.h"

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
#include "moxygen/util/TimedBaton.h"

#include <boost/variant.hpp>

namespace moxygen {

class MoQSession : public MoQControlCodec::ControlCallback,
                   public proxygen::WebTransportHandler,
                   public Publisher,
                   public Subscriber,
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

  class ServerSetupCallback {
   public:
    virtual ~ServerSetupCallback() = default;
    virtual folly::Try<ServerSetup> onClientSetup(ClientSetup clientSetup) = 0;
  };

  explicit MoQSession(
      folly::MaybeManagedPtr<proxygen::WebTransport> wt,
      folly::EventBase* evb)
      : dir_(MoQControlCodec::Direction::CLIENT), wt_(wt), evb_(evb) {}

  explicit MoQSession(
      folly::MaybeManagedPtr<proxygen::WebTransport> wt,
      ServerSetupCallback& serverSetupCallback,
      folly::EventBase* evb)
      : dir_(MoQControlCodec::Direction::SERVER),
        wt_(wt),
        evb_(evb),
        serverSetupCallback_(&serverSetupCallback) {}

  void setPublishHandler(std::shared_ptr<Publisher> publishHandler) {
    publishHandler_ = std::move(publishHandler);
  }

  void setSubscribeHandler(std::shared_ptr<Subscriber> subscribeHandler) {
    subscribeHandler_ = std::move(subscribeHandler);
  }

  [[nodiscard]] folly::EventBase* getEventBase() const {
    return evb_;
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

  void setMaxConcurrentSubscribes(uint64_t maxConcurrent) {
    if (maxConcurrent > maxConcurrentSubscribes_) {
      auto delta = maxConcurrent - maxConcurrentSubscribes_;
      maxSubscribeID_ += delta;
      sendMaxSubscribeID(/*signalWriteLoop=*/true);
    }
  }

  uint64_t maxSubscribeID() const {
    return maxSubscribeID_;
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

  void setPublisherStatsCallback(
      std::shared_ptr<MoQPublisherStatsCallback> publisherStatsCallback) {
    publisherStatsCallback_ = publisherStatsCallback;
  }

  void setSubscriberStatsCallback(
      std::shared_ptr<MoQSubscriberStatsCallback> subscriberStatsCallback) {
    subscriberStatsCallback_ = subscriberStatsCallback;
  }

  class PublisherImpl {
   public:
    PublisherImpl(
        MoQSession* session,
        SubscribeID subscribeID,
        Priority subPriority,
        GroupOrder groupOrder)
        : session_(session),
          subscribeID_(subscribeID),
          subPriority_(subPriority),
          groupOrder_(groupOrder) {}
    virtual ~PublisherImpl() = default;

    SubscribeID subscribeID() const {
      return subscribeID_;
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

    virtual void reset(ResetStreamErrorCode error) = 0;

    virtual void onStreamCreated() {}

    virtual void onStreamComplete(const ObjectHeader& finalHeader) = 0;

    folly::Expected<folly::Unit, MoQPublishError> subscribeDone(
        SubscribeDone subDone);

    void fetchComplete();

    proxygen::WebTransport* getWebTransport() const {
      if (session_) {
        return session_->wt_;
      }
      return nullptr;
    }

   protected:
    MoQSession* session_{nullptr};
    SubscribeID subscribeID_;
    uint8_t subPriority_;
    GroupOrder groupOrder_;
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
      TrackAlias trackAlias);
  std::shared_ptr<FetchTrackReceiveState> getFetchTrackReceiveState(
      SubscribeID subscribeID);

 private:
  static const folly::RequestToken& sessionRequestToken();

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

  class ReceiverSubscriptionHandle;
  class ReceiverFetchHandle;

  void onClientSetup(ClientSetup clientSetup) override;
  void onServerSetup(ServerSetup setup) override;
  void onSubscribe(SubscribeRequest subscribeRequest) override;
  void onSubscribeUpdate(SubscribeUpdate subscribeUpdate) override;
  void onSubscribeOk(SubscribeOk subscribeOk) override;
  void onSubscribeError(SubscribeError subscribeError) override;
  void onUnsubscribe(Unsubscribe unsubscribe) override;
  void onSubscribeDone(SubscribeDone subscribeDone) override;
  void onMaxSubscribeId(MaxSubscribeId maxSubId) override;
  void onSubscribesBlocked(SubscribesBlocked subscribesBlocked) override;
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
  void removeSubscriptionState(TrackAlias alias, SubscribeID id);
  void checkForCloseOnDrain();

  void retireSubscribeId(bool signalWriteLoop);
  void sendMaxSubscribeID(bool signalWriteLoop);
  void fetchComplete(SubscribeID subscribeID);

  // Get the max subscribe id from the setup params. If MAX_SUBSCRIBE_ID key is
  // not present, we default to 0 as specified. 0 means that the peer MUST NOT
  // create any subscriptions
  static uint64_t getMaxSubscribeIdIfPresent(
      const std::vector<SetupParameter>& params);

  //  Closes the session if the subscribeID is invalid, that is,
  //  subscribeID <= maxSubscribeID_;
  //  TODO: Add this to all messages that have subscribeId
  bool closeSessionIfSubscribeIdInvalid(SubscribeID subscribeID);

  MoQControlCodec::Direction dir_;
  folly::MaybeManagedPtr<proxygen::WebTransport> wt_;
  folly::EventBase* evb_{nullptr}; // keepalive?
  folly::IOBufQueue controlWriteBuf_{folly::IOBufQueue::cacheChainLength()};
  moxygen::TimedBaton controlWriteEvent_;

  // Track Alias -> Receive State
  folly::F14FastMap<
      TrackAlias,
      std::shared_ptr<SubscribeTrackReceiveState>,
      TrackAlias::hash>
      subTracks_;
  folly::F14FastMap<
      SubscribeID,
      std::shared_ptr<FetchTrackReceiveState>,
      SubscribeID::hash>
      fetches_;
  folly::F14FastMap<SubscribeID, TrackAlias, SubscribeID::hash>
      subIdToTrackAlias_;

  // Publisher State
  // Track Namespace -> Promise<AnnounceOK>
  folly::F14FastMap<
      TrackNamespace,
      folly::coro::Promise<folly::Expected<AnnounceOk, AnnounceError>>,
      TrackNamespace::hash>
      pendingAnnounce_;

  folly::F14FastMap<
      TrackNamespace,
      folly::coro::Promise<
          folly::Expected<SubscribeAnnouncesOk, SubscribeAnnouncesError>>,
      TrackNamespace::hash>
      pendingSubscribeAnnounces_;

  // Track Status
  folly::F14FastMap<
      FullTrackName,
      folly::coro::Promise<TrackStatus>,
      FullTrackName::hash>
      trackStatuses_;

  // Subscriber ID -> metadata about a publish track
  folly::
      F14FastMap<SubscribeID, std::shared_ptr<PublisherImpl>, SubscribeID::hash>
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

  uint64_t nextTrackId_{0};
  uint64_t closedSubscribes_{0};
  // TODO: Make this value configurable. maxConcurrentSubscribes_ represents
  // the maximum number of concurrent subscriptions to a given sessions, set
  // to the initial MAX_SUBSCRIBE_ID
  uint64_t maxConcurrentSubscribes_{100};
  uint64_t peerMaxSubscribeID_{0};

  folly::coro::Promise<ServerSetup> setupPromise_;
  bool setupComplete_{false};
  bool draining_{false};
  bool receivedGoaway_{false};
  folly::CancellationSource cancellationSource_;

  // SubscribeID must be a unique monotonically increasing number that is
  // less than maxSubscribeID.
  uint64_t nextSubscribeID_{0};
  uint64_t maxSubscribeID_{0};

  ServerSetupCallback* serverSetupCallback_{nullptr};
  std::shared_ptr<Publisher> publishHandler_;
  std::shared_ptr<Subscriber> subscribeHandler_;

  std::shared_ptr<MoQPublisherStatsCallback> publisherStatsCallback_{nullptr};
  std::shared_ptr<MoQSubscriberStatsCallback> subscriberStatsCallback_{nullptr};
};
} // namespace moxygen

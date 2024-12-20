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
#include "moxygen/util/TimedBaton.h"

#include <boost/variant.hpp>

namespace moxygen {

class MoQSession : public MoQControlCodec::ControlCallback,
                   public proxygen::WebTransportHandler,
                   public std::enable_shared_from_this<MoQSession> {
 private:
  class TrackReceiveState;

 public:
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
        serverSetupCallback_(&serverSetupCallback) {
    // SERVER sessions use this promise/future as a signal
    std::tie(setupPromise_, setupFuture_) =
        folly::coro::makePromiseContract<ServerSetup>();
  }

  [[nodiscard]] folly::EventBase* getEventBase() const {
    return evb_;
  }

  ~MoQSession() override;

  void start();
  void drain();
  void close(folly::Optional<SessionCloseErrorCode> error = folly::none);

  folly::coro::Task<ServerSetup> setup(ClientSetup setup);
  folly::coro::Task<void> clientSetupComplete() {
    XCHECK(dir_ == MoQControlCodec::Direction::SERVER);
    // TODO timeout
    co_await std::move(setupFuture_);
  }

  void setMaxConcurrentSubscribes(uint64_t maxConcurrent) {
    if (maxConcurrent > maxConcurrentSubscribes_) {
      auto delta = maxConcurrent - maxConcurrentSubscribes_;
      maxSubscribeID_ += delta;
      sendMaxSubscribeID(/*signalWriteLoop=*/true);
    }
  }

  using MoQMessage = boost::variant<
      Announce,
      Unannounce,
      AnnounceCancel,
      SubscribeAnnounces,
      UnsubscribeAnnounces,
      SubscribeRequest,
      SubscribeUpdate,
      Unsubscribe,
      Fetch,
      TrackStatusRequest,
      TrackStatus,
      Goaway>;

  class ControlVisitor : public boost::static_visitor<> {
   public:
    ControlVisitor() = default;
    virtual ~ControlVisitor() = default;
    virtual void operator()(ClientSetup /*setup*/) const {
      XLOG(INFO) << "ClientSetup";
    }

    virtual void operator()(Announce announce) const {
      XLOG(INFO) << "Announce ns=" << announce.trackNamespace;
    }

    virtual void operator()(Unannounce unannounce) const {
      XLOG(INFO) << "Unannounce ns=" << unannounce.trackNamespace;
    }

    virtual void operator()(AnnounceCancel announceCancel) const {
      XLOG(INFO) << "AnnounceCancel ns=" << announceCancel.trackNamespace;
    }

    virtual void operator()(SubscribeAnnounces subscribeAnnounces) const {
      XLOG(INFO) << "subscribeAnnounces nsp="
                 << subscribeAnnounces.trackNamespacePrefix;
    }

    virtual void operator()(UnsubscribeAnnounces unsubscribeAnnounces) const {
      XLOG(INFO) << "UnsubscribeAnnounces nsp="
                 << unsubscribeAnnounces.trackNamespacePrefix;
    }

    virtual void operator()(AnnounceError announceError) const {
      XLOG(INFO) << "AnnounceError ns=" << announceError.trackNamespace
                 << " code=" << announceError.errorCode
                 << " reason=" << announceError.reasonPhrase;
    }

    virtual void operator()(SubscribeRequest subscribe) const {
      XLOG(INFO) << "Subscribe ftn=" << subscribe.fullTrackName.trackNamespace
                 << subscribe.fullTrackName.trackName;
    }

    virtual void operator()(SubscribeUpdate subscribeUpdate) const {
      XLOG(INFO) << "SubscribeUpdate subID=" << subscribeUpdate.subscribeID;
    }

    virtual void operator()(Unsubscribe unsubscribe) const {
      XLOG(INFO) << "Unsubscribe subID=" << unsubscribe.subscribeID;
    }
    virtual void operator()(Fetch fetch) const {
      XLOG(INFO) << "Fetch subID=" << fetch.subscribeID;
    }
    virtual void operator()(TrackStatusRequest trackStatusRequest) const {
      XLOG(INFO) << "Subscribe ftn="
                 << trackStatusRequest.fullTrackName.trackNamespace
                 << trackStatusRequest.fullTrackName.trackName;
    }
    virtual void operator()(TrackStatus trackStatus) const {
      XLOG(INFO) << "Subscribe ftn=" << trackStatus.fullTrackName.trackNamespace
                 << trackStatus.fullTrackName.trackName;
    }
    virtual void operator()(Goaway goaway) const {
      XLOG(INFO) << "Goaway, newURI=" << goaway.newSessionUri;
    }

   private:
  };

  folly::coro::AsyncGenerator<MoQMessage> controlMessages();

  folly::coro::Task<folly::Expected<AnnounceOk, AnnounceError>> announce(
      Announce ann);
  void announceOk(AnnounceOk annOk);
  void announceError(AnnounceError announceError);
  void unannounce(Unannounce unannounce);

  folly::coro::Task<
      folly::Expected<SubscribeAnnouncesOk, SubscribeAnnouncesError>>
  subscribeAnnounces(SubscribeAnnounces ann);
  void subscribeAnnouncesOk(SubscribeAnnouncesOk annOk);
  void subscribeAnnouncesError(SubscribeAnnouncesError subscribeAnnouncesError);
  void unsubscribeAnnounces(UnsubscribeAnnounces unsubscribeAnnounces);

  uint64_t maxSubscribeID() const {
    return maxSubscribeID_;
  }

  static GroupOrder resolveGroupOrder(
      GroupOrder pubOrder,
      GroupOrder subOrder) {
    return subOrder == GroupOrder::Default ? pubOrder : subOrder;
  }

  using SubscribeResult = folly::Expected<SubscribeOk, SubscribeError>;
  folly::coro::Task<SubscribeResult> subscribe(
      SubscribeRequest sub,
      std::shared_ptr<TrackConsumer> callback);
  std::shared_ptr<TrackConsumer> subscribeOk(SubscribeOk subOk);
  void subscribeError(SubscribeError subErr);
  void unsubscribe(Unsubscribe unsubscribe);
  void subscribeUpdate(SubscribeUpdate subUpdate);

  folly::coro::Task<folly::Expected<SubscribeID, FetchError>> fetch(
      Fetch fetch,
      std::shared_ptr<FetchConsumer> fetchCallback);
  std::shared_ptr<FetchConsumer> fetchOk(FetchOk fetchOk);
  void fetchError(FetchError fetchError);
  void fetchCancel(FetchCancel fetchCancel);

  class PublisherImpl {
   public:
    PublisherImpl(
        MoQSession* session,
        SubscribeID subscribeID,
        Priority priority,
        GroupOrder groupOrder)
        : session_(session),
          subscribeID_(subscribeID),
          priority_(priority),
          groupOrder_(groupOrder) {}
    virtual ~PublisherImpl() = default;

    SubscribeID subscribeID() const {
      return subscribeID_;
    }
    uint8_t priority() const {
      return priority_;
    }
    void setPriority(uint8_t priority) {
      priority_ = priority;
    }
    void setGroupOrder(GroupOrder groupOrder) {
      groupOrder_ = groupOrder;
    }

    virtual void reset(ResetStreamErrorCode error) = 0;

    virtual void onStreamComplete(const ObjectHeader& finalHeader) = 0;

    folly::Expected<folly::Unit, MoQPublishError> subscribeDone(
        SubscribeDone subDone);

    void fetchComplete();

   protected:
    proxygen::WebTransport* getWebTransport() const {
      if (session_) {
        return session_->wt_;
      }
      return nullptr;
    }

    MoQSession* session_{nullptr};
    SubscribeID subscribeID_;
    uint8_t priority_;
    GroupOrder groupOrder_;
  };

  void onNewUniStream(proxygen::WebTransport::StreamReadHandle* rh) override;
  void onNewBidiStream(proxygen::WebTransport::BidiStreamHandle bh) override;
  void onDatagram(std::unique_ptr<folly::IOBuf> datagram) override;
  void onSessionEnd(folly::Optional<uint32_t>) override {
    XLOG(DBG1) << __func__ << " sess=" << this;
    close();
  }

  // The following wrapper classes allow implementation details in the anonymous
  // namespace can use parts of TrackReceiveState without making the entire
  // class public.
  class CallbackBase {
   public:
    CallbackBase() = default;
    explicit CallbackBase(std::shared_ptr<TrackReceiveState> trackReceiveState)
        : trackReceiveState_(std::move(trackReceiveState)) {}
    operator bool() const {
      return bool(trackReceiveState_);
    }
    folly::CancellationToken getCancelToken() const;

   protected:
    std::shared_ptr<TrackReceiveState> trackReceiveState_;
  };

  class SubscribeCallback : public CallbackBase {
   public:
    SubscribeCallback() = default;
    explicit SubscribeCallback(
        std::shared_ptr<TrackReceiveState> trackReceiveState)
        : CallbackBase(std::move(trackReceiveState)) {}
    std::shared_ptr<TrackConsumer> get() const;
    void reset();
  };

  class FetchCallback : public CallbackBase {
   public:
    FetchCallback() = default;
    explicit FetchCallback(std::shared_ptr<TrackReceiveState> trackReceiveState)
        : CallbackBase(std::move(trackReceiveState)) {}
    std::shared_ptr<FetchConsumer> get() const;
    void reset();
  };

  SubscribeCallback getSubscribeCallback(TrackAlias trackAlias);
  FetchCallback getFetchCallback(SubscribeID subscribeID);

 private:
  void cleanup();

  folly::coro::Task<void> controlWriteLoop(
      proxygen::WebTransport::StreamWriteHandle* writeHandle);
  folly::coro::Task<void> controlReadLoop(
      proxygen::WebTransport::StreamReadHandle* readHandle);
  folly::coro::Task<void> unidirectionalReadLoop(
      std::shared_ptr<MoQSession> session,
      proxygen::WebTransport::StreamReadHandle* readHandle);

  void subscribeDone(SubscribeDone subDone);

  void onClientSetup(ClientSetup clientSetup) override;
  void onServerSetup(ServerSetup setup) override;
  void onSubscribe(SubscribeRequest subscribeRequest) override;
  void onSubscribeUpdate(SubscribeUpdate subscribeUpdate) override;
  void onSubscribeOk(SubscribeOk subscribeOk) override;
  void onSubscribeError(SubscribeError subscribeError) override;
  void onUnsubscribe(Unsubscribe unsubscribe) override;
  void onSubscribeDone(SubscribeDone subscribeDone) override;
  void onMaxSubscribeId(MaxSubscribeId maxSubId) override;
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
  folly::coro::UnboundedQueue<MoQMessage, true, true> controlMessages_;

  // Subscriber State
  class TrackReceiveState {
   public:
    TrackReceiveState(
        FullTrackName fullTrackName,
        SubscribeID subscribeID,
        std::shared_ptr<TrackConsumer> callback,
        std::shared_ptr<FetchConsumer> fetchCallback)
        : callback_(std::move(callback)),
          fetchCallback_(std::move(fetchCallback)),
          fullTrackName_(std::move(fullTrackName)),
          subscribeID_(subscribeID) {
      auto contract = folly::coro::makePromiseContract<SubscribeResult>();
      promise_ = std::move(contract.first);
      future_ = std::move(contract.second);
      auto contract2 = folly::coro::makePromiseContract<
          folly::Expected<SubscribeID, FetchError>>();
      fetchPromise_ = std::move(contract2.first);
      fetchFuture_ = std::move(contract2.second);
    }

    void setTrackName(FullTrackName trackName) {
      fullTrackName_ = std::move(trackName);
    }

    [[nodiscard]] const FullTrackName& fullTrackName() const {
      return fullTrackName_;
    }

    [[nodiscard]] SubscribeID subscribeID() const {
      return subscribeID_;
    }

    void setNewObjectTimeout(std::chrono::milliseconds objectTimeout) {
      objectTimeout_ = objectTimeout;
    }

    folly::CancellationToken getCancelToken() {
      return cancelSource_.getToken();
    }

    folly::coro::Task<SubscribeResult> ready() {
      co_return co_await std::move(future_);
    }

    void removeCallback() {
      callback_.reset();
      fetchCallback_.reset();
      cancelSource_.requestCancellation();
    }
    void subscribeOK(SubscribeOk subscribeOK) {
      groupOrder_ = subscribeOK.groupOrder;
      promise_.setValue(std::move(subscribeOK));
    }
    void subscribeError(SubscribeError subErr) {
      XLOG(DBG1) << __func__ << " trackReceiveState=" << this;
      if (!promise_.isFulfilled()) {
        subErr.subscribeID = subscribeID_;
        promise_.setValue(folly::makeUnexpected(std::move(subErr)));
      } else {
        subscribeDone(
            {subscribeID_,
             SubscribeDoneStatusCode::INTERNAL_ERROR,
             "closed locally",
             folly::none});
      }
    }

    void subscribeDone(SubscribeDone subDone) {
      XLOG(DBG1) << __func__ << " trackReceiveState=" << this;
      if (callback_) {
        callback_->subscribeDone(std::move(subDone));
      } // else, unsubscribe raced with subscribeDone and callback was removed
    }

    folly::coro::Task<folly::Expected<SubscribeID, FetchError>> fetchReady() {
      co_return co_await std::move(fetchFuture_);
    }
    void fetchOK() {
      XLOG(DBG1) << __func__ << " trackReceiveState=" << this;
      fetchPromise_.setValue(subscribeID_);
    }
    void fetchError(FetchError fetchErr) {
      if (!promise_.isFulfilled()) {
        fetchErr.subscribeID = subscribeID_;
        fetchPromise_.setValue(folly::makeUnexpected(std::move(fetchErr)));
      } // there's likely a missing case here from shutdown
    }

    void setAllDataReceived() {
      allDataReceived_ = true;
    }

    bool allDataReceived() const {
      return allDataReceived_;
    }

    bool fetchOkReceived() const {
      return fetchPromise_.isFulfilled();
    }

    // Accessed By SubscribeCallback/FetchCallback
    std::shared_ptr<TrackConsumer> callback_;
    std::shared_ptr<FetchConsumer> fetchCallback_;

   private:
    FullTrackName fullTrackName_;
    SubscribeID subscribeID_;
    folly::coro::Promise<SubscribeResult> promise_;
    folly::coro::Future<SubscribeResult> future_;
    using FetchResult = folly::Expected<SubscribeID, FetchError>;
    folly::coro::Promise<FetchResult> fetchPromise_;
    folly::coro::Future<FetchResult> fetchFuture_;
    GroupOrder groupOrder_;
    std::chrono::milliseconds objectTimeout_{std::chrono::hours(24)};
    folly::CancellationSource cancelSource_;
    bool allDataReceived_{false};
  };

  // Track Alias -> Track Handle
  folly::F14FastMap<
      TrackAlias,
      std::shared_ptr<TrackReceiveState>,
      TrackAlias::hash>
      subTracks_;
  folly::F14FastMap<
      SubscribeID,
      std::shared_ptr<TrackReceiveState>,
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

  // Subscriber ID -> metadata about a publish track
  folly::
      F14FastMap<SubscribeID, std::shared_ptr<PublisherImpl>, SubscribeID::hash>
          pubTracks_;
  uint64_t nextTrackId_{0};
  uint64_t closedSubscribes_{0};
  // TODO: Make this value configurable. maxConcurrentSubscribes_ represents
  // the maximum number of concurrent subscriptions to a given sessions, set
  // to the initial MAX_SUBSCRIBE_ID
  uint64_t maxConcurrentSubscribes_{100};
  uint64_t peerMaxSubscribeID_{0};

  folly::coro::Promise<ServerSetup> setupPromise_;
  folly::coro::Future<ServerSetup> setupFuture_;
  bool setupComplete_{false};
  bool draining_{false};
  folly::CancellationSource cancellationSource_;

  // SubscribeID must be a unique monotonically increasing number that is
  // less than maxSubscribeID.
  uint64_t nextSubscribeID_{0};
  uint64_t maxSubscribeID_{0};

  ServerSetupCallback* serverSetupCallback_{nullptr};
};
} // namespace moxygen

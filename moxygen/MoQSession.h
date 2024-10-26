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
#include "moxygen/util/TimedBaton.h"

#include <boost/variant.hpp>

namespace moxygen {

class MoQSession : public MoQControlCodec::ControlCallback,
                   public MoQObjectStreamCodec::ObjectCallback,
                   public proxygen::WebTransportHandler {
 public:
  explicit MoQSession(
      MoQControlCodec::Direction dir,
      folly::MaybeManagedPtr<proxygen::WebTransport> wt,
      folly::EventBase* evb)
      : dir_(dir), wt_(wt), evb_(evb) {}

  [[nodiscard]] folly::EventBase* getEventBase() const {
    return evb_;
  }

  ~MoQSession() override;

  void start();
  void close(folly::Optional<SessionCloseErrorCode> error = folly::none);

  void setup(ClientSetup setup);
  void setup(ServerSetup setup);

  using MoQMessage = boost::variant<
      ClientSetup,
      ServerSetup,
      Announce,
      Unannounce,
      AnnounceCancel,
      SubscribeAnnounces,
      UnsubscribeAnnounces,
      SubscribeRequest,
      SubscribeUpdate,
      Unsubscribe,
      SubscribeDone,
      MaxSubscribeId,
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
    virtual void operator()(ServerSetup setup) const {
      XLOG(INFO) << "ServerSetup, version=" << setup.selectedVersion;
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

    virtual void operator()(SubscribeDone subscribeDone) const {
      XLOG(INFO) << "SubscribeDone subID=" << subscribeDone.subscribeID;
    }

    virtual void operator()(Unsubscribe unsubscribe) const {
      XLOG(INFO) << "Unsubscribe subID=" << unsubscribe.subscribeID;
    }
    virtual void operator()(MaxSubscribeId maxSubId) const {
      XLOG(INFO) << fmt::format(
          "MaxSubscribeId subID={}", maxSubId.subscribeID);
    }
    virtual void operator()(Fetch fetch) const {
      XLOG(INFO) << "Fetch subID=" << fetch.subscribeID;
    }
    virtual void operator()(FetchCancel fetchCancel) const {
      XLOG(INFO) << "FetchCancel subID=" << fetchCancel.subscribeID;
    }
    virtual void operator()(FetchOk fetchOk) const {
      XLOG(INFO) << "FetchOk subID=" << fetchOk.subscribeID;
    }
    virtual void operator()(FetchError fetchError) const {
      XLOG(INFO) << "FetchError subID=" << fetchError.subscribeID;
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

  static GroupOrder resolveGroupOrder(
      GroupOrder pubOrder,
      GroupOrder subOrder) {
    return subOrder == GroupOrder::Default ? pubOrder : subOrder;
  }

  class TrackHandle {
   public:
    TrackHandle(
        FullTrackName fullTrackName,
        uint64_t subscribeID,
        folly::CancellationToken token)
        : fullTrackName_(std::move(fullTrackName)),
          subscribeID_(subscribeID),
          cancelToken_(std::move(token)) {
      auto contract = folly::coro::makePromiseContract<
          folly::Expected<std::shared_ptr<TrackHandle>, SubscribeError>>();
      promise_ = std::move(contract.first);
      future_ = std::move(contract.second);
    }

    void setTrackName(FullTrackName trackName) {
      fullTrackName_ = std::move(trackName);
    }

    [[nodiscard]] const FullTrackName& fullTrackName() const {
      return fullTrackName_;
    }

    uint64_t subscribeID() const {
      return subscribeID_;
    }

    [[nodiscard]] folly::CancellationToken getCancelToken() const {
      return cancelToken_;
    }

    void fin();

    folly::coro::Task<
        folly::Expected<std::shared_ptr<TrackHandle>, SubscribeError>>
    ready() {
      co_return co_await std::move(future_);
    }

    void subscribeOK(
        std::shared_ptr<TrackHandle> self,
        GroupOrder order,
        folly::Optional<AbsoluteLocation> latest) {
      XCHECK_EQ(self.get(), this);
      groupOrder_ = order;
      latest_ = std::move(latest);
      promise_.setValue(std::move(self));
    }
    void subscribeError(SubscribeError subErr) {
      if (!promise_.isFulfilled()) {
        promise_.setValue(folly::makeUnexpected(std::move(subErr)));
      }
    }

    struct ObjectSource {
      ObjectHeader header;
      FullTrackName fullTrackName;
      folly::CancellationToken cancelToken;

      folly::coro::UnboundedQueue<std::unique_ptr<folly::IOBuf>, true, true>
          payloadQueue;

      folly::coro::Task<std::unique_ptr<folly::IOBuf>> payload() {
        if (header.status != ObjectStatus::NORMAL) {
          co_return nullptr;
        }
        folly::IOBufQueue payloadBuf{folly::IOBufQueue::cacheChainLength()};
        while (true) {
          auto buf = co_await folly::coro::co_withCancellation(
              cancelToken, payloadQueue.dequeue());
          if (!buf) {
            co_return payloadBuf.move();
          }
          payloadBuf.append(std::move(buf));
        }
      }
    };

    void onObjectHeader(ObjectHeader objHeader);
    void onObjectPayload(
        uint64_t groupId,
        uint64_t id,
        std::unique_ptr<folly::IOBuf> payload,
        bool eom);

    folly::coro::AsyncGenerator<std::shared_ptr<ObjectSource>> objects();

    GroupOrder groupOrder() const {
      return groupOrder_;
    }

    folly::Optional<AbsoluteLocation> latest() {
      return latest_;
    }

   private:
    FullTrackName fullTrackName_;
    uint64_t subscribeID_;
    folly::coro::Promise<
        folly::Expected<std::shared_ptr<TrackHandle>, SubscribeError>>
        promise_;
    folly::coro::Future<
        folly::Expected<std::shared_ptr<TrackHandle>, SubscribeError>>
        future_;
    folly::
        F14FastMap<std::pair<uint64_t, uint64_t>, std::shared_ptr<ObjectSource>>
            objects_;
    folly::coro::UnboundedQueue<std::shared_ptr<ObjectSource>, true, true>
        newObjects_;
    GroupOrder groupOrder_;
    folly::Optional<AbsoluteLocation> latest_;
    folly::CancellationToken cancelToken_;
  };

  folly::coro::Task<
      folly::Expected<std::shared_ptr<TrackHandle>, SubscribeError>>
  subscribe(SubscribeRequest sub);
  void subscribeOk(SubscribeOk subOk);
  void subscribeError(SubscribeError subErr);
  void unsubscribe(Unsubscribe unsubscribe);
  void subscribeDone(SubscribeDone subDone);

  class WebTransportException : public std::runtime_error {
   public:
    explicit WebTransportException(
        proxygen::WebTransport::ErrorCode error,
        const std::string& msg)
        : std::runtime_error(msg), errorCode(error) {}

    proxygen::WebTransport::ErrorCode errorCode;
  };

  // Publish this object.
  folly::SemiFuture<folly::Unit> publish(
      const ObjectHeader& objHeader,
      uint64_t payloadOffset,
      std::unique_ptr<folly::IOBuf> payload,
      bool eom);
  folly::SemiFuture<folly::Unit> publishStreamPerObject(
      const ObjectHeader& objHeader,
      uint64_t payloadOffset,
      std::unique_ptr<folly::IOBuf> payload,
      bool eom);
  folly::SemiFuture<folly::Unit> publishStatus(const ObjectHeader& objHeader);

  void onNewUniStream(proxygen::WebTransport::StreamReadHandle* rh) override;
  void onNewBidiStream(proxygen::WebTransport::BidiStreamHandle bh) override;
  void onDatagram(std::unique_ptr<folly::IOBuf> datagram) override;
  void onSessionEnd(folly::Optional<uint32_t>) override {
    XLOG(DBG1) << __func__ << " sess=" << this;
    close();
  }

  folly::coro::Task<void> setupComplete();

 private:
  folly::coro::Task<void> controlWriteLoop(
      proxygen::WebTransport::StreamWriteHandle* writeHandle);
  folly::coro::Task<void> readLoop(
      StreamType streamType,
      proxygen::WebTransport::StreamReadHandle* readHandle);

  void onClientSetup(ClientSetup clientSetup) override;
  void onServerSetup(ServerSetup serverSetup) override;
  void onObjectHeader(ObjectHeader objectHeader) override;
  void onObjectPayload(
      uint64_t subscribeID,
      uint64_t trackAlias,
      uint64_t groupID,
      uint64_t id,
      std::unique_ptr<folly::IOBuf> payload,
      bool eom) override;
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

  folly::SemiFuture<folly::Unit> publishImpl(
      const ObjectHeader& objHeader,
      uint64_t payloadOffset,
      std::unique_ptr<folly::IOBuf> payload,
      bool eom,
      bool streamPerObject);

  uint64_t order(const ObjectHeader& objHeader);

  struct PublishKey {
    uint64_t subscribeID;
    uint64_t group;
    uint64_t subgroup;
    ForwardPreference pref;
    uint64_t object;

    bool operator==(const PublishKey& other) const {
      if (subscribeID != other.subscribeID || pref != other.pref) {
        return false;
      }
      if (pref == ForwardPreference::Datagram) {
        return object == other.object;
      } else if (pref == ForwardPreference::Subgroup) {
        return group == other.group && subgroup == other.subgroup;
      } else if (pref == ForwardPreference::Track) {
        return true;
      }
      return false;
    }

    struct hash {
      size_t operator()(const PublishKey& ook) const {
        if (ook.pref == ForwardPreference::Datagram) {
          return folly::hash::hash_combine(
              ook.subscribeID, ook.group, ook.object);
        } else if (ook.pref == ForwardPreference::Subgroup) {
          return folly::hash::hash_combine(
              ook.subscribeID, ook.group, ook.subgroup);
        } // else if (ook.pref == ForwardPreference::Track) {
        return folly::hash::hash_combine(ook.subscribeID);
      }
    };
  };

  struct PublishData {
    uint64_t streamID;
    uint64_t group;
    uint64_t subgroup;
    uint64_t objectID;
    folly::Optional<uint64_t> objectLength;
    uint64_t offset;
    bool streamPerObject;
  };

  // Get the max subscribe id from the setup params. If MAX_SUBSCRIBE_ID key is
  // not present, we default to 0 as specified. 0 means that the peer MUST NOT
  // create any subscriptions
  static uint64_t getMaxSubscribeIdIfPresent(
      const std::vector<SetupParameter>& params);

  //  Closes the session if the subscribeID is invalid, that is,
  //  subscribeID <= maxSubscribeID_;
  //  TODO: Add this to all messages that have subscribeId
  void closeSessionIfSubscribeIdInvalid(uint64_t subscribeID);

  MoQControlCodec::Direction dir_;
  folly::MaybeManagedPtr<proxygen::WebTransport> wt_;
  folly::EventBase* evb_{nullptr}; // keepalive?
  folly::IOBufQueue controlWriteBuf_{folly::IOBufQueue::cacheChainLength()};
  moxygen::TimedBaton controlWriteEvent_;
  folly::coro::UnboundedQueue<MoQMessage, true, true> controlMessages_;
  // Subscriber State
  // Subscribe ID -> Track Handle
  folly::F14FastMap<uint64_t, std::shared_ptr<TrackHandle>> subTracks_;

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

  struct PubTrack {
    uint8_t priority;
    GroupOrder groupOrder;
  };
  folly::F14FastMap<uint64_t, PubTrack> pubTracks_;
  folly::F14FastMap<PublishKey, PublishData, PublishKey::hash> publishDataMap_;
  uint64_t nextTrackId_{0};
  uint64_t closedSubscribes_{0};
  // TODO: Make this value configurable. kMaxConcurrentSubscribes_ represents
  // the maximum number of concurrent subscriptions to a given sessions.
  const uint64_t kMaxConcurrentSubscribes_{100};
  uint64_t peerMaxSubscribeID_{0};

  moxygen::TimedBaton sentSetup_;
  moxygen::TimedBaton receivedSetup_;
  bool setupComplete_{false};
  folly::CancellationSource cancellationSource_;

  // SubscribeID must be a unique monotonically increasing number that is
  // less than maxSubscribeID.
  uint64_t nextSubscribeID_{0};
  uint64_t maxSubscribeID_{0};
};
} // namespace moxygen

/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <proxygen/lib/http/webtransport/WebTransport.h>
#include "moxygen/MoQCodec.h"

#include <folly/container/F14Set.h>
#include <folly/experimental/coro/AsyncGenerator.h>
#include <folly/experimental/coro/Promise.h>
#include <folly/experimental/coro/Task.h>
#include <folly/experimental/coro/UnboundedQueue.h>
#include <folly/logging/xlog.h>
#include "moxygen/util/TimedBaton.h"

#include <boost/variant.hpp>

namespace moxygen {

class MoQSession : public MoQCodec::Callback {
 public:
  explicit MoQSession(
      MoQCodec::Direction dir,
      proxygen::WebTransport* wt,
      folly::EventBase* evb)
      : dir_(dir), wt_(wt), evb_(evb) {}

  [[nodiscard]] folly::EventBase* getEventBase() const {
    return evb_;
  }

  ~MoQSession() override;

  void start();
  void close();

  void setup(ClientSetup setup);
  void setup(ServerSetup setup);

  using MoQMessage = boost::variant<
      ClientSetup,
      ServerSetup,
      Announce,
      Unannounce,
      AnnounceCancel,
      SubscribeRequest,
      Unsubscribe,
      SubscribeDone,
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

    virtual void operator()(AnnounceError announceError) const {
      XLOG(INFO) << "AnnounceError ns=" << announceError.trackNamespace
                 << " code=" << announceError.errorCode
                 << " reason=" << announceError.reasonPhrase;
    }

    virtual void operator()(SubscribeRequest subscribe) const {
      XLOG(INFO) << "Subscribe ftn=" << subscribe.fullTrackName.trackNamespace
                 << subscribe.fullTrackName.trackName;
    }

    virtual void operator()(SubscribeDone subscribeDone) const {
      XLOG(INFO) << "SubscribeDone subID=" << subscribeDone.subscribeID;
    }

    virtual void operator()(Unsubscribe unsubscribe) const {
      XLOG(INFO) << "Unsubscribe subID=" << unsubscribe.subscribeID;
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

    void subscribeOK(std::shared_ptr<TrackHandle> self) {
      XCHECK_EQ(self.get(), this);
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
    folly::CancellationToken cancelToken_;
  };

  folly::coro::Task<
      folly::Expected<std::shared_ptr<TrackHandle>, SubscribeError>>
  subscribe(SubscribeRequest sub);
  void subscribeOk(SubscribeOk subOk);
  void subscribeError(SubscribeError subErr);
  void unsubscribe(Unsubscribe unsubscribe);
  void subscribeDone(SubscribeDone subDone);

  // Publish this object.
  void publish(
      const ObjectHeader& objHeader,
      uint64_t payloadOffset,
      std::unique_ptr<folly::IOBuf> payload,
      bool eom);

  void onNewUniStream(proxygen::WebTransport::StreamReadHandle* rh);
  void onNewBidiStream(proxygen::WebTransport::BidiStreamHandle bh);

  folly::coro::Task<void> setupComplete();

 private:
  folly::coro::Task<void> controlWriteLoop(
      proxygen::WebTransport::StreamWriteHandle* writeHandle);
  enum class StreamType { CONTROL, DATA };
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
  void onSubscribeOk(SubscribeOk subscribeOk) override;
  void onSubscribeError(SubscribeError subscribeError) override;
  void onUnsubscribe(Unsubscribe unsubscribe) override;
  void onSubscribeDone(SubscribeDone subscribeDone) override;
  void onAnnounce(Announce announce) override;
  void onAnnounceOk(AnnounceOk announceOk) override;
  void onAnnounceError(AnnounceError announceError) override;
  void onUnannounce(Unannounce unannounce) override;
  void onAnnounceCancel(AnnounceCancel announceCancel) override;
  void onGoaway(Goaway goaway) override;
  void onConnectionError(ErrorCode error) override;

  struct PublishKey {
    uint64_t subscribeID;
    uint64_t group;
    uint64_t sendOrder;
    ForwardPreference pref;
    uint64_t object;

    bool operator==(const PublishKey& other) const {
      if (subscribeID != other.subscribeID || pref != other.pref ||
          sendOrder != other.sendOrder) {
        return false;
      }
      if (pref == ForwardPreference::Object ||
          pref == ForwardPreference::Datagram) {
        return object == other.object;
      } else if (pref == ForwardPreference::Group) {
        return group == other.group;
      } else if (pref == ForwardPreference::Track) {
        return true;
      }
      return false;
    }

    struct hash {
      size_t operator()(const PublishKey& ook) const {
        if (ook.pref == ForwardPreference::Object ||
            ook.pref == ForwardPreference::Datagram) {
          return folly::hash::hash_combine(
              ook.subscribeID, ook.group, ook.sendOrder, ook.object);
        } else if (ook.pref == ForwardPreference::Group) {
          return folly::hash::hash_combine(
              ook.subscribeID, ook.group, ook.sendOrder, ook.group);
        } // else if (ook.pref == ForwardPreference::Track) {
        return folly::hash::hash_combine(
            ook.subscribeID, ook.group, ook.sendOrder);
      }
    };
  };

  struct PublishData {
    uint64_t streamID;
    uint64_t group;
    uint64_t objectID;
    folly::Optional<uint64_t> objectLength;
    uint64_t offset;
  };

  MoQCodec::Direction dir_;
  proxygen::WebTransport* wt_{nullptr};
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
      std::string,
      folly::coro::Promise<folly::Expected<AnnounceOk, AnnounceError>>>
      pendingAnnounce_;
  folly::F14FastMap<uint64_t, FullTrackName> pubTracks_;
  folly::F14FastMap<PublishKey, PublishData, PublishKey::hash> publishDataMap_;
  uint64_t nextTrackId_{0};

  moxygen::TimedBaton sentSetup_;
  moxygen::TimedBaton receivedSetup_;
  bool setupComplete_{false};
  folly::CancellationSource cancellationSource_;

  uint64_t nextSubscribeID_{0};
};
} // namespace moxygen

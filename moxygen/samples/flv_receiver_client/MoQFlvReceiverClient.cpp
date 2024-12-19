/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <folly/portability/GFlags.h>
#include "moxygen/MoQClient.h"
#include "moxygen/ObjectReceiver.h"

#include <folly/init/Init.h>
#include <folly/io/async/AsyncSignalHandler.h>
#include <signal.h>

DEFINE_string(
    connect_url,
    "https://localhost:4433/moq",
    "URL for webtransport server");
DEFINE_string(track_namespace, "flvstreamer", "Track Namespace");
DEFINE_string(track_namespace_delimiter, "/", "Track Namespace Delimiter");
// TODO DEFINE_string(video_track_name, "video0", "Video track Name");
// TODO: Fix and add proper audo & video parsing. This is set to video0 on
// purpose to test the video track
DEFINE_string(track_name, "video0", "Track Name");
DEFINE_int32(connect_timeout, 1000, "Connect timeout (ms)");
DEFINE_int32(transaction_timeout, 120, "Transaction timeout (s)");
DEFINE_bool(quic_transport, false, "Use raw QUIC transport");
DEFINE_bool(fetch, false, "Use fetch rather than subscribe");

namespace {
using namespace moxygen;

struct SubParams {
  LocationType locType;
  folly::Optional<AbsoluteLocation> start;
  folly::Optional<AbsoluteLocation> end;
};

class TrackReceiverHandler : public ObjectReceiverCallback {
 public:
  ~TrackReceiverHandler() override = default;
  FlowControlState onObject(const ObjectHeader&, Payload payload) override {
    if (payload) {
      std::cout << "Received payload. Size="
                << payload->computeChainDataLength() << std::endl;
    }
    return FlowControlState::UNBLOCKED;
  }
  void onObjectStatus(const ObjectHeader& objHeader) override {
    std::cout << "ObjectStatus=" << uint32_t(objHeader.status) << std::endl;
  }
  void onEndOfStream() override {}
  void onError(ResetStreamErrorCode error) override {
    std::cout << "Stream Error=" << folly::to_underlying(error) << std::endl;
  }

  void subscribeDone(SubscribeDone) override {
    baton.post();
  }

  folly::coro::Baton baton;
};

class MoQFlvReceiverClient {
 public:
  MoQFlvReceiverClient(
      folly::EventBase* evb,
      proxygen::URL url,
      FullTrackName ftn)
      : moqClient_(
            evb,
            std::move(url),
            (FLAGS_quic_transport ? MoQClient::TransportType::QUIC
                                  : MoQClient::TransportType::H3_WEBTRANSPORT)),
        fullTrackName_(std::move(ftn)) {}

  folly::coro::Task<void> run(SubscribeRequest sub) noexcept {
    XLOG(INFO) << __func__;
    auto g =
        folly::makeGuard([func = __func__] { XLOG(INFO) << "exit " << func; });
    try {
      co_await moqClient_.setupMoQSession(
          std::chrono::milliseconds(FLAGS_connect_timeout),
          std::chrono::seconds(FLAGS_transaction_timeout),
          Role::SUBSCRIBER);
      auto exec = co_await folly::coro::co_current_executor;
      controlReadLoop().scheduleOn(exec).start();

      SubParams subParams{sub.locType, sub.start, sub.end};
      sub.locType = LocationType::LatestObject;
      sub.start = folly::none;
      sub.end = folly::none;
      subRxHandler_ = std::make_shared<ObjectReceiver>(
          ObjectReceiver::SUBSCRIBE, &trackReceiverHandler_);
      auto track =
          co_await moqClient_.moqSession_->subscribe(sub, subRxHandler_);
      if (track.hasValue()) {
        subscribeID_ = track->subscribeID;
        XLOG(DBG1) << "subscribeID=" << subscribeID_;
        auto latest = track->latest;
        if (latest) {
          XLOG(INFO) << "Latest={" << latest->group << ", " << latest->object
                     << "}";
        }
      } else {
        XLOG(INFO) << "SubscribeError id=" << track.error().subscribeID
                   << " code=" << track.error().errorCode
                   << " reason=" << track.error().reasonPhrase;
      }
      moqClient_.moqSession_->drain();
    } catch (const std::exception& ex) {
      XLOG(ERR) << ex.what();
      co_return;
    }
    co_await trackReceiverHandler_.baton;
    XLOG(INFO) << __func__ << " done";
  }

  void stop() {
    moqClient_.moqSession_->unsubscribe({subscribeID_});
    moqClient_.moqSession_->close();
  }

  folly::coro::Task<void> controlReadLoop() {
    class ControlVisitor : public MoQSession::ControlVisitor {
     public:
      explicit ControlVisitor(MoQFlvReceiverClient& client) : client_(client) {}

      void operator()(Announce announce) const override {
        XLOG(WARN) << "Announce ns=" << announce.trackNamespace;
        // text client doesn't expect server or relay to announce anything,
        // but announce OK anyways
        client_.moqClient_.moqSession_->announceOk({announce.trackNamespace});
      }

      void operator()(SubscribeRequest subscribeReq) const override {
        XLOG(INFO) << "SubscribeRequest";
        client_.moqClient_.moqSession_->subscribeError(
            {subscribeReq.subscribeID, 404, "don't care"});
      }

      void operator()(Goaway) const override {
        XLOG(INFO) << "Goaway";
        client_.moqClient_.moqSession_->unsubscribe({client_.subscribeID_});
      }

     private:
      MoQFlvReceiverClient& client_;
    };
    XLOG(INFO) << __func__;
    auto g =
        folly::makeGuard([func = __func__] { XLOG(INFO) << "exit " << func; });
    ControlVisitor visitor(*this);
    MoQSession::ControlVisitor* vptr(&visitor);
    while (auto msg =
               co_await moqClient_.moqSession_->controlMessages().next()) {
      boost::apply_visitor(*vptr, msg.value());
    }
  }

  MoQClient moqClient_;
  FullTrackName fullTrackName_;
  SubscribeID subscribeID_{0};
  TrackReceiverHandler trackReceiverHandler_;
  std::shared_ptr<ObjectReceiver> subRxHandler_;
};
} // namespace

using namespace moxygen;

int main(int argc, char* argv[]) {
  folly::Init init(&argc, &argv, false);
  folly::EventBase eventBase;
  proxygen::URL url(FLAGS_connect_url);
  if (!url.isValid() || !url.hasHost()) {
    XLOG(ERR) << "Invalid url: " << FLAGS_connect_url;
  }
  TrackNamespace ns =
      TrackNamespace(FLAGS_track_namespace, FLAGS_track_namespace_delimiter);
  MoQFlvReceiverClient flvReceiverClient(
      &eventBase,
      std::move(url),
      moxygen::FullTrackName({ns, FLAGS_track_name}));
  class SigHandler : public folly::AsyncSignalHandler {
   public:
    explicit SigHandler(folly::EventBase* evb, std::function<void(int)> fn)
        : folly::AsyncSignalHandler(evb), fn_(std::move(fn)) {
      registerSignalHandler(SIGTERM);
      registerSignalHandler(SIGINT);
    }
    void signalReceived(int signum) noexcept override {
      fn_(signum);
      unreg();
    }

    void unreg() {
      unregisterSignalHandler(SIGTERM);
      unregisterSignalHandler(SIGINT);
    }

   private:
    std::function<void(int)> fn_;
  };
  SigHandler handler(&eventBase, [&flvReceiverClient](int) mutable {
    flvReceiverClient.stop();
  });
  auto subParams =
      SubParams{LocationType::LatestObject, folly::none, folly::none};
  const auto subscribeID = 0;
  const auto trackAlias = 1;
  flvReceiverClient
      .run(
          {subscribeID,
           trackAlias,
           moxygen::FullTrackName({std::move(ns), FLAGS_track_name}),
           0,
           GroupOrder::OldestFirst,
           subParams.locType,
           subParams.start,
           subParams.end,
           {}})
      .scheduleOn(&eventBase)
      .start()
      .via(&eventBase)
      .thenTry([&handler](auto) { handler.unreg(); });
  eventBase.loop();
}

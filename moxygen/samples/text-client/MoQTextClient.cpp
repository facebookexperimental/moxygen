/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <folly/portability/GFlags.h>
#include "moxygen/MoQClient.h"
#include "moxygen/MoQLocation.h"
#include "moxygen/ObjectReceiver.h"

#include <folly/base64.h>
#include <folly/init/Init.h>
#include <folly/io/async/AsyncSignalHandler.h>
#include <signal.h>

DEFINE_string(connect_url, "", "URL for webtransport server");
DEFINE_string(track_namespace, "", "Track Namespace");
DEFINE_string(track_namespace_delimiter, "/", "Track Namespace Delimiter");
DEFINE_string(track_name, "", "Track Name");
DEFINE_string(sg, "", "Start group, defaults to latest");
DEFINE_string(so, "", "Start object, defaults to 0 when sg is set or latest");
DEFINE_string(eg, "", "End group");
DEFINE_int32(connect_timeout, 1000, "Connect timeout (ms)");
DEFINE_int32(transaction_timeout, 120, "Transaction timeout (s)");
DEFINE_bool(quic_transport, false, "Use raw QUIC transport");
DEFINE_bool(fetch, false, "Use fetch rather than subscribe");
DEFINE_bool(jfetch, false, "Joining fetch");

namespace {
using namespace moxygen;

struct SubParams {
  LocationType locType;
  folly::Optional<AbsoluteLocation> start;
  uint64_t endGroup;
};

SubParams flags2params() {
  SubParams result;
  std::string soStr(FLAGS_so);
  if (FLAGS_sg.empty()) {
    if (soStr.empty()) {
      result.locType = LocationType::LatestObject;
      return result;
    } else {
      XLOG(ERR) << "Invalid: sg blank, so=" << soStr;
      exit(1);
    }
  } else if (soStr.empty()) {
    soStr = std::string("0");
  }
  if (FLAGS_jfetch) {
    XLOG(ERR) << "Joining fetch requires empty sg";
    exit(1);
  }
  result.start.emplace(
      folly::to<uint64_t>(FLAGS_sg), folly::to<uint64_t>(soStr));
  if (FLAGS_eg.empty()) {
    result.locType = LocationType::AbsoluteStart;
    return result;
  } else {
    result.locType = LocationType::AbsoluteRange;
    result.endGroup = folly::to<uint64_t>(FLAGS_eg);
    return result;
  }
  return result;
}

class TextHandler : public ObjectReceiverCallback {
 public:
  explicit TextHandler(bool fetch) : fetch_(fetch) {}
  ~TextHandler() override = default;
  FlowControlState onObject(const ObjectHeader& header, Payload payload)
      override {
    for (const auto& ext : header.extensions) {
      if (ext.type & 0x1) {
        std::cout << "data extension="
                  << folly::base64Encode(
                         {(const char*)(ext.arrayValue.data()),
                          ext.arrayValue.size()})

                  << std::endl;
      } else {
        std::cout << "int extension=" << ext.intValue << std::endl;
      }
    }

    if (payload) {
      std::cout << payload->moveToFbString() << std::endl;
    }
    return FlowControlState::UNBLOCKED;
  }
  void onObjectStatus(const ObjectHeader& objHeader) override {
    std::cout << "ObjectStatus=" << uint32_t(objHeader.status) << std::endl;
  }
  void onEndOfStream() override {
    if (fetch_) {
      std::cout << __func__ << std::endl;
      baton.post();
    }
  }
  void onError(ResetStreamErrorCode error) override {
    std::cout << "Stream Error=" << folly::to_underlying(error) << std::endl;
  }

  void onSubscribeDone(SubscribeDone) override {
    CHECK(!fetch_);
    std::cout << __func__ << std::endl;
    baton.post();
  }

  folly::coro::Baton baton;

 private:
  bool fetch_{false};
};

class MoQTextClient : public Subscriber,
                      public std::enable_shared_from_this<MoQTextClient> {
 public:
  MoQTextClient(folly::EventBase* evb, proxygen::URL url, FullTrackName ftn)
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
          /*publishHandler=*/nullptr,
          /*subscribeHandler=*/shared_from_this());

      Publisher::SubscribeResult track;
      if (FLAGS_jfetch) {
        // Call join() for joining fetch
        fetchTextReceiver_ = std::make_shared<ObjectReceiver>(
            ObjectReceiver::FETCH, &fetchTextHandler_);
        auto joinResult = co_await moqClient_.moqSession_->join(
            sub,
            subTextReceiver_,
            0,
            sub.priority,
            sub.groupOrder,
            {},
            fetchTextReceiver_);
        track = joinResult.subscribeResult;
      } else {
        track =
            co_await moqClient_.moqSession_->subscribe(sub, subTextReceiver_);
      }
      bool needFetch = false;
      AbsoluteLocation fetchEnd{sub.endGroup + 1, 0};
      if (track.hasValue()) {
        subscription_ = std::move(track.value());
        auto subscribeID = subscription_->subscribeOk().subscribeID;
        XLOG(DBG1) << "subscribeID=" << subscribeID;
        auto latest = subscription_->subscribeOk().latest;
        if (latest) {
          XLOG(INFO) << "Latest={" << latest->group << ", " << latest->object
                     << "}";
        }
        if ((sub.locType == LocationType::AbsoluteStart ||
             sub.locType == LocationType::AbsoluteRange) &&
            subscription_ && subscription_->subscribeOk().latest &&
            *sub.start < *subscription_->subscribeOk().latest) {
          XLOG(INFO) << "Start before latest, need FETCH";
          needFetch = true;
          fetchEnd = AbsoluteLocation{
              subscription_->subscribeOk().latest->group,
              subscription_->subscribeOk().latest->object + 1};
        }
      } else {
        XLOG(INFO) << "SubscribeError id=" << track.error().subscribeID
                   << " code=" << folly::to_underlying(track.error().errorCode)
                   << " reason=" << track.error().reasonPhrase;
        subTextReceiver_.reset();
        if (track.error().errorCode == SubscribeErrorCode::INVALID_RANGE) {
          XLOG(INFO) << "End before latest, need FETCH";
          needFetch = true;
        }
      }
      if (needFetch) {
        fetchTextReceiver_ = std::make_shared<ObjectReceiver>(
            ObjectReceiver::FETCH, &fetchTextHandler_);
        auto fetchTrack = co_await moqClient_.moqSession_->fetch(
            Fetch(
                SubscribeID(0),
                sub.fullTrackName,
                *sub.start,
                fetchEnd,
                sub.priority,
                sub.groupOrder),
            fetchTextReceiver_);
        if (fetchTrack.hasError()) {
          XLOG(ERR) << "Fetch failed err="
                    << folly::to_underlying(fetchTrack.error().errorCode)
                    << " reason=" << fetchTrack.error().reasonPhrase;
          fetchTextReceiver_.reset();
        }
      }
      if (moqClient_.moqSession_) {
        moqClient_.moqSession_->drain();
      }
    } catch (const std::exception& ex) {
      XLOG(ERR) << folly::exceptionStr(ex);
      co_return;
    }

    if (subTextReceiver_) {
      co_await subTextHandler_.baton;
    }
    if (fetchTextReceiver_) {
      co_await fetchTextHandler_.baton;
    }

    XLOG(INFO) << __func__ << " done";
  }

  folly::coro::Task<AnnounceResult> announce(
      Announce announce,
      std::shared_ptr<AnnounceCallback>) override {
    XLOG(INFO) << "Announce ns=" << announce.trackNamespace;
    // text client doesn't expect server or relay to announce anything,
    // but announce OK anyways
    return folly::coro::makeTask<AnnounceResult>(
        std::make_shared<AnnounceHandle>(AnnounceOk{announce.trackNamespace}));
  }

  void goaway(Goaway goaway) override {
    XLOG(INFO) << "Goaway uri=" << goaway.newSessionUri;
    stop();
  }

  void stop() {
    subTextHandler_.baton.post();
    fetchTextHandler_.baton.post();
    // TODO: maybe need fetchCancel
    if (subscription_) {
      subscription_->unsubscribe();
      subscription_.reset();
    }
    if (moqClient_.moqSession_) {
      moqClient_.moqSession_->close(SessionCloseErrorCode::NO_ERROR);
    }
  }

  MoQClient moqClient_;
  FullTrackName fullTrackName_;
  std::shared_ptr<Publisher::SubscriptionHandle> subscription_;
  TextHandler subTextHandler_{/*fetch=*/false};
  TextHandler fetchTextHandler_{/*fetch=*/true};
  std::shared_ptr<ObjectReceiver> subTextReceiver_{
      std::make_shared<ObjectReceiver>(
          ObjectReceiver::SUBSCRIBE,
          &subTextHandler_)};
  std::shared_ptr<ObjectReceiver> fetchTextReceiver_;
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
  auto textClient = std::make_shared<MoQTextClient>(
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
  SigHandler handler(
      &eventBase, [&textClient](int) mutable { textClient->stop(); });
  auto subParams = flags2params();
  const auto subscribeID = 0;
  const auto trackAlias = 1;
  textClient
      ->run(
          {subscribeID,
           trackAlias,
           moxygen::FullTrackName({std::move(ns), FLAGS_track_name}),
           0,
           GroupOrder::OldestFirst,
           subParams.locType,
           subParams.start,
           subParams.endGroup,
           {}})
      .scheduleOn(&eventBase)
      .start()
      .via(&eventBase)
      .thenTry([&handler](auto) { handler.unreg(); });
  eventBase.loop();
}

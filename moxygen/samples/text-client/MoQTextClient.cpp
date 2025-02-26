/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <folly/portability/GFlags.h>
#include "moxygen/MoQClient.h"
#include "moxygen/MoQLocation.h"
#include "moxygen/ObjectReceiver.h"

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
DEFINE_string(eo, "", "End object, leave blank for entire group");
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

SubParams flags2params() {
  SubParams result;
  std::string soStr(FLAGS_so);
  if (FLAGS_sg.empty()) {
    if (soStr.empty()) {
      result.locType = LocationType::LatestObject;
      return result;
    } else if (auto so = folly::to<uint64_t>(soStr) > 0) {
      XLOG(ERR) << "Invalid: sg blank, so=" << so;
      exit(1);
    } else {
      result.locType = LocationType::LatestGroup;
      return result;
    }
  } else if (soStr.empty()) {
    soStr = std::string("0");
  }
  result.start.emplace(
      folly::to<uint64_t>(FLAGS_sg), folly::to<uint64_t>(soStr));
  if (FLAGS_eg.empty()) {
    result.locType = LocationType::AbsoluteStart;
    return result;
  } else {
    result.locType = LocationType::AbsoluteRange;
    result.end.emplace(
        folly::to<uint64_t>(FLAGS_eg),
        (FLAGS_eo.empty() ? 0 : folly::to<uint64_t>(FLAGS_eo) + 1));
    return result;
  }
  return result;
}

class TextHandler : public ObjectReceiverCallback {
 public:
  ~TextHandler() override = default;
  FlowControlState onObject(const ObjectHeader&, Payload payload) override {
    if (payload) {
      std::cout << payload->moveToFbString() << std::endl;
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

  void onSubscribeDone(SubscribeDone) override {
    std::cout << __func__ << std::endl;
    baton.post();
  }

  folly::coro::Baton baton;
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
      SubParams subParams{sub.locType, sub.start, sub.end};
      sub.locType = LocationType::LatestObject;
      sub.start = folly::none;
      sub.end = folly::none;
      subTextHandler_ = std::make_shared<ObjectReceiver>(
          ObjectReceiver::SUBSCRIBE, &textHandler_);

      auto trackStatus =
          co_await moqClient_.moqSession_->trackStatus({fullTrackName_});
      if (trackStatus.statusCode != TrackStatusCode::IN_PROGRESS) {
        XLOG(ERR) << "Track is currently not in progress! TrackStatusCode: "
                  << std::to_string((uint32_t)trackStatus.statusCode);
        co_return;
      }

      auto track =
          co_await moqClient_.moqSession_->subscribe(sub, subTextHandler_);
      if (track.hasValue()) {
        subscription_ = std::move(track.value());
        auto subscribeID = subscription_->subscribeOk().subscribeID;
        XLOG(DBG1) << "subscribeID=" << subscribeID;
        auto latest = subscription_->subscribeOk().latest;
        if (latest) {
          XLOG(INFO) << "Latest={" << latest->group << ", " << latest->object
                     << "}";
        }
        if (subParams.start && latest) {
          // There was a specific start and the track has started
          auto range = toSubscribeRange(
              subParams.start, subParams.end, subParams.locType, *latest);
          if (range.start <= *latest) {
            AbsoluteLocation fetchEnd = *latest;
            // The start was before latest, need to FETCH
            if (range.end < *latest) {
              // The end is before latest, UNSUBSCRIBE
              XLOG(DBG1) << "end={" << range.end.group << ","
                         << range.end.object << "} before latest, unsubscribe";
              textHandler_.baton.post();
              subscription_->unsubscribe();
              subscription_.reset();
              fetchEnd = range.end;
              if (fetchEnd.object == 0) {
                fetchEnd.group--;
              }
            }
            if (FLAGS_fetch) {
              XLOG(DBG1) << "FETCH start={" << range.start.group << ","
                         << range.start.object << "} end={" << fetchEnd.group
                         << "," << fetchEnd.object << "}";
              fetchTextHandler_ = std::make_shared<ObjectReceiver>(
                  ObjectReceiver::FETCH, &textHandler_);
              auto fetchTrack = co_await moqClient_.moqSession_->fetch(
                  Fetch(
                      SubscribeID(0),
                      sub.fullTrackName,
                      sub.priority,
                      sub.groupOrder,
                      range.start,
                      fetchEnd),
                  fetchTextHandler_);
              if (fetchTrack.hasError()) {
                XLOG(ERR) << "Fetch failed err=" << fetchTrack.error().errorCode
                          << " reason=" << fetchTrack.error().reasonPhrase;
              } else {
                XLOG(DBG1) << "subscribeID=" << fetchTrack.value();
              }
            }
          } // else we started from current or no content - nothing to FETCH
          if (subParams.end && (!latest || range.end > *latest)) {
            // The end is set but after latest, SUBSCRIBE_UPDATE for the end
            XLOG(DBG1) << "Setting subscribe end={" << range.end.group << ","
                       << range.end.object << "} before latest, update";
            subscription_->subscribeUpdate(
                {subscribeID,
                 latest.value_or(AbsoluteLocation{0, 0}),
                 range.end,
                 sub.priority,
                 sub.params});
          }
        }
      } else {
        XLOG(INFO) << "SubscribeError id=" << track.error().subscribeID
                   << " code=" << track.error().errorCode
                   << " reason=" << track.error().reasonPhrase;
      }
      if (moqClient_.moqSession_) {
        moqClient_.moqSession_->drain();
      }
    } catch (const std::exception& ex) {
      XLOG(ERR) << folly::exceptionStr(ex);
      co_return;
    }
    co_await textHandler_.baton;
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
    textHandler_.baton.post();
    // TODO: maybe need fetchCancel + fetchTextHandler_.baton.post()
    if (subscription_) {
      subscription_->unsubscribe();
      subscription_.reset();
    }
    moqClient_.moqSession_->close(SessionCloseErrorCode::NO_ERROR);
  }

  MoQClient moqClient_;
  FullTrackName fullTrackName_;
  std::shared_ptr<Publisher::SubscriptionHandle> subscription_;
  TextHandler textHandler_;
  std::shared_ptr<ObjectReceiver> subTextHandler_;
  std::shared_ptr<ObjectReceiver> fetchTextHandler_;
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
           subParams.end,
           {}})
      .scheduleOn(&eventBase)
      .start()
      .via(&eventBase)
      .thenTry([&handler](auto) { handler.unreg(); });
  eventBase.loop();
}

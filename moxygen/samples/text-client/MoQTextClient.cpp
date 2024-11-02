/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <folly/portability/GFlags.h>
#include "moxygen/MoQClient.h"

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

namespace {
using namespace moxygen;

class MoQTextClient {
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
          Role::SUBSCRIBER);
      auto exec = co_await folly::coro::co_current_executor;
      controlReadLoop().scheduleOn(exec).start();

      auto track = co_await moqClient_.moqSession_->subscribe(std::move(sub));
      if (track.hasValue()) {
        subscribeID_ = track.value()->subscribeID();
        co_await readTrack(std::move(track.value()));
      } else {
        XLOG(INFO) << "SubscribeError id=" << track.error().subscribeID
                   << " code=" << track.error().errorCode
                   << " reason=" << track.error().reasonPhrase;
      }
    } catch (const std::exception& ex) {
      XLOG(ERR) << ex.what();
      co_return;
    }
    XLOG(INFO) << __func__ << " done";
  }

  void stop() {
    moqClient_.moqSession_->unsubscribe({subscribeID_});
    moqClient_.moqSession_->close();
  }

  folly::coro::Task<void> controlReadLoop() {
    class ControlVisitor : public MoQSession::ControlVisitor {
     public:
      explicit ControlVisitor(MoQTextClient& client) : client_(client) {}

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

      void operator()(SubscribeDone) const override {
        XLOG(INFO) << "SubscribeDone";
        client_.moqClient_.moqSession_->close();
      }

      virtual void operator()(Goaway) const override {
        XLOG(INFO) << "Goaway";
        client_.moqClient_.moqSession_->unsubscribe({client_.subscribeID_});
      }

     private:
      MoQTextClient& client_;
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

  folly::coro::Task<void> readTrack(
      std::shared_ptr<MoQSession::TrackHandle> track) {
    XLOG(INFO) << __func__;
    if (auto latest = track->latest()) {
      XLOG(INFO) << "Latest={" << latest->group << ", " << latest->object
                 << "}";
    }
    auto g =
        folly::makeGuard([func = __func__] { XLOG(INFO) << "exit " << func; });
    // TODO: check track.value()->getCancelToken()
    while (auto obj = co_await track->objects().next()) {
      auto payload = co_await obj.value()->payload();
      if (payload) {
        std::cout << payload->moveToFbString() << std::endl;
      }
    }
  }
  MoQClient moqClient_;
  FullTrackName fullTrackName_;
  uint64_t subscribeID_{0};
};
} // namespace

using namespace moxygen;
namespace {

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
} // namespace

int main(int argc, char* argv[]) {
  folly::Init init(&argc, &argv, false);
  folly::EventBase eventBase;
  proxygen::URL url(FLAGS_connect_url);
  if (!url.isValid() || !url.hasHost()) {
    XLOG(ERR) << "Invalid url: " << FLAGS_connect_url;
  }
  TrackNamespace ns =
      TrackNamespace(FLAGS_track_namespace, FLAGS_track_namespace_delimiter);
  MoQTextClient textClient(
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
      &eventBase, [&textClient](int) mutable { textClient.stop(); });
  auto subParams = flags2params();
  const auto subscribeID = 0;
  const auto trackAlias = 1;
  textClient
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

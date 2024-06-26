/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <folly/experimental/coro/Sleep.h>
#include <compare>
#include <iomanip>
#include "moxygen/MoQLocation.h"
#include "moxygen/MoQServer.h"
#include "moxygen/relay/MoQForwarder.h"
#include "moxygen/relay/MoQRelayClient.h"

using namespace quic::samples;
using namespace proxygen;

DEFINE_string(relay_url, "", "Use specified relay");
DEFINE_int32(relay_connect_timeout, 1000, "Connect timeout (ms)");
DEFINE_int32(relay_transaction_timeout, 120, "Transaction timeout (s)");
DEFINE_string(cert, "", "Cert path");
DEFINE_string(key, "", "Key path");
DEFINE_int32(port, 9667, "Server Port");

namespace {
using namespace moxygen;

class MoQDateServer : MoQServer {
 public:
  explicit MoQDateServer(folly::EventBase* evb)
      : MoQServer(FLAGS_port, FLAGS_cert, FLAGS_key, "/moq-date"),
        forwarder_(dateTrackName()) {
    if (!FLAGS_relay_url.empty()) {
      proxygen::URL url(FLAGS_relay_url);
      if (!url.isValid() || !url.hasHost()) {
        XLOG(ERR) << "Invalid url: " << FLAGS_relay_url;
      }
      relayClient_ = std::make_unique<MoQRelayClient>(
          evb, url, [this](std::shared_ptr<MoQSession> session) {
            return std::make_unique<DateControlVisitor>(
                *this, std::move(session));
          });
      relayClient_
          ->run(
              Role::PUBLISHER,
              {"moq-date"},
              std::chrono::milliseconds(FLAGS_relay_connect_timeout),
              std::chrono::seconds(FLAGS_relay_transaction_timeout))
          .scheduleOn(evb)
          .start();
    }
  }

  class DateControlVisitor : public MoQServer::ControlVisitor {
   public:
    DateControlVisitor(
        MoQDateServer& server,
        std::shared_ptr<MoQSession> clientSession)
        : MoQServer::ControlVisitor(std::move(clientSession)),
          server_(server) {}

    void operator()(ServerSetup) const override {
      if (!server_.relayClient_) {
        XLOG(ERR) << "Server received ServerSetup";
        clientSession_->close();
      }
    }

    void operator()(SubscribeRequest subscribeReq) const override {
      XLOG(INFO) << "SubscribeRequest track ns="
                 << subscribeReq.fullTrackName.trackNamespace
                 << " name=" << subscribeReq.fullTrackName.trackName;
      if (subscribeReq.fullTrackName != server_.dateTrackName()) {
        clientSession_->subscribeError(
            {subscribeReq.subscribeID, 403, "unexpected subscribe"});
      } else {
        server_.onSubscribe(std::move(subscribeReq), clientSession_);
      }
    }

    void operator()(SubscribeUpdateRequest subscribeUpdate) const override {
      XLOG(INFO) << "SubscribeUpdate id=" << subscribeUpdate.subscribeID;
      if (!server_.onSubscribeUpdate(subscribeUpdate)) {
        clientSession_->subscribeError(
            {subscribeUpdate.subscribeID, 403, "unexpected subscribe update"});
      }
    }

    void operator()(Unsubscribe unsubscribe) const override {
      XLOG(INFO) << "Unsubscribe id=" << unsubscribe.subscribeID;
      server_.unsubscribe(clientSession_, std::move(unsubscribe));
    }

    void operator()(Goaway) const override {
      XLOG(INFO) << "Goaway";
    }

   private:
    MoQDateServer& server_;
  };

  std::unique_ptr<ControlVisitor> makeControlVisitor(
      std::shared_ptr<MoQSession> clientSession) override {
    return std::make_unique<DateControlVisitor>(
        *this, std::move(clientSession));
  }

  void onSubscribe(
      SubscribeRequest subReq,
      std::shared_ptr<MoQSession> clientSession) {
    auto now = std::chrono::system_clock::now();
    auto in_time_t = std::chrono::system_clock::to_time_t(now);
    AbsoluteLocation nowLoc(
        {uint64_t(in_time_t / 60), uint64_t(in_time_t % 60) + 1});
    auto range = toSubscribeRange(subReq, nowLoc);
    clientSession->subscribeOk(
        {subReq.subscribeID, std::chrono::milliseconds(0), nowLoc});

    bool done = catchup(
        clientSession, subReq.subscribeID, subReq.trackAlias, range, nowLoc);
    if (!done) {
      forwarder_.setLatest(nowLoc);
      forwarder_.addSubscriber(
          {std::move(clientSession),
           subReq.subscribeID,
           subReq.trackAlias,
           range});
    }
  }

  bool onSubscribeUpdate(const SubscribeUpdateRequest& subscribeUpdate) {
    return forwarder_.updateSubscriber(subscribeUpdate);
  }

  bool catchup(
      std::shared_ptr<MoQSession> clientSession,
      uint64_t subscribeID,
      uint64_t trackAlias,
      SubscribeRange range,
      AbsoluteLocation now) {
    if (range.start >= now) {
      return false;
    }
    time_t t =
        range.start.group * 60 + std::max(range.start.object, (uint64_t)1) - 1;
    while (range.start < now && range.start < range.end) {
      auto n = publishDate(
          t, false, clientSession, subscribeID, trackAlias, range.end);
      t++;
      // publishDate publishes two objects for obj = 0
      range.start.object += n;
      if (range.start.object > 60) {
        range.start.group++;
        range.start.object = 0;
      }
    }
    if (range.end <= now) {
      clientSession->subscribeDone(
          {subscribeID,
           SubscribeDoneStatusCode::SUBSCRIPTION_ENDED,
           "",
           range.start});
      return true;
    }
    return false;
  }

  folly::coro::Task<void> publishDateLoop() {
    bool first = false;
    auto cancelToken = co_await folly::coro::co_current_cancellation_token;
    while (!cancelToken.isCancellationRequested()) {
      if (!forwarder_.empty()) {
        auto now = std::chrono::system_clock::now();
        auto in_time_t = std::chrono::system_clock::to_time_t(now);

        publishDate(in_time_t, first, nullptr, 0, 0, folly::none);
        first = false;
      }
      co_await folly::coro::sleep(std::chrono::seconds(1));
    }
  }

  size_t publishDate(
      time_t in_time_t,
      bool forceGroup,
      const std::shared_ptr<MoQSession>& session,
      uint64_t subscribeID,
      uint64_t trackAlias,
      folly::Optional<AbsoluteLocation> end) {
    size_t objectsPublished = 0;
    struct tm local_tm;
    auto lt = ::localtime_r(&in_time_t, &local_tm);
    std::stringstream ss;
    ss << std::put_time(lt, "%Y-%m-%d %H:%M:");
    XLOG(DBG1) << ss.str() << lt->tm_sec;
    AbsoluteLocation nowLoc(
        {uint64_t(in_time_t / 60), uint64_t(lt->tm_sec + 1)});
    if (lt->tm_sec == 0 || forceGroup) {
      ObjectHeader objHeader(
          {0,
           0,
           nowLoc.group,
           0,
           0,
           ForwardPreference::Object,
           ObjectStatus::NORMAL,
           folly::none});
      if (session) {
        publishObjectToSession(
            session,
            subscribeID,
            trackAlias,
            std::move(objHeader),
            folly::IOBuf::copyBuffer(ss.str()));
      } else {
        forwarder_.publish(
            std::move(objHeader), folly::IOBuf::copyBuffer(ss.str()));
      }
      objectsPublished++;
    }
    if (!end || nowLoc < *end) {
      auto secBuf = folly::to<std::string>(lt->tm_sec);
      ObjectHeader objHeader(
          {0,
           0,
           nowLoc.group,
           nowLoc.object,
           0,
           ForwardPreference::Object,
           ObjectStatus::NORMAL,
           folly::none});
      if (session) {
        publishObjectToSession(
            session,
            subscribeID,
            trackAlias,
            std::move(objHeader),
            folly::IOBuf::copyBuffer(secBuf));
      } else {
        forwarder_.publish(
            std::move(objHeader), folly::IOBuf::copyBuffer(secBuf));
      }
      objectsPublished++;
    }
    return objectsPublished;
  }

  void publishObjectToSession(
      const std::shared_ptr<MoQSession>& session,
      uint64_t subscribeID,
      uint64_t trackAlias,
      ObjectHeader inObjHeader,
      std::unique_ptr<folly::IOBuf> payload) {
    session->getEventBase()->runImmediatelyOrRunInEventBaseThread(
        [session,
         subscribeID,
         trackAlias,
         objHeader = std::move(inObjHeader),
         buf = payload->clone()]() mutable {
          objHeader.subscribeID = subscribeID;
          objHeader.trackAlias = trackAlias;
          session->publish(std::move(objHeader), 0, std::move(buf), true);
        });
  }

  void unsubscribe(
      std::shared_ptr<MoQSession> session,
      Unsubscribe unsubscribe) {
    forwarder_.removeSession(session, unsubscribe.subscribeID);
  }

  void terminateClientSession(std::shared_ptr<MoQSession> session) override {
    XLOG(INFO) << __func__;
    forwarder_.removeSession(session);
  }

 private:
  [[nodiscard]] FullTrackName dateTrackName() const {
    return FullTrackName({"moq-date", "/date"});
  }
  MoQForwarder forwarder_;
  std::unique_ptr<MoQRelayClient> relayClient_;
};
} // namespace
int main(int argc, char* argv[]) {
  folly::Init init(&argc, &argv, true);
  folly::EventBase evb;
  MoQDateServer moqDateServer(&evb);
  moqDateServer.publishDateLoop().scheduleOn(&evb).start();
  evb.loopForever();
  return 0;
}

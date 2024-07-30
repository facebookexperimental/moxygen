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
DEFINE_bool(stream_per_group, false, "Use one stream for each group");

namespace {
using namespace moxygen;

class MoQDateServer : MoQServer {
 public:
  explicit MoQDateServer(folly::EventBase* evb, ForwardPreference pref)
      : MoQServer(FLAGS_port, FLAGS_cert, FLAGS_key, "/moq-date"),
        forwarder_(dateTrackName()),
        pref_(pref) {
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

    void operator()(SubscribeUpdate subscribeUpdate) const override {
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
        {subReq.subscribeID,
         std::chrono::milliseconds(0),
         MoQSession::resolveGroupOrder(
             GroupOrder::OldestFirst, subReq.groupOrder),
         nowLoc});

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

  bool onSubscribeUpdate(const SubscribeUpdate& subscribeUpdate) {
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
    MoQForwarder forwarder(dateTrackName());
    forwarder.addSubscriber({clientSession, subscribeID, trackAlias, range});
    time_t t =
        range.start.group * 60 + std::max(range.start.object, (uint64_t)1) - 1;
    while (range.start < now && range.start < range.end) {
      auto n =
          publishDate(forwarder, t, false, subscribeID, trackAlias, range.end);
      t++;
      // publishDate publishes two objects for obj = 0
      range.start.object += n;
      if (range.start.object > 60) {
        range.start.group++;
        range.start.object = 0;
      }
    }
    return forwarder.empty();
  }

  folly::coro::Task<void> publishDateLoop() {
    bool first = false;
    auto cancelToken = co_await folly::coro::co_current_cancellation_token;
    while (!cancelToken.isCancellationRequested()) {
      if (!forwarder_.empty()) {
        auto now = std::chrono::system_clock::now();
        auto in_time_t = std::chrono::system_clock::to_time_t(now);

        publishDate(forwarder_, in_time_t, first, 0, 0, folly::none);
        first = false;
      }
      co_await folly::coro::sleep(std::chrono::seconds(1));
    }
  }

  size_t publishDate(
      MoQForwarder& forwarder,
      time_t in_time_t,
      bool forceGroup,
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
          {subscribeID,
           trackAlias,
           nowLoc.group,
           0,
           0,
           pref_,
           ObjectStatus::NORMAL,
           folly::none});
      forwarder.publish(objHeader, folly::IOBuf::copyBuffer(ss.str()));
      objectsPublished++;
    }
    if (!end || nowLoc < *end) {
      auto secBuf = folly::to<std::string>(lt->tm_sec);
      ObjectHeader objHeader(
          {subscribeID,
           trackAlias,
           nowLoc.group,
           nowLoc.object,
           0,
           pref_,
           ObjectStatus::NORMAL,
           folly::none});
      forwarder.publish(objHeader, folly::IOBuf::copyBuffer(secBuf));
      objectsPublished++;
      if (nowLoc.object == 60) {
        objHeader.status = ObjectStatus::END_OF_GROUP;
        objHeader.id++;
        forwarder.publish(std::move(objHeader), nullptr);
      }
    }
    return objectsPublished;
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
  static FullTrackName dateTrackName() {
    return FullTrackName({"moq-date", "/date"});
  }
  MoQForwarder forwarder_;
  std::unique_ptr<MoQRelayClient> relayClient_;
  ForwardPreference pref_{ForwardPreference::Group};
};
} // namespace
int main(int argc, char* argv[]) {
  folly::Init init(&argc, &argv, true);
  folly::EventBase evb;
  MoQDateServer moqDateServer(
      &evb,
      FLAGS_stream_per_group ? ForwardPreference::Group
                             : ForwardPreference::Object);
  moqDateServer.publishDateLoop().scheduleOn(&evb).start();
  evb.loopForever();
  return 0;
}

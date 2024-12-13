/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <folly/coro/Sleep.h>
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
DEFINE_bool(stream_per_object, false, "Use one stream for each object");

namespace {
using namespace moxygen;

class MoQDateServer : MoQServer {
 public:
  explicit MoQDateServer(folly::EventBase* evb, bool streamPerObject)
      : MoQServer(FLAGS_port, FLAGS_cert, FLAGS_key, "/moq-date"),
        forwarder_(dateTrackName()),
        streamPerObject_(streamPerObject) {
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
              {TrackNamespace({"moq-date"})},
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

    void operator()(SubscribeRequest subscribeReq) const override {
      XLOG(INFO) << "SubscribeRequest track ns="
                 << subscribeReq.fullTrackName.trackNamespace
                 << " name=" << subscribeReq.fullTrackName.trackName
                 << " subscribe id=" << subscribeReq.subscribeID
                 << " track alias=" << subscribeReq.trackAlias;
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

    void operator()(Fetch fetch) const override {
      XLOG(INFO) << "Fetch track ns=" << fetch.fullTrackName.trackNamespace
                 << " name=" << fetch.fullTrackName.trackName
                 << " subscribe id=" << fetch.subscribeID;
      if (fetch.fullTrackName != server_.dateTrackName()) {
        clientSession_->fetchError(
            {fetch.subscribeID, 403, "unexpected fetch"});
      } else {
        server_.onFetch(std::move(fetch), clientSession_);
      }
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
    if (range.start < nowLoc) {
      clientSession->subscribeError(
          {subReq.subscribeID, 400, "start in the past, use FETCH"});
      return;
    }
    clientSession->subscribeOk(
        {subReq.subscribeID,
         std::chrono::milliseconds(0),
         MoQSession::resolveGroupOrder(
             GroupOrder::OldestFirst, subReq.groupOrder),
         nowLoc});

    forwarder_.setLatest(nowLoc);
    forwarder_.addSubscriber(
        {std::move(clientSession),
         subReq.subscribeID,
         subReq.trackAlias,
         range});
  }

  void onFetch(Fetch fetch, std::shared_ptr<MoQSession> clientSession) {
    if (fetch.end < fetch.start) {
      clientSession->fetchError({fetch.subscribeID, 400, "No objects"});
      return;
    }
    auto now = std::chrono::system_clock::now();
    auto in_time_t = std::chrono::system_clock::to_time_t(now);
    AbsoluteLocation nowLoc(
        {uint64_t(in_time_t / 60), uint64_t(in_time_t % 60) + 1});
    auto range = toSubscribeRange(
        fetch.start, fetch.end, LocationType::AbsoluteRange, nowLoc);
    if (range.start > nowLoc) {
      clientSession->fetchError(
          {fetch.subscribeID, 400, "fetch starts in future"});
      return;
    }
    range.end = std::min(range.end, nowLoc);
    clientSession->fetchOk(
        {fetch.subscribeID,
         MoQSession::resolveGroupOrder(
             GroupOrder::OldestFirst, fetch.groupOrder),
         0, // not end of track
         nowLoc,
         {}});

    catchup(clientSession, fetch.subscribeID, range, nowLoc);
  }

  bool onSubscribeUpdate(const SubscribeUpdate& subscribeUpdate) {
    return forwarder_.updateSubscriber(subscribeUpdate);
  }

  void catchup(
      std::shared_ptr<MoQSession> clientSession,
      SubscribeID subscribeID,
      SubscribeRange range,
      AbsoluteLocation now) {
    if (range.start.object > 61) {
      XLOG(ERR) << "Invalid start object";
      return;
    }
    time_t t =
        range.start.group * 60 + std::max(range.start.object, (uint64_t)1) - 1;
    auto pubFn = [clientSession, subscribeID](
                     ObjectHeader objHdr,
                     std::unique_ptr<folly::IOBuf> payload,
                     uint64_t payloadOffset,
                     bool eom,
                     bool) {
      objHdr.trackIdentifier = subscribeID;
      if (objHdr.status == ObjectStatus::NORMAL) {
        XLOG(DBG1) << "Publish normal object trackIdentifier="
                   << std::get<SubscribeID>(objHdr.trackIdentifier);
        clientSession
            ->publish(
                objHdr, subscribeID, payloadOffset, std::move(payload), eom)
            .via(clientSession->getEventBase());
      } else {
        clientSession->publishStatus(objHdr, subscribeID);
      }
    };
    while (range.start < now && range.start < range.end) {
      auto n = publishDate(
          pubFn,
          t,
          false,
          subscribeID,
          TrackAlias(0),
          ForwardPreference::Fetch,
          range.end);
      t++;
      // publishDate publishes two objects for obj = 0
      range.start.object += n;
      if (range.start.object > 60) {
        range.start.group++;
        range.start.object = 0;
      }
    }
    // TODO - empty range may log an error?
    clientSession->closeFetchStream(subscribeID);
  }

  folly::coro::Task<void> publishDateLoop() {
    bool first = false;
    auto cancelToken = co_await folly::coro::co_current_cancellation_token;
    auto pubFn = [this](
                     ObjectHeader objHdr,
                     std::unique_ptr<folly::IOBuf> payload,
                     uint64_t payloadOffset,
                     bool eom,
                     bool streamPerObject) {
      forwarder_.publish(
          std::move(objHdr),
          std::move(payload),
          payloadOffset,
          eom,
          streamPerObject);
    };
    while (!cancelToken.isCancellationRequested()) {
      if (!forwarder_.empty()) {
        auto now = std::chrono::system_clock::now();
        auto in_time_t = std::chrono::system_clock::to_time_t(now);
        publishDate(
            pubFn,
            in_time_t,
            first,
            0,
            0,
            ForwardPreference::Subgroup,
            folly::none);
        first = false;
      }
      co_await folly::coro::sleep(std::chrono::seconds(1));
    }
  }

  size_t publishDate(
      const std::function<void(
          ObjectHeader,
          std::unique_ptr<folly::IOBuf>,
          uint64_t,
          bool,
          bool)>& publishFn,
      time_t in_time_t,
      bool forceGroup,
      SubscribeID,
      TrackAlias trackAlias,
      ForwardPreference pref,
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
          {trackAlias,
           nowLoc.group,
           /*subgroup=*/0,
           /*object=*/0,
           /*priority*/ 0,
           pref,
           ObjectStatus::NORMAL,
           folly::none});
      publishFn(
          objHeader,
          folly::IOBuf::copyBuffer(ss.str()),
          0,
          true,
          !end && streamPerObject_);
      objectsPublished++;
    }
    if (!end || nowLoc < *end) {
      auto secBuf = folly::to<std::string>(lt->tm_sec);
      uint64_t subgroup = streamPerObject_ ? nowLoc.object + 1 : 0;
      ObjectHeader objHeader(
          {trackAlias,
           nowLoc.group,
           subgroup,
           nowLoc.object,
           /*priority=*/0,
           pref,
           ObjectStatus::NORMAL,
           folly::none});
      publishFn(
          objHeader,
          folly::IOBuf::copyBuffer(secBuf),
          0,
          true,
          !end && streamPerObject_ && nowLoc.object < 60);
      objectsPublished++;
      if (nowLoc.object == 60) {
        objHeader.status = ObjectStatus::END_OF_GROUP;
        objHeader.id++;
        publishFn(std::move(objHeader), nullptr, 0, true, false);
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
    return FullTrackName({TrackNamespace({"moq-date"}), "/date"});
  }
  MoQForwarder forwarder_;
  std::unique_ptr<MoQRelayClient> relayClient_;
  bool streamPerObject_{false};
};
} // namespace
int main(int argc, char* argv[]) {
  folly::Init init(&argc, &argv, true);
  folly::EventBase evb;
  MoQDateServer moqDateServer(&evb, FLAGS_stream_per_object);
  moqDateServer.publishDateLoop().scheduleOn(&evb).start();
  evb.loopForever();
  return 0;
}

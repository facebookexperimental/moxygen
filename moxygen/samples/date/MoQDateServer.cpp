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
      publishDateLoop().scheduleOn(evb).start();
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
      if (!server_.onSubscribeUpdate(clientSession_, subscribeUpdate)) {
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
    if (!loopRunning_) {
      // start date loop on first server connect
      loopRunning_ = true;
      publishDateLoop().scheduleOn(clientSession->getEventBase()).start();
    }
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
    auto trackPublisher = clientSession->subscribeOk(
        {subReq.subscribeID,
         std::chrono::milliseconds(0),
         MoQSession::resolveGroupOrder(
             GroupOrder::OldestFirst, subReq.groupOrder),
         nowLoc});

    forwarder_.setLatest(nowLoc);
    forwarder_.addSubscriber(std::make_shared<MoQForwarder::Subscriber>(
        std::move(clientSession),
        subReq.subscribeID,
        subReq.trackAlias,
        range,
        std::move(trackPublisher)));
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
    auto fetchPub = clientSession->fetchOk(
        {fetch.subscribeID,
         MoQSession::resolveGroupOrder(
             GroupOrder::OldestFirst, fetch.groupOrder),
         0, // not end of track
         nowLoc,
         {}});

    catchup(fetchPub, range, nowLoc)
        .scheduleOn(clientSession->getEventBase())
        .start();
  }

  bool onSubscribeUpdate(
      const std::shared_ptr<MoQSession>& session,
      const SubscribeUpdate& subscribeUpdate) {
    return forwarder_.updateSubscriber(session, subscribeUpdate);
  }

  Payload minutePayload(uint64_t group) {
    time_t in_time_t = group * 60;
    struct tm local_tm;
    auto lt = ::localtime_r(&in_time_t, &local_tm);
    std::stringstream ss;
    ss << std::put_time(lt, "%Y-%m-%d %H:%M:");
    XLOG(DBG1) << ss.str() << lt->tm_sec;
    return folly::IOBuf::copyBuffer(ss.str());
  }

  Payload secondPayload(uint64_t object) {
    XCHECK_GT(object, 0llu);
    auto secBuf = folly::to<std::string>(object - 1);
    XLOG(DBG1) << (object - 1);
    return folly::IOBuf::copyBuffer(secBuf);
  }

  folly::coro::Task<void> catchup(
      std::shared_ptr<FetchConsumer> fetchPub,
      SubscribeRange range,
      AbsoluteLocation now) {
    if (range.start.object > 61) {
      XLOG(ERR) << "Invalid start object";
      co_return;
    }
    while (range.start < now && range.start < range.end) {
      uint64_t subgroup = streamPerObject_ ? range.start.object : 0;
      folly::Expected<folly::Unit, MoQPublishError> res{folly::unit};
      if (range.start.object == 0) {
        res = fetchPub->object(
            range.start.group,
            subgroup,
            range.start.object,
            minutePayload(range.start.group),
            false);
      } else if (range.start.object <= 60) {
        res = fetchPub->object(
            range.start.group,
            subgroup,
            range.start.object,
            secondPayload(range.start.object),
            false);
      } else {
        res = fetchPub->endOfGroup(
            range.start.group,
            subgroup,
            range.start.object,
            /*finFetch=*/false);
      }
      if (!res) {
        XLOG(ERR) << "catchup error: " << res.error().what();
        if (res.error().code == MoQPublishError::BLOCKED) {
          XLOG(DBG1) << "Fetch blocked, waiting";
          auto awaitRes = fetchPub->awaitReadyToConsume();
          if (!awaitRes) {
            XLOG(ERR) << "awaitReadyToConsume error: "
                      << awaitRes.error().what();
            fetchPub->reset(ResetStreamErrorCode::INTERNAL_ERROR);
            co_return;
          }
          co_await std::move(awaitRes.value());
        } else {
          fetchPub->reset(ResetStreamErrorCode::INTERNAL_ERROR);
          co_return;
        }
      }
      range.start.object++;
      if (range.start.object > 61) {
        range.start.group++;
        range.start.object = 0;
      }
    }
    // TODO - empty range may log an error?
    fetchPub->endOfFetch();
  }

  folly::coro::Task<void> publishDateLoop() {
    auto cancelToken = co_await folly::coro::co_current_cancellation_token;
    std::shared_ptr<SubgroupConsumer> subgroupPublisher;
    while (!cancelToken.isCancellationRequested()) {
      if (!forwarder_.empty()) {
        auto now = std::chrono::system_clock::now();
        auto in_time_t = std::chrono::system_clock::to_time_t(now);
        if (streamPerObject_) {
          publishDate(uint64_t(in_time_t / 60), uint64_t(in_time_t % 60));
        } else {
          subgroupPublisher = publishDate(
              subgroupPublisher,
              uint64_t(in_time_t / 60),
              uint64_t(in_time_t % 60));
        }
      }
      co_await folly::coro::sleep(std::chrono::seconds(1));
    }
  }

  std::shared_ptr<SubgroupConsumer> publishDate(
      std::shared_ptr<SubgroupConsumer> subgroupPublisher,
      uint64_t group,
      uint64_t second) {
    uint64_t subgroup = 0;
    uint64_t object = second;
    if (!subgroupPublisher) {
      subgroupPublisher =
          forwarder_.beginSubgroup(group, subgroup, /*priority=*/0).value();
    }
    if (object == 0) {
      subgroupPublisher->object(0, minutePayload(group), false);
    }
    object++;
    subgroupPublisher->object(object, secondPayload(object), false);
    if (object >= 60) {
      object++;
      subgroupPublisher->endOfGroup(object);
      subgroupPublisher.reset();
    }
    return subgroupPublisher;
  }

  void publishDate(uint64_t group, uint64_t second) {
    uint64_t subgroup = second;
    uint64_t object = second;
    ObjectHeader header{
        TrackAlias(0),
        group,
        subgroup,
        object,
        /*priority=*/0,
        ObjectStatus::NORMAL,
        folly::none};
    if (second == 0) {
      forwarder_.objectStream(header, minutePayload(group));
    }
    header.subgroup++;
    header.id++;
    forwarder_.objectStream(header, secondPayload(header.id));
    if (header.id >= 60) {
      header.subgroup++;
      header.id++;
      header.status = ObjectStatus::END_OF_GROUP;
      forwarder_.objectStream(header, nullptr);
    }
  }

  void unsubscribe(
      std::shared_ptr<MoQSession> session,
      Unsubscribe unsubscribe) {
    forwarder_.removeSession(session);
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
  bool loopRunning_{false};
};
} // namespace
int main(int argc, char* argv[]) {
  folly::Init init(&argc, &argv, true);
  folly::EventBase evb;
  MoQDateServer moqDateServer(&evb, FLAGS_stream_per_object);
  evb.loopForever();
  return 0;
}

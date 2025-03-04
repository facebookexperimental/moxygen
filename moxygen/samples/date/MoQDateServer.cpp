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
DEFINE_bool(quic_transport, false, "Use raw QUIC transport");

namespace {
using namespace moxygen;

class MoQDateServer : public MoQServer,
                      public Publisher,
                      public std::enable_shared_from_this<MoQDateServer> {
 public:
  explicit MoQDateServer(bool streamPerObject)
      : MoQServer(FLAGS_port, FLAGS_cert, FLAGS_key, "/moq-date"),
        forwarder_(dateTrackName()),
        streamPerObject_(streamPerObject) {}

  bool startRelayClient(folly::EventBase* evb) {
    proxygen::URL url(FLAGS_relay_url);
    if (!url.isValid() || !url.hasHost()) {
      XLOG(ERR) << "Invalid url: " << FLAGS_relay_url;
      return false;
    }
    relayClient_ = std::make_unique<MoQRelayClient>(evb, url);
    relayClient_ = std::make_unique<MoQRelayClient>(
        evb,
        url,
        (FLAGS_quic_transport ? MoQClient::TransportType::QUIC
                              : MoQClient::TransportType::H3_WEBTRANSPORT));
    relayClient_
        ->run(
            /*publisher=*/shared_from_this(),
            /*subscriber=*/nullptr,
            {TrackNamespace({"moq-date"})},
            std::chrono::milliseconds(FLAGS_relay_connect_timeout),
            std::chrono::seconds(FLAGS_relay_transaction_timeout))
        .scheduleOn(evb)
        .start();
    loopRunning_ = true;
    publishDateLoop().scheduleOn(evb).start();
    return true;
  }

  void onNewSession(std::shared_ptr<MoQSession> clientSession) override {
    clientSession->setPublishHandler(shared_from_this());
    if (!loopRunning_) {
      // start date loop on first server connect
      loopRunning_ = true;
      publishDateLoop().scheduleOn(clientSession->getEventBase()).start();
    }
  }

  folly::coro::Task<TrackStatusResult> trackStatus(
      TrackStatusRequest trackStatusRequest) override {
    XLOG(DBG1) << __func__ << trackStatusRequest.fullTrackName;
    if (trackStatusRequest.fullTrackName != dateTrackName()) {
      co_return TrackStatus{
          trackStatusRequest.fullTrackName,
          TrackStatusCode::TRACK_NOT_EXIST,
          folly::none};
    }
    // TODO: add other trackSTatus codes
    // TODO: unify this with subscribe. You can get the same information both
    // ways
    auto now = std::chrono::system_clock::now();
    auto in_time_t = std::chrono::system_clock::to_time_t(now);
    co_return TrackStatus{
        trackStatusRequest.fullTrackName,
        TrackStatusCode::IN_PROGRESS,
        AbsoluteLocation{uint64_t(in_time_t / 60), uint64_t(in_time_t % 60)}};
  }

  folly::coro::Task<SubscribeResult> subscribe(
      SubscribeRequest subReq,
      std::shared_ptr<TrackConsumer> consumer) override {
    XLOG(INFO) << "SubscribeRequest track ns="
               << subReq.fullTrackName.trackNamespace
               << " name=" << subReq.fullTrackName.trackName
               << " subscribe id=" << subReq.subscribeID
               << " track alias=" << subReq.trackAlias;
    if (subReq.fullTrackName != dateTrackName()) {
      co_return folly::makeUnexpected(SubscribeError{
          subReq.subscribeID,
          SubscribeErrorCode::TRACK_NOT_EXIST,
          "unexpected subscribe"});
    }
    auto now = std::chrono::system_clock::now();
    auto in_time_t = std::chrono::system_clock::to_time_t(now);
    AbsoluteLocation nowLoc(
        {uint64_t(in_time_t / 60), uint64_t(in_time_t % 60) + 1});
    auto range = toSubscribeRange(subReq, nowLoc);
    if (range.start < nowLoc) {
      co_return folly::makeUnexpected(SubscribeError{
          subReq.subscribeID,
          SubscribeErrorCode::INVALID_RANGE,
          "start in the past, use FETCH"});
    }
    forwarder_.setLatest(nowLoc);
    co_return forwarder_.addSubscriber(
        MoQSession::getRequestSession(), subReq, std::move(consumer));
  }

  class FetchHandle : public Publisher::FetchHandle {
   public:
    explicit FetchHandle(FetchOk ok) : Publisher::FetchHandle(std::move(ok)) {}
    void fetchCancel() override {
      cancelSource.requestCancellation();
    }
    folly::CancellationSource cancelSource;
  };

  folly::coro::Task<FetchResult> fetch(
      Fetch fetch,
      std::shared_ptr<FetchConsumer> consumer) override {
    auto clientSession = MoQSession::getRequestSession();
    XLOG(INFO) << "Fetch track ns=" << fetch.fullTrackName.trackNamespace
               << " name=" << fetch.fullTrackName.trackName
               << " subscribe id=" << fetch.subscribeID;
    if (fetch.fullTrackName != dateTrackName()) {
      co_return folly::makeUnexpected(FetchError{
          fetch.subscribeID,
          FetchErrorCode::TRACK_NOT_EXIST,
          "unexpected fetch"});
    }
    auto [standalone, joining] = fetchType(fetch);
    StandaloneFetch sf;
    if (joining) {
      auto res = forwarder_.resolveJoiningFetch(clientSession, *joining);
      if (res.hasError()) {
        XLOG(ERR) << "Bad joining fetch id=" << fetch.subscribeID
                  << " err=" << res.error().reasonPhrase;
        co_return folly::makeUnexpected(res.error());
      }
      sf = StandaloneFetch(res.value().start, res.value().end);
      standalone = &sf;
    }
    if (standalone->end < standalone->start &&
        !(standalone->start.group == standalone->end.group &&
          standalone->end.object == 0)) {
      co_return folly::makeUnexpected(FetchError{
          fetch.subscribeID, FetchErrorCode::INVALID_RANGE, "No objects"});
    }
    auto now = std::chrono::system_clock::now();
    auto in_time_t = std::chrono::system_clock::to_time_t(now);
    AbsoluteLocation nowLoc(
        {uint64_t(in_time_t / 60), uint64_t(in_time_t % 60) + 1});
    auto range = toSubscribeRange(
        standalone->start,
        standalone->end,
        LocationType::AbsoluteRange,
        nowLoc);
    if (range.start > nowLoc) {
      co_return folly::makeUnexpected(FetchError{
          fetch.subscribeID,
          FetchErrorCode::INVALID_RANGE,
          "fetch starts in future"});
    }

    auto fetchHandle = std::make_shared<FetchHandle>(FetchOk{
        fetch.subscribeID,
        MoQSession::resolveGroupOrder(
            GroupOrder::OldestFirst, fetch.groupOrder),
        0, // not end of track
        nowLoc,
        {}});
    folly::coro::co_withCancellation(
        fetchHandle->cancelSource.getToken(),
        catchup(std::move(consumer), range, nowLoc))
        .scheduleOn(clientSession->getEventBase())
        .start();
    co_return fetchHandle;
  }

  void goaway(Goaway goaway) override {
    XLOG(INFO) << "Processing goaway uri=" << goaway.newSessionUri;
    auto session = MoQSession::getRequestSession();
    if (relayClient_ && relayClient_->getSession() == session) {
      // TODO: relay is going away
    } else {
      forwarder_.removeSession(session);
    }
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
      co_return;
    }
    auto token = co_await folly::coro::co_current_cancellation_token;
    while (!token.isCancellationRequested() && range.start <= now &&
           range.start < range.end) {
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
    if (token.isCancellationRequested()) {
      fetchPub->reset(ResetStreamErrorCode::CANCELLED);
    } else {
      // TODO - empty range may log an error?
      XLOG(ERR) << "endOfFetch";
      XLOG(ERR) << "Range: start=" << range.start.group << "."
                << range.start.object << " end=" << range.end.group << "."
                << range.end.object;
      fetchPub->endOfFetch();
    }
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

  void terminateClientSession(std::shared_ptr<MoQSession> session) override {
    XLOG(INFO) << __func__;
    forwarder_.removeSession(session);
  }

 private:
  static FullTrackName dateTrackName() {
    return FullTrackName({TrackNamespace({"moq-date"}), "date"});
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
  auto server = std::make_shared<MoQDateServer>(FLAGS_stream_per_object);
  if (!FLAGS_relay_url.empty() && !server->startRelayClient(&evb)) {
    return 1;
  }
  evb.loopForever();
  return 0;
}

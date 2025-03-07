/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <folly/coro/Sleep.h>
#include <moxygen/MoQLocation.h>
#include <moxygen/MoQServer.h>
#include <moxygen/MoQWebTransportClient.h>
#include <moxygen/relay/MoQForwarder.h>
#include <moxygen/relay/MoQRelayClient.h>
#include <iomanip>

using namespace quic::samples;
using namespace proxygen;

DEFINE_string(relay_url, "", "Use specified relay");
DEFINE_int32(relay_connect_timeout, 1000, "Connect timeout (ms)");
DEFINE_int32(relay_transaction_timeout, 120, "Transaction timeout (s)");
DEFINE_string(cert, "", "Cert path");
DEFINE_string(key, "", "Key path");
DEFINE_int32(port, 9667, "Server Port");
DEFINE_string(
    mode,
    "spg",
    "Transmission mode for track: stream-per-group (spg), "
    "stream-per-object(spo), datagram");
DEFINE_bool(quic_transport, false, "Use raw QUIC transport");

namespace {
using namespace moxygen;

static const Extensions kExtensions{
    {0xacedecade, 1977, {}},
    {0xdeadbeef, 0, {1, 2, 3, 4, 5}}};

class MoQDateServer : public MoQServer,
                      public Publisher,
                      public std::enable_shared_from_this<MoQDateServer> {
 public:
  enum class Mode { STREAM_PER_GROUP, STREAM_PER_OBJECT, DATAGRAM };

  explicit MoQDateServer(Mode mode)
      : MoQServer(FLAGS_port, FLAGS_cert, FLAGS_key, "/moq-date"),
        forwarder_(dateTrackName()),
        mode_(mode) {}

  bool startRelayClient() {
    proxygen::URL url(FLAGS_relay_url);
    if (!url.isValid() || !url.hasHost()) {
      XLOG(ERR) << "Invalid url: " << FLAGS_relay_url;
      return false;
    }
    auto evb = getWorkerEvbs()[0];
    relayClient_ = std::make_unique<MoQRelayClient>(
        (FLAGS_quic_transport
             ? std::make_unique<MoQClient>(evb, url)
             : std::make_unique<MoQWebTransportClient>(evb, url)));
    relayClient_
        ->run(
            /*publisher=*/shared_from_this(),
            /*subscriber=*/nullptr,
            {TrackNamespace({"moq-date"})},
            std::chrono::milliseconds(FLAGS_relay_connect_timeout),
            std::chrono::seconds(FLAGS_relay_transaction_timeout))
        .scheduleOn(evb)
        .start();
    return true;
  }

  void onNewSession(std::shared_ptr<MoQSession> clientSession) override {
    clientSession->setPublishHandler(shared_from_this());
  }

  std::pair<uint64_t, uint64_t> now() {
    auto now = std::chrono::system_clock::now();
    auto in_time_t = std::chrono::system_clock::to_time_t(now);
    // +1 is because object two objects are published at second=0
    return {uint64_t(in_time_t / 60), uint64_t(in_time_t % 60)};
  }

  AbsoluteLocation nowLocation() {
    auto [minute, second] = now();
    // +1 is because object two objects are published at second=0
    return {minute, second + 1};
  }

  AbsoluteLocation updateLatest() {
    if (!loopRunning_) {
      forwarder_.setLatest(nowLocation());
    }
    return *forwarder_.latest();
  }

  folly::coro::Task<TrackStatusResult> trackStatus(
      TrackStatusRequest trackStatusRequest) override {
    XLOG(DBG1) << __func__ << trackStatusRequest.fullTrackName;
    if (trackStatusRequest.fullTrackName != dateTrackName()) {
      co_return TrackStatus{
          std::move(trackStatusRequest.fullTrackName),
          TrackStatusCode::TRACK_NOT_EXIST,
          folly::none};
    }
    // TODO: add other trackSTatus codes
    // TODO: unify this with subscribe. You can get the same information both
    // ways
    auto latest = updateLatest();
    co_return TrackStatus{
        std::move(trackStatusRequest.fullTrackName),
        TrackStatusCode::IN_PROGRESS,
        latest};
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
    auto latest = updateLatest();
    if (subReq.locType == LocationType::AbsoluteRange &&
        subReq.endGroup < latest.group) {
      co_return folly::makeUnexpected(SubscribeError{
          subReq.subscribeID,
          SubscribeErrorCode::INVALID_RANGE,
          "Range in the past, use FETCH"});
      // start may be in the past, it will get adjusted forward to latest
    }

    auto session = MoQSession::getRequestSession();
    if (!loopRunning_) {
      loopRunning_ = true;
      publishDateLoop().scheduleOn(session->getEventBase()).start();
    }

    co_return forwarder_.addSubscriber(
        std::move(session), subReq, std::move(consumer));
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
    auto latest = updateLatest();
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
    } else if (standalone->end > latest) {
      standalone->end = latest;
      standalone->end.object++; // exclusive range, include latest
    }
    if (standalone->end < standalone->start &&
        !(standalone->start.group == standalone->end.group &&
          standalone->end.object == 0)) {
      co_return folly::makeUnexpected(FetchError{
          fetch.subscribeID, FetchErrorCode::INVALID_RANGE, "No objects"});
    }
    if (standalone->start > latest) {
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
        latest,
        {}});
    folly::coro::co_withCancellation(
        fetchHandle->cancelSource.getToken(),
        catchup(std::move(consumer), {standalone->start, standalone->end}))
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
      SubscribeRange range) {
    if (range.start.object > 61) {
      co_return;
    }
    XLOG(ERR) << "Range: start=" << range.start.group << "."
              << range.start.object << " end=" << range.end.group << "."
              << range.end.object;
    auto token = co_await folly::coro::co_current_cancellation_token;
    while (!token.isCancellationRequested() && range.start < range.end) {
      uint64_t subgroup =
          mode_ == Mode::STREAM_PER_OBJECT ? range.start.object : 0;
      folly::Expected<folly::Unit, MoQPublishError> res{folly::unit};
      if (range.start.object == 0) {
        res = fetchPub->object(
            range.start.group,
            subgroup,
            range.start.object,
            minutePayload(range.start.group),
            kExtensions);
      } else if (range.start.object <= 60) {
        res = fetchPub->object(
            range.start.group,
            subgroup,
            range.start.object,
            secondPayload(range.start.object));
      } else {
        res = fetchPub->endOfGroup(
            range.start.group, subgroup, range.start.object);
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
      if (forwarder_.empty()) {
        forwarder_.setLatest(nowLocation());
      } else {
        auto [minute, second] = now();
        switch (mode_) {
          case Mode::STREAM_PER_GROUP:
            subgroupPublisher = publishDate(subgroupPublisher, minute, second);
            break;
          case Mode::STREAM_PER_OBJECT:
            publishDate(minute, second);
            break;
          case Mode::DATAGRAM:
            publishDategram(minute, second);
            break;
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
      subgroupPublisher->object(0, minutePayload(group), kExtensions, false);
    }
    object++;
    subgroupPublisher->object(
        object, secondPayload(object), noExtensions(), false);
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
        noExtensions(),
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

  void publishDategram(uint64_t group, uint64_t second) {
    uint64_t object = second;
    ObjectHeader header{
        TrackAlias(0),
        group,
        0, // subgroup unused for datagrams
        object,
        /*p=*/0, // priority
        ObjectStatus::NORMAL,
        kExtensions,
        folly::none};
    if (second == 0) {
      forwarder_.datagram(header, minutePayload(group));
    }
    header.id++;
    header.extensions.clear();
    forwarder_.datagram(header, secondPayload(header.id));
    if (header.id >= 60) {
      header.id++;
      header.status = ObjectStatus::END_OF_GROUP;
      forwarder_.datagram(header, nullptr);
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
  Mode mode_{Mode::STREAM_PER_GROUP};
  bool loopRunning_{false};
};
} // namespace
int main(int argc, char* argv[]) {
  folly::Init init(&argc, &argv, true);
  folly::EventBase evb;
  MoQDateServer::Mode mode;
  if (FLAGS_mode == "spg") {
    mode = MoQDateServer::Mode::STREAM_PER_GROUP;
  } else if (FLAGS_mode == "spo") {
    mode = MoQDateServer::Mode::STREAM_PER_OBJECT;
  } else if (FLAGS_mode == "datagram") {
    mode = MoQDateServer::Mode::DATAGRAM;
  } else {
    XLOG(ERR) << "Invalid mode: " << FLAGS_mode;
    return 1;
  }
  auto server = std::make_shared<MoQDateServer>(mode);
  if (!FLAGS_relay_url.empty() && !server->startRelayClient()) {
    return 1;
  }
  evb.loopForever();
  return 0;
}

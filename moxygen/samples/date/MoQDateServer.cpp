/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <folly/coro/BlockingWait.h>
#include <folly/coro/Sleep.h>
#include <moxygen/MoQClient.h>
#include <moxygen/MoQLocation.h>
#include <moxygen/MoQServer.h>
#include <moxygen/MoQWebTransportClient.h>
#include <moxygen/events/MoQFollyExecutorImpl.h>
#include <moxygen/mlog/FileMLogger.h>
#include <moxygen/mlog/FileMLoggerFactory.h>
#include <moxygen/relay/MoQForwarder.h>
#include <moxygen/relay/MoQRelayClient.h>
#include <moxygen/util/InsecureVerifierDangerousDoNotUseInProduction.h>
#include <moxygen/util/SignalHandler.h>
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
DEFINE_bool(publish, false, "Send PUBLISH to subscriber");
DEFINE_string(ns, "moq-date", "Namespace for date track");
DEFINE_bool(
    use_legacy_setup,
    false,
    "If true, use only moq-00 ALPN (legacy). If false, use latest "
    "draft ALPN with fallback to legacy");
DEFINE_int32(delivery_timeout, 0, "the delivery timeout in ms for server");
DEFINE_bool(
    insecure,
    false,
    "Use insecure verifier (skip certificate validation)");
DEFINE_string(mlog_path, "", "Path to mlog file");

namespace {
using namespace moxygen;

uint8_t extTestBuff[5] = {0x01, 0x02, 0x03, 0x04, 0x05};
static const Extensions kExtensions{
    std::vector<Extension>{
        {0xacedecade, 1977},
        {0xdeadbeef,
         folly::IOBuf::copyBuffer(extTestBuff, sizeof(extTestBuff))}},
    {} // empty immutable extensions
};

class DateSubscriptionHandle : public Publisher::SubscriptionHandle {
 public:
  explicit DateSubscriptionHandle() : Publisher::SubscriptionHandle() {}

  // To Be Implemented
  void unsubscribe() override {}

  // To Be Implemented
  folly::coro::Task<folly::Expected<SubscribeUpdateOk, SubscribeUpdateError>>
  subscribeUpdate(SubscribeUpdate update) override {
    co_return folly::makeUnexpected(
        SubscribeUpdateError{
            update.requestID,
            SubscribeUpdateErrorCode::NOT_SUPPORTED,
            "Subscribe update not implemented"});
  }
};

// DatePublisher - Publisher logic with no server dependencies
class DatePublisher : public Publisher {
 public:
  enum class Mode { STREAM_PER_GROUP, STREAM_PER_OBJECT, DATAGRAM };

  explicit DatePublisher(
      Mode mode,
      std::string ns = "moq-date",
      int deliveryTimeout = 0)
      : mode_(mode), ns_(std::move(ns)), deliveryTimeout_(deliveryTimeout) {}

  void ensureForwarder(folly::Executor* executor) {
    if (!forwarder_) {
      forwarder_ = std::make_unique<MoQForwarder>(dateTrackName(), executor);
    }
  }

  folly::coro::Task<PublishRequest> callPublish(
      MoQRelayClient* relayClient,
      TrackNamespace ns,
      uint64_t requestId,
      MoQExecutor::KeepAlive executor) {
    // Form PublishRequest
    PublishRequest req{
        requestId,
        FullTrackName{ns, "date"},
        TrackAlias(requestId),
        GroupOrder::Default,
        std::nullopt,
        true,
    };

    XLOG(INFO) << "PublishRequest track ns=" << req.fullTrackName.trackNamespace
               << " name=" << req.fullTrackName.trackName
               << " requestID=" << req.requestID
               << " track alias=" << req.trackAlias;

    // Use relayClient to publish to relayServer
    auto session = relayClient->getSession();

    // Create a default handle
    auto handle = std::make_shared<DateSubscriptionHandle>();

    auto publishResponse = session->publish(req, handle);
    if (!publishResponse.hasValue()) {
      XLOG(ERR) << "Publish error: " << publishResponse.error().reasonPhrase;
      co_return req;
    }
    auto consumer = publishResponse.value().consumer;
    // Begin a subgroup on consumer
    auto subConsumer = consumer->beginSubgroup(0, 0, 128);
    if (subConsumer.hasError()) {
      XLOG(ERR) << "Subgroup error: " << subConsumer.error().what();
      co_return req;
    }

    // Transform PubReq to SubReq
    SubscribeRequest subReq = {
        .requestID = req.requestID,
        .fullTrackName = req.fullTrackName,
        .groupOrder = req.groupOrder,
        .forward = req.forward,
        .locType = LocationType::LargestObject};

    // Add as a subscriber to forwarder
    ensureForwarder(executor.get());
    forwarder_->addSubscriber(session, subReq, consumer);

    if (!loopRunning_) {
      loopRunning_ = true;
      publishDateLoop(subConsumer.value()).scheduleOn(executor.get()).start();
    }

    co_return req;
  }

  void stopPublishLoop() {
    loopCancelSource_.requestCancellation();
  }

  void removeSubscriber(
      std::shared_ptr<MoQSession> session,
      std::optional<PublishDone> pubDone,
      const std::string& reason) {
    forwarder_->removeSubscriber(
        std::move(session), std::move(pubDone), reason);
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

  AbsoluteLocation updateLargest() {
    if (!loopRunning_) {
      forwarder_->setLargest(nowLocation());
    }
    return *forwarder_->largest();
  }

  folly::coro::Task<TrackStatusResult> trackStatus(
      TrackStatus trackStatus) override {
    XLOG(DBG1) << __func__ << trackStatus.fullTrackName;
    if (trackStatus.fullTrackName != dateTrackName()) {
      co_return folly::makeUnexpected(
          TrackStatusError{
              trackStatus.requestID,
              TrackStatusErrorCode::TRACK_NOT_EXIST,
              "The requested track does not exist"});
    }
    auto largest = updateLargest();
    co_return TrackStatusOk{
        .requestID = trackStatus.requestID,
        .expires = std::chrono::milliseconds(0),
        .groupOrder = GroupOrder::OldestFirst,
        .largest = largest,
        .fullTrackName = trackStatus.fullTrackName,
        .statusCode = TrackStatusCode(0)};
  }

  folly::coro::Task<SubscribeResult> subscribe(
      SubscribeRequest subReq,
      std::shared_ptr<TrackConsumer> consumer) override {
    XLOG(INFO) << "SubscribeRequest track ns="
               << subReq.fullTrackName.trackNamespace
               << " name=" << subReq.fullTrackName.trackName
               << " requestID=" << subReq.requestID;
    if (subReq.fullTrackName != dateTrackName()) {
      co_return folly::makeUnexpected(
          SubscribeError{
              subReq.requestID,
              SubscribeErrorCode::TRACK_NOT_EXIST,
              "unexpected subscribe"});
    }
    auto largest = updateLargest();
    if (subReq.locType == LocationType::AbsoluteRange &&
        subReq.endGroup < largest.group) {
      co_return folly::makeUnexpected(
          SubscribeError{
              subReq.requestID,
              SubscribeErrorCode::INVALID_RANGE,
              "Range in the past, use FETCH"});
    }

    auto alias = TrackAlias(subReq.requestID.value);
    consumer->setTrackAlias(alias);
    auto session = MoQSession::getRequestSession();
    ensureForwarder(session->getExecutor());
    if (!loopRunning_) {
      loopRunning_ = true;
      co_withExecutor(session->getExecutor(), publishDateLoop()).start();
    }

    forwarder_->setDeliveryTimeout(deliveryTimeout_);
    auto subscriber = forwarder_->addSubscriber(
        std::move(session), subReq, std::move(consumer));
    co_return subscriber;
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
               << " requestID=" << fetch.requestID;
    if (fetch.fullTrackName != dateTrackName()) {
      co_return folly::makeUnexpected(
          FetchError{
              fetch.requestID,
              FetchErrorCode::TRACK_NOT_EXIST,
              "unexpected fetch"});
    }
    auto largest = updateLargest();
    auto [standalone, joining] = fetchType(fetch);
    StandaloneFetch sf;
    if (joining) {
      auto res = forwarder_->resolveJoiningFetch(clientSession, *joining);
      if (res.hasError()) {
        XLOG(ERR) << "Bad joining fetch id=" << fetch.requestID
                  << " err=" << res.error().reasonPhrase;
        co_return folly::makeUnexpected(res.error());
      }
      sf = StandaloneFetch(res.value().start, res.value().end);
      standalone = &sf;
    } else if (standalone->end > largest) {
      standalone->end = largest;
      standalone->end.object++; // exclusive range, include largest
    }
    if (standalone->end < standalone->start &&
        !(standalone->start.group == standalone->end.group &&
          standalone->end.object == 0)) {
      co_return folly::makeUnexpected(
          FetchError{
              fetch.requestID, FetchErrorCode::INVALID_RANGE, "No objects"});
    }
    if (standalone->start > largest) {
      co_return folly::makeUnexpected(
          FetchError{
              fetch.requestID,
              FetchErrorCode::INVALID_RANGE,
              "fetch starts in future"});
    }
    XLOG(DBG1) << "Fetch {" << standalone->start.group << ","
               << standalone->start.object << "}.." << standalone->end.group
               << "," << standalone->end.object << "}";

    auto fetchHandle = std::make_shared<FetchHandle>(FetchOk{
        fetch.requestID,
        MoQSession::resolveGroupOrder(
            GroupOrder::OldestFirst, fetch.groupOrder),
        0, // not end of track
        largest});
    co_withExecutor(
        clientSession->getExecutor(),
        folly::coro::co_withCancellation(
            fetchHandle->cancelSource.getToken(),
            catchup(std::move(consumer), {standalone->start, standalone->end})))
        .start();
    co_return fetchHandle;
  }

  void goaway(Goaway goaway) override {
    XLOG(INFO) << "Processing goaway uri=" << goaway.newSessionUri;
    auto session = MoQSession::getRequestSession();
    // TODO: relay is going away
    forwarder_->removeSubscriber(session, std::nullopt, "goaway");
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

  folly::coro::Task<void> publishDateLoop(
      std::shared_ptr<SubgroupConsumer> subConsumer = nullptr) {
    auto cancelToken = co_await folly::coro::co_current_cancellation_token;
    std::shared_ptr<SubgroupConsumer> subgroupPublisher;
    uint64_t currentMinute = now().first;
    if (subConsumer) {
      subgroupPublisher = subConsumer;
    }
    while (!cancelToken.isCancellationRequested() &&
           !loopCancelSource_.isCancellationRequested()) {
      auto [minute, second] = now();
      if (forwarder_->empty()) {
        forwarder_->setLargest(nowLocation());
        // Reset subgroupPublisher when crossing minute boundary
        // Otherwise we try to use the same subgroup publisher and publish does
        // not happen
        if (minute != currentMinute) {
          subgroupPublisher.reset();
          currentMinute = minute;
        }
      } else {
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
          forwarder_->beginSubgroup(group, subgroup, /*priority=*/0).value();
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
        group,
        subgroup,
        object,
        /*priorityIn=*/0,
        ObjectStatus::NORMAL,
        noExtensions(),
        std::nullopt};
    if (second == 0) {
      forwarder_->objectStream(header, minutePayload(group));
    }
    header.subgroup++;
    header.id++;
    forwarder_->objectStream(header, secondPayload(header.id));
    if (header.id >= 60) {
      header.subgroup++;
      header.id++;
      header.status = ObjectStatus::END_OF_GROUP;
      forwarder_->objectStream(header, nullptr);
    }
  }

  void publishDategram(uint64_t group, uint64_t second) {
    uint64_t object = second;
    ObjectHeader header{
        group,
        0, // subgroup unused for datagrams
        object,
        /*priorityIn=*/0, // priority
        ObjectStatus::NORMAL,
        noExtensions(),
        std::nullopt};
    if (second == 0) {
      forwarder_->datagram(header, minutePayload(group));
    }
    header.id++;
    forwarder_->datagram(header, secondPayload(header.id));
    if (header.id >= 60) {
      header.id++;
      header.status = ObjectStatus::END_OF_GROUP;
      forwarder_->datagram(header, nullptr);
    }
  }

 private:
  FullTrackName dateTrackName() const {
    return FullTrackName({TrackNamespace({ns_}), "date"});
  }

  Mode mode_{Mode::STREAM_PER_GROUP};
  std::string ns_;
  int deliveryTimeout_;
  std::unique_ptr<MoQForwarder> forwarder_;
  bool loopRunning_{false};
  folly::CancellationSource loopCancelSource_;
};

// MoQDateServer - Wrapper for MoQServer to manage DatePublisher
class MoQDateServer : public MoQServer {
 public:
  MoQDateServer(
      const std::string& cert,
      const std::string& key,
      const std::string& endpoint,
      std::shared_ptr<DatePublisher> publisher)
      : MoQServer(cert, key, endpoint), publisher_(std::move(publisher)) {}

  MoQDateServer(
      std::shared_ptr<const fizz::server::FizzServerContext> fizzContext,
      const std::string& endpoint,
      std::shared_ptr<DatePublisher> publisher)
      : MoQServer(std::move(fizzContext), endpoint),
        publisher_(std::move(publisher)) {}

  void onNewSession(std::shared_ptr<MoQSession> clientSession) override {
    clientSession->setPublishHandler(publisher_);
  }

  void terminateClientSession(std::shared_ptr<MoQSession> session) override {
    XLOG(INFO) << __func__;
    publisher_->removeSubscriber(
        std::move(session), std::nullopt, "terminateClientSession");
  }

 private:
  std::shared_ptr<DatePublisher> publisher_;
};

std::unique_ptr<MoQRelayClient> createRelayClient(
    folly::EventBase* workerEvb,
    std::shared_ptr<DatePublisher> publisher,
    std::shared_ptr<MLoggerFactory> loggerFactory) {
  proxygen::URL url(FLAGS_relay_url);
  if (!url.isValid() || !url.hasHost()) {
    XLOG(ERR) << "Invalid url: " << FLAGS_relay_url;
    return nullptr;
  }

  auto moqEvb = std::make_unique<MoQFollyExecutorImpl>(workerEvb);

  auto verifier = FLAGS_insecure
      ? std::make_shared<
            moxygen::test::InsecureVerifierDangerousDoNotUseInProduction>()
      : nullptr;

  auto relayClient = std::make_unique<MoQRelayClient>(
      (FLAGS_quic_transport ? std::make_unique<MoQClient>(
                                  moqEvb->keepAlive(),
                                  url,
                                  MoQRelaySession::createRelaySessionFactory(),
                                  verifier)
                            : std::make_unique<MoQWebTransportClient>(
                                  moqEvb->keepAlive(),
                                  url,
                                  MoQRelaySession::createRelaySessionFactory(),
                                  verifier)));

  if (loggerFactory) {
    relayClient->setLogger(loggerFactory->createMLogger());
  }

  std::vector<std::string> alpns =
      getDefaultMoqtProtocols(!FLAGS_use_legacy_setup);

  folly::coro::blockingWait(
      relayClient
          ->setup(
              /*publisher=*/publisher,
              /*subscriber=*/nullptr,
              std::chrono::milliseconds(FLAGS_relay_connect_timeout),
              std::chrono::seconds(FLAGS_relay_transaction_timeout),
              quic::TransportSettings(),
              alpns)
          .scheduleOn(workerEvb)
          .start());

  relayClient->run(/*publisher=*/publisher, {TrackNamespace(FLAGS_ns, "/")})
      .scheduleOn(moqEvb.get())
      .start();

  if (FLAGS_publish) {
    publisher
        ->callPublish(
            relayClient.get(),
            TrackNamespace(FLAGS_ns, "/"),
            0,
            moqEvb->keepAlive())
        .scheduleOn(workerEvb)
        .start();
  }

  return relayClient;
}

} // namespace

int main(int argc, char* argv[]) {
  folly::Init init(&argc, &argv, true);

  // Parse mode
  DatePublisher::Mode mode;
  if (FLAGS_mode == "spg") {
    mode = DatePublisher::Mode::STREAM_PER_GROUP;
  } else if (FLAGS_mode == "spo") {
    mode = DatePublisher::Mode::STREAM_PER_OBJECT;
  } else if (FLAGS_mode == "datagram") {
    mode = DatePublisher::Mode::DATAGRAM;
  } else {
    XLOG(ERR) << "Invalid mode: " << FLAGS_mode;
    return 1;
  }

  // Create publisher
  auto publisher =
      std::make_shared<DatePublisher>(mode, FLAGS_ns, FLAGS_delivery_timeout);

  // Create logger factory if mlog path is specified
  std::shared_ptr<moxygen::FileMLoggerFactory> loggerFactory;
  if (!FLAGS_mlog_path.empty()) {
    loggerFactory = std::make_shared<moxygen::FileMLoggerFactory>(
        FLAGS_mlog_path, moxygen::VantagePoint::SERVER);
  }

  folly::EventBase evb;
  std::shared_ptr<MoQDateServer> server;

  if (FLAGS_insecure) {
    server = std::make_shared<MoQDateServer>(
        quic::samples::createFizzServerContextWithInsecureDefault(
            []() {
              std::vector<std::string> alpns = {"h3"};
              auto moqt = getDefaultMoqtProtocols(!FLAGS_use_legacy_setup);
              alpns.insert(alpns.end(), moqt.begin(), moqt.end());
              return alpns;
            }(),
            fizz::server::ClientAuthMode::None,
            "" /* cert */,
            "" /* key */),
        "/moq-date",
        publisher);
  } else {
    server = std::make_shared<MoQDateServer>(
        FLAGS_cert, FLAGS_key, "/moq-date", publisher);
  }

  if (loggerFactory) {
    server->setMLoggerFactory(loggerFactory);
  }

  folly::SocketAddress addr("::", FLAGS_port);
  server->start(addr);
  server->waitUntilInitialized();

  // Create relay client if relay URL is specified
  std::unique_ptr<MoQRelayClient> relayClient;
  if (!FLAGS_relay_url.empty()) {
    relayClient =
        createRelayClient(server->getWorkerEvbs()[0], publisher, loggerFactory);
    if (!relayClient) {
      return 1;
    }
  }

  moxygen::SignalHandler handler(
      &evb, [&publisher, &server, &relayClient](int) {
        publisher->stopPublishLoop();
        if (relayClient) {
          relayClient->shutdown();
        }
        server->stop();
      });

  evb.loopForever();

  return 0;
}

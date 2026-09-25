/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <moxygen/samples/media_server/MoQMediaListeners.h>

#include <moxygen/MoQVersions.h>
#include <moxygen/ObjectReceiver.h>
#include <moxygen/events/MoQFollyExecutorImpl.h>
#include <moxygen/relay/MoQRelayClient.h>
#include <moxygen/samples/media_server/BroadcastFactory.h>
#include <moxygen/samples/media_server/MediaSourceResolver.h>
#include <moxygen/samples/media_server/MoQBroadcast.h>
#include <moxygen/samples/media_server/sources/CatalogSource.h>
#include <moxygen/samples/util/Utils.h>
#include <moxygen/util/InsecureVerifierDangerousDoNotUseInProduction.h>

#include <folly/coro/BlockingWait.h>
#include <folly/coro/Promise.h>
#include <folly/coro/Timeout.h>
#include <folly/io/async/ScopedEventBaseThread.h>
#include <folly/net/NetOps.h>
#include <folly/portability/GTest.h>

#include <atomic>
#include <memory>
#include <string>
#include <system_error>
#include <vector>

using namespace std::chrono_literals;

namespace moxygen::media_server::test {
namespace {

const std::string kCatalogDoc = R"({"version":"draft-01","tracks":[]})";
const FullTrackName kCatalogFtn{
    TrackNamespace("file/test", "/"),
    std::string(kCatalogTrackName)};

class CatalogOnlyResolver : public MediaSourceResolver {
 public:
  folly::coro::Task<std::shared_ptr<SegmentSource>> openTrack(
      const TrackNamespace& /*ns*/,
      const std::string& trackName) override {
    if (trackName != kCatalogTrackName) {
      co_return nullptr;
    }
    co_return std::make_shared<CatalogSource>(kCatalogDoc);
  }
};

class CountingFactory : public BroadcastFactory {
 public:
  explicit CountingFactory(folly::EventBase* evb) : evb_(evb) {}

  std::shared_ptr<MoQBroadcast> makeBroadcast(
      const TrackNamespace& ns) override {
    EXPECT_TRUE(evb_->isInEventBaseThread())
        << "session request reached the dispatcher off its EventBase";
    ++broadcastsMade;
    return std::make_shared<MoQBroadcast>(
        ns, std::make_shared<CatalogOnlyResolver>(), evb_);
  }

  std::atomic<int> broadcastsMade{0};

 private:
  folly::EventBase* evb_;
};

class NopCallback : public ObjectReceiverCallback {
 public:
  FlowControlState onObject(
      std::optional<TrackAlias> /*trackAlias*/,
      const ObjectHeader& /*header*/,
      Payload /*payload*/) override {
    return FlowControlState::UNBLOCKED;
  }
  void onObjectStatus(
      std::optional<TrackAlias> /*trackAlias*/,
      const ObjectHeader& /*header*/) override {}
  void onEndOfStream() override {}
  void onError(ResetStreamErrorCode /*error*/) override {}
  void onPublishDone(PublishDone /*done*/) override {}
};

class CatalogCallback : public NopCallback {
 public:
  explicit CatalogCallback(folly::coro::Promise<std::string> doc)
      : doc_(std::move(doc)) {}

  FlowControlState onObject(
      std::optional<TrackAlias> /*trackAlias*/,
      const ObjectHeader& /*header*/,
      Payload payload) override {
    if (payload) {
      doc_.trySetValue(payload->moveToFbString().toStdString());
    }
    return FlowControlState::UNBLOCKED;
  }

 private:
  folly::coro::Promise<std::string> doc_;
};

// Binds `addr` without SO_REUSEPORT; returns the socket, which the caller must
// close, or an invalid one if the bind failed.
folly::NetworkSocket bindExclusive(const folly::SocketAddress& addr, int type) {
  auto fd = folly::netops::socket(addr.getFamily(), type, 0);
  sockaddr_storage storage{};
  const auto len = addr.getAddress(&storage);
  if (folly::netops::bind(fd, reinterpret_cast<sockaddr*>(&storage), len) !=
      0) {
    folly::netops::close(fd);
    return folly::NetworkSocket();
  }
  return fd;
}

// A client session holding a catalog subscription open, so the broadcast it
// joined stays alive while another client subscribes.
struct CatalogSubscriber {
  std::unique_ptr<MoQRelayClient> client;
  std::shared_ptr<Publisher::SubscriptionHandle> subscription;
  std::shared_ptr<Publisher::FetchHandle> fetch;
};

// Returns the catalog document received over the joining FETCH.
folly::coro::Task<std::string> subscribeAndFetchCatalog(
    CatalogSubscriber& sub,
    const FullTrackName& ftn) {
  co_await sub.client->setup(
      /*publisher=*/nullptr,
      /*subscriber=*/nullptr,
      /*connectTimeout=*/2s,
      /*transactionTimeout=*/5s,
      quic::TransportSettings(),
      getMoqtProtocols("", true));
  auto session = sub.client->getSession();
  auto subResult = co_await session->subscribe(
      SubscribeRequest::make(
          ftn,
          /*priority=*/0,
          GroupOrder::OldestFirst,
          /*forward=*/true,
          LocationType::LargestObject,
          /*start=*/std::nullopt,
          /*endGroup=*/0,
          /*inputParams=*/{}),
      std::make_shared<ObjectReceiver>(
          ObjectReceiver::SUBSCRIBE, std::make_shared<NopCallback>()));
  if (subResult.hasError()) {
    throw std::runtime_error(
        "subscribe failed: " + subResult.error().reasonPhrase);
  }
  sub.subscription = std::move(subResult.value());

  Fetch fetchReq(
      RequestID{0},
      /*jsid=*/std::nullopt,
      /*joiningStart=*/0,
      FetchType::RELATIVE_JOINING);
  fetchReq.fullTrackName = ftn;
  auto [docPromise, docFuture] =
      folly::coro::makePromiseContract<std::string>();
  auto fetchResult = co_await session->fetch(
      fetchReq,
      std::make_shared<ObjectReceiver>(
          ObjectReceiver::FETCH,
          std::make_shared<CatalogCallback>(std::move(docPromise))));
  if (fetchResult.hasError()) {
    throw std::runtime_error(
        "fetch failed: " + fetchResult.error().reasonPhrase);
  }
  sub.fetch = std::move(fetchResult.value());
  co_return co_await std::move(docFuture);
}

class MoQMediaListenersTest : public ::testing::Test {
 protected:
  void TearDown() override {
    listeners_.stop();
    clientEvb()->runInEventBaseThreadAndWait([this] {
      for (auto& sub : subscribers_) {
        if (auto session = sub->client->getSession()) {
          session->close(SessionCloseErrorCode::NO_ERROR);
        }
      }
      subscribers_.clear();
    });
  }

  void startListeners(bool quic, bool qmux) {
    listeners_ = startMediaListeners(
        dispatcher_,
        folly::SocketAddress("127.0.0.1", 0),
        MediaListenerOptions{.quic = quic, .qmux = qmux, .insecure = true});
  }

  // Connects over `transport`, subscribes to the catalog and joins it with a
  // FETCH. Returns the catalog document received.
  std::string subscribeToCatalog(
      const folly::SocketAddress& serverAddr,
      samples::TransportType transport) {
    auto sub = std::make_unique<CatalogSubscriber>();
    sub->client =
        std::make_unique<MoQRelayClient>(samples::makeRelayClientTransport(
            clientExecutor_,
            proxygen::URL(
                "moqt",
                serverAddr.getAddressStr(),
                serverAddr.getPort(),
                "/moq-media"),
            std::make_shared<
                moxygen::test::InsecureVerifierDangerousDoNotUseInProduction>(),
            transport));
    auto& subRef = *subscribers_.emplace_back(std::move(sub));
    return folly::coro::blockingWait(co_withExecutor(
        clientEvb(),
        folly::coro::timeout(
            subscribeAndFetchCatalog(subRef, kCatalogFtn), 10s)));
  }

  folly::EventBase* clientEvb() {
    return clientThread_.getEventBase();
  }

  folly::ScopedEventBaseThread serverThread_{"media-server"};
  std::shared_ptr<CountingFactory> factory_{
      std::make_shared<CountingFactory>(serverThread_.getEventBase())};
  std::shared_ptr<MoQBroadcastDispatcher> dispatcher_{
      std::make_shared<MoQBroadcastDispatcher>(
          factory_,
          *serverThread_.getEventBase())};
  MediaListeners listeners_;

  folly::ScopedEventBaseThread clientThread_{"media-client"};
  std::shared_ptr<MoQFollyExecutorImpl> clientExecutor_{
      std::make_shared<MoQFollyExecutorImpl>(clientThread_.getEventBase())};
  std::vector<std::unique_ptr<CatalogSubscriber>> subscribers_;
};

} // namespace

TEST_F(MoQMediaListenersTest, QuicAndQmuxShareOnePortAndOneBroadcast) {
  startListeners(/*quic=*/true, /*qmux=*/true);
  ASSERT_TRUE(listeners_.quic);
  ASSERT_TRUE(listeners_.qmux);
  // Started with port 0; clients dial one host:port for both transports.
  const auto serverAddr = listeners_.quic->getAddress();
  ASSERT_NE(serverAddr.getPort(), 0);
  ASSERT_EQ(listeners_.qmux->getAddress().getPort(), serverAddr.getPort());

  EXPECT_EQ(
      subscribeToCatalog(serverAddr, samples::TransportType::QUIC),
      kCatalogDoc);
  EXPECT_EQ(
      subscribeToCatalog(serverAddr, samples::TransportType::QMUX),
      kCatalogDoc);
  EXPECT_EQ(factory_->broadcastsMade, 1);

  // Both sessions are still live; stopping must close them without hanging.
  listeners_.stop();
  EXPECT_FALSE(listeners_.quic);
  EXPECT_FALSE(listeners_.qmux);
}

// QUIC only: MoQQmuxServer stops itself when destroyed, MoQServer does not.
TEST_F(MoQMediaListenersTest, MoveAssignmentStopsReplacedListeners) {
  startListeners(/*quic=*/true, /*qmux=*/false);
  ASSERT_TRUE(listeners_.quic);
  const auto oldAddr = listeners_.quic->getAddress();

  listeners_ = MediaListeners{};

  EXPECT_THROW(
      subscribeToCatalog(oldAddr, samples::TransportType::QUIC),
      std::exception);
}

TEST_F(MoQMediaListenersTest, QmuxOnlyServesOverQmux) {
  startListeners(/*quic=*/false, /*qmux=*/true);
  ASSERT_FALSE(listeners_.quic);
  ASSERT_TRUE(listeners_.qmux);

  EXPECT_EQ(
      subscribeToCatalog(
          listeners_.qmux->getAddress(), samples::TransportType::QMUX),
      kCatalogDoc);
}

TEST_F(MoQMediaListenersTest, QuicOnlyServesOverQuic) {
  startListeners(/*quic=*/true, /*qmux=*/false);
  ASSERT_TRUE(listeners_.quic);
  ASSERT_FALSE(listeners_.qmux);

  EXPECT_EQ(
      subscribeToCatalog(
          listeners_.quic->getAddress(), samples::TransportType::QUIC),
      kCatalogDoc);
}

TEST_F(MoQMediaListenersTest, QmuxBindFailureStopsQuicAndThrows) {
  // Hold the TCP port so QMUX cannot bind it, while QUIC (UDP) still can.
  auto tcp = bindExclusive(folly::SocketAddress("127.0.0.1", 0), SOCK_STREAM);
  ASSERT_NE(tcp, folly::NetworkSocket());
  ASSERT_EQ(folly::netops::listen(tcp, 1), 0);
  folly::SocketAddress takenAddr;
  takenAddr.setFromLocalAddress(tcp);

  EXPECT_THROW(
      startMediaListeners(
          dispatcher_, takenAddr, MediaListenerOptions{.insecure = true}),
      std::system_error);

  // The QUIC listener that did start must have been stopped, freeing its UDP
  // port for an exclusive bind.
  auto udp = bindExclusive(takenAddr, SOCK_DGRAM);
  EXPECT_NE(udp, folly::NetworkSocket());
  if (udp != folly::NetworkSocket()) {
    folly::netops::close(udp);
  }
  folly::netops::close(tcp);
}

} // namespace moxygen::media_server::test

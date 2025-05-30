// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#include "moxygen/moqtest/MoQTestClient.h"
#include "folly/coro/BlockingWait.h"
#include "folly/init/Init.h"
#include "folly/io/async/ScopedEventBaseThread.h"
#include "moxygen/MoQClient.h"
#include "moxygen/moqtest/Utils.h"

namespace moxygen {

DEFINE_string(url, "http://localhost:9999", "URL to connect to");
DEFINE_int32(connect_timeout, 1000, "connect timeout in ms");
DEFINE_int32(transaction_timeout, 1000, "transaction timeout in ms");
const int kDefaultRequestId = 0;
const std::string kDefaultTrackName = "test";
const GroupOrder kDefaultGroupOrder = GroupOrder::OldestFirst;
const LocationType kDefaultLocationType = LocationType::NextGroupStart;
const uint64_t kDefaultEndGroup = 10;
const TrackAlias kDefaultTrackAlias = TrackAlias(0);
const uint64_t kLastObjectInTrack = 10;
const uint64_t kLastGroupInTrack = 10;

MoQTestClient::MoQTestClient(folly::EventBase* evb, proxygen::URL url)
    : moqClient_(std::make_unique<MoQClient>(evb, std::move(url))) {}

folly::coro::Task<void> MoQTestClient::connect(folly::EventBase* evb) {
  co_await moqClient_->setupMoQSession(
      std::chrono::milliseconds(FLAGS_connect_timeout),
      std::chrono::seconds(FLAGS_transaction_timeout),
      nullptr,
      shared_from_this());
  co_return;
}

void MoQTestClient::initialize() {
  // Create a receiver for the client
  subReceiver_ = std::make_shared<ObjectReceiver>(
      ObjectReceiver::SUBSCRIBE,
      std::shared_ptr<MoQTestClient>(shared_from_this()));
  fetchReceiver_ = std::make_shared<ObjectReceiver>(
      ObjectReceiver::FETCH,
      std::shared_ptr<MoQTestClient>(shared_from_this()));
}

folly::coro::Task<moxygen::TrackNamespace> MoQTestClient::subscribe(
    MoQTestParameters params) {
  auto trackNamespace = convertMoqTestParamToTrackNamespace(&params);

  // Create a SubRequest with the created TrackNamespace as its fullTrackName
  SubscribeRequest sub;
  sub.requestID = kDefaultRequestId;

  FullTrackName ftn;
  ftn.trackNamespace = trackNamespace.value();
  ftn.trackName = kDefaultTrackName;

  sub.fullTrackName = ftn;
  sub.trackAlias = kDefaultTrackAlias;
  sub.groupOrder = kDefaultGroupOrder;
  sub.locType = kDefaultLocationType;
  sub.endGroup = kDefaultEndGroup;

  // Subscribe to the reciever
  auto res = co_await moqClient_->moqSession_->subscribe(sub, subReceiver_);

  co_return trackNamespace.value();
}

folly::coro::Task<moxygen::TrackNamespace> MoQTestClient::fetch(
    MoQTestParameters params) {
  auto trackNamespace = convertMoqTestParamToTrackNamespace(&params);

  // Create a Fetch with the created TrackNamespace as its fullTrackName
  Fetch fetch;
  fetch.requestID = kDefaultRequestId;

  FullTrackName ftn;
  ftn.trackNamespace = trackNamespace.value();
  ftn.trackName = kDefaultTrackName;
  fetch.fullTrackName = ftn;
  fetch.groupOrder = kDefaultGroupOrder;

  // Fetch to the reciever
  auto res = co_await moqClient_->moqSession_->fetch(fetch, fetchReceiver_);

  co_return trackNamespace.value();
}

ObjectReceiverCallback::FlowControlState MoQTestClient::onObject(
    const ObjectHeader& objHeader,
    Payload payload) {
  std::cout << "onObject" << std::endl;
  // Leave Unblocked For Now
  return ObjectReceiverCallback::FlowControlState::UNBLOCKED;
}

void MoQTestClient::onObjectStatus(const ObjectHeader& objHeader) {
  std::cout << "onObjectStatus" << std::endl;
}

void MoQTestClient::onEndOfStream() {
  std::cout << "onEndOfStream" << std::endl;
}

void MoQTestClient::onError(ResetStreamErrorCode) {
  std::cout << "onError" << std::endl;
}
void MoQTestClient::onSubscribeDone(SubscribeDone done) {
  std::cout << "onSubscribeDone" << std::endl;
}

} // namespace moxygen

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, false);
  folly::Init init(&argc, &argv);

  folly::ScopedEventBaseThread evb;

  // Initialize Client with url
  moxygen::MoQTestParameters kDefaultMoqParams;
  kDefaultMoqParams.lastObjectInTrack = moxygen::kLastObjectInTrack;
  kDefaultMoqParams.lastGroupInTrack = moxygen::kLastGroupInTrack;

  auto url = proxygen::URL(moxygen::FLAGS_url);
  std::shared_ptr<moxygen::MoQTestClient> client =
      std::make_shared<moxygen::MoQTestClient>(evb.getEventBase(), url);
  client->initialize();

  // Connect Client to Server
  LOG(INFO) << "Connecting to " << url.getHostAndPort();
  folly::coro::blockingWait(
      client->connect(evb.getEventBase()).scheduleOn(evb.getEventBase()));

  LOG(INFO) << "Subscribing to " << url.getHostAndPort();
  // Test a Subscribe Call
  folly::coro::blockingWait(
      client->subscribe(kDefaultMoqParams).scheduleOn(evb.getEventBase()));

  sleep(100);

  return 0;
}

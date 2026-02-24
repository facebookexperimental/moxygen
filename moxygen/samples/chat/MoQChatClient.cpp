/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <moxygen/ObjectReceiver.h>
#include <moxygen/samples/chat/MoQChatClient.h>

#include <folly/init/Init.h>
#include <folly/io/async/EventBase.h>
#include <folly/portability/GFlags.h>
#include <moxygen/util/InsecureVerifierDangerousDoNotUseInProduction.h>

DEFINE_string(connect_url, "", "URL for webtransport server");
DEFINE_bool(
    insecure,
    false,
    "Use insecure verifier (skip certificate validation)");
DEFINE_string(chat_id, "", "ID for the chat to join");
DEFINE_string(username, "", "Username to join chat");
DEFINE_string(device, "12345", "Device ID");
DEFINE_int32(connect_timeout, 1000, "Connect timeout (ms)");
DEFINE_int32(transaction_timeout, 120, "Transaction timeout (s)");
DEFINE_string(
    versions,
    "",
    "Comma-separated MoQ draft versions (e.g. \"14,16\"). Empty = all supported.");

namespace moxygen {

MoQChatClient::MoQChatClient(
    folly::EventBase* evb,
    proxygen::URL url,
    std::string chatID,
    std::string username,
    std::string deviceId)
    : chatID_(std::move(chatID)),
      username_(std::move(username)),
      deviceId_(std::move(deviceId)),
      timestampString_(
          folly::to<std::string>(std::chrono::system_clock::to_time_t(
              std::chrono::system_clock::now()))),
      executor_(std::make_shared<MoQFollyExecutorImpl>(evb)),
      moqClient_(
          executor_,
          std::move(url),
          FLAGS_insecure
              ? std::make_shared<
                    moxygen::test::
                        InsecureVerifierDangerousDoNotUseInProduction>()
              : nullptr) {}

folly::coro::Task<void> MoQChatClient::run() noexcept {
  XLOG(INFO) << __func__;
  auto g =
      folly::makeGuard([func = __func__] { XLOG(INFO) << "exit " << func; });
  try {
    auto alpns = getMoqtProtocols(FLAGS_versions, true);
    co_await moqClient_.setup(
        /*publisher=*/shared_from_this(),
        /*subscriber=*/shared_from_this(),
        std::chrono::milliseconds(FLAGS_connect_timeout),
        std::chrono::seconds(FLAGS_transaction_timeout),
        quic::TransportSettings(),
        alpns);
    // the publishNamespace and subscribe publishNamespaces should be in
    // parallel
    auto publishNamespaceRes =
        co_await moqClient_.getSession()->publishNamespace(
            {RequestID(0), participantTrackName(username_)});
    if (publishNamespaceRes.hasError()) {
      XLOG(ERR) << "PublishNamespace failed err="
                << publishNamespaceRes.error().reasonPhrase;
      co_return;
    }
    publishNamespaceHandle_ = std::move(publishNamespaceRes.value());
    uint64_t negotiatedVersion =
        *(moqClient_.getSession()->getNegotiatedVersion());
    // subscribe to the catalog track from the beginning of the largest group
    SubscribeNamespace subAnn{
        .requestID = RequestID(0),
        .trackNamespacePrefix = TrackNamespace(chatPrefix()),
        .forward = true,
        .options = SubscribeNamespaceOptions::BOTH,
    };
    subAnn.params.insertParam(getAuthParam(negotiatedVersion, username_));
    auto sa = co_await moqClient_.getSession()->subscribeNamespace(
        subAnn,
        std::make_shared<ChatNamespacePublishHandle>(
            shared_from_this(), TrackNamespace(chatPrefix())));
    if (sa.hasValue()) {
      XLOG(INFO) << "subscribeNamespace success";
      folly::getGlobalCPUExecutor()->add(
          [self = shared_from_this()] { self->publishLoop(); });
      subscribeNamespaceHandle_ = std::move(sa.value());
    } else {
      XLOG(INFO) << "SubscribeNamespace reqID=" << sa.error().requestID.value
                 << " code=" << folly::to_underlying(sa.error().errorCode)
                 << " reason=" << sa.error().reasonPhrase;
    }
  } catch (const std::exception& ex) {
    XLOG(ERR) << folly::exceptionStr(ex);
    co_return;
  }
  XLOG(INFO) << __func__ << " done";
}

folly::coro::Task<Subscriber::PublishNamespaceResult>
MoQChatClient::publishNamespace(
    PublishNamespace publishNamespace,
    std::shared_ptr<PublishNamespaceCallback>) {
  XLOG(INFO) << "PublishNamespace ns=" << publishNamespace.trackNamespace;
  auto trackNamespaceCopy = publishNamespace.trackNamespace;
  if (publishNamespace.trackNamespace.startsWith(
          TrackNamespace(chatPrefix()))) {
    if (publishNamespace.trackNamespace.size() != 5) {
      co_return folly::makeUnexpected(
          PublishNamespaceError{
              publishNamespace.requestID,
              PublishNamespaceErrorCode::UNINTERESTED,
              "Invalid chat publishNamespace"});
    }
    co_withExecutor(
        moqClient_.getSession()->getExecutor(),
        subscribeToUser(std::move(publishNamespace.trackNamespace)))
        .start();
  } else {
    co_return folly::makeUnexpected(
        PublishNamespaceError{
            publishNamespace.requestID,
            PublishNamespaceErrorCode::UNINTERESTED,
            "don't care"});
  }
  co_return std::make_shared<PublishNamespaceHandle>(
      PublishNamespaceOk{
          .requestID = publishNamespace.requestID, .requestSpecificParams = {}},
      shared_from_this(),
      std::move(trackNamespaceCopy));
}

void MoQChatClient::publishNamespaceDone(const TrackNamespace& ns) {
  if (ns.size() < 5) {
    XLOG(ERR) << "Invalid publishNamespaceDone namespace size=" << ns.size();
    return;
  }
  std::string username = ns[2];
  std::string deviceId = ns[3];
  XLOG(INFO) << "publishNamespaceDone user=" << username
             << " device=" << deviceId;
  auto userIt = subscriptions_.find(username);
  if (userIt == subscriptions_.end()) {
    return;
  }
  auto& userTracks = userIt->second;
  for (auto it = userTracks.begin(); it != userTracks.end(); ++it) {
    if (it->deviceId == deviceId) {
      if (it->subscription) {
        it->subscription->unsubscribe();
        it->subscription.reset();
      }
      userTracks.erase(it);
      break;
    }
  }
  if (userTracks.empty()) {
    subscriptions_.erase(userIt);
  }
}

void MoQChatClient::ChatNamespacePublishHandle::namespaceMsg(
    const TrackNamespace& trackNamespaceSuffix) {
  // Reconstruct the full namespace from prefix + suffix
  auto parts = prefix_.trackNamespace;
  parts.insert(
      parts.end(),
      trackNamespaceSuffix.trackNamespace.begin(),
      trackNamespaceSuffix.trackNamespace.end());
  TrackNamespace fullNs(std::move(parts));
  XLOG(INFO) << "Namespace ns=" << fullNs;
  if (fullNs.size() != 5) {
    XLOG(ERR) << "Invalid chat namespace, expected 5 parts";
    return;
  }
  co_withExecutor(
      client_->moqClient_.getSession()->getExecutor(),
      client_->subscribeToUser(std::move(fullNs)))
      .start();
}

folly::coro::Task<Publisher::SubscribeResult> MoQChatClient::subscribe(
    SubscribeRequest subscribeReq,
    std::shared_ptr<TrackConsumer> consumer) {
  XLOG(INFO) << "SubscribeRequest";
  if (subscribeReq.fullTrackName.trackNamespace !=
      participantTrackName(username_)) {
    co_return folly::makeUnexpected(
        SubscribeError{
            subscribeReq.requestID,
            SubscribeErrorCode::TRACK_NOT_EXIST,
            "no such track"});
  }
  if (publisher_) {
    co_return folly::makeUnexpected(
        SubscribeError{
            subscribeReq.requestID,
            SubscribeErrorCode::INTERNAL_ERROR,
            "Duplicate subscribe for track"});
  }
  chatRequestID_.emplace(subscribeReq.requestID);
  chatTrackAlias_.emplace(TrackAlias(subscribeReq.requestID.value));
  std::optional<AbsoluteLocation> largest;
  if (nextGroup_ > 0) {
    largest.emplace(nextGroup_ - 1, 0);
  }
  publisher_ = std::move(consumer);
  setSubscribeOk(
      {subscribeReq.requestID,
       *chatTrackAlias_,
       std::chrono::milliseconds(0),
       MoQSession::resolveGroupOrder(
           GroupOrder::OldestFirst, subscribeReq.groupOrder),
       largest});
  co_return shared_from_this();
}

void MoQChatClient::unsubscribe() {
  // MoQChatClient only publishes a single track at a time.
  XLOG(INFO) << "UNSUBSCRIBE reqID=" << *chatRequestID_;
  publisher_.reset();
  chatRequestID_.reset();
  chatTrackAlias_.reset();
}

void MoQChatClient::publishLoop() {
  XLOG(INFO) << __func__;
  auto g =
      folly::makeGuard([func = __func__] { XLOG(INFO) << "exit " << func; });
  std::string input;
  auto evb = moqClient_.getEventBase();
  auto keepAlive = folly::getKeepAliveToken(evb);
  auto token = moqClient_.getSession()->getCancelToken();
  while (!token.isCancellationRequested() && std::cin.good() &&
         !std::cin.eof()) {
    std::getline(std::cin, input);
    if (token.isCancellationRequested()) {
      XLOG(DBG1) << "Detected deleted moqSession, cleaning up";
      break;
    }
    evb->add([self = shared_from_this(), input] { self->handleInput(input); });
    if (input == "/leave") {
      break;
    }
  }
}

void MoQChatClient::handleInput(const std::string& input) {
  if (input == "/leave") {
    XLOG(INFO) << "Leaving chat";
    publishNamespaceHandle_->publishNamespaceDone();
    subscribeNamespaceHandle_->unsubscribeNamespace();
    if (publisher_) {
      publisher_->objectStream(
          {nextGroup_++,
           /*subgroupIn=*/0,
           /*idIn=*/0,
           /*priorityIn=*/0,
           ObjectStatus::END_OF_TRACK},
          nullptr);
      // Publisher=TrackReceiveState which contains a shared_ptr to the
      // chat client (to deliver unsubscribe/subscribeUpdate).  It *must*
      // be reset to prevent memory leaks
      publisher_.reset();
    }
    moqClient_.getSession()->close(SessionCloseErrorCode::NO_ERROR);
    moqClient_.shutdown();
  } else if (publisher_) {
    publisher_->objectStream(
        {nextGroup_++,
         /*subgroupIn=*/0,
         /*idIn=*/0,
         /*priorityIn=*/0,
         ObjectStatus::NORMAL},
        folly::IOBuf::copyBuffer(input));
  }
}

folly::coro::Task<void> MoQChatClient::subscribeToUser(
    TrackNamespace trackNamespace) {
  CHECK_GE(trackNamespace.size(), 5);
  std::string username = trackNamespace[2];
  std::string deviceId = trackNamespace[3];
  std::string timestampStr = trackNamespace[4];
  XLOG(INFO) << __func__ << " user=" << username << " device=" << deviceId
             << " ts=" << timestampStr;
  auto g =
      folly::makeGuard([func = __func__] { XLOG(INFO) << "exit " << func; });
  if (username == username_) {
    XLOG(INFO) << "Ignoring self";
    co_return;
  }
  std::chrono::seconds timestamp(folly::to<uint32_t>(timestampStr));
  // user -> list(device, ts)
  auto& userTracks = subscriptions_[username];
  UserTrack* userTrackPtr = nullptr;
  for (auto& userTrack : userTracks) {
    if (userTrack.deviceId == deviceId) {
      if (userTrack.timestamp < timestamp) {
        XLOG(INFO) << "Device has later track, unsubscribing";
        if (userTrack.subscription) {
          userTrack.subscription->unsubscribe();
          userTrack.subscription.reset();
        } else {
          XLOG(INFO) << "Subscribe in progress, bad?";
        }
        userTrackPtr = &userTrack;
        break;
      } else {
        XLOG(INFO) << "PublishNamespace for old track, ignoring";
        co_return;
      }
    } else {
      // not the device we're looking for
      continue;
    }
  }
  if (!userTrackPtr) {
    // no tracks for this device
    userTrackPtr =
        &userTracks.emplace_back(UserTrack({deviceId, timestamp, 0}));
  }
  // now subscribe and update timestamp.
  class ChatObjectHandler : public ObjectReceiverCallback {
   public:
    explicit ChatObjectHandler(MoQChatClient& client, std::string username)
        : client_(client), username_(username) {}
    ~ChatObjectHandler() override = default;
    FlowControlState onObject(
        std::optional<TrackAlias> /* trackAlias */,
        const ObjectHeader&,
        Payload payload) override {
      if (payload) {
        std::cout << username_ << ": ";
        payload->coalesce();
        std::cout << payload->moveToFbString() << std::endl;
      }
      return FlowControlState::UNBLOCKED;
    }
    void onObjectStatus(
        std::optional<TrackAlias> /* trackAlias */,
        const ObjectHeader&) override {}
    void onEndOfStream() override {}
    void onError(ResetStreamErrorCode error) override {
      std::cout << "Stream Error=" << folly::to_underlying(error) << std::endl;
    }

    void onPublishDone(PublishDone pubDone) override {
      XLOG(INFO) << "PublishDone: " << pubDone.reasonPhrase;
      client_.publishDone(std::move(pubDone));
      baton.post();
    }

    folly::coro::Baton baton;

   private:
    MoQChatClient& client_;
    std::string username_;
  };
  auto handler = std::make_shared<ChatObjectHandler>(*this, username);

  auto req = SubscribeRequest::make(FullTrackName({trackNamespace, "chat"}));
  auto track = co_await co_awaitTry(moqClient_.getSession()->subscribe(
      std::move(req),
      std::make_shared<ObjectReceiver>(ObjectReceiver::SUBSCRIBE, handler)));
  if (track.hasException()) {
    // subscribe failed
    XLOG(ERR) << track.exception();
    co_return;
  }
  if (track.value().hasError()) {
    XLOG(INFO) << "SubscribeError id=" << track->error().requestID
               << " code=" << folly::to_underlying(track->error().errorCode)
               << " reason=" << track->error().reasonPhrase;
    co_return;
  }

  userTrackPtr->subscription = std::move(track->value());
  userTrackPtr->requestID = userTrackPtr->subscription->subscribeOk().requestID;
  userTrackPtr->timestamp = timestamp;
  co_await handler->baton;
}

void MoQChatClient::publishDone(PublishDone pubDone) {
  for (auto& userTracks : subscriptions_) {
    for (auto userTrackIt = userTracks.second.begin();
         userTrackIt != userTracks.second.end();
         ++userTrackIt) {
      if (userTrackIt->requestID == pubDone.requestID) {
        if (userTrackIt->subscription) {
          userTrackIt->subscription.reset();
        }
        userTracks.second.erase(userTrackIt);
        break;
      }
    }
  }
}
} // namespace moxygen

int main(int argc, char* argv[]) {
  folly::Init init(&argc, &argv, false);
  folly::EventBase eventBase;
  proxygen::URL url(FLAGS_connect_url);
  if (!url.isValid() || !url.hasHost()) {
    XLOG(ERR) << "Invalid url: " << FLAGS_connect_url;
  }
  auto chatClient = std::make_shared<moxygen::MoQChatClient>(
      &eventBase, std::move(url), FLAGS_chat_id, FLAGS_username, FLAGS_device);
  co_withExecutor(&eventBase, chatClient->run()).start();
  eventBase.loop();
}

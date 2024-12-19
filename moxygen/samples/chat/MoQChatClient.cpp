/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/samples/chat/MoQChatClient.h"
#include "moxygen/ObjectReceiver.h"

#include <folly/String.h>
#include <folly/init/Init.h>

DEFINE_string(connect_url, "", "URL for webtransport server");
DEFINE_string(chat_id, "", "ID for the chat to join");
DEFINE_string(username, "", "Username to join chat");
DEFINE_string(device, "12345", "Device ID");
DEFINE_int32(connect_timeout, 1000, "Connect timeout (ms)");
DEFINE_int32(transaction_timeout, 120, "Transaction timeout (s)");

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

      moqClient_(evb, std::move(url)) {}

folly::coro::Task<void> MoQChatClient::run() noexcept {
  XLOG(INFO) << __func__;
  auto g =
      folly::makeGuard([func = __func__] { XLOG(INFO) << "exit " << func; });
  try {
    co_await moqClient_.setupMoQSession(
        std::chrono::milliseconds(FLAGS_connect_timeout),
        std::chrono::seconds(FLAGS_transaction_timeout));
    auto exec = co_await folly::coro::co_current_executor;
    controlReadLoop().scheduleOn(exec).start();

    // the announce and subscribe should be in parallel
    auto announceRes = co_await moqClient_.moqSession_->announce(
        {participantTrackName(username_), {}});
    // subscribe to the catalog track from the beginning of the latest group
    auto sa = co_await moqClient_.moqSession_->subscribeAnnounces(
        {TrackNamespace(chatPrefix()),
         {{folly::to_underlying(TrackRequestParamKey::AUTHORIZATION),
           username_}}});
    if (sa.hasValue()) {
      XLOG(INFO) << "subscribeAnnounces success";
      folly::getGlobalCPUExecutor()->add([this] { publishLoop(); });
    } else {
      XLOG(INFO) << "SubscribeAnnounces id=" << sa.error().trackNamespacePrefix
                 << " code=" << sa.error().errorCode
                 << " reason=" << sa.error().reasonPhrase;
    }
  } catch (const std::exception& ex) {
    XLOG(ERR) << ex.what();
    co_return;
  }
  XLOG(INFO) << __func__ << " done";
}

folly::coro::Task<void> MoQChatClient::controlReadLoop() {
  class ControlVisitor : public MoQSession::ControlVisitor {
   public:
    explicit ControlVisitor(MoQChatClient& client) : client_(client) {}

    void operator()(Announce announce) const override {
      XLOG(INFO) << "Announce ns=" << announce.trackNamespace;
      if (announce.trackNamespace.startsWith(
              TrackNamespace(client_.chatPrefix()))) {
        if (announce.trackNamespace.size() != 5) {
          client_.moqClient_.moqSession_->announceError(
              {announce.trackNamespace, 400, "Invalid announce"});
        }
        client_.moqClient_.moqSession_->announceOk({announce.trackNamespace});
        client_.subscribeToUser(std::move(announce.trackNamespace))
            .scheduleOn(client_.moqClient_.moqSession_->getEventBase())
            .start();
      } else {
        client_.moqClient_.moqSession_->announceError(
            {announce.trackNamespace, 404, "don't care"});
      }
    }

    void operator()(SubscribeRequest subscribeReq) const override {
      XLOG(INFO) << "SubscribeRequest";
      if (subscribeReq.fullTrackName.trackNamespace !=
          client_.participantTrackName(client_.username_)) {
        client_.moqClient_.moqSession_->subscribeError(
            {subscribeReq.subscribeID, 404, "no such track"});
        return;
      }
      client_.chatSubscribeID_.emplace(subscribeReq.subscribeID);
      client_.chatTrackAlias_.emplace(subscribeReq.trackAlias);
      folly::Optional<AbsoluteLocation> latest;
      if (client_.nextGroup_ > 0) {
        latest.emplace(client_.nextGroup_ - 1, 0);
      }
      client_.publisher_ = client_.moqClient_.moqSession_->subscribeOk(
          {subscribeReq.subscribeID,
           std::chrono::milliseconds(0),
           MoQSession::resolveGroupOrder(
               GroupOrder::OldestFirst, subscribeReq.groupOrder),
           latest});
    }

    void operator()(Unsubscribe unsubscribe) const override {
      XLOG(INFO) << "Unsubscribe id=" << unsubscribe.subscribeID;
      if (client_.chatSubscribeID_ &&
          unsubscribe.subscribeID == *client_.chatSubscribeID_) {
        client_.chatSubscribeID_.reset();
        client_.chatTrackAlias_.reset();
        if (client_.publisher_) {
          client_.publisher_->subscribeDone(
              {unsubscribe.subscribeID,
               SubscribeDoneStatusCode::UNSUBSCRIBED,
               "",
               folly::none});
          client_.publisher_.reset();
        }
      }
    }

   private:
    MoQChatClient& client_;
  };
  XLOG(INFO) << __func__;
  auto g =
      folly::makeGuard([func = __func__] { XLOG(INFO) << "exit " << func; });
  ControlVisitor visitor(*this);
  MoQSession::ControlVisitor* vptr(&visitor);
  while (auto msg = co_await moqClient_.moqSession_->controlMessages().next()) {
    boost::apply_visitor(*vptr, msg.value());
  }
}

void MoQChatClient::publishLoop() {
  XLOG(INFO) << __func__;
  auto g =
      folly::makeGuard([func = __func__] { XLOG(INFO) << "exit " << func; });
  std::string input;
  folly::Executor::KeepAlive keepAlive(moqClient_.getEventBase());
  while (moqClient_.moqSession_ && std::cin.good() && !std::cin.eof()) {
    std::getline(std::cin, input);
    if (!moqClient_.moqSession_) {
      break;
    }
    moqClient_.getEventBase()->runInEventBaseThread([this, input] {
      if (input == "/leave") {
        XLOG(INFO) << "Leaving chat";
        moqClient_.moqSession_->close();
        moqClient_.moqSession_.reset();
      } else if (chatSubscribeID_) {
        if (publisher_) {
          publisher_->objectStream(
              {*chatTrackAlias_,
               nextGroup_++,
               /*subgroup=*/0,
               /*id=*/0,
               /*pri=*/0,
               ForwardPreference::Subgroup,
               ObjectStatus::NORMAL},
              folly::IOBuf::copyBuffer(input));
        }
      }
    });
    if (input == "/leave") {
      break;
    }
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
        moqClient_.moqSession_->unsubscribe({userTrack.subscribeId});
        userTrackPtr = &userTrack;
        break;
      } else {
        XLOG(INFO) << "Announce for old track, ignoring";
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
    FlowControlState onObject(const ObjectHeader&, Payload payload) override {
      if (payload) {
        std::cout << username_ << ": ";
        payload->coalesce();
        std::cout << payload->moveToFbString() << std::endl;
      }
      return FlowControlState::UNBLOCKED;
    }
    void onObjectStatus(const ObjectHeader&) override {}
    void onEndOfStream() override {}
    void onError(ResetStreamErrorCode error) override {
      std::cout << "Stream Error=" << folly::to_underlying(error) << std::endl;
    }

    void onSubscribeDone(SubscribeDone subDone) override {
      XLOG(INFO) << "SubscribeDone: " << subDone.reasonPhrase;
      if (subDone.statusCode != SubscribeDoneStatusCode::UNSUBSCRIBED &&
          client_.moqClient_.moqSession_) {
        client_.moqClient_.moqSession_->unsubscribe({subDone.subscribeID});
      }
      client_.subscribeDone(std::move(subDone));
      baton.post();
    }

    folly::coro::Baton baton;

   private:
    MoQChatClient& client_;
    std::string username_;
  };
  ChatObjectHandler handler(*this, username);

  auto track = co_await co_awaitTry(moqClient_.moqSession_->subscribe(
      {0,
       0,
       FullTrackName({trackNamespace, "chat"}),
       0,
       GroupOrder::OldestFirst,
       LocationType::LatestGroup,
       folly::none,
       folly::none,
       {}},
      std::make_shared<ObjectReceiver>(ObjectReceiver::SUBSCRIBE, &handler)));
  if (track.hasException()) {
    // subscribe failed
    XLOG(ERR) << track.exception();
    co_return;
  }
  if (track.value().hasError()) {
    XLOG(INFO) << "SubscribeError id=" << track->error().subscribeID
               << " code=" << track->error().errorCode
               << " reason=" << track->error().reasonPhrase;
    co_return;
  }

  userTrackPtr->subscribeId = track->value().subscribeID;
  userTrackPtr->timestamp = timestamp;
  co_await handler.baton;
}

void MoQChatClient::subscribeDone(SubscribeDone subDone) {
  for (auto& userTracks : subscriptions_) {
    for (auto userTrackIt = userTracks.second.begin();
         userTrackIt != userTracks.second.end();
         ++userTrackIt) {
      if (userTrackIt->subscribeId == subDone.subscribeID) {
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
  moxygen::MoQChatClient chatClient(
      &eventBase, std::move(url), FLAGS_chat_id, FLAGS_username, FLAGS_device);
  chatClient.run().scheduleOn(&eventBase).start();
  eventBase.loop();
}

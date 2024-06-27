/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/samples/chat/MoQChatClient.h"

#include <folly/String.h>
#include <folly/init/Init.h>

DEFINE_string(connect_url, "", "URL for webtransport server");
DEFINE_string(chat_id, "", "ID for the chat to join");
DEFINE_string(username, "", "Username to join chat");
DEFINE_int32(connect_timeout, 1000, "Connect timeout (ms)");
DEFINE_int32(transaction_timeout, 120, "Transaction timeout (s)");

namespace moxygen {

MoQChatClient::MoQChatClient(
    folly::EventBase* evb,
    proxygen::URL url,
    std::string chatID,
    std::string username)
    : chatID_(std::move(chatID)),
      username_(std::move(username)),
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
    auto catalogTrack = co_await moqClient_.moqSession_->subscribe(
        {0,
         0,
         catalogTrackName(),
         LocationType::LatestGroup,
         folly::none,
         folly::none,
         {{folly::to_underlying(TrackRequestParamKey::AUTHORIZATION),
           username_}}});
    if (catalogTrack.hasValue()) {
      readCatalogUpdates(catalogTrack.value()).scheduleOn(exec).start();
      folly::getGlobalCPUExecutor()->add([this] { publishLoop(); });
    } else {
      XLOG(INFO) << "SubscribeError id=" << catalogTrack.error().subscribeID
                 << " code=" << catalogTrack.error().errorCode
                 << " reason=" << catalogTrack.error().reasonPhrase;
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
      XLOG(INFO) << "Announce";
      // chat client doesn't expect server or relay to announce anything
      client_.moqClient_.moqSession_->announceError(
          {announce.trackNamespace, 404, "don't care"});
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
      client_.moqClient_.moqSession_->subscribeOk(
          {subscribeReq.subscribeID, std::chrono::milliseconds(0), latest});
    }

    void operator()(SubscribeDone) const override {
      XLOG(INFO) << "SubscribeDone";
      // TODO: should be handled in session
    }

    void operator()(Unsubscribe unsubscribe) const override {
      XLOG(INFO) << "Unsubscribe id=" << unsubscribe.subscribeID;
      if (client_.chatSubscribeID_ &&
          unsubscribe.subscribeID == *client_.chatSubscribeID_) {
        client_.chatSubscribeID_.reset();
        client_.chatTrackAlias_.reset();
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

folly::coro::Task<void> MoQChatClient::readCatalogUpdates(
    std::shared_ptr<MoQSession::TrackHandle> catalogTrack) {
  XLOG(INFO) << __func__;
  auto g =
      folly::makeGuard([func = __func__] { XLOG(INFO) << "exit " << func; });
  auto exec = co_await folly::coro::co_current_executor;
  // TODO: check track.value()->getCancelToken()
  while (auto obj = co_await catalogTrack->objects().next()) {
    XLOG(INFO) << "catalog object";
    // TODO: should there be a way to STOP_SENDING an object without breaking
    // the stream it's on?
    auto catalogBuf = co_await obj.value()->payload();
    // ok have the full object
    if (obj.value()->header.id == 0 && catalogBuf) {
      XLOG(INFO) << "new catalog";
      // parse catalog into list of usernames
      catalogBuf->coalesce();
      auto catalogString = catalogBuf->moveToFbString();
      std::vector<std::string> usernames;
      folly::split("\n", catalogString, usernames);
      if (usernames.empty() || usernames[0] != "version=1") {
        XLOG(ERR) << "Bad catalog version";
        continue;
      } else {
        std::swap(usernames[0], usernames[usernames.size() - 1]);
        usernames.resize(usernames.size() - 1);
      }
      for (auto& username : usernames) {
        XLOG(INFO) << username;
        if (username == username_) {
          continue;
        }
        if (subscriptions_.find(username) == subscriptions_.end()) {
          XLOG(INFO) << "Subscribing to new user " << username;
          subscribeToUser(username).scheduleOn(exec).start();
        }
      }
      for (auto& username : subscriptions_) {
        if (std::find(usernames.begin(), usernames.end(), username) ==
            usernames.end()) {
          XLOG(INFO) << username << " left the chat";
          subscriptions_.erase(username);
          break;
        }
      }
    }
    // ignore delta updates for now
  }
  XLOG(INFO) << "Catalog track finished";
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
        moqClient_.moqSession_->publish(
            {*chatSubscribeID_,
             *chatTrackAlias_,
             nextGroup_++,
             0,
             0,
             ForwardPreference::Object,
             ObjectStatus::NORMAL},
            0,
            folly::IOBuf::copyBuffer(input),
            true);
      }
    });
    if (input == "/leave") {
      break;
    }
  }
}

folly::coro::Task<void> MoQChatClient::subscribeToUser(std::string username) {
  XLOG(INFO) << __func__ << " user=" << username;
  auto g =
      folly::makeGuard([func = __func__] { XLOG(INFO) << "exit " << func; });
  auto track = co_await co_awaitTry(moqClient_.moqSession_->subscribe(
      {0,
       0,
       FullTrackName({participantTrackName(username), ""}),
       LocationType::LatestGroup,
       folly::none,
       folly::none,
       {}}));
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
  subscriptions_.insert(username);
  while (auto obj = co_await track->value()->objects().next()) {
    // how to cancel this loop
    auto payload = co_await obj.value()->payload();
    if (payload) {
      std::cout << username << ": ";
      payload->coalesce();
      std::cout << payload->moveToFbString() << std::endl;
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
      &eventBase, std::move(url), FLAGS_chat_id, FLAGS_username);
  chatClient.run().scheduleOn(&eventBase).start();
  eventBase.loop();
}

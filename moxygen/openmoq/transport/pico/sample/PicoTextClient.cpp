/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/MoQSession.h"
#include "moxygen/MoQTypes.h"
#include "moxygen/MoQVersions.h"
#include "moxygen/ObjectReceiver.h"
#include "moxygen/openmoq/transport/pico/MoQPicoQuicEventBaseClient.h"

#include <folly/init/Init.h>
#include <folly/io/async/EventBase.h>
#include <folly/logging/xlog.h>
#include <gflags/gflags.h>
#include <proxygen/lib/utils/URL.h>

#include <csignal>
#include <iostream>

DEFINE_string(connect_url, "", "URL of the MoQ server (e.g. moqt://host:9668)");
DEFINE_string(track_namespace, "", "Track namespace");
DEFINE_string(track_namespace_delimiter, "/", "Track namespace delimiter");
DEFINE_string(track_name, "", "Track name");
DEFINE_string(
    versions,
    "",
    "Comma-separated MoQ draft versions (e.g. \"14,16\"). Empty = all supported.");
DEFINE_string(
    cert_root,
    "",
    "CA bundle path for TLS (empty = skip verification)");

namespace {
using namespace moxygen;

class PicoTextClient;

class TextHandler : public ObjectReceiverCallback {
 public:
  FlowControlState onObject(
      std::optional<TrackAlias>,
      const ObjectHeader&,
      Payload payload) override {
    if (payload) {
      std::cout << payload->moveToFbString() << "\n";
    }
    return FlowControlState::UNBLOCKED;
  }
  void onObjectStatus(
      std::optional<TrackAlias>,
      const ObjectHeader& h) override {
    std::cout << "ObjectStatus=" << uint32_t(h.status) << "\n";
  }
  void onEndOfStream() override {}
  void onError(ResetStreamErrorCode e) override {
    std::cout << "StreamError=" << folly::to_underlying(e) << "\n";
  }
  void onPublishDone(PublishDone) override;

  void setClient(PicoTextClient* client) {
    client_ = client;
  }

 private:
  PicoTextClient* client_{nullptr};
};

class PicoTextClient : public MoQPicoQuicEventBaseClient {
 public:
  PicoTextClient(
      std::string endpoint,
      folly::Executor::KeepAlive<folly::EventBase> evb,
      std::string certRoot)
      : MoQPicoQuicEventBaseClient(
            std::move(endpoint), std::move(evb), std::move(certRoot)) {}

  void onSession(std::shared_ptr<MoQSession> session) override {
    session_ = session;
    auto receiver = std::make_shared<ObjectReceiver>(
        ObjectReceiver::SUBSCRIBE, handler_);
    TrackNamespace ns(FLAGS_track_namespace, FLAGS_track_namespace_delimiter);
    FullTrackName ftn{std::move(ns), FLAGS_track_name};

    folly::coro::co_withExecutor(
        getEventBase(),
        subscribe(
            std::move(session), std::move(receiver), std::move(ftn)))
        .start();
  }

  folly::coro::Task<void> subscribe(
      std::shared_ptr<MoQSession> session,
      std::shared_ptr<ObjectReceiver> receiver,
      FullTrackName ftn) {
    auto result = co_await session->subscribe(
        SubscribeRequest::make(ftn), receiver);
    if (result.hasValue()) {
      subscription_ = std::move(result.value());
      XLOG(INFO) << "Subscribed to "
                 << folly::join("/", ftn.trackNamespace.trackNamespace)
                 << "/" << ftn.trackName;
      session->drain();
    } else {
      XLOG(ERR) << "Subscribe error: " << result.error().reasonPhrase;
      stop();
    }
  }

  void stop() {
    if (subscription_) {
      std::move(subscription_)->unsubscribe();
    }
    if (session_) {
      std::move(session_)->close(SessionCloseErrorCode::NO_ERROR);
    }
    close();
  }

 private:
  std::shared_ptr<TextHandler> handler_{[this] {
    auto h = std::make_shared<TextHandler>();
    h->setClient(this);
    return h;
  }()};
  std::shared_ptr<MoQSession> session_;
  std::shared_ptr<Publisher::SubscriptionHandle> subscription_;
};

void TextHandler::onPublishDone(PublishDone) {
  if (client_) {
    client_->stop();
  }
}

PicoTextClient* gClient{nullptr};

void signalHandler(int) {
  if (gClient) {
    gClient->stop();
  }
}

} // namespace

int main(int argc, char* argv[]) {
  folly::Init init(&argc, &argv, true);

  proxygen::URL url(FLAGS_connect_url);
  if (!url.isValid() || !url.hasHost()) {
    XLOG(ERR) << "Invalid --connect_url: " << FLAGS_connect_url;
    return 1;
  }

  folly::EventBase evb;

  PicoTextClient client(
      /*endpoint=*/"",
      folly::getKeepAliveToken(&evb),
      FLAGS_cert_root);
  gClient = &client;

  std::signal(SIGINT, signalHandler);
  std::signal(SIGTERM, signalHandler);

  auto alpns = getMoqtProtocols(FLAGS_versions, /*includeWt=*/true);
  CHECK(!alpns.empty()) << "No ALPN for versions: " << FLAGS_versions;

  folly::SocketAddress addr(
      url.getHost(), url.getPort(), /*allowNameLookup=*/true);
  client.connect(addr, url.getHost(), alpns.front());

  evb.loop();

  gClient = nullptr;
  return 0;
}

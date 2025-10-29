/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <folly/base64.h>
#include <folly/coro/Sleep.h>
#include <folly/init/Init.h>
#include <folly/io/async/AsyncSignalHandler.h>
#include <folly/portability/GFlags.h>
#include <signal.h>
#include <moxygen/MoQWebTransportClient.h>
#include <moxygen/ObjectReceiver.h>
#include <moxygen/relay/MoQRelayClient.h>

DEFINE_string(connect_url, "", "URL for webtransport server");
DEFINE_string(track_namespace, "", "Track Namespace");
DEFINE_string(track_namespace_delimiter, "/", "Track Namespace Delimiter");
DEFINE_string(track_name, "", "Track Name");
DEFINE_string(sg, "", "Start group, defaults to largest");
DEFINE_string(so, "", "Start object, defaults to 0 when sg is set or largest");
DEFINE_string(eg, "", "End group");
DEFINE_int32(connect_timeout, 1000, "Connect timeout (ms)");
DEFINE_int32(transaction_timeout, 120, "Transaction timeout (s)");
DEFINE_int32(
    join_start,
    0,
    "Joining start, only used if either jrfetch or jafetch is true");
DEFINE_bool(quic_transport, false, "Use raw QUIC transport");
DEFINE_bool(fetch, false, "Use fetch rather than subscribe");
DEFINE_bool(jrfetch, false, "Joining relative fetch");
DEFINE_bool(jafetch, false, "Joining absolute fetch");
DEFINE_bool(forward, true, "Forward flag for subscriptions");
DEFINE_bool(
    publish,
    false,
    "If client will act as a receiver for publish call (don't call subscribe and call sub_announce)");
DEFINE_bool(
    unsubscribe,
    false,
    "If client will unsubscribe from PUBLISH track after a specified time");
DEFINE_uint64(unsubscribe_time, 30, "Time to unsubscribe in seconds");

namespace {
using namespace moxygen;

struct SubParams {
  LocationType locType;
  folly::Optional<AbsoluteLocation> start;
  uint64_t endGroup;
};

SubParams flags2params() {
  SubParams result;
  std::string soStr(FLAGS_so);
  if (FLAGS_sg.empty()) {
    if (soStr.empty()) {
      result.locType = LocationType::LargestObject;
      return result;
    } else {
      XLOG(ERR) << "Invalid: sg blank, so=" << soStr;
      exit(1);
    }
  } else if (soStr.empty()) {
    soStr = std::string("0");
  }
  if (FLAGS_jafetch || FLAGS_jrfetch) {
    XLOG(ERR) << "Joining fetch requires empty sg";
    exit(1);
  }
  result.start.emplace(
      folly::to<uint64_t>(FLAGS_sg), folly::to<uint64_t>(soStr));
  if (FLAGS_eg.empty()) {
    result.locType = LocationType::AbsoluteStart;
    return result;
  } else {
    result.locType = LocationType::AbsoluteRange;
    result.endGroup = folly::to<uint64_t>(FLAGS_eg);
    return result;
  }
  return result;
}

class TextHandler : public ObjectReceiverCallback {
 public:
  explicit TextHandler(bool fetch) : fetch_(fetch) {}
  ~TextHandler() override = default;
  FlowControlState onObject(
      folly::Optional<TrackAlias> /*trackAlias*/,
      const ObjectHeader& header,
      Payload payload) override {
    const auto& mutable_exts = header.extensions.getMutableExtensions();
    for (const auto& ext : mutable_exts) {
      if (ext.type & 0x1) {
        ext.arrayValue->coalesce();
        std::cout << "data extension="
                  << folly::base64Encode(
                         {(const char*)(ext.arrayValue->data()),
                          ext.arrayValue->length()})

                  << std::endl;
      } else {
        std::cout << "int extension=" << ext.intValue << std::endl;
      }
    }

    if (payload) {
      std::cout << payload->moveToFbString() << std::endl;
    }
    return FlowControlState::UNBLOCKED;
  }
  void onObjectStatus(
      folly::Optional<TrackAlias> /*trackAlias*/,
      const ObjectHeader& objHeader) override {
    std::cout << "ObjectStatus=" << uint32_t(objHeader.status) << std::endl;
  }
  void onEndOfStream() override {
    if (fetch_) {
      std::cout << __func__ << std::endl;
      baton.post();
    }
  }
  void onError(ResetStreamErrorCode error) override {
    std::cout << "Stream Error=" << folly::to_underlying(error) << std::endl;
  }

  void onSubscribeDone(SubscribeDone) override {
    CHECK(!fetch_);
    std::cout << __func__ << std::endl;
    baton.post();
  }

  folly::coro::Baton baton;

 private:
  bool fetch_{false};
};

class MoQTextClient : public Subscriber,
                      public std::enable_shared_from_this<MoQTextClient> {
 public:
  MoQTextClient(
      std::shared_ptr<MoQFollyExecutorImpl> evb,
      proxygen::URL url,
      FullTrackName ftn)
      : moqClient_(
            FLAGS_quic_transport
                ? std::make_unique<MoQClient>(
                      evb,
                      std::move(url),
                      MoQRelaySession::createRelaySessionFactory())
                : std::make_unique<MoQWebTransportClient>(
                      evb,
                      std::move(url),
                      MoQRelaySession::createRelaySessionFactory())),
        fullTrackName_(std::move(ftn)) {}

  folly::coro::Task<MoQSession::SubscribeAnnouncesResult> subscribeAnnounces(
      SubscribeAnnounces subAnn) {
    auto res = co_await moqClient_.getSession()->subscribeAnnounces(subAnn);
    if (res.hasValue()) {
      subAnnouncesHandle_ = res.value();
    }
    co_return res;
  }

  // Response To PUBLISH
  PublishResult publish(
      PublishRequest pub,
      std::shared_ptr<SubscriptionHandle> handle) override {
    moxygen::PublishOk publishOk;
    publishOk.requestID = pub.requestID;

    // Default Params for Now
    publishOk.forward = true;
    publishOk.subscriberPriority = kDefaultPriority;
    publishOk.groupOrder = moxygen::GroupOrder::Default;
    publishOk.locType = moxygen::LocationType::AbsoluteStart;
    publishOk.start = moxygen::AbsoluteLocation(0, 0);

    if (handle) {
      subHandles_.push_back(handle);
    }

    // Build a PublishResponse
    moxygen::Subscriber::PublishConsumerAndReplyTask publishResponse{
        subTextReceiver_,
        folly::coro::makeTask(
            folly::Expected<moxygen::PublishOk, moxygen::PublishError>(
                publishOk))};

    return publishResponse;
  }

  folly::coro::Task<void> run(SubscribeRequest sub) noexcept {
    XLOG(INFO) << __func__;
    auto g =
        folly::makeGuard([func = __func__] { XLOG(INFO) << "exit " << func; });
    try {
      co_await moqClient_.setup(
          /*publisher=*/nullptr,
          /*subscriber=*/shared_from_this(),
          std::chrono::milliseconds(FLAGS_connect_timeout),
          std::chrono::seconds(FLAGS_transaction_timeout),
          quic::TransportSettings());

      if (FLAGS_publish) {
        SubscribeAnnounces subAnn{
            sub.requestID, sub.fullTrackName.trackNamespace, sub.params};
        co_await subscribeAnnounces(subAnn);

        if (FLAGS_unsubscribe) {
          co_await folly::coro::sleep(
              std::chrono::seconds(FLAGS_unsubscribe_time));

          for (auto& handle : subHandles_) {
            handle->unsubscribe();
          }
        }

        co_await subTextHandler_->baton;

        co_return;
      }

      Publisher::SubscribeResult track;
      if (FLAGS_jafetch || FLAGS_jrfetch) {
        FetchType fetchType = FLAGS_jafetch ? FetchType::ABSOLUTE_JOINING
                                            : FetchType::RELATIVE_JOINING;

        // Call join() for joining fetch
        fetchTextReceiver_ = std::make_shared<ObjectReceiver>(
            ObjectReceiver::FETCH, fetchTextHandler_);
        auto joinResult = co_await moqClient_.getSession()->join(
            sub,
            subTextReceiver_,
            FLAGS_join_start,
            sub.priority,
            sub.groupOrder,
            {},
            fetchTextReceiver_,
            fetchType);
        track = joinResult.subscribeResult;
      } else {
        track =
            co_await moqClient_.getSession()->subscribe(sub, subTextReceiver_);
      }
      bool needFetch = false;
      AbsoluteLocation fetchEnd{sub.endGroup + 1, 0};
      if (track.hasValue()) {
        subscription_ = std::move(track.value());
        auto requestID = subscription_->subscribeOk().requestID;
        XLOG(DBG1) << "requestID=" << requestID;
        auto largest = subscription_->subscribeOk().largest;
        if (largest) {
          XLOG(INFO) << "Largest={" << largest->group << ", " << largest->object
                     << "}";
        }
        if ((sub.locType == LocationType::AbsoluteStart ||
             sub.locType == LocationType::AbsoluteRange) &&
            subscription_ && subscription_->subscribeOk().largest &&
            *sub.start < *subscription_->subscribeOk().largest) {
          XLOG(INFO) << "Start before largest, need FETCH";
          needFetch = true;
          fetchEnd = AbsoluteLocation{
              subscription_->subscribeOk().largest->group,
              subscription_->subscribeOk().largest->object + 1};
          if (sub.locType == LocationType::AbsoluteRange &&
              fetchEnd.group >= sub.endGroup) {
            fetchEnd.group = sub.endGroup + 1;
            fetchEnd.object = 0;
            subscription_->unsubscribe();
            subscription_.reset();
            subTextReceiver_.reset();
          }
        }
      } else {
        XLOG(INFO) << "SubscribeError id=" << track.error().requestID
                   << " code=" << folly::to_underlying(track.error().errorCode)
                   << " reason=" << track.error().reasonPhrase;
        subTextReceiver_.reset();
        if (track.error().errorCode == SubscribeErrorCode::INVALID_RANGE) {
          XLOG(INFO) << "End before largest, need FETCH";
          needFetch = true;
        }
      }
      if (needFetch) {
        fetchTextReceiver_ = std::make_shared<ObjectReceiver>(
            ObjectReceiver::FETCH, fetchTextHandler_);
        auto fetchTrack = co_await moqClient_.getSession()->fetch(
            Fetch(
                RequestID(0),
                sub.fullTrackName,
                *sub.start,
                fetchEnd,
                sub.priority,
                sub.groupOrder),
            fetchTextReceiver_);
        if (fetchTrack.hasError()) {
          XLOG(ERR) << "Fetch failed err="
                    << folly::to_underlying(fetchTrack.error().errorCode)
                    << " reason=" << fetchTrack.error().reasonPhrase;
          fetchTextReceiver_.reset();
        }
      }
      if (moqClient_.getSession()) {
        moqClient_.getSession()->drain();
      }
    } catch (const std::exception& ex) {
      XLOG(ERR) << folly::exceptionStr(ex);
      co_return;
    }

    if (subTextReceiver_) {
      co_await subTextHandler_->baton;
    }
    if (fetchTextReceiver_) {
      co_await fetchTextHandler_->baton;
    }

    XLOG(INFO) << __func__ << " done";
  }

  folly::coro::Task<AnnounceResult> announce(
      Announce announce,
      std::shared_ptr<AnnounceCallback>) override {
    XLOG(INFO) << "Announce ns=" << announce.trackNamespace;
    // text client doesn't expect server or relay to announce anything,
    // but announce OK anyways
    return folly::coro::makeTask<AnnounceResult>(
        std::make_shared<AnnounceHandle>(AnnounceOk{announce.requestID, {}}));
  }

  void goaway(Goaway goaway) override {
    XLOG(INFO) << "Goaway uri=" << goaway.newSessionUri;
    stop();
  }

  void stop() {
    subTextHandler_->baton.post();
    fetchTextHandler_->baton.post();
    // TODO: maybe need fetchCancel
    if (subscription_) {
      subscription_->unsubscribe();
      subscription_.reset();
    }
    if (moqClient_.getSession()) {
      moqClient_.getSession()->close(SessionCloseErrorCode::NO_ERROR);
    }
  }

  MoQRelayClient moqClient_;
  FullTrackName fullTrackName_;
  std::shared_ptr<Publisher::SubscriptionHandle> subscription_;
  std::shared_ptr<TextHandler> subTextHandler_{
      std::make_shared<TextHandler>(/*fetch=*/false)};
  std::shared_ptr<TextHandler> fetchTextHandler_{
      std::make_shared<TextHandler>(/*fetch=*/true)};
  std::shared_ptr<ObjectReceiver> subTextReceiver_{
      std::make_shared<ObjectReceiver>(
          ObjectReceiver::SUBSCRIBE,
          subTextHandler_)};
  std::shared_ptr<ObjectReceiver> fetchTextReceiver_;
  std::shared_ptr<Publisher::SubscribeAnnouncesHandle> subAnnouncesHandle_;
  std::vector<std::shared_ptr<Publisher::SubscriptionHandle>> subHandles_;
};
} // namespace

using namespace moxygen;

int main(int argc, char* argv[]) {
  folly::Init init(&argc, &argv, false);
  folly::EventBase eventBase;
  proxygen::URL url(FLAGS_connect_url);
  if (!url.isValid() || !url.hasHost()) {
    XLOG(ERR) << "Invalid url: " << FLAGS_connect_url;
  }
  int sumFetchCount =
      int(FLAGS_jafetch) + int(FLAGS_jrfetch) + int(FLAGS_fetch);
  CHECK(sumFetchCount <= 1)
      << "Can specify at most one of jafetch or jrfetch or fetch";
  TrackNamespace ns =
      TrackNamespace(FLAGS_track_namespace, FLAGS_track_namespace_delimiter);
  std::shared_ptr<MoQFollyExecutorImpl> moqEvb =
      std::make_shared<MoQFollyExecutorImpl>(&eventBase);
  auto textClient = std::make_shared<MoQTextClient>(
      moqEvb, std::move(url), moxygen::FullTrackName({ns, FLAGS_track_name}));

  class SigHandler : public folly::AsyncSignalHandler {
   public:
    explicit SigHandler(folly::EventBase* evb, std::function<void(int)> fn)
        : folly::AsyncSignalHandler(evb), fn_(std::move(fn)) {
      registerSignalHandler(SIGTERM);
      registerSignalHandler(SIGINT);
    }
    void signalReceived(int signum) noexcept override {
      fn_(signum);
      unreg();
    }

    void unreg() {
      unregisterSignalHandler(SIGTERM);
      unregisterSignalHandler(SIGINT);
    }

   private:
    std::function<void(int)> fn_;
  };

  SigHandler handler(&eventBase, [&textClient](int) mutable {
    textClient->stop();
    textClient->moqClient_.shutdown();
  });

  auto subParams = flags2params();
  co_withExecutor(
      &eventBase,
      textClient->run(
          SubscribeRequest::make(
              moxygen::FullTrackName({std::move(ns), FLAGS_track_name}),
              0,
              GroupOrder::OldestFirst,
              FLAGS_forward,
              subParams.locType,
              subParams.start,
              subParams.endGroup,
              {})))
      .start()
      .via(&eventBase)
      .thenTry([&handler](auto) { handler.unreg(); });
  eventBase.loop();
}

/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <folly/base64.h>
#include <folly/coro/Sleep.h>
#include <folly/init/Init.h>
#include <folly/portability/GFlags.h>

// The following macros are defined both in 'libev' and 'libevent'.
#undef EV_READ
#undef EV_WRITE
#undef EV_TIMEOUT
#undef EV_SIGNAL
#undef EVLOOP_NONBLOCK

#include <moxygen/MoQClientMobile.h>
#include <moxygen/ObjectReceiver.h>

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
    "If client will act as a receiver for publish call (don't call subscribe and call sub_publishNamespace)");
DEFINE_bool(
    unsubscribe,
    false,
    "If client will unsubscribe from PUBLISH track after a specified time");
DEFINE_uint64(unsubscribe_time, 30, "Time to unsubscribe in seconds");
DEFINE_bool(
    use_legacy_setup,
    false,
    "If true, use only moq-00 ALPN (legacy). If false, use latest draft ALPN with fallback to legacy");
DEFINE_uint64(
    delivery_timeout,
    0,
    "Delivery timeout in milliseconds (0 = disabled)");

namespace {
using namespace moxygen;

struct SubParams {
  LocationType locType;
  std::optional<AbsoluteLocation> start;
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
      std::optional<TrackAlias> /*trackAlias*/,
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
      std::optional<TrackAlias> /*trackAlias*/,
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

class MoQTextClientMobile
    : public Subscriber,
      public std::enable_shared_from_this<MoQTextClientMobile> {
 public:
  MoQTextClientMobile(
      std::shared_ptr<MoQLibevExecutorImpl> evb,
      proxygen::URL url,
      FullTrackName ftn)
      : moqClient_(std::make_unique<MoQClientMobile>(evb, std::move(url))),
        fullTrackName_(std::move(ftn)) {}

  folly::coro::Task<MoQSession::SubscribeNamespaceResult> subscribeNamespace(
      SubscribeNamespace subAnn) {
    auto res = co_await moqClient_->moqSession_->subscribeNamespace(subAnn);
    if (res.hasValue()) {
      subscribeNamespaceHandle_ = res.value();
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
      // Default to experimental protocols, override to legacy if flag set
      std::vector<std::string> alpns =
          getDefaultMoqtProtocols(!FLAGS_use_legacy_setup);
      co_await moqClient_->setupMoQSession(
          std::chrono::milliseconds(FLAGS_connect_timeout),
          std::chrono::seconds(FLAGS_transaction_timeout),
          /*publishHandler=*/nullptr,
          /*subscribeHandler=*/shared_from_this(),
          quic::TransportSettings(),
          alpns);

      if (FLAGS_publish) {
        SubscribeNamespace subAnn{
            sub.requestID,
            sub.fullTrackName.trackNamespace,
            true /* forward */,
            sub.params};
        co_await subscribeNamespace(subAnn);

        if (FLAGS_unsubscribe) {
          co_await folly::coro::sleep(
              std::chrono::seconds(FLAGS_unsubscribe_time));

          for (auto& handle : subHandles_) {
            handle->unsubscribe();
          }
        }

        co_return;
      }

      Publisher::SubscribeResult track;
      if (FLAGS_jafetch || FLAGS_jrfetch) {
        FetchType fetchType = FLAGS_jafetch ? FetchType::ABSOLUTE_JOINING
                                            : FetchType::RELATIVE_JOINING;

        // Call join() for joining fetch
        fetchTextReceiver_ = std::make_shared<ObjectReceiver>(
            ObjectReceiver::FETCH, fetchTextHandler_);
        auto joinResult = co_await moqClient_->moqSession_->join(
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
            co_await moqClient_->moqSession_->subscribe(sub, subTextReceiver_);
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
        auto fetchTrack = co_await moqClient_->moqSession_->fetch(
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
      if (moqClient_->moqSession_) {
        moqClient_->moqSession_->drain();
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

  folly::coro::Task<PublishNamespaceResult> publishNamespace(
      PublishNamespace publishNamespace,
      std::shared_ptr<PublishNamespaceCallback>) override {
    XLOG(INFO) << "PublishNamespace ns=" << publishNamespace.trackNamespace;
    // text client doesn't expect server or relay to publishNamespace anything,
    // but publishNamespace OK anyways
    return folly::coro::makeTask<PublishNamespaceResult>(
        std::make_shared<PublishNamespaceHandle>(PublishNamespaceOk{
            .requestID = publishNamespace.requestID,
            .requestSpecificParams = {}}));
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
    if (moqClient_->moqSession_) {
      moqClient_->moqSession_->close(SessionCloseErrorCode::NO_ERROR);
    }
  }

  std::unique_ptr<MoQClientMobile> moqClient_;
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
  std::shared_ptr<Publisher::SubscribeNamespaceHandle>
      subscribeNamespaceHandle_;
  std::vector<std::shared_ptr<Publisher::SubscriptionHandle>> subHandles_;
};
} // namespace

using namespace moxygen;

struct EvLoopWeak : public quic::LibevQuicEventBase::EvLoopWeak {
  explicit EvLoopWeak(struct ev_loop* evLoop) : evLoop_(evLoop) {}
  struct ev_loop* get() override {
    return evLoop_;
  }
  struct ev_loop* evLoop_{nullptr};
  std::optional<pthread_t> getEventLoopThread() override {
    return pthread_self();
  }
};

int main(int argc, char* argv[]) {
  folly::Init init(&argc, &argv, false);
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

  struct ev_loop* evLoop = ev_loop_new(0);
  std::shared_ptr<MoQLibevExecutorImpl> moqEvb =
      std::make_shared<MoQLibevExecutorImpl>(
          std::make_unique<EvLoopWeak>(evLoop));

  auto textClient = std::make_shared<MoQTextClientMobile>(
      moqEvb, std::move(url), moxygen::FullTrackName({ns, FLAGS_track_name}));

  auto subParams = flags2params();
  std::vector<Parameter> params{};
  if (FLAGS_delivery_timeout > 0) {
    params.emplace_back(
        folly::to_underlying(TrackRequestParamKey::DELIVERY_TIMEOUT),
        FLAGS_delivery_timeout);
  }
  co_withExecutor(
      moqEvb.get(),
      textClient->run(
          SubscribeRequest::make(
              moxygen::FullTrackName({std::move(ns), FLAGS_track_name}),
              0,
              GroupOrder::OldestFirst,
              FLAGS_forward,
              subParams.locType,
              subParams.start,
              subParams.endGroup,
              params)))
      .start()
      .via(moqEvb.get());

  moqEvb->loopForever();
}

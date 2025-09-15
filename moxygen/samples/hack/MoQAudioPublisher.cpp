/*
 *  Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 *  This source code is licensed under the MIT license found in the LICENSE
 *  file in the root directory of this source tree.
 *
 */

// MoQAudioPublisher.cpp - audio-only publisher extracted from MoQVideoPublisher

#include <folly/coro/BlockingWait.h>
#include <folly/io/IOBuf.h>
#include <proxygen/lib/utils/URL.h>
#include <moxygen/extensions/FbTimestampExt.h>
#include <moxygen/moq_mi/MoQMi.h>
#include <moxygen/relay/MoQRelayClient.h>
#include <moxygen/samples/hack/MoQAudioPublisher.h>

constexpr std::chrono::milliseconds kConnectTimeout = std::chrono::seconds(5);
constexpr std::chrono::seconds kTransactionTimeout = std::chrono::seconds(60);

namespace {
uint64_t currentTimeMilliseconds() {
  auto now = std::chrono::system_clock::now();
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             now.time_since_epoch())
      .count();
}

const uint8_t AUDIO_STREAM_PRIORITY = 100; /* Lower is higher pri */
} // namespace

namespace moxygen {
class LocalSubscriptionHandle : public SubscriptionHandle {
 public:
  explicit LocalSubscriptionHandle(RequestID rid, TrackAlias alias) {
    SubscribeOk ok{
        rid,
        alias,
        std::chrono::milliseconds(0),
        GroupOrder::OldestFirst,
        folly::none,
        {}};
    setSubscribeOk(std::move(ok));
  }
  void unsubscribe() override {}
  void subscribeUpdate(SubscribeUpdate) override {}
};

std::pair<uint64_t, uint64_t> MoQAudioPublisher::getRttMicros() {
  uint64_t srtt = 0;
  uint64_t lrtt = 0;
  auto* evb = evbThread_ ? evbThread_->getEventBase() : nullptr;
  if (!evb || !relayClient_) {
    return {srtt, lrtt};
  }
  // Query on EB thread to avoid races
  evb->runInEventBaseThreadAndWait([&]() {
    if (auto session = relayClient_->getSession()) {
      auto ti = session->getTransportInfo();
      srtt = static_cast<uint64_t>(ti.srtt.count());
      lrtt = static_cast<uint64_t>(ti.lrtt.count());
    }
  });
  return {srtt, lrtt};
}

void MoQAudioPublisher::noteClientAudioSendTs(uint64_t ptsUs, uint64_t t0Us) {
  std::lock_guard<std::mutex> g(t0Mutex_);
  if (t0ByPts_.size() > 1024) {
    t0ByPts_.clear();
  }
  t0ByPts_[ptsUs] = t0Us;
}

bool MoQAudioPublisher::setup(
    const std::string& connectURL,
    std::shared_ptr<Subscriber> subscriber) {
  proxygen::URL url(connectURL);
  if (!url.isValid() || !url.hasHost()) {
    XLOG(ERR) << "Invalid url: " << connectURL;
    return false;
  }
  relayClient_ = std::make_unique<MoQRelayClient>(
      std::make_unique<MoQClient>(&moqExecutor_, url));

  cancel_ = folly::CancellationSource();
  running_ = true;
  folly::coro::blockingWait(co_withExecutor(
                                evbThread_->getEventBase(),
                                relayClient_->setup(
                                    /*publisher=*/shared_from_this(),
                                    /*subscriber=*/subscriber,
                                    kConnectTimeout,
                                    kTransactionTimeout))
                                .start());

  {
    auto* evb = evbThread_->getEventBase();
    relayStarted_.store(true, std::memory_order_relaxed);
    std::weak_ptr<MoQAudioPublisher> selfWeak = shared_from_this();
    auto selfPub = shared_from_this();
    auto ns = audioForwarder_.fullTrackName().trackNamespace;
    co_withExecutor(
        evb,
        folly::coro::co_invoke(
            [selfWeak, selfPub, ns]() -> folly::coro::Task<void> {
              auto selfOwner = selfWeak.lock();
              if (!selfOwner)
                co_return;
              auto relay = selfOwner->relayClient_.get();
              if (!relay) {
                selfOwner->runDone_.post();
                co_return;
              }
              try {
                co_await folly::coro::co_withCancellation(
                    selfOwner->cancel_.getToken(), relay->run(selfPub, {ns}));
              } catch (const folly::OperationCancelled&) {
                XLOG(DBG1) << "relay->run cancelled";
              }
              if (selfOwner) {
                selfOwner->runDone_.post();
              }
              co_return;
            }))
        .start();
  }

  if (auto session = relayClient_->getSession()) {
    auto ftn = audioForwarder_.fullTrackName();
    auto* evb = evbThread_->getEventBase();
    co_withExecutor(evb, initialAudioPublish(session, ftn)).start();
  } else {
    XLOG(ERR) << "No session available for audio publish";
  }

  return true;
}

folly::coro::Task<void> MoQAudioPublisher::initialAudioPublish(
    std::shared_ptr<MoQSession> session,
    FullTrackName ftn) {
  PublishRequest pub;
  pub.fullTrackName = std::move(ftn);
  pub.groupOrder = GroupOrder::OldestFirst;
  pub.forward = true;

  auto handle =
      std::make_shared<LocalSubscriptionHandle>(RequestID(0), TrackAlias(0));
  auto res = session->publish(std::move(pub), handle);
  if (!res) {
    XLOG(ERR) << "PUBLISH(audio) failed: code="
              << folly::to_underlying(res.error().errorCode)
              << " reason=" << res.error().reasonPhrase;
    co_return;
  }
  audioTrackPublisher_ = std::move(res->consumer);
  audioPublishReady_ = true;

  // Await reply in background on EB and clear readiness on error
  {
    auto replyTask = folly::coro::co_withCancellation(
        cancel_.getToken(), std::move(res->reply));
    auto* replyEvb = evbThread_->getEventBase();
    std::weak_ptr<MoQAudioPublisher> selfWeak = shared_from_this();
    co_withExecutor(
        replyEvb,
        folly::coro::co_invoke(
            [selfWeak, replyTask = std::move(replyTask)]() mutable
            -> folly::coro::Task<void> {
              try {
                auto reply = co_await std::move(replyTask);
                if (reply.hasError()) {
                  if (auto self = selfWeak.lock()) {
                    self->audioPublishReady_ = false;
                    self->audioTrackPublisher_.reset();
                  }
                }
              } catch (const std::exception&) {
                if (auto self = selfWeak.lock()) {
                  self->audioPublishReady_ = false;
                  self->audioTrackPublisher_.reset();
                }
              }
              co_return;
            }))
        .start();
  }
  co_return;
}

folly::coro::Task<Publisher::SubscribeResult> MoQAudioPublisher::subscribe(
    SubscribeRequest sub,
    std::shared_ptr<TrackConsumer> callback) {
  if (sub.fullTrackName == audioForwarder_.fullTrackName()) {
    co_return audioForwarder_.addSubscriber(
        MoQSession::getRequestSession(), sub, std::move(callback));
  }

  XLOG(ERR) << "Unknown track " << sub.fullTrackName;
  co_return folly::makeUnexpected(SubscribeError{
      sub.requestID, SubscribeErrorCode::TRACK_NOT_EXIST, "Unknown track"});
}

void MoQAudioPublisher::publishAudioFrame(
    std::chrono::microseconds ptsUs,
    uint64_t flags,
    Payload payload) {
  std::weak_ptr<MoQAudioPublisher> selfWeak = shared_from_this();
  evbThread_->getEventBase()->add(
      [selfWeak, ptsUs, flags, payload = std::move(payload)]() mutable {
        auto self = selfWeak.lock();
        if (!self) {
          return;
        }
        if (!self->running_) {
          return;
        }
        self->publishAudioFrameImpl(ptsUs, flags, std::move(payload));
      });
}

void MoQAudioPublisher::publishAudioFrameImpl(
    std::chrono::microseconds ptsUs,
    uint64_t /*flags*/,
    Payload payload) {
  if (!audioTrackPublisher_ || !audioPublishReady_) {
    XLOG(DBG1) << "Audio publish path not established yet";
    return;
  }

  auto item = std::make_unique<MediaItem>();
  item->type = MediaType::AUDIO;
  item->id = audioSeqId_++;
  item->timescale = 1000000;
  item->pts = ptsUs.count();
  item->dts = item->pts;
  item->sampleFreq = 44100;
  item->numChannels = 1;

  if (lastAudioPts_) {
    item->duration = item->pts - *lastAudioPts_;
    if (item->duration <= 0 || item->duration > 100000) {
      item->duration = 23220; // default for AAC 44.1kHz
    }
  } else {
    item->duration = 23220; // default for first frame
  }

  lastAudioPts_ = item->pts;
  item->wallclock = currentTimeMilliseconds();
  item->isIdr = false;
  item->isEOF = false;
  item->data = std::move(payload);

  publishAudioFrameToMoQ(std::move(item));
}

void MoQAudioPublisher::publishAudioFrameToMoQ(
    std::unique_ptr<MediaItem> item) {
  auto id = item->id;
  auto pts = item->pts;
  auto moqMiObj = MoQMi::encodeToMoQMi(std::move(item));
  if (!moqMiObj) {
    XLOG(ERR) << "Failed to encode audio frame";
    return;
  }

  ObjectHeader objHeader = ObjectHeader{
      /*groupIn=*/id,
      /*subgroupIn=*/0,
      /*idIn=*/0,
      AUDIO_STREAM_PRIORITY,
      ObjectStatus::NORMAL,
      std::move(moqMiObj->extensions)};

  if (useTimestampExt_.load(std::memory_order_relaxed)) {
    uint64_t t0 = 0;
    {
      std::lock_guard<std::mutex> g(t0Mutex_);
      auto it = t0ByPts_.find(pts);
      if (it != t0ByPts_.end()) {
        t0 = it->second;
        t0ByPts_.erase(it);
      }
    }
    if (t0 == 0) {
      t0 = fbext::nowUsMono();
    }
    fbext::appendIntExtIfMissing(
        objHeader.extensions, fbext::kExtFbTsClientSendUs, t0);
  }

  if (auto res = audioTrackPublisher_->objectStream(
          objHeader, std::move(moqMiObj->payload));
      !res) {
    XLOG(ERR) << "audio objectStream error: " << res.error().describe();
  }
}

void MoQAudioPublisher::endPublish() {
  std::weak_ptr<MoQAudioPublisher> selfWeak = shared_from_this();
  evbThread_->getEventBase()->add([selfWeak] {
    if (auto self = selfWeak.lock()) {
      self->running_ = false;
      self->cancel_.requestCancellation();
    }
  });

  evbThread_->getEventBase()->runInEventBaseThreadAndWait([selfWeak] {
    if (auto self = selfWeak.lock()) {
      self->audioPublishReady_ = false;
      self->audioTrackPublisher_.reset();
    }
  });

  if (relayStarted_.load(std::memory_order_relaxed)) {
    runDone_.wait();
  }

  if (relayClient_) {
    std::weak_ptr<MoQAudioPublisher> selfWeak2 = shared_from_this();
    evbThread_->getEventBase()->runInEventBaseThreadAndWait([selfWeak2] {
      if (auto self2 = selfWeak2.lock()) {
        self2->relayClient_->shutdown();
      }
    });
  }
}

} // namespace moxygen

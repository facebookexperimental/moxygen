/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/lane_manager/CatalogReader.h"

#include "moxygen/MoQSession.h"
#include "moxygen/ObjectReceiver.h"
#include "moxygen/lane_manager/ManagedSubscription.h"
#include "moxygen/msf/MediaCatalog.h"

#include <folly/CancellationToken.h>
#include <folly/base64.h>
#include <folly/coro/Error.h>
#include <folly/coro/SharedPromise.h>
#include <folly/coro/Sleep.h>
#include <folly/coro/Timeout.h>
#include <folly/coro/WithCancellation.h>
#include <folly/logging/xlog.h>

#include <algorithm>
#include <chrono>
#include <utility>

namespace moxygen::lane_manager {

namespace {

using namespace std::chrono_literals;

constexpr auto kCatalogFetchTimeout = 5s;

folly::coro::Task<void> waitForCatalogRetry(
    std::chrono::milliseconds remaining) {
  constexpr auto kMaxSleep =
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::hours(24));
  while (remaining > 0ms) {
    const auto part = std::min(remaining, kMaxSleep);
    co_await folly::coro::sleep(part);
    remaining -= part;
  }
}

bool retryableCatalogFetch(const FetchError& error) {
  if (error.retryInterval && error.retryInterval->count() == 0) {
    return false;
  }
  return error.errorCode == FetchErrorCode::INTERNAL_ERROR ||
      error.errorCode == FetchErrorCode::TIMEOUT ||
      error.errorCode == FetchErrorCode::GOING_AWAY ||
      error.errorCode == FetchErrorCode::EXCESSIVE_LOAD ||
      error.errorCode == FetchErrorCode::DOES_NOT_EXIST ||
      error.errorCode == FetchErrorCode::REDIRECT ||
      error.errorCode == FetchErrorCode::CANCELLED;
}

template <typename T, typename S>
std::optional<T> positive(std::optional<S> value) {
  if (value && *value > 0) {
    return static_cast<T>(*value);
  }
  return std::nullopt;
}

std::shared_ptr<const folly::IOBuf> decodeInit(
    const MediaCatalog& catalog,
    const std::string& initRef) {
  for (const auto& init : catalog.initDataList) {
    if (init.id == initRef && init.type == "inline") {
      try {
        return folly::IOBuf::copyBuffer(folly::base64Decode(init.data));
      } catch (const std::exception& ex) {
        XLOG(ERR) << "[lane_manager] bad init " << initRef << ": " << ex.what();
      }
    }
  }
  return nullptr;
}

LaneInfo makeLane(const MediaCatalog& catalog, const CatalogTrack& track) {
  LaneInfo lane;
  lane.trackName = track.name;
  lane.codec = track.codec;
  lane.bitrateBps = positive<uint64_t>(track.bitrate);
  lane.avgBitrateBps = positive<uint64_t>(track.avgBitrate);
  lane.width = positive<uint32_t>(track.width);
  lane.height = positive<uint32_t>(track.height);
  lane.sampleRate = positive<uint32_t>(track.samplerate);
  lane.init = decodeInit(catalog, track.initRef);
  return lane;
}

uint64_t size(const LaneInfo& lane) {
  if (lane.width && lane.height) {
    return uint64_t{*lane.width} * *lane.height;
  }
  return lane.sampleRate.value_or(0);
}

std::optional<SwitchingSet> makeSet(
    const MediaCatalog& catalog,
    std::string_view role) {
  std::optional<SwitchingSet> set;
  std::optional<int32_t> altGroup;
  for (const auto& track : catalog.tracks) {
    // FBMS passthrough ("live-pst-*") is not a switchable lane.
    if (track.role != role || track.name.find("-pst-") != std::string::npos) {
      continue;
    }
    if (!set) {
      set.emplace();
      set->id = track.altGroup.value_or(0);
      altGroup = track.altGroup;
    } else if (!altGroup || track.altGroup != altGroup) {
      continue;
    }
    set->lanes.push_back(makeLane(catalog, track));
  }
  if (!set) {
    return set;
  }
  const bool byBitrate =
      std::all_of(set->lanes.begin(), set->lanes.end(), [](const auto& lane) {
        return lane.bitrateBps.has_value();
      });
  std::stable_sort(
      set->lanes.begin(),
      set->lanes.end(),
      [byBitrate](const LaneInfo& left, const LaneInfo& right) {
        return byBitrate ? *left.bitrateBps < *right.bitrateBps
                         : size(left) < size(right);
      });
  return set;
}

} // namespace

struct CatalogReader::Acquisition {
  bool needsCatalog() const {
    return !closed && !ready.isFulfilled();
  }

  void cancelFetch() {
    if (auto handle = std::exchange(fetch, nullptr)) {
      handle->fetchCancel();
    }
  }

  std::shared_ptr<ManagedSubscription> subscription;
  std::shared_ptr<Publisher::FetchHandle> fetch;
  std::optional<FetchError> error;
  folly::coro::SharedPromise<void> ready;
  folly::CancellationSource cancelled;
  bool opened{false};
  bool closed{false};
};

class CatalogReader::Callback : public ObjectReceiverCallback {
 public:
  explicit Callback(CatalogReader* reader) : reader(reader) {}

  FlowControlState onObject(
      std::optional<TrackAlias> /*trackAlias*/,
      const ObjectHeader& /*header*/,
      Payload payload) override {
    if (reader) {
      reader->onCatalog(std::move(payload));
    }
    return FlowControlState::UNBLOCKED;
  }
  void onObjectStatus(
      std::optional<TrackAlias> /*trackAlias*/,
      const ObjectHeader& /*header*/) override {}
  void onEndOfStream() override {}
  void onError(ResetStreamErrorCode /*error*/) override {}
  void onPublishDone(PublishDone /*done*/) override {
    if (reader) {
      reader->onSubscriptionDone();
    }
  }

  CatalogReader* reader;
};

CatalogReader::CatalogReader(
    std::shared_ptr<Publisher> proxy,
    TrackNamespace broadcast,
    std::chrono::milliseconds firstCatalogWait,
    Mode mode)
    : proxy_(std::move(proxy)),
      track_{std::move(broadcast), std::string(kCatalogTrackName)},
      callback_(std::make_shared<Callback>(this)),
      acquisition_(std::make_shared<Acquisition>()),
      firstCatalogWait_(firstCatalogWait),
      mode_(mode) {}

CatalogReader::~CatalogReader() {
  close();
}

folly::coro::Task<bool> CatalogReader::subscribe(RequestID requestID) {
  auto acquisition = acquisition_;
  const auto wait = firstCatalogWait_;
  if (acquisition->closed) {
    co_return false;
  }
  if (!acquisition->opened) {
    acquisition->opened = true;
    if (mode_ == Mode::ImmutableSnapshot) {
      folly::coro::co_withExecutor(
          co_await folly::coro::co_current_executor,
          folly::coro::co_withCancellation(
              acquisition->cancelled.getToken(),
              fetchSnapshot(
                  proxy_,
                  acquisition,
                  callback_,
                  Fetch(
                      requestID,
                      track_,
                      AbsoluteLocation{0, 0},
                      AbsoluteLocation{0, 1}))))
          .start();
    } else {
      startSubscription(requestID);
    }
  }
  co_await folly::coro::timeout(acquisition->ready.getFuture(), wait);
  co_return !acquisition->closed;
}

void CatalogReader::startSubscription(RequestID requestID) {
  auto acquisition = acquisition_;
  auto& executor = *MoQSession::getRequestContext().executor;
  auto request = SubscribeRequest::make(
      track_,
      kDefaultPriority,
      std::nullopt,
      /*forward=*/true,
      LocationFilter::nextObject());
  request.requestID = requestID;
  acquisition->subscription = std::make_shared<ManagedSubscription>(
      proxy_,
      std::move(request),
      std::make_shared<ObjectReceiver>(ObjectReceiver::SUBSCRIBE, callback_),
      executor);
  folly::coro::co_withExecutor(
      &executor,
      folly::coro::co_withCancellation(
          acquisition->cancelled.getToken(),
          fetchInitialCatalog(acquisition, callback_, requestID)))
      .start();
}

folly::coro::Task<void> CatalogReader::fetchInitialCatalog(
    std::shared_ptr<Acquisition> acquisition,
    std::shared_ptr<Callback> callback,
    RequestID requestID) {
  const auto token = acquisition->cancelled.getToken();
  while (acquisition->needsCatalog()) {
    auto fetched = co_await folly::coro::co_awaitTry(
        folly::coro::timeoutNoDiscard(
            acquisition->subscription->subscribeWithJoiningFetch(
                requestID,
                std::make_shared<ObjectReceiver>(
                    ObjectReceiver::FETCH, callback),
                FetchType::RELATIVE_JOINING),
            kCatalogFetchTimeout));
    std::optional<std::chrono::milliseconds> retryInterval;
    if (fetched.hasValue() && fetched->hasValue()) {
      auto handle = std::move(fetched->value());
      if (acquisition->closed) {
        handle->fetchCancel();
        co_return;
      }
      acquisition->fetch = std::move(handle);
      auto ready = co_await folly::coro::co_awaitTry(
          folly::coro::timeout(
              acquisition->ready.getFuture(), kCatalogFetchTimeout));
      if (!acquisition->needsCatalog()) {
        co_return;
      }
      acquisition->cancelFetch();
      if (!ready.hasException<folly::FutureTimeout>()) {
        co_return;
      }
    } else {
      if (!acquisition->needsCatalog() || token.isCancellationRequested()) {
        co_return;
      }
      if (fetched.hasValue()) {
        const auto& error = fetched->error();
        if (!retryableCatalogFetch(error)) {
          XLOG(WARN) << "[lane_manager] catalog joining FETCH rejected; "
                        "waiting for live catalog: "
                     << error.reasonPhrase;
          co_return;
        }
        XLOG(WARN) << "[lane_manager] catalog joining FETCH failed: "
                   << error.reasonPhrase;
        retryInterval = error.retryInterval;
      } else {
        XLOG(WARN) << "[lane_manager] catalog joining FETCH exception: "
                   << fetched.exception().what();
      }
    }
    if (!acquisition->needsCatalog() || token.isCancellationRequested()) {
      co_return;
    }
    const auto delay = std::max(100ms, retryInterval.value_or(0ms));
    XLOG(DBG1) << "[lane_manager] catalog joining FETCH retryDelayMs="
               << delay.count();
    auto waited = co_await folly::coro::co_awaitTry(waitForCatalogRetry(delay));
    if (waited.hasException()) {
      co_return;
    }
  }
}

folly::coro::Task<void> CatalogReader::fetchSnapshot(
    std::shared_ptr<Publisher> proxy,
    std::shared_ptr<Acquisition> acquisition,
    std::shared_ptr<Callback> callback,
    Fetch fetch) {
  const auto requestID = fetch.requestID;
  folly::coro::co_withExecutor(
      co_await folly::coro::co_current_executor,
      folly::coro::co_withCancellation(
          acquisition->cancelled.getToken(),
          waitForSnapshot(acquisition, callback, requestID)))
      .start();
  auto fetched = co_await folly::coro::co_awaitTry(proxy->fetch(
      std::move(fetch),
      std::make_shared<ObjectReceiver>(ObjectReceiver::FETCH, callback)));
  if (fetched.hasException()) {
    if (!acquisition->needsCatalog()) {
      co_return;
    }
    XLOG(WARN) << "[lane_manager] catalog snapshot FETCH exception: "
               << fetched.exception().what();
    if (callback->reader) {
      callback->reader->fail(
          FetchError{
              requestID,
              fetched.hasException<folly::FutureTimeout>()
                  ? FetchErrorCode::TIMEOUT
                  : FetchErrorCode::INTERNAL_ERROR,
              fetched.exception().what().toStdString()});
    }
    co_return;
  }
  if (fetched->hasError()) {
    XLOG(ERR) << "[lane_manager] catalog snapshot FETCH failed: "
              << fetched->error().reasonPhrase;
    if (callback->reader) {
      callback->reader->fail(std::move(fetched->error()));
    }
    co_return;
  }
  if (acquisition->closed) {
    fetched->value()->fetchCancel();
    co_return;
  }
  acquisition->fetch = std::move(fetched->value());
}

folly::coro::Task<void> CatalogReader::waitForSnapshot(
    std::shared_ptr<Acquisition> acquisition,
    std::shared_ptr<Callback> callback,
    RequestID requestID) {
  auto ready = co_await folly::coro::co_awaitTry(
      folly::coro::timeout(
          acquisition->ready.getFuture(), kCatalogFetchTimeout));
  if (ready.hasException<folly::FutureTimeout>() &&
      acquisition->needsCatalog()) {
    XLOG(WARN) << "[lane_manager] catalog snapshot acquisition timed out";
    if (callback->reader) {
      callback->reader->fail(
          FetchError{
              requestID,
              FetchErrorCode::TIMEOUT,
              "catalog snapshot acquisition timed out"});
    }
  }
}

std::optional<FetchError> CatalogReader::error() const {
  return acquisition_->error;
}

void CatalogReader::fail(FetchError error) {
  if (!acquisition_->needsCatalog()) {
    return;
  }
  acquisition_->error = std::move(error);
  close();
}

void CatalogReader::close() {
  auto acquisition = acquisition_;
  acquisition->closed = true;
  callback_->reader = nullptr;
  acquisition->cancelled.requestCancellation();
  if (auto subscription = acquisition->subscription) {
    subscription->unsubscribe();
  }
  acquisition->cancelFetch();
  if (!acquisition->ready.isFulfilled()) {
    acquisition->ready.setValue();
  }
}

void CatalogReader::onSubscriptionDone() {
  auto acquisition = acquisition_;
  if (acquisition->subscription && acquisition->subscription->ended()) {
    if (acquisition->needsCatalog()) {
      acquisition->error = acquisition->subscription->terminalError();
    }
    acquisition->closed = true;
    callback_->reader = nullptr;
    acquisition->cancelled.requestCancellation();
    acquisition->cancelFetch();
    if (!acquisition->ready.isFulfilled()) {
      acquisition->ready.setValue();
    }
  }
}

void CatalogReader::setUpdateCallback(std::function<void()> onUpdate) {
  onUpdate_ = std::move(onUpdate);
}

const SwitchingSet* CatalogReader::switchingSet(Role role) const {
  const auto& set = role == Role::Video ? video_ : audio_;
  return set ? &*set : nullptr;
}

void CatalogReader::onCatalog(Payload payload) {
  auto acquisition = acquisition_;
  if (!payload) {
    return;
  }
  auto catalog = parseCatalog(payload->coalesce());
  if (!catalog) {
    return;
  }
  video_ = makeSet(*catalog, "video");
  audio_ = makeSet(*catalog, "audio");
  if (video_ && audio_) {
    video_->audioAllowanceBps = audio_->lanes.front().bitrateBps;
  }
  if (onUpdate_) {
    onUpdate_();
  }
  if (!acquisition->ready.isFulfilled()) {
    acquisition->ready.setValue();
  }
}

} // namespace moxygen::lane_manager

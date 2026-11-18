/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/lane_manager/CatalogReader.h"

#include "moxygen/ObjectReceiver.h"
#include "moxygen/msf/MediaCatalog.h"

#include <folly/base64.h>
#include <folly/coro/Error.h>
#include <folly/coro/Timeout.h>
#include <folly/logging/xlog.h>

#include <algorithm>

namespace moxygen::lane_manager {

namespace {

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
  void onPublishDone(PublishDone /*done*/) override {}

  CatalogReader* reader;
};

CatalogReader::CatalogReader(
    std::shared_ptr<Publisher> publisher,
    TrackNamespace broadcast,
    std::chrono::milliseconds firstCatalogWait)
    : publisher_(std::move(publisher)),
      track_{std::move(broadcast), std::string(kCatalogTrackName)},
      callback_(std::make_shared<Callback>(this)),
      firstCatalogWait_(firstCatalogWait) {}

CatalogReader::~CatalogReader() {
  close();
}

folly::coro::Task<bool> CatalogReader::open(RequestID requestID) {
  if (!opened_) {
    opened_ = true;
    auto request = SubscribeRequest::make(
        track_,
        kDefaultPriority,
        std::nullopt,
        /*forward=*/true,
        LocationFilter::nextObject());
    request.requestID = requestID;
    auto subscribed = co_await folly::coro::co_awaitTry(publisher_->subscribe(
        std::move(request),
        std::make_shared<ObjectReceiver>(
            ObjectReceiver::SUBSCRIBE, callback_)));
    if (subscribed.hasException()) {
      close();
      co_yield folly::coro::co_error(std::move(subscribed.exception()));
    }
    if (subscribed->hasError()) {
      XLOG(ERR) << "[lane_manager] catalog SUBSCRIBE failed: "
                << subscribed->error().reasonPhrase;
      close();
      co_return false;
    }
    if (closed_) {
      subscribed->value()->unsubscribe();
      co_return false;
    }
    subscription_ = std::move(subscribed->value());
    // A failed FETCH leaves the next published catalog to complete open().
    Fetch fetch(requestID, requestID, 0, FetchType::RELATIVE_JOINING);
    fetch.fullTrackName = track_;
    auto fetched = co_await publisher_->fetch(
        std::move(fetch),
        std::make_shared<ObjectReceiver>(ObjectReceiver::FETCH, callback_));
    if (fetched.hasValue()) {
      fetch_ = std::move(fetched.value());
    }
  }
  co_await folly::coro::timeout(ready_.getFuture(), firstCatalogWait_);
  co_return !closed_;
}

void CatalogReader::close() {
  closed_ = true;
  callback_->reader = nullptr;
  if (auto subscription = std::exchange(subscription_, nullptr)) {
    subscription->unsubscribe();
  }
  if (auto fetch = std::exchange(fetch_, nullptr)) {
    fetch->fetchCancel();
  }
  if (!ready_.isFulfilled()) {
    ready_.setValue();
  }
}

const SwitchingSet* CatalogReader::switchingSet(Role role) const {
  const auto& set = role == Role::Video ? video_ : audio_;
  return set ? &*set : nullptr;
}

void CatalogReader::onCatalog(Payload payload) {
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
  if (!ready_.isFulfilled()) {
    ready_.setValue();
  }
}

} // namespace moxygen::lane_manager

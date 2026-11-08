/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/proxy/MoQFetchCacheWriter.h"

#include <utility>

namespace moxygen {

class MoQFetchCacheWriter::CacheSink final
    : public StreamingObjectReceiverCallback,
      public StreamingObjectPayloadConsumer,
      public std::enable_shared_from_this<CacheSink> {
 public:
  CacheSink(
      std::weak_ptr<MoQGroupCache> cache,
      OnFinished onFinished,
      GroupOrder groupOrder)
      : cache_(std::move(cache)),
        start_(kLocationMin),
        end_(kLocationMax),
        onFinished_(std::move(onFinished)),
        groupOrder_(groupOrder) {}

  CacheSink(
      std::weak_ptr<MoQGroupCache> cache,
      AbsoluteLocation start,
      AbsoluteLocation end,
      OnFinished onFinished,
      GroupOrder groupOrder)
      : CacheSink(std::move(cache), std::move(onFinished), groupOrder) {
    start_ = start;
    endBound_ = end;
    startKnown_ = true;
  }

  ~CacheSink() override {
    finish(false);
    if (onFinished_) {
      std::move(onFinished_)(false);
    }
  }

  bool setRange(AbsoluteLocation start, AbsoluteLocation end) {
    const bool invalid = end < start || (startKnown_ && start != start_) ||
        endBound_ < end ||
        (observedMin_ &&
         (*observedMin_ < start || !(observedMax_.value() < end)));
    start_ = start;
    end_ = end;
    startKnown_ = true;
    rangeKnown_ = true;
    if (invalid) {
      invalidate();
    } else {
      completeFirstEdge();
      if (finished_ && clean_) {
        completeLastEdge();
      }
    }
    notifyFinished();
    return !disabled_;
  }

  folly::
      Expected<std::shared_ptr<StreamingObjectPayloadConsumer>, MoQPublishError>
      onObjectBegin(StreamingObjectContext context) override {
    auto prepared = prepare(std::move(context.header));
    if (prepared.hasError()) {
      return folly::makeUnexpected(std::move(prepared.error()));
    }
    pending_ = std::move(prepared.value());
    return std::static_pointer_cast<StreamingObjectPayloadConsumer>(
        shared_from_this());
  }

  folly::Expected<folly::Unit, MoQPublishError> onPayload(
      Payload payload,
      StreamingObjectPayloadMetadata metadata) override {
    if (pending_ && payload) {
      if (pending_->payload) {
        pending_->payload->appendToChain(std::move(payload));
      } else {
        pending_->payload = std::move(payload);
      }
    }
    if (!metadata.endOfObject || !pending_) {
      return folly::unit;
    }
    auto pending = std::move(*pending_);
    pending_.reset();
    return store(std::move(pending));
  }

  void onObjectStatus(StreamingObjectContext context) override {
    const auto group = context.header.group;
    const bool endsGroup = isEndStatus(context.header.status);
    context.header.length = 0;
    auto result = record(std::move(context.header));
    if (result.hasError()) {
      statusError_ = std::move(result.error());
    } else if (endsGroup && currentGroupCacheable_) {
      auto start = AbsoluteLocation{group, 0};
      if (startKnown_ && start_.group == group) {
        start = start_;
      }
      completeRange(start, {group + 1, 0});
    }
  }

  void onEndOfStream(StreamingSubgroupContext) override {}

  void onError(ResetStreamErrorCode) override {
    finish(false);
  }

  void onError(StreamingSubgroupContext, ResetStreamErrorCode) override {
    finish(false);
  }

  void onPublishDone(PublishDone) override {}

  void onUnknownRange(StreamingUnknownRange) override {
    cacheable_ = false;
    completionDisabled_ = true;
  }

  void onAllDataReceived() override {
    finish(true);
  }

  std::optional<MoQPublishError> takeStatusError() {
    return std::exchange(statusError_, std::nullopt);
  }

 private:
  struct Pending {
    ObjectHeader header;
    Payload payload;
    MoQCacheCharge charge;
  };

  folly::Expected<folly::Unit, MoQPublishError> record(ObjectHeader header) {
    auto prepared = prepare(std::move(header));
    if (prepared.hasError()) {
      return folly::makeUnexpected(std::move(prepared.error()));
    }
    if (!prepared.value()) {
      return folly::unit;
    }
    return store(std::move(*prepared.value()));
  }

  folly::Expected<folly::Unit, MoQPublishError> store(Pending object) {
    auto cache = cache_.lock();
    if (!cache) {
      cacheable_ = false;
      markCurrentGroupUncacheable();
      return folly::unit;
    }
    const bool datagram = object.header.forwardingPreferenceIsDatagram;
    auto result = cache->storeFetchObject(
        std::move(object.header),
        std::move(object.payload),
        datagram,
        std::move(object.charge));
    if (result == MoQGroupCache::Result::Malformed) {
      return malformed();
    }
    cacheable_ &= result == MoQGroupCache::Result::Recorded;
    if (result != MoQGroupCache::Result::Recorded) {
      markCurrentGroupUncacheable();
    }
    return folly::unit;
  }

  folly::Expected<std::optional<Pending>, MoQPublishError> prepare(
      ObjectHeader header) {
    if (disabled_) {
      return std::optional<Pending>{};
    }
    auto location = AbsoluteLocation{header.group, header.id};
    if (!observedMin_ || location < *observedMin_) {
      observedMin_ = location;
    }
    if (!observedMax_ || *observedMax_ < location) {
      observedMax_ = location;
    }
    auto cache = cache_.lock();
    const auto& endLimit = rangeKnown_ ? end_ : endBound_;
    if ((startKnown_ && location < start_) || !(location < endLimit)) {
      invalidate();
      return std::optional<Pending>{};
    }
    auto advanced = advanceTo(header.group);
    if (advanced.hasError()) {
      return folly::makeUnexpected(std::move(advanced.error()));
    }
    if (!cache || cache->isShutdown()) {
      cacheable_ = false;
      markCurrentGroupUncacheable();
      return std::optional<Pending>{};
    }
    auto charge = cache->reserveFetchObject(header);
    if (charge.hasError()) {
      return malformed();
    }
    if (!charge.value()) {
      const auto next = location.next();
      const bool alreadyCached = next &&
          cache->inspect(location, *next) == MoQGroupCache::Coverage::Ready;
      cacheable_ &= alreadyCached;
      if (!alreadyCached) {
        markCurrentGroupUncacheable();
      }
      return std::optional<Pending>{};
    }
    return Pending{std::move(header), nullptr, std::move(*charge.value())};
  }

  folly::Unexpected<MoQPublishError> malformed() {
    cacheable_ = false;
    completionDisabled_ = true;
    finish(false);
    return folly::makeUnexpected(
        MoQPublishError(MoQPublishError::MALFORMED_TRACK, "malformed FETCH"));
  }

  folly::Expected<folly::Unit, MoQPublishError> advanceTo(uint64_t group) {
    if (!currentGroup_) {
      firstGroup_ = currentGroup_ = group;
      return folly::unit;
    }
    if (*currentGroup_ == group) {
      return folly::unit;
    }

    auto completed = folly::Expected<folly::Unit, MoQPublishError>(folly::unit);
    if (completionDisabled_) {
      currentGroup_ = group;
      currentGroupCacheable_ = true;
      return folly::unit;
    }
    if (groupOrder_ == GroupOrder::OldestFirst && currentGroupCacheable_) {
      auto start = AbsoluteLocation{*currentGroup_, 0};
      if (*currentGroup_ == *firstGroup_ && startKnown_) {
        start = start_;
      }
      completed = complete(start, {group, 0});
    } else if (groupOrder_ == GroupOrder::NewestFirst) {
      if (*currentGroup_ == *firstGroup_ && !rangeKnown_) {
        completed = complete({group + 1, 0}, {*currentGroup_, 0});
      } else if (currentGroupCacheable_) {
        auto end = *currentGroup_ == *firstGroup_
            ? end_
            : AbsoluteLocation{*currentGroup_ + 1, 0};
        completed = complete({group + 1, 0}, end);
      }
    }
    if (completed.hasError()) {
      return completed;
    }
    if (!firstSuccessor_) {
      firstSuccessor_ = group;
    }
    currentGroup_ = group;
    currentGroupCacheable_ = true;
    return folly::unit;
  }

  folly::Expected<folly::Unit, MoQPublishError> complete(
      AbsoluteLocation start,
      AbsoluteLocation end) {
    if (completionDisabled_ || !(start < end)) {
      return folly::unit;
    }
    auto cache = cache_.lock();
    if (!cache) {
      cacheable_ = false;
      return folly::unit;
    }
    auto result = cache->completeFetch(start, end);
    if (result == MoQGroupCache::Result::Malformed) {
      return malformed();
    }
    if (result == MoQGroupCache::Result::NotCached &&
        cache->inspect(start, end) == MoQGroupCache::Coverage::Miss) {
      cacheable_ = false;
    }
    return folly::unit;
  }

  void completeRange(AbsoluteLocation start, AbsoluteLocation end) {
    auto result = complete(start, end);
    if (result.hasError()) {
      statusError_ = std::move(result.error());
    }
  }

  void completeFirstEdge() {
    if (completionDisabled_ || !firstSuccessor_ || !firstGroupCacheable_) {
      return;
    }
    if (groupOrder_ == GroupOrder::OldestFirst) {
      completeRange(start_, {*firstSuccessor_, 0});
    } else {
      completeRange({*firstSuccessor_ + 1, 0}, end_);
    }
  }

  void completeLastEdge() {
    if (completionDisabled_) {
      return;
    }
    if (!currentGroup_) {
      completeRange(start_, end_);
      return;
    }
    if (!currentGroupCacheable_) {
      return;
    }
    if (groupOrder_ == GroupOrder::OldestFirst) {
      completeRange(
          *currentGroup_ == *firstGroup_ ? start_
                                         : AbsoluteLocation{*currentGroup_, 0},
          end_);
    } else {
      completeRange(
          start_,
          *currentGroup_ == *firstGroup_
              ? end_
              : AbsoluteLocation{*currentGroup_ + 1, 0});
    }
  }

  void invalidate() {
    cacheable_ = false;
    completionDisabled_ = true;
    disabled_ = true;
    if (std::exchange(invalidated_, true)) {
      return;
    }
    pending_.reset();
    if (auto cache = cache_.lock()) {
      cache->clear();
    }
  }

  void markCurrentGroupUncacheable() {
    currentGroupCacheable_ = false;
    if (currentGroup_ == firstGroup_) {
      firstGroupCacheable_ = false;
    }
  }

  void finish(bool clean) {
    if (std::exchange(finished_, true)) {
      return;
    }
    pending_.reset();
    clean_ = clean;
    disabled_ |= !clean;
    completionDisabled_ |= !clean;
    cacheable_ &= clean;
    if (clean_) {
      if (rangeKnown_) {
        completeLastEdge();
      } else if (
          firstSuccessor_ && currentGroupCacheable_ && !completionDisabled_ &&
          groupOrder_ == GroupOrder::NewestFirst) {
        auto start = startKnown_ ? start_ : AbsoluteLocation{*currentGroup_, 0};
        completeRange(start, {*currentGroup_ + 1, 0});
      }
    }
    notifyFinished();
  }

  void notifyFinished() {
    if (onFinished_ && finished_ && (!clean_ || rangeKnown_)) {
      std::move(onFinished_)(clean_ && cacheable_);
    }
  }

  std::weak_ptr<MoQGroupCache> cache_;
  AbsoluteLocation start_;
  AbsoluteLocation end_;
  AbsoluteLocation endBound_{kLocationMax};
  OnFinished onFinished_;
  std::optional<Pending> pending_;
  std::optional<AbsoluteLocation> observedMin_;
  std::optional<AbsoluteLocation> observedMax_;
  std::optional<uint64_t> firstGroup_;
  std::optional<uint64_t> firstSuccessor_;
  std::optional<uint64_t> currentGroup_;
  std::optional<MoQPublishError> statusError_;
  GroupOrder groupOrder_;
  bool cacheable_{true};
  bool completionDisabled_{false};
  bool firstGroupCacheable_{true};
  bool currentGroupCacheable_{true};
  bool disabled_{false};
  bool invalidated_{false};
  bool startKnown_{false};
  bool rangeKnown_{false};
  bool finished_{false};
  bool clean_{false};
};

MoQFetchCacheWriter::MoQFetchCacheWriter(
    std::weak_ptr<MoQGroupCache> cache,
    OnFinished onFinished,
    GroupOrder groupOrder)
    : MoQFetchCacheWriter(
          std::make_shared<CacheSink>(
              std::move(cache),
              std::move(onFinished),
              groupOrder),
          groupOrder) {}

MoQFetchCacheWriter::MoQFetchCacheWriter(
    std::weak_ptr<MoQGroupCache> cache,
    AbsoluteLocation start,
    AbsoluteLocation end,
    OnFinished onFinished,
    GroupOrder groupOrder)
    : MoQFetchCacheWriter(
          std::make_shared<CacheSink>(
              std::move(cache),
              start,
              end,
              std::move(onFinished),
              groupOrder),
          groupOrder) {}

MoQFetchCacheWriter::MoQFetchCacheWriter(
    std::shared_ptr<CacheSink> sink,
    GroupOrder groupOrder)
    : StreamingObjectReceiver(StreamingObjectReceiver::FETCH, sink, groupOrder),
      sink_(std::move(sink)) {}

bool MoQFetchCacheWriter::setRange(
    AbsoluteLocation start,
    AbsoluteLocation end) {
  return sink_->setRange(start, end);
}

folly::Expected<folly::Unit, MoQPublishError> MoQFetchCacheWriter::endOfGroup(
    uint64_t groupID,
    uint64_t subgroupID,
    uint64_t objectID,
    bool finFetch) {
  return statusResult(
      StreamingObjectReceiver::endOfGroup(
          groupID, subgroupID, objectID, finFetch));
}

folly::Expected<folly::Unit, MoQPublishError>
MoQFetchCacheWriter::endOfTrackAndGroup(
    uint64_t groupID,
    uint64_t subgroupID,
    uint64_t objectID) {
  return statusResult(
      StreamingObjectReceiver::endOfTrackAndGroup(
          groupID, subgroupID, objectID));
}

folly::Expected<folly::Unit, MoQPublishError> MoQFetchCacheWriter::statusResult(
    folly::Expected<folly::Unit, MoQPublishError> result) {
  if (result.hasError()) {
    return result;
  }
  auto error = sink_->takeStatusError();
  if (error) {
    return folly::makeUnexpected(std::move(*error));
  }
  return folly::unit;
}

} // namespace moxygen

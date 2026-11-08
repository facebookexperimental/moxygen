/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/Function.h>
#include <memory>

#include "moxygen/StreamingObjectReceiver.h"
#include "moxygen/proxy/MoQGroupCache.h"

namespace moxygen {

// Writes one upstream FETCH response into a track's group cache.
class MoQFetchCacheWriter final : public StreamingObjectReceiver {
 public:
  using OnFinished = folly::Function<void(bool cacheable)>;

  MoQFetchCacheWriter(
      std::weak_ptr<MoQGroupCache> cache,
      OnFinished onFinished,
      GroupOrder groupOrder = GroupOrder::OldestFirst);
  MoQFetchCacheWriter(
      std::weak_ptr<MoQGroupCache> cache,
      AbsoluteLocation start,
      AbsoluteLocation end,
      OnFinished onFinished,
      GroupOrder groupOrder = GroupOrder::OldestFirst);

  // Supplies the actual response range once FETCH_OK arrives. Returns false
  // if earlier response data was outside the accepted range.
  bool setRange(AbsoluteLocation start, AbsoluteLocation end);

  folly::Expected<folly::Unit, MoQPublishError> endOfGroup(
      uint64_t groupID,
      uint64_t subgroupID,
      uint64_t objectID,
      bool finFetch) override;
  folly::Expected<folly::Unit, MoQPublishError> endOfTrackAndGroup(
      uint64_t groupID,
      uint64_t subgroupID,
      uint64_t objectID) override;

 private:
  class CacheSink;

  MoQFetchCacheWriter(std::shared_ptr<CacheSink> sink, GroupOrder groupOrder);
  folly::Expected<folly::Unit, MoQPublishError> statusResult(
      folly::Expected<folly::Unit, MoQPublishError> result);

  std::shared_ptr<CacheSink> sink_;
};

} // namespace moxygen

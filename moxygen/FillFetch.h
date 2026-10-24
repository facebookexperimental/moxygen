/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/Function.h>
#include "moxygen/MoQConsumers.h"
#include "moxygen/Publisher.h"

namespace moxygen {

// The FETCH that serves a fill, or nullopt if there is nothing to fill.
// requestID is the SUBSCRIBE or REQUEST_UPDATE that requested the fill. The
// fill inherits the subscription's values, except those FILL_PARAMETERS
// override.
std::optional<Fetch> makeFillFetch(
    const FullTrackName& fullTrackName,
    RequestID requestID,
    const LocationFilter& subscriptionFilter,
    const FillParameters& fill,
    std::optional<AbsoluteLocation> largest,
    uint8_t priority,
    GroupOrder groupOrder,
    const TrackRequestParameters& subscriptionParams);

using FillFetchFn = folly::Function<folly::coro::Task<Publisher::FetchResult>(
    Fetch,
    std::shared_ptr<FetchConsumer>)>;

// Serves a fill from `fetchFn`. The fill stream opens before this returns, so
// that a PUBLISH_DONE sent right after counts it.
void serveFillFromFetch(
    FillFetchFn fetchFn,
    std::shared_ptr<TrackConsumer> consumer,
    Fetch fetch);

// Serves a fill from `publisher`'s own fetch().
void serveFillFromFetch(
    std::shared_ptr<Publisher> publisher,
    std::shared_ptr<TrackConsumer> consumer,
    Fetch fetch);

} // namespace moxygen

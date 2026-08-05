/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <moxygen/media_server/MoQMediaSource.h>
#include <moxygen/relay/MoQForwarder.h>

#include <folly/coro/Task.h>

#include <memory>

namespace moxygen::media_server {

// Drains source->objects() into the forwarder, translating grouping per
// spec().mode (SubgroupPerGroup: a new subgroup on each group change;
// StreamPerObject: one stream per object) and handling BLOCKED backpressure.
// Runs on the publisher event base; one instance per track serving stack.
//
// Terminal behavior:
//  - source generator completes (a finite/ended track) ->
//  publishDone(TRACK_ENDED).
//  - the ambient cancellation token is cancelled (the stack was torn down
//    because its last subscriber left) -> unwind WITHOUT publishDone; the
//    forwarder is being dropped anyway.
folly::coro::Task<void> runPublishLoop(
    std::shared_ptr<SegmentSource> source,
    std::shared_ptr<MoQForwarder> forwarder,
    bool waitForSubscriber = false);

} // namespace moxygen::media_server

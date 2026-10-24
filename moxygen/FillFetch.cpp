/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/FillFetch.h"

#include <folly/logging/xlog.h>
#include "moxygen/MoQLocation.h"
#include "moxygen/MoQSession.h"

namespace moxygen {

std::optional<Fetch> makeFillFetch(
    const FullTrackName& fullTrackName,
    RequestID requestID,
    const LocationFilter& subscriptionFilter,
    const FillParameters& fill,
    std::optional<AbsoluteLocation> largest,
    uint8_t priority,
    GroupOrder groupOrder,
    const TrackRequestParameters& subscriptionParams) {
  if (!largest) {
    return std::nullopt;
  }
  auto range =
      toFetchRange(fill.locationFilter.value_or(subscriptionFilter), largest);
  if (isEmptyFetchRange(range)) {
    return std::nullopt;
  }
  // Authorization is the only subscription parameter valid on a FETCH.
  std::vector<Parameter> auth;
  for (const auto& param : subscriptionParams) {
    if (param.key ==
        folly::to_underlying(TrackRequestParamKey::AUTHORIZATION_TOKEN)) {
      auth.push_back(param);
    }
  }
  return Fetch(
      requestID,
      fullTrackName,
      range.start,
      range.end,
      fill.subscriberPriority.value_or(priority),
      fill.groupOrder.value_or(groupOrder),
      auth);
}

void serveFillFromFetch(
    FillFetchFn fetchFn,
    std::shared_ptr<TrackConsumer> consumer,
    Fetch fetch) {
  auto session = MoQSession::getRequestSession();
  if (!session) {
    XLOG(ERR) << "Fill requested outside a session id=" << fetch.requestID;
    return;
  }
  auto opened = consumer->beginFill(fetch.requestID);
  if (opened.hasError()) {
    XLOG(DBG1) << "Fill refused id=" << fetch.requestID
               << " err=" << opened.error().describe();
    return;
  }
  // A cancelled fill reaches the publisher as a failed write, so the returned
  // handle is dropped.
  co_withExecutor(
      session->getExecutor(),
      folly::coro::co_invoke(
          [fetchFn = std::move(fetchFn),
           fill = std::move(opened.value()),
           fetch = std::move(fetch)]() mutable -> folly::coro::Task<void> {
            auto requestID = fetch.requestID;
            auto res = co_await folly::coro::co_awaitTry(
                fetchFn(std::move(fetch), fill));
            if (res.hasException() || res->hasError()) {
              XLOG(DBG1) << "Fill fetch failed id=" << requestID;
              fill->reset(ResetStreamErrorCode::INTERNAL_ERROR);
            }
          }))
      .start();
}

void serveFillFromFetch(
    std::shared_ptr<Publisher> publisher,
    std::shared_ptr<TrackConsumer> consumer,
    Fetch fetch) {
  serveFillFromFetch(
      [publisher = std::move(publisher)](
          Fetch fetch, std::shared_ptr<FetchConsumer> fill) {
        return publisher->fetch(std::move(fetch), std::move(fill));
      },
      std::move(consumer),
      std::move(fetch));
}

} // namespace moxygen

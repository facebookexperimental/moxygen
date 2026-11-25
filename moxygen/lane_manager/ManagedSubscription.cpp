/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/lane_manager/ManagedSubscription.h"

#include "moxygen/MoQSession.h"

#include <folly/ScopeGuard.h>
#include <folly/coro/Sleep.h>
#include <folly/coro/Timeout.h>
#include <folly/coro/WithCancellation.h>
#include <folly/futures/Future.h>
#include <folly/io/async/Request.h>

#include <algorithm>
#include <tuple>
#include <utility>

namespace moxygen::lane_manager {
namespace {

using namespace std::chrono_literals;

constexpr auto kDefaultRetryDelay = 100ms;
constexpr auto kJoiningFetchWait = 5s;
const folly::RequestToken kMoQSessionToken{"moq_session"};

bool retryable(const SubscribeError& error) {
  if (error.retryInterval && error.retryInterval->count() == 0) {
    return false;
  }
  switch (error.errorCode) {
    case RequestErrorCode::TIMEOUT:
    case RequestErrorCode::GOING_AWAY:
    case RequestErrorCode::EXCESSIVE_LOAD:
    case RequestErrorCode::DOES_NOT_EXIST:
    case RequestErrorCode::REDIRECT:
      return true;
    case RequestErrorCode::INTERNAL_ERROR:
    case RequestErrorCode::UNAUTHORIZED:
    case RequestErrorCode::NOT_SUPPORTED:
    case RequestErrorCode::MALFORMED_AUTH_TOKEN:
    case RequestErrorCode::EXPIRED_AUTH_TOKEN:
    case RequestErrorCode::INVALID_RANGE:
    case RequestErrorCode::MALFORMED_TRACK:
    case RequestErrorCode::DUPLICATE_SUBSCRIPTION:
    case RequestErrorCode::UNINTERESTED:
    case RequestErrorCode::PREFIX_OVERLAP:
    case RequestErrorCode::NAMESPACE_TOO_LARGE:
    case RequestErrorCode::INVALID_JOINING_REQUEST_ID:
    case RequestErrorCode::UNSUPPORTED_EXTENSION:
    case RequestErrorCode::CANCELLED:
      return false;
  }
  return false;
}

bool retryable(PublishDoneStatusCode code) {
  switch (code) {
    case PublishDoneStatusCode::INTERNAL_ERROR:
    case PublishDoneStatusCode::SUBSCRIPTION_ENDED:
    case PublishDoneStatusCode::GOING_AWAY:
    case PublishDoneStatusCode::TOO_FAR_BEHIND:
    case PublishDoneStatusCode::EXCESSIVE_LOAD:
    case PublishDoneStatusCode::SESSION_CLOSED:
      return true;
    case PublishDoneStatusCode::UNAUTHORIZED:
    case PublishDoneStatusCode::TRACK_ENDED:
    case PublishDoneStatusCode::EXPIRED:
    case PublishDoneStatusCode::UPDATE_FAILED:
    case PublishDoneStatusCode::MALFORMED_TRACK:
      return false;
  }
  return false;
}

std::chrono::milliseconds retryDelay(const SubscribeError& error) {
  if (!error.retryInterval) {
    return kDefaultRetryDelay;
  }
  return std::max(1ms, *error.retryInterval - 1ms);
}

PublishDoneStatusCode publishDoneStatus(const SubscribeError& error) {
  switch (error.errorCode) {
    case RequestErrorCode::UNAUTHORIZED:
    case RequestErrorCode::MALFORMED_AUTH_TOKEN:
    case RequestErrorCode::EXPIRED_AUTH_TOKEN:
      return PublishDoneStatusCode::UNAUTHORIZED;
    case RequestErrorCode::MALFORMED_TRACK:
    case RequestErrorCode::INVALID_RANGE:
    case RequestErrorCode::UNSUPPORTED_EXTENSION:
      return PublishDoneStatusCode::MALFORMED_TRACK;
    case RequestErrorCode::INTERNAL_ERROR:
    case RequestErrorCode::TIMEOUT:
    case RequestErrorCode::NOT_SUPPORTED:
    case RequestErrorCode::GOING_AWAY:
    case RequestErrorCode::EXCESSIVE_LOAD:
    case RequestErrorCode::DOES_NOT_EXIST:
    case RequestErrorCode::REDIRECT:
    case RequestErrorCode::DUPLICATE_SUBSCRIPTION:
    case RequestErrorCode::UNINTERESTED:
    case RequestErrorCode::PREFIX_OVERLAP:
    case RequestErrorCode::NAMESPACE_TOO_LARGE:
    case RequestErrorCode::INVALID_JOINING_REQUEST_ID:
    case RequestErrorCode::CANCELLED:
      return PublishDoneStatusCode::INTERNAL_ERROR;
  }
  return PublishDoneStatusCode::INTERNAL_ERROR;
}

} // namespace

ManagedSubscription::ManagedSubscription(
    std::shared_ptr<Publisher> upstream,
    SubscribeRequest request,
    std::shared_ptr<TrackConsumer> downstreamLaneConsumer,
    MoQExecutor& executor)
    : upstream_(std::move(upstream)),
      requestTemplate_(std::move(request)),
      downstreamLaneConsumer_(std::move(downstreamLaneConsumer)),
      executor_(executor),
      requestSession_(MoQSession::getRequestSession()),
      stateChanged_(std::make_shared<folly::coro::SharedPromise<void>>()),
      attemptRegistered_(std::make_shared<folly::coro::SharedPromise<bool>>()) {
}

ManagedSubscription::~ManagedSubscription() {
  unsubscribe();
}

folly::coro::Task<ManagedSubscription::SubscribeResult>
ManagedSubscription::subscribe() {
  if (state_ == State::Idle) {
    startAttempts(0ms);
  }
  while (true) {
    switch (state_) {
      case State::Active:
        co_return subscription_->subscribeOk();
      case State::Ended:
        if (attemptException_) {
          attemptException_.throw_exception();
        }
        co_return folly::makeUnexpected(*lastError_);
      case State::Stopped:
        co_return folly::makeUnexpected(
            SubscribeError{
                requestTemplate_.requestID,
                RequestErrorCode::CANCELLED,
                "stopped"});
      case State::Idle:
      case State::Pending:
      case State::RetryWaiting: {
        auto changed = stateChanged_;
        co_await changed->getFuture();
        break;
      }
    }
  }
}

folly::coro::Task<Publisher::FetchResult>
ManagedSubscription::subscribeWithJoiningFetch(
    RequestID downstreamRequestID,
    std::shared_ptr<FetchConsumer> consumer,
    FetchType fetchType) {
  if (fetchType != FetchType::RELATIVE_JOINING) {
    co_return folly::makeUnexpected(
        FetchError{
            downstreamRequestID, FetchErrorCode::NOT_SUPPORTED, "fetch type"});
  }
  Fetch fetch(
      downstreamRequestID,
      requestTemplate_.requestID,
      0,
      fetchType,
      requestTemplate_.priority);
  co_return co_await fetchWhenReady(
      std::move(fetch), std::move(consumer), true);
}

folly::coro::Task<Publisher::FetchResult>
ManagedSubscription::subscribeWithJoiningFetch(
    Fetch fetch,
    std::shared_ptr<FetchConsumer> consumer) {
  return fetchWhenReady(std::move(fetch), std::move(consumer), false);
}

folly::coro::Task<Publisher::FetchResult> ManagedSubscription::fetchWhenReady(
    Fetch fetch,
    std::shared_ptr<FetchConsumer> consumer,
    bool useSubscriptionGroupOrder) {
  auto self = shared_from_this();
  if (!std::holds_alternative<JoiningFetch>(fetch.args) ||
      std::get<JoiningFetch>(fetch.args).fetchType !=
          FetchType::RELATIVE_JOINING) {
    co_return folly::makeUnexpected(
        FetchError{
            fetch.requestID, FetchErrorCode::NOT_SUPPORTED, "fetch type"});
  }
  auto fetchable = co_await folly::coro::co_awaitTry(
      folly::coro::timeout(self->waitUntilFetchable(), kJoiningFetchWait));
  if (fetchable.hasException<folly::FutureTimeout>()) {
    co_return folly::makeUnexpected(
        FetchError{
            fetch.requestID,
            FetchErrorCode::TIMEOUT,
            "subscription unavailable"});
  }
  if (fetchable.hasException()) {
    co_yield folly::coro::co_error(std::move(fetchable).exception());
  }
  if (!fetchable->has_value()) {
    if (state_ == State::Ended && lastError_) {
      auto error = *lastError_;
      error.requestID = fetch.requestID;
      co_return folly::makeUnexpected(std::move(error));
    }
    co_return folly::makeUnexpected(
        FetchError{fetch.requestID, FetchErrorCode::CANCELLED, "stopped"});
  }
  if (useSubscriptionGroupOrder) {
    fetch.groupOrder = **fetchable;
  }
  co_return co_await self->issueJoiningFetch(
      std::move(fetch), std::move(consumer));
}

folly::coro::Task<std::optional<GroupOrder>>
ManagedSubscription::waitUntilFetchable() {
  if (state_ == State::Idle) {
    startAttempts(0ms);
  }
  while (true) {
    switch (state_) {
      case State::Active:
        co_return subscription_->subscribeOk().groupOrder;
      case State::Ended:
        if (attemptException_) {
          attemptException_.throw_exception();
        }
        co_return std::nullopt;
      case State::Stopped:
        co_return std::nullopt;
      case State::Pending: {
        auto registration = attemptRegistered_;
        const bool registered = co_await registration->getFuture();
        if (registered) {
          co_await folly::coro::co_reschedule_on_current_executor;
          if (registration == attemptRegistered_ &&
              (state_ == State::Pending || state_ == State::Active)) {
            co_return state_ == State::Active
                ? subscription_->subscribeOk().groupOrder
                : requestTemplate_.groupOrder.value_or(GroupOrder::OldestFirst);
          }
        }
        break;
      }
      case State::Idle:
      case State::RetryWaiting: {
        auto changed = stateChanged_;
        co_await changed->getFuture();
        break;
      }
    }
  }
}

folly::coro::Task<Publisher::FetchResult>
ManagedSubscription::issueJoiningFetch(
    Fetch fetch,
    std::shared_ptr<FetchConsumer> consumer) {
  const auto downstreamRequestID = fetch.requestID;
  std::get<JoiningFetch>(fetch.args).joiningRequestID =
      requestTemplate_.requestID;
  fetch.fullTrackName = requestTemplate_.fullTrackName;
  auto requestSession = requestSession_.lock();
  if (!requestSession) {
    co_return folly::makeUnexpected(
        FetchError{downstreamRequestID, FetchErrorCode::CANCELLED, "stopped"});
  }
  folly::RequestContextScopeGuard context;
  folly::RequestContext::get()->setContextData(
      kMoQSessionToken,
      std::make_unique<MoQSession::MoQSessionRequestData>(
          std::move(requestSession)));
  co_return co_await upstream_->fetch(std::move(fetch), std::move(consumer));
}

void ManagedSubscription::unsubscribe() {
  if (state_ == State::Stopped) {
    return;
  }
  setState(State::Stopped);
  stopSource_.requestCancellation();
  if (auto subscription = std::exchange(subscription_, nullptr)) {
    subscription->unsubscribe();
  }
}

void ManagedSubscription::setState(State state) {
  if (state_ == state) {
    return;
  }
  state_ = state;
  if ((state == State::Ended || state == State::Stopped) &&
      !attemptRegistered_->isFulfilled()) {
    attemptRegistered_->setValue(false);
  }
  auto changed = std::exchange(
      stateChanged_, std::make_shared<folly::coro::SharedPromise<void>>());
  changed->setValue();
}

void ManagedSubscription::startAttempts(std::chrono::milliseconds delay) {
  if (attemptInFlight_ || state_ == State::Ended || state_ == State::Stopped) {
    return;
  }
  attemptInFlight_ = true;
  folly::coro::co_withExecutor(
      &executor_, runAttempts(shared_from_this(), delay))
      .start();
}

folly::coro::Task<void> ManagedSubscription::runAttempts(
    std::shared_ptr<ManagedSubscription> self,
    std::chrono::milliseconds delay) {
  auto finished = folly::makeGuard([&] { self->attemptInFlight_ = false; });
  const auto token = self->stopSource_.getToken();
  while (self->state_ != State::Ended && self->state_ != State::Stopped) {
    if (delay.count() > 0) {
      self->setState(State::RetryWaiting);
      auto waited = co_await folly::coro::co_awaitTry(
          folly::coro::co_withCancellation(token, folly::coro::sleep(delay)));
      if (waited.hasException()) {
        co_return;
      }
    }
    if (self->state_ == State::Stopped) {
      co_return;
    }

    auto registration = self->attemptRegistered_ =
        std::make_shared<folly::coro::SharedPromise<bool>>();
    self->setState(State::Pending);
    auto requestSession = self->requestSession_.lock();
    if (!requestSession) {
      self->setState(State::Stopped);
      co_return;
    }
    auto request = self->requestTemplate_;
    if (self->wasActive_) {
      request.filter = LocationFilter::relativeStart(0);
    }
    folly::Try<Publisher::SubscribeResult> attempted;
    {
      folly::RequestContextScopeGuard context;
      folly::RequestContext::get()->setContextData(
          kMoQSessionToken,
          std::make_unique<MoQSession::MoQSessionRequestData>(
              std::move(requestSession)));
      attempted = co_await folly::coro::co_awaitTry(
          folly::coro::co_withCancellation(
              token,
              self->upstream_->subscribe(
                  std::move(request), std::shared_ptr<TrackConsumer>(self))));
    }
    if (!registration->isFulfilled()) {
      registration->setValue(false);
    }
    if (self->state_ == State::Stopped) {
      if (attempted.hasValue() && attempted->hasValue()) {
        attempted->value()->unsubscribe();
      }
      co_return;
    }
    if (self->state_ == State::Ended || self->state_ == State::RetryWaiting) {
      if (attempted.hasValue() && attempted->hasValue()) {
        attempted->value()->unsubscribe();
      }
      if (self->state_ == State::Ended) {
        co_return;
      }
      delay = kDefaultRetryDelay;
      continue;
    }
    if (attempted.hasException<folly::FutureTimeout>()) {
      self->lastError_ = SubscribeError{
          self->requestTemplate_.requestID,
          RequestErrorCode::TIMEOUT,
          "subscription timeout",
          1ms};
      delay = retryDelay(*self->lastError_);
      continue;
    }
    if (attempted.hasException()) {
      self->attemptException_ = std::move(attempted).exception();
      self->lastError_ = SubscribeError{
          self->requestTemplate_.requestID,
          RequestErrorCode::INTERNAL_ERROR,
          "subscription exception",
          0ms};
      self->setState(State::Ended);
      std::ignore = self->downstreamLaneConsumer_->publishDone(
          PublishDone{
              self->requestTemplate_.requestID,
              PublishDoneStatusCode::INTERNAL_ERROR,
              0,
              "subscription exception"});
      co_return;
    }

    auto result = std::move(attempted).value();
    if (result.hasValue()) {
      self->subscription_ = std::move(result.value());
      self->lastError_.reset();
      self->attemptException_ = {};
      self->wasActive_ = true;
      self->setState(State::Active);
      co_return;
    }
    self->lastError_ = std::move(result.error());
    if (!retryable(*self->lastError_)) {
      self->setState(State::Ended);
      std::ignore = self->downstreamLaneConsumer_->publishDone(
          PublishDone{
              self->requestTemplate_.requestID,
              publishDoneStatus(*self->lastError_),
              0,
              self->lastError_->reasonPhrase});
      co_return;
    }
    delay = retryDelay(*self->lastError_);
  }
}

folly::Expected<folly::Unit, MoQPublishError>
ManagedSubscription::setTrackAlias(TrackAlias alias) {
  auto result = downstreamLaneConsumer_->setTrackAlias(alias);
  if (result.hasValue() && !attemptRegistered_->isFulfilled()) {
    attemptRegistered_->setValue(true);
  }
  return result;
}

folly::Expected<std::shared_ptr<SubgroupConsumer>, MoQPublishError>
ManagedSubscription::beginSubgroup(
    uint64_t groupID,
    uint64_t subgroupID,
    Priority priority,
    BeginSubgroupOptions options) {
  return downstreamLaneConsumer_->beginSubgroup(
      groupID, subgroupID, priority, options);
}

folly::Expected<folly::SemiFuture<folly::Unit>, MoQPublishError>
ManagedSubscription::awaitStreamCredit() {
  return downstreamLaneConsumer_->awaitStreamCredit();
}

folly::Expected<folly::Unit, MoQPublishError> ManagedSubscription::objectStream(
    const ObjectHeader& header,
    Payload payload,
    bool lastInGroup) {
  return downstreamLaneConsumer_->objectStream(
      header, std::move(payload), lastInGroup);
}

folly::Expected<folly::Unit, MoQPublishError> ManagedSubscription::datagram(
    const ObjectHeader& header,
    Payload payload,
    bool lastInGroup) {
  return downstreamLaneConsumer_->datagram(
      header, std::move(payload), lastInGroup);
}

folly::Expected<folly::Unit, MoQPublishError> ManagedSubscription::publishDone(
    PublishDone pubDone) {
  auto self = shared_from_this();
  if (state_ != State::Active && state_ != State::Pending) {
    return folly::unit;
  }
  [[maybe_unused]] auto subscription = std::exchange(subscription_, nullptr);
  if (retryable(pubDone.statusCode)) {
    setState(State::RetryWaiting);
    auto result = downstreamLaneConsumer_->publishDone(std::move(pubDone));
    startAttempts(0ms);
    return result;
  }
  lastError_ = SubscribeError{
      requestTemplate_.requestID,
      RequestErrorCode::DOES_NOT_EXIST,
      pubDone.reasonPhrase,
      0ms};
  setState(State::Ended);
  auto result = downstreamLaneConsumer_->publishDone(std::move(pubDone));
  return result;
}

void ManagedSubscription::setDeliveryCallback(
    std::shared_ptr<DeliveryCallback> callback) {
  downstreamLaneConsumer_->setDeliveryCallback(std::move(callback));
}

} // namespace moxygen::lane_manager

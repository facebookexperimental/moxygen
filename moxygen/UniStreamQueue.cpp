/*
 * Copyright (c) OpenMOQ contributors.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/UniStreamQueue.h"

#include <folly/logging/xlog.h>

namespace moxygen {

std::optional<uint64_t> UniStreamQueue::request(
    quic::PriorityQueue::Priority priority,
    Grant grant) {
  if (!wt_) {
    return std::nullopt;
  }
  auto id = nextId_++;
  requests_.emplace(id, std::move(grant));
  order_.insertOrUpdate(
      quic::PriorityQueue::Identifier::fromStreamID(id), priority);
  waitForCredit();
  return id;
}

void UniStreamQueue::cancel(uint64_t id) {
  if (requests_.erase(id)) {
    order_.erase(quic::PriorityQueue::Identifier::fromStreamID(id));
  }
}

void UniStreamQueue::close() {
  wt_ = nullptr;
  awaitCredit_ = nullptr;
  failAll(token_);
}

void UniStreamQueue::failAll(folly::CancellationToken token) {
  while (!token.isCancellationRequested() && !requests_.empty()) {
    grantFirst(nullptr);
  }
}

void UniStreamQueue::waitForCredit() {
  if (waiting_ || requests_.empty() || !wt_) {
    return;
  }
  waiting_ = true;
  auto credit = awaitCredit_();
  const bool creditWasReady = credit.isReady();
  // Granting from the executor keeps grants out of request()'s caller.
  std::move(credit).via(exec_).thenTry(
      [this, token = token_, creditWasReady](folly::Try<folly::Unit>&& result) {
        if (!token.isCancellationRequested()) {
          onCredit(std::move(result), creditWasReady);
        }
      });
}

void UniStreamQueue::onCredit(
    folly::Try<folly::Unit> credit,
    bool creditWasReady) {
  if (!wt_) {
    waiting_ = false;
    return;
  }
  auto token = token_;
  if (credit.hasException()) {
    XLOG(DBG4) << "Uni stream credit wait failed: " << credit.exception();
    waiting_ = false;
    failAll(token);
    return;
  }
  // Still waiting while granting, so a request from inside a grant joins this
  // pass. A grant can also close the queue.
  while (!token.isCancellationRequested() && !requests_.empty() && wt_) {
    auto stream = wt_->createUniStream();
    if (stream) {
      grantFirst(stream.value());
      continue;
    }
    if (creditWasReady) {
      // The transport reported credit that it can't use. Waiting would spin.
      XLOG(ERR) << "Uni stream creation failed with credit available";
      grantFirst(nullptr);
    }
    break;
  }
  if (!token.isCancellationRequested()) {
    waiting_ = false;
    waitForCredit();
  }
}

void UniStreamQueue::grantFirst(
    proxygen::WebTransport::StreamWriteHandle* writeHandle) {
  auto id = order_.peekNextScheduledID();
  order_.erase(id);
  auto it = requests_.find(id.asStreamID());
  auto grant = std::move(it->second);
  requests_.erase(it);
  grant(writeHandle);
}

} // namespace moxygen

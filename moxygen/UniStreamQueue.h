/*
 * Copyright (c) OpenMOQ contributors.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/CancellationToken.h>
#include <folly/Executor.h>
#include <folly/Function.h>
#include <folly/container/F14Map.h>
#include <folly/futures/Future.h>
#include <proxygen/lib/http/webtransport/WebTransport.h>
#include <quic/priority/HTTPPriorityQueue.h>

#include <optional>
#include <utility>

namespace moxygen {

// Hands locally-initiated uni streams to waiting requests as peer credit
// arrives, highest priority first and FIFO within a priority.
class UniStreamQueue {
 public:
  // A nullptr write handle means no stream is coming.
  using Grant =
      folly::Function<void(proxygen::WebTransport::StreamWriteHandle*)>;
  // Must share the owner's wait. Transports allow one credit waiter.
  using AwaitCredit = folly::Function<folly::SemiFuture<folly::Unit>()>;

  // The owner must cancel token before it destroys the queue.
  UniStreamQueue(
      proxygen::WebTransport* wt,
      folly::Executor* exec,
      AwaitCredit awaitCredit,
      folly::CancellationToken token)
      : wt_(wt),
        exec_(exec),
        awaitCredit_(std::move(awaitCredit)),
        token_(std::move(token)) {}
  // Credit callbacks capture this.
  UniStreamQueue(UniStreamQueue&&) = delete;
  UniStreamQueue& operator=(UniStreamQueue&&) = delete;

  // std::nullopt once closed.
  std::optional<uint64_t> request(
      quic::PriorityQueue::Priority priority,
      Grant grant);
  // The grant is dropped without being called.
  void cancel(uint64_t id);
  // Fails every pending request.
  void close();

  size_t size() const {
    return requests_.size();
  }

 private:
  void waitForCredit();
  void onCredit(folly::Try<folly::Unit> credit, bool creditWasReady);
  void grantFirst(proxygen::WebTransport::StreamWriteHandle* writeHandle);
  // A grant can destroy this queue. Loops check a token copy before they touch
  // members again.
  void failAll(folly::CancellationToken token);

  proxygen::WebTransport* wt_;
  folly::Executor* exec_;
  AwaitCredit awaitCredit_;
  folly::F14FastMap<uint64_t, Grant> requests_;
  // Ties go to the lower ID, so requests of equal priority are FIFO.
  quic::HTTPPriorityQueue order_;
  uint64_t nextId_{0};
  bool waiting_{false};
  folly::CancellationToken token_;
};

} // namespace moxygen

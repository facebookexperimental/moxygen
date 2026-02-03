/*
 *  Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 *  This source code is licensed under the MIT license found in the LICENSE
 *  file in the root directory of this source tree.
 *
 */

#pragma once

#include <folly/coro/UnboundedQueue.h>
#include <moxygen/ObjectReceiver.h>

namespace moxygen {

class QueueCallback : public ObjectReceiverCallback {
 public:
  struct Object {
    moxygen::ObjectHeader header;
    moxygen::Payload payload;
  };
  folly::coro::UnboundedQueue<folly::Expected<Object, folly::Unit>> queue;

  FlowControlState onObject(
      std::optional<TrackAlias> /* trackAlias */,
      const ObjectHeader& objHeader,
      Payload payload) override {
    queue.enqueue(Object({objHeader, std::move(payload)}));
    return FlowControlState::UNBLOCKED;
  }
  void onObjectStatus(
      std::optional<TrackAlias> /* trackAlias */,
      const ObjectHeader& hdr) override {
    queue.enqueue(Object({hdr, nullptr}));
  }
  void onEndOfStream() override {
    queue.enqueue(folly::makeUnexpected(folly::unit));
  }
  void onError(ResetStreamErrorCode) override {
    queue.enqueue(folly::makeUnexpected(folly::unit));
  }
  void onPublishDone(PublishDone) override {
    queue.enqueue(folly::makeUnexpected(folly::unit));
  }
};
} // namespace moxygen

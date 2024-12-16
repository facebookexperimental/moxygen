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

  FlowControlState onObject(const ObjectHeader& objHeader, Payload payload)
      override {
    queue.enqueue(Object({objHeader, std::move(payload)}));
    return FlowControlState::UNBLOCKED;
  }
  void onObjectStatus(const ObjectHeader& hdr) override {
    queue.enqueue(Object({hdr, nullptr}));
  }
  void onEndOfStream() override {
    queue.enqueue(folly::makeUnexpected(folly::unit));
  }
  void onError(ResetStreamErrorCode) override {
    queue.enqueue(folly::makeUnexpected(folly::unit));
  }
  void onSubscribeDone(SubscribeDone) override {
    queue.enqueue(folly::makeUnexpected(folly::unit));
  }
};
} // namespace moxygen

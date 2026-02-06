/*
 *  Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 *  This source code is licensed under the MIT license found in the LICENSE
 *  file in the root directory of this source tree.
 *
 */

#pragma once

#include <proxygen/lib/http/webtransport/WebTransport.h>
#include <moxygen/MoQPublishError.h>
#include <moxygen/MoQTypes.h>

namespace moxygen {

// Callback interface for delivery notifications
class DeliveryCallback {
 public:
  virtual ~DeliveryCallback() = default;

  // Called when an object has been delivered successfully
  virtual void onDelivered(
      const std::optional<TrackAlias>& maybeTrackAlias,
      uint64_t groupId,
      uint64_t subgroupId,
      uint64_t objectId) = 0;

  // Called when we, for instance, reset the stream, and can't
  // guarantee delivery of the object.
  virtual void onDeliveryCancelled(
      const std::optional<TrackAlias>& maybeTrackAlias,
      uint64_t groupId,
      uint64_t subgroupId,
      uint64_t objectId) = 0;
};

// MoQ Consumers
//
// These interfaces are used both for writing and reading track data.
//
// A publisher will acquire a consumer object from a session and invoke the
// methods.  The implementation "consumes" the data by serializing it on the
// wire.
//
// A subscriber will provide a consumer object to a producer (the session or
// another producer) and that producer will invoke the methods as data becomes
// available.

// Interface for Publishing and Receiving objects on a subgroup
class SubgroupConsumer {
 public:
  virtual ~SubgroupConsumer() = default;

  // SubgroupConsumer enforces API semantics.
  //
  // It's an error to deliver any object or status if the object ID isn't
  // strictly larger than the last delivered one on this subgroup.
  //
  // When using beginObject/objectPayload to deliver a streaming object, the
  // total number of bytes delivered must equal the value in the length in
  // beginObject.
  //
  // It's an error to begin delivering any object/status or close the
  // stream in the middle of a streaming object.
  //
  // If any method returns a MoQPublishError, the SubgroupConsumer is
  // implicitly reset and no further API calls should be made on it.
  // Calling reset() after an error is a no-op.

  // Deliver the next object on this subgroup.
  virtual folly::Expected<folly::Unit, MoQPublishError> object(
      uint64_t objectID,
      Payload payload,
      Extensions extensions = noExtensions(),
      bool finSubgroup = false) = 0;

  // Advance the reliable offset of the subgroup stream to the
  // current offset.
  virtual void checkpoint() {}

  // Begin delivering the next object in this subgroup.
  virtual folly::Expected<folly::Unit, MoQPublishError> beginObject(
      uint64_t objectID,
      uint64_t length,
      Payload initialPayload,
      Extensions extensions = noExtensions()) = 0;

  // Deliver the next chunk of data in the current object.  The return value is
  // IN_PROGRESS if the object is not yet complete, DONE if the payload exactly
  // matched the remaining expected size.
  virtual folly::Expected<ObjectPublishStatus, MoQPublishError> objectPayload(
      Payload payload,
      bool finSubgroup = false) = 0;

  // Deliver Object Status=EndOfGroup for the given object ID.  This implies
  // endOfSubgroup.
  virtual folly::Expected<folly::Unit, MoQPublishError> endOfGroup(
      uint64_t endOfGroupObjectID) = 0;

  // Deliver Object Status=EndOfTrackAndGroup for the given object ID.  This
  // implies endOfSubgroup.
  virtual folly::Expected<folly::Unit, MoQPublishError> endOfTrackAndGroup(
      uint64_t endOfTrackObjectID) = 0;

  // Inform the consumer the subgroup is complete.  If the consumer is writing,
  // this closes the underlying transport stream.  This can only be called if
  // the publisher knows the entire subgroup has been delivered.
  virtual folly::Expected<folly::Unit, MoQPublishError> endOfSubgroup() = 0;

  // Inform the consumer that the subgroup terminates with an error.  If the
  // consumer is writing, this resets the transport stream with the given error
  // code.  The stream will be reliably delivered up to the last checkpoint().
  virtual void reset(ResetStreamErrorCode error) = 0;

  // This function will never be called by the library. It is meant to only be
  // used by the application. The publisher can use this signal if it wants to
  // pace data according to the rate at which the consumer is consuming it. If
  // the publisher ignores this signal (which is perfectly valid), it may get
  // a TOO_FAR_BEHIND if the client is unable to keep up.
  virtual folly::Expected<folly::SemiFuture<uint64_t>, MoQPublishError>
  awaitReadyToConsume() {
    return folly::makeSemiFuture<uint64_t>(0);
  }
};

// Interface for Publishing and Receiving Subscriptions
//
// Note that for now, both the stream interface and datagram interface coexist
// even though the specification disallows mixing and matching those right now.
//
// A note on subscription flow control:
//
// For consumers that are writing, publishing data on a track consumes resources
// which are freed as the subscribers receive and acknowledge data.  The
// underlying session will terminate a subscription that exceeds publisher side
// resource limits.
class TrackConsumer {
 public:
  virtual ~TrackConsumer() = default;

  // Set the Track Alias for this track.  This is called by publishers in
  // response to SUBSCRIBE requests.  This can fail if the alias is already
  // in use or has already been set.
  virtual folly::Expected<folly::Unit, MoQPublishError> setTrackAlias(
      TrackAlias alias) = 0;

  // Begin delivering a new subgroup in the specified group.  If the consumer is
  // writing, this Can fail with MoQPublishError::BLOCKED when out of stream
  // credit.  containsLastInGroup indicates this subgroup contains the last
  // object in the group.
  virtual folly::Expected<std::shared_ptr<SubgroupConsumer>, MoQPublishError>
  beginSubgroup(
      uint64_t groupID,
      uint64_t subgroupID,
      Priority priority,
      bool containsLastInGroup = false) = 0;

  // Wait for additional stream credit.
  virtual folly::Expected<folly::SemiFuture<folly::Unit>, MoQPublishError>
  awaitStreamCredit() = 0;

  // Deliver a single-object or object status subgroup.  header.length must
  // equal payload length, or be 0 for non-NORMAL status.  Can fail with
  // MoQPublishError::BLOCKED when out of stream credit.  lastInGroup indicates
  // the object is the last in the group.
  virtual folly::Expected<folly::Unit, MoQPublishError> objectStream(
      const ObjectHeader& header,
      Payload payload,
      bool lastInGroup = false) = 0;

  // Deliver a datagram in this track.  This can be dropped by the sender or
  // receiver if resources are low.  lastInGroup indicates the object is the
  // last in the group.
  virtual folly::Expected<folly::Unit, MoQPublishError> datagram(
      const ObjectHeader& header,
      Payload payload,
      bool lastInGroup = false) = 0;

  // Inform the consumer that the publisher will not open any new subgroups or
  // send any new datagrams for this track.
  virtual folly::Expected<folly::Unit, MoQPublishError> publishDone(
      PublishDone pubDone) = 0;

  // Set a callback to be notified when objects are delivered.
  virtual void setDeliveryCallback(std::shared_ptr<DeliveryCallback> callback) {
    // Default implementation is a no-op. This is only implemented by the
    // library-provided implementation of TrackConsumer.
  }
};

// Interface for Publishing and Receiving objects for a Fetch
//
// A note on Fetch flow control:
//
// Fetches are not intended to be consumed in real-time and can apply
// backpressure to the publisher.  The API is similar to write(2), and a
// returned MoQPublishError with code=BLOCKED signals the publisher to stop
// invoking methods.
//
// Publishers can use awaitReadyToConsume to determine when it is ok to resume.
//
// When used as a read interface, the application can return BLOCKED to initiate
// backpressure, but the library may still have some already read data that
// will be parsed and additional APIs may be invoked.
//
class FetchConsumer {
 public:
  virtual ~FetchConsumer() = default;

  // FetchConsumer enforces API semantics.
  //
  // It’s an error to deliver any object or status if the object ID isn’t
  // strictly larger than the last delivered one on this group.  It's an
  // error to deliver a group smaller than the previously delivered group.
  //
  // When using beginObject/objectPayload to deliver a streaming object,
  // the total number of bytes delivered must equal the value in the length
  // in beginObject.
  //
  // It's an error to begin delivering any object or status, or close in the
  // middle of a streaming object.

  // Deliver the next object in this FETCH response.
  virtual folly::Expected<folly::Unit, MoQPublishError> object(
      uint64_t groupID,
      uint64_t subgroupID,
      uint64_t objectID,
      Payload payload,
      Extensions extensions = noExtensions(),
      bool finFetch = false) = 0;

  // Advance the reliable offset of the fetch stream to the current offset.
  virtual void checkpoint() {}

  // Begin delivering the next object in this subgroup.
  virtual folly::Expected<folly::Unit, MoQPublishError> beginObject(
      uint64_t groupID,
      uint64_t subgroupID,
      uint64_t objectID,
      uint64_t length,
      Payload initialPayload,
      Extensions extensions = noExtensions()) = 0;

  virtual folly::Expected<ObjectPublishStatus, MoQPublishError> objectPayload(
      Payload payload,
      bool finSubgroup = false) = 0;

  // Deliver Object Status=EndOfGroup for the given object ID.
  virtual folly::Expected<folly::Unit, MoQPublishError> endOfGroup(
      uint64_t groupID,
      uint64_t subgroupID,
      uint64_t objectID,
      bool finFetch = false) = 0;

  // Deliver Object Status=EndOfTrackAndGroup for the given object ID.  This
  // implies endOfFetch.
  virtual folly::Expected<folly::Unit, MoQPublishError> endOfTrackAndGroup(
      uint64_t groupID,
      uint64_t subgroupID,
      uint64_t objectID) = 0;

  // Inform the consumer the fetch is complete.  If the consumer is writing,
  // this closes the underlying transport stream.  This can only be called if
  // the publisher knows the entire fetch has been delivered.
  virtual folly::Expected<folly::Unit, MoQPublishError> endOfFetch() = 0;

  // Inform the consumer that the fetch terminates with an error.  If the
  // consumer is writing, this resets the transport stream with the given error
  // code.  The stream will be reliably delivered up to the last checkpoint().
  virtual void reset(ResetStreamErrorCode error) = 0;

  // Wait for the fetch to become writable
  virtual folly::Expected<folly::SemiFuture<uint64_t>, MoQPublishError>
  awaitReadyToConsume() = 0;
};

} // namespace moxygen

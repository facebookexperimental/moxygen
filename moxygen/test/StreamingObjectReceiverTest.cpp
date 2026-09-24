/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <folly/portability/GTest.h>
#include <moxygen/StreamingObjectReceiver.h>

#include <functional>

using namespace moxygen;

namespace {

Payload makePayload(const std::string& value) {
  return folly::IOBuf::copyBuffer(value);
}

struct PayloadChunk {
  std::string value;
  StreamingObjectPayloadMetadata metadata;
};

class RecordingPayloadConsumer : public StreamingObjectPayloadConsumer {
 public:
  folly::Expected<folly::Unit, MoQPublishError> onPayload(
      Payload payload,
      StreamingObjectPayloadMetadata metadata) override {
    if (events) {
      events->push_back(
          metadata.endOfObject ? name + ":payload-final" : name + ":payload");
    }
    chunks.push_back(
        PayloadChunk{
            payload ? payload->moveToFbString().toStdString() : std::string(),
            std::move(metadata)});
    auto action = std::move(onPayloadAction);
    if (action) {
      action();
    }
    if (payloadErrorCode) {
      return folly::makeUnexpected(
          MoQPublishError(*payloadErrorCode, "payload rejected"));
    }
    return folly::unit;
  }

  void onError(ResetStreamErrorCode error) override {
    errors.push_back(error);
  }

  std::string name;
  std::vector<std::string>* events{nullptr};
  std::function<void()> onPayloadAction;
  std::optional<MoQPublishError::Code> payloadErrorCode;
  std::vector<PayloadChunk> chunks;
  std::vector<ResetStreamErrorCode> errors;
};

struct RecordedSubgroupError {
  StreamingSubgroupContext context;
  ResetStreamErrorCode error;
};

class RecordingReceiverCallback : public StreamingObjectReceiverCallback {
 public:
  folly::
      Expected<std::shared_ptr<StreamingObjectPayloadConsumer>, MoQPublishError>
      onObjectBegin(StreamingObjectContext context) override {
    contexts.push_back(std::move(context));
    if (nextObjectBeginError) {
      return folly::makeUnexpected(
          MoQPublishError(*nextObjectBeginError, "object rejected"));
    }
    auto consumer = std::make_shared<RecordingPayloadConsumer>();
    consumer->name = "object" + std::to_string(consumers.size());
    consumer->events = &events;
    consumer->onPayloadAction = std::move(nextOnPayloadAction);
    consumer->payloadErrorCode = nextPayloadError;
    consumers.push_back(consumer);
    events.push_back(consumer->name + ":begin");
    auto action = std::move(onObjectBeginAction);
    if (action) {
      action();
    }
    return consumer;
  }

  void onObjectStatus(StreamingObjectContext context) override {
    statuses.push_back(std::move(context));
    auto action = std::move(onObjectStatusAction);
    if (action) {
      action();
    }
  }

  void onEndOfStream(StreamingSubgroupContext context) override {
    endOfStreams.push_back(std::move(context));
    auto action = std::move(onEndOfStreamAction);
    if (action) {
      action();
    }
  }

  void onError(StreamingSubgroupContext context, ResetStreamErrorCode error)
      override {
    errors.push_back(RecordedSubgroupError{std::move(context), error});
  }

  void onPublishDone(PublishDone /*done*/) override {
    ++publishDoneCount;
    auto action = std::move(onPublishDoneAction);
    if (action) {
      action();
    }
  }

  void onAllDataReceived() override {
    ++allDataReceivedCount;
  }

  void onUnknownRange(StreamingUnknownRange range) override {
    unknownRanges.push_back(std::move(range));
  }

  std::optional<MoQPublishError::Code> nextObjectBeginError;
  std::optional<MoQPublishError::Code> nextPayloadError;
  std::function<void()> onObjectBeginAction;
  std::function<void()> onObjectStatusAction;
  std::function<void()> nextOnPayloadAction;
  std::function<void()> onPublishDoneAction;
  std::function<void()> onEndOfStreamAction;
  std::vector<std::string> events;
  std::vector<StreamingObjectContext> contexts;
  std::vector<std::shared_ptr<RecordingPayloadConsumer>> consumers;
  std::vector<StreamingObjectContext> statuses;
  std::vector<StreamingSubgroupContext> endOfStreams;
  std::vector<RecordedSubgroupError> errors;
  std::vector<StreamingUnknownRange> unknownRanges;
  size_t publishDoneCount{0};
  size_t allDataReceivedCount{0};
};

std::shared_ptr<SubgroupConsumer> beginSubgroup(
    const std::shared_ptr<StreamingObjectReceiver>& receiver,
    uint64_t group,
    uint64_t subgroup,
    BeginSubgroupOptions options = {}) {
  auto result = receiver->beginSubgroup(group, subgroup, 7, options);
  EXPECT_TRUE(result.hasValue());
  return result.hasValue() ? *result : nullptr;
}

using FetchTerminal =
    std::function<folly::Expected<folly::Unit, MoQPublishError>(
        const std::shared_ptr<StreamingObjectReceiver>&)>;

void expectMalformedFetchTerminal(const FetchTerminal& terminal) {
  auto callback = std::make_shared<RecordingReceiverCallback>();
  auto receiver = std::make_shared<StreamingObjectReceiver>(
      StreamingObjectReceiver::FETCH, callback);
  ASSERT_TRUE(
      receiver->beginObject(1, 2, 3, 4, makePayload("a"), {}).hasValue());
  ASSERT_EQ(callback->consumers.size(), 1);
  auto payloadConsumer = callback->consumers.front();

  auto result = terminal(receiver);

  ASSERT_TRUE(result.hasError());
  EXPECT_EQ(result.error().code, MoQPublishError::API_ERROR);
  ASSERT_EQ(payloadConsumer->errors.size(), 1);
  EXPECT_EQ(payloadConsumer->errors[0], ResetStreamErrorCode::MALFORMED_TRACK);
  ASSERT_EQ(callback->errors.size(), 1);
  EXPECT_EQ(callback->errors[0].error, ResetStreamErrorCode::MALFORMED_TRACK);
  EXPECT_EQ(callback->allDataReceivedCount, 0);

  receiver->reset(ResetStreamErrorCode::CANCELLED);
  EXPECT_TRUE(receiver->endOfFetch().hasError());
  EXPECT_EQ(payloadConsumer->errors.size(), 1);
  EXPECT_EQ(callback->errors.size(), 1);
  EXPECT_EQ(callback->allDataReceivedCount, 0);
}

} // namespace

TEST(
    StreamingObjectReceiverTest,
    ForwardsHeaderAndEachPayloadChunkImmediately) {
  auto callback = std::make_shared<RecordingReceiverCallback>();
  auto receiver = std::make_shared<StreamingObjectReceiver>(
      StreamingObjectReceiver::SUBSCRIBE, callback);
  ASSERT_TRUE(receiver->setTrackAlias(TrackAlias(19)).hasValue());

  BeginSubgroupOptions options;
  options.containsLastInGroup = true;
  options.beginsWithFirstObject = false;
  options.subgroupIDFormat = SubgroupIDFormat::Zero;
  options.includeExtensions = false;
  auto subgroup = beginSubgroup(receiver, 11, 13, options);
  ASSERT_NE(subgroup, nullptr);

  Extensions extensions;
  extensions.insertMutableExtension(Extension(2, 23));
  extensions.insertImmutableExtension(Extension(4, 29));
  ASSERT_TRUE(
      subgroup->beginObject(17, 6, makePayload("abc"), extensions).hasValue());

  ASSERT_EQ(callback->contexts.size(), 1);
  const auto& context = callback->contexts.front();
  ASSERT_TRUE(context.trackAlias.has_value());
  EXPECT_EQ(context.trackAlias->value, 19);
  EXPECT_EQ(context.header.group, 11);
  EXPECT_EQ(context.header.subgroup, 13);
  EXPECT_EQ(context.header.id, 17);
  EXPECT_EQ(context.header.priority, 7);
  EXPECT_EQ(context.header.length, 6);
  EXPECT_EQ(context.header.status, ObjectStatus::NORMAL);
  EXPECT_EQ(context.header.extensions, extensions);
  EXPECT_FALSE(context.lastInGroup.has_value());
  ASSERT_TRUE(context.subgroupOptions.has_value());
  EXPECT_TRUE(context.subgroupOptions->containsLastInGroup);
  EXPECT_FALSE(context.subgroupOptions->beginsWithFirstObject);
  EXPECT_EQ(context.subgroupOptions->subgroupIDFormat, SubgroupIDFormat::Zero);
  EXPECT_FALSE(context.subgroupOptions->includeExtensions);

  ASSERT_EQ(callback->consumers.size(), 1);
  auto payloadConsumer = callback->consumers.front();
  ASSERT_EQ(payloadConsumer->chunks.size(), 1);
  EXPECT_EQ(payloadConsumer->chunks[0].value, "abc");
  EXPECT_FALSE(payloadConsumer->chunks[0].metadata.endOfObject);

  auto result = subgroup->objectPayload(makePayload("def"), false);
  ASSERT_TRUE(result.hasValue());
  EXPECT_EQ(*result, ObjectPublishStatus::DONE);
  ASSERT_EQ(payloadConsumer->chunks.size(), 2);
  EXPECT_EQ(payloadConsumer->chunks[1].value, "def");
  EXPECT_TRUE(payloadConsumer->chunks[1].metadata.endOfObject);
  EXPECT_FALSE(payloadConsumer->chunks[1].metadata.endOfSubgroup);
  EXPECT_FALSE(payloadConsumer->chunks[1].metadata.lastInGroup.has_value());
  const std::vector<std::string> expectedEvents{
      "object0:begin", "object0:payload", "object0:payload-final"};
  EXPECT_EQ(callback->events, expectedEvents);
}

TEST(StreamingObjectReceiverTest, InterleavedSubgroupsKeepPayloadsSeparated) {
  auto callback = std::make_shared<RecordingReceiverCallback>();
  auto receiver = std::make_shared<StreamingObjectReceiver>(
      StreamingObjectReceiver::SUBSCRIBE, callback);
  auto first = beginSubgroup(receiver, 1, 10);
  auto second = beginSubgroup(receiver, 2, 20);
  ASSERT_NE(first, nullptr);
  ASSERT_NE(second, nullptr);

  ASSERT_TRUE(first->beginObject(100, 4, makePayload("a"), {}).hasValue());
  ASSERT_TRUE(second->beginObject(200, 4, makePayload("x"), {}).hasValue());
  ASSERT_EQ(callback->consumers.size(), 2);
  auto firstConsumer = callback->consumers[0];
  auto secondConsumer = callback->consumers[1];

  EXPECT_EQ(
      first->objectPayload(makePayload("bcd"), false).value(),
      ObjectPublishStatus::DONE);
  EXPECT_EQ(
      second->objectPayload(makePayload("yz1"), false).value(),
      ObjectPublishStatus::DONE);

  ASSERT_EQ(firstConsumer->chunks.size(), 2);
  EXPECT_EQ(firstConsumer->chunks[0].value, "a");
  EXPECT_EQ(firstConsumer->chunks[1].value, "bcd");
  ASSERT_EQ(secondConsumer->chunks.size(), 2);
  EXPECT_EQ(secondConsumer->chunks[0].value, "x");
  EXPECT_EQ(secondConsumer->chunks[1].value, "yz1");
  const std::vector<std::string> expectedEvents{
      "object0:begin",
      "object0:payload",
      "object1:begin",
      "object1:payload",
      "object0:payload-final",
      "object1:payload-final"};
  EXPECT_EQ(callback->events, expectedEvents);
}

TEST(StreamingObjectReceiverTest, ZeroLengthObjectCompletesAtBegin) {
  auto callback = std::make_shared<RecordingReceiverCallback>();
  auto receiver = std::make_shared<StreamingObjectReceiver>(
      StreamingObjectReceiver::SUBSCRIBE, callback);
  auto subgroup = beginSubgroup(receiver, 1, 2);
  ASSERT_NE(subgroup, nullptr);

  EXPECT_TRUE(subgroup->beginObject(3, 0, nullptr, {}).hasValue());

  ASSERT_EQ(callback->consumers.size(), 1);
  ASSERT_EQ(callback->consumers[0]->chunks.size(), 1);
  EXPECT_TRUE(callback->consumers[0]->chunks[0].value.empty());
  EXPECT_TRUE(callback->consumers[0]->chunks[0].metadata.endOfObject);
}

TEST(StreamingObjectReceiverTest, OneShotObjectStartsThenCompletes) {
  auto callback = std::make_shared<RecordingReceiverCallback>();
  auto receiver = std::make_shared<StreamingObjectReceiver>(
      StreamingObjectReceiver::SUBSCRIBE, callback);
  BeginSubgroupOptions options;
  options.containsLastInGroup = true;
  auto subgroup = beginSubgroup(receiver, 4, 5, options);
  ASSERT_NE(subgroup, nullptr);

  EXPECT_TRUE(subgroup->object(6, makePayload("data"), {}, true).hasValue());

  ASSERT_EQ(callback->contexts.size(), 1);
  EXPECT_EQ(callback->contexts[0].header.length, 4);
  ASSERT_TRUE(callback->contexts[0].lastInGroup.has_value());
  EXPECT_TRUE(*callback->contexts[0].lastInGroup);
  ASSERT_EQ(callback->consumers.size(), 1);
  ASSERT_EQ(callback->consumers[0]->chunks.size(), 1);
  EXPECT_EQ(callback->consumers[0]->chunks[0].value, "data");
  EXPECT_TRUE(callback->consumers[0]->chunks[0].metadata.endOfObject);
  EXPECT_TRUE(callback->consumers[0]->chunks[0].metadata.endOfSubgroup);
  ASSERT_TRUE(
      callback->consumers[0]->chunks[0].metadata.lastInGroup.has_value());
  EXPECT_TRUE(*callback->consumers[0]->chunks[0].metadata.lastInGroup);
}

TEST(
    StreamingObjectReceiverTest,
    GroupEndStatusCanFollowObjectsInAnyUndeclaredSubgroup) {
  auto callback = std::make_shared<RecordingReceiverCallback>();
  auto receiver = std::make_shared<StreamingObjectReceiver>(
      StreamingObjectReceiver::SUBSCRIBE, callback);
  auto subgroup = beginSubgroup(receiver, 4, 5);
  ASSERT_NE(subgroup, nullptr);

  EXPECT_TRUE(subgroup->object(6, makePayload("data")).hasValue());
  EXPECT_TRUE(subgroup->endOfGroup(7).hasValue());

  ASSERT_EQ(callback->contexts.size(), 1);
  EXPECT_FALSE(callback->contexts[0].lastInGroup.has_value());
  ASSERT_EQ(callback->consumers.size(), 1);
  EXPECT_FALSE(
      callback->consumers[0]->chunks[0].metadata.lastInGroup.has_value());
  ASSERT_EQ(callback->statuses.size(), 1);
  EXPECT_EQ(callback->statuses[0].lastInGroup, true);
}

TEST(
    StreamingObjectReceiverTest,
    FinOnUndeclaredSubgroupLeavesLastInGroupUnknown) {
  auto callback = std::make_shared<RecordingReceiverCallback>();
  auto receiver = std::make_shared<StreamingObjectReceiver>(
      StreamingObjectReceiver::SUBSCRIBE, callback);
  auto subgroup = beginSubgroup(receiver, 4, 5);
  ASSERT_NE(subgroup, nullptr);

  ASSERT_TRUE(subgroup->beginObject(6, 4, makePayload("ab"), {}).hasValue());
  ASSERT_TRUE(subgroup->objectPayload(makePayload("cd"), true).hasValue());

  ASSERT_EQ(callback->contexts.size(), 1);
  EXPECT_FALSE(callback->contexts[0].lastInGroup.has_value());
  ASSERT_EQ(callback->consumers.size(), 1);
  const auto& metadata = callback->consumers[0]->chunks.back().metadata;
  EXPECT_TRUE(metadata.endOfSubgroup);
  EXPECT_FALSE(metadata.lastInGroup.has_value());
}

TEST(StreamingObjectReceiverTest, FinAfterChunkedObjectIsReportedSeparately) {
  auto callback = std::make_shared<RecordingReceiverCallback>();
  auto receiver = std::make_shared<StreamingObjectReceiver>(
      StreamingObjectReceiver::SUBSCRIBE, callback);
  BeginSubgroupOptions options;
  options.containsLastInGroup = true;
  auto subgroup = beginSubgroup(receiver, 1, 2, options);
  ASSERT_NE(subgroup, nullptr);
  ASSERT_TRUE(subgroup->beginObject(3, 4, makePayload("ab"), {}).hasValue());
  ASSERT_TRUE(subgroup->objectPayload(makePayload("cd"), false).hasValue());
  ASSERT_EQ(callback->consumers.size(), 1);
  const auto& metadata = callback->consumers[0]->chunks.back().metadata;
  EXPECT_TRUE(metadata.endOfObject);
  EXPECT_FALSE(metadata.endOfSubgroup);
  EXPECT_FALSE(metadata.lastInGroup.has_value());
  EXPECT_TRUE(callback->endOfStreams.empty());

  ASSERT_TRUE(subgroup->endOfSubgroup().hasValue());

  ASSERT_EQ(callback->endOfStreams.size(), 1);
  EXPECT_EQ(callback->endOfStreams[0].groupID, 1);
  EXPECT_EQ(callback->endOfStreams[0].subgroupID, 2);
  EXPECT_TRUE(callback->errors.empty());
}

TEST(StreamingObjectReceiverTest, StatusPreservesSubgroupOptions) {
  auto callback = std::make_shared<RecordingReceiverCallback>();
  auto receiver = std::make_shared<StreamingObjectReceiver>(
      StreamingObjectReceiver::SUBSCRIBE, callback);
  BeginSubgroupOptions options;
  options.containsLastInGroup = true;
  auto subgroup = beginSubgroup(receiver, 7, 8, options);
  ASSERT_NE(subgroup, nullptr);

  EXPECT_TRUE(subgroup->endOfGroup(9).hasValue());

  ASSERT_EQ(callback->statuses.size(), 1);
  const auto& status = callback->statuses[0];
  EXPECT_EQ(status.header.group, 7);
  EXPECT_EQ(status.header.subgroup, 8);
  EXPECT_EQ(status.header.id, 9);
  EXPECT_EQ(status.header.status, ObjectStatus::END_OF_GROUP);
  EXPECT_FALSE(status.header.length.has_value());
  EXPECT_EQ(status.lastInGroup, true);
  ASSERT_TRUE(status.subgroupOptions.has_value());
  EXPECT_TRUE(status.subgroupOptions->containsLastInGroup);
  EXPECT_TRUE(callback->consumers.empty());
}

TEST(
    StreamingObjectReceiverTest,
    ReentrantResetDuringSubscribeEndOfGroupDoesNotReportError) {
  auto callback = std::make_shared<RecordingReceiverCallback>();
  auto receiver = std::make_shared<StreamingObjectReceiver>(
      StreamingObjectReceiver::SUBSCRIBE, callback);
  auto subgroup = beginSubgroup(receiver, 1, 2);
  ASSERT_NE(subgroup, nullptr);
  callback->onObjectStatusAction = [subgroup] {
    subgroup->reset(ResetStreamErrorCode::CANCELLED);
  };

  auto result = subgroup->endOfGroup(3);

  EXPECT_TRUE(result.hasValue());
  ASSERT_EQ(callback->statuses.size(), 1);
  EXPECT_EQ(callback->statuses[0].header.status, ObjectStatus::END_OF_GROUP);
  EXPECT_TRUE(callback->errors.empty());
}

TEST(
    StreamingObjectReceiverTest,
    ReentrantResetDuringSubscribeEndOfTrackDoesNotReportError) {
  auto callback = std::make_shared<RecordingReceiverCallback>();
  auto receiver = std::make_shared<StreamingObjectReceiver>(
      StreamingObjectReceiver::SUBSCRIBE, callback);
  auto subgroup = beginSubgroup(receiver, 1, 2);
  ASSERT_NE(subgroup, nullptr);
  callback->onObjectStatusAction = [subgroup] {
    subgroup->reset(ResetStreamErrorCode::CANCELLED);
  };

  auto result = subgroup->endOfTrackAndGroup(3);

  EXPECT_TRUE(result.hasValue());
  ASSERT_EQ(callback->statuses.size(), 1);
  EXPECT_EQ(callback->statuses[0].header.status, ObjectStatus::END_OF_TRACK);
  EXPECT_TRUE(callback->errors.empty());
}

TEST(
    StreamingObjectReceiverTest,
    ReentrantResetDuringFinalFetchEndOfGroupDoesNotReportError) {
  auto callback = std::make_shared<RecordingReceiverCallback>();
  auto receiver = std::make_shared<StreamingObjectReceiver>(
      StreamingObjectReceiver::FETCH, callback);
  callback->onObjectStatusAction = [receiver] {
    receiver->reset(ResetStreamErrorCode::CANCELLED);
  };

  auto result = receiver->endOfGroup(1, 2, 3, true);

  EXPECT_TRUE(result.hasValue());
  ASSERT_EQ(callback->statuses.size(), 1);
  EXPECT_EQ(callback->statuses[0].header.status, ObjectStatus::END_OF_GROUP);
  EXPECT_TRUE(callback->errors.empty());
  EXPECT_EQ(callback->allDataReceivedCount, 1);
}

TEST(
    StreamingObjectReceiverTest,
    ReentrantResetDuringFetchEndOfTrackDoesNotReportError) {
  auto callback = std::make_shared<RecordingReceiverCallback>();
  auto receiver = std::make_shared<StreamingObjectReceiver>(
      StreamingObjectReceiver::FETCH, callback);
  callback->onObjectStatusAction = [receiver] {
    receiver->reset(ResetStreamErrorCode::CANCELLED);
  };

  auto result = receiver->endOfTrackAndGroup(1, 2, 3);

  EXPECT_TRUE(result.hasValue());
  ASSERT_EQ(callback->statuses.size(), 1);
  EXPECT_EQ(callback->statuses[0].header.status, ObjectStatus::END_OF_TRACK);
  EXPECT_TRUE(callback->errors.empty());
  EXPECT_EQ(callback->allDataReceivedCount, 1);
}

TEST(StreamingObjectReceiverTest, ResetTerminatesIncompleteObject) {
  auto callback = std::make_shared<RecordingReceiverCallback>();
  auto receiver = std::make_shared<StreamingObjectReceiver>(
      StreamingObjectReceiver::SUBSCRIBE, callback);
  auto subgroup = beginSubgroup(receiver, 1, 2);
  ASSERT_NE(subgroup, nullptr);
  ASSERT_TRUE(subgroup->beginObject(3, 6, makePayload("abc"), {}).hasValue());
  auto payloadConsumer = callback->consumers.front();

  subgroup->reset(ResetStreamErrorCode::CANCELLED);

  ASSERT_EQ(payloadConsumer->errors.size(), 1);
  EXPECT_EQ(payloadConsumer->errors[0], ResetStreamErrorCode::CANCELLED);
  ASSERT_EQ(callback->errors.size(), 1);
  EXPECT_EQ(callback->errors[0].error, ResetStreamErrorCode::CANCELLED);
  EXPECT_EQ(callback->errors[0].context.groupID, 1);
  EXPECT_EQ(callback->errors[0].context.subgroupID, 2);
  EXPECT_TRUE(subgroup->endOfSubgroup().hasValue());
  EXPECT_EQ(callback->errors.size(), 1);
  EXPECT_TRUE(callback->endOfStreams.empty());
}

TEST(StreamingObjectReceiverTest, EarlyFinForwardsChunkThenReportsTruncation) {
  auto callback = std::make_shared<RecordingReceiverCallback>();
  auto receiver = std::make_shared<StreamingObjectReceiver>(
      StreamingObjectReceiver::SUBSCRIBE, callback);
  auto subgroup = beginSubgroup(receiver, 1, 2);
  ASSERT_NE(subgroup, nullptr);
  ASSERT_TRUE(subgroup->beginObject(3, 6, makePayload("ab"), {}).hasValue());
  auto payloadConsumer = callback->consumers.front();

  auto result = subgroup->objectPayload(makePayload("c"), true);

  ASSERT_TRUE(result.hasError());
  EXPECT_EQ(result.error().code, MoQPublishError::API_ERROR);
  ASSERT_EQ(payloadConsumer->chunks.size(), 2);
  EXPECT_EQ(payloadConsumer->chunks[1].value, "c");
  EXPECT_FALSE(payloadConsumer->chunks[1].metadata.endOfObject);
  ASSERT_EQ(payloadConsumer->errors.size(), 1);
  EXPECT_EQ(payloadConsumer->errors[0], ResetStreamErrorCode::MALFORMED_TRACK);
  ASSERT_EQ(callback->errors.size(), 1);
  EXPECT_EQ(callback->errors[0].error, ResetStreamErrorCode::MALFORMED_TRACK);
}

TEST(StreamingObjectReceiverTest, RejectsPayloadBeyondDeclaredLength) {
  auto callback = std::make_shared<RecordingReceiverCallback>();
  auto receiver = std::make_shared<StreamingObjectReceiver>(
      StreamingObjectReceiver::SUBSCRIBE, callback);
  auto subgroup = beginSubgroup(receiver, 1, 2);
  ASSERT_NE(subgroup, nullptr);
  ASSERT_TRUE(subgroup->beginObject(3, 3, makePayload("a"), {}).hasValue());
  auto payloadConsumer = callback->consumers.front();

  auto result = subgroup->objectPayload(makePayload("bcd"), false);

  ASSERT_TRUE(result.hasError());
  EXPECT_EQ(result.error().code, MoQPublishError::API_ERROR);
  EXPECT_EQ(payloadConsumer->chunks.size(), 1);
  ASSERT_EQ(payloadConsumer->errors.size(), 1);
  EXPECT_EQ(payloadConsumer->errors[0], ResetStreamErrorCode::MALFORMED_TRACK);
}

TEST(StreamingObjectReceiverTest, CompleteObjectValidatesDeclaredLength) {
  auto callback = std::make_shared<RecordingReceiverCallback>();
  auto receiver = std::make_shared<StreamingObjectReceiver>(
      StreamingObjectReceiver::SUBSCRIBE, callback);

  auto result =
      receiver->objectStream(ObjectHeader(1, 2, 3, 4, 5), makePayload("four"));

  ASSERT_TRUE(result.hasError());
  EXPECT_EQ(result.error().code, MoQPublishError::API_ERROR);
  EXPECT_TRUE(callback->contexts.empty());
}

TEST(StreamingObjectReceiverTest, DatagramPreservesDeliveryMetadata) {
  auto callback = std::make_shared<RecordingReceiverCallback>();
  auto receiver = std::make_shared<StreamingObjectReceiver>(
      StreamingObjectReceiver::SUBSCRIBE, callback);

  EXPECT_TRUE(
      receiver->datagram(ObjectHeader(1, 0, 3, 4, 4), makePayload("data"), true)
          .hasValue());

  ASSERT_EQ(callback->contexts.size(), 1);
  EXPECT_TRUE(callback->contexts[0].header.forwardingPreferenceIsDatagram);
  EXPECT_EQ(callback->contexts[0].lastInGroup, true);
  ASSERT_EQ(callback->consumers.size(), 1);
  EXPECT_TRUE(callback->consumers[0]->chunks[0].metadata.endOfObject);
}

TEST(StreamingObjectReceiverTest, ObjectStreamProducesNoEndOfStream) {
  auto callback = std::make_shared<RecordingReceiverCallback>();
  auto receiver = std::make_shared<StreamingObjectReceiver>(
      StreamingObjectReceiver::SUBSCRIBE, callback);

  EXPECT_TRUE(
      receiver
          ->objectStream(ObjectHeader(1, 2, 3, 4, 4), makePayload("data"), true)
          .hasValue());

  ASSERT_EQ(callback->consumers.size(), 1);
  const auto& metadata = callback->consumers[0]->chunks[0].metadata;
  EXPECT_TRUE(metadata.endOfObject);
  EXPECT_FALSE(metadata.endOfSubgroup);
  EXPECT_EQ(metadata.lastInGroup, true);
  EXPECT_TRUE(callback->endOfStreams.empty());
}

TEST(StreamingObjectReceiverTest, FetchPropagatesPayloadFailure) {
  auto callback = std::make_shared<RecordingReceiverCallback>();
  callback->nextPayloadError = MoQPublishError::TOO_FAR_BEHIND;
  auto receiver = std::make_shared<StreamingObjectReceiver>(
      StreamingObjectReceiver::FETCH, callback);

  auto result = receiver->object(1, 2, 3, makePayload("data"), {}, true, false);

  ASSERT_TRUE(result.hasError());
  EXPECT_EQ(result.error().code, MoQPublishError::TOO_FAR_BEHIND);
  EXPECT_EQ(callback->allDataReceivedCount, 0);
  ASSERT_EQ(callback->consumers.size(), 1);
  ASSERT_EQ(callback->consumers[0]->errors.size(), 1);
  EXPECT_EQ(
      callback->consumers[0]->errors[0], ResetStreamErrorCode::TOO_FAR_BEHIND);
  ASSERT_EQ(callback->errors.size(), 1);
  EXPECT_EQ(callback->errors[0].error, ResetStreamErrorCode::TOO_FAR_BEHIND);

  auto later = receiver->object(1, 2, 4, makePayload("more"), {}, true, false);
  ASSERT_TRUE(later.hasError());
  EXPECT_EQ(callback->contexts.size(), 1);
  EXPECT_EQ(callback->consumers[0]->errors.size(), 1);
}

TEST(StreamingObjectReceiverTest, SubscribeModeRejectsFetchCallbacks) {
  auto callback = std::make_shared<RecordingReceiverCallback>();
  auto receiver = std::make_shared<StreamingObjectReceiver>(
      StreamingObjectReceiver::SUBSCRIBE, callback);

  auto objectResult = receiver->object(1, 2, 3, makePayload("data"), {}, false);
  auto payloadResult = receiver->objectPayload(makePayload("data"), false);
  auto unknownRangeResult = receiver->endOfUnknownRange(1, 3, false);
  auto terminalResult = receiver->endOfFetch();

  ASSERT_TRUE(objectResult.hasError());
  EXPECT_EQ(objectResult.error().code, MoQPublishError::API_ERROR);
  ASSERT_TRUE(payloadResult.hasError());
  EXPECT_EQ(payloadResult.error().code, MoQPublishError::API_ERROR);
  ASSERT_TRUE(unknownRangeResult.hasError());
  EXPECT_EQ(unknownRangeResult.error().code, MoQPublishError::API_ERROR);
  ASSERT_TRUE(terminalResult.hasError());
  EXPECT_EQ(terminalResult.error().code, MoQPublishError::API_ERROR);

  receiver->reset(ResetStreamErrorCode::CANCELLED);
  EXPECT_TRUE(callback->errors.empty());
}

TEST(StreamingObjectReceiverTest, PublishDoneWaitsForSubgroupTerminal) {
  auto callback = std::make_shared<RecordingReceiverCallback>();
  auto receiver = std::make_shared<StreamingObjectReceiver>(
      StreamingObjectReceiver::SUBSCRIBE, callback);
  auto subgroup = beginSubgroup(receiver, 1, 2);
  ASSERT_NE(subgroup, nullptr);

  PublishDone done;
  done.requestID = RequestID(1);
  done.statusCode = PublishDoneStatusCode::SUBSCRIPTION_ENDED;
  ASSERT_TRUE(receiver->publishDone(std::move(done)).hasValue());
  EXPECT_EQ(callback->publishDoneCount, 1);
  EXPECT_EQ(callback->allDataReceivedCount, 0);

  ASSERT_TRUE(subgroup->endOfSubgroup().hasValue());
  ASSERT_EQ(callback->endOfStreams.size(), 1);
  EXPECT_EQ(callback->endOfStreams[0].groupID, 1);
  EXPECT_EQ(callback->endOfStreams[0].subgroupID, 2);
  EXPECT_EQ(callback->allDataReceivedCount, 1);
}

TEST(
    StreamingObjectReceiverTest,
    ReentrantResetDuringSubscribeEndOfStreamDoesNotReportError) {
  auto callback = std::make_shared<RecordingReceiverCallback>();
  auto receiver = std::make_shared<StreamingObjectReceiver>(
      StreamingObjectReceiver::SUBSCRIBE, callback);
  auto subgroup = beginSubgroup(receiver, 1, 2);
  ASSERT_NE(subgroup, nullptr);
  callback->onEndOfStreamAction = [subgroup] {
    subgroup->reset(ResetStreamErrorCode::CANCELLED);
  };

  auto result = subgroup->endOfSubgroup();

  EXPECT_TRUE(result.hasValue());
  EXPECT_EQ(callback->endOfStreams.size(), 1);
  EXPECT_TRUE(callback->errors.empty());
}

TEST(StreamingObjectReceiverTest, EmptyFetchTerminalIsNotAnEndOfStream) {
  auto callback = std::make_shared<RecordingReceiverCallback>();
  auto receiver = std::make_shared<StreamingObjectReceiver>(
      StreamingObjectReceiver::FETCH, callback);

  auto result = receiver->endOfFetch();

  EXPECT_TRUE(result.hasValue());
  EXPECT_TRUE(callback->endOfStreams.empty());
  EXPECT_TRUE(callback->errors.empty());
  EXPECT_EQ(callback->allDataReceivedCount, 1);
}

TEST(StreamingObjectReceiverTest, PublishDoneRejectsNewDelivery) {
  auto callback = std::make_shared<RecordingReceiverCallback>();
  auto receiver = std::make_shared<StreamingObjectReceiver>(
      StreamingObjectReceiver::SUBSCRIBE, callback);
  auto openSubgroup = beginSubgroup(receiver, 1, 2);
  ASSERT_NE(openSubgroup, nullptr);

  PublishDone done;
  done.requestID = RequestID(1);
  done.statusCode = PublishDoneStatusCode::SUBSCRIPTION_ENDED;
  ASSERT_TRUE(receiver->publishDone(std::move(done)).hasValue());

  EXPECT_TRUE(receiver->beginSubgroup(2, 3, 7).hasError());
  EXPECT_TRUE(
      receiver->objectStream(ObjectHeader(2, 3, 4, 7, 4), makePayload("data"))
          .hasError());
  EXPECT_TRUE(
      receiver->datagram(ObjectHeader(2, 0, 4, 7, 4), makePayload("data"))
          .hasError());
  EXPECT_TRUE(callback->contexts.empty());

  EXPECT_TRUE(openSubgroup->object(3, makePayload("ok"), {}, true).hasValue());
  EXPECT_EQ(callback->contexts.size(), 1);
  EXPECT_EQ(callback->allDataReceivedCount, 1);
}

TEST(StreamingObjectReceiverTest, PublishDoneIsTerminalDuringCallback) {
  auto callback = std::make_shared<RecordingReceiverCallback>();
  auto receiver = std::make_shared<StreamingObjectReceiver>(
      StreamingObjectReceiver::SUBSCRIBE, callback);
  bool subgroupRejected = false;
  bool objectRejected = false;
  bool datagramRejected = false;
  callback->onPublishDoneAction = [&] {
    subgroupRejected = receiver->beginSubgroup(1, 2, 7).hasError();
    objectRejected =
        receiver->objectStream(ObjectHeader(1, 2, 3, 7, 4), makePayload("data"))
            .hasError();
    datagramRejected =
        receiver->datagram(ObjectHeader(1, 0, 3, 7, 4), makePayload("data"))
            .hasError();
  };

  PublishDone done;
  done.requestID = RequestID(1);
  done.statusCode = PublishDoneStatusCode::SUBSCRIPTION_ENDED;
  ASSERT_TRUE(receiver->publishDone(std::move(done)).hasValue());

  EXPECT_TRUE(subgroupRejected);
  EXPECT_TRUE(objectRejected);
  EXPECT_TRUE(datagramRejected);
  EXPECT_TRUE(callback->contexts.empty());
  EXPECT_EQ(callback->publishDoneCount, 1);
  EXPECT_EQ(callback->allDataReceivedCount, 1);
}

TEST(StreamingObjectReceiverTest, PublishDoneDuringDatagramBeginCancelsIt) {
  auto callback = std::make_shared<RecordingReceiverCallback>();
  auto receiver = std::make_shared<StreamingObjectReceiver>(
      StreamingObjectReceiver::SUBSCRIBE, callback);
  callback->onObjectBeginAction = [&] {
    PublishDone done;
    done.requestID = RequestID(1);
    done.statusCode = PublishDoneStatusCode::SESSION_CLOSED;
    ASSERT_TRUE(receiver->publishDone(std::move(done)).hasValue());
  };

  EXPECT_TRUE(
      receiver->datagram(ObjectHeader(1, 0, 3, 7, 4), makePayload("data"))
          .hasError());

  ASSERT_EQ(callback->consumers.size(), 1);
  EXPECT_TRUE(callback->consumers[0]->chunks.empty());
  EXPECT_EQ(
      callback->consumers[0]->errors,
      std::vector<ResetStreamErrorCode>{ResetStreamErrorCode::CANCELLED});
  EXPECT_EQ(callback->allDataReceivedCount, 1);
}

TEST(
    StreamingObjectReceiverTest,
    PublishDoneDuringFinalPayloadKeepsItComplete) {
  auto callback = std::make_shared<RecordingReceiverCallback>();
  auto receiver = std::make_shared<StreamingObjectReceiver>(
      StreamingObjectReceiver::SUBSCRIBE, callback);
  callback->nextOnPayloadAction = [&] {
    PublishDone done;
    done.requestID = RequestID(1);
    done.statusCode = PublishDoneStatusCode::SESSION_CLOSED;
    ASSERT_TRUE(receiver->publishDone(std::move(done)).hasValue());
  };

  EXPECT_TRUE(
      receiver->objectStream(ObjectHeader(1, 2, 3, 7, 4), makePayload("data"))
          .hasValue());

  ASSERT_EQ(callback->consumers.size(), 1);
  ASSERT_EQ(callback->consumers[0]->chunks.size(), 1);
  EXPECT_TRUE(callback->consumers[0]->chunks[0].metadata.endOfObject);
  EXPECT_TRUE(callback->consumers[0]->errors.empty());
  EXPECT_EQ(callback->allDataReceivedCount, 1);
}

TEST(
    StreamingObjectReceiverTest,
    PayloadFailureAfterPublishDoneFailsOnlyObject) {
  auto callback = std::make_shared<RecordingReceiverCallback>();
  auto receiver = std::make_shared<StreamingObjectReceiver>(
      StreamingObjectReceiver::SUBSCRIBE, callback);
  callback->nextOnPayloadAction = [&] {
    PublishDone done;
    done.requestID = RequestID(1);
    done.statusCode = PublishDoneStatusCode::SESSION_CLOSED;
    ASSERT_TRUE(receiver->publishDone(std::move(done)).hasValue());
  };
  callback->nextPayloadError = MoQPublishError::TOO_FAR_BEHIND;

  auto result =
      receiver->objectStream(ObjectHeader(1, 2, 3, 7, 4), makePayload("data"));

  ASSERT_TRUE(result.hasError());
  EXPECT_EQ(result.error().code, MoQPublishError::TOO_FAR_BEHIND);
  ASSERT_EQ(callback->consumers.size(), 1);
  EXPECT_EQ(
      callback->consumers[0]->errors,
      std::vector<ResetStreamErrorCode>{ResetStreamErrorCode::TOO_FAR_BEHIND});
  EXPECT_TRUE(callback->errors.empty());
  EXPECT_EQ(callback->allDataReceivedCount, 1);
}

TEST(StreamingObjectReceiverTest, PayloadFailureTerminatesSubgroup) {
  auto callback = std::make_shared<RecordingReceiverCallback>();
  callback->nextPayloadError = MoQPublishError::TOO_FAR_BEHIND;
  auto receiver = std::make_shared<StreamingObjectReceiver>(
      StreamingObjectReceiver::SUBSCRIBE, callback);
  auto subgroup = beginSubgroup(receiver, 1, 2);
  ASSERT_NE(subgroup, nullptr);

  auto result = subgroup->beginObject(3, 6, makePayload("abc"), noExtensions());
  ASSERT_TRUE(result.hasError());
  EXPECT_EQ(result.error().code, MoQPublishError::TOO_FAR_BEHIND);
  ASSERT_EQ(callback->consumers.size(), 1);
  auto consumer = callback->consumers.front();
  ASSERT_EQ(consumer->chunks.size(), 1);
  ASSERT_EQ(consumer->errors.size(), 1);
  EXPECT_EQ(consumer->errors[0], ResetStreamErrorCode::TOO_FAR_BEHIND);
  ASSERT_EQ(callback->errors.size(), 1);
  EXPECT_EQ(callback->errors[0].error, ResetStreamErrorCode::TOO_FAR_BEHIND);

  auto laterPayload = subgroup->objectPayload(makePayload("def"), false);
  ASSERT_TRUE(laterPayload.hasError());
  EXPECT_EQ(consumer->chunks.size(), 1);
  EXPECT_EQ(consumer->errors.size(), 1);
  EXPECT_EQ(callback->errors.size(), 1);
}

TEST(StreamingObjectReceiverTest, FinalPayloadFailureDoesNotReportDone) {
  auto callback = std::make_shared<RecordingReceiverCallback>();
  auto receiver = std::make_shared<StreamingObjectReceiver>(
      StreamingObjectReceiver::SUBSCRIBE, callback);
  auto subgroup = beginSubgroup(receiver, 1, 2);
  ASSERT_NE(subgroup, nullptr);
  ASSERT_TRUE(subgroup->beginObject(3, 6, makePayload("abc"), {}).hasValue());
  ASSERT_EQ(callback->consumers.size(), 1);
  auto consumer = callback->consumers[0];
  consumer->payloadErrorCode = MoQPublishError::TOO_FAR_BEHIND;

  auto result = subgroup->objectPayload(makePayload("def"), true);

  ASSERT_TRUE(result.hasError());
  EXPECT_EQ(result.error().code, MoQPublishError::TOO_FAR_BEHIND);
  ASSERT_EQ(consumer->chunks.size(), 2);
  EXPECT_TRUE(consumer->chunks.back().metadata.endOfObject);
  ASSERT_EQ(consumer->errors.size(), 1);
  EXPECT_EQ(consumer->errors[0], ResetStreamErrorCode::TOO_FAR_BEHIND);
  ASSERT_EQ(callback->errors.size(), 1);
  EXPECT_TRUE(callback->endOfStreams.empty());
  EXPECT_EQ(callback->allDataReceivedCount, 0);

  EXPECT_TRUE(subgroup->objectPayload(makePayload("late"), false).hasError());
  EXPECT_EQ(consumer->chunks.size(), 2);
  EXPECT_EQ(consumer->errors.size(), 1);
}

TEST(StreamingObjectReceiverTest, ObjectBeginFailurePreservesError) {
  auto callback = std::make_shared<RecordingReceiverCallback>();
  callback->nextObjectBeginError = MoQPublishError::TOO_FAR_BEHIND;
  auto receiver = std::make_shared<StreamingObjectReceiver>(
      StreamingObjectReceiver::SUBSCRIBE, callback);
  auto subgroup = beginSubgroup(receiver, 1, 2);
  ASSERT_NE(subgroup, nullptr);

  auto result = subgroup->beginObject(3, 4, nullptr, {});

  ASSERT_TRUE(result.hasError());
  EXPECT_EQ(result.error().code, MoQPublishError::TOO_FAR_BEHIND);
  EXPECT_TRUE(callback->consumers.empty());
  ASSERT_EQ(callback->errors.size(), 1);
  EXPECT_EQ(callback->errors[0].error, ResetStreamErrorCode::TOO_FAR_BEHIND);

  callback->nextObjectBeginError.reset();
  EXPECT_TRUE(subgroup->object(4, makePayload("next"), {}, false).hasError());
  EXPECT_EQ(callback->contexts.size(), 1);
  EXPECT_EQ(callback->errors.size(), 1);
}

TEST(StreamingObjectReceiverTest, CallbackApiErrorIsReportedAsInternalError) {
  auto callback = std::make_shared<RecordingReceiverCallback>();
  callback->nextObjectBeginError = MoQPublishError::API_ERROR;
  auto receiver = std::make_shared<StreamingObjectReceiver>(
      StreamingObjectReceiver::SUBSCRIBE, callback);
  auto subgroup = beginSubgroup(receiver, 1, 2);
  ASSERT_NE(subgroup, nullptr);

  EXPECT_TRUE(subgroup->beginObject(3, 4, nullptr, {}).hasError());

  ASSERT_EQ(callback->errors.size(), 1);
  EXPECT_EQ(callback->errors[0].error, ResetStreamErrorCode::INTERNAL_ERROR);
}

TEST(StreamingObjectReceiverTest, EndOfSubgroupFailsActiveObject) {
  auto callback = std::make_shared<RecordingReceiverCallback>();
  auto receiver = std::make_shared<StreamingObjectReceiver>(
      StreamingObjectReceiver::SUBSCRIBE, callback);
  auto subgroup = beginSubgroup(receiver, 1, 2);
  ASSERT_NE(subgroup, nullptr);
  PublishDone done;
  done.requestID = RequestID(1);
  done.statusCode = PublishDoneStatusCode::SUBSCRIPTION_ENDED;
  ASSERT_TRUE(receiver->publishDone(std::move(done)).hasValue());
  ASSERT_TRUE(subgroup->beginObject(3, 4, makePayload("ab"), {}).hasValue());

  auto result = subgroup->endOfSubgroup();

  ASSERT_TRUE(result.hasError());
  const std::vector<ResetStreamErrorCode> expectedErrors{
      ResetStreamErrorCode::MALFORMED_TRACK};
  EXPECT_EQ(callback->consumers[0]->errors, expectedErrors);
  ASSERT_EQ(callback->errors.size(), 1);
  EXPECT_EQ(callback->errors[0].error, ResetStreamErrorCode::MALFORMED_TRACK);
  EXPECT_TRUE(callback->endOfStreams.empty());
  EXPECT_EQ(callback->allDataReceivedCount, 1);
}

TEST(StreamingObjectReceiverTest, DroppedSubgroupFailsInFlightObject) {
  auto callback = std::make_shared<RecordingReceiverCallback>();
  auto receiver = std::make_shared<StreamingObjectReceiver>(
      StreamingObjectReceiver::SUBSCRIBE, callback);
  auto subgroup = beginSubgroup(receiver, 1, 2);
  ASSERT_NE(subgroup, nullptr);
  ASSERT_TRUE(subgroup->object(0, makePayload("done"), {}, false).hasValue());
  ASSERT_TRUE(subgroup->beginObject(1, 6, makePayload("abc"), {}).hasValue());
  ASSERT_EQ(callback->consumers.size(), 2);

  subgroup.reset();

  EXPECT_TRUE(callback->consumers[0]->errors.empty());
  const std::vector<ResetStreamErrorCode> expectedErrors{
      ResetStreamErrorCode::CANCELLED};
  EXPECT_EQ(callback->consumers[1]->errors, expectedErrors);
}

TEST(StreamingObjectReceiverTest, DroppedFetchReceiverFailsInFlightObject) {
  auto callback = std::make_shared<RecordingReceiverCallback>();
  auto receiver = std::make_shared<StreamingObjectReceiver>(
      StreamingObjectReceiver::FETCH, callback);
  ASSERT_TRUE(
      receiver->beginObject(1, 2, 3, 4, makePayload("a"), {}).hasValue());
  ASSERT_EQ(callback->consumers.size(), 1);

  receiver.reset();

  const std::vector<ResetStreamErrorCode> expectedErrors{
      ResetStreamErrorCode::CANCELLED};
  EXPECT_EQ(callback->consumers[0]->errors, expectedErrors);
}

TEST(StreamingObjectReceiverTest, DroppedSubgroupIsCancelledAndRetired) {
  auto callback = std::make_shared<RecordingReceiverCallback>();
  auto receiver = std::make_shared<StreamingObjectReceiver>(
      StreamingObjectReceiver::SUBSCRIBE, callback);
  auto finished = beginSubgroup(receiver, 1, 2);
  auto dropped = beginSubgroup(receiver, 1, 3);
  ASSERT_NE(finished, nullptr);
  ASSERT_NE(dropped, nullptr);
  PublishDone done;
  done.requestID = RequestID(1);
  done.statusCode = PublishDoneStatusCode::SUBSCRIPTION_ENDED;
  ASSERT_TRUE(receiver->publishDone(std::move(done)).hasValue());
  ASSERT_TRUE(finished->endOfSubgroup().hasValue());
  finished.reset();
  EXPECT_EQ(callback->allDataReceivedCount, 0);
  EXPECT_TRUE(callback->errors.empty());

  dropped.reset();

  ASSERT_EQ(callback->errors.size(), 1);
  EXPECT_EQ(callback->errors[0].context.groupID, 1);
  EXPECT_EQ(callback->errors[0].context.subgroupID, 3);
  EXPECT_EQ(callback->errors[0].error, ResetStreamErrorCode::CANCELLED);
  EXPECT_EQ(callback->allDataReceivedCount, 1);
}

TEST(StreamingObjectReceiverTest, FinalFetchObjectSignalsOnlyAllDataReceived) {
  auto callback = std::make_shared<RecordingReceiverCallback>();
  auto receiver = std::make_shared<StreamingObjectReceiver>(
      StreamingObjectReceiver::FETCH, callback);

  auto result = receiver->object(1, 2, 3, makePayload("data"), {}, true);

  ASSERT_TRUE(result.hasValue());
  ASSERT_EQ(callback->consumers.size(), 1);
  ASSERT_EQ(callback->consumers[0]->chunks.size(), 1);
  const auto& metadata = callback->consumers[0]->chunks[0].metadata;
  EXPECT_TRUE(metadata.endOfObject);
  EXPECT_FALSE(metadata.endOfSubgroup);
  EXPECT_TRUE(callback->endOfStreams.empty());
  EXPECT_EQ(callback->allDataReceivedCount, 1);
}

TEST(StreamingObjectReceiverTest, LatePayloadDoesNotFailFinishedFetch) {
  auto callback = std::make_shared<RecordingReceiverCallback>();
  auto receiver = std::make_shared<StreamingObjectReceiver>(
      StreamingObjectReceiver::FETCH, callback);
  ASSERT_TRUE(
      receiver->object(1, 2, 3, makePayload("data"), {}, true).hasValue());

  auto late = receiver->objectPayload(makePayload("x"), false);

  ASSERT_TRUE(late.hasError());
  EXPECT_EQ(late.error().code, MoQPublishError::API_ERROR);
  EXPECT_TRUE(receiver->endOfFetch().hasValue());
  EXPECT_TRUE(callback->errors.empty());
  EXPECT_EQ(callback->allDataReceivedCount, 1);
}

TEST(StreamingObjectReceiverTest, CompleteObjectPayloadFailureFailsOnlyObject) {
  auto callback = std::make_shared<RecordingReceiverCallback>();
  callback->nextPayloadError = MoQPublishError::TOO_FAR_BEHIND;
  auto receiver = std::make_shared<StreamingObjectReceiver>(
      StreamingObjectReceiver::SUBSCRIBE, callback);

  auto result =
      receiver->objectStream(ObjectHeader(1, 2, 3, 4, 4), makePayload("data"));

  ASSERT_TRUE(result.hasError());
  EXPECT_EQ(result.error().code, MoQPublishError::TOO_FAR_BEHIND);
  ASSERT_EQ(callback->consumers.size(), 1);
  ASSERT_EQ(callback->consumers[0]->errors.size(), 1);
  EXPECT_EQ(
      callback->consumers[0]->errors[0], ResetStreamErrorCode::TOO_FAR_BEHIND);
  ASSERT_EQ(callback->errors.size(), 1);
  EXPECT_EQ(callback->errors[0].context.groupID, 1);
  EXPECT_EQ(callback->errors[0].context.subgroupID, 2);

  callback->nextPayloadError.reset();
  EXPECT_TRUE(
      receiver->objectStream(ObjectHeader(1, 3, 4, 4, 4), makePayload("more"))
          .hasValue());
  EXPECT_NE(beginSubgroup(receiver, 2, 0), nullptr);
}

TEST(StreamingObjectReceiverTest, DatagramFailureDoesNotEndSubscription) {
  auto callback = std::make_shared<RecordingReceiverCallback>();
  callback->nextPayloadError = MoQPublishError::TOO_FAR_BEHIND;
  auto receiver = std::make_shared<StreamingObjectReceiver>(
      StreamingObjectReceiver::SUBSCRIBE, callback);

  auto result =
      receiver->datagram(ObjectHeader(1, 0, 3, 4, 4), makePayload("data"));

  ASSERT_TRUE(result.hasError());
  ASSERT_EQ(callback->consumers.size(), 1);
  const std::vector<ResetStreamErrorCode> expectedErrors{
      ResetStreamErrorCode::TOO_FAR_BEHIND};
  EXPECT_EQ(callback->consumers[0]->errors, expectedErrors);
  EXPECT_TRUE(callback->errors.empty());

  callback->nextPayloadError.reset();
  EXPECT_TRUE(
      receiver->datagram(ObjectHeader(1, 0, 4, 4, 4), makePayload("more"))
          .hasValue());
  auto subgroup = beginSubgroup(receiver, 2, 0);
  ASSERT_NE(subgroup, nullptr);
  PublishDone done;
  done.requestID = RequestID(1);
  done.statusCode = PublishDoneStatusCode::SUBSCRIPTION_ENDED;
  ASSERT_TRUE(receiver->publishDone(std::move(done)).hasValue());
  ASSERT_TRUE(subgroup->endOfSubgroup().hasValue());
  EXPECT_EQ(callback->contexts.size(), 2);
  EXPECT_EQ(callback->publishDoneCount, 1);
  EXPECT_EQ(callback->allDataReceivedCount, 1);
}

TEST(StreamingObjectReceiverTest, RejectedDatagramDoesNotReportSubgroupError) {
  auto callback = std::make_shared<RecordingReceiverCallback>();
  callback->nextObjectBeginError = MoQPublishError::TOO_FAR_BEHIND;
  auto receiver = std::make_shared<StreamingObjectReceiver>(
      StreamingObjectReceiver::SUBSCRIBE, callback);

  auto result =
      receiver->datagram(ObjectHeader(1, 0, 3, 4, 4), makePayload("data"));

  ASSERT_TRUE(result.hasError());
  EXPECT_EQ(result.error().code, MoQPublishError::TOO_FAR_BEHIND);
  EXPECT_TRUE(callback->errors.empty());
  callback->nextObjectBeginError.reset();
  EXPECT_TRUE(
      receiver->datagram(ObjectHeader(1, 0, 4, 4, 4), makePayload("more"))
          .hasValue());
  EXPECT_EQ(callback->consumers.size(), 1);
}

TEST(StreamingObjectReceiverTest, SubgroupCompletionCarriesGroupBoundary) {
  auto callback = std::make_shared<RecordingReceiverCallback>();
  auto receiver = std::make_shared<StreamingObjectReceiver>(
      StreamingObjectReceiver::SUBSCRIBE, callback);
  BeginSubgroupOptions options;
  options.containsLastInGroup = true;
  auto subgroup = beginSubgroup(receiver, 1, 2, options);
  ASSERT_NE(subgroup, nullptr);

  ASSERT_TRUE(subgroup->beginObject(3, 4, makePayload("ab"), {}).hasValue());
  auto result = subgroup->objectPayload(makePayload("cd"), true);

  ASSERT_TRUE(result.hasValue());
  ASSERT_EQ(callback->consumers.size(), 1);
  const auto& metadata = callback->consumers[0]->chunks.back().metadata;
  EXPECT_TRUE(metadata.endOfObject);
  EXPECT_TRUE(metadata.endOfSubgroup);
  EXPECT_EQ(metadata.lastInGroup, true);
}

TEST(StreamingObjectReceiverTest, FinWithFinalBytesStillReportsEndOfStream) {
  auto callback = std::make_shared<RecordingReceiverCallback>();
  auto receiver = std::make_shared<StreamingObjectReceiver>(
      StreamingObjectReceiver::SUBSCRIBE, callback);
  auto subgroup = beginSubgroup(receiver, 1, 2);
  ASSERT_NE(subgroup, nullptr);
  ASSERT_TRUE(subgroup->beginObject(3, 4, makePayload("ab"), {}).hasValue());
  auto consumer = callback->consumers[0];
  size_t chunksAtEndOfStream = 0;
  callback->onEndOfStreamAction = [&chunksAtEndOfStream, consumer] {
    chunksAtEndOfStream = consumer->chunks.size();
  };

  ASSERT_TRUE(subgroup->objectPayload(makePayload("cd"), true).hasValue());
  ASSERT_TRUE(subgroup->endOfSubgroup().hasValue());

  EXPECT_EQ(chunksAtEndOfStream, 2);
  ASSERT_EQ(callback->endOfStreams.size(), 1);
  EXPECT_EQ(callback->endOfStreams[0].groupID, 1);
  EXPECT_EQ(callback->endOfStreams[0].subgroupID, 2);
  EXPECT_TRUE(callback->errors.empty());
}

TEST(StreamingObjectReceiverTest, ReentrantResetDuringStreamingBeginIsFinal) {
  auto callback = std::make_shared<RecordingReceiverCallback>();
  auto receiver = std::make_shared<StreamingObjectReceiver>(
      StreamingObjectReceiver::SUBSCRIBE, callback);
  auto subgroup = beginSubgroup(receiver, 1, 2);
  ASSERT_NE(subgroup, nullptr);
  callback->onObjectBeginAction = [subgroup] {
    subgroup->reset(ResetStreamErrorCode::CANCELLED);
  };

  auto result = subgroup->beginObject(3, 4, makePayload("data"), {});

  ASSERT_TRUE(result.hasError());
  ASSERT_EQ(callback->consumers.size(), 1);
  EXPECT_TRUE(callback->consumers[0]->chunks.empty());
  ASSERT_EQ(callback->consumers[0]->errors.size(), 1);
  EXPECT_EQ(callback->consumers[0]->errors[0], ResetStreamErrorCode::CANCELLED);
  ASSERT_EQ(callback->errors.size(), 1);
  EXPECT_TRUE(callback->endOfStreams.empty());
}

TEST(StreamingObjectReceiverTest, ReentrantResetDuringOneShotBeginIsFinal) {
  auto callback = std::make_shared<RecordingReceiverCallback>();
  auto receiver = std::make_shared<StreamingObjectReceiver>(
      StreamingObjectReceiver::SUBSCRIBE, callback);
  auto subgroup = beginSubgroup(receiver, 1, 2);
  ASSERT_NE(subgroup, nullptr);
  callback->onObjectBeginAction = [subgroup] {
    subgroup->reset(ResetStreamErrorCode::CANCELLED);
  };

  auto result = subgroup->object(3, makePayload("data"), {}, true);

  ASSERT_TRUE(result.hasError());
  ASSERT_EQ(callback->consumers.size(), 1);
  EXPECT_TRUE(callback->consumers[0]->chunks.empty());
  ASSERT_EQ(callback->consumers[0]->errors.size(), 1);
  EXPECT_EQ(callback->consumers[0]->errors[0], ResetStreamErrorCode::CANCELLED);
  ASSERT_EQ(callback->errors.size(), 1);
  EXPECT_EQ(callback->allDataReceivedCount, 0);
}

TEST(StreamingObjectReceiverTest, ReentrantResetDuringOneShotPayloadIsFinal) {
  auto callback = std::make_shared<RecordingReceiverCallback>();
  auto receiver = std::make_shared<StreamingObjectReceiver>(
      StreamingObjectReceiver::SUBSCRIBE, callback);
  auto subgroup = beginSubgroup(receiver, 1, 2);
  ASSERT_NE(subgroup, nullptr);
  callback->nextOnPayloadAction = [subgroup] {
    subgroup->reset(ResetStreamErrorCode::CANCELLED);
  };

  auto result = subgroup->object(3, makePayload("data"), {}, true);

  ASSERT_TRUE(result.hasError());
  ASSERT_EQ(callback->consumers.size(), 1);
  EXPECT_EQ(callback->consumers[0]->chunks.size(), 1);
  ASSERT_EQ(callback->consumers[0]->errors.size(), 1);
  EXPECT_EQ(callback->consumers[0]->errors[0], ResetStreamErrorCode::CANCELLED);
  ASSERT_EQ(callback->errors.size(), 1);
  EXPECT_TRUE(callback->endOfStreams.empty());
  EXPECT_EQ(callback->allDataReceivedCount, 0);

  EXPECT_TRUE(subgroup->object(4, makePayload("late"), {}).hasError());
  EXPECT_EQ(callback->contexts.size(), 1);
  EXPECT_EQ(callback->consumers[0]->errors.size(), 1);
}

TEST(
    StreamingObjectReceiverTest,
    ReentrantResetDuringStreamingInitialPayloadIsFinal) {
  auto callback = std::make_shared<RecordingReceiverCallback>();
  auto receiver = std::make_shared<StreamingObjectReceiver>(
      StreamingObjectReceiver::SUBSCRIBE, callback);
  auto subgroup = beginSubgroup(receiver, 1, 2);
  ASSERT_NE(subgroup, nullptr);
  callback->nextOnPayloadAction = [subgroup] {
    subgroup->reset(ResetStreamErrorCode::CANCELLED);
  };

  auto result = subgroup->beginObject(3, 6, makePayload("abc"), {});

  ASSERT_TRUE(result.hasError());
  ASSERT_EQ(callback->consumers.size(), 1);
  EXPECT_EQ(callback->consumers[0]->chunks.size(), 1);
  ASSERT_EQ(callback->consumers[0]->errors.size(), 1);
  EXPECT_EQ(callback->consumers[0]->errors[0], ResetStreamErrorCode::CANCELLED);
  ASSERT_EQ(callback->errors.size(), 1);

  EXPECT_TRUE(subgroup->objectPayload(makePayload("def"), false).hasError());
  EXPECT_EQ(callback->consumers[0]->chunks.size(), 1);
  EXPECT_EQ(callback->consumers[0]->errors.size(), 1);
}

TEST(
    StreamingObjectReceiverTest,
    ReentrantResetDuringStreamingFinalPayloadIsFinal) {
  auto callback = std::make_shared<RecordingReceiverCallback>();
  auto receiver = std::make_shared<StreamingObjectReceiver>(
      StreamingObjectReceiver::SUBSCRIBE, callback);
  auto subgroup = beginSubgroup(receiver, 1, 2);
  ASSERT_NE(subgroup, nullptr);
  ASSERT_TRUE(subgroup->beginObject(3, 6, makePayload("abc"), {}).hasValue());
  ASSERT_EQ(callback->consumers.size(), 1);
  auto consumer = callback->consumers[0];
  consumer->onPayloadAction = [subgroup] {
    subgroup->reset(ResetStreamErrorCode::CANCELLED);
  };

  auto result = subgroup->objectPayload(makePayload("def"), true);

  ASSERT_TRUE(result.hasError());
  ASSERT_EQ(consumer->chunks.size(), 2);
  EXPECT_TRUE(consumer->chunks.back().metadata.endOfObject);
  ASSERT_EQ(consumer->errors.size(), 1);
  EXPECT_EQ(consumer->errors[0], ResetStreamErrorCode::CANCELLED);
  ASSERT_EQ(callback->errors.size(), 1);
  EXPECT_TRUE(callback->endOfStreams.empty());
  EXPECT_EQ(callback->allDataReceivedCount, 0);

  EXPECT_TRUE(subgroup->objectPayload(makePayload("late"), false).hasError());
  EXPECT_EQ(consumer->chunks.size(), 2);
  EXPECT_EQ(consumer->errors.size(), 1);
}

TEST(StreamingObjectReceiverTest, SubgroupRejectsNonIncreasingLocations) {
  auto callback = std::make_shared<RecordingReceiverCallback>();
  auto receiver = std::make_shared<StreamingObjectReceiver>(
      StreamingObjectReceiver::SUBSCRIBE, callback);
  auto duplicate = beginSubgroup(receiver, 1, 2);
  ASSERT_NE(duplicate, nullptr);
  ASSERT_TRUE(duplicate->object(3, makePayload("a"), {}, false).hasValue());

  auto duplicateResult = duplicate->object(3, makePayload("b"), {}, false);

  ASSERT_TRUE(duplicateResult.hasError());
  EXPECT_EQ(callback->contexts.size(), 1);
  EXPECT_EQ(callback->errors.size(), 1);

  auto decreasing = beginSubgroup(receiver, 4, 5);
  ASSERT_NE(decreasing, nullptr);
  ASSERT_TRUE(decreasing->object(9, makePayload("a"), {}, false).hasValue());
  auto decreasingResult = decreasing->endOfGroup(8);
  ASSERT_TRUE(decreasingResult.hasError());
  EXPECT_EQ(callback->statuses.size(), 0);
  EXPECT_EQ(callback->errors.size(), 2);
}

TEST(StreamingObjectReceiverTest, FetchRejectsNonIncreasingLocations) {
  auto callback = std::make_shared<RecordingReceiverCallback>();
  auto receiver = std::make_shared<StreamingObjectReceiver>(
      StreamingObjectReceiver::FETCH, callback);
  ASSERT_TRUE(
      receiver->object(2, 1, 3, makePayload("a"), {}, false).hasValue());

  auto result = receiver->object(1, 2, 4, makePayload("b"), {}, false);

  ASSERT_TRUE(result.hasError());
  EXPECT_EQ(callback->contexts.size(), 1);
  ASSERT_EQ(callback->errors.size(), 1);
  EXPECT_EQ(callback->errors[0].context.groupID, 1);
  EXPECT_EQ(callback->errors[0].context.subgroupID, 2);
  EXPECT_EQ(callback->allDataReceivedCount, 0);
}

TEST(StreamingObjectReceiverTest, NewestFirstFetchAcceptsDescendingGroups) {
  auto callback = std::make_shared<RecordingReceiverCallback>();
  auto receiver = std::make_shared<StreamingObjectReceiver>(
      StreamingObjectReceiver::FETCH, callback, GroupOrder::NewestFirst);
  ASSERT_TRUE(
      receiver->object(5, 0, 1, makePayload("a"), {}, false).hasValue());
  ASSERT_TRUE(
      receiver->object(5, 0, 2, makePayload("b"), {}, false).hasValue());
  ASSERT_TRUE(
      receiver->object(3, 0, 0, makePayload("c"), {}, false).hasValue());

  auto result = receiver->endOfGroup(3, 0, 1, true);

  ASSERT_TRUE(result.hasValue());
  EXPECT_EQ(callback->contexts.size(), 3);
  EXPECT_TRUE(callback->errors.empty());
  EXPECT_EQ(callback->allDataReceivedCount, 1);
}

TEST(StreamingObjectReceiverTest, NewestFirstFetchRejectsAscendingGroups) {
  auto callback = std::make_shared<RecordingReceiverCallback>();
  auto receiver = std::make_shared<StreamingObjectReceiver>(
      StreamingObjectReceiver::FETCH, callback, GroupOrder::NewestFirst);
  ASSERT_TRUE(
      receiver->object(3, 0, 1, makePayload("a"), {}, false).hasValue());

  auto result = receiver->object(5, 0, 0, makePayload("b"), {}, false);

  ASSERT_TRUE(result.hasError());
  EXPECT_EQ(callback->contexts.size(), 1);
  ASSERT_EQ(callback->errors.size(), 1);
  EXPECT_EQ(callback->errors[0].context.groupID, 5);
  EXPECT_EQ(callback->allDataReceivedCount, 0);
}

TEST(StreamingObjectReceiverTest, DefaultOrderFetchRejectsDescendingGroups) {
  auto callback = std::make_shared<RecordingReceiverCallback>();
  auto receiver = std::make_shared<StreamingObjectReceiver>(
      StreamingObjectReceiver::FETCH, callback, GroupOrder::Default);
  ASSERT_TRUE(
      receiver->object(5, 0, 1, makePayload("a"), {}, false).hasValue());

  auto result = receiver->object(3, 0, 0, makePayload("b"), {}, false);

  ASSERT_TRUE(result.hasError());
  EXPECT_EQ(callback->contexts.size(), 1);
  EXPECT_EQ(callback->errors.size(), 1);
}

TEST(StreamingObjectReceiverTest, FetchRejectsDuplicateStatusLocation) {
  auto callback = std::make_shared<RecordingReceiverCallback>();
  auto receiver = std::make_shared<StreamingObjectReceiver>(
      StreamingObjectReceiver::FETCH, callback);
  ASSERT_TRUE(
      receiver->object(1, 2, 3, makePayload("a"), {}, false).hasValue());

  auto result = receiver->endOfGroup(1, 2, 3, true);

  ASSERT_TRUE(result.hasError());
  EXPECT_TRUE(callback->statuses.empty());
  ASSERT_EQ(callback->errors.size(), 1);
  EXPECT_EQ(callback->allDataReceivedCount, 0);
}

TEST(StreamingObjectReceiverTest, EndOfGroupFailsActiveFetchObject) {
  expectMalformedFetchTerminal(
      [](const auto& receiver) { return receiver->endOfGroup(1, 2, 4, true); });
}

TEST(StreamingObjectReceiverTest, EndOfTrackFailsActiveFetchObject) {
  expectMalformedFetchTerminal([](const auto& receiver) {
    return receiver->endOfTrackAndGroup(1, 2, 4);
  });
}

TEST(StreamingObjectReceiverTest, EndOfFetchFailsActiveFetchObject) {
  expectMalformedFetchTerminal(
      [](const auto& receiver) { return receiver->endOfFetch(); });
}

TEST(StreamingObjectReceiverTest, UnknownRangeFailsActiveFetchObject) {
  expectMalformedFetchTerminal([](const auto& receiver) {
    return receiver->endOfUnknownRange(1, 4, true);
  });
}

TEST(StreamingObjectReceiverTest, FinalOneShotFailsActiveFetchObject) {
  expectMalformedFetchTerminal([](const auto& receiver) {
    return receiver->object(1, 2, 4, makePayload("b"), {}, true);
  });
}

TEST(StreamingObjectReceiverTest, SubgroupErrorsRetainIndependentIdentity) {
  auto callback = std::make_shared<RecordingReceiverCallback>();
  auto receiver = std::make_shared<StreamingObjectReceiver>(
      StreamingObjectReceiver::SUBSCRIBE, callback);
  auto first = beginSubgroup(receiver, 1, 10);
  auto second = beginSubgroup(receiver, 2, 20);
  ASSERT_NE(first, nullptr);
  ASSERT_NE(second, nullptr);

  first->reset(ResetStreamErrorCode::CANCELLED);
  second->reset(ResetStreamErrorCode::INTERNAL_ERROR);

  ASSERT_EQ(callback->errors.size(), 2);
  EXPECT_EQ(callback->errors[0].context.groupID, 1);
  EXPECT_EQ(callback->errors[0].context.subgroupID, 10);
  EXPECT_EQ(callback->errors[1].context.groupID, 2);
  EXPECT_EQ(callback->errors[1].context.subgroupID, 20);
}

TEST(StreamingObjectReceiverTest, UnknownRangeRetainsFetchIdentity) {
  auto callback = std::make_shared<RecordingReceiverCallback>();
  auto receiver = std::make_shared<StreamingObjectReceiver>(
      StreamingObjectReceiver::FETCH, callback);
  ASSERT_TRUE(receiver->setTrackAlias(TrackAlias(7)).hasValue());

  auto result = receiver->endOfUnknownRange(4, 9, true);

  ASSERT_TRUE(result.hasValue());
  ASSERT_EQ(callback->unknownRanges.size(), 1);
  ASSERT_TRUE(callback->unknownRanges[0].trackAlias.has_value());
  EXPECT_EQ(callback->unknownRanges[0].trackAlias->value, 7);
  EXPECT_EQ(callback->unknownRanges[0].groupID, 4);
  EXPECT_EQ(callback->unknownRanges[0].objectID, 9);
  EXPECT_EQ(callback->allDataReceivedCount, 1);
}

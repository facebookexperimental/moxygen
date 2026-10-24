/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <folly/portability/GMock.h>
#include <folly/portability/GTest.h>
#include <moxygen/ObjectReceiver.h>

using namespace moxygen;
using namespace testing;

namespace {

class MockObjectReceiverCallback : public ObjectReceiverCallback {
 public:
  MOCK_METHOD(
      FlowControlState,
      onObject,
      (std::optional<TrackAlias>, const ObjectHeader&, Payload),
      (override));
  MOCK_METHOD(
      void,
      onObjectStatus,
      (std::optional<TrackAlias>, const ObjectHeader&),
      (override));
  MOCK_METHOD(void, onEndOfStream, (), (override));
  MOCK_METHOD(void, onError, (ResetStreamErrorCode), (override));
  MOCK_METHOD(void, onPublishDone, (PublishDone), (override));
  MOCK_METHOD(void, onAllDataReceived, (), (override));
  MOCK_METHOD(void, onGoaway, (Goaway), (override));
};

Payload makePayload(const std::string& str) {
  return folly::IOBuf::copyBuffer(str);
}

// Stands in for the application's fill consumer, which every fill on the
// subscription shares.
class CountingFillConsumer : public FetchConsumer {
 public:
  size_t objects{0};
  size_t terminals{0};

  folly::Expected<folly::Unit, MoQPublishError> object(
      uint64_t,
      uint64_t,
      uint64_t,
      Payload,
      Extensions = noExtensions(),
      bool finFetch = false,
      bool = false) override {
    ++objects;
    if (finFetch) {
      ++terminals;
    }
    return folly::unit;
  }

  folly::Expected<folly::Unit, MoQPublishError> beginObject(
      uint64_t,
      uint64_t,
      uint64_t,
      uint64_t,
      Payload,
      Extensions = noExtensions()) override {
    return folly::unit;
  }

  folly::Expected<ObjectPublishStatus, MoQPublishError> objectPayload(
      Payload,
      bool = false) override {
    return ObjectPublishStatus::DONE;
  }

  folly::Expected<folly::Unit, MoQPublishError>
  endOfGroup(uint64_t, uint64_t, uint64_t, bool finFetch = false) override {
    if (finFetch) {
      ++terminals;
    }
    return folly::unit;
  }

  folly::Expected<folly::Unit, MoQPublishError>
  endOfTrackAndGroup(uint64_t, uint64_t, uint64_t) override {
    ++terminals;
    return folly::unit;
  }

  folly::Expected<folly::Unit, MoQPublishError> endOfFetch() override {
    ++terminals;
    return folly::unit;
  }

  void reset(ResetStreamErrorCode) override {
    ++terminals;
  }

  folly::Expected<folly::SemiFuture<uint64_t>, MoQPublishError>
  awaitReadyToConsume() override {
    return folly::makeSemiFuture<uint64_t>(0);
  }
};

} // namespace

class ObjectReceiverTest : public Test {
 protected:
  void SetUp() override {
    callback_ = std::make_shared<MockObjectReceiverCallback>();
  }

  std::shared_ptr<MockObjectReceiverCallback> callback_;
};

// One implementation serves both the TrackConsumer and FetchConsumer bases --
// Type only decides whether fetchPublisher_ is built -- so SUBSCRIBE covers
// the FETCH path too.
TEST_F(ObjectReceiverTest, GoawayForwardedToCallback) {
  auto receiver = std::make_shared<ObjectReceiver>(
      ObjectReceiver::Type::SUBSCRIBE, callback_);

  Goaway goaway;
  goaway.newSessionUri = "moqt://relay-b.example/path";

  EXPECT_CALL(
      *callback_,
      onGoaway(Field(&Goaway::newSessionUri, "moqt://relay-b.example/path")));

  receiver->goaway(goaway);
}

TEST_F(ObjectReceiverTest, PublishDoneDelivery) {
  auto receiver = std::make_shared<ObjectReceiver>(
      ObjectReceiver::Type::SUBSCRIBE, callback_);

  PublishDone done;
  done.requestID = RequestID(1);
  done.statusCode = PublishDoneStatusCode::SUBSCRIPTION_ENDED;

  EXPECT_CALL(*callback_, onPublishDone(_)).Times(1);
  // onAllDataReceived should be called since no subgroups are open
  EXPECT_CALL(*callback_, onAllDataReceived()).Times(1);

  auto result = receiver->publishDone(std::move(done));
  EXPECT_TRUE(result.hasValue());
}

TEST_F(ObjectReceiverTest, AllDataReceivedAfterSubgroupClose) {
  auto receiver = std::make_shared<ObjectReceiver>(
      ObjectReceiver::Type::SUBSCRIBE, callback_);
  receiver->setTrackAlias(TrackAlias(1));

  // Start a subgroup
  EXPECT_CALL(*callback_, onEndOfStream()).Times(1);
  auto subgroupResult =
      receiver->beginSubgroup(/*groupID=*/0, /*subgroupID=*/0, /*priority=*/0);
  ASSERT_TRUE(subgroupResult.hasValue());
  auto subgroup = *subgroupResult;

  // Deliver publishDone while subgroup is open
  PublishDone done;
  done.requestID = RequestID(1);
  done.statusCode = PublishDoneStatusCode::SUBSCRIPTION_ENDED;

  EXPECT_CALL(*callback_, onPublishDone(_)).Times(1);
  // onAllDataReceived should NOT be called yet because subgroup is open
  EXPECT_CALL(*callback_, onAllDataReceived()).Times(0);

  auto pubDoneResult = receiver->publishDone(std::move(done));
  EXPECT_TRUE(pubDoneResult.hasValue());

  // Now close the subgroup - onAllDataReceived should fire
  EXPECT_CALL(*callback_, onAllDataReceived()).Times(1);
  auto endResult = subgroup->endOfSubgroup();
  EXPECT_TRUE(endResult.hasValue());
}

// A streamed object whose final payload chunk carries finSubgroup closes the
// subgroup, same as an explicit endOfSubgroup().
TEST_F(ObjectReceiverTest, AllDataReceivedAfterObjectPayloadFinSubgroup) {
  auto receiver = std::make_shared<ObjectReceiver>(
      ObjectReceiver::Type::SUBSCRIBE, callback_);
  receiver->setTrackAlias(TrackAlias(1));

  auto subgroupResult =
      receiver->beginSubgroup(/*groupID=*/0, /*subgroupID=*/0, /*priority=*/0);
  ASSERT_TRUE(subgroupResult.hasValue());
  auto subgroup = *subgroupResult;

  PublishDone done;
  done.requestID = RequestID(1);
  done.statusCode = PublishDoneStatusCode::SUBSCRIPTION_ENDED;
  EXPECT_CALL(*callback_, onPublishDone(_)).Times(1);
  EXPECT_CALL(*callback_, onAllDataReceived()).Times(0);
  EXPECT_TRUE(receiver->publishDone(std::move(done)).hasValue());

  // Deliver a 6 byte object in two chunks; only the second completes it.
  EXPECT_CALL(*callback_, onObject(_, _, _))
      .WillOnce(Return(ObjectReceiverCallback::FlowControlState::UNBLOCKED));
  EXPECT_TRUE(subgroup
                  ->beginObject(
                      /*objectID=*/0,
                      /*length=*/6,
                      makePayload("abc"),
                      noExtensions())
                  .hasValue());

  EXPECT_CALL(*callback_, onAllDataReceived()).Times(1);
  auto res = subgroup->objectPayload(makePayload("def"), /*finSubgroup=*/true);
  ASSERT_TRUE(res.hasValue());
  EXPECT_EQ(res.value(), ObjectPublishStatus::DONE);
}

// A status datagram has no payload, so it belongs on onObjectStatus like every
// other status object, not on onObject.
TEST_F(ObjectReceiverTest, StatusDatagramGoesToOnObjectStatus) {
  auto receiver = std::make_shared<ObjectReceiver>(
      ObjectReceiver::Type::SUBSCRIBE, callback_);
  receiver->setTrackAlias(TrackAlias(1));

  EXPECT_CALL(*callback_, onObject(_, _, _))
      .WillOnce(Return(ObjectReceiverCallback::FlowControlState::UNBLOCKED));
  EXPECT_TRUE(receiver
                  ->datagram(
                      ObjectHeader(0, 0, 0, 0, 3),
                      makePayload("abc"),
                      /*lastInGroup=*/false)
                  .hasValue());
  Mock::VerifyAndClearExpectations(callback_.get());

  EXPECT_CALL(*callback_, onObject(_, _, _)).Times(0);
  EXPECT_CALL(*callback_, onObjectStatus(_, _)).Times(1);
  EXPECT_TRUE(receiver
                  ->datagram(
                      ObjectHeader(0, 0, 1, 0, ObjectStatus::END_OF_GROUP),
                      nullptr,
                      /*lastInGroup=*/false)
                  .hasValue());
}

// FETCH terminals.  A publisher ends a fetch with exactly one of these, and
// each has to produce exactly one onAllDataReceived.
TEST_F(ObjectReceiverTest, FetchEndOfFetchIsTerminal) {
  auto receiver =
      std::make_shared<ObjectReceiver>(ObjectReceiver::Type::FETCH, callback_);

  EXPECT_CALL(*callback_, onEndOfStream()).Times(1);
  EXPECT_CALL(*callback_, onAllDataReceived()).Times(1);
  EXPECT_TRUE(receiver->endOfFetch().hasValue());
}

TEST_F(ObjectReceiverTest, FetchObjectWithFinIsTerminal) {
  auto receiver =
      std::make_shared<ObjectReceiver>(ObjectReceiver::Type::FETCH, callback_);

  // A non-final object must not terminate the fetch.
  EXPECT_CALL(*callback_, onObject(_, _, _))
      .WillOnce(Return(ObjectReceiverCallback::FlowControlState::UNBLOCKED));
  EXPECT_CALL(*callback_, onAllDataReceived()).Times(0);
  EXPECT_TRUE(receiver
                  ->object(
                      /*groupID=*/0,
                      /*subgroupID=*/0,
                      /*objectID=*/0,
                      makePayload("abc"),
                      noExtensions(),
                      /*finFetch=*/false)
                  .hasValue());
  Mock::VerifyAndClearExpectations(callback_.get());

  EXPECT_CALL(*callback_, onObject(_, _, _))
      .WillOnce(Return(ObjectReceiverCallback::FlowControlState::UNBLOCKED));
  EXPECT_CALL(*callback_, onAllDataReceived()).Times(1);
  EXPECT_TRUE(receiver
                  ->object(
                      /*groupID=*/0,
                      /*subgroupID=*/0,
                      /*objectID=*/1,
                      makePayload("def"),
                      noExtensions(),
                      /*finFetch=*/true)
                  .hasValue());
}

TEST_F(ObjectReceiverTest, FetchObjectPayloadWithFinIsTerminal) {
  auto receiver =
      std::make_shared<ObjectReceiver>(ObjectReceiver::Type::FETCH, callback_);

  EXPECT_CALL(*callback_, onObject(_, _, _))
      .WillOnce(Return(ObjectReceiverCallback::FlowControlState::UNBLOCKED));
  EXPECT_TRUE(receiver
                  ->beginObject(
                      /*groupID=*/0,
                      /*subgroupID=*/0,
                      /*objectID=*/0,
                      /*length=*/6,
                      makePayload("abc"),
                      noExtensions())
                  .hasValue());

  EXPECT_CALL(*callback_, onAllDataReceived()).Times(1);
  auto res = receiver->objectPayload(makePayload("def"), /*finSubgroup=*/true);
  ASSERT_TRUE(res.hasValue());
  EXPECT_EQ(res.value(), ObjectPublishStatus::DONE);
}

TEST_F(ObjectReceiverTest, FetchEndOfGroupWithFinIsTerminal) {
  auto receiver =
      std::make_shared<ObjectReceiver>(ObjectReceiver::Type::FETCH, callback_);

  // End of a group that isn't the end of the fetch.
  EXPECT_CALL(*callback_, onObjectStatus(_, _)).Times(1);
  EXPECT_CALL(*callback_, onAllDataReceived()).Times(0);
  EXPECT_TRUE(receiver
                  ->endOfGroup(
                      /*groupID=*/0,
                      /*subgroupID=*/0,
                      /*objectID=*/2,
                      /*finFetch=*/false)
                  .hasValue());
  Mock::VerifyAndClearExpectations(callback_.get());

  EXPECT_CALL(*callback_, onObjectStatus(_, _)).Times(1);
  EXPECT_CALL(*callback_, onAllDataReceived()).Times(1);
  EXPECT_TRUE(receiver
                  ->endOfGroup(
                      /*groupID=*/1,
                      /*subgroupID=*/0,
                      /*objectID=*/2,
                      /*finFetch=*/true)
                  .hasValue());
}

TEST_F(ObjectReceiverTest, FetchEndOfTrackAndGroupIsTerminal) {
  auto receiver =
      std::make_shared<ObjectReceiver>(ObjectReceiver::Type::FETCH, callback_);

  EXPECT_CALL(*callback_, onObjectStatus(_, _)).Times(1);
  EXPECT_CALL(*callback_, onAllDataReceived()).Times(1);
  EXPECT_TRUE(receiver
                  ->endOfTrackAndGroup(
                      /*groupID=*/0, /*subgroupID=*/0, /*objectID=*/2)
                  .hasValue());
}

TEST_F(ObjectReceiverTest, FetchEndOfUnknownRangeWithFinIsTerminal) {
  auto receiver =
      std::make_shared<ObjectReceiver>(ObjectReceiver::Type::FETCH, callback_);

  // An unknown range in the middle of a fetch isn't the end of it.
  EXPECT_CALL(*callback_, onAllDataReceived()).Times(0);
  EXPECT_TRUE(receiver
                  ->endOfUnknownRange(
                      /*groupID=*/0, /*objectID=*/2, /*finFetch=*/false)
                  .hasValue());
  Mock::VerifyAndClearExpectations(callback_.get());

  EXPECT_CALL(*callback_, onAllDataReceived()).Times(1);
  EXPECT_TRUE(receiver
                  ->endOfUnknownRange(
                      /*groupID=*/1, /*objectID=*/2, /*finFetch=*/true)
                  .hasValue());
}

// A publisher that sends two terminals still gets one onAllDataReceived.
TEST_F(ObjectReceiverTest, FetchTerminalIsDeliveredOnce) {
  auto receiver =
      std::make_shared<ObjectReceiver>(ObjectReceiver::Type::FETCH, callback_);

  EXPECT_CALL(*callback_, onObject(_, _, _))
      .WillOnce(Return(ObjectReceiverCallback::FlowControlState::UNBLOCKED));
  EXPECT_CALL(*callback_, onEndOfStream()).Times(1);
  EXPECT_CALL(*callback_, onAllDataReceived()).Times(1);

  EXPECT_TRUE(receiver
                  ->object(
                      /*groupID=*/0,
                      /*subgroupID=*/0,
                      /*objectID=*/0,
                      makePayload("abc"),
                      noExtensions(),
                      /*finFetch=*/true)
                  .hasValue());
  EXPECT_TRUE(receiver->endOfFetch().hasValue());
}

// BLOCKED still means the object arrived, so the fetch is over.
TEST_F(ObjectReceiverTest, FetchObjectPayloadIsTerminalWhenBlocked) {
  auto receiver =
      std::make_shared<ObjectReceiver>(ObjectReceiver::Type::FETCH, callback_);

  EXPECT_CALL(*callback_, onObject(_, _, _))
      .WillOnce(Return(ObjectReceiverCallback::FlowControlState::BLOCKED));
  EXPECT_TRUE(receiver
                  ->beginObject(
                      /*groupID=*/0,
                      /*subgroupID=*/0,
                      /*objectID=*/0,
                      /*length=*/6,
                      makePayload("abc"),
                      noExtensions())
                  .hasValue());

  EXPECT_CALL(*callback_, onAllDataReceived()).Times(1);
  auto res = receiver->objectPayload(makePayload("def"), /*finSubgroup=*/true);
  ASSERT_TRUE(res.hasError());
  EXPECT_EQ(res.error().code, MoQPublishError::BLOCKED);
}

// Two fills each get their own consumer; all data is reported only after both
// end.
TEST_F(ObjectReceiverTest, AllDataReceivedWaitsForEveryFill) {
  auto receiver = std::make_shared<ObjectReceiver>(
      ObjectReceiver::Type::SUBSCRIBE, callback_);
  receiver->setTrackAlias(TrackAlias(1));
  std::vector<std::shared_ptr<CountingFillConsumer>> fillConsumers;
  receiver->setFillReceiverFactory([&fillConsumers](RequestID) {
    return fillConsumers.emplace_back(std::make_shared<CountingFillConsumer>());
  });

  auto firstResult = receiver->beginFill(RequestID(1));
  ASSERT_TRUE(firstResult.hasValue());
  auto second = receiver->beginFill(RequestID(2));
  ASSERT_TRUE(second.hasValue());
  auto first = *firstResult;

  PublishDone done;
  done.requestID = RequestID(1);
  done.statusCode = PublishDoneStatusCode::SUBSCRIPTION_ENDED;
  EXPECT_CALL(*callback_, onPublishDone(_)).Times(1);
  EXPECT_CALL(*callback_, onAllDataReceived()).Times(0);
  EXPECT_TRUE(receiver->publishDone(std::move(done)).hasValue());

  // The first fill ends while the second is still streaming.
  EXPECT_TRUE(first->endOfFetch().hasValue());
  Mock::VerifyAndClearExpectations(callback_.get());

  EXPECT_CALL(*callback_, onAllDataReceived()).Times(1);
  EXPECT_TRUE((*second)
                  ->object(
                      /*groupID=*/4,
                      /*subgroupID=*/0,
                      /*objectID=*/0,
                      makePayload("abc"),
                      noExtensions(),
                      /*finFetch=*/true)
                  .hasValue());

  ASSERT_EQ(fillConsumers.size(), 2);
  EXPECT_EQ(fillConsumers[0]->terminals, 1);
  EXPECT_EQ(fillConsumers[0]->objects, 0);
  EXPECT_EQ(fillConsumers[1]->terminals, 1);
  EXPECT_EQ(fillConsumers[1]->objects, 1);
}

namespace {
// Every way a fill can end.
const std::vector<std::pair<std::string, std::function<void(FetchConsumer&)>>>
    kFillTerminals = {
        {"object",
         [](FetchConsumer& c) {
           c.object(4, 0, 0, makePayload("abc"), noExtensions(), true);
         }},
        {"endOfGroup", [](FetchConsumer& c) { c.endOfGroup(4, 0, 0, true); }},
        {"endOfTrackAndGroup",
         [](FetchConsumer& c) { c.endOfTrackAndGroup(4, 0, 0); }},
        {"endOfFetch", [](FetchConsumer& c) { c.endOfFetch(); }},
        {"reset",
         [](FetchConsumer& c) { c.reset(ResetStreamErrorCode::CANCELLED); }},
        {"objectPayload",
         [](FetchConsumer& c) {
           c.beginObject(4, 0, 0, 3, makePayload("abc"));
           c.objectPayload(makePayload("abc"), /*finSubgroup=*/true);
         }},
        {"dropped", [](FetchConsumer&) {}},
};
} // namespace

// Each terminal must release the fill, or all data is never reported.
TEST_F(ObjectReceiverTest, EveryFillTerminalReleasesTheFill) {
  for (const auto& [name, endFill] : kFillTerminals) {
    SCOPED_TRACE(name);
    auto callback = std::make_shared<NiceMock<MockObjectReceiverCallback>>();
    auto receiver = std::make_shared<ObjectReceiver>(
        ObjectReceiver::Type::SUBSCRIBE, callback);
    receiver->setTrackAlias(TrackAlias(1));
    receiver->setFillReceiverFactory(
        [](RequestID) { return std::make_shared<CountingFillConsumer>(); });

    auto fillResult = receiver->beginFill(RequestID(1));
    ASSERT_TRUE(fillResult.hasValue());
    // Moved out so the "dropped" case really does release the last reference.
    auto fill = std::move(fillResult.value());

    PublishDone done;
    done.requestID = RequestID(1);
    done.statusCode = PublishDoneStatusCode::SUBSCRIPTION_ENDED;
    EXPECT_CALL(*callback, onAllDataReceived()).Times(0);
    EXPECT_TRUE(receiver->publishDone(std::move(done)).hasValue());
    Mock::VerifyAndClearExpectations(callback.get());

    EXPECT_CALL(*callback, onAllDataReceived()).Times(1);
    endFill(*fill);
    // "dropped" ends the fill by releasing the last reference instead.
    fill.reset();
    Mock::VerifyAndClearExpectations(callback.get());
  }
}

// The last chunk completes the object even when the application blocks, so
// the fill still ends.
TEST_F(ObjectReceiverTest, BlockedLastChunkReleasesTheFill) {
  auto receiver = std::make_shared<ObjectReceiver>(
      ObjectReceiver::Type::SUBSCRIBE, callback_);
  receiver->setTrackAlias(TrackAlias(1));
  auto fillCallback = std::make_shared<NiceMock<MockObjectReceiverCallback>>();
  receiver->setFillReceiverFactory([fillCallback](RequestID) {
    return std::make_shared<ObjectReceiver>(
        ObjectReceiver::Type::FETCH, fillCallback);
  });
  auto fillResult = receiver->beginFill(RequestID(1));
  ASSERT_TRUE(fillResult.hasValue());
  auto fill = *fillResult;

  PublishDone done;
  done.requestID = RequestID(1);
  done.statusCode = PublishDoneStatusCode::SUBSCRIPTION_ENDED;
  EXPECT_CALL(*callback_, onPublishDone(_));
  EXPECT_CALL(*callback_, onAllDataReceived()).Times(0);
  EXPECT_TRUE(receiver->publishDone(std::move(done)).hasValue());
  Mock::VerifyAndClearExpectations(callback_.get());

  EXPECT_CALL(*fillCallback, onObject(_, _, _))
      .WillOnce(Return(ObjectReceiverCallback::FlowControlState::BLOCKED));
  EXPECT_CALL(*callback_, onAllDataReceived());
  EXPECT_TRUE(fill->beginObject(4, 0, 0, 6, makePayload("abc")).hasValue());
  auto res = fill->objectPayload(makePayload("def"), /*finSubgroup=*/true);
  ASSERT_TRUE(res.hasError());
  EXPECT_EQ(res.error().code, MoQPublishError::BLOCKED);
}

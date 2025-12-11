/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
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
      (folly::Optional<TrackAlias>, const ObjectHeader&, Payload),
      (override));
  MOCK_METHOD(
      void,
      onObjectStatus,
      (folly::Optional<TrackAlias>, const ObjectHeader&),
      (override));
  MOCK_METHOD(void, onEndOfStream, (), (override));
  MOCK_METHOD(void, onError, (ResetStreamErrorCode), (override));
  MOCK_METHOD(void, onSubscribeDone, (SubscribeDone), (override));
  MOCK_METHOD(void, onAllDataReceived, (), (override));
};

Payload makePayload(const std::string& str) {
  return folly::IOBuf::copyBuffer(str);
}

} // namespace

class ObjectReceiverTest : public Test {
 protected:
  void SetUp() override {
    callback_ = std::make_shared<MockObjectReceiverCallback>();
  }

  std::shared_ptr<MockObjectReceiverCallback> callback_;
};

TEST_F(ObjectReceiverTest, SubscribeDoneDelivery) {
  auto receiver = std::make_shared<ObjectReceiver>(
      ObjectReceiver::Type::SUBSCRIBE, callback_);

  SubscribeDone done;
  done.requestID = RequestID(1);
  done.statusCode = SubscribeDoneStatusCode::SUBSCRIPTION_ENDED;

  EXPECT_CALL(*callback_, onSubscribeDone(_)).Times(1);
  // onAllDataReceived should be called since no subgroups are open
  EXPECT_CALL(*callback_, onAllDataReceived()).Times(1);

  auto result = receiver->subscribeDone(std::move(done));
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

  // Deliver subscribeDone while subgroup is open
  SubscribeDone done;
  done.requestID = RequestID(1);
  done.statusCode = SubscribeDoneStatusCode::SUBSCRIPTION_ENDED;

  EXPECT_CALL(*callback_, onSubscribeDone(_)).Times(1);
  // onAllDataReceived should NOT be called yet because subgroup is open
  EXPECT_CALL(*callback_, onAllDataReceived()).Times(0);

  auto subDoneResult = receiver->subscribeDone(std::move(done));
  EXPECT_TRUE(subDoneResult.hasValue());

  // Now close the subgroup - onAllDataReceived should fire
  EXPECT_CALL(*callback_, onAllDataReceived()).Times(1);
  auto endResult = subgroup->endOfSubgroup();
  EXPECT_TRUE(endResult.hasValue());
}

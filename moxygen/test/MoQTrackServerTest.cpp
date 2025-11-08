/*
 *  Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 *  This source code is licensed under the MIT license found in the LICENSE
 *  file in the root directory of this source tree.
 *
 */

#include <folly/portability/GTest.h>
#include "folly/Expected.h"
#include "folly/coro/BlockingWait.h"
#include "moxygen/moqtest/MoQTestServer.h"
#include "moxygen/moqtest/Utils.h"
#include "moxygen/test/Mocks.h"

namespace {

const std::string kDefaultTrackName = "test";
const moxygen::TrackAlias kDefaultTrackAlias = moxygen::TrackAlias(0);

class MoQTrackServerTest : public testing::Test {
 public:
  void CreateDefaultTrackNamespace() {
    track_.trackNamespace = {
        "moq-test-00",
        "0",
        "0",
        "0",
        "10",
        "1",
        "1",
        "1",
        "1",
        "1",
        "1",
        "1",
        "0",
        "0",
        "0",
        "0"};
  }

  void CreateDefaultMoQTestParameters() {
    params_.forwardingPreference = moxygen::ForwardingPreference(0);
    params_.startGroup = 0;
    params_.startObject = 0;
    params_.lastGroupInTrack = 10;
    params_.lastObjectInTrack = 1;
    params_.objectsPerGroup = 1;
    params_.sizeOfObjectZero = 1;
    params_.sizeOfObjectGreaterThanZero = 1;
    params_.objectFrequency = 1;
    params_.groupIncrement = 1;
    params_.objectIncrement = 1;
    params_.sendEndOfGroupMarkers = false;
    params_.testIntegerExtension = false;
    params_.testVariableExtension = false;
    params_.publisherDeliveryTimeout = 0;
  }

  moxygen::MoQTestParameters params_;
  moxygen::TrackNamespace track_;
  moxygen::MoQTestServer server_ = moxygen::MoQTestServer();
};

} // namespace

// Subscription Testing
TEST_F(
    MoQTrackServerTest,
    TestSubscribeFunctionReturnsSubscribeErrorWithInvalidParams) {
  moxygen::SubscribeRequest req;
  MoQTrackServerTest::CreateDefaultTrackNamespace();
  track_.trackNamespace[0] = "invalid";
  req.requestID = 0;
  req.fullTrackName.trackNamespace = track_;
  req.trackAlias = kDefaultTrackAlias;

  // Call the subscribe method
  auto task = server_.subscribe(req, nullptr);

  // Wait for the coroutine to complete and get the result
  auto result = folly::coro::blockingWait(std::move(task));

  // Check that the result is an error
  ASSERT_TRUE(result.hasError());

  // Verify the error details
  const auto& error = result.error();
  EXPECT_EQ(error.requestID, req.requestID);
  EXPECT_EQ(error.errorCode, moxygen::SubscribeErrorCode::NOT_SUPPORTED);
  EXPECT_EQ(error.reasonPhrase, "Invalid Parameters");
}
TEST_F(MoQTrackServerTest, ValidateSubscribeWithForwardPreferenceZero) {
  MoQTrackServerTest::CreateDefaultMoQTestParameters();

  // Create a mock track consumer
  auto mockConsumer = std::make_shared<moxygen::MockTrackConsumer>();

  // Set expectations for beginSubgroup
  for (int groupId = 0; groupId <= 10; groupId++) {
    // Create a mock subgroup consumer
    auto mockSubgroupConsumer =
        std::make_shared<moxygen::MockSubgroupConsumer>();
    EXPECT_CALL(*mockConsumer, beginSubgroup(groupId, 0, testing::_))
        .Times(1)
        .WillRepeatedly(testing::Return(mockSubgroupConsumer));
    for (int objectId = 0; objectId <= params_.lastObjectInTrack; objectId++) {
      // Find Object Size
      int objectSize = moxygen::getObjectSize(objectId, &params_);

      // Set expectations for beginObject
      EXPECT_CALL(
          *mockSubgroupConsumer,
          object(objectId, testing::_, testing::_, testing::_))
          .Times(1)
          .WillOnce([objectSize](
                        auto,
                        std::unique_ptr<folly::IOBuf> payload,
                        const auto&,
                        auto) {
            auto payloadLength = (*payload).length();
            EXPECT_EQ(payloadLength, objectSize);
            return folly::Expected<folly::Unit, moxygen::MoQPublishError>({});
          });
      // .WillOnce(::testing::Return(
      //     folly::Expected<folly::Unit, moxygen::MoQPublishError>({})));
    }
    // Set expectations for endOfSubgroup
    EXPECT_CALL(*mockSubgroupConsumer, endOfSubgroup())
        .Times(1)
        .WillOnce(
            ::testing::Return(
                folly::Expected<folly::Unit, moxygen::MoQPublishError>({})));
  }

  // Call the onSubscribe method
  auto task = server_.sendOneSubgroupPerGroup(params_, mockConsumer);

  // Wait for the coroutine to complete
  folly::coro::blockingWait(std::move(task));
}

TEST_F(
    MoQTrackServerTest,
    ValidateSubscribeWithForwardPreferenceZeroWithExtensions) {
  MoQTrackServerTest::CreateDefaultMoQTestParameters();
  int integerExtension = 1;
  int variableExtension = 1;
  params_.testIntegerExtension = integerExtension;
  params_.testVariableExtension = variableExtension;

  // Create a mock track consumer
  auto mockConsumer = std::make_shared<moxygen::MockTrackConsumer>();

  // Set expectations for beginSubgroup
  for (int groupId = 0; groupId <= 10; groupId++) {
    // Create a mock subgroup consumer
    auto mockSubgroupConsumer =
        std::make_shared<moxygen::MockSubgroupConsumer>();
    EXPECT_CALL(*mockConsumer, beginSubgroup(groupId, 0, testing::_))
        .Times(1)
        .WillRepeatedly(testing::Return(mockSubgroupConsumer));
    for (int objectId = 0; objectId <= params_.lastObjectInTrack; objectId++) {
      // Find Object Size
      int objectSize = moxygen::getObjectSize(objectId, &params_);

      // Set expectations for beginObject
      EXPECT_CALL(
          *mockSubgroupConsumer,
          object(objectId, testing::_, testing::_, testing::_))
          .Times(1)
          .WillOnce([objectSize, variableExtension](
                        auto,
                        std::unique_ptr<folly::IOBuf> payload,
                        auto extensions,
                        auto) {
            // Check Extensions Generate
            EXPECT_EQ(extensions.size(), 2);

            // Check if Integer Extension type is 2*Field
            auto& mutable_exts = extensions.getMutableExtensions();
            EXPECT_EQ(mutable_exts[0].type, 2);

            // Check if Variable Extension type is 2*Field + 1
            EXPECT_EQ(mutable_exts[1].type, 3);

            // Check if Variable Extension is within size range of 1-20
            bool check =
                mutable_exts[1].arrayValue->computeChainDataLength() >= 1 &&
                mutable_exts[1].arrayValue->computeChainDataLength() <= 20;
            EXPECT_TRUE(check);

            // Check Payload
            auto payloadLength = (*payload).length();
            EXPECT_EQ(payloadLength, objectSize);
            return folly::Expected<folly::Unit, moxygen::MoQPublishError>({});
          })
          .WillRepeatedly(
              ::testing::Return(
                  folly::Expected<folly::Unit, moxygen::MoQPublishError>({})));
    }
    // Set expectations for endOfSubgroup
    EXPECT_CALL(*mockSubgroupConsumer, endOfSubgroup())
        .Times(1)
        .WillRepeatedly(
            ::testing::Return(
                folly::Expected<folly::Unit, moxygen::MoQPublishError>({})));
  }

  // Call the onSubscribe method
  auto task = server_.sendOneSubgroupPerGroup(params_, mockConsumer);

  // Wait for the coroutine to complete
  folly::coro::blockingWait(std::move(task));
}

TEST_F(MoQTrackServerTest, ValidateSubscribeWithForwardPreferenceOne) {
  MoQTrackServerTest::CreateDefaultMoQTestParameters();
  params_.forwardingPreference = moxygen::ForwardingPreference(1);

  // Create a mock track consumer
  auto mockConsumer = std::make_shared<moxygen::MockTrackConsumer>();

  // Create a mock subgroup consumer
  auto mockSubgroupConsumer = std::make_shared<moxygen::MockSubgroupConsumer>();

  // Set expectations for beginSubgroup
  for (int groupId = 0; groupId <= 10; groupId++) {
    for (int objectId = 0; objectId <= params_.lastObjectInTrack; objectId++) {
      // Set expectations for beginObject
      int objectSize = moxygen::getObjectSize(objectId, &params_);
      // Create a mock subgroup consumer
      auto mockSubgroupConsumer =
          std::make_shared<moxygen::MockSubgroupConsumer>();
      EXPECT_CALL(*mockConsumer, beginSubgroup(groupId, objectId, testing::_))
          .Times(1)
          .WillOnce(testing::Return(mockSubgroupConsumer));
      EXPECT_CALL(
          *mockSubgroupConsumer,
          object(objectId, testing::_, testing::_, testing::_))
          .Times(1)
          .WillOnce([objectSize](
                        auto,
                        std::unique_ptr<folly::IOBuf> payload,
                        const auto&,
                        auto) {
            auto payloadLength = (*payload).length();
            EXPECT_EQ(payloadLength, objectSize);
            return folly::Expected<folly::Unit, moxygen::MoQPublishError>({});
          })
          .WillOnce(
              ::testing::Return(
                  folly::Expected<folly::Unit, moxygen::MoQPublishError>({})));
      // Set expectations for endOfSubgroup
      EXPECT_CALL(*mockSubgroupConsumer, endOfSubgroup())
          .Times(1)
          .WillOnce(
              ::testing::Return(
                  folly::Expected<folly::Unit, moxygen::MoQPublishError>({})));
    }
  }

  // Call the onSubscribe method
  auto task = server_.sendOneSubgroupPerObject(params_, mockConsumer);

  // Wait for the coroutine to complete
  folly::coro::blockingWait(std::move(task));
}

TEST_F(MoQTrackServerTest, ValidateSubscribeWithForwardPreferenceTwo) {
  MoQTrackServerTest::CreateDefaultMoQTestParameters();
  params_.forwardingPreference = moxygen::ForwardingPreference(2);
  params_.lastObjectInTrack = 2;
  params_.objectsPerGroup = 2;

  // Create a mock track consumer
  auto mockConsumer = std::make_shared<moxygen::MockTrackConsumer>();

  // Set expectations for beginSubgroup
  for (int groupId = 0; groupId <= 10; groupId++) {
    // Create a mock subgroup consumer
    auto mockSubgroupConsumerOne =
        std::make_shared<moxygen::MockSubgroupConsumer>();
    auto mockSubgroupConsumerZero =
        std::make_shared<moxygen::MockSubgroupConsumer>();
    EXPECT_CALL(*mockConsumer, beginSubgroup(groupId, 0, testing::_))
        .Times(1)
        .WillRepeatedly(testing::Return(mockSubgroupConsumerZero));
    EXPECT_CALL(*mockConsumer, beginSubgroup(groupId, 1, testing::_))
        .Times(1)
        .WillRepeatedly(testing::Return(mockSubgroupConsumerOne));

    for (int objectId = 0; objectId <= params_.lastObjectInTrack; objectId++) {
      int objectSize = moxygen::getObjectSize(objectId, &params_);
      // Set expectations for beginObject
      if (objectId % 2 == 0) {
        EXPECT_CALL(
            *mockSubgroupConsumerZero,
            object(objectId, testing::_, testing::_, testing::_))
            .Times(1)
            .WillOnce([objectSize](
                          auto,
                          std::unique_ptr<folly::IOBuf> payload,
                          const auto&,
                          auto) {
              auto payloadLength = (*payload).length();
              EXPECT_EQ(payloadLength, objectSize);
              return folly::Expected<folly::Unit, moxygen::MoQPublishError>({});
            })
            .WillOnce(
                ::testing::Return(
                    folly::Expected<folly::Unit, moxygen::MoQPublishError>(
                        {})));
      } else {
        EXPECT_CALL(
            *mockSubgroupConsumerOne,
            object(objectId, testing::_, testing::_, testing::_))
            .Times(1)
            .WillOnce([objectSize](
                          auto,
                          std::unique_ptr<folly::IOBuf> payload,
                          const auto&,
                          auto) {
              auto payloadLength = (*payload).length();
              EXPECT_EQ(payloadLength, objectSize);
              return folly::Expected<folly::Unit, moxygen::MoQPublishError>({});
            })
            .WillOnce(
                ::testing::Return(
                    folly::Expected<folly::Unit, moxygen::MoQPublishError>(
                        {})));
      }
    }
    // Set expectations for endOfSubgroup
    EXPECT_CALL(*mockSubgroupConsumerZero, endOfSubgroup())
        .Times(1)
        .WillOnce(
            ::testing::Return(
                folly::Expected<folly::Unit, moxygen::MoQPublishError>({})));
    EXPECT_CALL(*mockSubgroupConsumerOne, endOfSubgroup())
        .Times(1)
        .WillOnce(
            ::testing::Return(
                folly::Expected<folly::Unit, moxygen::MoQPublishError>({})));
  }

  // Call the onSubscribe method
  auto task = server_.sendTwoSubgroupsPerGroup(params_, mockConsumer);

  // Wait for the coroutine to complete
  folly::coro::blockingWait(std::move(task));
}

TEST_F(
    MoQTrackServerTest,
    ValidateSubscribeWithForwardPreferenceTwoWithEndOfGroupMarkers) {
  MoQTrackServerTest::CreateDefaultMoQTestParameters();
  params_.forwardingPreference = moxygen::ForwardingPreference(2);
  params_.lastObjectInTrack = 2;
  params_.objectsPerGroup = 2;
  params_.sendEndOfGroupMarkers = true;

  // Create a mock track consumer
  auto mockConsumer = std::make_shared<moxygen::MockTrackConsumer>();

  // Set expectations for beginSubgroup
  for (int groupId = 0; groupId <= 10; groupId++) {
    // Create a mock subgroup consumer
    auto mockSubgroupConsumerOne =
        std::make_shared<moxygen::MockSubgroupConsumer>();
    auto mockSubgroupConsumerZero =
        std::make_shared<moxygen::MockSubgroupConsumer>();
    EXPECT_CALL(*mockConsumer, beginSubgroup(groupId, 0, testing::_))
        .Times(1)
        .WillRepeatedly(testing::Return(mockSubgroupConsumerZero));
    EXPECT_CALL(*mockConsumer, beginSubgroup(groupId, 1, testing::_))
        .Times(1)
        .WillRepeatedly(testing::Return(mockSubgroupConsumerOne));

    for (int objectId = 0; objectId <= params_.lastObjectInTrack; objectId++) {
      int objectSize = moxygen::getObjectSize(objectId, &params_);
      // Set expectations for beginObject
      if (objectId % 2 == 1 && objectId == params_.lastObjectInTrack) {
        EXPECT_CALL(*mockSubgroupConsumerZero, endOfGroup(objectId, testing::_))
            .WillOnce(
                ::testing::Return(
                    folly::Expected<folly::Unit, moxygen::MoQPublishError>(
                        {})));

        EXPECT_CALL(*mockSubgroupConsumerOne, endOfSubgroup())
            .Times(1)
            .WillOnce(
                ::testing::Return(
                    folly::Expected<folly::Unit, moxygen::MoQPublishError>(
                        {})));
      } else if (objectId == params_.lastObjectInTrack) {
        EXPECT_CALL(*mockSubgroupConsumerOne, endOfGroup(objectId, testing::_))
            .WillOnce(
                ::testing::Return(
                    folly::Expected<folly::Unit, moxygen::MoQPublishError>(
                        {})));

        EXPECT_CALL(*mockSubgroupConsumerZero, endOfSubgroup())
            .Times(1)
            .WillOnce(
                ::testing::Return(
                    folly::Expected<folly::Unit, moxygen::MoQPublishError>(
                        {})));
      } else if (objectId % 2 == 0) {
        EXPECT_CALL(
            *mockSubgroupConsumerZero,
            object(objectId, testing::_, testing::_, testing::_))
            .Times(1)
            .WillOnce([objectSize](
                          auto,
                          std::unique_ptr<folly::IOBuf> payload,
                          const auto&,
                          auto) {
              auto payloadLength = (*payload).length();
              EXPECT_EQ(payloadLength, objectSize);
              return folly::Expected<folly::Unit, moxygen::MoQPublishError>({});
            });
      } else {
        EXPECT_CALL(
            *mockSubgroupConsumerOne,
            object(objectId, testing::_, testing::_, testing::_))
            .Times(1)
            .WillOnce([objectSize](
                          auto,
                          std::unique_ptr<folly::IOBuf> payload,
                          const auto&,
                          auto) {
              auto payloadLength = (*payload).length();
              EXPECT_EQ(payloadLength, objectSize);
              return folly::Expected<folly::Unit, moxygen::MoQPublishError>({});
            });
      }
    }
  }

  // Call the onSubscribe method
  auto task = server_.sendTwoSubgroupsPerGroup(params_, mockConsumer);

  // Wait for the coroutine to complete
  folly::coro::blockingWait(std::move(task));
}

TEST_F(MoQTrackServerTest, ValidateSubscribeWithForwardPreferenceThree) {
  MoQTrackServerTest::CreateDefaultMoQTestParameters();
  params_.forwardingPreference = moxygen::ForwardingPreference(3);
  params_.lastObjectInTrack = 1;
  params_.objectsPerGroup = 1;
  params_.lastGroupInTrack = 1;
  params_.sendEndOfGroupMarkers = false;

  moxygen::SubscribeRequest sub;
  sub.requestID = 0;
  sub.trackAlias = kDefaultTrackAlias;
  sub.groupOrder = moxygen::GroupOrder(0x1);
  sub.fullTrackName.trackNamespace = track_;
  params_.testIntegerExtension = -1;
  params_.testVariableExtension = -1;

  // Create a mock track consumer
  auto mockConsumer = std::make_shared<moxygen::MockTrackConsumer>();

  // Expect setTrackAlias call
  EXPECT_CALL(*mockConsumer, setTrackAlias(*sub.trackAlias))
      .WillOnce(
          testing::Return(
              folly::Expected<folly::Unit, moxygen::MoQPublishError>(
                  folly::unit)));

  // Build Expect Calls
  for (int groupNum = 1; groupNum >= 0; groupNum--) {
    for (int objectId = 1; objectId >= 0; objectId--) {
      // Set expectations for setTrackAlias and datagram
      moxygen::ObjectHeader expectedHeader;
      expectedHeader.group = groupNum;
      expectedHeader.id = objectId;
      expectedHeader.extensions = moxygen::Extensions(
          moxygen::getExtensions(
              params_.testIntegerExtension, params_.testVariableExtension),
          {});

      int objectSize = moxygen::getObjectSize(objectId, &params_);

      EXPECT_CALL(*mockConsumer, datagram(expectedHeader, testing::_))
          .Times(1)
          .WillOnce([expectedHeader,
                     objectSize,
                     expectedTrackAlias = *sub.trackAlias](
                        const auto& header, auto objectPayload) {
            // Check Object Header
            EXPECT_EQ(expectedHeader.group, header.group);
            EXPECT_EQ(expectedHeader.id, header.id);
            EXPECT_EQ(expectedHeader.extensions, header.extensions);

            // Check Object Payload
            auto payloadLength = (*objectPayload).length();
            EXPECT_EQ(payloadLength, objectSize);
            return folly::Expected<folly::Unit, moxygen::MoQPublishError>({});
          });
    }
  }

  // Call the sendObjectsForForwardPreferenceThree method
  auto task = server_.sendDatagram(sub, params_, mockConsumer);

  // Wait for the coroutine to complete
  folly::coro::blockingWait(std::move(task));
}

// Fetch Testing
TEST_F(
    MoQTrackServerTest,
    TestFetchFunctionReturnsSubscribeErrorWithInvalidParams) {
  moxygen::Fetch req;
  MoQTrackServerTest::CreateDefaultTrackNamespace();
  track_.trackNamespace[0] = "invalid";
  req.requestID = 0;
  req.fullTrackName.trackNamespace = track_;

  // Call the subscribe method
  auto task = server_.fetch(req, nullptr);

  // Wait for the coroutine to complete and get the result
  auto result = folly::coro::blockingWait(std::move(task));

  // Check that the result is an error
  ASSERT_TRUE(result.hasError());

  // Verify the error details
  const auto& error = result.error();
  EXPECT_EQ(error.requestID, req.requestID);
  EXPECT_EQ(error.errorCode, moxygen::FetchErrorCode::NOT_SUPPORTED);
  EXPECT_EQ(error.reasonPhrase, "Invalid Parameters");
}
TEST_F(MoQTrackServerTest, ValidateFetchWithForwardPreferenceZero) {
  MoQTrackServerTest::CreateDefaultMoQTestParameters();

  // Create a mock track consumer
  auto mockConsumer = std::make_shared<moxygen::MockFetchConsumer>();

  // Set expectations for beginSubgroup
  for (int groupId = 0; groupId <= 10; groupId++) {
    // Create a mock subgroup consumer
    for (int objectId = 0; objectId <= params_.lastObjectInTrack; objectId++) {
      // Find Object Size
      int objectSize = moxygen::getObjectSize(objectId, &params_);

      // Set expectations for beginObject
      EXPECT_CALL(
          *mockConsumer,
          object(groupId, 0, objectId, testing::_, testing::_, testing::_))
          .Times(1)
          .WillOnce([objectSize](
                        auto,
                        auto,
                        auto,
                        std::unique_ptr<folly::IOBuf> payload,
                        const auto&,
                        auto) {
            auto payloadLength = (*payload).length();
            EXPECT_EQ(payloadLength, objectSize);
            return folly::Expected<folly::Unit, moxygen::MoQPublishError>({});
          })
          .WillRepeatedly(
              ::testing::Return(
                  folly::Expected<folly::Unit, moxygen::MoQPublishError>({})));
    }
  }

  EXPECT_CALL(*mockConsumer, endOfFetch())
      .Times(1)
      .WillOnce(
          ::testing::Return(
              folly::Expected<folly::Unit, moxygen::MoQPublishError>({})));

  // Call the onSubscribe method
  auto task = server_.fetchOneSubgroupPerGroup(params_, mockConsumer);

  // Wait for the coroutine to complete
  folly::coro::blockingWait(std::move(task));
}

TEST_F(
    MoQTrackServerTest,
    ValidateFetchWithForwardPreferenceZeroWithExtensions) {
  MoQTrackServerTest::CreateDefaultMoQTestParameters();
  int integerExtension = 1;
  int variableExtension = 1;
  params_.testIntegerExtension = integerExtension;
  params_.testVariableExtension = variableExtension;

  // Create a mock track consumer
  auto mockConsumer = std::make_shared<moxygen::MockFetchConsumer>();

  // Set expectations for beginSubgroup
  for (int groupId = 0; groupId <= 10; groupId++) {
    // Create a mock subgroup consumer
    for (int objectId = 0; objectId <= params_.lastObjectInTrack; objectId++) {
      // Find Object Size
      int objectSize = moxygen::getObjectSize(objectId, &params_);

      // Set expectations for beginObject
      EXPECT_CALL(
          *mockConsumer,
          object(groupId, 0, objectId, testing::_, testing::_, testing::_))
          .Times(1)
          .WillOnce([objectSize, integerExtension](
                        auto,
                        auto,
                        auto,
                        std::unique_ptr<folly::IOBuf> payload,
                        auto extensions,
                        auto) {
            // Check Extensions Generate
            EXPECT_EQ(extensions.size(), 2);

            // Check if Integer Extension type is 2*Field
            auto& mutable_exts = extensions.getMutableExtensions();
            EXPECT_EQ(mutable_exts[0].type, 2);

            // Check if Variable Extension type is 2*Field + 1
            EXPECT_EQ(mutable_exts[1].type, 3);

            // Check if Variable Extension is within size range of 1-20
            bool check =
                mutable_exts[1].arrayValue->computeChainDataLength() >= 1 &&
                mutable_exts[1].arrayValue->computeChainDataLength() <= 20;
            EXPECT_TRUE(check);

            // Check Payload
            auto payloadLength = (*payload).length();
            EXPECT_EQ(payloadLength, objectSize);
            return folly::Expected<folly::Unit, moxygen::MoQPublishError>({});
          })
          .WillRepeatedly(
              ::testing::Return(
                  folly::Expected<folly::Unit, moxygen::MoQPublishError>({})));
    }
  }

  EXPECT_CALL(*mockConsumer, endOfFetch())
      .Times(1)
      .WillOnce(
          ::testing::Return(
              folly::Expected<folly::Unit, moxygen::MoQPublishError>({})));

  // Call the onSubscribe method
  auto task = server_.fetchOneSubgroupPerGroup(params_, mockConsumer);

  // Wait for the coroutine to complete
  folly::coro::blockingWait(std::move(task));
}

TEST_F(MoQTrackServerTest, ValidateFetchWithForwardPreferenceOne) {
  MoQTrackServerTest::CreateDefaultMoQTestParameters();
  // Create a mock track consumer
  auto mockConsumer = std::make_shared<moxygen::MockFetchConsumer>();

  // Set expectations for beginSubgroup
  for (int groupId = 0; groupId <= 10; groupId++) {
    // Create a mock subgroup consumer
    for (int objectId = 0; objectId <= params_.lastObjectInTrack; objectId++) {
      // Find Object Size
      int objectSize = moxygen::getObjectSize(objectId, &params_);

      // Set expectations for beginObject
      EXPECT_CALL(
          *mockConsumer,
          object(
              groupId, objectId, objectId, testing::_, testing::_, testing::_))
          .Times(1)
          .WillOnce([objectSize](
                        auto,
                        auto,
                        auto,
                        std::unique_ptr<folly::IOBuf> payload,
                        const auto&,
                        auto) {
            // Check Payload
            auto payloadLength = (*payload).length();
            EXPECT_EQ(payloadLength, objectSize);
            return folly::Expected<folly::Unit, moxygen::MoQPublishError>({});
          })
          .WillRepeatedly(
              ::testing::Return(
                  folly::Expected<folly::Unit, moxygen::MoQPublishError>({})));
    }
  }

  EXPECT_CALL(*mockConsumer, endOfFetch())
      .Times(1)
      .WillOnce(
          ::testing::Return(
              folly::Expected<folly::Unit, moxygen::MoQPublishError>({})));

  // Call the onSubscribe method
  auto task = server_.fetchOneSubgroupPerObject(params_, mockConsumer);

  // Wait for the coroutine to complete
  folly::coro::blockingWait(std::move(task));
}

TEST_F(MoQTrackServerTest, ValidateFetchWithForwardPreferenceTwo) {
  MoQTrackServerTest::CreateDefaultMoQTestParameters();
  // Create a mock track consumer
  auto mockConsumer = std::make_shared<moxygen::MockFetchConsumer>();

  // Set expectations for beginSubgroup
  for (int groupId = 0; groupId <= 10; groupId++) {
    // Create a mock subgroup consumer
    for (int objectId = 0; objectId <= params_.lastObjectInTrack; objectId++) {
      // Find Object Size
      int objectSize = moxygen::getObjectSize(objectId, &params_);
      int subGroupId = objectId % 2;
      // Set expectations for beginObject
      EXPECT_CALL(
          *mockConsumer,
          object(
              groupId,
              subGroupId,
              objectId,
              testing::_,
              testing::_,
              testing::_))
          .Times(1)
          .WillOnce([objectSize](
                        auto,
                        auto,
                        auto,
                        std::unique_ptr<folly::IOBuf> payload,
                        const auto&,
                        auto) {
            // Check Payload
            auto payloadLength = (*payload).length();
            EXPECT_EQ(payloadLength, objectSize);
            return folly::Expected<folly::Unit, moxygen::MoQPublishError>({});
          })
          .WillRepeatedly(
              ::testing::Return(
                  folly::Expected<folly::Unit, moxygen::MoQPublishError>({})));
    }
  }

  EXPECT_CALL(*mockConsumer, endOfFetch())
      .Times(1)
      .WillOnce(
          ::testing::Return(
              folly::Expected<folly::Unit, moxygen::MoQPublishError>({})));

  // Call the onSubscribe method
  auto task = server_.fetchOneSubgroupPerObject(params_, mockConsumer);

  // Wait for the coroutine to complete
  folly::coro::blockingWait(std::move(task));
}

TEST_F(
    MoQTrackServerTest,
    ValidateFetchWithForwardPreferenceTwoAndEndOfGroupMarkers) {
  MoQTrackServerTest::CreateDefaultMoQTestParameters();
  params_.sendEndOfGroupMarkers = true;
  params_.objectsPerGroup = 10;

  // Create a mock track consumer
  auto mockConsumer = std::make_shared<moxygen::MockFetchConsumer>();

  // Set expectations for beginSubgroup
  for (int groupId = 0; groupId <= 10; groupId++) {
    // Create a mock subgroup consumer
    for (int objectId = 0; objectId <= params_.lastObjectInTrack; objectId++) {
      // Find Object Size
      int objectSize = moxygen::getObjectSize(objectId, &params_);
      int subGroupId = (objectId - params_.startObject) % 2;
      // Set expectations for beginObject
      if (objectId != params_.lastObjectInTrack) {
        EXPECT_CALL(
            *mockConsumer,
            object(
                groupId,
                subGroupId,
                objectId,
                testing::_,
                testing::_,
                testing::_))
            .Times(1)
            .WillOnce([objectSize](
                          auto,
                          auto,
                          auto,
                          std::unique_ptr<folly::IOBuf> payload,
                          const auto&,
                          auto) {
              // Check Payload
              auto payloadLength = (*payload).length();
              EXPECT_EQ(payloadLength, objectSize);
              return folly::Expected<folly::Unit, moxygen::MoQPublishError>({});
            })
            .WillOnce(
                ::testing::Return(
                    folly::Expected<folly::Unit, moxygen::MoQPublishError>(
                        {})));
      } else {
        EXPECT_CALL(
            *mockConsumer,
            endOfGroup(groupId, subGroupId, objectId, testing::_, testing::_))
            .Times(1)
            .WillOnce(
                testing::Return(
                    folly::Expected<folly::Unit, moxygen::MoQPublishError>(
                        {})));
      }
    }
  }

  EXPECT_CALL(*mockConsumer, endOfFetch())
      .Times(1)
      .WillOnce(
          ::testing::Return(
              folly::Expected<folly::Unit, moxygen::MoQPublishError>({})));

  // Call the onSubscribe method
  auto task = server_.fetchTwoSubgroupsPerGroup(params_, mockConsumer);

  // Wait for the coroutine to complete
  folly::coro::blockingWait(std::move(task));
}

TEST_F(
    MoQTrackServerTest,
    ValidateFetchWithForwardPreferenceThreeReturnsError) {
  moxygen::Fetch req;
  MoQTrackServerTest::CreateDefaultTrackNamespace();
  track_.trackNamespace[1] = "3";
  req.requestID = 0;
  req.fullTrackName.trackNamespace = track_;

  // Call the subscribe method
  auto task = server_.fetch(req, nullptr);

  // Wait for the coroutine to complete and get the result
  auto result = folly::coro::blockingWait(std::move(task));

  // Check that the result is an error
  ASSERT_TRUE(result.hasError());

  // Verify the error details
  const auto& error = result.error();
  EXPECT_EQ(error.requestID, req.requestID);
  EXPECT_EQ(error.errorCode, moxygen::FetchErrorCode::NOT_SUPPORTED);
  EXPECT_EQ(
      error.reasonPhrase,
      "Datagram Forwarding Preference is not supported for fetch");
}

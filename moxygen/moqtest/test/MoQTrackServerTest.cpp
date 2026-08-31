/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <folly/portability/GTest.h>
#include "folly/Expected.h"
#include "folly/coro/Baton.h"
#include "folly/coro/BlockingWait.h"
#include "folly/coro/GtestHelpers.h"
#include "folly/coro/Sleep.h"
#include "moxygen/moqtest/MoQTestPublisher.h"
#include "moxygen/moqtest/Utils.h"
#include "moxygen/test/MockMoQSession.h"
#include "moxygen/test/Mocks.h"

namespace {

const std::string kDefaultTrackName = "test";

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

  std::shared_ptr<moxygen::MoQSession> CreateSession() {
    auto session =
        std::make_shared<testing::NiceMock<moxygen::test::MockMoQSession>>();
    ON_CALL(*session, getNegotiatedVersion())
        .WillByDefault(
            testing::Return(
                std::optional<uint64_t>(moxygen::kVersionDraftCurrent)));
    return session;
  }

  moxygen::SubscribeRequest MakeSubscribe(uint64_t requestID) {
    moxygen::SubscribeRequest sub;
    sub.requestID = moxygen::RequestID(requestID);
    sub.fullTrackName = {track_, kDefaultTrackName};
    sub.locType = moxygen::LocationType::NextGroupStart;
    sub.forward = true;
    return sub;
  }

  // subscribe() reads the requesting session out of the RequestContext, the
  // way MoQSession sets it up for a real request.
  folly::coro::Task<moxygen::Publisher::SubscribeResult> SubscribeAs(
      std::shared_ptr<moxygen::MoQSession> session,
      moxygen::SubscribeRequest sub,
      std::shared_ptr<moxygen::TrackConsumer> consumer) {
    folly::RequestContextScopeGuard guard;
    folly::RequestContext::get()->setContextData(
        SessionRequestToken(),
        std::make_unique<moxygen::MoQSession::MoQSessionRequestData>(
            std::move(session)));
    co_return co_await publisher_->subscribe(
        std::move(sub), std::move(consumer));
  }

  static const folly::RequestToken& SessionRequestToken() {
    static folly::RequestToken token("moq_session");
    return token;
  }

  moxygen::MoQTestParameters params_;
  moxygen::TrackNamespace track_;
  std::shared_ptr<moxygen::MoQTestPublisher> publisher_{
      std::make_shared<moxygen::MoQTestPublisher>()};
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

  // Call the subscribe method
  auto task = publisher_->subscribe(req, nullptr);

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
    EXPECT_CALL(
        *mockConsumer, beginSubgroup(groupId, 0, testing::_, testing::_))
        .WillRepeatedly(testing::Return(mockSubgroupConsumer));
    for (int objectId = 0; objectId <= params_.lastObjectInTrack; objectId++) {
      // Find Object Size
      auto objectSize = moxygen::getObjectSize(objectId, &params_);

      // Set expectations for beginObject
      EXPECT_CALL(
          *mockSubgroupConsumer,
          object(objectId, testing::_, testing::_, testing::_))
          .WillOnce([objectSize](
                        auto,
                        std::unique_ptr<folly::IOBuf> payload,
                        const auto&,
                        auto) {
            auto payloadLength = (*payload).length();
            EXPECT_EQ(payloadLength, objectSize);
            return folly::Expected<folly::Unit, moxygen::MoQPublishError>(
                folly::unit);
          });
    }
    // Set expectations for endOfSubgroup
    EXPECT_CALL(*mockSubgroupConsumer, endOfSubgroup())
        .WillOnce(
            ::testing::Return(
                folly::Expected<folly::Unit, moxygen::MoQPublishError>(
                    folly::unit)));
  }

  // Call the onSubscribe method
  auto task = publisher_->sendOneSubgroupPerGroup(params_, mockConsumer);

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
    EXPECT_CALL(
        *mockConsumer, beginSubgroup(groupId, 0, testing::_, testing::_))
        .WillRepeatedly(testing::Return(mockSubgroupConsumer));
    for (int objectId = 0; objectId <= params_.lastObjectInTrack; objectId++) {
      // Find Object Size
      auto objectSize = moxygen::getObjectSize(objectId, &params_);

      // Set expectations for beginObject
      EXPECT_CALL(
          *mockSubgroupConsumer,
          object(objectId, testing::_, testing::_, testing::_))
          .WillOnce([objectSize](
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
            return folly::Expected<folly::Unit, moxygen::MoQPublishError>(
                folly::unit);
          })
          .WillRepeatedly(
              ::testing::Return(
                  folly::Expected<folly::Unit, moxygen::MoQPublishError>(
                      folly::unit)));
    }
    // Set expectations for endOfSubgroup
    EXPECT_CALL(*mockSubgroupConsumer, endOfSubgroup())
        .WillRepeatedly(
            ::testing::Return(
                folly::Expected<folly::Unit, moxygen::MoQPublishError>(
                    folly::unit)));
  }

  // Call the onSubscribe method
  auto task = publisher_->sendOneSubgroupPerGroup(params_, mockConsumer);

  // Wait for the coroutine to complete
  folly::coro::blockingWait(std::move(task));
}

TEST_F(MoQTrackServerTest, ValidateSubscribeWithForwardPreferenceOne) {
  MoQTrackServerTest::CreateDefaultMoQTestParameters();
  params_.forwardingPreference = moxygen::ForwardingPreference(1);

  // Create a mock track consumer
  auto mockConsumer = std::make_shared<moxygen::MockTrackConsumer>();

  // Set expectations for beginSubgroup
  for (int groupId = 0; groupId <= 10; groupId++) {
    for (int objectId = 0; objectId <= params_.lastObjectInTrack; objectId++) {
      // Set expectations for beginObject
      auto objectSize = moxygen::getObjectSize(objectId, &params_);
      // Create a mock subgroup consumer
      auto mockSubgroupConsumer =
          std::make_shared<moxygen::MockSubgroupConsumer>();
      EXPECT_CALL(
          *mockConsumer,
          beginSubgroup(groupId, objectId, testing::_, testing::_))
          .WillOnce(testing::Return(mockSubgroupConsumer));
      EXPECT_CALL(
          *mockSubgroupConsumer, object(objectId, testing::_, testing::_, true))
          .WillOnce([objectSize](
                        auto,
                        std::unique_ptr<folly::IOBuf> payload,
                        const auto&,
                        auto) {
            auto payloadLength = (*payload).length();
            EXPECT_EQ(payloadLength, objectSize);
            return folly::Expected<folly::Unit, moxygen::MoQPublishError>(
                folly::unit);
          });
    }
  }

  // Call the onSubscribe method
  auto task = publisher_->sendOneSubgroupPerObject(params_, mockConsumer);

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
    EXPECT_CALL(
        *mockConsumer, beginSubgroup(groupId, 0, testing::_, testing::_))
        .WillRepeatedly(testing::Return(mockSubgroupConsumerZero));
    EXPECT_CALL(
        *mockConsumer, beginSubgroup(groupId, 1, testing::_, testing::_))
        .WillRepeatedly(testing::Return(mockSubgroupConsumerOne));

    for (int objectId = 0; objectId <= params_.lastObjectInTrack; objectId++) {
      auto objectSize = moxygen::getObjectSize(objectId, &params_);
      // Set expectations for beginObject
      if (objectId % 2 == 0) {
        EXPECT_CALL(
            *mockSubgroupConsumerZero,
            object(objectId, testing::_, testing::_, testing::_))
            .WillOnce([objectSize](
                          auto,
                          std::unique_ptr<folly::IOBuf> payload,
                          const auto&,
                          auto) {
              auto payloadLength = (*payload).length();
              EXPECT_EQ(payloadLength, objectSize);
              return folly::Expected<folly::Unit, moxygen::MoQPublishError>(
                  folly::unit);
            });
      } else {
        EXPECT_CALL(
            *mockSubgroupConsumerOne,
            object(objectId, testing::_, testing::_, testing::_))
            .WillOnce([objectSize](
                          auto,
                          std::unique_ptr<folly::IOBuf> payload,
                          const auto&,
                          auto) {
              auto payloadLength = (*payload).length();
              EXPECT_EQ(payloadLength, objectSize);
              return folly::Expected<folly::Unit, moxygen::MoQPublishError>(
                  folly::unit);
            });
      }
    }
    // Set expectations for endOfSubgroup
    EXPECT_CALL(*mockSubgroupConsumerZero, endOfSubgroup())
        .WillOnce(
            ::testing::Return(
                folly::Expected<folly::Unit, moxygen::MoQPublishError>(
                    folly::unit)));
    EXPECT_CALL(*mockSubgroupConsumerOne, endOfSubgroup())

        .WillOnce(
            ::testing::Return(
                folly::Expected<folly::Unit, moxygen::MoQPublishError>(
                    folly::unit)));
  }

  // Call the onSubscribe method
  auto task = publisher_->sendTwoSubgroupsPerGroup(params_, mockConsumer);

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
    EXPECT_CALL(
        *mockConsumer, beginSubgroup(groupId, 0, testing::_, testing::_))

        .WillRepeatedly(testing::Return(mockSubgroupConsumerZero));
    EXPECT_CALL(
        *mockConsumer, beginSubgroup(groupId, 1, testing::_, testing::_))

        .WillRepeatedly(testing::Return(mockSubgroupConsumerOne));

    for (int objectId = 0; objectId <= params_.lastObjectInTrack; objectId++) {
      auto objectSize = moxygen::getObjectSize(objectId, &params_);
      // Set expectations for beginObject
      if (objectId % 2 == 0) {
        if (objectId == params_.lastObjectInTrack) {
          EXPECT_CALL(*mockSubgroupConsumerZero, endOfGroup(objectId))
              .WillOnce(
                  ::testing::Return(
                      folly::Expected<folly::Unit, moxygen::MoQPublishError>(
                          folly::unit)));

          EXPECT_CALL(*mockSubgroupConsumerOne, endOfSubgroup())
              .WillOnce(
                  ::testing::Return(
                      folly::Expected<folly::Unit, moxygen::MoQPublishError>(
                          folly::unit)));
        } else {
          EXPECT_CALL(
              *mockSubgroupConsumerZero,
              object(objectId, testing::_, testing::_, testing::_))
              .WillOnce([objectSize](
                            auto,
                            std::unique_ptr<folly::IOBuf> payload,
                            const auto&,
                            auto) {
                auto payloadLength = (*payload).length();
                EXPECT_EQ(payloadLength, objectSize);
                return folly::Expected<folly::Unit, moxygen::MoQPublishError>(
                    folly::unit);
              });
        }
      } else {
        EXPECT_CALL(
            *mockSubgroupConsumerOne,
            object(objectId, testing::_, testing::_, testing::_))
            .WillOnce([objectSize](
                          auto,
                          std::unique_ptr<folly::IOBuf> payload,
                          const auto&,
                          auto) {
              auto payloadLength = (*payload).length();
              EXPECT_EQ(payloadLength, objectSize);
              return folly::Expected<folly::Unit, moxygen::MoQPublishError>(
                  folly::unit);
            });
      }
    }
  }

  // Call the onSubscribe method
  auto task = publisher_->sendTwoSubgroupsPerGroup(params_, mockConsumer);

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
  sub.groupOrder = moxygen::GroupOrder(0x1);
  sub.fullTrackName.trackNamespace = track_;
  params_.testIntegerExtension = -1;
  params_.testVariableExtension = -1;

  // Create a mock track consumer
  auto mockConsumer = std::make_shared<moxygen::MockTrackConsumer>();

  // Build Expect Calls
  for (int groupNum = 1; groupNum >= 0; groupNum--) {
    for (int objectId = 1; objectId >= 0; objectId--) {
      moxygen::ObjectHeader expectedHeader;
      expectedHeader.group = groupNum;
      expectedHeader.id = objectId;
      expectedHeader.priority = moxygen::publisherPriorityForGroup(groupNum);
      expectedHeader.extensions = moxygen::Extensions(
          moxygen::getExtensions(
              params_.testIntegerExtension, params_.testVariableExtension),
          {});

      auto objectSize = moxygen::getObjectSize(objectId, &params_);

      EXPECT_CALL(
          *mockConsumer, datagram(expectedHeader, testing::_, testing::_))
          .WillOnce([expectedHeader, objectSize](
                        const auto& header, auto objectPayload, bool) {
            // Check Object Header
            EXPECT_EQ(expectedHeader.group, header.group);
            EXPECT_EQ(expectedHeader.id, header.id);
            EXPECT_EQ(expectedHeader.extensions, header.extensions);

            // Check Object Payload
            auto payloadLength = (*objectPayload).length();
            EXPECT_EQ(payloadLength, objectSize);
            return folly::Expected<folly::Unit, moxygen::MoQPublishError>(
                folly::unit);
          });
    }
  }

  // Call the sendObjectsForForwardPreferenceThree method
  auto task = publisher_->sendDatagram(sub.requestID, params_, mockConsumer);

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
  auto task = publisher_->fetch(req, nullptr);

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
      auto objectSize = moxygen::getObjectSize(objectId, &params_);

      // Set expectations for beginObject
      EXPECT_CALL(
          *mockConsumer,
          object(
              groupId,
              0,
              objectId,
              testing::_,
              testing::_,
              testing::_,
              testing::_))
          .WillOnce([objectSize](
                        auto,
                        auto,
                        auto,
                        std::unique_ptr<folly::IOBuf> payload,
                        const auto&,
                        auto,
                        auto) {
            auto payloadLength = (*payload).length();
            EXPECT_EQ(payloadLength, objectSize);
            return folly::Expected<folly::Unit, moxygen::MoQPublishError>(
                folly::unit);
          })
          .WillRepeatedly(
              ::testing::Return(
                  folly::Expected<folly::Unit, moxygen::MoQPublishError>(
                      folly::unit)));
    }
  }

  EXPECT_CALL(*mockConsumer, endOfFetch())
      .WillOnce(
          ::testing::Return(
              folly::Expected<folly::Unit, moxygen::MoQPublishError>(
                  folly::unit)));

  // Call the onSubscribe method
  auto task = publisher_->fetchOneSubgroupPerGroup(
      params_, mockConsumer, moxygen::resolveFetchWindow(params_));

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
      auto objectSize = moxygen::getObjectSize(objectId, &params_);

      // Set expectations for beginObject
      EXPECT_CALL(
          *mockConsumer,
          object(
              groupId,
              0,
              objectId,
              testing::_,
              testing::_,
              testing::_,
              testing::_))
          .WillOnce([objectSize](
                        auto,
                        auto,
                        auto,
                        std::unique_ptr<folly::IOBuf> payload,
                        auto extensions,
                        auto,
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
            return folly::Expected<folly::Unit, moxygen::MoQPublishError>(
                folly::unit);
          })
          .WillRepeatedly(
              ::testing::Return(
                  folly::Expected<folly::Unit, moxygen::MoQPublishError>(
                      folly::unit)));
    }
  }

  EXPECT_CALL(*mockConsumer, endOfFetch())
      .WillOnce(
          ::testing::Return(
              folly::Expected<folly::Unit, moxygen::MoQPublishError>(
                  folly::unit)));

  // Call the onSubscribe method
  auto task = publisher_->fetchOneSubgroupPerGroup(
      params_, mockConsumer, moxygen::resolveFetchWindow(params_));

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
      auto objectSize = moxygen::getObjectSize(objectId, &params_);

      // Set expectations for beginObject
      EXPECT_CALL(
          *mockConsumer,
          object(
              groupId,
              objectId,
              objectId,
              testing::_,
              testing::_,
              testing::_,
              testing::_))
          .WillOnce([objectSize](
                        auto,
                        auto,
                        auto,
                        std::unique_ptr<folly::IOBuf> payload,
                        const auto&,
                        auto,
                        auto) {
            // Check Payload
            auto payloadLength = (*payload).length();
            EXPECT_EQ(payloadLength, objectSize);
            return folly::Expected<folly::Unit, moxygen::MoQPublishError>(
                folly::unit);
          })
          .WillRepeatedly(
              ::testing::Return(
                  folly::Expected<folly::Unit, moxygen::MoQPublishError>(
                      folly::unit)));
    }
  }

  EXPECT_CALL(*mockConsumer, endOfFetch())
      .WillOnce(
          ::testing::Return(
              folly::Expected<folly::Unit, moxygen::MoQPublishError>(
                  folly::unit)));

  // Call the onSubscribe method
  auto task = publisher_->fetchOneSubgroupPerObject(
      params_, mockConsumer, moxygen::resolveFetchWindow(params_));

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
      auto objectSize = moxygen::getObjectSize(objectId, &params_);
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
              testing::_,
              testing::_))
          .WillOnce([objectSize](
                        auto,
                        auto,
                        auto,
                        std::unique_ptr<folly::IOBuf> payload,
                        const auto&,
                        auto,
                        auto) {
            // Check Payload
            auto payloadLength = (*payload).length();
            EXPECT_EQ(payloadLength, objectSize);
            return folly::Expected<folly::Unit, moxygen::MoQPublishError>(
                folly::unit);
          });
    }
  }

  EXPECT_CALL(*mockConsumer, endOfFetch())
      .WillOnce(
          ::testing::Return(
              folly::Expected<folly::Unit, moxygen::MoQPublishError>(
                  folly::unit)));

  // Call the onSubscribe method
  auto task = publisher_->fetchOneSubgroupPerObject(
      params_, mockConsumer, moxygen::resolveFetchWindow(params_));

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
      auto objectSize = moxygen::getObjectSize(objectId, &params_);
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
                testing::_,
                testing::_))
            .WillOnce([objectSize](
                          auto,
                          auto,
                          auto,
                          std::unique_ptr<folly::IOBuf> payload,
                          const auto&,
                          auto,
                          auto) {
              // Check Payloadƒ
              auto payloadLength = (*payload).length();
              EXPECT_EQ(payloadLength, objectSize);
              return folly::Expected<folly::Unit, moxygen::MoQPublishError>(
                  folly::unit);
            });
      } else {
        EXPECT_CALL(
            *mockConsumer,
            endOfGroup(groupId, subGroupId, objectId, testing::_))
            .WillOnce(
                testing::Return(
                    folly::Expected<folly::Unit, moxygen::MoQPublishError>(
                        folly::unit)));
      }
    }
  }

  EXPECT_CALL(*mockConsumer, endOfFetch())
      .WillOnce(
          ::testing::Return(
              folly::Expected<folly::Unit, moxygen::MoQPublishError>(
                  folly::unit)));

  // Call the onSubscribe method
  auto task = publisher_->fetchTwoSubgroupsPerGroup(
      params_, mockConsumer, moxygen::resolveFetchWindow(params_));

  // Wait for the coroutine to complete
  folly::coro::blockingWait(std::move(task));
}

TEST_F(MoQTrackServerTest, ValidateFetchWithForwardPreferenceThree) {
  MoQTrackServerTest::CreateDefaultMoQTestParameters();
  params_.forwardingPreference = moxygen::ForwardingPreference(3);
  params_.sendEndOfGroupMarkers = false;

  // Create a mock fetch consumer
  auto mockConsumer = std::make_shared<moxygen::MockFetchConsumer>();

  // Build Expect Calls
  for (int groupId = 0; groupId <= 10; groupId++) {
    // Create a mock subgroup consumer
    for (int objectId = 0; objectId <= params_.lastObjectInTrack; objectId++) {
      moxygen::ObjectHeader expectedHeader;
      expectedHeader.group = groupId;
      expectedHeader.id = objectId;
      expectedHeader.extensions = moxygen::Extensions(
          moxygen::getExtensions(
              params_.testIntegerExtension, params_.testVariableExtension),
          {});

      auto objectSize = moxygen::getObjectSize(objectId, &params_);

      // Datagram objects carry no subgroup: the object is flagged and sits in
      // subgroup 0, which the framer omits from draft 16 onwards.
      EXPECT_CALL(
          *mockConsumer,
          object(
              expectedHeader.group,
              0 /* subgroupId */,
              expectedHeader.id,
              testing::_,
              testing::_,
              testing::_,
              true /* forwardingPreferenceIsDatagram */))
          .WillOnce([expectedHeader, objectSize](
                        auto,
                        auto,
                        auto,
                        std::unique_ptr<folly::IOBuf> objectPayload,
                        const auto& /*extensions*/,
                        auto,
                        auto) {
            // TODO: Extensions don't match?
            // EXPECT_EQ(expectedHeader.extensions, extensions);

            // Check Object Payload
            auto payloadLength = (*objectPayload).length();
            EXPECT_EQ(payloadLength, objectSize);
            return folly::Expected<folly::Unit, moxygen::MoQPublishError>(
                folly::unit);
          });
    }
  }

  EXPECT_CALL(*mockConsumer, endOfFetch())
      .WillOnce(
          ::testing::Return(
              folly::Expected<folly::Unit, moxygen::MoQPublishError>(
                  folly::unit)));

  // Datagram tracks fetch back through the one-subgroup-per-group generator
  auto task = publisher_->fetchOneSubgroupPerGroup(
      params_, mockConsumer, moxygen::resolveFetchWindow(params_));

  // Wait for the coroutine to complete
  folly::coro::blockingWait(std::move(task));
}

TEST_F(MoQTrackServerTest, FetchOfADatagramTrackSkipsEndOfGroupMarkers) {
  MoQTrackServerTest::CreateDefaultMoQTestParameters();
  params_.forwardingPreference = moxygen::ForwardingPreference::DATAGRAM;
  params_.sendEndOfGroupMarkers = true;

  auto mockConsumer = std::make_shared<moxygen::MockFetchConsumer>();
  EXPECT_CALL(
      *mockConsumer,
      object(
          testing::_,
          testing::_,
          testing::_,
          testing::_,
          testing::_,
          testing::_,
          true /* forwardingPreferenceIsDatagram */))
      .WillRepeatedly(
          ::testing::Return(
              folly::Expected<folly::Unit, moxygen::MoQPublishError>(
                  folly::unit)));
  // END_OF_GROUP is a status object with nowhere to carry the datagram flag, so
  // a datagram track must not emit one even when the track asks for markers.
  EXPECT_CALL(
      *mockConsumer, endOfGroup(testing::_, testing::_, testing::_, testing::_))
      .Times(0);
  EXPECT_CALL(*mockConsumer, endOfFetch())
      .WillOnce(
          ::testing::Return(
              folly::Expected<folly::Unit, moxygen::MoQPublishError>(
                  folly::unit)));

  folly::coro::blockingWait(publisher_->fetchOneSubgroupPerGroup(
      params_, mockConsumer, moxygen::resolveFetchWindow(params_)));
}

// Standalone FETCH range Testing
TEST_F(MoQTrackServerTest, FetchGeneratorOnlyEmitsObjectsInsideTheWindow) {
  MoQTrackServerTest::CreateDefaultMoQTestParameters();
  // Distinct sizes so the payload pins that object sizing keys off the track's
  // startObject, not wherever the window happens to open.
  params_.sizeOfObjectZero = 8;
  params_.sizeOfObjectGreaterThanZero = 3;

  auto mockConsumer = std::make_shared<moxygen::MockFetchConsumer>();
  // group, objectID, payload length
  std::vector<std::tuple<uint64_t, uint64_t, size_t>> received;
  EXPECT_CALL(
      *mockConsumer,
      object(
          testing::_,
          testing::_,
          testing::_,
          testing::_,
          testing::_,
          testing::_,
          testing::_))
      .WillRepeatedly([&received](
                          uint64_t group,
                          uint64_t,
                          uint64_t objectId,
                          std::unique_ptr<folly::IOBuf> payload,
                          const auto&,
                          auto,
                          auto) {
        received.emplace_back(group, objectId, payload->length());
        return folly::Expected<folly::Unit, moxygen::MoQPublishError>(
            folly::unit);
      });
  EXPECT_CALL(*mockConsumer, endOfFetch())
      .WillOnce(
          ::testing::Return(
              folly::Expected<folly::Unit, moxygen::MoQPublishError>(
                  folly::unit)));

  // The track carries objects 0 and 1 of groups 0 through 10.  End object 1 is
  // exclusive, so the range stops after object 0 of group 4.
  auto window = moxygen::resolveFetchWindow(
      params_, moxygen::StandaloneFetch({2, 1}, {4, 1}));
  folly::coro::blockingWait(
      publisher_->fetchOneSubgroupPerGroup(params_, mockConsumer, window));

  // Object 0 keeps sizeOfObjectZero even though the window opens on object 1.
  const std::vector<std::tuple<uint64_t, uint64_t, size_t>> expected{
      {2, 1, 3}, {3, 0, 8}, {3, 1, 3}, {4, 0, 8}};
  EXPECT_EQ(received, expected);
}

TEST_F(MoQTrackServerTest, FetchGeneratorEmitsNothingForAnEmptyWindow) {
  MoQTrackServerTest::CreateDefaultMoQTestParameters();

  auto mockConsumer = std::make_shared<moxygen::MockFetchConsumer>();
  EXPECT_CALL(
      *mockConsumer,
      object(
          testing::_,
          testing::_,
          testing::_,
          testing::_,
          testing::_,
          testing::_,
          testing::_))
      .Times(0);
  EXPECT_CALL(*mockConsumer, endOfFetch())
      .WillOnce(
          ::testing::Return(
              folly::Expected<folly::Unit, moxygen::MoQPublishError>(
                  folly::unit)));

  // A default window selects nothing, so the loop bounds must exclude {0, 0}.
  folly::coro::blockingWait(publisher_->fetchOneSubgroupPerGroup(
      params_, mockConsumer, moxygen::MoQTestFetchWindow{}));
}

TEST_F(MoQTrackServerTest, FetchTwoSubgroupsWindowKeepsObjectParity) {
  MoQTrackServerTest::CreateDefaultMoQTestParameters();
  params_.forwardingPreference =
      moxygen::ForwardingPreference::TWO_SUBGROUPS_PER_GROUP;
  params_.objectsPerGroup = 4;
  params_.lastObjectInTrack = 4;

  auto mockConsumer = std::make_shared<moxygen::MockFetchConsumer>();
  // group, subgroup, objectID
  std::vector<std::tuple<uint64_t, uint64_t, uint64_t>> received;
  EXPECT_CALL(
      *mockConsumer,
      object(
          testing::_,
          testing::_,
          testing::_,
          testing::_,
          testing::_,
          testing::_,
          testing::_))
      .WillRepeatedly([&received](
                          uint64_t group,
                          uint64_t subgroup,
                          uint64_t objectId,
                          std::unique_ptr<folly::IOBuf>,
                          const auto&,
                          auto,
                          auto) {
        received.emplace_back(group, subgroup, objectId);
        return folly::Expected<folly::Unit, moxygen::MoQPublishError>(
            folly::unit);
      });
  EXPECT_CALL(*mockConsumer, endOfFetch())
      .WillOnce(
          ::testing::Return(
              folly::Expected<folly::Unit, moxygen::MoQPublishError>(
                  folly::unit)));

  // Opens on object 3, which is odd, so the first object of the window lands
  // in subgroup 1 rather than subgroup 0.
  auto window = moxygen::resolveFetchWindow(
      params_, moxygen::StandaloneFetch({1, 3}, {2, 2}));
  folly::coro::blockingWait(
      publisher_->fetchTwoSubgroupsPerGroup(params_, mockConsumer, window));

  const std::vector<std::tuple<uint64_t, uint64_t, uint64_t>> expected{
      {1, 1, 3}, {1, 0, 4}, {2, 0, 0}, {2, 1, 1}};
  EXPECT_EQ(received, expected);
}

TEST_F(MoQTrackServerTest, FetchWindowTruncatingAGroupSkipsItsEndOfGroup) {
  MoQTrackServerTest::CreateDefaultMoQTestParameters();
  params_.sendEndOfGroupMarkers = true;
  params_.objectsPerGroup = 3;
  params_.lastObjectInTrack = 4;

  auto mockConsumer = std::make_shared<moxygen::MockFetchConsumer>();
  std::vector<std::pair<uint64_t, uint64_t>> objects;
  std::vector<std::pair<uint64_t, uint64_t>> endOfGroups;
  EXPECT_CALL(
      *mockConsumer,
      object(
          testing::_,
          testing::_,
          testing::_,
          testing::_,
          testing::_,
          testing::_,
          testing::_))
      .WillRepeatedly([&objects](
                          uint64_t group,
                          uint64_t,
                          uint64_t objectId,
                          std::unique_ptr<folly::IOBuf>,
                          const auto&,
                          auto,
                          auto) {
        objects.emplace_back(group, objectId);
        return folly::Expected<folly::Unit, moxygen::MoQPublishError>(
            folly::unit);
      });
  EXPECT_CALL(
      *mockConsumer, endOfGroup(testing::_, testing::_, testing::_, testing::_))
      .WillRepeatedly(
          [&endOfGroups](uint64_t group, uint64_t, uint64_t objectId, auto) {
            endOfGroups.emplace_back(group, objectId);
            return folly::Expected<folly::Unit, moxygen::MoQPublishError>(
                folly::unit);
          });
  EXPECT_CALL(*mockConsumer, endOfFetch())
      .WillOnce(
          ::testing::Return(
              folly::Expected<folly::Unit, moxygen::MoQPublishError>(
                  folly::unit)));

  // Group 1 runs to completion and gets its marker at object 4; group 2 is cut
  // off at object 2, so it gets none.
  auto window = moxygen::resolveFetchWindow(
      params_, moxygen::StandaloneFetch({1, 0}, {2, 3}));
  folly::coro::blockingWait(
      publisher_->fetchOneSubgroupPerGroup(params_, mockConsumer, window));

  const std::vector<std::pair<uint64_t, uint64_t>> expectedObjects{
      {1, 0}, {1, 1}, {1, 2}, {1, 3}, {2, 0}, {2, 1}, {2, 2}};
  const std::vector<std::pair<uint64_t, uint64_t>> expectedEndOfGroups{{1, 4}};
  EXPECT_EQ(objects, expectedObjects);
  EXPECT_EQ(endOfGroups, expectedEndOfGroups);
}

CO_TEST_F(MoQTrackServerTest, FetchOkReportsTheEndOfTheRequestedRange) {
  MoQTrackServerTest::CreateDefaultTrackNamespace();
  moxygen::Fetch req;
  req.requestID = 0;
  req.fullTrackName.trackNamespace = track_;
  req.args = moxygen::StandaloneFetch({2, 0}, {4, 1});

  auto mockConsumer =
      std::make_shared<testing::NiceMock<moxygen::MockFetchConsumer>>();
  ON_CALL(
      *mockConsumer,
      object(
          testing::_,
          testing::_,
          testing::_,
          testing::_,
          testing::_,
          testing::_,
          testing::_))
      .WillByDefault(
          ::testing::Return(
              folly::Expected<folly::Unit, moxygen::MoQPublishError>(
                  folly::unit)));
  folly::coro::Baton done;
  EXPECT_CALL(*mockConsumer, endOfFetch()).WillOnce([&done] {
    done.post();
    return folly::Expected<folly::Unit, moxygen::MoQPublishError>(folly::unit);
  });

  auto result = co_await publisher_->fetch(req, mockConsumer);
  CO_ASSERT_TRUE(result.hasValue());
  const auto& ok = result.value()->fetchOk();
  // End Location is the request's own end, which the track doesn't cut short.
  EXPECT_EQ(ok.endLocation, (moxygen::AbsoluteLocation{4, 1}));
  EXPECT_EQ(ok.endOfTrack, 0);
  EXPECT_EQ(ok.groupOrder, moxygen::GroupOrder::OldestFirst);

  co_await done;
}

CO_TEST_F(MoQTrackServerTest, FetchOkReportsEndOfTrackWhenTheRangeCoversIt) {
  MoQTrackServerTest::CreateDefaultTrackNamespace();
  moxygen::Fetch req;
  req.requestID = 0;
  req.fullTrackName.trackNamespace = track_;
  req.args = moxygen::StandaloneFetch({0, 0}, {10, 0});

  auto mockConsumer =
      std::make_shared<testing::NiceMock<moxygen::MockFetchConsumer>>();
  ON_CALL(
      *mockConsumer,
      object(
          testing::_,
          testing::_,
          testing::_,
          testing::_,
          testing::_,
          testing::_,
          testing::_))
      .WillByDefault(
          ::testing::Return(
              folly::Expected<folly::Unit, moxygen::MoQPublishError>(
                  folly::unit)));
  folly::coro::Baton done;
  EXPECT_CALL(*mockConsumer, endOfFetch()).WillOnce([&done] {
    done.post();
    return folly::Expected<folly::Unit, moxygen::MoQPublishError>(folly::unit);
  });

  auto result = co_await publisher_->fetch(req, mockConsumer);
  CO_ASSERT_TRUE(result.hasValue());
  const auto& ok = result.value()->fetchOk();
  // The request asked for all of group 10; the track stops after object 1, so
  // End Location is clamped to one past it rather than the requested {11, 0}.
  EXPECT_EQ(ok.endLocation, (moxygen::AbsoluteLocation{10, 2}));
  EXPECT_EQ(ok.endOfTrack, 1);

  co_await done;
}

CO_TEST_F(MoQTrackServerTest, FetchOutsideTheTrackSendsNoObjects) {
  MoQTrackServerTest::CreateDefaultTrackNamespace();
  moxygen::Fetch req;
  req.requestID = 0;
  req.fullTrackName.trackNamespace = track_;
  req.args = moxygen::StandaloneFetch({50, 0}, {60, 0});

  auto mockConsumer = std::make_shared<moxygen::MockFetchConsumer>();
  EXPECT_CALL(
      *mockConsumer,
      object(
          testing::_,
          testing::_,
          testing::_,
          testing::_,
          testing::_,
          testing::_,
          testing::_))
      .Times(0);
  folly::coro::Baton done;
  EXPECT_CALL(*mockConsumer, endOfFetch()).WillOnce([&done] {
    done.post();
    return folly::Expected<folly::Unit, moxygen::MoQPublishError>(folly::unit);
  });

  auto result = co_await publisher_->fetch(req, mockConsumer);
  CO_ASSERT_TRUE(result.hasValue());
  // The track ends long before the request starts.  Reporting the track's end
  // would put End Location below Start Location, which a receiver must treat
  // as a protocol violation, so the response covers a zero-length range.
  EXPECT_EQ(
      result.value()->fetchOk().endLocation,
      (moxygen::AbsoluteLocation{50, 0}));
  EXPECT_EQ(result.value()->fetchOk().endOfTrack, 0);

  co_await done;
}

CO_TEST_F(MoQTrackServerTest, FetchOfADatagramTrackFlagsEachObject) {
  MoQTrackServerTest::CreateDefaultTrackNamespace();
  track_.trackNamespace[1] =
      std::to_string(static_cast<int>(moxygen::ForwardingPreference::DATAGRAM));
  moxygen::Fetch req;
  req.requestID = 0;
  req.fullTrackName.trackNamespace = track_;
  req.args = moxygen::StandaloneFetch({0, 0}, {1, 0});

  auto mockConsumer = std::make_shared<moxygen::MockFetchConsumer>();
  // group, subgroup, objectID, forwardingPreferenceIsDatagram
  std::vector<std::tuple<uint64_t, uint64_t, uint64_t, bool>> received;
  EXPECT_CALL(
      *mockConsumer,
      object(
          testing::_,
          testing::_,
          testing::_,
          testing::_,
          testing::_,
          testing::_,
          testing::_))
      .WillRepeatedly([&received](
                          uint64_t group,
                          uint64_t subgroup,
                          uint64_t objectId,
                          std::unique_ptr<folly::IOBuf>,
                          const auto&,
                          auto,
                          bool isDatagram) {
        received.emplace_back(group, subgroup, objectId, isDatagram);
        return folly::Expected<folly::Unit, moxygen::MoQPublishError>(
            folly::unit);
      });
  folly::coro::Baton done;
  EXPECT_CALL(*mockConsumer, endOfFetch()).WillOnce([&done] {
    done.post();
    return folly::Expected<folly::Unit, moxygen::MoQPublishError>(folly::unit);
  });

  auto result = co_await publisher_->fetch(req, mockConsumer);
  CO_ASSERT_TRUE(result.hasValue());
  co_await done;

  // A datagram object carries no subgroup, so every object is flagged and sits
  // in subgroup 0 -- the framer omits the field entirely from draft 16.
  const std::vector<std::tuple<uint64_t, uint64_t, uint64_t, bool>> expected{
      {0, 0, 0, true}, {0, 0, 1, true}, {1, 0, 0, true}, {1, 0, 1, true}};
  EXPECT_EQ(received, expected);
}

CO_TEST_F(MoQTrackServerTest, CancelAllStopsAnInFlightFetch) {
  MoQTrackServerTest::CreateDefaultTrackNamespace();
  // Groups 0-2 of two objects at 50ms each, so the whole track is ~300ms and
  // the wait below is comfortably longer than the tail we expect not to get.
  track_.trackNamespace[4] = "2";
  track_.trackNamespace[9] = "50";
  moxygen::Fetch req;
  req.requestID = 0;
  req.fullTrackName.trackNamespace = track_;
  req.args = moxygen::StandaloneFetch({0, 0}, {2, 0});

  auto mockConsumer =
      std::make_shared<testing::NiceMock<moxygen::MockFetchConsumer>>();
  // Posted on the second object so the cancel lands mid-track rather than at a
  // point the clock happens to pick.
  auto midTrack = std::make_shared<folly::coro::Baton>();
  constexpr int kObjectsBeforeCancel = 2;
  int objects = 0;
  ON_CALL(
      *mockConsumer,
      object(
          testing::_,
          testing::_,
          testing::_,
          testing::_,
          testing::_,
          testing::_,
          testing::_))
      .WillByDefault([&objects, midTrack] {
        if (++objects == kObjectsBeforeCancel) {
          midTrack->post();
        }
        return folly::Expected<folly::Unit, moxygen::MoQPublishError>(
            folly::unit);
      });
  // A cancelled fetch unwinds without completing, so endOfFetch never fires.
  EXPECT_CALL(*mockConsumer, endOfFetch()).Times(0);

  auto result = co_await publisher_->fetch(req, mockConsumer);
  CO_ASSERT_TRUE(result.hasValue());
  co_await *midTrack;
  publisher_->cancelAll();

  // The rest of the track would have landed inside this wait, so a count that
  // has not moved is the generator having stopped, not the clock being slow.
  co_await folly::coro::sleep(std::chrono::milliseconds(300));
  EXPECT_EQ(objects, kObjectsBeforeCancel);
}

TEST_F(MoQTrackServerTest, FetchRejectsAJoiningRequest) {
  MoQTrackServerTest::CreateDefaultTrackNamespace();
  moxygen::Fetch req;
  req.requestID = 0;
  req.fullTrackName.trackNamespace = track_;
  req.args = moxygen::JoiningFetch(
      moxygen::RequestID(1), 0, moxygen::FetchType::RELATIVE_JOINING);

  auto result = folly::coro::blockingWait(publisher_->fetch(req, nullptr));

  ASSERT_TRUE(result.hasError());
  EXPECT_EQ(result.error().errorCode, moxygen::FetchErrorCode::NOT_SUPPORTED);
}

TEST_F(MoQTrackServerTest, FetchRejectsDescendingGroupOrder) {
  MoQTrackServerTest::CreateDefaultTrackNamespace();
  moxygen::Fetch req;
  req.requestID = 0;
  req.fullTrackName.trackNamespace = track_;
  req.groupOrder = moxygen::GroupOrder::NewestFirst;

  auto result = folly::coro::blockingWait(publisher_->fetch(req, nullptr));

  ASSERT_TRUE(result.hasError());
  EXPECT_EQ(result.error().errorCode, moxygen::FetchErrorCode::NOT_SUPPORTED);
}

// requestUpdate Testing
// Verify that the handle returned by subscribe() is a MoQForwarder::Subscriber
// that properly handles requestUpdate forward=0 (pause) and forward=1 (resume),
// and that objects published while paused are not delivered to the consumer.
TEST_F(MoQTrackServerTest, RequestUpdateTogglesForward) {
  using namespace testing;
  using namespace moxygen::test;

  auto session = std::make_shared<NiceMock<MockMoQSession>>();
  ON_CALL(*session, getNegotiatedVersion())
      .WillByDefault(
          Return(std::optional<uint64_t>(moxygen::kVersionDraftCurrent)));

  moxygen::TrackNamespace ns{std::vector<std::string>{"moq-test-00"}};
  auto forwarder = std::make_shared<moxygen::MoQForwarder>(
      moxygen::FullTrackName{ns, "test"});

  auto mockConsumer = std::make_shared<NiceMock<moxygen::MockTrackConsumer>>();
  ON_CALL(*mockConsumer, setTrackAlias(_))
      .WillByDefault(
          Return(folly::makeExpected<moxygen::MoQPublishError>(folly::unit)));
  ON_CALL(*mockConsumer, publishDone(_))
      .WillByDefault(
          Return(folly::makeExpected<moxygen::MoQPublishError>(folly::unit)));

  moxygen::SubscribeRequest sub;
  sub.requestID = moxygen::RequestID(1);
  sub.fullTrackName = forwarder->fullTrackName();
  sub.locType = moxygen::LocationType::LargestObject;
  sub.forward = true;

  auto subscriber = forwarder->addSubscriber(session, sub, mockConsumer);
  ASSERT_NE(subscriber, nullptr);
  EXPECT_TRUE(subscriber->shouldForward);

  // forward=0: pause delivery
  moxygen::RequestUpdate pauseUpdate;
  pauseUpdate.requestID = moxygen::RequestID(2);
  pauseUpdate.existingRequestID = sub.requestID;
  pauseUpdate.forward = false;
  auto pauseResult =
      folly::coro::blockingWait(subscriber->requestUpdate(pauseUpdate));
  ASSERT_TRUE(pauseResult.hasValue())
      << "requestUpdate(forward=0) must succeed";
  EXPECT_FALSE(subscriber->shouldForward);

  // Publish a subgroup while paused — consumer must not be called.
  EXPECT_CALL(*mockConsumer, beginSubgroup(0, 0, _, _)).Times(0);
  {
    auto sgRes = forwarder->beginSubgroup(0, 0, 0);
    ASSERT_TRUE(sgRes.hasValue());
    EXPECT_TRUE((*sgRes)->endOfSubgroup().hasValue());
  }

  // forward=1: resume delivery
  moxygen::RequestUpdate resumeUpdate;
  resumeUpdate.requestID = moxygen::RequestID(3);
  resumeUpdate.existingRequestID = sub.requestID;
  resumeUpdate.forward = true;
  auto resumeResult =
      folly::coro::blockingWait(subscriber->requestUpdate(resumeUpdate));
  ASSERT_TRUE(resumeResult.hasValue())
      << "requestUpdate(forward=1) must succeed";
  EXPECT_TRUE(subscriber->shouldForward);

  // Publish a subgroup while resumed — consumer must receive it.
  auto mockSg = std::make_shared<NiceMock<moxygen::MockSubgroupConsumer>>();
  ON_CALL(*mockSg, endOfSubgroup())
      .WillByDefault(
          Return(folly::makeExpected<moxygen::MoQPublishError>(folly::unit)));
  EXPECT_CALL(*mockConsumer, beginSubgroup(1, 0, _, _))
      .WillOnce(Return(
          folly::makeExpected<moxygen::MoQPublishError>(
              std::shared_ptr<moxygen::SubgroupConsumer>(mockSg))));
  {
    auto sgRes = forwarder->beginSubgroup(1, 0, 0);
    ASSERT_TRUE(sgRes.hasValue());
    EXPECT_TRUE((*sgRes)->endOfSubgroup().hasValue());
  }
}

// Forwarder sharing tests
//
// Every subscriber to a track attaches to one forwarder fed by one generator,
// so a subscriber that arrives mid-track joins it in progress.

namespace {

// Mock track consumer that records the groups it is asked to open a subgroup
// for, and posts `done` on publishDone.
class GroupRecorder {
 public:
  GroupRecorder() {
    auto ok = folly::makeExpected<moxygen::MoQPublishError>(folly::unit);
    ON_CALL(*consumer_, setTrackAlias(testing::_))
        .WillByDefault(testing::Return(ok));
    ON_CALL(*subgroup_, object(testing::_, testing::_, testing::_, testing::_))
        .WillByDefault(testing::Return(ok));
    ON_CALL(*subgroup_, endOfGroup(testing::_))
        .WillByDefault(testing::Return(ok));
    ON_CALL(*subgroup_, endOfSubgroup()).WillByDefault(testing::Return(ok));

    ON_CALL(*consumer_, publishDone(testing::_))
        .WillByDefault([this](const auto&) {
          done.post();
          return folly::makeExpected<moxygen::MoQPublishError>(folly::unit);
        });
    ON_CALL(
        *consumer_,
        beginSubgroup(testing::_, testing::_, testing::_, testing::_))
        .WillByDefault([this](uint64_t group, uint64_t, uint8_t, auto) {
          groups_.push_back(group);
          if (groups_.size() == groupTarget_) {
            reachedTarget_.post();
          }
          return folly::makeExpected<moxygen::MoQPublishError>(
              std::shared_ptr<moxygen::SubgroupConsumer>(subgroup_));
        });
  }

  std::shared_ptr<moxygen::TrackConsumer> consumer() const {
    return consumer_;
  }

  const std::vector<uint64_t>& groups() const {
    return groups_;
  }

  folly::coro::Baton done;

  // The generator runs on real time, so wait for its output, not the clock.
  folly::coro::Task<void> waitForGroups(size_t count) {
    if (groups_.size() >= count) {
      co_return;
    }
    groupTarget_ = count;
    reachedTarget_.reset();
    co_await reachedTarget_;
  }

 private:
  std::shared_ptr<testing::NiceMock<moxygen::MockTrackConsumer>> consumer_{
      std::make_shared<testing::NiceMock<moxygen::MockTrackConsumer>>()};
  std::shared_ptr<testing::NiceMock<moxygen::MockSubgroupConsumer>> subgroup_{
      std::make_shared<testing::NiceMock<moxygen::MockSubgroupConsumer>>()};
  std::vector<uint64_t> groups_;
  folly::coro::Baton reachedTarget_;
  size_t groupTarget_{0};
};

} // namespace

CO_TEST_F(MoQTrackServerTest, LateSubscriberJoinsTheTrackInProgress) {
  MoQTrackServerTest::CreateDefaultTrackNamespace();
  // 20 groups of two objects at 50ms each, so the track is still running when
  // the second subscriber arrives.
  track_.trackNamespace[4] = "20";
  track_.trackNamespace[9] = "50";

  GroupRecorder first;
  auto firstRes =
      co_await SubscribeAs(CreateSession(), MakeSubscribe(4), first.consumer());
  CO_ASSERT_TRUE(firstRes.hasValue());
  EXPECT_EQ(firstRes.value()->subscribeOk().trackAlias, moxygen::TrackAlias(4));
  EXPECT_FALSE(firstRes.value()->subscribeOk().largest.has_value());

  // Two groups in, so the second subscriber joins past the first's group.
  co_await first.waitForGroups(2);

  GroupRecorder second;
  auto secondRes = co_await SubscribeAs(
      CreateSession(), MakeSubscribe(7), second.consumer());
  CO_ASSERT_TRUE(secondRes.hasValue());
  // Each subscriber gets its own request ID as its alias, so a session that
  // subscribes to several tracks is never handed the same alias twice.
  EXPECT_EQ(
      secondRes.value()->subscribeOk().trackAlias, moxygen::TrackAlias(7));
  EXPECT_TRUE(secondRes.value()->subscribeOk().largest.has_value());

  co_await second.waitForGroups(1);
  publisher_->cancelAll();

  const auto& firstGroups = first.groups();
  const auto& secondGroups = second.groups();
  CO_ASSERT_LE(secondGroups.size(), firstGroups.size());
  EXPECT_GT(secondGroups.front(), firstGroups.front());
  // One generator feeding both: the late subscriber's groups are the tail of
  // what the first subscriber saw, not a second copy starting at group 0.
  EXPECT_EQ(
      secondGroups,
      std::vector<uint64_t>(
          firstGroups.end() - secondGroups.size(), firstGroups.end()));
}

CO_TEST_F(MoQTrackServerTest, TrackRestartsAfterItEnds) {
  MoQTrackServerTest::CreateDefaultTrackNamespace();
  track_.trackNamespace[4] = "1";
  track_.trackNamespace[9] = "1";

  auto session = CreateSession();
  GroupRecorder first;
  auto firstRes =
      co_await SubscribeAs(session, MakeSubscribe(0), first.consumer());
  CO_ASSERT_TRUE(firstRes.hasValue());
  co_await first.done;

  // The finished track is retired, so this regenerates it rather than failing
  // against a drained forwarder.
  GroupRecorder second;
  auto secondRes =
      co_await SubscribeAs(session, MakeSubscribe(1), second.consumer());
  CO_ASSERT_TRUE(secondRes.hasValue());
  co_await second.done;
  EXPECT_EQ(first.groups(), second.groups());
}

CO_TEST_F(MoQTrackServerTest, UnsubscribeStopsTheGenerator) {
  MoQTrackServerTest::CreateDefaultTrackNamespace();
  track_.trackNamespace[4] = "20";
  track_.trackNamespace[9] = "50";

  GroupRecorder recorder;
  auto res = co_await SubscribeAs(
      CreateSession(), MakeSubscribe(0), recorder.consumer());
  CO_ASSERT_TRUE(res.hasValue());
  co_await recorder.waitForGroups(1);

  res.value()->unsubscribe();
  const auto groupsAtUnsubscribe = recorder.groups().size();
  co_await folly::coro::sleep(std::chrono::milliseconds(300));
  EXPECT_EQ(recorder.groups().size(), groupsAtUnsubscribe);
}

CO_TEST_F(MoQTrackServerTest, CancelAllStopsAnInFlightTrack) {
  MoQTrackServerTest::CreateDefaultTrackNamespace();
  track_.trackNamespace[4] = "20";
  track_.trackNamespace[9] = "50";

  GroupRecorder recorder;
  auto res = co_await SubscribeAs(
      CreateSession(), MakeSubscribe(0), recorder.consumer());
  CO_ASSERT_TRUE(res.hasValue());
  co_await recorder.waitForGroups(1);

  publisher_->cancelAll();
  // The generator checks for cancellation inside the object loop, so it can
  // open one more group before it unwinds.
  const auto groupsAtCancel = recorder.groups().size();
  co_await folly::coro::sleep(std::chrono::milliseconds(300));
  EXPECT_LE(recorder.groups().size(), groupsAtCancel + 1);
  EXPECT_LT(recorder.groups().size(), 20u);
}

// Subgroup header encoding tests
//
// The server must pick the most compact subgroup header the draft allows:
// elide the subgroup ID when it is zero or equal to the subgroup's first
// object ID, only claim extensions when the track carries them, and mark the
// subgroup that ends the group.

namespace {

// Collects the BeginSubgroupOptions the server publishes with, keyed by
// subgroup ID.  Every subgroup in a moq-test group uses the same options
// regardless of group number.
class SubgroupOptionsRecorder {
 public:
  explicit SubgroupOptionsRecorder(
      std::shared_ptr<moxygen::MockTrackConsumer> consumer)
      : consumer_(std::move(consumer)) {
    subgroupConsumer_ =
        std::make_shared<testing::NiceMock<moxygen::MockSubgroupConsumer>>();
    auto ok = folly::makeExpected<moxygen::MoQPublishError>(folly::unit);
    ON_CALL(
        *subgroupConsumer_,
        object(testing::_, testing::_, testing::_, testing::_))
        .WillByDefault(testing::Return(ok));
    ON_CALL(*subgroupConsumer_, endOfGroup(testing::_))
        .WillByDefault(testing::Return(ok));
    ON_CALL(*subgroupConsumer_, endOfSubgroup())
        .WillByDefault(testing::Return(ok));

    EXPECT_CALL(
        *consumer_,
        beginSubgroup(testing::_, testing::_, testing::_, testing::_))
        .WillRepeatedly(
            [this](
                uint64_t,
                uint64_t subgroupID,
                uint8_t,
                moxygen::TrackConsumer::BeginSubgroupOptions options) {
              optionsBySubgroup_[subgroupID] = options;
              return folly::makeExpected<moxygen::MoQPublishError>(
                  std::shared_ptr<moxygen::SubgroupConsumer>(
                      subgroupConsumer_));
            });
  }

  const moxygen::TrackConsumer::BeginSubgroupOptions& operator[](
      uint64_t subgroupID) const {
    auto it = optionsBySubgroup_.find(subgroupID);
    EXPECT_NE(it, optionsBySubgroup_.end())
        << "no subgroup " << subgroupID << " was opened";
    return it->second;
  }

  size_t numSubgroups() const {
    return optionsBySubgroup_.size();
  }

 private:
  std::shared_ptr<moxygen::MockTrackConsumer> consumer_;
  std::shared_ptr<testing::NiceMock<moxygen::MockSubgroupConsumer>>
      subgroupConsumer_;
  std::map<uint64_t, moxygen::TrackConsumer::BeginSubgroupOptions>
      optionsBySubgroup_;
};

} // namespace

TEST_F(MoQTrackServerTest, SubgroupEncodingOneSubgroupPerGroup) {
  MoQTrackServerTest::CreateDefaultMoQTestParameters();
  // -1 disables an extension; the fixture default of 0 selects extension ID 0
  params_.testIntegerExtension = -1;
  params_.testVariableExtension = -1;
  auto mockConsumer = std::make_shared<moxygen::MockTrackConsumer>();
  SubgroupOptionsRecorder recorder(mockConsumer);

  folly::coro::blockingWait(
      publisher_->sendOneSubgroupPerGroup(params_, mockConsumer));

  ASSERT_EQ(recorder.numSubgroups(), 1);
  // Subgroup 0 is implied by the stream type, the track has no extensions, and
  // the group's only subgroup necessarily carries its last object.
  EXPECT_EQ(recorder[0].subgroupIDFormat, moxygen::SubgroupIDFormat::Zero);
  EXPECT_FALSE(recorder[0].includeExtensions);
  EXPECT_TRUE(recorder[0].containsLastInGroup);
  EXPECT_TRUE(recorder[0].beginsWithFirstObject);
}

TEST_F(MoQTrackServerTest, SubgroupEncodingClaimsExtensionsWhenConfigured) {
  MoQTrackServerTest::CreateDefaultMoQTestParameters();
  params_.testIntegerExtension = 1;
  params_.testVariableExtension = -1;
  auto mockConsumer = std::make_shared<moxygen::MockTrackConsumer>();
  SubgroupOptionsRecorder recorder(mockConsumer);

  folly::coro::blockingWait(
      publisher_->sendOneSubgroupPerGroup(params_, mockConsumer));

  EXPECT_TRUE(recorder[0].includeExtensions);
}

TEST_F(MoQTrackServerTest, SubgroupEncodingOneSubgroupPerObject) {
  MoQTrackServerTest::CreateDefaultMoQTestParameters();
  params_.forwardingPreference = moxygen::ForwardingPreference(1);
  auto mockConsumer = std::make_shared<moxygen::MockTrackConsumer>();
  SubgroupOptionsRecorder recorder(mockConsumer);

  folly::coro::blockingWait(
      publisher_->sendOneSubgroupPerObject(params_, mockConsumer));

  // Objects 0 and 1 each get their own subgroup, numbered after the object.
  ASSERT_EQ(recorder.numSubgroups(), 2);
  EXPECT_EQ(recorder[0].subgroupIDFormat, moxygen::SubgroupIDFormat::Zero);
  EXPECT_FALSE(recorder[0].containsLastInGroup);
  // Subgroup 1 holds object 1, so the ID is derivable from the first object.
  EXPECT_EQ(
      recorder[1].subgroupIDFormat, moxygen::SubgroupIDFormat::FirstObject);
  EXPECT_TRUE(recorder[1].containsLastInGroup);
}

TEST_F(MoQTrackServerTest, SubgroupEncodingTwoSubgroupsPerGroup) {
  MoQTrackServerTest::CreateDefaultMoQTestParameters();
  params_.forwardingPreference = moxygen::ForwardingPreference(2);
  params_.lastObjectInTrack = 2;
  params_.objectsPerGroup = 2;
  auto mockConsumer = std::make_shared<moxygen::MockTrackConsumer>();
  SubgroupOptionsRecorder recorder(mockConsumer);

  folly::coro::blockingWait(
      publisher_->sendTwoSubgroupsPerGroup(params_, mockConsumer));

  ASSERT_EQ(recorder.numSubgroups(), 2);
  // Objects 0 and 2 land on subgroup 0, object 1 on subgroup 1.
  EXPECT_EQ(recorder[0].subgroupIDFormat, moxygen::SubgroupIDFormat::Zero);
  EXPECT_TRUE(recorder[0].containsLastInGroup);
  EXPECT_EQ(
      recorder[1].subgroupIDFormat, moxygen::SubgroupIDFormat::FirstObject);
  EXPECT_FALSE(recorder[1].containsLastInGroup);
}

TEST_F(MoQTrackServerTest, SubgroupEncodingFallsBackToExplicitSubgroupID) {
  MoQTrackServerTest::CreateDefaultMoQTestParameters();
  params_.forwardingPreference = moxygen::ForwardingPreference(2);
  params_.objectsPerGroup = 2;
  params_.objectIncrement = 3;
  params_.lastObjectInTrack = 6;
  auto mockConsumer = std::make_shared<moxygen::MockTrackConsumer>();
  SubgroupOptionsRecorder recorder(mockConsumer);

  folly::coro::blockingWait(
      publisher_->sendTwoSubgroupsPerGroup(params_, mockConsumer));

  ASSERT_EQ(recorder.numSubgroups(), 2);
  // Objects 0 and 6 land on subgroup 0, object 3 on subgroup 1.  Subgroup 1
  // starts at object 3, so neither elision applies and the ID goes on the wire.
  EXPECT_EQ(recorder[0].subgroupIDFormat, moxygen::SubgroupIDFormat::Zero);
  EXPECT_TRUE(recorder[0].containsLastInGroup);
  EXPECT_EQ(recorder[1].subgroupIDFormat, moxygen::SubgroupIDFormat::Present);
  EXPECT_FALSE(recorder[1].containsLastInGroup);
}

TEST_F(MoQTrackServerTest, EndOfGroupMarksTheLastObjectAGroupCarries) {
  MoQTrackServerTest::CreateDefaultMoQTestParameters();
  // Objects 0, 2 and 4 of each group.  The increment doesn't divide the range,
  // so the group's last object is 4 while lastObjectInTrack is 5.
  params_.lastGroupInTrack = 1;
  params_.objectsPerGroup = 2;
  params_.objectIncrement = 2;
  params_.lastObjectInTrack = 5;
  params_.sendEndOfGroupMarkers = true;

  auto mockConsumer = std::make_shared<moxygen::MockTrackConsumer>();
  // group, objectID
  std::vector<std::pair<uint64_t, uint64_t>> objects;
  std::vector<std::pair<uint64_t, uint64_t>> endOfGroups;
  for (uint64_t group = 0; group <= params_.lastGroupInTrack; group++) {
    auto mockSubgroup = std::make_shared<moxygen::MockSubgroupConsumer>();
    EXPECT_CALL(*mockConsumer, beginSubgroup(group, 0, testing::_, testing::_))
        .WillRepeatedly(testing::Return(mockSubgroup));
    EXPECT_CALL(
        *mockSubgroup, object(testing::_, testing::_, testing::_, testing::_))
        .WillRepeatedly(
            [&objects, group](uint64_t objectId, auto, const auto&, auto) {
              objects.emplace_back(group, objectId);
              return folly::Expected<folly::Unit, moxygen::MoQPublishError>(
                  folly::unit);
            });
    EXPECT_CALL(*mockSubgroup, endOfGroup(testing::_))
        .WillRepeatedly([&endOfGroups, group](uint64_t objectId) {
          endOfGroups.emplace_back(group, objectId);
          return folly::Expected<folly::Unit, moxygen::MoQPublishError>(
              folly::unit);
        });
    EXPECT_CALL(*mockSubgroup, endOfSubgroup())
        .WillRepeatedly(
            ::testing::Return(
                folly::Expected<folly::Unit, moxygen::MoQPublishError>(
                    folly::unit)));
  }

  folly::coro::blockingWait(
      publisher_->sendOneSubgroupPerGroup(params_, mockConsumer));

  const std::vector<std::pair<uint64_t, uint64_t>> expectedObjects{
      {0, 0}, {0, 2}, {1, 0}, {1, 2}};
  const std::vector<std::pair<uint64_t, uint64_t>> expectedEndOfGroups{
      {0, 4}, {1, 4}};
  EXPECT_EQ(objects, expectedObjects);
  EXPECT_EQ(endOfGroups, expectedEndOfGroups);
}

TEST_F(MoQTrackServerTest, DatagramSignalsEndOfGroupOnLastObject) {
  MoQTrackServerTest::CreateDefaultMoQTestParameters();
  params_.forwardingPreference = moxygen::ForwardingPreference(3);
  params_.lastGroupInTrack = 1;
  params_.lastObjectInTrack = 2;
  params_.objectsPerGroup = 2;

  moxygen::SubscribeRequest sub;
  sub.requestID = 0;
  auto mockConsumer =
      std::make_shared<testing::NiceMock<moxygen::MockTrackConsumer>>();
  ON_CALL(*mockConsumer, setTrackAlias(testing::_))
      .WillByDefault(
          testing::Return(
              folly::makeExpected<moxygen::MoQPublishError>(folly::unit)));

  std::vector<std::pair<uint64_t, bool>> endOfGroupByObject;
  ON_CALL(*mockConsumer, datagram(testing::_, testing::_, testing::_))
      .WillByDefault([&endOfGroupByObject](
                         const moxygen::ObjectHeader& header,
                         moxygen::Payload,
                         bool endOfGroup) {
        endOfGroupByObject.emplace_back(header.id, endOfGroup);
        return folly::makeExpected<moxygen::MoQPublishError>(folly::unit);
      });

  folly::coro::blockingWait(
      publisher_->sendDatagram(sub.requestID, params_, mockConsumer));

  // Two groups of objects 0..2; only object 2 ends its group.
  const std::vector<std::pair<uint64_t, bool>> expected{
      {0, false}, {1, false}, {2, true}, {0, false}, {1, false}, {2, true}};
  EXPECT_EQ(endOfGroupByObject, expected);
}

TEST_F(MoQTrackServerTest, DatagramEndOfGroupMarkerIsAnEmptyStatusObject) {
  MoQTrackServerTest::CreateDefaultMoQTestParameters();
  params_.forwardingPreference = moxygen::ForwardingPreference::DATAGRAM;
  params_.lastGroupInTrack = 0;
  params_.objectsPerGroup = 2;
  params_.objectIncrement = 2;
  // lastObjectInTrack is off the increment grid, so the group stops at object
  // 2 and never reaches 3.
  params_.lastObjectInTrack = 3;
  params_.sendEndOfGroupMarkers = true;

  auto mockConsumer =
      std::make_shared<testing::NiceMock<moxygen::MockTrackConsumer>>();
  ON_CALL(*mockConsumer, setTrackAlias(testing::_))
      .WillByDefault(
          testing::Return(
              folly::makeExpected<moxygen::MoQPublishError>(folly::unit)));

  // (object, status, end-of-group bit, carries extensions)
  std::vector<std::tuple<uint64_t, moxygen::ObjectStatus, bool, bool>> sent;
  ON_CALL(*mockConsumer, datagram(testing::_, testing::_, testing::_))
      .WillByDefault([&sent](
                         const moxygen::ObjectHeader& header,
                         moxygen::Payload,
                         bool endOfGroup) {
        sent.emplace_back(
            header.id, header.status, endOfGroup, !header.extensions.empty());
        return folly::makeExpected<moxygen::MoQPublishError>(folly::unit);
      });

  folly::coro::blockingWait(
      publisher_->sendDatagram(moxygen::RequestID(0), params_, mockConsumer));

  // The marker lands on object 2, drops the extensions that draft 15+ rejects
  // on a non-NORMAL status object, and leaves the end-of-group bit clear
  // because the type byte cannot carry it alongside a status.
  const std::vector<std::tuple<uint64_t, moxygen::ObjectStatus, bool, bool>>
      expected{
          {0, moxygen::ObjectStatus::NORMAL, false, true},
          {2, moxygen::ObjectStatus::END_OF_GROUP, false, false}};
  EXPECT_EQ(sent, expected);
}

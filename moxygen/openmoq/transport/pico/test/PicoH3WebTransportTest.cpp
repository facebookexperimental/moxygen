/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <folly/portability/GTest.h>
#include <folly/io/IOBuf.h>
#include <gmock/gmock.h>
#include <picoquic.h>

#include <moxygen/openmoq/transport/pico/PicoH3WebTransport.h>

using namespace moxygen;
using namespace testing;

namespace {

// Mock picoquic connection - we can't easily mock the actual picoquic
// structures, so these tests focus on the adapter's internal logic
// that can be tested without a real connection.

class PicoH3WebTransportTest : public Test {
 protected:
  void SetUp() override {
    // Tests that require actual picoquic connection will be skipped
    // This test suite focuses on interface contracts and error handling
  }
};

// Test that error codes are defined (interface contract verification)
TEST_F(PicoH3WebTransportTest, StreamCreationErrorCodeDefined) {
  // Verify STREAM_CREATION_ERROR is a valid error code
  auto errorCode = proxygen::WebTransport::ErrorCode::STREAM_CREATION_ERROR;
  EXPECT_NE(static_cast<int>(errorCode), 0);
}

// Test that INVALID_STREAM_ID error code is defined
TEST_F(PicoH3WebTransportTest, InvalidStreamIdErrorCodeDefined) {
  // Verify INVALID_STREAM_ID is a valid error code
  auto errorCode = proxygen::WebTransport::ErrorCode::INVALID_STREAM_ID;
  EXPECT_NE(static_cast<int>(errorCode), 0);
}

// Test error code definitions
TEST_F(PicoH3WebTransportTest, ErrorCodeValues) {
  // Verify error codes are properly defined
  EXPECT_NE(
      static_cast<int>(proxygen::WebTransport::ErrorCode::STREAM_CREATION_ERROR),
      static_cast<int>(proxygen::WebTransport::ErrorCode::INVALID_STREAM_ID));
  EXPECT_NE(
      static_cast<int>(proxygen::WebTransport::ErrorCode::INVALID_STREAM_ID),
      static_cast<int>(proxygen::WebTransport::ErrorCode::GENERIC_ERROR));
}

// Test IOBuf creation for stream data
TEST_F(PicoH3WebTransportTest, IOBufCreation) {
  const std::string testData = "Hello, WebTransport!";
  auto buf = folly::IOBuf::copyBuffer(testData);

  ASSERT_NE(buf, nullptr);
  EXPECT_EQ(buf->computeChainDataLength(), testData.size());
  EXPECT_EQ(
      std::string(reinterpret_cast<const char*>(buf->data()), buf->length()),
      testData);
}

// Test empty IOBuf handling
TEST_F(PicoH3WebTransportTest, EmptyIOBuf) {
  auto buf = folly::IOBuf::copyBuffer("");
  ASSERT_NE(buf, nullptr);
  EXPECT_EQ(buf->computeChainDataLength(), 0);
}

// Test stream ID classification (bidi vs uni)
TEST_F(PicoH3WebTransportTest, StreamIdClassification) {
  // QUIC stream ID format:
  // - Bit 0: Client-initiated (0) or Server-initiated (1)
  // - Bit 1: Bidirectional (0) or Unidirectional (1)

  // Client-initiated bidirectional: 0, 4, 8, ...
  EXPECT_TRUE(PICOQUIC_IS_BIDIR_STREAM_ID(0));
  EXPECT_TRUE(PICOQUIC_IS_BIDIR_STREAM_ID(4));
  EXPECT_TRUE(PICOQUIC_IS_BIDIR_STREAM_ID(8));

  // Server-initiated bidirectional: 1, 5, 9, ...
  EXPECT_TRUE(PICOQUIC_IS_BIDIR_STREAM_ID(1));
  EXPECT_TRUE(PICOQUIC_IS_BIDIR_STREAM_ID(5));

  // Client-initiated unidirectional: 2, 6, 10, ...
  EXPECT_FALSE(PICOQUIC_IS_BIDIR_STREAM_ID(2));
  EXPECT_FALSE(PICOQUIC_IS_BIDIR_STREAM_ID(6));

  // Server-initiated unidirectional: 3, 7, 11, ...
  EXPECT_FALSE(PICOQUIC_IS_BIDIR_STREAM_ID(3));
  EXPECT_FALSE(PICOQUIC_IS_BIDIR_STREAM_ID(7));
}

} // namespace

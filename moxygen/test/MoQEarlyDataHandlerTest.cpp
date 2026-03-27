/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <folly/portability/GTest.h>
#include <moxygen/MoQEarlyDataHandler.h>

using namespace moxygen;

class MoQEarlyDataHandlerTest : public ::testing::Test {
 protected:
  MoQEarlyDataHandler handler_;
};

TEST_F(MoQEarlyDataHandlerTest, GetAndValidateRoundtrip) {
  handler_.setCurrentParams(100, 1024);
  auto buf = handler_.get();
  ASSERT_NE(buf, nullptr);

  // Validate with same params should succeed
  EXPECT_TRUE(handler_.validate(std::string("moqt-17"), buf));
}

TEST_F(MoQEarlyDataHandlerTest, ValidateWithLargerCurrentParams) {
  // Serialize with small params
  handler_.setCurrentParams(50, 512);
  auto buf = handler_.get();
  ASSERT_NE(buf, nullptr);

  // Increase current params — cached (smaller) values should still be valid
  handler_.setCurrentParams(100, 1024);
  EXPECT_TRUE(handler_.validate(std::string("moqt-17"), buf));
}

TEST_F(MoQEarlyDataHandlerTest, ValidateRejectsCachedMaxRequestIDTooLarge) {
  // Serialize with large MAX_REQUEST_ID
  handler_.setCurrentParams(200, 1024);
  auto buf = handler_.get();
  ASSERT_NE(buf, nullptr);

  // Lower current MAX_REQUEST_ID — cached value is too large
  handler_.setCurrentParams(100, 1024);
  EXPECT_FALSE(handler_.validate(std::string("moqt-17"), buf));
}

TEST_F(
    MoQEarlyDataHandlerTest,
    ValidateRejectsCachedMaxAuthTokenCacheSizeTooLarge) {
  // Serialize with large MAX_AUTH_TOKEN_CACHE_SIZE
  handler_.setCurrentParams(100, 2048);
  auto buf = handler_.get();
  ASSERT_NE(buf, nullptr);

  // Lower current MAX_AUTH_TOKEN_CACHE_SIZE — cached value is too large
  handler_.setCurrentParams(100, 1024);
  EXPECT_FALSE(handler_.validate(std::string("moqt-17"), buf));
}

TEST_F(MoQEarlyDataHandlerTest, ValidateWithZeroParams) {
  handler_.setCurrentParams(0, 0);
  auto buf = handler_.get();
  ASSERT_NE(buf, nullptr);

  EXPECT_TRUE(handler_.validate(std::string("moqt-17"), buf));
}

TEST_F(MoQEarlyDataHandlerTest, ValidateWithNullAppParams) {
  handler_.setCurrentParams(100, 1024);
  EXPECT_FALSE(handler_.validate(std::string("moqt-17"), nullptr));
}

TEST_F(MoQEarlyDataHandlerTest, ValidateWithEmptyAppParams) {
  handler_.setCurrentParams(100, 1024);
  auto emptyBuf = folly::IOBuf::create(0);
  EXPECT_FALSE(handler_.validate(std::string("moqt-17"), emptyBuf));
}

TEST_F(MoQEarlyDataHandlerTest, ValidateWithTruncatedAppParams) {
  // Only encode one varint instead of two
  handler_.setCurrentParams(100, 1024);
  auto buf = handler_.get();
  ASSERT_NE(buf, nullptr);

  // Truncate the buffer to only contain the first varint
  folly::io::Cursor cursor(buf.get());
  auto firstVarint = quic::follyutils::decodeQuicInteger(cursor);
  ASSERT_TRUE(firstVarint.has_value());
  auto truncatedBuf = folly::IOBuf::create(firstVarint->second);
  memcpy(truncatedBuf->writableData(), buf->data(), firstVarint->second);
  truncatedBuf->append(firstVarint->second);

  EXPECT_FALSE(handler_.validate(std::string("moqt-17"), truncatedBuf));
}

TEST_F(MoQEarlyDataHandlerTest, ValidateWithNoAlpn) {
  handler_.setCurrentParams(100, 1024);
  auto buf = handler_.get();
  ASSERT_NE(buf, nullptr);

  // No ALPN — should still validate based on params
  EXPECT_TRUE(handler_.validate(std::nullopt, buf));
}

TEST_F(MoQEarlyDataHandlerTest, ValidateExactMatch) {
  handler_.setCurrentParams(100, 1024);
  auto buf = handler_.get();
  ASSERT_NE(buf, nullptr);

  // Exact same params should validate
  handler_.setCurrentParams(100, 1024);
  EXPECT_TRUE(handler_.validate(std::string("moqt-17"), buf));
}

TEST_F(MoQEarlyDataHandlerTest, LargeValues) {
  handler_.setCurrentParams(quic::kEightByteLimit, quic::kEightByteLimit);
  auto buf = handler_.get();
  ASSERT_NE(buf, nullptr);

  EXPECT_TRUE(handler_.validate(std::string("moqt-17"), buf));
}

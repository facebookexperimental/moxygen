/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <folly/Expected.h>
#include <folly/portability/GTest.h>
#include <moxygen/MoQTokenCache.h>

using namespace moxygen;

class MoQTokenCacheTest : public ::testing::Test {
 protected:
  MoQTokenCache cache_{24};
  uint64_t tokenType_ = 1;
  MoQTokenCache::TokenValue makeToken() {
    return std::string("test");
  }
};
TEST_F(MoQTokenCacheTest, RegisterTokenSuccess) {
  auto result = cache_.registerToken(tokenType_, makeToken());
  EXPECT_TRUE(result.hasValue());
}

TEST_F(MoQTokenCacheTest, RegisterTokenLimitExceeded) {
  auto token = makeToken();
  auto result = cache_.registerToken(tokenType_, token);
  auto alias = result.value();
  EXPECT_TRUE(result.hasValue());
  EXPECT_EQ(cache_.getTotalSize(), MoQTokenCache::cachedSize(token));
  EXPECT_FALSE(cache_.canRegister(0, makeToken()));
  result = cache_.registerToken(2, makeToken());
  EXPECT_TRUE(result.hasError());
  EXPECT_EQ(result.error(), MoQTokenCache::ErrorCode::LIMIT_EXCEEDED);
  EXPECT_TRUE(cache_.deleteToken(alias));
  EXPECT_EQ(cache_.getTotalSize(), 0);
  result = cache_.registerToken(tokenType_, makeToken());
  EXPECT_TRUE(result.hasValue());
  EXPECT_NE(result.value(), alias);
}

TEST_F(MoQTokenCacheTest, GetAliasForTokenSuccess) {
  auto alias = cache_.registerToken(tokenType_, makeToken()).value();
  auto result = cache_.getAliasForToken(tokenType_, makeToken());
  EXPECT_TRUE(result.hasValue());
  EXPECT_EQ(result.value(), alias);
}

TEST_F(MoQTokenCacheTest, GetAliasForTokenUnknown) {
  auto result = cache_.getAliasForToken(tokenType_, makeToken());
  EXPECT_TRUE(result.hasError());
  EXPECT_EQ(result.error(), MoQTokenCache::ErrorCode::UNKNOWN_TOKEN);
}

TEST_F(MoQTokenCacheTest, DeleteTokenSuccess) {
  auto alias = cache_.registerToken(tokenType_, makeToken()).value();
  auto result = cache_.deleteToken(alias);
  EXPECT_TRUE(result.hasValue());
}

TEST_F(MoQTokenCacheTest, DeleteTokenUnknownAlias) {
  auto result = cache_.deleteToken(999); // Use an alias that doesn't exist
  EXPECT_TRUE(result.hasError());
  EXPECT_EQ(result.error(), MoQTokenCache::ErrorCode::UNKNOWN_ALIAS);
}

TEST_F(MoQTokenCacheTest, GetTokenForAliasSuccess) {
  auto alias = cache_.registerToken(tokenType_, makeToken()).value();
  auto result = cache_.getTokenForAlias(alias);
  EXPECT_TRUE(result.hasValue());
  EXPECT_EQ(result.value().tokenType, tokenType_);
  EXPECT_EQ(result.value().tokenValue, makeToken());
}

TEST_F(MoQTokenCacheTest, GetTokenForAliasUnknownAlias) {
  auto result = cache_.getTokenForAlias(999); // Use an alias that doesn't exist
  EXPECT_TRUE(result.hasError());
  EXPECT_EQ(result.error(), MoQTokenCache::ErrorCode::UNKNOWN_ALIAS);
}

TEST_F(MoQTokenCacheTest, GetAliasForEvictionSuccess) {
  auto token = makeToken();
  auto cachedSize = MoQTokenCache::cachedSize(token);
  MoQTokenCache cache_{3 * cachedSize}; // Increase the size to hold 3 tokens
  auto alias1 = cache_.registerToken(tokenType_, token).value();
  auto alias2 = cache_.registerToken(tokenType_, token).value();
  auto alias3 = cache_.registerToken(tokenType_, token).value();
  EXPECT_EQ(cache_.getTotalSize(), 3 * cachedSize);

  auto result = cache_.evictLRU();
  EXPECT_EQ(result, alias1); // Evict the first token
  EXPECT_EQ(cache_.getTotalSize(),
            2 * cachedSize); // Verify size is reduced

  auto alias4 = cache_.registerToken(tokenType_, makeToken())
                    .value(); // Insert another token
  EXPECT_EQ(cache_.getTotalSize(), 3 * cachedSize);

  // Access the 2nd inserted token to move it to the end of LRU
  auto token2 = cache_.getTokenForAlias(alias2);
  EXPECT_TRUE(token2.hasValue());

  result = cache_.evictLRU();
  EXPECT_EQ(result, alias3); // Evict the 3rd inserted token
  EXPECT_EQ(cache_.getTotalSize(),
            2 * cachedSize); // Verify size is reduced

  result = cache_.evictLRU();
  EXPECT_EQ(result, alias4); // Evict the 4th inserted token
  EXPECT_EQ(cache_.getTotalSize(),
            cachedSize); // Verify size is reduced

  result = cache_.evictLRU();
  EXPECT_EQ(result, alias2);           // Evict the 2nd inserted token
  EXPECT_EQ(cache_.getTotalSize(), 0); // Verify size is reduced
}

TEST_F(MoQTokenCacheTest, SetMaxSizeWithEvictTrue) {
  auto token = makeToken();
  auto cachedSize = MoQTokenCache::cachedSize(token);
  MoQTokenCache cache_{3 * cachedSize}; // Increase the size to hold 3 tokens
  EXPECT_EQ(
      cache_.maxTokenSize(), 3 * cachedSize - MoQTokenCache::kTokenOverhead);

  cache_.registerToken(tokenType_, token).value();
  cache_.registerToken(tokenType_, token).value();

  auto alias3 = cache_.registerToken(tokenType_, token).value();
  EXPECT_EQ(cache_.getTotalSize(), 3 * cachedSize);

  // Set max size to 2 tokens worth and allow eviction
  cache_.setMaxSize(2 * cachedSize, true);
  EXPECT_EQ(cache_.getTotalSize(), 2 * cachedSize); // Verify size is reduced

  // Check which token was evicted
  auto result = cache_.getTokenForAlias(alias3);
  EXPECT_TRUE(result.hasError()); // The newest token should be evicted
}

TEST_F(MoQTokenCacheTest, SetMaxSizeWithEvictFalseError) {
  auto token = makeToken();
  auto cachedSize = MoQTokenCache::cachedSize(token);
  MoQTokenCache cache_{3 * cachedSize}; // Increase the size to hold 3 tokens
  cache_.registerToken(tokenType_, token).value();
  cache_.registerToken(tokenType_, token).value();
  cache_.registerToken(tokenType_, token).value();
  EXPECT_EQ(cache_.getTotalSize(), 3 * cachedSize);

  // Attempt to set max size to 2 tokens worth without allowing eviction
  auto result = cache_.setMaxSize(2 * cachedSize, false);
  EXPECT_TRUE(result.hasError());
  EXPECT_EQ(result.error(), MoQTokenCache::ErrorCode::LIMIT_EXCEEDED);
}

TEST_F(MoQTokenCacheTest, RegisterTokenDuplicateAliasError) {
  auto token = makeToken();
  auto alias1 = cache_.registerToken(tokenType_, token).value();
  auto result = cache_.registerToken(alias1, tokenType_, token);
  EXPECT_TRUE(result.hasError());
  EXPECT_EQ(result.error(), MoQTokenCache::ErrorCode::DUPLICATE_ALIAS);
}

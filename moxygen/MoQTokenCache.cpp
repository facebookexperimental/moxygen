/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <moxygen/MoQTokenCache.h>

#include <folly/logging/xlog.h>

namespace moxygen {
folly::Expected<MoQTokenCache::Alias, MoQTokenCache::ErrorCode>
MoQTokenCache::registerToken(uint64_t tokenType, TokenValue tokenValue) {
  // Generate a new alias
  Alias alias = nextAlias_;

  // Call the second registerToken function
  auto res = registerToken(alias, tokenType, std::move(tokenValue));
  if (res.hasError()) {
    return folly::makeUnexpected(res.error());
  }

  // Increment the next alias
  nextAlias_++;
  return alias;
}

// for decoder -- alias is set by caller
folly::Expected<folly::Unit, MoQTokenCache::ErrorCode>
MoQTokenCache::registerToken(
    Alias alias,
    uint64_t tokenType,
    TokenValue tokenValue) {
  // Check if the cache size limit is exceeded
  if (aliasToToken_.contains(alias)) {
    return folly::makeUnexpected(ErrorCode::DUPLICATE_ALIAS);
  }
  auto tokenSize = cachedSize(tokenValue);
  if (totalSize_ + tokenSize > maxSize_) {
    return folly::makeUnexpected(ErrorCode::LIMIT_EXCEEDED);
  }
  totalSize_ += tokenSize;

  // Insert the new token into the cache with the provided alias
  lru_.push_back(alias);
  XLOG(DBG4) << "Inserting alias: " << alias << ", TokenValue: " << tokenValue;

  aliasToToken_.emplace(
      alias, CachedToken{tokenType, std::move(tokenValue), --lru_.end()});
  return folly::unit;
}

folly::Expected<MoQTokenCache::Alias, MoQTokenCache::ErrorCode>
MoQTokenCache::getAliasForToken(
    uint64_t tokenType,
    const TokenValue& tokenValue) {
  for (const auto& [alias, cachedToken] : aliasToToken_) {
    if (cachedToken.tokenType == tokenType &&
        cachedToken.tokenValue == tokenValue) {
      return alias;
    }
  }
  return folly::makeUnexpected(ErrorCode::UNKNOWN_TOKEN);
}

folly::Expected<folly::Unit, MoQTokenCache::ErrorCode>
MoQTokenCache::deleteToken(Alias alias) {
  auto it = aliasToToken_.find(alias);
  if (it == aliasToToken_.end()) {
    return folly::makeUnexpected(ErrorCode::UNKNOWN_ALIAS);
  }

  XLOG(DBG4) << "Deleting alias: " << alias
             << ", TokenValue: " << it->second.tokenValue;
  auto size = cachedSize(it->second.tokenValue);
  XCHECK_GE(totalSize_, size);
  totalSize_ -= size;
  lru_.erase(it->second.aliasIt);
  aliasToToken_.erase(it);
  return folly::Unit();
}

folly::Expected<MoQTokenCache::TokenTypeAndValue, MoQTokenCache::ErrorCode>
MoQTokenCache::getTokenForAlias(Alias alias) {
  auto it = aliasToToken_.find(alias);
  if (it == aliasToToken_.end()) {
    return folly::makeUnexpected(ErrorCode::UNKNOWN_ALIAS);
  }

  auto& cachedToken = it->second;
  lru_.splice(lru_.end(), lru_, cachedToken.aliasIt);
  return TokenTypeAndValue{cachedToken.tokenType, cachedToken.tokenValue};
}

MoQTokenCache::Alias MoQTokenCache::evictHelper(std::list<Alias>::iterator it) {
  XCHECK(it != lru_.end());
  auto alias = *it;
  lru_.erase(it);
  auto tokenIt = aliasToToken_.find(alias);
  XCHECK(tokenIt != aliasToToken_.end());
  XCHECK_GE(totalSize_, cachedSize(tokenIt->second.tokenValue));
  XLOG(DBG4) << "Removing alias: " << alias
             << ", TokenValue: " << tokenIt->second.tokenValue;
  totalSize_ -= cachedSize(tokenIt->second.tokenValue);
  aliasToToken_.erase(tokenIt);
  return alias;
}

MoQTokenCache::Alias MoQTokenCache::evictOldest() {
  XCHECK(!lru_.empty());
  return evictHelper(lru_.begin());
}

MoQTokenCache::Alias MoQTokenCache::evictNewest() {
  XCHECK(!lru_.empty());
  return evictHelper(--lru_.end());
}

} // namespace moxygen

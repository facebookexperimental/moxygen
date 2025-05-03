/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/Expected.h>
#include <folly/container/F14Map.h>
#include <cstdint>
#include <list>

namespace moxygen {
class MoQTokenCache {
 public:
  explicit MoQTokenCache(uint64_t maxSize = 0) : maxSize_(maxSize) {}

  enum class ErrorCode {
    DUPLICATE_ALIAS = 0,
    LIMIT_EXCEEDED = 1,
    UNKNOWN_TOKEN = 2,
    UNKNOWN_ALIAS = 3,
  };

  folly::Expected<folly::Unit, ErrorCode> setMaxSize(uint64_t maxSize) {
    if (maxSize < totalSize_) {
      return folly::makeUnexpected(ErrorCode::LIMIT_EXCEEDED);
    }
    maxSize_ = maxSize;
    return folly::unit;
  }

  using Alias = uint64_t;
  using TokenValue = std::string;
  // For encoder -- alias is set automatically
  folly::Expected<Alias, ErrorCode> registerToken(
      uint64_t tokenType,
      TokenValue tokenValue);

  // for decoder -- alias is set by caller
  folly::Expected<folly::Unit, ErrorCode>
  registerToken(Alias alias, uint64_t tokenType, TokenValue tokenValue);

  folly::Expected<Alias, ErrorCode> getAliasForToken(
      uint64_t tokenType,
      const TokenValue& tokenValue);

  folly::Expected<folly::Unit, ErrorCode> deleteToken(Alias alias);
  struct TokenTypeAndValue {
    uint64_t tokenType;
    TokenValue tokenValue;
  };
  folly::Expected<TokenTypeAndValue, ErrorCode> getTokenForAlias(Alias alias);
  Alias evictOne();

  uint64_t getTotalSize() const {
    return totalSize_;
  }

  bool canRegister(uint64_t tokenType, const TokenValue& tokenValue) const {
    // Check if adding this token would exceed the max size
    return (totalSize_ + cachedSize(tokenValue)) <= maxSize_;
  }

  size_t maxTokenSize() const {
    return maxSize_ < 8 ? 0 : maxSize_ - 8;
  }

 private:
  size_t cachedSize(const TokenValue& token) const {
    return token.size() + 8;
  }

  struct CachedToken {
    uint64_t tokenType;
    TokenValue tokenValue;
    std::list<Alias>::iterator aliasIt;
  };
  folly::F14FastMap<Alias, CachedToken> aliasToToken_;
  std::list<Alias> lru_; // technically only used by encoders, but it's O(1)
  uint64_t maxSize_;
  uint64_t nextAlias_ = 0;
  uint64_t totalSize_ = 0;
};
} // namespace moxygen

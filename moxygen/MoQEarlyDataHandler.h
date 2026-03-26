/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/io/Cursor.h>
#include <folly/io/IOBuf.h>
#include <folly/logging/xlog.h>
#include <quic/codec/QuicInteger.h>
#include <quic/folly_utils/Utils.h>
#include <quic/state/EarlyDataAppParamsHandler.h>

namespace moxygen {

/**
 * EarlyDataAppParamsHandler for MoQ.
 *
 * Serializes/validates MAX_REQUEST_ID and MAX_AUTH_TOKEN_CACHE_SIZE
 * as two varints. No version prefix — ALPN handles versioning.
 *
 * Client side: setCurrentParams() is called with values from SERVER_SETUP.
 * Server side: setCurrentParams() is called with the server's own params.
 * On validation, cached values must be <= current values.
 */
class MoQEarlyDataHandler : public quic::EarlyDataAppParamsHandler {
 public:
  void setCurrentParams(uint64_t maxRequestID, uint64_t maxAuthTokenCacheSize) {
    maxRequestID_ = maxRequestID;
    maxAuthTokenCacheSize_ = maxAuthTokenCacheSize;
  }

  bool validate(
      const quic::Optional<std::string>& /*alpn*/,
      const quic::BufPtr& appParams) override {
    XLOG(DBG4) << "MoQEarlyDataHandler::validate called, appParams="
               << (appParams ? "non-null" : "null");
    if (!appParams) {
      return false;
    }
    folly::io::Cursor cursor(appParams.get());
    auto cachedMaxRequestID = quic::follyutils::decodeQuicInteger(cursor);
    if (!cachedMaxRequestID) {
      XLOG(DBG2) << "Failed to decode cached MAX_REQUEST_ID";
      return false;
    }
    auto cachedMaxAuthTokenCacheSize =
        quic::follyutils::decodeQuicInteger(cursor);
    if (!cachedMaxAuthTokenCacheSize) {
      XLOG(DBG2) << "Failed to decode cached MAX_AUTH_TOKEN_CACHE_SIZE";
      return false;
    }
    // Cached values must be <= current so the peer can honor what was promised
    if (cachedMaxRequestID->first > maxRequestID_) {
      XLOG(DBG2) << "Cached MAX_REQUEST_ID " << cachedMaxRequestID->first
                 << " > current " << maxRequestID_;
      return false;
    }
    if (cachedMaxAuthTokenCacheSize->first > maxAuthTokenCacheSize_) {
      XLOG(DBG2) << "Cached MAX_AUTH_TOKEN_CACHE_SIZE "
                 << cachedMaxAuthTokenCacheSize->first << " > current "
                 << maxAuthTokenCacheSize_;
      return false;
    }
    return true;
  }

  quic::BufPtr get() override {
    XLOG(DBG4) << "MoQEarlyDataHandler::get called, maxRequestID="
               << maxRequestID_
               << " maxAuthTokenCacheSize=" << maxAuthTokenCacheSize_;
    folly::IOBufQueue buf{folly::IOBufQueue::cacheChainLength()};
    auto writeVarint = [&buf](uint64_t val) {
      auto size = quic::getQuicIntegerSize(val).value();
      folly::io::QueueAppender appender(&buf, size);
      auto appenderOp = [appender = std::move(appender)](auto v) mutable {
        appender.writeBE(folly::tag<decltype(v)>, v);
      };
      auto res = quic::encodeQuicInteger(val, appenderOp);
      DCHECK(res.has_value());
    };
    writeVarint(maxRequestID_);
    writeVarint(maxAuthTokenCacheSize_);
    return buf.move();
  }

 private:
  uint64_t maxRequestID_{0};
  uint64_t maxAuthTokenCacheSize_{0};
};

} // namespace moxygen

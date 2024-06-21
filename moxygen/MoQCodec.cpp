/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/MoQCodec.h"

#include <folly/logging/xlog.h>

#include <iomanip>

namespace moxygen {

void MoQCodec::onIngress(std::unique_ptr<folly::IOBuf> data, bool eom) {
  ingress_.append(std::move(data));
  folly::io::Cursor cursor(ingress_.front());
  while (!connError_ &&
         ((ingress_.chainLength() > 0 && !cursor.isAtEnd()) ||
          (eom && parseState_ == ParseState::OBJECT_PAYLOAD_NO_LENGTH))) {
    switch (parseState_) {
      case ParseState::FRAME_HEADER_TYPE: {
        auto newCursor = cursor;
        auto type = quic::decodeQuicInteger(newCursor);
        if (!type) {
          connError_ = ErrorCode::PARSE_UNDERFLOW;
          break;
        }
        cursor = newCursor;
        curFrameType_ = FrameType(type->first);
        auto res = checkFrameAllowed(curFrameType_);
        if (!res) {
          XLOG(DBG4) << "Frame not allowed: 0x" << std::setfill('0')
                     << std::setw(sizeof(uint64_t) * 2) << std::hex
                     << (uint64_t)curFrameType_ << " on streamID=" << streamId_;
          connError_.emplace(ErrorCode::PARSE_ERROR);
          break;
        }
        if (callback_) {
          callback_->onFrame(curFrameType_);
        }
        parseState_ = ParseState::FRAME_PAYLOAD;
        [[fallthrough]];
      }
      case ParseState::FRAME_PAYLOAD: {
        auto newCursor = cursor;
        if (curFrameType_ == FrameType::OBJECT_STREAM ||
            curFrameType_ == FrameType::OBJECT_DATAGRAM) {
          auto res = parseObjectHeader(newCursor, curFrameType_);
          if (res.hasError()) {
            connError_ = res.error();
            break;
          }
          curObjectHeader_ = res.value();
          parseState_ = ParseState::OBJECT_PAYLOAD_NO_LENGTH;
          if (callback_) {
            callback_->onObjectHeader(std::move(res.value()));
          }
        } else if (
            curFrameType_ == FrameType::STREAM_HEADER_TRACK ||
            curFrameType_ == FrameType::STREAM_HEADER_GROUP) {
          auto res = parseStreamHeader(newCursor, curFrameType_);
          if (res.hasError()) {
            connError_ = res.error();
            break;
          }
          curObjectHeader_ = res.value();
          parseState_ = ParseState::MULTI_OBJECT_HEADER;
        } else {
          auto res = parseFrame(newCursor);
          if (res.hasError()) {
            connError_ = res.error();
            break;
          }
          parseState_ = ParseState::FRAME_HEADER_TYPE;
        }
        cursor = newCursor;
        break;
      }
      case ParseState::MULTI_OBJECT_HEADER: {
        auto newCursor = cursor;
        auto res =
            parseMultiObjectHeader(newCursor, curFrameType_, curObjectHeader_);
        if (res.hasError()) {
          connError_ = res.error();
          break;
        }
        curObjectHeader_ = res.value();
        parseState_ = ParseState::OBJECT_PAYLOAD;
        if (callback_) {
          callback_->onObjectHeader(std::move(res.value()));
        }
        cursor = newCursor;
        [[fallthrough]];
      }
      case ParseState::OBJECT_PAYLOAD: {
        auto newCursor = cursor;
        // need to check for bufLen == 0?
        std::unique_ptr<folly::IOBuf> payload;
        // TODO: skip clone and do split
        uint64_t chunkLen = 0;
        XCHECK(curObjectHeader_.length);
        XLOG(DBG2) << "Parsing object with length, need="
                   << *curObjectHeader_.length;
        if (ingress_.chainLength() > 0 && newCursor.canAdvance(1)) {
          chunkLen = newCursor.cloneAtMost(payload, *curObjectHeader_.length);
        }
        *curObjectHeader_.length -= chunkLen;
        if (eom && *curObjectHeader_.length != 0) {
          connError_ = ErrorCode::PARSE_ERROR;
          break;
        }
        bool endOfObject = (*curObjectHeader_.length == 0);
        if (callback_ && (payload || endOfObject)) {
          callback_->onObjectPayload(
              curObjectHeader_.subscribeID,
              curObjectHeader_.trackAlias,
              curObjectHeader_.group,
              curObjectHeader_.id,
              std::move(payload),
              endOfObject);
        }
        if (endOfObject) {
          parseState_ = ParseState::MULTI_OBJECT_HEADER;
        }
        cursor = newCursor;
        break;
      }
      case ParseState::OBJECT_PAYLOAD_NO_LENGTH: {
        auto newCursor = cursor;
        // need to check for bufLen == 0?
        std::unique_ptr<folly::IOBuf> payload;
        // TODO: skip clone and do split
        if (ingress_.chainLength() > 0 && newCursor.canAdvance(1)) {
          newCursor.cloneAtMost(payload, std::numeric_limits<uint64_t>::max());
        }
        XCHECK(!curObjectHeader_.length);
        if (callback_ && (payload || eom)) {
          callback_->onObjectPayload(
              curObjectHeader_.subscribeID,
              curObjectHeader_.trackAlias,
              curObjectHeader_.group,
              curObjectHeader_.id,
              std::move(payload),
              eom);
        }
        if (eom) {
          parseState_ = ParseState::FRAME_HEADER_TYPE;
        }
        cursor = newCursor;
      }
    }
  }
  if (connError_) {
    if (connError_.value() == ErrorCode::PARSE_UNDERFLOW && !eom) {
      auto remainingLen = cursor.totalLength(); // must be less than 1 message
      ingress_.trimStart(ingress_.chainLength() - remainingLen);
      connError_.reset();
      return;
    } else if (callback_) {
      XLOG(ERR) << "Conn error=" << uint32_t(*connError_);
      callback_->onConnectionError(*connError_);
    }
  }
  // we parsed everything, or connection error
  ingress_.move();
}

folly::Expected<folly::Unit, ErrorCode> MoQCodec::parseFrame(
    folly::io::Cursor& cursor) {
  XLOG(DBG4) << "parsing frame type=" << folly::to_underlying(curFrameType_);
  switch (curFrameType_) {
    case FrameType::CLIENT_SETUP: {
      auto res = parseClientSetup(cursor);
      if (res) {
        if (callback_) {
          callback_->onClientSetup(std::move(res.value()));
        }
      } else {
        return folly::makeUnexpected(res.error());
      }
      break;
    }
    case FrameType::SERVER_SETUP: {
      auto res = parseServerSetup(cursor);
      if (res) {
        if (callback_) {
          callback_->onServerSetup(std::move(res.value()));
        }
      } else {
        return folly::makeUnexpected(res.error());
      }
      break;
    }
    case FrameType::OBJECT_STREAM:
    case FrameType::STREAM_HEADER_TRACK:
    case FrameType::STREAM_HEADER_GROUP:
    case FrameType::OBJECT_DATAGRAM: {
      CHECK(false);
      break;
    }
    case FrameType::SUBSCRIBE: {
      auto res = parseSubscribeRequest(cursor);
      if (res) {
        if (callback_) {
          callback_->onSubscribe(std::move(res.value()));
        }
      } else {
        return folly::makeUnexpected(res.error());
      }
      break;
    }
    case FrameType::SUBSCRIBE_UPDATE: {
      auto res = parseSubscribeUpdateRequest(cursor);
      if (res) {
        if (callback_) {
          callback_->onSubscribeUpdate(std::move(res.value()));
        }
      } else {
        return folly::makeUnexpected(res.error());
      }
      break;
    }
    case FrameType::SUBSCRIBE_OK: {
      auto res = parseSubscribeOk(cursor);
      if (res) {
        if (callback_) {
          callback_->onSubscribeOk(std::move(res.value()));
        }
      } else {
        return folly::makeUnexpected(res.error());
      }
      break;
    }
    case FrameType::SUBSCRIBE_ERROR: {
      auto res = parseSubscribeError(cursor);
      if (res) {
        if (callback_) {
          callback_->onSubscribeError(std::move(res.value()));
        }
      } else {
        return folly::makeUnexpected(res.error());
      }
      break;
    }
    case FrameType::UNSUBSCRIBE: {
      auto res = parseUnsubscribe(cursor);
      if (res) {
        if (callback_) {
          callback_->onUnsubscribe(std::move(res.value()));
        }
      } else {
        return folly::makeUnexpected(res.error());
      }
      break;
    }
    case FrameType::SUBSCRIBE_DONE: {
      auto res = parseSubscribeDone(cursor);
      if (res) {
        if (callback_) {
          callback_->onSubscribeDone(std::move(res.value()));
        }
      } else {
        return folly::makeUnexpected(res.error());
      }
      break;
    }
    case FrameType::ANNOUNCE: {
      auto res = parseAnnounce(cursor);
      if (res) {
        if (callback_) {
          callback_->onAnnounce(std::move(res.value()));
        }
      } else {
        return folly::makeUnexpected(res.error());
      }
      break;
    }
    case FrameType::ANNOUNCE_OK: {
      auto res = parseAnnounceOk(cursor);
      if (res) {
        if (callback_) {
          callback_->onAnnounceOk(std::move(res.value()));
        }
      } else {
        return folly::makeUnexpected(res.error());
      }
      break;
    }
    case FrameType::ANNOUNCE_ERROR: {
      auto res = parseAnnounceError(cursor);
      if (res) {
        if (callback_) {
          callback_->onAnnounceError(std::move(res.value()));
        }
      } else {
        return folly::makeUnexpected(res.error());
      }
      break;
    }
    case FrameType::UNANNOUNCE: {
      auto res = parseUnannounce(cursor);
      if (res) {
        if (callback_) {
          callback_->onUnannounce(std::move(res.value()));
        }
      } else {
        return folly::makeUnexpected(res.error());
      }
      break;
    }
    case FrameType::ANNOUNCE_CANCEL: {
      auto res = parseAnnounceCancel(cursor);
      if (res) {
        if (callback_) {
          callback_->onAnnounceCancel(std::move(res.value()));
        }
      } else {
        return folly::makeUnexpected(res.error());
      }
      break;
    }
    case FrameType::TRACK_STATUS_REQUEST: {
      auto res = parseTrackStatusRequest(cursor);
      if (res) {
        if (callback_) {
          callback_->onTrackStatusRequest(std::move(res.value()));
        }
      } else {
        return folly::makeUnexpected(res.error());
      }
      break;
    }
    case FrameType::TRACK_STATUS: {
      auto res = parseTrackStatus(cursor);
      if (res) {
        if (callback_) {
          callback_->onTrackStatus(std::move(res.value()));
        }
      } else {
        return folly::makeUnexpected(res.error());
      }
      break;
    }
    case FrameType::GOAWAY: {
      auto res = parseGoaway(cursor);
      if (res) {
        if (callback_) {
          callback_->onGoaway(std::move(res.value()));
        }
      } else {
        return folly::makeUnexpected(res.error());
      }
      break;
    }
  }
  return folly::unit;
}

} // namespace moxygen

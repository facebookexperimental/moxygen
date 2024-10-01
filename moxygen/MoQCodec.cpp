/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/MoQCodec.h"

#include <folly/logging/xlog.h>

#include <iomanip>

namespace moxygen {

void MoQCodec::onIngressStart(std::unique_ptr<folly::IOBuf> data) {
  ingress_.append(std::move(data));
}

void MoQControlCodec::onIngress(std::unique_ptr<folly::IOBuf> data, bool eom) {
  onIngressStart(std::move(data));
  size_t remainingLength = ingress_.chainLength();
  folly::io::Cursor cursor(ingress_.front());
  while (!connError_ && remainingLength > 0) {
    switch (parseState_) {
      case ParseState::FRAME_HEADER_TYPE: {
        auto type = quic::decodeQuicInteger(cursor);
        if (!type) {
          XLOG(DBG6) << __func__ << " underflow";
          connError_ = ErrorCode::PARSE_UNDERFLOW;
          break;
        }
        curFrameType_ = FrameType(type->first);
        remainingLength -= type->second;
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
        parseState_ = ParseState::FRAME_LENGTH;
        [[fallthrough]];
      }
      case ParseState::FRAME_LENGTH: {
        auto length = quic::decodeQuicInteger(cursor);
        if (!length) {
          XLOG(DBG6) << __func__ << " underflow";
          connError_ = ErrorCode::PARSE_UNDERFLOW;
          break;
        }
        curFrameLength_ = length->first;
        remainingLength -= length->second;
        parseState_ = ParseState::FRAME_PAYLOAD;
        [[fallthrough]];
      }
      case ParseState::FRAME_PAYLOAD: {
        if (remainingLength < curFrameLength_) {
          XLOG(DBG6) << __func__ << " underflow";
          connError_ = ErrorCode::PARSE_UNDERFLOW;
          break;
        }
        auto res = parseFrame(cursor);
        if (res.hasError()) {
          XLOG(DBG6) << __func__ << " " << uint32_t(res.error());
          if (res.error() == ErrorCode::PARSE_UNDERFLOW) {
            XLOG(ERR) << "Frame underflow -> parse error";
            connError_ = ErrorCode::PARSE_ERROR;
          } else {
            connError_ = res.error();
          }
          break;
        }
        parseState_ = ParseState::FRAME_HEADER_TYPE;
        remainingLength -= curFrameLength_;
        break;
      }
    }
  }
  onIngressEnd(remainingLength, eom, callback_);
}

void MoQCodec::onIngressEnd(
    size_t remainingLength,
    bool eom,
    Callback* callback) {
  if (connError_) {
    if (connError_.value() == ErrorCode::PARSE_UNDERFLOW && !eom) {
      ingress_.trimStart(ingress_.chainLength() - remainingLength);
      connError_.reset();
      return;
    } else if (callback) {
      XLOG(ERR) << "Conn error=" << uint32_t(*connError_);
      callback->onConnectionError(*connError_);
    }
  }
  // we parsed everything, or connection error
  ingress_.move();
}

void MoQObjectStreamCodec::onIngress(
    std::unique_ptr<folly::IOBuf> data,
    bool eom) {
  onIngressStart(std::move(data));
  folly::io::Cursor cursor(ingress_.front());
  while (!connError_ &&
         ((ingress_.chainLength() > 0 && !cursor.isAtEnd())/* ||
          (eom && parseState_ == ParseState::OBJECT_PAYLOAD_NO_LENGTH)*/)) {
    switch (parseState_) {
      case ParseState::STREAM_HEADER_TYPE: {
        auto newCursor = cursor;
        auto type = quic::decodeQuicInteger(newCursor);
        if (!type) {
          XLOG(DBG6) << __func__ << " underflow";
          connError_ = ErrorCode::PARSE_UNDERFLOW;
          break;
        }
        cursor = newCursor;
        streamType_ = StreamType(type->first);
        switch (streamType_) {
          case StreamType::OBJECT_DATAGRAM:
            parseState_ = ParseState::DATAGRAM;
            break;
          case StreamType::STREAM_HEADER_TRACK:
          case StreamType::STREAM_HEADER_SUBGROUP:
            parseState_ = ParseState::OBJECT_STREAM;
            break;
          //  CONTROL doesn't have a wire type yet.
          default:
            XLOG(DBG4) << "Stream not allowed: 0x" << std::setfill('0')
                       << std::setw(sizeof(uint64_t) * 2) << std::hex
                       << (uint64_t)streamType_ << " on streamID=" << streamId_;
            connError_.emplace(ErrorCode::PARSE_ERROR);
            break;
        }
        break;
      }
      case ParseState::DATAGRAM: {
        auto newCursor = cursor;
        auto res = parseObjectHeader(newCursor, ingress_.chainLength());
        if (res.hasError()) {
          XLOG(DBG6) << __func__ << " " << uint32_t(res.error());
          connError_ = res.error();
          break;
        }
        cursor = newCursor;
        curObjectHeader_ = res.value();
        parseState_ = ParseState::OBJECT_PAYLOAD;
        if (callback_) {
          callback_->onObjectHeader(std::move(res.value()));
        }
        break;
      }
      case ParseState::OBJECT_STREAM: {
        auto newCursor = cursor;
        auto res = parseStreamHeader(newCursor, streamType_);
        if (res.hasError()) {
          XLOG(DBG6) << __func__ << " " << uint32_t(res.error());
          connError_ = res.error();
          break;
        }
        curObjectHeader_ = res.value();
        parseState_ = ParseState::MULTI_OBJECT_HEADER;
        cursor = newCursor;
        [[fallthrough]];
      }
      case ParseState::MULTI_OBJECT_HEADER: {
        auto newCursor = cursor;
        auto res =
            parseMultiObjectHeader(newCursor, streamType_, curObjectHeader_);
        if (res.hasError()) {
          XLOG(DBG6) << __func__ << " " << uint32_t(res.error());
          connError_ = res.error();
          break;
        }
        curObjectHeader_ = res.value();
        if (callback_) {
          callback_->onObjectHeader(std::move(res.value()));
        }
        cursor = newCursor;
        if (curObjectHeader_.status == ObjectStatus::NORMAL) {
          parseState_ = ParseState::OBJECT_PAYLOAD;
        } else {
          parseState_ = ParseState::MULTI_OBJECT_HEADER;
          break;
        }
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
          XLOG(DBG4) << __func__ << " " << uint32_t(*connError_);
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
#if 0
// This code is no longer reachable, but I'm leaving it here in case
// the wire format changes back
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
#endif
    }
  }
  size_t remainingLength = 0;
  if (!eom && !cursor.isAtEnd()) {
    remainingLength = cursor.totalLength(); // must be less than 1 message
  }
  onIngressEnd(remainingLength, eom, callback_);
}

folly::Expected<folly::Unit, ErrorCode> MoQControlCodec::parseFrame(
    folly::io::Cursor& cursor) {
  XLOG(DBG4) << "parsing frame type=" << folly::to_underlying(curFrameType_);
  switch (curFrameType_) {
    case FrameType::CLIENT_SETUP: {
      auto res = parseClientSetup(cursor, curFrameLength_);
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
      auto res = parseServerSetup(cursor, curFrameLength_);
      if (res) {
        if (callback_) {
          callback_->onServerSetup(std::move(res.value()));
        }
      } else {
        return folly::makeUnexpected(res.error());
      }
      break;
    }
    case FrameType::SUBSCRIBE: {
      auto res = parseSubscribeRequest(cursor, curFrameLength_);
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
      auto res = parseSubscribeUpdate(cursor, curFrameLength_);
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
      auto res = parseSubscribeOk(cursor, curFrameLength_);
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
      auto res = parseSubscribeError(cursor, curFrameLength_);
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
      auto res = parseUnsubscribe(cursor, curFrameLength_);
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
      auto res = parseSubscribeDone(cursor, curFrameLength_);
      if (res) {
        if (callback_) {
          callback_->onSubscribeDone(std::move(res.value()));
        }
      } else {
        return folly::makeUnexpected(res.error());
      }
      break;
    }
    case FrameType::MAX_SUBSCRIBE_ID: {
      auto res = parseMaxSubscribeId(cursor, curFrameLength_);
      if (res) {
        if (callback_) {
          callback_->onMaxSubscribeId(std::move(res.value()));
        }
      } else {
        return folly::makeUnexpected(res.error());
      }
      break;
    }
    case FrameType::ANNOUNCE: {
      auto res = parseAnnounce(cursor, curFrameLength_);
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
      auto res = parseAnnounceOk(cursor, curFrameLength_);
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
      auto res = parseAnnounceError(cursor, curFrameLength_);
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
      auto res = parseUnannounce(cursor, curFrameLength_);
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
      auto res = parseAnnounceCancel(cursor, curFrameLength_);
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
      auto res = parseTrackStatusRequest(cursor, curFrameLength_);
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
      auto res = parseTrackStatus(cursor, curFrameLength_);
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
      auto res = parseGoaway(cursor, curFrameLength_);
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

/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/flv_parser/FlvCoder.h"
#include <netinet/in.h>

namespace moxygen::flv {

std::unique_ptr<folly::IOBuf> FlvCoder::writeTag(FlvTag tag) {
  folly::IOBufQueue queue =
      folly::IOBufQueue(folly::IOBufQueue::cacheChainLength());
  if (tag.index() == FlvTagTypeIndex::FLV_TAG_INDEX_READCMD) {
    auto readsCmd = std::get<flv::FlvReadCmd>(tag);
    if (readsCmd == flv::FlvReadCmd::FLV_EOF) {
      return nullptr;
    }
    return queue.move(); // Empty
  }

  if (!headerWrote_) {
    auto buf = folly::IOBuf::copyBuffer(&flvHeader_, sizeof(flvHeader_));
    queue.append(std::move(buf));
    uint32_t tagSize = 0x00;
    queue.append(write4Bytes(tagSize));
    headerWrote_ = true;
  }

  size_t tagSize = 0;
  if (tag.index() == FlvTagTypeIndex::FLV_TAG_INDEX_SCRIPT) {
    // Script
    auto dataTag =
        std::move(std::get<FlvTagTypeIndex::FLV_TAG_INDEX_SCRIPT>(tag));
    auto tagHeader = writeTagHeader(*dataTag);
    tagSize += TAG_HEADER_SIZE;
    queue.append(std::move(tagHeader));

    tagSize += dataTag->data->computeChainDataLength();
    queue.append(std::move(dataTag->data));
  } else if (tag.index() == FlvTagTypeIndex::FLV_TAG_INDEX_VIDEO) {
    // Video
    auto videoTag =
        std::move(std::get<FlvTagTypeIndex::FLV_TAG_INDEX_VIDEO>(tag));
    auto tagHeader = writeTagHeader(*videoTag);
    tagSize += TAG_HEADER_SIZE;
    queue.append(std::move(tagHeader));

    auto videoTagHeader = writeVideoTagHeader(*videoTag);
    tagSize += VIDEO_TAG_HEADER_SIZE;
    queue.append(std::move(videoTagHeader));

    tagSize += videoTag->data->computeChainDataLength();
    queue.append(std::move(videoTag->data));
  } else if (tag.index() == FlvTagTypeIndex::FLV_TAG_INDEX_AUDIO) {
    // Audio
    auto audioTag =
        std::move(std::get<FlvTagTypeIndex::FLV_TAG_INDEX_AUDIO>(tag));
    auto tagHeader = writeTagHeader(*audioTag);
    tagSize += TAG_HEADER_SIZE;
    queue.append(std::move(tagHeader));

    auto audioTagHeader = writeAudioTagHeader(*audioTag);
    tagSize += AUDIO_TAG_HEADER_SIZE;
    queue.append(std::move(audioTagHeader));

    tagSize += audioTag->data->computeChainDataLength();
    queue.append(std::move(audioTag->data));
  }
  CHECK_GT(tagSize, 0);
  queue.append(write4Bytes(static_cast<uint32_t>(tagSize)));
  return queue.move();
}

std::unique_ptr<folly::IOBuf> FlvCoder::writeTagHeader(
    const FlvTagBase& tagBase) {
  folly::IOBufQueue queue =
      folly::IOBufQueue(folly::IOBufQueue::cacheChainLength());
  queue.append(writeByte(std::byte(tagBase.type)));
  queue.append(write3Bytes(tagBase.size));
  queue.append(write3Bytes(tagBase.timestamp & 0xFFFFFF));
  queue.append(writeByte(std::byte((tagBase.timestamp >> 24) & 0xFF)));
  queue.append(write3Bytes(tagBase.streamId));

  return queue.move();
}

std::unique_ptr<folly::IOBuf> FlvCoder::writeVideoTagHeader(
    const FlvVideoTag& tagVideo) {
  folly::IOBufQueue queue =
      folly::IOBufQueue(folly::IOBufQueue::cacheChainLength());
  CHECK_EQ(tagVideo.codecId, 7);
  std::byte tmp = std::byte(tagVideo.frameType << 4 | (tagVideo.codecId & 0xF));
  queue.append(writeByte(tmp));
  queue.append(writeByte(std::byte(tagVideo.avcPacketType)));
  queue.append(write3Bytes(tagVideo.compositionTime));

  return queue.move();
}

std::unique_ptr<folly::IOBuf> FlvCoder::writeAudioTagHeader(
    const FlvAudioTag& tagAudio) {
  CHECK_EQ(tagAudio.soundFormat, 10);
  folly::IOBufQueue queue =
      folly::IOBufQueue(folly::IOBufQueue::cacheChainLength());

  std::byte tmp = std::byte(
      (tagAudio.soundFormat & 0b1111) << 4 | (tagAudio.soundRate & 0b11) << 2 |
      (tagAudio.soundSize & 0b1) << 1 | (tagAudio.soundType & 0b1));
  queue.append(writeByte(tmp));
  queue.append(writeByte(std::byte(tagAudio.aacPacketType)));

  return queue.move();
}

std::unique_ptr<folly::IOBuf> FlvCoder::write4Bytes(uint32_t v) {
  auto nv = htonl(v);
  return folly::IOBuf::copyBuffer(&nv, 4);
}

std::unique_ptr<folly::IOBuf> FlvCoder::write3Bytes(uint32_t v) {
  auto nv = htonl(v & 0x00FFFFFF) >> 8;
  return folly::IOBuf::copyBuffer(&nv, 3);
}

std::unique_ptr<folly::IOBuf> FlvCoder::writeByte(std::byte b) {
  return folly::IOBuf::copyBuffer(&b, 1);
}

} // namespace moxygen::flv

// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#include "moxygen/flv_parser/FlvWriter.h"
#include <netinet/in.h>

namespace moxygen::flv {

bool FlvWriter::writeTag(FlvTag tag) {
  if (tag.index() == FlvTagTypeIndex::FLV_TAG_INDEX_READCMD) {
    auto readsCmd = std::get<flv::FlvReadCmd>(tag);
    if (readsCmd == flv::FlvReadCmd::FLV_EOF) {
      return false;
    }
    return true;
  }

  if (!headerWrote_) {
    f_.write(flvHeader_, sizeof(flvHeader_));
    uint32_t tagSize = 0x00;
    write4Bytes(tagSize);
    headerWrote_ = true;
  }

  size_t tagSize = 0;
  if (tag.index() == FlvTagTypeIndex::FLV_TAG_INDEX_SCRIPT) {
    // Script
    auto dataTag =
        std::move(std::get<FlvTagTypeIndex::FLV_TAG_INDEX_SCRIPT>(tag));
    tagSize = tagSize + writeTagHeader(*dataTag);
    tagSize = tagSize + writeIoBuf(std::move(dataTag->data));
  } else if (tag.index() == FlvTagTypeIndex::FLV_TAG_INDEX_VIDEO) {
    // Video
    auto videoTag =
        std::move(std::get<FlvTagTypeIndex::FLV_TAG_INDEX_VIDEO>(tag));
    tagSize = tagSize + writeTagHeader(*videoTag);
    tagSize = tagSize + writeVideoTagHeader(*videoTag);
    tagSize = tagSize + writeIoBuf(std::move(videoTag->data));
  } else if (tag.index() == FlvTagTypeIndex::FLV_TAG_INDEX_AUDIO) {
    // Audio
    auto audioTag =
        std::move(std::get<FlvTagTypeIndex::FLV_TAG_INDEX_AUDIO>(tag));
    tagSize = tagSize + writeTagHeader(*audioTag);
    tagSize = tagSize + writeAudioTagHeader(*audioTag);
    tagSize = tagSize + writeIoBuf(std::move(audioTag->data));
  }
  CHECK(tagSize > 0);
  write4Bytes(tagSize);
  f_.flush();

  return true;
}

size_t FlvWriter::writeTagHeader(const FlvTagBase& tagBase) {
  size_t ret = 11;
  writeByte(std::byte(tagBase.type));
  write3Bytes(tagBase.size);
  write3Bytes(tagBase.timestamp & 0xFFFFFF);
  writeByte(std::byte((tagBase.timestamp >> 24) & 0xFF));
  write3Bytes(tagBase.streamId);

  return ret;
}

size_t FlvWriter::writeVideoTagHeader(const FlvVideoTag& tagVideo) {
  CHECK(tagVideo.codecId == 7);
  size_t ret = 5;
  std::byte tmp = std::byte(tagVideo.frameType << 4 | (tagVideo.codecId & 0xF));
  writeByte(tmp);
  writeByte(std::byte(tagVideo.avcPacketType));
  write3Bytes(tagVideo.compositionTime);

  return ret;
}

size_t FlvWriter::writeAudioTagHeader(const FlvAudioTag& tagAudio) {
  CHECK(tagAudio.soundFormat == 10);
  size_t ret = 1;
  std::byte tmp = std::byte(
      (tagAudio.soundFormat & 0b1111) << 4 | (tagAudio.soundRate & 0b11) << 2 |
      (tagAudio.soundSize & 0b1) << 1 | (tagAudio.soundType & 0b1));
  writeByte(tmp);
  writeByte(std::byte(tagAudio.aacPacketType));

  return ret;
}

void FlvWriter::write4Bytes(uint32_t v) {
  auto nv = htonl(v);
  f_.write(reinterpret_cast<char*>(&nv), 4);
}

void FlvWriter::write3Bytes(uint32_t v) {
  auto nv = htonl(v & 0x00FFFFFF) >> 8;
  f_.write(reinterpret_cast<char*>(&nv), 3);
}

void FlvWriter::writeByte(std::byte b) {
  f_.write(reinterpret_cast<const char*>(&b), 1);
}

size_t FlvWriter::writeIoBuf(std::unique_ptr<folly::IOBuf> buf) {
  size_t ret = 0;
  CHECK(buf != nullptr);

  // Write all the chain if IOBuf without coalesing
  const folly::IOBuf* current = buf.get();
  do {
    if (current->length() > 0) {
      f_.write(
          reinterpret_cast<const char*>(current->data()), current->length());
      ret += current->length();
    }
    current = current->next();

  } while (current != buf.get());

  return ret;
}

} // namespace moxygen::flv

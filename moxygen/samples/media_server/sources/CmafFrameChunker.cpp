/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <moxygen/samples/media_server/sources/CmafFrameChunker.h>

#include <cstring>
#include <limits>

namespace moxygen::media_server {
namespace {

constexpr uint32_t kTfhdBaseDataOffsetPresent = 0x000001;
constexpr uint32_t kTfhdSampleDescriptionIndexPresent = 0x000002;
constexpr uint32_t kTfhdDefaultSampleDurationPresent = 0x000008;
constexpr uint32_t kTfhdDefaultSampleSizePresent = 0x000010;
constexpr uint32_t kTfhdDefaultSampleFlagsPresent = 0x000020;
constexpr uint32_t kTfhdDurationIsEmpty = 0x010000;
constexpr uint32_t kTfhdDefaultBaseIsMoof = 0x020000;

constexpr uint32_t kTrunDataOffsetPresent = 0x000001;
constexpr uint32_t kTrunFirstSampleFlagsPresent = 0x000004;
constexpr uint32_t kTrunSampleDurationPresent = 0x000100;
constexpr uint32_t kTrunSampleSizePresent = 0x000200;
constexpr uint32_t kTrunSampleFlagsPresent = 0x000400;
constexpr uint32_t kTrunSampleCompositionTimeOffsetPresent = 0x000800;
constexpr uint32_t kSampleIsNonSyncSample = 0x00010000;

constexpr uint32_t kOutputTfhdFlags =
    kTfhdSampleDescriptionIndexPresent | kTfhdDefaultBaseIsMoof;
constexpr uint32_t kOutputTrunFlags = kTrunDataOffsetPresent |
    kTrunSampleDurationPresent | kTrunSampleSizePresent |
    kTrunSampleFlagsPresent | kTrunSampleCompositionTimeOffsetPresent;
constexpr uint32_t kOutputMoofSize = 108;
constexpr uint32_t kOutputMdatPayloadOffset = kOutputMoofSize + 8;

uint32_t readBE24(const uint8_t* p) {
  return (uint32_t(p[0]) << 16) | (uint32_t(p[1]) << 8) | uint32_t(p[2]);
}

uint32_t readBE32(const uint8_t* p) {
  return (uint32_t(p[0]) << 24) | (uint32_t(p[1]) << 16) |
      (uint32_t(p[2]) << 8) | uint32_t(p[3]);
}

uint64_t readBE64(const uint8_t* p) {
  uint64_t value = 0;
  for (size_t i = 0; i < 8; ++i) {
    value = (value << 8) | p[i];
  }
  return value;
}

void appendBE32(std::string& out, uint32_t value) {
  out.push_back(static_cast<char>((value >> 24) & 0xff));
  out.push_back(static_cast<char>((value >> 16) & 0xff));
  out.push_back(static_cast<char>((value >> 8) & 0xff));
  out.push_back(static_cast<char>(value & 0xff));
}

void appendBE64(std::string& out, uint64_t value) {
  for (int shift = 56; shift >= 0; shift -= 8) {
    out.push_back(static_cast<char>((value >> shift) & 0xff));
  }
}

void appendBoxHeader(std::string& out, uint32_t size, const char* type) {
  appendBE32(out, size);
  out.append(type, 4);
}

struct Box {
  size_t start{0};
  size_t payloadStart{0};
  size_t end{0};
};

std::optional<Box> boxAt(std::string_view data, size_t offset, size_t limit) {
  if (offset > limit || limit > data.size() || limit - offset < 8) {
    return std::nullopt;
  }
  const auto* bytes = reinterpret_cast<const uint8_t*>(data.data());
  uint64_t size = readBE32(bytes + offset);
  size_t headerSize = 8;
  if (size == 1) {
    if (limit - offset < 16) {
      return std::nullopt;
    }
    size = readBE64(bytes + offset + 8);
    headerSize = 16;
  } else if (size == 0) {
    size = limit - offset;
  }
  if (size < headerSize || size > limit - offset) {
    return std::nullopt;
  }
  return Box{offset, offset + headerSize, offset + static_cast<size_t>(size)};
}

bool hasType(std::string_view data, const Box& box, const char* type) {
  return std::memcmp(data.data() + box.start + 4, type, 4) == 0;
}

std::optional<Box>
findChild(std::string_view data, const Box& parent, const char* type) {
  size_t offset = parent.payloadStart;
  while (offset < parent.end) {
    auto child = boxAt(data, offset, parent.end);
    if (!child) {
      return std::nullopt;
    }
    if (hasType(data, *child, type)) {
      return child;
    }
    offset = child->end;
  }
  return std::nullopt;
}

std::optional<Box>
findUniqueChild(std::string_view data, const Box& parent, const char* type) {
  std::optional<Box> match;
  size_t offset = parent.payloadStart;
  while (offset < parent.end) {
    auto child = boxAt(data, offset, parent.end);
    if (!child) {
      return std::nullopt;
    }
    if (hasType(data, *child, type)) {
      if (match) {
        return std::nullopt;
      }
      match = child;
    }
    offset = child->end;
  }
  return match;
}

std::optional<Box> findTopLevel(std::string_view data, const char* type) {
  size_t offset = 0;
  while (offset < data.size()) {
    auto box = boxAt(data, offset, data.size());
    if (!box) {
      return std::nullopt;
    }
    if (hasType(data, *box, type)) {
      return box;
    }
    offset = box->end;
  }
  return std::nullopt;
}

struct Sample {
  uint32_t duration{0};
  uint32_t size{0};
  uint32_t flags{0};
  uint32_t compositionTimeOffset{0};
  uint8_t trunVersion{0};
  size_t dataOffset{0};
};

std::optional<uint32_t>
readOptionalBE32(const uint8_t* bytes, size_t& cursor, size_t end) {
  if (end - cursor < 4) {
    return std::nullopt;
  }
  const uint32_t value = readBE32(bytes + cursor);
  cursor += 4;
  return value;
}

std::string makeChunk(
    uint32_t trackId,
    uint32_t sampleDescriptionIndex,
    uint32_t sequenceNumber,
    uint64_t decodeTime,
    const Sample& sample,
    std::string_view fragment) {
  std::string out;
  out.reserve(kOutputMdatPayloadOffset + sample.size);

  appendBoxHeader(out, kOutputMoofSize, "moof");

  appendBoxHeader(out, 16, "mfhd");
  appendBE32(out, 0);
  appendBE32(out, sequenceNumber);

  appendBoxHeader(out, 84, "traf");

  appendBoxHeader(out, 20, "tfhd");
  appendBE32(out, kOutputTfhdFlags);
  appendBE32(out, trackId);
  appendBE32(out, sampleDescriptionIndex);

  appendBoxHeader(out, 20, "tfdt");
  appendBE32(out, 0x01000000);
  appendBE64(out, decodeTime);

  appendBoxHeader(out, 36, "trun");
  appendBE32(out, (uint32_t(sample.trunVersion) << 24) | kOutputTrunFlags);
  appendBE32(out, 1);
  appendBE32(out, kOutputMdatPayloadOffset);
  appendBE32(out, sample.duration);
  appendBE32(out, sample.size);
  appendBE32(out, sample.flags);
  appendBE32(out, sample.compositionTimeOffset);

  appendBoxHeader(out, sample.size + 8, "mdat");
  out.append(fragment.data() + sample.dataOffset, sample.size);
  return out;
}

} // namespace

CmafFrameChunker::CmafFrameChunker(std::string_view initializationSegment) {
  auto moov = findTopLevel(initializationSegment, "moov");
  if (!moov) {
    return;
  }
  auto mvex = findChild(initializationSegment, *moov, "mvex");
  if (!mvex) {
    return;
  }
  auto trex = findChild(initializationSegment, *mvex, "trex");
  if (!trex || trex->end - trex->payloadStart < 24) {
    return;
  }
  const auto* bytes =
      reinterpret_cast<const uint8_t*>(initializationSegment.data());
  const size_t fields = trex->payloadStart + 4;
  defaults_ = TrackDefaults{
      readBE32(bytes + fields),
      readBE32(bytes + fields + 4),
      readBE32(bytes + fields + 8),
      readBE32(bytes + fields + 12),
      readBE32(bytes + fields + 16)};
}

std::optional<ChunkedCmafFragment> CmafFrameChunker::chunk(
    std::string_view fragment) {
  auto moof = boxAt(fragment, 0, fragment.size());
  if (!moof || !hasType(fragment, *moof, "moof")) {
    return std::nullopt;
  }
  auto mdat = boxAt(fragment, moof->end, fragment.size());
  if (!mdat || !hasType(fragment, *mdat, "mdat")) {
    return std::nullopt;
  }

  auto traf = findUniqueChild(fragment, *moof, "traf");
  if (!traf) {
    return std::nullopt;
  }
  auto tfhd = findUniqueChild(fragment, *traf, "tfhd");
  auto tfdt = findUniqueChild(fragment, *traf, "tfdt");
  auto trun = findUniqueChild(fragment, *traf, "trun");
  if (!tfhd || !tfdt || !trun) {
    return std::nullopt;
  }

  const auto* bytes = reinterpret_cast<const uint8_t*>(fragment.data());
  if (tfhd->end - tfhd->payloadStart < 8) {
    return std::nullopt;
  }
  const uint32_t tfhdFlags = readBE24(bytes + tfhd->payloadStart + 1);
  if ((tfhdFlags & (kTfhdBaseDataOffsetPresent | kTfhdDurationIsEmpty)) != 0) {
    return std::nullopt;
  }
  const uint32_t trackId = readBE32(bytes + tfhd->payloadStart + 4);
  TrackDefaults trackDefaults;
  trackDefaults.trackId = trackId;
  if (defaults_ && defaults_->trackId == trackId) {
    trackDefaults = *defaults_;
  }

  size_t tfhdCursor = tfhd->payloadStart + 8;
  if ((tfhdFlags & kTfhdSampleDescriptionIndexPresent) != 0) {
    auto value = readOptionalBE32(bytes, tfhdCursor, tfhd->end);
    if (!value) {
      return std::nullopt;
    }
    trackDefaults.sampleDescriptionIndex = *value;
  }
  if ((tfhdFlags & kTfhdDefaultSampleDurationPresent) != 0) {
    auto value = readOptionalBE32(bytes, tfhdCursor, tfhd->end);
    if (!value) {
      return std::nullopt;
    }
    trackDefaults.sampleDuration = *value;
  }
  if ((tfhdFlags & kTfhdDefaultSampleSizePresent) != 0) {
    auto value = readOptionalBE32(bytes, tfhdCursor, tfhd->end);
    if (!value) {
      return std::nullopt;
    }
    trackDefaults.sampleSize = *value;
  }
  if ((tfhdFlags & kTfhdDefaultSampleFlagsPresent) != 0) {
    auto value = readOptionalBE32(bytes, tfhdCursor, tfhd->end);
    if (!value) {
      return std::nullopt;
    }
    trackDefaults.sampleFlags = *value;
  }
  if (trackId == 0 || trackDefaults.sampleDescriptionIndex == 0) {
    return std::nullopt;
  }

  if (tfdt->end - tfdt->payloadStart < 8) {
    return std::nullopt;
  }
  const uint8_t tfdtVersion = bytes[tfdt->payloadStart];
  uint64_t baseMediaDecodeTime = 0;
  if (tfdtVersion == 1) {
    if (tfdt->end - tfdt->payloadStart < 12) {
      return std::nullopt;
    }
    baseMediaDecodeTime = readBE64(bytes + tfdt->payloadStart + 4);
  } else if (tfdtVersion == 0) {
    baseMediaDecodeTime = readBE32(bytes + tfdt->payloadStart + 4);
  } else {
    return std::nullopt;
  }

  if (trun->end - trun->payloadStart < 8) {
    return std::nullopt;
  }
  const uint8_t trunVersion = bytes[trun->payloadStart];
  if (trunVersion > 1) {
    return std::nullopt;
  }
  const uint32_t trunFlags = readBE24(bytes + trun->payloadStart + 1);
  const uint32_t sampleCount = readBE32(bytes + trun->payloadStart + 4);
  if (sampleCount == 0 || sampleCount > mdat->end - mdat->payloadStart) {
    return std::nullopt;
  }

  size_t trunCursor = trun->payloadStart + 8;
  size_t sampleDataOffset = mdat->payloadStart;
  if ((trunFlags & kTrunDataOffsetPresent) != 0) {
    auto rawOffset = readOptionalBE32(bytes, trunCursor, trun->end);
    if (!rawOffset) {
      return std::nullopt;
    }
    const int64_t offset = static_cast<int32_t>(*rawOffset);
    if (offset < 0 || static_cast<uint64_t>(offset) < mdat->payloadStart ||
        static_cast<uint64_t>(offset) > mdat->end) {
      return std::nullopt;
    }
    sampleDataOffset = static_cast<size_t>(offset);
  }

  std::optional<uint32_t> firstSampleFlags;
  if ((trunFlags & kTrunFirstSampleFlagsPresent) != 0) {
    firstSampleFlags = readOptionalBE32(bytes, trunCursor, trun->end);
    if (!firstSampleFlags) {
      return std::nullopt;
    }
  }

  std::vector<Sample> samples;
  samples.reserve(sampleCount);
  size_t nextSampleOffset = sampleDataOffset;
  for (uint32_t i = 0; i < sampleCount; ++i) {
    Sample sample;
    sample.duration = trackDefaults.sampleDuration;
    sample.size = trackDefaults.sampleSize;
    sample.flags = i == 0 && firstSampleFlags ? *firstSampleFlags
                                              : trackDefaults.sampleFlags;
    sample.trunVersion = trunVersion;

    if ((trunFlags & kTrunSampleDurationPresent) != 0) {
      auto value = readOptionalBE32(bytes, trunCursor, trun->end);
      if (!value) {
        return std::nullopt;
      }
      sample.duration = *value;
    }
    if ((trunFlags & kTrunSampleSizePresent) != 0) {
      auto value = readOptionalBE32(bytes, trunCursor, trun->end);
      if (!value) {
        return std::nullopt;
      }
      sample.size = *value;
    }
    if ((trunFlags & kTrunSampleFlagsPresent) != 0) {
      auto value = readOptionalBE32(bytes, trunCursor, trun->end);
      if (!value) {
        return std::nullopt;
      }
      sample.flags = *value;
    }
    if ((trunFlags & kTrunSampleCompositionTimeOffsetPresent) != 0) {
      auto value = readOptionalBE32(bytes, trunCursor, trun->end);
      if (!value) {
        return std::nullopt;
      }
      sample.compositionTimeOffset = *value;
    }
    if (sample.duration == 0 || sample.size == 0 ||
        sample.size > mdat->end - nextSampleOffset) {
      return std::nullopt;
    }
    sample.dataOffset = nextSampleOffset;
    nextSampleOffset += sample.size;
    samples.push_back(sample);
  }

  ChunkedCmafFragment result;
  result.baseMediaDecodeTime = baseMediaDecodeTime;
  result.objects.reserve(samples.size());
  uint64_t decodeTime = baseMediaDecodeTime;
  for (const auto& sample : samples) {
    if (sample.size > std::numeric_limits<uint32_t>::max() - 8 ||
        decodeTime > std::numeric_limits<uint64_t>::max() - sample.duration) {
      return std::nullopt;
    }
    result.objects.push_back(
        ChunkedCmafObject{
            makeChunk(
                trackId,
                trackDefaults.sampleDescriptionIndex,
                nextSequenceNumber_++,
                decodeTime,
                sample,
                fragment),
            (sample.flags & kSampleIsNonSyncSample) == 0});
    decodeTime += sample.duration;
  }
  return result;
}

} // namespace moxygen::media_server

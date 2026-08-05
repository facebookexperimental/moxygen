/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <moxygen/media_server/sources/Fmp4MediaSource.h>

#include <moxygen/media_server/MediaCatalog.h>
#include <moxygen/media_server/sources/CatalogSource.h>

#include <folly/base64.h>
#include <folly/coro/Sleep.h>
#include <folly/io/IOBuf.h>
#include <folly/logging/xlog.h>

#include <glob.h>
#include <algorithm>
#include <cctype>
#include <cstdint>
#include <cstring>
#include <deque>
#include <fstream>
#include <iterator>
#include <utility>

namespace moxygen::media_server {

namespace {

uint32_t readBE32(const uint8_t* p) {
  return (uint32_t(p[0]) << 24) | (uint32_t(p[1]) << 16) |
      (uint32_t(p[2]) << 8) | uint32_t(p[3]);
}

uint64_t readBE64(const uint8_t* p) {
  uint64_t v = 0;
  for (int i = 0; i < 8; ++i) {
    v = (v << 8) | p[i];
  }
  return v;
}

// Find the first child box of `type` within [start, end); npos if absent.
size_t findBox(const uint8_t* d, size_t start, size_t end, const char* type) {
  size_t off = start;
  while (off + 8 <= end) {
    uint64_t size = readBE32(d + off);
    if (size == 1 && off + 16 <= end) {
      size = readBE64(d + off + 8);
    }
    if (size < 8 || off + size > end) {
      break;
    }
    if (std::memcmp(d + off + 4, type, 4) == 0) {
      return off;
    }
    off += size;
  }
  return std::string::npos;
}

// Extract a fragment's presentation-time anchor from moof -> traf -> tfdt
// (baseMediaDecodeTime): a content-derived, monotonic value used as the MoQ
// group id. Returns 0 if the boxes aren't found.
uint64_t extractSegmentStartPts(const uint8_t* d, size_t len) {
  if (len < 8 || std::memcmp(d + 4, "moof", 4) != 0) {
    return 0;
  }
  const size_t moofEnd = std::min<size_t>(readBE32(d), len);
  const size_t trafOff = findBox(d, 8, moofEnd, "traf");
  if (trafOff == std::string::npos) {
    return 0;
  }
  const size_t trafEnd =
      std::min<size_t>(trafOff + readBE32(d + trafOff), moofEnd);
  const size_t tfdtOff = findBox(d, trafOff + 8, trafEnd, "tfdt");
  if (tfdtOff == std::string::npos || tfdtOff + 12 > len) {
    return 0;
  }
  const uint8_t version = d[tfdtOff + 8];
  const uint8_t* p = d + tfdtOff + 12; // size(4)+type(4)+version(1)+flags(3)
  if (version == 1) {
    return tfdtOff + 20 <= len ? readBE64(p) : 0;
  }
  return readBE32(p);
}

// Overwrite the fragment's tfdt baseMediaDecodeTime in place. Looping replays
// the same bytes, so without this the decode timeline resets to 0 each pass and
// the player stalls at the loop boundary; rewriting it to the loop-offset PTS
// keeps the media timeline monotonic, matching the (already offset) group id.
void rewriteBaseMediaDecodeTime(std::string& bytes, uint64_t value) {
  auto* d = reinterpret_cast<uint8_t*>(bytes.data());
  const size_t len = bytes.size();
  if (len < 8 || std::memcmp(d + 4, "moof", 4) != 0) {
    return;
  }
  const size_t moofEnd = std::min<size_t>(readBE32(d), len);
  const size_t trafOff = findBox(d, 8, moofEnd, "traf");
  if (trafOff == std::string::npos) {
    return;
  }
  const size_t trafEnd =
      std::min<size_t>(trafOff + readBE32(d + trafOff), moofEnd);
  const size_t tfdtOff = findBox(d, trafOff + 8, trafEnd, "tfdt");
  if (tfdtOff == std::string::npos || tfdtOff + 12 > len) {
    return;
  }
  const uint8_t version = d[tfdtOff + 8];
  uint8_t* p = d + tfdtOff + 12; // size(4)+type(4)+version(1)+flags(3)
  if (version == 1) {
    if (tfdtOff + 20 > len) {
      return;
    }
    for (int i = 7; i >= 0; --i) {
      p[i] = static_cast<uint8_t>(value & 0xff);
      value >>= 8;
    }
  } else {
    auto v = static_cast<uint32_t>(value);
    for (int i = 3; i >= 0; --i) {
      p[i] = static_cast<uint8_t>(v & 0xff);
      v >>= 8;
    }
  }
}

// One media fragment: its presentation-time anchor (group id) and bytes.
struct Fragment {
  uint64_t pts;
  std::string bytes; // moof + mdat
};

struct ParsedMp4 {
  std::string init; // ftyp + moov
  std::vector<Fragment> fragments;
};

// Read + natural-sort + concatenate the files matching `pattern` (e.g.
// "<dir>/baseline480_v*.mp4") into one byte run, so pre-packaged CMAF/DASH
// segments (init v0 + media v1..vN) can be served directly.
std::string readGlobConcat(const std::string& pattern) {
  glob_t g{};
  std::string out;
  if (glob(pattern.c_str(), GLOB_NOSORT, nullptr, &g) == 0) {
    std::vector<std::string> paths(g.gl_pathv, g.gl_pathv + g.gl_pathc);
    // Natural order by the filename's trailing integer so v2 precedes v10.
    std::sort(paths.begin(), paths.end(), [](const auto& a, const auto& b) {
      auto num = [](const std::string& s) -> long {
        size_t e = s.find_last_of('.');
        if (e == std::string::npos) {
          e = s.size();
        }
        size_t start = e;
        while (start > 0 && std::isdigit((unsigned char)s[start - 1])) {
          --start;
        }
        return start < e ? std::stol(s.substr(start, e - start)) : -1;
      };
      const long na = num(a);
      const long nb = num(b);
      return na != nb ? na < nb : a < b;
    });
    for (const auto& p : paths) {
      std::ifstream in(p, std::ios::binary);
      out.append(
          std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>());
    }
  }
  globfree(&g);
  return out;
}

// Split a fragmented MP4 into its init segment and its (moof+mdat) fragments by
// walking the top-level box list. Everything before the first moof is init;
// each moof plus the following mdat is one fragment. Each fragment's
// segmentStartPts is extracted from its tfdt.
ParsedMp4 parseFragmentedMp4Bytes(const std::string& data) {
  const auto* d = reinterpret_cast<const uint8_t*>(data.data());
  const size_t len = data.size();

  struct Box {
    size_t off;
    size_t size;
    std::string type;
  };
  std::vector<Box> boxes;
  size_t off = 0;
  size_t firstMoof = std::string::npos;
  while (off + 8 <= len) {
    uint64_t size = readBE32(d + off);
    std::string type(reinterpret_cast<const char*>(d + off + 4), 4);
    if (size == 1 && off + 16 <= len) {
      size = readBE64(d + off + 8);
    }
    if (size < 8 || off + size > len) {
      break;
    }
    if (type == "moof" && firstMoof == std::string::npos) {
      firstMoof = off;
    }
    boxes.push_back({off, static_cast<size_t>(size), std::move(type)});
    off += size;
  }

  ParsedMp4 out;
  out.init = data.substr(0, firstMoof == std::string::npos ? len : firstMoof);
  for (size_t i = 0; i < boxes.size(); ++i) {
    if (boxes[i].type != "moof") {
      continue;
    }
    const size_t fragStart = boxes[i].off;
    size_t fragEnd = boxes[i].off + boxes[i].size;
    if (i + 1 < boxes.size() && boxes[i + 1].type == "mdat") {
      fragEnd = boxes[i + 1].off + boxes[i + 1].size;
      ++i;
    }
    std::string bytes = data.substr(fragStart, fragEnd - fragStart);
    uint64_t pts = extractSegmentStartPts(
        reinterpret_cast<const uint8_t*>(bytes.data()), bytes.size());
    out.fragments.push_back(Fragment{pts, std::move(bytes)});
  }
  return out;
}

// Whole-file bulk read; empty string if the file can't be opened (the caller
// treats that as "track unavailable" rather than crashing).
std::string readFile(const std::string& path) {
  std::ifstream in(path, std::ios::binary | std::ios::ate);
  if (!in.good()) {
    XLOG(WARN) << "[Fmp4Source] cannot open mp4: " << path;
    return {};
  }
  const std::streamsize size = in.tellg();
  in.seekg(0);
  std::string data(static_cast<size_t>(size), '\0');
  in.read(data.data(), size);
  return data;
}

ParsedMp4 parseFragmentedMp4(const std::string& path) {
  return parseFragmentedMp4Bytes(readFile(path));
}

// Directory of `path` including the trailing slash, or "" if there is none.
std::string parentDir(const std::string& path) {
  const auto slash = path.find_last_of('/');
  return slash == std::string::npos ? "" : path.substr(0, slash + 1);
}

// Resolve a track's sourceFile: absolute as-is, else relative to the catalog.
std::string resolveSourcePath(const std::string& dir, const std::string& src) {
  return (!src.empty() && src[0] == '/') ? src : dir + src;
}

bool fileExists(const std::string& path) {
  return std::ifstream(path).good();
}

// Load authored track metadata from the catalog file. Falls back to a minimal
// single-track catalog if the file is absent or malformed.
MediaCatalog loadCatalogMetadata(const std::string& path) {
  std::ifstream in(path, std::ios::binary);
  if (in.good()) {
    std::string data(
        (std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    auto parsed = parseCatalog(
        folly::ByteRange(
            reinterpret_cast<const uint8_t*>(data.data()), data.size()));
    if (parsed && !parsed->tracks.empty()) {
      XLOG(INFO) << "[Fmp4Source] loaded catalog metadata from " << path
                 << " tracks=" << parsed->tracks.size();
      return std::move(*parsed);
    }
    XLOG(WARN) << "[Fmp4Source] catalog " << path
               << " missing/malformed; using default single-track catalog";
  } else {
    XLOG(WARN) << "[Fmp4Source] no catalog file at " << path
               << "; using default single-track catalog";
  }
  MediaCatalog def;
  def.tracks.push_back(
      CatalogTrack{
          .name = "cmaf0",
          .role = "video",
          .packaging = "cmaf",
          .isLive = true,
          .initRef = "cmaf0-init",
          .mimeType = "video/mp4"});
  return def;
}

MediaObject makeObject(uint64_t group, const std::string& bytes) {
  return MediaObject{
      .group = group,
      .object = 0,
      .payload = folly::IOBuf::copyBuffer(bytes),
      .extensions = noExtensions()};
}

// Map an MSF role to the MoQ forwarding spec. Audio sorts before video (lower
// priority value = higher priority); both use one subgroup per fragment.
TrackSpec specForRole(const std::string& name, const std::string& role) {
  TrackSpec spec;
  spec.name = name;
  spec.mode = ForwardMode::SubgroupPerGroup;
  if (role == "audio") {
    spec.kind = TrackKind::Audio;
    spec.priority = 100;
  } else if (role == "caption" || role == "subtitle") {
    spec.kind = TrackKind::Subtitle;
    spec.priority = 150;
  } else {
    spec.kind = TrackKind::Video;
    spec.priority = 200;
  }
  return spec;
}

// A per-track segment feed backed by one parsed fragmented-MP4 file.
class Fmp4Track : public SegmentSource {
 public:
  Fmp4Track(
      const std::string& name,
      const std::string& role,
      ParsedMp4 parsed,
      std::chrono::milliseconds interval,
      bool loop)
      : spec_(specForRole(name, role)),
        parsed_(std::move(parsed)),
        interval_(interval),
        loop_(loop) {
    XLOG(INFO) << "[Fmp4Source] track=" << spec_.name << " role=" << role
               << " initBytes=" << parsed_.init.size()
               << " fragments=" << parsed_.fragments.size() << " firstPts="
               << (parsed_.fragments.empty() ? 0
                                             : parsed_.fragments.front().pts)
               << " lastPts="
               << (parsed_.fragments.empty() ? 0 : parsed_.fragments.back().pts)
               << " loop=" << loop_;
  }

  const TrackSpec& spec() const override {
    return spec_;
  }

  folly::coro::AsyncGenerator<MediaObject&&> objects() override {
    // group id = the fragment's segmentStartPts. When looping, offset each pass
    // by loopSpan so group ids stay strictly monotonic, and rewrite each
    // fragment's tfdt by the same offset so the decode timeline stays monotonic
    // too (else the decoder jumps backward at the loop boundary and stalls).
    const uint64_t span = loopSpan();
    uint64_t base = 0;
    do {
      for (const auto& fragment : parsed_.fragments) {
        const uint64_t group = base + fragment.pts;
        std::string bytes = fragment.bytes;
        if (base != 0) {
          rewriteBaseMediaDecodeTime(bytes, group);
        }
        // Keep the last few GOPs so a joining FETCH can hand a subscriber some
        // backfill (a startup buffer) contiguous with the live subscribe.
        recent_.emplace_back(group, bytes);
        if (recent_.size() > kRecentGops) {
          recent_.pop_front();
        }
        co_yield makeObject(group, bytes);
        co_await folly::coro::sleep(interval_);
      }
      base += span;
    } while (loop_);
  }

  folly::coro::AsyncGenerator<MediaObject&&> fetch(
      AbsoluteLocation start,
      AbsoluteLocation end) override {
    // Serve buffered recently-published objects whose (loop-offset) group is in
    // [start.group, end.group). Snapshot synchronously first so the live
    // publish loop can keep mutating recent_ while we yield.
    std::vector<std::pair<uint64_t, std::string>> snapshot;
    for (const auto& [group, bytes] : recent_) {
      if (group >= start.group && group < end.group) {
        snapshot.emplace_back(group, bytes);
      }
    }
    for (const auto& entry : snapshot) {
      co_yield makeObject(entry.first, entry.second);
    }
  }

 private:
  // Monotonic offset per loop pass = span of one pass (last pts + one gap).
  uint64_t loopSpan() const {
    if (parsed_.fragments.empty()) {
      return 1;
    }
    const uint64_t gap = parsed_.fragments.size() >= 2
        ? parsed_.fragments[1].pts - parsed_.fragments[0].pts
        : parsed_.fragments.back().pts + 1;
    return parsed_.fragments.back().pts + gap;
  }

  TrackSpec spec_;
  ParsedMp4 parsed_;
  std::chrono::milliseconds interval_;
  bool loop_;
  static constexpr size_t kRecentGops = 3;
  std::deque<std::pair<uint64_t, std::string>> recent_;
};

} // namespace

Fmp4MediaSource::Fmp4MediaSource(
    std::string catalogPath,
    std::chrono::milliseconds fragmentInterval,
    bool loop)
    : catalogPath_(std::move(catalogPath)),
      fragmentInterval_(fragmentInterval),
      loop_(loop) {}

std::string Fmp4MediaSource::catalog() {
  // Assemble the served catalog: authored metadata + each track's init segment
  // inlined as base64 (read from the head of its fMP4). sourceFile is
  // input-only and never serialized.
  auto catalog = loadCatalogMetadata(catalogPath_);
  const auto dir = parentDir(catalogPath_);
  catalog.initDataList.clear();
  for (auto& track : catalog.tracks) {
    if (track.initRef.empty()) {
      track.initRef = track.name + "-init";
    }
    const auto path = resolveSourcePath(dir, track.sourceFile);
    const bool isGlob = path.find('*') != std::string::npos;
    auto parsed = isGlob ? parseFragmentedMp4Bytes(readGlobConcat(path))
                         : parseFragmentedMp4(path);
    catalog.initDataList.push_back(
        CatalogInitData{
            .id = track.initRef,
            .type = "inline",
            .data = folly::base64Encode(parsed.init)});
  }
  auto json = serializeCatalog(catalog);
  XLOG(INFO) << "[Fmp4Source] assembled catalog tracks="
             << catalog.tracks.size() << " catalogBytes=" << json.size();
  return json;
}

std::shared_ptr<SegmentSource> Fmp4MediaSource::openTrack(
    const std::string& trackName) {
  if (trackName == kCatalogTrackName) {
    return std::make_shared<CatalogSource>(catalog());
  }
  auto catalog = loadCatalogMetadata(catalogPath_);
  const auto dir = parentDir(catalogPath_);
  for (auto& track : catalog.tracks) {
    if (track.name != trackName) {
      continue;
    }
    const auto path = resolveSourcePath(dir, track.sourceFile);
    const bool isGlob = path.find('*') != std::string::npos;
    if (!isGlob && !fileExists(path)) {
      XLOG(WARN) << "[Fmp4Source] openTrack " << trackName
                 << " file missing: " << path;
      return nullptr;
    }
    XLOG(INFO) << "[Fmp4Source] openTrack " << trackName
               << " role=" << track.role << (isGlob ? " segments=" : " file=")
               << path;
    auto parsed = isGlob ? parseFragmentedMp4Bytes(readGlobConcat(path))
                         : parseFragmentedMp4(path);
    if (parsed.fragments.empty()) {
      XLOG(WARN) << "[Fmp4Source] openTrack " << trackName
                 << " has no media fragments: " << path;
      return nullptr;
    }
    return std::make_shared<Fmp4Track>(
        track.name, track.role, std::move(parsed), fragmentInterval_, loop_);
  }
  XLOG(WARN) << "[Fmp4Source] openTrack " << trackName << " not in catalog";
  return nullptr;
}

} // namespace moxygen::media_server

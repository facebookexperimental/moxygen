/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <moxygen/samples/media_server/sources/Fmp4MediaSource.h>

#include <moxygen/samples/media_server/MediaCatalog.h>
#include <moxygen/samples/media_server/sources/CatalogSource.h>
#include <moxygen/samples/media_server/sources/CmafFrameChunker.h>
#include <moxygen/samples/media_server/sources/FilePrFaultState.h>

#include <folly/base64.h>
#include <folly/coro/Baton.h>
#include <folly/coro/Sleep.h>
#include <folly/hash/Hash.h>
#include <folly/io/IOBuf.h>
#include <folly/logging/xlog.h>

#include <glob.h>
#include <algorithm>
#include <cctype>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <deque>
#include <fstream>
#include <iterator>
#include <limits>
#include <optional>
#include <utility>

namespace moxygen::media_server {

// One media-to-wall-clock mapping shared by every track in this file-backed
// broadcast. It is accessed only on the media server's EventBase.
class Fmp4PlaybackTimeline {
 public:
  using Clock = std::chrono::steady_clock;

  struct Window {
    Clock::time_point start;
    Clock::time_point end;
  };

  void addTrack() {
    ++activeTracks_;
  }

  void removeTrack() {
    if (activeTracks_ > 0 && --activeTracks_ == 0) {
      epoch_.reset();
      mediaOrigin_.reset();
    }
  }

  Window windowFor(
      std::chrono::nanoseconds mediaTime,
      std::chrono::milliseconds width) {
    if (!epoch_) {
      epoch_ = Clock::now();
      mediaOrigin_ = mediaTime;
    }

    const auto offset = mediaTime - *mediaOrigin_;
    const auto widthNs =
        std::chrono::duration_cast<std::chrono::nanoseconds>(width);
    const auto offsetNs = offset.count();
    // Integer division truncates negative values toward zero; adjust it to
    // floor so every timestamp maps to a half-open [start, end) window.
    const int64_t windowIndex = offsetNs >= 0
        ? offsetNs / widthNs.count()
        : -((-offsetNs + widthNs.count() - 1) / widthNs.count());
    const auto start = *epoch_ + widthNs * windowIndex;
    return Window{start, start + widthNs};
  }

  std::chrono::nanoseconds currentMediaTime() const {
    return *mediaOrigin_ +
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            Clock::now() - *epoch_);
  }

 private:
  size_t activeTracks_{0};
  std::optional<Clock::time_point> epoch_;
  std::optional<std::chrono::nanoseconds> mediaOrigin_;
};

namespace {

constexpr uint64_t kNanosecondsPerSecond = 1'000'000'000;

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

struct BoxRange {
  size_t payloadStart;
  size_t end;
};

std::optional<BoxRange>
getBoxRange(const uint8_t* d, size_t off, size_t limit) {
  if (off + 8 > limit) {
    return std::nullopt;
  }
  uint64_t size = readBE32(d + off);
  size_t headerSize = 8;
  if (size == 1) {
    if (off + 16 > limit) {
      return std::nullopt;
    }
    size = readBE64(d + off + 8);
    headerSize = 16;
  } else if (size == 0) {
    size = limit - off;
  }
  if (size < headerSize || size > limit - off) {
    return std::nullopt;
  }
  return BoxRange{off + headerSize, off + static_cast<size_t>(size)};
}

// Find the first child box of `type` within [start, end); npos if absent.
size_t findBox(const uint8_t* d, size_t start, size_t end, const char* type) {
  size_t off = start;
  while (off + 8 <= end) {
    auto range = getBoxRange(d, off, end);
    if (!range) {
      break;
    }
    if (std::memcmp(d + off + 4, type, 4) == 0) {
      return off;
    }
    off = range->end;
  }
  return std::string::npos;
}

uint32_t extractTrackTimescale(const std::string& init) {
  const auto* d = reinterpret_cast<const uint8_t*>(init.data());
  const size_t len = init.size();
  const size_t moovOff = findBox(d, 0, len, "moov");
  if (moovOff == std::string::npos) {
    return 0;
  }
  auto moov = getBoxRange(d, moovOff, len);
  if (!moov) {
    return 0;
  }
  const size_t trakOff = findBox(d, moov->payloadStart, moov->end, "trak");
  if (trakOff == std::string::npos) {
    return 0;
  }
  auto trak = getBoxRange(d, trakOff, moov->end);
  if (!trak) {
    return 0;
  }
  const size_t mdiaOff = findBox(d, trak->payloadStart, trak->end, "mdia");
  if (mdiaOff == std::string::npos) {
    return 0;
  }
  auto mdia = getBoxRange(d, mdiaOff, trak->end);
  if (!mdia) {
    return 0;
  }
  const size_t mdhdOff = findBox(d, mdia->payloadStart, mdia->end, "mdhd");
  if (mdhdOff == std::string::npos) {
    return 0;
  }
  auto mdhd = getBoxRange(d, mdhdOff, mdia->end);
  if (!mdhd || mdhd->payloadStart + 4 > mdhd->end) {
    return 0;
  }
  const uint8_t version = d[mdhd->payloadStart];
  const size_t timescaleOff = mdhd->payloadStart + 4 + (version == 1 ? 16 : 8);
  if (version > 1 || timescaleOff + 4 > mdhd->end) {
    return 0;
  }
  return readBE32(d + timescaleOff);
}

std::chrono::nanoseconds mediaTime(uint64_t ticks, uint32_t timescale) {
  const uint64_t wholeSeconds = ticks / timescale;
  const uint64_t remainder = ticks % timescale;
  const uint64_t fractionalNs = remainder * kNanosecondsPerSecond / timescale;
  if (wholeSeconds >
      (static_cast<uint64_t>(std::numeric_limits<int64_t>::max()) -
       fractionalNs) /
          kNanosecondsPerSecond) {
    return std::chrono::nanoseconds::max();
  }
  return std::chrono::nanoseconds(
      wholeSeconds * kNanosecondsPerSecond + fractionalNs);
}

uint64_t mediaTicks(std::chrono::nanoseconds time, uint32_t timescale) {
  if (time <= std::chrono::nanoseconds::zero()) {
    return 0;
  }
  const uint64_t ns = time.count();
  const uint64_t wholeSeconds = ns / kNanosecondsPerSecond;
  const uint64_t remainder = ns % kNanosecondsPerSecond;
  if (wholeSeconds > (std::numeric_limits<uint64_t>::max() -
                      remainder * timescale / kNanosecondsPerSecond) /
          timescale) {
    return std::numeric_limits<uint64_t>::max();
  }
  return wholeSeconds * timescale +
      remainder * timescale / kNanosecondsPerSecond;
}

uint64_t extractBaseMediaDecodeTime(const uint8_t* d, size_t len) {
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
  return version == 0 ? readBE32(p) : 0;
}

// Overwrite the fragment's tfdt baseMediaDecodeTime in place. Looping replays
// the same bytes, so without this the decode timeline resets to 0 each pass and
// the player stalls at the loop boundary; applying the loop's decode-time
// offset keeps the media timeline monotonic, matching the already-offset group
// id.
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
  uint64_t baseMediaDecodeTime;
  std::vector<ChunkedCmafObject> objects;
};

struct ParsedMp4 {
  std::string init; // ftyp + moov
  std::vector<Fragment> fragments;
  uint32_t timescale{0};
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
// decode-time anchor is extracted from its tfdt, and its samples become CMAF
// objects.
ParsedMp4 parseFragmentedMp4Bytes(
    const std::string& data,
    bool parseMedia = true) {
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
  out.timescale = extractTrackTimescale(out.init);
  if (!parseMedia) {
    return out;
  }
  CmafFrameChunker chunker(out.init);
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
    auto chunked = chunker.chunk(bytes);
    if (!chunked) {
      XLOG(WARN) << "[Fmp4Source] cannot split fragment "
                 << out.fragments.size() << " into single-sample CMAF chunks";
      out.fragments.clear();
      return out;
    }
    out.fragments.push_back(
        Fragment{chunked->baseMediaDecodeTime, std::move(chunked->objects)});
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

ParsedMp4 parseFragmentedMp4(const std::string& path, bool parseMedia = true) {
  return parseFragmentedMp4Bytes(readFile(path), parseMedia);
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

MediaObject makeObject(
    uint64_t group,
    uint64_t subgroup,
    uint64_t object,
    const std::string& bytes,
    std::chrono::milliseconds publishDelay = std::chrono::milliseconds{0},
    bool endOfSubgroup = false,
    bool endOfGroup = false) {
  return MediaObject{
      .group = group,
      .subgroup = subgroup,
      .object = object,
      .publishDelay = publishDelay,
      .endOfSubgroup = endOfSubgroup,
      .endOfGroup = endOfGroup,
      .payload = folly::IOBuf::copyBuffer(bytes),
      .extensions = noExtensions()};
}

MediaObject
makeObject(uint64_t group, const std::string& bytes, bool endOfGroup = false) {
  return makeObject(
      group,
      /*subgroup=*/0,
      /*object=*/0,
      bytes,
      std::chrono::milliseconds::zero(),
      /*endOfSubgroup=*/true,
      endOfGroup);
}

int64_t epochMilliseconds() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             std::chrono::system_clock::now().time_since_epoch())
      .count();
}

// A retained catalog whose complete snapshots advance one group at a time.
// Group 0 is available immediately via FETCH; later groups are published at
// the configured interval and become the new result for subsequent FETCHes.
class UpdatingCatalogSource : public SegmentSource {
 public:
  UpdatingCatalogSource(
      std::vector<MediaCatalog> snapshots,
      std::chrono::milliseconds updateInterval)
      : snapshots_(std::move(snapshots)), updateInterval_(updateInterval) {
    XCHECK(!snapshots_.empty());
    XCHECK_GT(updateInterval_.count(), 0);
    spec_.name = std::string(kCatalogTrackName);
    spec_.kind = TrackKind::Catalog;
    spec_.mode = ForwardMode::SubgroupPerGroup;
    spec_.priority = 0;
    spec_.initialLargest = AbsoluteLocation{0, 0};
    materialize(0);
  }

  const TrackSpec& spec() const override {
    return spec_;
  }

  folly::coro::AsyncGenerator<MediaObject&&> objects() override {
    for (size_t group = 1; group < snapshots_.size(); ++group) {
      co_await folly::coro::sleep(updateInterval_);
      materialize(group);
      co_yield makeObject(group, latestDocument_, true);
    }

    folly::coro::Baton never;
    co_await never;
  }

  folly::coro::AsyncGenerator<MediaObject&&> fetch(
      AbsoluteLocation /*start*/,
      AbsoluteLocation /*end*/) override {
    co_yield makeObject(latestGroup_, latestDocument_, true);
  }

 private:
  void materialize(size_t group) {
    auto& snapshot = snapshots_.at(group);
    snapshot.generatedAt = epochMilliseconds();
    latestGroup_ = group;
    latestDocument_ = serializeCatalog(snapshot);
    XLOG(INFO) << "[Fmp4Source] catalog snapshot group=" << group
               << " tracks=" << snapshot.tracks.size()
               << " bytes=" << latestDocument_.size();
  }

  TrackSpec spec_;
  std::vector<MediaCatalog> snapshots_;
  std::chrono::milliseconds updateInterval_;
  uint64_t latestGroup_{0};
  std::string latestDocument_;
};

// Map an MSF role to the MoQ forwarding spec. Audio sorts before video (lower
// priority value = higher priority); both use subgroup stream delivery.
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
      std::shared_ptr<Fmp4PlaybackTimeline> timeline,
      uint32_t dropPercent,
      uint64_t dropSeed,
      bool enableFilePrFaults,
      uint64_t subgroupsPerGroup,
      bool loop)
      : spec_(specForRole(name, role)),
        parsed_(std::move(parsed)),
        interval_(interval),
        timeline_(std::move(timeline)),
        dropPercent_(
            spec_.kind == TrackKind::Video || spec_.kind == TrackKind::Audio
                ? dropPercent
                : 0),
        dropSeed_(
            folly::hash::hash_128_to_64(
                dropSeed,
                folly::hash::fnv64_FIXED(name))),
        filePrFaults_(enableFilePrFaults),
        subgroupsPerGroup_(subgroupsPerGroup),
        loop_(loop) {
    XCHECK_GT(subgroupsPerGroup_, 0);
    timeline_->addTrack();
    if (filePrFaults_) {
      filePrFaultState().registerTrack(spec_.name, role);
    }
    size_t objectCount = 0;
    for (const auto& fragment : parsed_.fragments) {
      objectCount += fragment.objects.size();
    }
    XLOG(INFO) << "[Fmp4Source] track=" << spec_.name << " role=" << role
               << " initBytes=" << parsed_.init.size()
               << " fragments=" << parsed_.fragments.size()
               << " objects=" << objectCount << " firstDecodeTime="
               << (parsed_.fragments.empty()
                       ? 0
                       : parsed_.fragments.front().baseMediaDecodeTime)
               << " lastDecodeTime="
               << (parsed_.fragments.empty()
                       ? 0
                       : parsed_.fragments.back().baseMediaDecodeTime)
               << " timescale=" << parsed_.timescale
               << " windowMs=" << interval_.count()
               << " dropPercent=" << dropPercent_
               << " filePrFaults=" << filePrFaults_
               << " subgroupsPerGroup=" << subgroupsPerGroup_
               << " loop=" << loop_;
  }

  ~Fmp4Track() override {
    if (dropPercent_ > 0) {
      XLOG(INFO) << "[Fmp4Source] track=" << spec_.name
                 << " partial-reliability candidates=" << dropCandidates_
                 << " dropped=" << droppedSegments_;
    }
    timeline_->removeTrack();
  }

  const TrackSpec& spec() const override {
    return spec_;
  }

  void onSubgroupPublishStarted(uint64_t group, uint64_t subgroup) override {
    auto held = std::find_if(
        heldSubgroups_.begin(),
        heldSubgroups_.end(),
        [group, subgroup](const auto& item) {
          return item.group == group && item.subgroup == subgroup;
        });
    if (held != heldSubgroups_.end()) {
      filePrFaultState().beginHoldRelease(spec_.name, held->commandId);
    }
  }

  void onSubgroupPublished(uint64_t group, uint64_t subgroup, size_t) override {
    auto held = std::find_if(
        heldSubgroups_.begin(),
        heldSubgroups_.end(),
        [group, subgroup](const auto& item) {
          return item.group == group && item.subgroup == subgroup;
        });
    if (held == heldSubgroups_.end()) {
      return;
    }
    const auto commandId = held->commandId;
    const auto duration = held->duration;
    const auto objects = held->objects;
    heldSubgroups_.erase(held);
    filePrFaultState().finishHold(
        spec_.name, commandId, group, subgroup, objects, duration);
  }

  folly::coro::AsyncGenerator<MediaObject&&> objects() override {
    const uint64_t span = loopSpan();
    uint64_t base = 0;
    (void)timeline_->windowFor(
        mediaTime(
            parsed_.fragments.front().baseMediaDecodeTime, parsed_.timescale),
        interval_);
    if (loop_) {
      const uint64_t currentTicks =
          mediaTicks(timeline_->currentMediaTime(), parsed_.timescale);
      const uint64_t firstDecodeTime =
          parsed_.fragments.front().baseMediaDecodeTime;
      if (currentTicks > firstDecodeTime) {
        base = ((currentTicks - firstDecodeTime) / span) * span;
      }
    }

    size_t skipped = 0;
    do {
      for (const auto& fragment : parsed_.fragments) {
        const uint64_t group = base + fragment.baseMediaDecodeTime;
        auto window = timeline_->windowFor(
            mediaTime(group, parsed_.timescale), interval_);
        auto now = Fmp4PlaybackTimeline::Clock::now();
        if (now >= window.end) {
          ++skipped;
          continue;
        }
        if (now < window.start) {
          co_await folly::coro::sleep(
              std::chrono::duration_cast<folly::HighResDuration>(
                  window.start - now));
        }
        if (Fmp4PlaybackTimeline::Clock::now() >= window.end) {
          ++skipped;
          continue;
        }
        if (skipped > 0) {
          XLOG(INFO) << "[Fmp4Source] track=" << spec_.name
                     << " skipped expired fragments=" << skipped;
          skipped = 0;
        }

        ++dropCandidates_;
        const bool randomDrop = shouldDrop(group);
        bool randomDropRecorded = false;
        bool explicitFaultInGroup = false;

        std::vector<ChunkedCmafObject> objects = fragment.objects;
        if (base != 0) {
          const uint64_t offset = group - fragment.baseMediaDecodeTime;
          for (auto& object : objects) {
            auto& bytes = object.bytes;
            const uint64_t sampleDecodeTime = extractBaseMediaDecodeTime(
                reinterpret_cast<const uint8_t*>(bytes.data()), bytes.size());
            rewriteBaseMediaDecodeTime(bytes, sampleDecodeTime + offset);
          }
        }

        std::vector<RetainedObject> publishedObjects;
        publishedObjects.reserve(objects.size());
        const uint64_t subgroupCount = subgroupsPerGroup_;
        std::vector<std::vector<RetainedObject>> subgroups(subgroupCount);
        for (size_t objectIndex = 0; objectIndex < objects.size();
             ++objectIndex) {
          const uint64_t subgroup =
              objectIndex * subgroupCount / objects.size();
          subgroups[subgroup].push_back(
              RetainedObject{
                  subgroup,
                  static_cast<uint64_t>(objectIndex),
                  objects[objectIndex].bytes,
                  objects[objectIndex].isSync});
        }

        for (uint64_t subgroup = 0; subgroup < subgroupCount; ++subgroup) {
          auto subgroupObjects = std::move(subgroups[subgroup]);
          std::optional<FilePrFaultDecision> fault;
          if (filePrFaults_) {
            auto& state = filePrFaultState();
            state.observeSubgroup(
                spec_.name, group, subgroup, subgroupObjects.size());
            fault = state.actionForSubgroup(spec_.name, group, subgroup);
            explicitFaultInGroup = explicitFaultInGroup || fault.has_value();
          }

          if (randomDrop && !explicitFaultInGroup) {
            if (!randomDropRecorded) {
              ++droppedSegments_;
              randomDropRecorded = true;
              XLOG(DBG1) << "[Fmp4Source] track=" << spec_.name
                         << " dropped group=" << group;
            }
            continue;
          }

          std::vector<RetainedObject> emitted;
          if (fault && fault->action == FilePrFaultAction::Drop) {
            const bool preserveIFrames =
                spec_.kind == TrackKind::Video && !fault->affectIFrames;
            if (preserveIFrames) {
              for (auto& object : subgroupObjects) {
                if (object.isSync) {
                  emitted.push_back(std::move(object));
                }
              }
            }
            const size_t dropped = subgroupObjects.size() - emitted.size();
            filePrFaultState().recordDrop(
                spec_.name,
                fault->commandId,
                group,
                subgroup,
                dropped,
                emitted.size());
            XLOG(INFO) << "[FilePrFault] track=" << spec_.name
                       << " dropped group=" << group << " subgroup=" << subgroup
                       << " objects=" << dropped
                       << " preservedIFrames=" << emitted.size();
          } else {
            emitted = std::move(subgroupObjects);
          }

          if (emitted.empty()) {
            continue;
          }

          const bool hold = fault && fault->action == FilePrFaultAction::Hold;
          const auto shouldDelayForHold = [&](const auto& object) {
            return hold &&
                (spec_.kind != TrackKind::Video || fault->affectIFrames ||
                 !object.isSync);
          };
          if (hold) {
            const size_t heldObjects = std::count_if(
                emitted.begin(), emitted.end(), shouldDelayForHold);
            if (heldObjects > 0) {
              heldSubgroups_.push_back(
                  HeldSubgroup{
                      group,
                      subgroup,
                      fault->commandId,
                      fault->duration,
                      heldObjects});
            } else {
              filePrFaultState().beginHoldRelease(spec_.name, fault->commandId);
              filePrFaultState().finishHold(
                  spec_.name,
                  fault->commandId,
                  group,
                  subgroup,
                  0,
                  fault->duration);
            }
            XLOG(INFO) << "[FilePrFault] track=" << spec_.name
                       << " holding group=" << group << " subgroup=" << subgroup
                       << " durationMs=" << fault->duration.count()
                       << " heldObjects=" << heldObjects
                       << " affectIFrames=" << fault->affectIFrames;
          }

          for (size_t index = 0; index < emitted.size(); ++index) {
            const bool last = index + 1 == emitted.size();
            const auto& object = emitted[index];
            const auto publishDelay = shouldDelayForHold(object)
                ? fault->duration
                : std::chrono::milliseconds::zero();
            publishedObjects.push_back(object);
            co_yield makeObject(
                group,
                object.subgroup,
                object.id,
                object.bytes,
                publishDelay,
                last,
                !filePrFaults_ && last && subgroup + 1 == subgroupCount);
          }
        }

        // Keep the last few GOPs so a joining FETCH can hand a subscriber some
        // backfill (a startup buffer) contiguous with the live subscribe.
        if (!publishedObjects.empty()) {
          recent_.push_back(RetainedGroup{group, std::move(publishedObjects)});
          if (recent_.size() > kRecentGops) {
            recent_.pop_front();
          }
        }
      }
      base += span;
    } while (loop_);
    if (skipped > 0) {
      XLOG(INFO) << "[Fmp4Source] track=" << spec_.name
                 << " skipped expired fragments=" << skipped;
    }
  }

  folly::coro::AsyncGenerator<MediaObject&&> fetch(
      AbsoluteLocation start,
      AbsoluteLocation end) override {
    // Serve buffered recently-published objects whose (loop-offset) group is in
    // [start.group, end.group). Snapshot synchronously first so the live
    // publish loop can keep mutating recent_ while we yield.
    std::vector<RetainedGroup> snapshot;
    for (const auto& retained : recent_) {
      if (retained.group >= start.group && retained.group < end.group) {
        snapshot.push_back(retained);
      }
    }
    for (const auto& retained : snapshot) {
      for (const auto& object : retained.objects) {
        co_yield makeObject(
            retained.group, object.subgroup, object.id, object.bytes);
      }
    }
  }

 private:
  bool shouldDrop(uint64_t group) const {
    if (dropPercent_ == 0) {
      return false;
    }
    const uint64_t sample =
        folly::hash::twang_mix64(folly::hash::hash_128_to_64(dropSeed_, group));
    return sample % 100 < dropPercent_;
  }

  uint64_t loopSpan() const {
    const auto& first = parsed_.fragments.front();
    const auto& last = parsed_.fragments.back();
    const uint64_t gap = parsed_.fragments.size() >= 2
        ? last.baseMediaDecodeTime -
            parsed_.fragments[parsed_.fragments.size() - 2].baseMediaDecodeTime
        : std::max<uint64_t>(
              1,
              mediaTicks(
                  std::chrono::duration_cast<std::chrono::nanoseconds>(
                      interval_),
                  parsed_.timescale));
    return last.baseMediaDecodeTime - first.baseMediaDecodeTime + gap;
  }

  TrackSpec spec_;
  ParsedMp4 parsed_;
  std::chrono::milliseconds interval_;
  std::shared_ptr<Fmp4PlaybackTimeline> timeline_;
  uint32_t dropPercent_;
  uint64_t dropSeed_;
  bool filePrFaults_;
  uint64_t subgroupsPerGroup_;
  bool loop_;
  uint64_t dropCandidates_{0};
  uint64_t droppedSegments_{0};
  static constexpr size_t kRecentGops = 3;
  struct RetainedObject {
    uint64_t subgroup;
    uint64_t id;
    std::string bytes;
    bool isSync;
  };
  struct RetainedGroup {
    uint64_t group;
    std::vector<RetainedObject> objects;
  };
  struct HeldSubgroup {
    uint64_t group;
    uint64_t subgroup;
    uint64_t commandId;
    std::chrono::milliseconds duration;
    size_t objects;
  };
  std::deque<RetainedGroup> recent_;
  std::vector<HeldSubgroup> heldSubgroups_;
};

} // namespace

Fmp4MediaSource::Fmp4MediaSource(
    std::string catalogPath,
    std::chrono::milliseconds fragmentInterval,
    bool loop)
    : catalogPath_(std::move(catalogPath)),
      timeline_(std::make_shared<Fmp4PlaybackTimeline>()),
      fragmentInterval_(fragmentInterval),
      loop_(loop) {
  XCHECK_GT(fragmentInterval_.count(), 0);
}

MediaCatalog Fmp4MediaSource::catalogMetadata() {
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
    auto parsed = isGlob
        ? parseFragmentedMp4Bytes(readGlobConcat(path), /*parseMedia=*/false)
        : parseFragmentedMp4(path, /*parseMedia=*/false);
    catalog.initDataList.push_back(
        CatalogInitData{
            .id = track.initRef,
            .type = "inline",
            .data = folly::base64Encode(parsed.init)});
  }
  return catalog;
}

std::string Fmp4MediaSource::catalog() {
  auto catalog = catalogMetadata();
  catalog.generatedAt = epochMilliseconds();
  auto json = serializeCatalog(catalog);
  XLOG(INFO) << "[Fmp4Source] assembled catalog tracks="
             << catalog.tracks.size() << " catalogBytes=" << json.size();
  return json;
}

std::vector<MediaCatalog> Fmp4MediaSource::abrCatalogSnapshots() {
  auto full = catalogMetadata();
  const auto videoCount = std::count_if(
      full.tracks.begin(), full.tracks.end(), [](const auto& track) {
        return track.role == "video";
      });
  const size_t snapshotCount = std::max<size_t>(1, videoCount);

  std::vector<MediaCatalog> snapshots;
  snapshots.reserve(snapshotCount);
  for (size_t visibleVideos = 1; visibleVideos <= snapshotCount;
       ++visibleVideos) {
    auto snapshot = full;
    snapshot.generatedAt.reset();
    snapshot.tracks.clear();
    snapshot.initDataList.clear();

    size_t seenVideos = 0;
    for (const auto& track : full.tracks) {
      const bool isVideo = track.role == "video";
      if (!isVideo || seenVideos++ < visibleVideos) {
        snapshot.tracks.push_back(track);
      }
    }
    for (const auto& init : full.initDataList) {
      const bool referenced = std::any_of(
          snapshot.tracks.begin(),
          snapshot.tracks.end(),
          [&init](const auto& track) { return track.initRef == init.id; });
      if (referenced) {
        snapshot.initDataList.push_back(init);
      }
    }
    snapshots.push_back(std::move(snapshot));
  }
  XLOG(INFO) << "[Fmp4Source] assembled ABR catalog snapshots="
             << snapshots.size() << " videoTracks=" << videoCount;
  return snapshots;
}

std::shared_ptr<SegmentSource> Fmp4MediaSource::openAbrCatalog(
    std::chrono::milliseconds updateInterval) {
  return std::make_shared<UpdatingCatalogSource>(
      abrCatalogSnapshots(), updateInterval);
}

std::shared_ptr<SegmentSource> Fmp4MediaSource::openTrack(
    const std::string& trackName,
    uint32_t dropPercent,
    uint64_t dropSeed,
    bool enableFilePrFaults,
    uint64_t subgroupsPerGroup) {
  XCHECK_LE(dropPercent, 100);
  XCHECK_GT(subgroupsPerGroup, 0);
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
    if (parsed.timescale == 0) {
      XLOG(WARN) << "[Fmp4Source] openTrack " << trackName
                 << " has no valid mdhd timescale: " << path;
      return nullptr;
    }
    for (size_t i = 1; i < parsed.fragments.size(); ++i) {
      if (parsed.fragments[i].baseMediaDecodeTime <=
          parsed.fragments[i - 1].baseMediaDecodeTime) {
        XLOG(WARN) << "[Fmp4Source] openTrack " << trackName
                   << " has non-monotonic fragment timestamps: " << path;
        return nullptr;
      }
    }
    return std::make_shared<Fmp4Track>(
        track.name,
        track.role,
        std::move(parsed),
        fragmentInterval_,
        timeline_,
        dropPercent,
        dropSeed,
        enableFilePrFaults,
        subgroupsPerGroup,
        loop_);
  }
  XLOG(WARN) << "[Fmp4Source] openTrack " << trackName << " not in catalog";
  return nullptr;
}

} // namespace moxygen::media_server

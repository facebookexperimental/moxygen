/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <folly/Benchmark.h>
#include <folly/init/Init.h>
#include <folly/io/Cursor.h>
#include <folly/io/IOBuf.h>
#include <folly/io/IOBufQueue.h>

#include <quic/codec/QuicInteger.h>
#include <quic/folly_utils/Utils.h>

#include <moxygen/MoQVarint.h>

#include <array>
#include <cstdint>
#include <memory>
#include <vector>

namespace {

using namespace moxygen;

// Representative values picking one near the high end of each varint length's
// usable range. QUIC varint supports only 1, 2, 4, 8 bytes; MoQ varint supports
// 1..9 bytes.
// Top of each length's usable range so the chosen value actually encodes
// into that length. L=K uses (1<<(7*K))-1 for K=1..8, and ~0ULL for L=9.
constexpr std::array<uint64_t, 9> kMoQValuesByLength = {
    (1ULL << 7) - 1,  // L=1: 0x7F
    (1ULL << 14) - 1, // L=2: 0x3FFF
    (1ULL << 21) - 1, // L=3: 0x1FFFFF
    (1ULL << 28) - 1, // L=4
    (1ULL << 35) - 1, // L=5
    (1ULL << 42) - 1, // L=6
    (1ULL << 49) - 1, // L=7
    (1ULL << 56) - 1, // L=8
    ~0ULL,            // L=9
};

// QUIC supports 1, 2, 4, 8 bytes. Index aligned to MoQ lengths above.
constexpr std::array<uint64_t, 4> kQuicValuesByLength = {
    (1ULL << 6) - 1,  // L=1: 0x3F
    (1ULL << 14) - 1, // L=2: 0x3FFF
    (1ULL << 30) - 1, // L=4
    (1ULL << 62) - 1, // L=8
};

// Pre-encoded buffers per length. For decode benchmarks we replay over the
// same buffer many times by resetting the cursor.

std::unique_ptr<folly::IOBuf> encodeMoQ(uint64_t value) {
  folly::IOBufQueue q{folly::IOBufQueue::cacheChainLength()};
  folly::io::QueueAppender appender(&q, 16);
  CHECK(!encodeMoQVarint(value, appender).hasError());
  return q.move();
}

std::unique_ptr<folly::IOBuf> encodeQuic(uint64_t value) {
  folly::IOBufQueue q{folly::IOBufQueue::cacheChainLength()};
  folly::io::QueueAppender appender(&q, 16);
  auto op = [a = std::move(appender)](auto val) mutable {
    a.writeBE(folly::tag<decltype(val)>, val);
  };
  CHECK(!quic::encodeQuicInteger(value, op).hasError());
  return q.move();
}

struct DecodeFixture {
  // One contiguous IOBuf per (codec, length). Decoders reset their cursor at
  // the head of this buffer each iteration.
  std::array<std::unique_ptr<folly::IOBuf>, 9> moqBufs;
  std::array<std::unique_ptr<folly::IOBuf>, 4> quicBufs;

  // Rainbow buffers: many varints concatenated, cycling through all lengths,
  // so decode benchmarks exercise the actual branch-prediction behavior on
  // mixed inputs instead of the constant-input path.
  std::unique_ptr<folly::IOBuf> moqRainbow;
  size_t moqRainbowCount = 0;
  std::unique_ptr<folly::IOBuf> quicRainbow;
  size_t quicRainbowCount = 0;
  static constexpr size_t kRainbowReps = 256;

  DecodeFixture() {
    for (size_t i = 0; i < 9; ++i) {
      moqBufs[i] = encodeMoQ(kMoQValuesByLength[i]);
    }
    for (size_t i = 0; i < 4; ++i) {
      quicBufs[i] = encodeQuic(kQuicValuesByLength[i]);
    }
    {
      folly::IOBufQueue q{folly::IOBufQueue::cacheChainLength()};
      folly::io::QueueAppender appender(&q, 16384);
      for (size_t r = 0; r < kRainbowReps; ++r) {
        for (size_t i = 0; i < 9; ++i) {
          CHECK(!encodeMoQVarint(kMoQValuesByLength[i], appender).hasError());
          ++moqRainbowCount;
        }
      }
      moqRainbow = q.move();
      moqRainbow->coalesce();
    }
    {
      folly::IOBufQueue q{folly::IOBufQueue::cacheChainLength()};
      folly::io::QueueAppender appender(&q, 16384);
      auto op = [&appender](auto val) {
        appender.writeBE(folly::tag<decltype(val)>, val);
      };
      for (size_t r = 0; r < kRainbowReps; ++r) {
        for (size_t i = 0; i < 4; ++i) {
          CHECK(
              !quic::encodeQuicInteger(kQuicValuesByLength[i], op).hasError());
          ++quicRainbowCount;
        }
      }
      quicRainbow = q.move();
      quicRainbow->coalesce();
    }
  }
};

DecodeFixture& fixture() {
  static DecodeFixture f;
  return f;
}

// ---- Decode benchmarks ----

void benchMoQDecode(size_t iters, size_t lengthIdx) {
  const folly::IOBuf& buf = *fixture().moqBufs[lengthIdx];
  for (size_t i = 0; i < iters; ++i) {
    folly::io::Cursor cursor(&buf);
    auto res = decodeMoQVarint(cursor);
    folly::doNotOptimizeAway(res);
  }
}

void benchQuicDecode(size_t iters, size_t lengthIdx) {
  const folly::IOBuf& buf = *fixture().quicBufs[lengthIdx];
  for (size_t i = 0; i < iters; ++i) {
    folly::io::Cursor cursor(&buf);
    auto res = quic::follyutils::decodeQuicInteger(cursor);
    folly::doNotOptimizeAway(res);
  }
}

// ---- Encode benchmarks ----
// To measure pure codec cost we pre-allocate one large IOBuf chunk (sized for
// all iterations) so QueueAppender never grows mid-loop and the only work in
// the timed region is the varint encode + 9-byte push.

void benchMoQEncode(size_t iters, size_t lengthIdx) {
  uint64_t value = kMoQValuesByLength[lengthIdx];
  folly::IOBufQueue q{folly::IOBufQueue::cacheChainLength()};
  folly::io::QueueAppender appender(&q, iters * 9 + 64);
  for (size_t i = 0; i < iters; ++i) {
    folly::makeUnpredictable(value);
    auto res = encodeMoQVarint(value, appender);
    folly::doNotOptimizeAway(res);
  }
  folly::doNotOptimizeAway(q);
}

void benchQuicEncode(size_t iters, size_t lengthIdx) {
  uint64_t value = kQuicValuesByLength[lengthIdx];
  folly::IOBufQueue q{folly::IOBufQueue::cacheChainLength()};
  folly::io::QueueAppender appender(&q, iters * 8 + 64);
  auto op = [&appender](auto val) {
    appender.writeBE(folly::tag<decltype(val)>, val);
  };
  for (size_t i = 0; i < iters; ++i) {
    folly::makeUnpredictable(value);
    auto res = quic::encodeQuicInteger(value, op);
    folly::doNotOptimizeAway(res);
  }
  folly::doNotOptimizeAway(q);
}

// ---- Rainbow benchmarks ----
// These exercise mixed-length inputs so the branch predictor sees realistic
// patterns rather than always-the-same-branch (which makes per-length numbers
// look unrealistically good).

void benchMoQDecodeRainbow(size_t iters) {
  const folly::IOBuf& buf = *fixture().moqRainbow;
  const size_t count = fixture().moqRainbowCount;
  folly::io::Cursor cursor(&buf);
  size_t decoded = 0;
  for (size_t i = 0; i < iters; ++i) {
    if (decoded == count) {
      cursor = folly::io::Cursor(&buf);
      decoded = 0;
    }
    auto res = decodeMoQVarint(cursor);
    folly::doNotOptimizeAway(res);
    ++decoded;
  }
}

void benchQuicDecodeRainbow(size_t iters) {
  const folly::IOBuf& buf = *fixture().quicRainbow;
  const size_t count = fixture().quicRainbowCount;
  folly::io::Cursor cursor(&buf);
  size_t decoded = 0;
  for (size_t i = 0; i < iters; ++i) {
    if (decoded == count) {
      cursor = folly::io::Cursor(&buf);
      decoded = 0;
    }
    auto res = quic::follyutils::decodeQuicInteger(cursor);
    folly::doNotOptimizeAway(res);
    ++decoded;
  }
}

void benchMoQEncodeRainbow(size_t iters) {
  folly::IOBufQueue q{folly::IOBufQueue::cacheChainLength()};
  folly::io::QueueAppender appender(&q, iters * 9 + 64);
  for (size_t i = 0; i < iters; ++i) {
    uint64_t value = kMoQValuesByLength[i % 9];
    folly::makeUnpredictable(value);
    auto res = encodeMoQVarint(value, appender);
    folly::doNotOptimizeAway(res);
  }
  folly::doNotOptimizeAway(q);
}

void benchQuicEncodeRainbow(size_t iters) {
  folly::IOBufQueue q{folly::IOBufQueue::cacheChainLength()};
  folly::io::QueueAppender appender(&q, iters * 8 + 64);
  auto op = [&appender](auto val) {
    appender.writeBE(folly::tag<decltype(val)>, val);
  };
  for (size_t i = 0; i < iters; ++i) {
    uint64_t value = kQuicValuesByLength[i % 4];
    folly::makeUnpredictable(value);
    auto res = quic::encodeQuicInteger(value, op);
    folly::doNotOptimizeAway(res);
  }
  folly::doNotOptimizeAway(q);
}

} // namespace

// Decode: MoQ for all 9 lengths; QUIC interleaved at matching length where it
// exists (1, 2, 4, 8) so the relative-time column compares directly.
BENCHMARK_NAMED_PARAM(benchMoQDecode, len1, 0)
BENCHMARK_RELATIVE_NAMED_PARAM(benchQuicDecode, len1_quic, 0)
BENCHMARK_NAMED_PARAM(benchMoQDecode, len2, 1)
BENCHMARK_RELATIVE_NAMED_PARAM(benchQuicDecode, len2_quic, 1)
BENCHMARK_NAMED_PARAM(benchMoQDecode, len3, 2)
BENCHMARK_NAMED_PARAM(benchMoQDecode, len4, 3)
BENCHMARK_RELATIVE_NAMED_PARAM(benchQuicDecode, len4_quic, 2)
BENCHMARK_NAMED_PARAM(benchMoQDecode, len5, 4)
BENCHMARK_NAMED_PARAM(benchMoQDecode, len6, 5)
BENCHMARK_NAMED_PARAM(benchMoQDecode, len7, 6)
BENCHMARK_NAMED_PARAM(benchMoQDecode, len8, 7)
BENCHMARK_RELATIVE_NAMED_PARAM(benchQuicDecode, len8_quic, 3)
BENCHMARK_NAMED_PARAM(benchMoQDecode, len9, 8)

BENCHMARK_DRAW_LINE();

// Encode: same layout.
BENCHMARK_NAMED_PARAM(benchMoQEncode, len1, 0)
BENCHMARK_RELATIVE_NAMED_PARAM(benchQuicEncode, len1_quic, 0)
BENCHMARK_NAMED_PARAM(benchMoQEncode, len2, 1)
BENCHMARK_RELATIVE_NAMED_PARAM(benchQuicEncode, len2_quic, 1)
BENCHMARK_NAMED_PARAM(benchMoQEncode, len3, 2)
BENCHMARK_NAMED_PARAM(benchMoQEncode, len4, 3)
BENCHMARK_RELATIVE_NAMED_PARAM(benchQuicEncode, len4_quic, 2)
BENCHMARK_NAMED_PARAM(benchMoQEncode, len5, 4)
BENCHMARK_NAMED_PARAM(benchMoQEncode, len6, 5)
BENCHMARK_NAMED_PARAM(benchMoQEncode, len7, 6)
BENCHMARK_NAMED_PARAM(benchMoQEncode, len8, 7)
BENCHMARK_RELATIVE_NAMED_PARAM(benchQuicEncode, len8_quic, 3)
BENCHMARK_NAMED_PARAM(benchMoQEncode, len9, 8)

BENCHMARK_DRAW_LINE();

// Rainbow: cycles through all lengths so branch prediction sees mixed inputs.
BENCHMARK(moqDecodeRainbow, iters) {
  benchMoQDecodeRainbow(iters);
}
BENCHMARK_RELATIVE(quicDecodeRainbow, iters) {
  benchQuicDecodeRainbow(iters);
}
BENCHMARK(moqEncodeRainbow, iters) {
  benchMoQEncodeRainbow(iters);
}
BENCHMARK_RELATIVE(quicEncodeRainbow, iters) {
  benchQuicEncodeRainbow(iters);
}

int main(int argc, char** argv) {
  folly::Init init(&argc, &argv);
  fixture(); // warm up
  folly::runBenchmarks();
  return 0;
}

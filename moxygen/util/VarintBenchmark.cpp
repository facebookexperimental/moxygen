#include <folly/Benchmark.h>
#include <folly/init/Init.h>
#include <quic/codec/QuicInteger.h>
#include <moxygen/util/MoQInteger.h>

void writeQuicVarint(
    folly::IOBufQueue& buf,
    uint64_t value,
    size_t& size,
    bool& error) noexcept {
  if (error) {
    return;
  }
  folly::io::QueueAppender appender(&buf, 32);
  auto appenderOp = [appender = std::move(appender)](auto val) mutable {
    appender.writeBE(val);
  };
  auto res = quic::encodeQuicInteger(value, appenderOp);
  if (res.hasError()) {
    error = true;
  } else {
    size += *res;
  }
}
void writeMoQVarint(
    folly::IOBufQueue& buf,
    uint64_t value,
    size_t& size,
    bool& error) noexcept {
  if (error) {
    return;
  }
  folly::io::QueueAppender appender(&buf, 32);
  auto appenderOp = [appender = std::move(appender)](auto val) mutable {
    appender.writeBE(val);
  };
  size += moxygen::encodeMoQInteger(value, appenderOp);
}

/*
hint:

  auto strLength = quic::decodeQuicInteger(cursor, length);
  if (!strLength) {
    return folly::makeUnexpected(ErrorCode::PARSE_UNDERFLOW);
  }

Optional<std::pair<uint64_t, size_t>> decodeMoQInteger(
    Cursor& cursor,
    uint64_t atMost = sizeof(uint64_t));


*/

void benchmarkMoQIntegerEncodeDecode(int iters, uint64_t value) {
  folly::IOBufQueue buf{folly::IOBufQueue::cacheChainLength()};
  for (int i = 0; i < iters; ++i) {
    size_t size = 0;
    bool error = false;
    writeMoQVarint(buf, value, size, error);
    folly::io::Cursor cursor(buf.front());
    auto decoded = moxygen::decodeMoQInteger(cursor);
    folly::doNotOptimizeAway(decoded);
    buf.trimStart(decoded->second);
  }
}

void benchmarkQuicIntegerEncodeDecode(int iters, uint64_t value) {
  folly::IOBufQueue buf{folly::IOBufQueue::cacheChainLength()};
  for (int i = 0; i < iters; ++i) {
    size_t size = 0;
    bool error = false;
    writeQuicVarint(buf, value, size, error);
    folly::io::Cursor cursor(buf.front());
    auto decoded = quic::decodeQuicInteger(cursor);
    folly::doNotOptimizeAway(decoded);
    buf.trimStart(decoded->second);
  }
}

BENCHMARK_PARAM(benchmarkMoQIntegerEncodeDecode, 0);
BENCHMARK_PARAM(benchmarkMoQIntegerEncodeDecode, 4096);
BENCHMARK_PARAM(benchmarkMoQIntegerEncodeDecode, 32768);
BENCHMARK_PARAM(benchmarkMoQIntegerEncodeDecode, 1099511627776);
BENCHMARK_PARAM(benchmarkMoQIntegerEncodeDecode, 1152921504606846976);

BENCHMARK_PARAM(benchmarkQuicIntegerEncodeDecode, 0);
BENCHMARK_PARAM(benchmarkQuicIntegerEncodeDecode, 4096);
BENCHMARK_PARAM(benchmarkQuicIntegerEncodeDecode, 32768);
BENCHMARK_PARAM(benchmarkQuicIntegerEncodeDecode, 1099511627776);

int main(int argc, char** argv) {
  folly::init(&argc, &argv);
  folly::runBenchmarks();
  return 0;
}

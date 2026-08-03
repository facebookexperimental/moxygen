/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <atomic>
#include <iomanip>
#include <memory>
#include <sstream>
#include <thread>
#include <vector>

#include <folly/FileUtil.h>
#include <folly/coro/BlockingWait.h>
#include <folly/coro/Sleep.h>
#include <folly/executors/IOThreadPoolExecutor.h>
#include <folly/init/Init.h>
#include <folly/io/async/ScopedEventBaseThread.h>
#include <folly/logging/xlog.h>

#include "moxygen/moqtest/MoQPerfTestClient.h"
#include "moxygen/samples/util/Utils.h"

// Declared in MoQPerfTestClient.cpp.
DECLARE_string(versions);

DEFINE_string(relay_url, "https://localhost:9999", "Relay URL to connect to");
DEFINE_string(
    transport,
    "h3wt",
    "Client transport: 'quic' (raw QUIC), 'h3wt' (HTTP/3 + WebTransport, "
    "default), 'qmux' (QMUX-on-TCP, TLS via Fizz mandatory).");
DEFINE_bool(
    quic_transport,
    false,
    "DEPRECATED: use --transport=quic (or --transport=h3wt) instead. "
    "Selects raw QUIC vs WebTransport.");
DEFINE_uint32(num_threads, 1, "Number of client threads to run");
DEFINE_uint32(duration, 60, "Test duration in seconds");
DEFINE_uint32(
    subscriber_ramp,
    50,
    "Max subscribers per second across all threads");
DEFINE_uint32(subscriber_max, 1000, "Max total subscribers");
DEFINE_uint32(
    first_object_size,
    7576,
    "Size of first object in group (I-frame)");
DEFINE_uint32(
    other_object_size,
    1894,
    "Size of other objects in group (P-frame)");
DEFINE_uint32(delivery_timeout, 500, "Delivery timeout in milliseconds");
DEFINE_uint32(objects_per_group, 30, "Number of objects per group");
DEFINE_string(
    metrics_out,
    "",
    "If set, write Prometheus .prom metrics (including the end-to-end latency "
    "histogram) to this path once per second, for a node_exporter textfile "
    "collector to scrape");

// Shared stats structure for cross-thread aggregation
struct SharedStats {
  std::atomic<uint64_t> totalObjects{0};
  std::atomic<uint64_t> totalBytes{0};
  std::atomic<uint32_t> totalSubscribers{0};
  std::atomic<uint32_t> totalResets{0};
  std::atomic<bool> trackEnded{false};
};

namespace {

struct PerfPromSnapshot {
  uint32_t subscribers{0};
  double throughputMbps{0.0};
  uint64_t totalObjects{0};
  uint64_t totalBytes{0};
  uint32_t totalResets{0};
  uint32_t totalFailures{0};
  double avgLatencyMs{0.0};
  moxygen::LatencyHistogram latency;
};

std::string escapeLabelValue(const std::string& v) {
  std::string out;
  out.reserve(v.size());
  for (char c : v) {
    if (c == '\\' || c == '"') {
      out.push_back('\\');
    }
    out.push_back(c);
  }
  return out;
}

// Rewrite the whole .prom file each tick. Buckets are cumulative over the run,
// so Prometheus rate()/histogram_quantile() work across scrapes. writeFileAtomic
// does the temp-write + rename the textfile collector requires.
void writePromFile(
    const std::string& path,
    const std::string& labels,
    const PerfPromSnapshot& s) {
  std::ostringstream os;
  auto gauge = [&](const char* name, const char* help, double value) {
    os << "# HELP " << name << " " << help << "\n"
       << "# TYPE " << name << " gauge\n"
       << name << "{" << labels << "} " << value << "\n";
  };
  auto counter = [&](const char* name, const char* help, uint64_t value) {
    os << "# HELP " << name << " " << help << "\n"
       << "# TYPE " << name << " counter\n"
       << name << "{" << labels << "} " << value << "\n";
  };

  gauge("moqperf_subscribers", "Active subscribers", s.subscribers);
  gauge(
      "moqperf_throughput_mbps",
      "Interval throughput in Mbps",
      s.throughputMbps);
  counter("moqperf_objects_total", "Objects received", s.totalObjects);
  counter("moqperf_bytes_total", "Bytes received", s.totalBytes);
  counter("moqperf_resets_total", "Subgroup resets", s.totalResets);
  counter("moqperf_failures_total", "Subscribe failures", s.totalFailures);
  gauge(
      "moqperf_latency_avg_ms",
      "Run-average end-to-end object latency in ms",
      s.avgLatencyMs);

  const char* h = "moqperf_object_latency_seconds";
  os << "# HELP " << h << " End-to-end object latency in seconds\n"
     << "# TYPE " << h << " histogram\n";
  auto cum = s.latency.cumulative();
  for (size_t i = 0; i < moxygen::LatencyHistogram::kNumBounds; ++i) {
    double leSec = static_cast<double>(moxygen::kLatencyBucketsMs[i]) / 1000.0;
    os << h << "_bucket{" << labels << ",le=\"" << leSec << "\"} " << cum[i]
       << "\n";
  }
  os << h << "_bucket{" << labels << ",le=\"+Inf\"} "
     << cum[moxygen::LatencyHistogram::kNumBounds] << "\n";
  os << h << "_sum{" << labels << "} "
     << (static_cast<double>(s.latency.sum()) / 1000.0) << "\n";
  os << h << "_count{" << labels << "} " << s.latency.count() << "\n";

  auto data = os.str();
  try {
    folly::writeFileAtomic(path, folly::StringPiece(data));
  } catch (const std::exception& ex) {
    XLOG(ERR) << "Failed to write metrics file " << path << ": " << ex.what();
  }
}

// Sum every client's cumulative latency histogram. Safe from any thread:
// snapshotLatencyHist() reads atomic counters, no EventBase hop.
moxygen::LatencyHistogram mergeLatency(
    const std::vector<std::unique_ptr<moxygen::MoQPerfTestClient>>& clients) {
  moxygen::LatencyHistogram hist;
  for (const auto& client : clients) {
    hist.merge(client->snapshotLatencyHist());
  }
  return hist;
}

} // namespace

// Stats aggregation coroutine
folly::coro::Task<void> aggregateStats(
    const std::vector<std::unique_ptr<moxygen::MoQPerfTestClient>>& clients,
    std::shared_ptr<SharedStats> sharedStats,
    folly::CancellationToken cancelToken,
    std::string metricsOut,
    std::string promLabels) {
  auto startTime = std::chrono::steady_clock::now();
  uint64_t lastTotalObjects = 0;
  uint64_t lastTotalBytes = 0;
  uint32_t lastTotalResets = 0;
  uint32_t lastTotalFailures = 0;

  while (!cancelToken.isCancellationRequested()) {
    co_await folly::coro::sleepReturnEarlyOnCancel(std::chrono::seconds(1));

    // Aggregate stats from all clients
    uint64_t totalObjects = 0;
    uint64_t totalBytes = 0;
    uint32_t peakSubscribers = 0;
    uint32_t currentSubscribers = 0;
    uint32_t totalResets = 0;
    uint32_t totalFailures = 0;
    uint32_t totalCompleted = 0;

    uint64_t totalLatencyMs = 0;
    uint64_t latencyObjects = 0;
    moxygen::MoQPerfTestClient::TestResults::IntervalLatency ivl;
    for (const auto& client : clients) {
      auto results = client->getResults();
      totalObjects += results.totalObjects;
      totalBytes += results.totalBytes;
      totalLatencyMs += results.totalLatencyMs;
      latencyObjects += results.latencyObjects;
      ivl.sumMs += results.intervalLatency.sumMs;
      ivl.count += results.intervalLatency.count;
      ivl.minMs = std::min(ivl.minMs, results.intervalLatency.minMs);
      ivl.maxMs = std::max(ivl.maxMs, results.intervalLatency.maxMs);
      peakSubscribers += results.subscribersReached;
      currentSubscribers += results.currentSubscribers;
      totalResets += results.totalResets;
      totalFailures += results.totalFailures;
      if (results.trackEnded) {
        totalCompleted++;
      }
    }

    // Update shared stats (use peak for final summary)
    sharedStats->totalObjects = totalObjects;
    sharedStats->totalBytes = totalBytes;
    sharedStats->totalSubscribers = peakSubscribers;
    sharedStats->totalResets = totalResets;

    // Calculate interval stats
    uint64_t intervalObjects =
        totalObjects >= lastTotalObjects ? totalObjects - lastTotalObjects : 0;
    uint64_t intervalBytes =
        totalBytes >= lastTotalBytes ? totalBytes - lastTotalBytes : 0;
    uint32_t intervalResets = totalResets - lastTotalResets;
    uint32_t intervalFailures = totalFailures - lastTotalFailures;

    lastTotalObjects = totalObjects;
    lastTotalBytes = totalBytes;
    lastTotalResets = totalResets;
    lastTotalFailures = totalFailures;

    auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
                       std::chrono::steady_clock::now() - startTime)
                       .count();

    double mbps = (intervalBytes * 8.0) / (1024.0 * 1024.0);
    double totalMB = totalBytes / (1024.0 * 1024.0);

    double runAvgLatencyMs = latencyObjects > 0
        ? static_cast<double>(totalLatencyMs) /
            static_cast<double>(latencyObjects)
        : 0.0;
    XLOG(INFO) << "[AGGREGATE] [" << elapsed
               << "s] Subs: " << currentSubscribers
               << " | Obj/s: " << intervalObjects << " | Mbps: " << std::fixed
               << std::setprecision(2) << mbps << " | Total: " << totalObjects
               << " objs, " << std::fixed << std::setprecision(2) << totalMB
               << " MB"
               << " | Latency(run avg): " << std::fixed << std::setprecision(1)
               << runAvgLatencyMs << " ms"
               << " | Latency(interval min/avg/max): "
               << (ivl.count > 0 ? ivl.minMs : 0) << "/" << std::fixed
               << std::setprecision(1)
               << (ivl.count > 0 ? static_cast<double>(ivl.sumMs) /
                           static_cast<double>(ivl.count)
                                 : 0.0)
               << "/" << (ivl.count > 0 ? ivl.maxMs : 0) << " ms"
               << " | Resets: " << intervalResets << "/s, " << totalResets
               << " total"
               << " | Failures: " << intervalFailures << "/s, " << totalFailures
               << " total"
               << " | Done: " << totalCompleted << "/" << clients.size();

    if (!metricsOut.empty()) {
      PerfPromSnapshot snap;
      snap.subscribers = currentSubscribers;
      snap.throughputMbps = mbps;
      snap.totalObjects = totalObjects;
      snap.totalBytes = totalBytes;
      snap.totalResets = totalResets;
      snap.totalFailures = totalFailures;
      snap.avgLatencyMs = runAvgLatencyMs;
      snap.latency = mergeLatency(clients);
      writePromFile(metricsOut, promLabels, snap);
    }

    // Check if all threads have completed
    if (totalCompleted >= clients.size()) {
      XLOG(INFO) << "[AGGREGATE] All tracks ended - stopping stats aggregation";
      sharedStats->trackEnded = true;
      break;
    }
  }
}

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, false);
  folly::Init init(&argc, &argv);
  auto transportType =
      moxygen::samples::selectClientTransport("transport", "quic_transport");
  const char* transportName =
      transportType == moxygen::samples::TransportType::QMUX   ? "QMUX"
      : transportType == moxygen::samples::TransportType::QUIC ? "QUIC"
                                                               : "WebTransport";

  XLOG(INFO) << "MoQ Performance Test Client (Multi-threaded)";
  XLOG(INFO) << "Relay URL: " << FLAGS_relay_url;
  XLOG(INFO) << "Transport: " << transportName;
  if (FLAGS_versions.empty()) {
    XLOG(INFO) << "MoQ versions: (all supported)";
  } else {
    XLOG(INFO) << "MoQ versions: " << FLAGS_versions;
  }
  XLOG(INFO) << "Number of threads: " << FLAGS_num_threads;
  XLOG(INFO) << "Duration: " << FLAGS_duration << " seconds";
  XLOG(INFO) << "Subscriber ramp (total): " << FLAGS_subscriber_ramp << "/sec";
  XLOG(INFO) << "Subscriber max: " << FLAGS_subscriber_max;
  XLOG(INFO) << "First object size: " << FLAGS_first_object_size << " bytes";
  XLOG(INFO) << "Other object size: " << FLAGS_other_object_size << " bytes";
  XLOG(INFO) << "Delivery timeout: " << FLAGS_delivery_timeout << " ms";

  if (FLAGS_num_threads == 0) {
    XLOG(ERR) << "Number of threads must be at least 1";
    return 1;
  }

  // Divide the max subscribers per second across all threads
  uint32_t subscriberRampPerThread =
      std::max(1u, FLAGS_subscriber_ramp / FLAGS_num_threads);
  uint32_t subscriberMaxPerThread =
      std::max(1u, FLAGS_subscriber_max / FLAGS_num_threads);

  XLOG(INFO) << "Subscriber ramp (per thread): " << subscriberRampPerThread
             << "/sec; Max subscribers (per thread): "
             << subscriberMaxPerThread;

  std::string versionsLabel = FLAGS_versions.empty() ? "all" : FLAGS_versions;
  std::ostringstream labelStream;
  labelStream << "transport=\"" << escapeLabelValue(transportName) << "\""
              << ",subs=\"" << FLAGS_subscriber_max << "\""
              << ",first_object_size=\"" << FLAGS_first_object_size << "\""
              << ",other_object_size=\"" << FLAGS_other_object_size << "\""
              << ",versions=\"" << escapeLabelValue(versionsLabel) << "\"";
  std::string promLabels = labelStream.str();

  try {
    auto url = proxygen::URL(FLAGS_relay_url);
    auto sharedStats = std::make_shared<SharedStats>();
    folly::CancellationSource cancelSource;

    XLOG(INFO) << "Starting " << FLAGS_num_threads << " client thread(s)...";

    // Create IO thread pool executor for client threads
    auto executor = std::make_unique<folly::IOThreadPoolExecutor>(
        FLAGS_num_threads,
        std::make_shared<folly::NamedThreadFactory>("MoQPerfTest"),
        folly::EventBaseManager::get(),
        folly::IOThreadPoolExecutor::Options().setWaitForAll(true));

    // Create clients and launch on executor
    std::vector<std::unique_ptr<moxygen::MoQPerfTestClient>> clients;
    uint32_t i = 0;
    for (auto& evb : executor->getAllEventBases()) {
      auto client = std::make_unique<moxygen::MoQPerfTestClient>(
          evb.get(),
          url,
          transportType,
          FLAGS_duration,
          subscriberRampPerThread,
          subscriberMaxPerThread,
          FLAGS_first_object_size,
          FLAGS_other_object_size,
          FLAGS_delivery_timeout,
          FLAGS_objects_per_group);

      XLOG(INFO) << "Thread " << i++ << " starting...";
      folly::coro::co_withExecutor(evb.get(), client->run()).start();
      clients.push_back(std::move(client));
    }

    // Start stats aggregation on separate thread
    folly::ScopedEventBaseThread statsThread;
    folly::coro::co_withExecutor(
        statsThread.getEventBase(),
        aggregateStats(
            clients,
            sharedStats,
            cancelSource.getToken(),
            FLAGS_metrics_out,
            promLabels))
        .start();

    // Wait for all client tasks to complete
    executor->stop();

    // Stop stats aggregation
    cancelSource.requestCancellation();
    // ScopedEventBaseThread destructor will wait for aggregateStats to finish

    // Print final results
    XLOG(INFO) << "========================================";
    XLOG(INFO) << "Final Test Summary (All Threads):";
    XLOG(INFO) << "  Threads: " << FLAGS_num_threads;
    XLOG(INFO) << "  Total Subscribers: "
               << sharedStats->totalSubscribers.load();
    XLOG(INFO) << "  Total Objects: " << sharedStats->totalObjects.load();
    XLOG(INFO) << "  Total Bytes: " << sharedStats->totalBytes.load();
    XLOG(INFO) << "  Total Resets: " << sharedStats->totalResets.load();

    // Calculate aggregate throughput and latency across all clients
    uint64_t totalBytes = 0;
    uint64_t totalLatencyMs = 0;
    uint64_t latencyObjects = 0;

    for (const auto& client : clients) {
      auto results = client->getResults();
      totalBytes += results.totalBytes;
      totalLatencyMs += results.totalLatencyMs;
      latencyObjects += results.latencyObjects;
    }

    auto duration = clients[0]->getResults().durationSeconds;
    XLOG(INFO) << "  Duration: " << duration << " seconds";

    double throughputMbps = 0.0;
    if (totalBytes > 0 && duration > 0) {
      double mbytes = static_cast<double>(totalBytes) / (1024.0 * 1024.0);
      throughputMbps = (mbytes * 8.0) / static_cast<double>(duration);
      XLOG(INFO) << "  Throughput: " << throughputMbps << " Mbps";
    }

    double avgLatencyMs = 0.0;
    if (latencyObjects > 0) {
      avgLatencyMs = static_cast<double>(totalLatencyMs) /
          static_cast<double>(latencyObjects);
      XLOG(INFO) << "  Avg Object Latency: " << avgLatencyMs << " ms ("
                 << latencyObjects << " objects measured)";
    }

    // Client threads are already joined (executor->stop()), so the histograms
    // are quiescent here; write one last snapshot to capture the final tail.
    if (!FLAGS_metrics_out.empty()) {
      PerfPromSnapshot snap;
      snap.throughputMbps = throughputMbps;
      snap.totalObjects = sharedStats->totalObjects.load();
      snap.totalBytes = totalBytes;
      snap.totalResets = sharedStats->totalResets.load();
      snap.avgLatencyMs = avgLatencyMs;
      snap.latency = mergeLatency(clients);
      writePromFile(FLAGS_metrics_out, promLabels, snap);
    }

    if (sharedStats->trackEnded.load()) {
      XLOG(INFO) << "  Result: SUCCESS - Track ended naturally";
      return 0;
    } else {
      XLOG(INFO) << "  Result: Test stopped";
      return 0;
    }

  } catch (const std::exception& ex) {
    XLOG(ERR) << "Exception: " << ex.what();
    return 1;
  }
}

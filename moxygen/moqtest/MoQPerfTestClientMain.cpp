/*
 *  Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 *  This source code is licensed under the MIT license found in the LICENSE
 *  file in the root directory of this source tree.
 *
 */

#include <atomic>
#include <iomanip>
#include <memory>
#include <thread>
#include <vector>

#include <folly/coro/BlockingWait.h>
#include <folly/coro/Sleep.h>
#include <folly/executors/IOThreadPoolExecutor.h>
#include <folly/init/Init.h>
#include <folly/io/async/ScopedEventBaseThread.h>
#include <folly/logging/xlog.h>

#include "moxygen/moqtest/MoQPerfTestClient.h"

DEFINE_string(relay_url, "https://localhost:9999", "Relay URL to connect to");
DEFINE_bool(
    quic_transport,
    false,
    "Use QUIC transport instead of WebTransport");
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

// Shared stats structure for cross-thread aggregation
struct SharedStats {
  std::atomic<uint64_t> totalObjects{0};
  std::atomic<uint64_t> totalBytes{0};
  std::atomic<uint32_t> totalSubscribers{0};
  std::atomic<uint32_t> totalResets{0};
  std::atomic<bool> trackEnded{false};
};

// Stats aggregation coroutine
folly::coro::Task<void> aggregateStats(
    const std::vector<std::unique_ptr<moxygen::MoQPerfTestClient>>& clients,
    std::shared_ptr<SharedStats> sharedStats,
    folly::CancellationToken cancelToken) {
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

    for (const auto& client : clients) {
      auto results = client->getResults();
      totalObjects += results.totalObjects;
      totalBytes += results.totalBytes;
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
    uint64_t intervalObjects = totalObjects - lastTotalObjects;
    uint64_t intervalBytes = totalBytes - lastTotalBytes;
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

    XLOG(INFO) << "[AGGREGATE] [" << elapsed
               << "s] Subs: " << currentSubscribers
               << " | Obj/s: " << intervalObjects << " | Mbps: " << std::fixed
               << std::setprecision(2) << mbps << " | Total: " << totalObjects
               << " objs, " << std::fixed << std::setprecision(2) << totalMB
               << " MB"
               << " | Resets: " << intervalResets << "/s, " << totalResets
               << " total"
               << " | Failures: " << intervalFailures << "/s, " << totalFailures
               << " total"
               << " | Done: " << totalCompleted << "/" << clients.size();

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

  XLOG(INFO) << "MoQ Performance Test Client (Multi-threaded)";
  XLOG(INFO) << "Relay URL: " << FLAGS_relay_url;
  XLOG(INFO) << "Transport: "
             << (FLAGS_quic_transport ? "QUIC" : "WebTransport");
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
          FLAGS_quic_transport,
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
        aggregateStats(clients, sharedStats, cancelSource.getToken()))
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

    // Calculate aggregate throughput across all clients
    uint64_t totalBytes = 0;

    for (const auto& client : clients) {
      auto results = client->getResults();
      totalBytes += results.totalBytes;
    }

    auto duration = clients[0]->getResults().durationSeconds;
    XLOG(INFO) << "  Duration: " << duration << " seconds";

    if (totalBytes > 0 && duration > 0) {
      double mbytes = static_cast<double>(totalBytes) / (1024.0 * 1024.0);
      double throughputMbps = (mbytes * 8.0) / static_cast<double>(duration);
      XLOG(INFO) << "  Throughput: " << throughputMbps << " Mbps";
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

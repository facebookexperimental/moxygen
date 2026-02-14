/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <folly/coro/BlockingWait.h>
#include <folly/init/Init.h>
#include <folly/logging/xlog.h>
#include <gflags/gflags.h>
#include <cstdlib>
#include <iostream>
#include <string>
#include <vector>

#include "moxygen/moqtest/interop/MoQInteropClient.h"

// CLI flags (also settable via environment variables)
DEFINE_string(relay, "", "Relay URL to test against (env: RELAY_URL)");
DEFINE_string(
    test,
    "",
    "Specific test case to run (env: TESTCASE). Runs all if not specified.");
DEFINE_bool(list, false, "List available test cases and exit");
DEFINE_bool(
    tls_disable_verify,
    false,
    "Disable TLS certificate verification (env: TLS_DISABLE_VERIFY=1)");
DEFINE_bool(verbose, false, "Enable verbose output (env: VERBOSE=1)");
DEFINE_int32(connect_timeout, 2000, "Connect timeout in milliseconds");
DEFINE_int32(transaction_timeout, 2000, "Transaction timeout in milliseconds");

namespace {

// Read environment variables as fallbacks for CLI flags
void applyEnvVarDefaults() {
  if (FLAGS_relay.empty()) {
    const char* relayEnv = std::getenv("RELAY_URL");
    if (relayEnv && relayEnv[0] != '\0') {
      FLAGS_relay = relayEnv;
    }
  }

  if (FLAGS_test.empty()) {
    const char* testEnv = std::getenv("TESTCASE");
    if (testEnv && testEnv[0] != '\0') {
      FLAGS_test = testEnv;
    }
  }

  if (!FLAGS_tls_disable_verify) {
    const char* tlsEnv = std::getenv("TLS_DISABLE_VERIFY");
    if (tlsEnv && std::string(tlsEnv) == "1") {
      FLAGS_tls_disable_verify = true;
    }
  }

  if (!FLAGS_verbose) {
    const char* verboseEnv = std::getenv("VERBOSE");
    if (verboseEnv && std::string(verboseEnv) == "1") {
      FLAGS_verbose = true;
    }
  }
}

// All available test case names
const std::vector<std::string> kAllTests = {
    "setup-only",
    "announce-only",
    "subscribe-error",
    "announce-subscribe",
    "publish-namespace-done",
    "subscribe-before-announce"};

bool isUseQuicTransport(const std::string& url) {
  // moqt:// scheme = raw QUIC, https:// = WebTransport
  return url.substr(0, 7) == "moqt://";
}

// Run a single test case by name
folly::coro::Task<moxygen::InteropTestResult> runTest(
    moxygen::MoQInteropClient& client,
    const std::string& testName) {
  if (testName == "setup-only") {
    co_return co_await client.testSetupOnly();
  } else if (testName == "announce-only") {
    co_return co_await client.testAnnounceOnly();
  } else if (testName == "subscribe-error") {
    co_return co_await client.testSubscribeError();
  } else if (testName == "announce-subscribe") {
    co_return co_await client.testAnnounceSubscribe();
  } else if (testName == "publish-namespace-done") {
    co_return co_await client.testPublishNamespaceDone();
  } else if (testName == "subscribe-before-announce") {
    co_return co_await client.testSubscribeBeforeAnnounce();
  } else {
    // Unknown test
    moxygen::InteropTestResult result;
    result.testName = testName;
    result.passed = false;
    result.message = "Unknown test case";
    co_return result;
  }
}

// Print TAP14 output for a test result
void printTapResult(
    int testNumber,
    const moxygen::InteropTestResult& result,
    bool verbose) {
  if (result.passed) {
    std::cout << "ok " << testNumber << " - " << result.testName << std::endl;
  } else {
    std::cout << "not ok " << testNumber << " - " << result.testName
              << std::endl;
  }

  // YAML diagnostic block
  std::cout << "  ---" << std::endl;
  std::cout << "  duration_ms: " << result.duration.count() << std::endl;
  if (!result.message.empty()) {
    if (result.passed) {
      std::cout << "  info: " << result.message << std::endl;
    } else {
      std::cout << "  error: " << result.message << std::endl;
    }
  }
  std::cout << "  ..." << std::endl;
}

} // namespace

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, false);
  folly::Init init(&argc, &argv);

  applyEnvVarDefaults();

  // --list: print test names and exit
  if (FLAGS_list) {
    for (const auto& name : kAllTests) {
      std::cout << name << std::endl;
    }
    return 0;
  }

  // Validate relay URL
  if (FLAGS_relay.empty()) {
    std::cerr << "Error: --relay URL or RELAY_URL env var is required"
              << std::endl;
    return 1;
  }

  auto url = proxygen::URL(FLAGS_relay);
  bool useQuic = isUseQuicTransport(FLAGS_relay);

  // Determine which tests to run
  std::vector<std::string> testsToRun;
  if (!FLAGS_test.empty()) {
    // Validate the test name
    bool found = false;
    for (const auto& name : kAllTests) {
      if (name == FLAGS_test) {
        found = true;
        break;
      }
    }
    if (!found) {
      std::cerr << "Error: unknown test case '" << FLAGS_test << "'"
                << std::endl;
      return 127; // Test not supported
    }
    testsToRun.push_back(FLAGS_test);
  } else {
    testsToRun = kAllTests;
  }

  // TAP14 header
  std::cout << "TAP version 14" << std::endl;
  std::cout << "# moxygen interop test client" << std::endl;
  std::cout << "# Relay: " << FLAGS_relay << std::endl;
  std::cout << "# Transport: " << (useQuic ? "QUIC" : "WebTransport")
            << std::endl;
  std::cout << "1.." << testsToRun.size() << std::endl;

  folly::EventBase evb;
  moxygen::MoQInteropClient client(
      &evb,
      url,
      useQuic,
      FLAGS_tls_disable_verify,
      std::chrono::milliseconds(FLAGS_connect_timeout),
      std::chrono::milliseconds(FLAGS_transaction_timeout));
  int passed = 0;
  int failed = 0;

  for (size_t i = 0; i < testsToRun.size(); ++i) {
    auto result = folly::coro::blockingWait(
        folly::coro::co_withExecutor(&evb, runTest(client, testsToRun[i])),
        &evb);

    printTapResult(static_cast<int>(i + 1), result, FLAGS_verbose);

    if (result.passed) {
      ++passed;
    } else {
      ++failed;
    }
  }

  // Drain pending cleanup (close callbacks, transport teardown)
  evb.runAfterDelay([&evb]() { evb.terminateLoopSoon(); }, 1000);
  evb.loop();

  return failed > 0 ? 1 : 0;
}

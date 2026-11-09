/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

// A standalone MoQProxy in front of upstream servers, e.g. moqtest_server, so
// the moqtest conformance suite can run through it, or switch test origins.

#include "moxygen/MoQRelaySession.h"
#include "moxygen/MoQServer.h"
#include "moxygen/MoQVersions.h"
#include "moxygen/events/MoQFollyExecutorImpl.h"
#include "moxygen/proxy/MoQProxy.h"
#include "moxygen/samples/util/Utils.h"
#include "moxygen/util/InsecureVerifierDangerousDoNotUseInProduction.h"
#include "moxygen/util/SignalHandler.h"

#include <fmt/format.h>
#include <folly/String.h>
#include <folly/coro/SharedPromise.h>
#include <folly/init/Init.h>
#include <folly/io/async/ScopedEventBaseThread.h>
#include <folly/synchronization/Baton.h>
#include <proxygen/httpserver/samples/hq/FizzContext.h>
#include <iostream>
#include <optional>
#include <vector>

DEFINE_string(cert, "", "Cert path");
DEFINE_string(key, "", "Key path");
DEFINE_string(endpoint, "/moq-proxy", "End point");
DEFINE_int32(port, 9670, "Proxy server port");
DEFINE_bool(
    insecure,
    false,
    "Serve a self-signed certificate and skip upstream certificate validation");
DEFINE_string(
    versions,
    "",
    "Comma-separated MoQ draft versions (e.g. \"14,16\"). Empty = all supported.");
DEFINE_string(
    upstream_url,
    "",
    "Comma-separated URLs of the upstream servers, in routing order "
    "(required)");
DEFINE_string(
    upstream_transport,
    "h3wt",
    "Upstream transport: 'quic', 'h3wt' or 'qmux'");
DEFINE_int32(upstream_connect_timeout_ms, 1000, "Upstream connect timeout");
DEFINE_int32(
    upstream_retry_ms,
    1000,
    "How long an upstream whose connect failed is passed over");
DEFINE_int32(
    upstream_idle_timeout_ms,
    0,
    "Upstream QUIC idle timeout; 0 keeps the transport's default");
DEFINE_bool(
    upstream_keepalive,
    false,
    "Keep quiet upstream QUIC connections alive within their idle timeout");
DEFINE_int32(
    upstream_transaction_timeout_ms,
    120000,
    "Upstream transaction timeout");
DEFINE_uint64(
    max_groups_per_track,
    3,
    "Newest groups cached per track; 0 sends every joining FETCH upstream");
DEFINE_uint64(max_cache_bytes, 10 * 1024 * 1024, "Shared cache byte budget");
DEFINE_int32(
    cache_completion_wait_ms,
    100,
    "How long a join waits for open streams to complete its range");
DEFINE_int32(
    replay_stall_timeout_ms,
    5000,
    "Reset a local replay that has no accepted write for this long");

namespace {
using namespace moxygen;

// Gives every track one upstream's session, connected when first needed and
// reconnected after it closes. The proxy tries one of these per upstream, in
// routing order, and moves on to the next when one fails. So for
// --upstream_retry_ms after a connect failed, this one fails at once. A session
// that received a GOAWAY refuses new SUBSCRIBEs, on draft 18 and later, which
// moves tracks on as a pool's health checks mark a draining server down.
class ClientUpstreamProvider : public MoQUpstreamProvider {
 public:
  ClientUpstreamProvider(
      std::shared_ptr<MoQFollyExecutorImpl> executor,
      proxygen::URL url,
      samples::TransportType transport)
      : executor_(std::move(executor)),
        url_(std::move(url)),
        transport_(transport) {}

  folly::coro::Task<MoQUpstreamSessionResult> getSession(
      const FullTrackName&,
      const TrackRequestParameters&,
      bool) override {
    const auto& cancelled = co_await folly::coro::co_current_cancellation_token;
    while (!shutdown_ && !cancelled.isCancellationRequested()) {
      if (open()) {
        co_return client_->moqSession_;
      }
      if (std::chrono::steady_clock::now() < retryAt_) {
        co_return failure("its last connect failed");
      }
      if (connecting_) {
        // Waits for the connect another track started, then looks again.
        co_await folly::coro::co_awaitTry(connecting_->getFuture());
        continue;
      }
      if (!co_await connect()) {
        co_return failure("its connect failed");
      }
    }
    co_return failure(
        shutdown_ ? "the proxy is shutting down"
                  : "cancelled while connecting");
  }

  // Closes the upstream session; call on the worker before it stops.
  void shutdown() {
    shutdown_ = true;
    client_.reset();
  }

 private:
  bool open() const {
    return client_ && client_->moqSession_ && !client_->moqSession_->isClosed();
  }

  MoQUpstreamSessionResult failure(std::string_view why) const {
    return folly::makeUnexpected(
        MoQUpstreamProviderError{fmt::format("{}: {}", url_.getUrl(), why)});
  }

  // Whether it connected. A connect that failed, unless cancelled, holds off
  // the next for --upstream_retry_ms.
  folly::coro::Task<bool> connect() {
    connecting_.emplace();
    auto client = co_await makeClient();
    const auto& cancelled = co_await folly::coro::co_current_cancellation_token;
    const bool connected = client && !shutdown_;
    if (connected) {
      client_ = std::move(client);
    } else if (!client && !cancelled.isCancellationRequested()) {
      retryAt_ = std::chrono::steady_clock::now() +
          std::chrono::milliseconds(FLAGS_upstream_retry_ms);
    }
    std::exchange(connecting_, std::nullopt)->setValue();
    co_return connected;
  }

  folly::coro::Task<std::unique_ptr<MoQClientBase>> makeClient() {
    // Named locals: gcc 11 mishandles temporaries in a co_await expression.
    quic::TransportSettings transportSettings;
    if (FLAGS_upstream_idle_timeout_ms > 0) {
      transportSettings.idleTimeout =
          std::chrono::milliseconds(FLAGS_upstream_idle_timeout_ms);
    }
    transportSettings.enableKeepalive = FLAGS_upstream_keepalive;
    const auto alpns = getMoqtProtocols(FLAGS_versions, true);
    std::shared_ptr<fizz::CertificateVerifier> verifier;
    if (FLAGS_insecure) {
      verifier = std::make_shared<
          test::InsecureVerifierDangerousDoNotUseInProduction>();
    }
    auto client = samples::makeRelayClientTransport(
        executor_, url_, std::move(verifier), transport_);
    if (!client) {
      co_return nullptr;
    }
    auto setup = co_await folly::coro::co_awaitTry(client->setupMoQSession(
        std::chrono::milliseconds(FLAGS_upstream_connect_timeout_ms),
        std::chrono::milliseconds(FLAGS_upstream_transaction_timeout_ms),
        /*publishHandler=*/nullptr,
        /*subscribeHandler=*/nullptr,
        transportSettings,
        alpns));
    if (setup.hasException() || !client->moqSession_) {
      XLOG(ERR) << "Upstream setup failed for " << url_.getUrl() << ": "
                << (setup.hasException() ? setup.exception().what()
                                         : "no session");
      co_return nullptr;
    }
    co_return client;
  }

  std::shared_ptr<MoQFollyExecutorImpl> executor_;
  proxygen::URL url_;
  samples::TransportType transport_;
  std::unique_ptr<MoQClientBase> client_;
  std::optional<folly::coro::SharedPromise<void>> connecting_;
  std::chrono::steady_clock::time_point retryAt_;
  bool shutdown_{false};
};

class ProxyServer : public MoQServer {
 public:
  template <typename... BaseArgs>
  explicit ProxyServer(std::shared_ptr<MoQProxy> proxy, BaseArgs&&... baseArgs)
      : MoQServer(std::forward<BaseArgs>(baseArgs)...),
        proxy_(std::move(proxy)) {}

  void onNewSession(std::shared_ptr<MoQSession> clientSession) override {
    clientSession->setPublishHandler(proxy_);
    clientSession->setSubscribeHandler(proxy_);
  }

 protected:
  std::shared_ptr<MoQSession> createSession(
      folly::MaybeManagedPtr<proxygen::WebTransport> wt,
      std::shared_ptr<MoQExecutor> executor) override {
    return std::make_shared<MoQRelaySession>(
        folly::MaybeManagedPtr<proxygen::WebTransport>(std::move(wt)),
        *this,
        std::move(executor));
  }

 private:
  std::shared_ptr<MoQProxy> proxy_;
};

void logJoinStats(const MoQProxyJoinStats& stats) {
  XLOG(INFO) << "Join stats: immediateHits=" << stats.immediateHits
             << " hitsAfterWait=" << stats.hitsAfterWait
             << " fallbacksMiss=" << stats.fallbacksMiss
             << " fallbacksWaitExpired=" << stats.fallbacksWaitExpired
             << " fallbacksPlanRejected=" << stats.fallbacksPlanRejected
             << " replayStallResets=" << stats.replayStallResets
             << " completionWaitTimeMs=" << stats.completionWaitTime.count();
}

std::vector<proxygen::URL> parseUpstreamUrls(const std::string& urls) {
  std::vector<std::string> specs;
  folly::split(',', urls, specs, /*ignoreEmpty=*/true);
  std::vector<proxygen::URL> parsed;
  for (const auto& spec : specs) {
    proxygen::URL url(std::string(folly::trimWhitespace(spec)));
    XLOG_IF(FATAL, !url.isValid() || !url.hasHost())
        << "Invalid URL in --upstream_url: " << spec;
    parsed.push_back(std::move(url));
  }
  XLOG_IF(FATAL, parsed.empty()) << "--upstream_url is required";
  return parsed;
}

} // namespace

int main(int argc, char* argv[]) {
  folly::Init init(&argc, &argv, true);
  auto upstreamUrls = parseUpstreamUrls(FLAGS_upstream_url);
  XLOG_IF(FATAL, FLAGS_upstream_idle_timeout_ms < 0)
      << "--upstream_idle_timeout_ms can't be negative";
  XLOG_IF(FATAL, FLAGS_upstream_retry_ms < 0)
      << "--upstream_retry_ms can't be negative";
  auto upstreamTransport =
      samples::parseTransportType(FLAGS_upstream_transport);
  XLOG_IF(FATAL, !upstreamTransport) << "Invalid --upstream_transport";

  // MoQProxy is single-threaded, so one worker serves every session.
  folly::ScopedEventBaseThread worker("MoQProxyWorker");
  auto* workerEvbPtr = worker.getEventBase();
  XCHECK(workerEvbPtr);
  auto& workerEvb = *workerEvbPtr;
  auto executor = std::make_shared<MoQFollyExecutorImpl>(&workerEvb);
  std::vector<std::shared_ptr<ClientUpstreamProvider>> providers;
  for (auto& url : upstreamUrls) {
    providers.push_back(
        std::make_shared<ClientUpstreamProvider>(
            executor, std::move(url), *upstreamTransport));
  }
  auto proxy = MoQProxy::create(
      std::vector<std::shared_ptr<MoQUpstreamProvider>>(
          providers.begin(), providers.end()),
      MoQProxyOptions{
          .maxGroupsPerTrack = FLAGS_max_groups_per_track,
          .maxCacheBytes = FLAGS_max_cache_bytes,
          .cacheCompletionWait =
              std::chrono::milliseconds(FLAGS_cache_completion_wait_ms),
          .replayStallTimeout =
              std::chrono::milliseconds(FLAGS_replay_stall_timeout_ms)});

  std::vector<std::string> alpns = {"h3"};
  auto moqt = getMoqtProtocols(FLAGS_versions, true);
  alpns.insert(alpns.end(), moqt.begin(), moqt.end());
  auto fizzContext = FLAGS_insecure
      ? quic::samples::createFizzServerContextWithInsecureDefault(
            alpns, fizz::server::ClientAuthMode::None, "", "")
      : quic::samples::createFizzServerContext(
            alpns,
            fizz::server::ClientAuthMode::Optional,
            FLAGS_cert,
            FLAGS_key);
  auto ts = MoQServer::defaultTransportSettings();
  ts.orderedReadCallbacks = true;
  auto server = std::make_shared<ProxyServer>(
      proxy,
      std::move(fizzContext),
      FLAGS_endpoint,
      MoQServer::Options{.transportSettings = ts});
  server->start(folly::SocketAddress("::", FLAGS_port), {&workerEvb});
  // Scripts wait for this line.
  std::cout << "ready port=" << server->getAddress().getPort() << std::endl;

  folly::EventBase evb;
  SignalHandler signalHandler(&evb, [&](int) {
    XLOG(INFO) << "Caught signal, stopping proxy server";
    workerEvb.runInEventBaseThreadAndWait([&proxy, &providers] {
      logJoinStats(proxy->joinStats());
      proxy->close();
      for (const auto& provider : providers) {
        provider->shutdown();
      }
    });
  });
  evb.loopForever();
  server->stop();
  // The proxy's tracks hold upstream sessions, whose transports must be
  // destroyed while the worker loop is still running.
  workerEvb.runInEventBaseThreadAndWait([&server, &proxy, &providers] {
    server.reset();
    proxy.reset();
    providers.clear();
  });
  // Closing QUIC connections finish on the worker loop; give them a moment
  // before it is destroyed.
  constexpr uint32_t kShutdownGraceMs = 1000;
  folly::Baton<> drained;
  workerEvb.runInEventBaseThread([&workerEvb, &drained] {
    workerEvb.runAfterDelay([&drained] { drained.post(); }, kShutdownGraceMs);
  });
  drained.wait();
  return 0;
}

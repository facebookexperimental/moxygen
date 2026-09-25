/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <moxygen/samples/media_server/MoQMediaListeners.h>

#include <moxygen/MoQVersions.h>
#include <moxygen/QmuxUtils.h>

#include <proxygen/httpserver/samples/hq/FizzContext.h>

#include <folly/logging/xlog.h>

#include <system_error>
#include <utility>
#include <vector>

namespace moxygen::media_server {

namespace {

constexpr auto kEndpoint = "/moq-media";
constexpr int kPickPortAttempts = 10;

std::vector<std::string> quicAlpns() {
  std::vector<std::string> alpns = {"h3"};
  auto moqt = getMoqtProtocols("", true);
  alpns.insert(alpns.end(), moqt.begin(), moqt.end());
  return alpns;
}

quic::samples::FizzServerContextPtr makeFizzContext(
    const std::vector<std::string>& alpns,
    const MediaListenerOptions& options) {
  return options.insecure
      ? quic::samples::createFizzServerContextWithInsecureDefault(
            alpns,
            fizz::server::ClientAuthMode::None,
            "" /* cert */,
            "" /* key */)
      : quic::samples::createFizzServerContext(
            alpns,
            fizz::server::ClientAuthMode::None,
            options.cert,
            options.key);
}

std::shared_ptr<MoQMediaServer> startQuicServer(
    std::shared_ptr<MoQBroadcastDispatcher> dispatcher,
    const folly::SocketAddress& addr,
    const MediaListenerOptions& options) {
  auto* evb = dispatcher->eventBase();
  auto server = std::make_shared<MoQMediaServer>(
      std::move(dispatcher), makeFizzContext(quicAlpns(), options), kEndpoint);
  server->start(addr, {evb});
  server->waitUntilInitialized();
  return server;
}

std::shared_ptr<MoQMediaQmuxServer> startQmuxServer(
    std::shared_ptr<MoQBroadcastDispatcher> dispatcher,
    const folly::SocketAddress& addr,
    const MediaListenerOptions& options) {
  auto* evb = dispatcher->eventBase();
  MoQMediaQmuxServer::Config config;
  config.selfTransportParams =
      qmuxParamsFromTransportSettings(MoQServer::defaultTransportSettings());
  // QMUX runs straight on TCP+TLS with no HTTP/3 layer, so no "h3" ALPN.
  auto server = std::make_shared<MoQMediaQmuxServer>(
      std::move(dispatcher),
      kEndpoint,
      makeFizzContext(getMoqtProtocols("", true), options),
      std::move(config));
  server->start(addr, {evb});
  return server;
}

MediaListeners startOnce(
    const std::shared_ptr<MoQBroadcastDispatcher>& dispatcher,
    const folly::SocketAddress& addr,
    const MediaListenerOptions& options) {
  MediaListeners listeners;
  listeners.evb = dispatcher->eventBase();
  auto bindAddr = addr;
  if (options.quic) {
    listeners.quic = startQuicServer(dispatcher, bindAddr, options);
    // Clients dial the same port for both transports, so with port 0 QMUX
    // takes the port QUIC was assigned.
    bindAddr.setPort(listeners.quic->getAddress().getPort());
  }
  if (options.qmux) {
    listeners.qmux = startQmuxServer(dispatcher, bindAddr, options);
  }
  return listeners;
}

} // namespace

MediaListeners& MediaListeners::operator=(MediaListeners&& other) {
  if (this != &other) {
    stop();
    quic = std::move(other.quic);
    qmux = std::move(other.qmux);
    evb = other.evb;
  }
  return *this;
}

void MediaListeners::stop() {
  if (!quic && !qmux) {
    return;
  }
  XDCHECK(!evb || !evb->isInEventBaseThread())
      << "MediaListeners::stop must not run on the dispatcher's EventBase";
  if (qmux) {
    qmux->stop();
    qmux.reset();
  }
  if (quic) {
    quic->stop();
    quic.reset();
  }
}

MediaListeners startMediaListeners(
    std::shared_ptr<MoQBroadcastDispatcher> dispatcher,
    const folly::SocketAddress& addr,
    const MediaListenerOptions& options) {
  // The TCP port matching QUIC's ephemeral UDP port can already be in use;
  // start over on a fresh port when it is.
  const bool pickPort = addr.getPort() == 0 && options.quic && options.qmux;
  for (int attempt = 1;; ++attempt) {
    try {
      return startOnce(dispatcher, addr, options);
    } catch (const std::system_error& ex) {
      if (!pickPort || ex.code() != std::errc::address_in_use) {
        throw;
      }
      if (attempt == kPickPortAttempts) {
        XLOG(ERR) << "[MediaListeners] no port free for both QUIC and QMUX "
                  << "after " << kPickPortAttempts << " attempts";
        throw;
      }
      XLOG(WARN) << "[MediaListeners] " << ex.what()
                 << "; retrying on another port (attempt " << attempt << "/"
                 << kPickPortAttempts << ")";
    }
  }
}

} // namespace moxygen::media_server

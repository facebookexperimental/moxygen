/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

// Verification subscriber for MoQMediaServer's fMP4 tracks. Subscribes to the
// well-known "catalog" track first (SUBSCRIBE + joining FETCH) to discover the
// media tracks and obtain each track's initialization segment (carried inline
// in the catalog as base64), then subscribes to EVERY media track the catalog
// lists, collecting each track's fragments keyed by group/subgroup/object and
// writing init + fragments in location order to a per-track output file
// (<output>.<track>.mp4), reconstructing a playable fragmented MP4 per track.
// Verify with `ffprobe`/`ffplay`.

#include <moxygen/MoQVersions.h>
#include <moxygen/ObjectReceiver.h>
#include <moxygen/events/MoQFollyExecutorImpl.h>
#include <moxygen/relay/MoQRelayClient.h>
#include <moxygen/samples/media_server/MediaCatalog.h>
#include <moxygen/samples/util/Utils.h>
#include <moxygen/util/InsecureVerifierDangerousDoNotUseInProduction.h>

#include <folly/Conv.h>
#include <folly/base64.h>
#include <folly/coro/Task.h>
#include <folly/coro/Timeout.h>
#include <folly/init/Init.h>
#include <folly/io/async/AsyncSignalHandler.h>
#include <folly/io/async/EventBase.h>
#include <folly/logging/xlog.h>
#include <folly/portability/GFlags.h>

#include <chrono>
#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <fstream>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <tuple>
#include <vector>

DEFINE_string(connect_url, "moqt://localhost:9779", "Server connect URL");
DEFINE_string(track_namespace, "file/moq-media", "Track namespace");
DEFINE_string(track_namespace_delimiter, "/", "Track namespace delimiter");
DEFINE_string(
    output,
    "/tmp/moq_received.mp4",
    "Output file base; each track is written to <output>.<track>.mp4");
DEFINE_int32(
    duration_s,
    0,
    "Safety cap (seconds) on how long to wait for end-of-stream before writing "
    "output. 0 = wait for the publisher's end-of-stream (publishDone) with no "
    "timeout");
DEFINE_int32(connect_timeout, 1000, "Connect timeout (ms)");
DEFINE_int32(transaction_timeout, 120, "Transaction timeout (s)");
DEFINE_bool(quic_transport, true, "Use raw QUIC transport");
DEFINE_bool(insecure, true, "Skip certificate validation");
DEFINE_string(versions, "", "Comma-separated MoQ draft versions; empty = all");

namespace {
using namespace moxygen;
using namespace moxygen::media_server;

// Collects received objects keyed by group/subgroup/object, then writes them in
// order.
class Mp4Handler : public ObjectReceiverCallback {
 public:
  FlowControlState onObject(
      std::optional<TrackAlias> /*trackAlias*/,
      const ObjectHeader& header,
      Payload payload) override {
    if (payload) {
      auto bytes = payload->moveToFbString().toStdString();
      XLOG(DBG1) << "[Mp4Receiver] onObject g=" << header.group
                 << " sg=" << header.subgroup << " o=" << header.id
                 << " bytes=" << bytes.size();
      const auto location =
          std::tuple{header.group, header.subgroup, header.id};
      const bool inserted =
          objects_.try_emplace(location, std::move(bytes)).second;
      if (!inserted) {
        XLOG(WARN) << "[Mp4Receiver] duplicate group=" << header.group
                   << " subgroup=" << header.subgroup << " object=" << header.id
                   << "; retaining first object";
      }
    }
    return FlowControlState::UNBLOCKED;
  }

  void onObjectStatus(
      std::optional<TrackAlias> /*trackAlias*/,
      const ObjectHeader& header) override {
    XLOG(DBG1) << "[Mp4Receiver] status g=" << header.group
               << " status=" << uint32_t(header.status);
  }

  void onEndOfStream() override {}

  void onError(ResetStreamErrorCode error) override {
    failed_ = true;
    XLOG(WARN) << "[Mp4Receiver] stream error=" << folly::to_underlying(error);
    baton.post();
  }

  void onPublishDone(PublishDone) override {
    XLOG(INFO) << "[Mp4Receiver] publishDone";
    baton.post();
  }

  // Init segment obtained from the catalog; written before the fragments so the
  // reassembled file decodes regardless of where we joined.
  void setInit(std::string init) {
    init_ = std::move(init);
  }

  std::optional<size_t> writeTo(const std::string& path) const {
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    if (!out.is_open()) {
      XLOG(ERR) << "[Mp4Receiver] failed to open output file: " << path;
      return std::nullopt;
    }
    size_t total = 0;
    if (!init_.empty()) {
      out.write(init_.data(), static_cast<std::streamsize>(init_.size()));
      total += init_.size();
    }
    for (const auto& entry : objects_) {
      const auto& bytes = entry.second;
      out.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
      total += bytes.size();
    }
    out.flush();
    if (!out.good()) {
      XLOG(ERR) << "[Mp4Receiver] failed to write output file: " << path;
      return std::nullopt;
    }
    return total;
  }

  size_t count() const {
    return objects_.size();
  }

  bool failed() const {
    return failed_;
  }

  folly::coro::Baton baton;

 private:
  std::string init_;
  std::map<std::tuple<uint64_t, uint64_t, uint64_t>, std::string> objects_;
  bool failed_{false};
};

// Collects the catalog document (a single retained object) and unblocks when it
// arrives.
class CatalogHandler : public ObjectReceiverCallback {
 public:
  FlowControlState onObject(
      std::optional<TrackAlias> /*trackAlias*/,
      const ObjectHeader& /*header*/,
      Payload payload) override {
    if (payload) {
      bytes += payload->moveToFbString().toStdString();
      // ObjectReceiver delivers a complete MoQ object in each callback, and
      // the joining FETCH returns exactly one retained catalog object.
      baton.post();
    }
    return FlowControlState::UNBLOCKED;
  }
  void onObjectStatus(
      std::optional<TrackAlias> /*trackAlias*/,
      const ObjectHeader& /*header*/) override {}
  void onEndOfStream() override {}
  void onError(ResetStreamErrorCode error) override {
    failed = true;
    XLOG(WARN) << "[Mp4Receiver] catalog stream error="
               << folly::to_underlying(error);
    baton.post();
  }
  void onPublishDone(PublishDone) override {}
  // FETCH stream closed (the catalog object has been delivered).
  void onAllDataReceived() override {
    baton.post();
  }

  folly::coro::Baton baton;
  std::string bytes;
  bool failed{false};
};

// Decode the init segment for `track` from the catalog's initDataList (base64).
std::string initForTrack(
    const MediaCatalog& catalog,
    const CatalogTrack& track) {
  for (const auto& init : catalog.initDataList) {
    if (init.id == track.initRef) {
      if (init.type != "inline") {
        XLOG(WARN) << "[Mp4Receiver] init type not inline: " << init.type;
        return {};
      }
      try {
        return folly::base64Decode(init.data);
      } catch (const std::exception& ex) {
        XLOG(ERR) << "[Mp4Receiver] init base64 decode failed: " << ex.what();
        return {};
      }
    }
  }
  XLOG(WARN) << "[Mp4Receiver] no initDataList entry for initRef="
             << track.initRef;
  return {};
}

// Insert ".<track>" before the extension of `base`
// (/tmp/out.mp4 -> /tmp/out.video0.mp4).
std::string outputPathForTrack(
    const std::string& base,
    const std::string& track) {
  const auto dot = base.find_last_of('.');
  const auto slash = base.find_last_of('/');
  if (dot == std::string::npos || (slash != std::string::npos && dot < slash)) {
    return base + "." + track + ".mp4";
  }
  return base.substr(0, dot) + "." + track + base.substr(dot);
}

class MoQMp4Receiver {
 public:
  MoQMp4Receiver(
      std::shared_ptr<MoQFollyExecutorImpl> evb,
      proxygen::URL url,
      std::shared_ptr<fizz::CertificateVerifier> verifier)
      : moqClient_(
            samples::makeRelayClientTransport(
                std::move(evb),
                std::move(url),
                std::move(verifier),
                FLAGS_quic_transport ? samples::TransportType::QUIC
                                     : samples::TransportType::WEB_TRANSPORT)) {
  }

  folly::coro::Task<bool> run(TrackNamespace ns) noexcept {
    bool success = true;
    try {
      auto alpns = getMoqtProtocols(FLAGS_versions, true);
      co_await moqClient_.setup(
          /*publisher=*/nullptr,
          /*subscriber=*/nullptr,
          std::chrono::milliseconds(FLAGS_connect_timeout),
          std::chrono::seconds(FLAGS_transaction_timeout),
          quic::TransportSettings(),
          alpns);

      // 1. SUBSCRIBE + joining FETCH(0) the catalog to discover tracks + obtain
      // init segments (the joining fetch delivers the current catalog object).
      auto catalog = co_await fetchCatalog(ns);
      if (!catalog || catalog->tracks.empty()) {
        XLOG(ERR) << "[Mp4Receiver] no catalog / no tracks; aborting";
        closeSession();
        co_return false;
      }
      XLOG(INFO) << "[Mp4Receiver] catalog tracks=" << catalog->tracks.size();

      // 2. Subscribe to every media track the catalog lists; each reassembles
      // to its own output file, with its init pulled from the catalog.
      for (const auto& track : catalog->tracks) {
        auto dl = std::make_shared<Download>();
        dl->name = track.name;
        dl->output = outputPathForTrack(FLAGS_output, track.name);
        dl->handler = std::make_shared<Mp4Handler>();
        dl->handler->setInit(initForTrack(*catalog, track));
        dl->receiver = std::make_shared<ObjectReceiver>(
            ObjectReceiver::SUBSCRIBE, dl->handler);
        FullTrackName ftn{ns, track.name};
        auto sub = SubscribeRequest::make(
            ftn,
            /*priority=*/0,
            GroupOrder::OldestFirst,
            /*forward=*/true,
            LocationType::LargestObject,
            /*start=*/std::nullopt,
            /*endGroup=*/0,
            /*inputParams=*/{});
        auto res =
            co_await moqClient_.getSession()->subscribe(sub, dl->receiver);
        if (res.hasError()) {
          success = false;
          XLOG(ERR) << "[Mp4Receiver] subscribe track=" << track.name
                    << " failed reason=" << res.error().reasonPhrase;
          continue;
        }
        dl->sub = std::move(res.value());
        XLOG(INFO) << "[Mp4Receiver] subscribed track=" << track.name
                   << " role=" << track.role << " codec=" << track.codec
                   << " -> " << dl->output;
        downloads_.push_back(std::move(dl));
      }
      if (downloads_.empty()) {
        XLOG(ERR) << "[Mp4Receiver] no tracks subscribed; aborting";
        closeSession();
        co_return false;
      }

      XLOG(INFO) << "[Mp4Receiver] waiting for end-of-stream on "
                 << downloads_.size() << " track(s)"
                 << (FLAGS_duration_s > 0
                         ? folly::to<std::string>(
                               " (max ", FLAGS_duration_s, "s)")
                         : std::string(" (no timeout)"));

      // Receive until every track signals end-of-stream (publishDone), or until
      // the optional safety cap fires.
      if (FLAGS_duration_s > 0) {
        auto res = co_await folly::coro::co_awaitTry(
            folly::coro::timeout(
                waitForEnd(), std::chrono::seconds(FLAGS_duration_s)));
        if (res.hasException()) {
          XLOG(WARN) << "[Mp4Receiver] no end-of-stream after "
                     << FLAGS_duration_s << "s; writing what was received";
        }
      } else {
        co_await waitForEnd();
        XLOG(INFO) << "[Mp4Receiver] all tracks ended";
      }
    } catch (const std::exception& ex) {
      success = false;
      XLOG(ERR) << "[Mp4Receiver] " << folly::exceptionStr(ex);
    }

    for (const auto& dl : downloads_) {
      if (dl->handler->failed()) {
        success = false;
      }
      const auto total = dl->handler->writeTo(dl->output);
      if (!total) {
        success = false;
        continue;
      }
      XLOG(INFO) << "[Mp4Receiver] track=" << dl->name
                 << " wrote objects=" << dl->handler->count()
                 << " bytes=" << *total << " to " << dl->output;
    }
    closeSession();
    co_return success;
  }

  // Unblocks run() so it writes output and exits. Used by the signal handler
  // for a graceful Ctrl-C (e.g. when capturing an endless live stream).
  void stop() {
    stopped_ = true;
    catalogSubHandler_->baton.post();
    catalogHandler_->baton.post();
    for (const auto& dl : downloads_) {
      dl->handler->baton.post();
    }
  }

 private:
  struct Download {
    std::string name;
    std::string output;
    std::shared_ptr<Mp4Handler> handler;
    std::shared_ptr<ObjectReceiver> receiver;
    std::shared_ptr<Publisher::SubscriptionHandle> sub;
  };

  void closeSession() {
    if (moqClient_.getSession()) {
      moqClient_.getSession()->close(SessionCloseErrorCode::NO_ERROR);
    }
  }

  // Gets the catalog via SUBSCRIBE + joining FETCH(0) (MSF/CMSF): the subscribe
  // leg carries future updates, the joining fetch delivers the current catalog
  // object. The two are issued sequentially (subscribe established first) so
  // the joining fetch never races ahead of the subscription it references.
  // Returns the parsed catalog document.
  folly::coro::Task<std::optional<MediaCatalog>> fetchCatalog(
      TrackNamespace ns) {
    FullTrackName catFtn{std::move(ns), std::string(kCatalogTrackName)};
    auto catSub = SubscribeRequest::make(
        catFtn,
        /*priority=*/0,
        GroupOrder::OldestFirst,
        /*forward=*/true,
        LocationType::LargestObject,
        /*start=*/std::nullopt,
        /*endGroup=*/0,
        /*inputParams=*/{});
    auto subResult = co_await moqClient_.getSession()->subscribe(
        catSub, catalogSubReceiver_);
    if (subResult.hasError()) {
      XLOG(ERR) << "[Mp4Receiver] catalog subscribe failed reason="
                << subResult.error().reasonPhrase;
      co_return std::nullopt;
    }
    catalogSubscription_ = std::move(subResult.value());

    // Joining FETCH offset 0 back-fills the current catalog object {0,0}. A
    // null joining request id lets the session resolve it to the catalog
    // subscription established above (by full track name).
    Fetch fetchReq(
        RequestID{0},
        /*jsid=*/std::nullopt,
        /*joiningStart=*/0,
        FetchType::RELATIVE_JOINING);
    fetchReq.fullTrackName = catFtn;
    auto fetchResult = co_await moqClient_.getSession()->fetch(
        fetchReq, catalogFetchReceiver_);
    if (fetchResult.hasError()) {
      XLOG(ERR) << "[Mp4Receiver] catalog joining fetch failed reason="
                << fetchResult.error().reasonPhrase;
      co_return std::nullopt;
    }
    catalogFetch_ = std::move(fetchResult.value());
    co_await catalogHandler_->baton;
    if (stopped_ || catalogSubHandler_->failed || catalogHandler_->failed) {
      co_return std::nullopt;
    }
    XLOG(INFO) << "[Mp4Receiver] catalog received via FETCH bytes="
               << catalogHandler_->bytes.size();
    co_return parseCatalog(
        folly::ByteRange(
            reinterpret_cast<const uint8_t*>(catalogHandler_->bytes.data()),
            catalogHandler_->bytes.size()));
  }

  folly::coro::Task<void> waitForEnd() {
    for (const auto& dl : downloads_) {
      co_await dl->handler->baton;
    }
  }

  MoQRelayClient moqClient_;
  std::vector<std::shared_ptr<Download>> downloads_;
  // Catalog: a SUBSCRIBE leg (future updates) + a FETCH leg (current catalog).
  std::shared_ptr<CatalogHandler> catalogSubHandler_{
      std::make_shared<CatalogHandler>()};
  std::shared_ptr<ObjectReceiver> catalogSubReceiver_{
      std::make_shared<ObjectReceiver>(
          ObjectReceiver::SUBSCRIBE,
          catalogSubHandler_)};
  std::shared_ptr<CatalogHandler> catalogHandler_{
      std::make_shared<CatalogHandler>()};
  std::shared_ptr<ObjectReceiver> catalogFetchReceiver_{
      std::make_shared<ObjectReceiver>(ObjectReceiver::FETCH, catalogHandler_)};
  std::shared_ptr<Publisher::SubscriptionHandle> catalogSubscription_;
  std::shared_ptr<Publisher::FetchHandle> catalogFetch_;
  bool stopped_{false};
};

} // namespace

int main(int argc, char* argv[]) {
  folly::Init init(&argc, &argv, false);

  folly::EventBase eventBase;
  proxygen::URL url(FLAGS_connect_url);
  const bool isValidMoqtUrl = FLAGS_quic_transport &&
      url.getScheme() == "moqt" && !url.getHost().empty();
  if ((!url.isValid() || !url.hasHost()) && !isValidMoqtUrl) {
    XLOG(ERR) << "Invalid connect_url: " << FLAGS_connect_url;
    return EXIT_FAILURE;
  }
  auto moqEvb = std::make_shared<moxygen::MoQFollyExecutorImpl>(&eventBase);
  std::shared_ptr<fizz::CertificateVerifier> verifier;
  if (FLAGS_insecure) {
    verifier = std::make_shared<
        moxygen::test::InsecureVerifierDangerousDoNotUseInProduction>();
  }

  moxygen::TrackNamespace ns(
      FLAGS_track_namespace, FLAGS_track_namespace_delimiter);
  auto client = std::make_shared<MoQMp4Receiver>(
      moqEvb, std::move(url), std::move(verifier));
  int exitCode = EXIT_FAILURE;

  // Graceful Ctrl-C: post the end-of-stream baton so run() writes and exits.
  class SigHandler : public folly::AsyncSignalHandler {
   public:
    SigHandler(folly::EventBase* evb, std::function<void(int)> fn)
        : folly::AsyncSignalHandler(evb), fn_(std::move(fn)) {
      registerSignalHandler(SIGINT);
      registerSignalHandler(SIGTERM);
    }
    void signalReceived(int sig) noexcept override {
      fn_(sig);
    }

   private:
    std::function<void(int)> fn_;
  };
  SigHandler sigHandler(&eventBase, [&client](int) { client->stop(); });

  folly::coro::co_withExecutor(&eventBase, client->run(std::move(ns)))
      .start()
      .via(&eventBase)
      .thenTry([&eventBase, &exitCode](auto&& result) {
        if (result.hasValue() && result.value()) {
          exitCode = EXIT_SUCCESS;
        }
        eventBase.terminateLoopSoon();
      });
  eventBase.loopForever();
  return exitCode;
}

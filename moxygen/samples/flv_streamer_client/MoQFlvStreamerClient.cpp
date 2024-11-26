/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <folly/init/Init.h>
#include <folly/io/async/AsyncSignalHandler.h>
#include <folly/portability/GFlags.h>
#include <signal.h>
#include <filesystem>
#include "moxygen/MoQClient.h"
#include "moxygen/flv_parser/FlvSequentialReader.h"
#include "moxygen/moq_mi/MoQMi.h"

DEFINE_string(input_flv_file, "", "FLV input fifo file");
DEFINE_string(
    connect_url,
    "https://localhost:4433/moq",
    "URL for webtransport server");
DEFINE_string(track_namespace, "flvstreamer", "Track Namespace");
DEFINE_string(track_namespace_delimiter, "/", "Track Namespace Delimiter");
DEFINE_string(video_track_name, "video0", "Video track Name");
DEFINE_string(audio_track_name, "audio0", "Audio track Name");
DEFINE_int32(connect_timeout, 1000, "Connect timeout (ms)");
DEFINE_int32(transaction_timeout, 120, "Transaction timeout (s)");
DEFINE_bool(quic_transport, false, "Use raw QUIC transport");

namespace {
using namespace moxygen;

class MoQFlvStreamerClient {
 public:
  MoQFlvStreamerClient(
      folly::EventBase* evb,
      proxygen::URL url,
      FullTrackName fvtn,
      FullTrackName fatn)
      : moqClient_(
            evb,
            std::move(url),
            (FLAGS_quic_transport ? MoQClient::TransportType::QUIC
                                  : MoQClient::TransportType::H3_WEBTRANSPORT)),
        evb_(evb),
        fullVideoTrackName_(std::move(fvtn)),
        fullAudioTrackName_(std::move(fatn)) {}

  folly::coro::Task<void> run(Announce ann) noexcept {
    XLOG(INFO) << __func__;
    auto g =
        folly::makeGuard([func = __func__] { XLOG(INFO) << "exit " << func; });
    try {
      // Create session
      co_await moqClient_.setupMoQSession(
          std::chrono::milliseconds(FLAGS_connect_timeout),
          std::chrono::seconds(FLAGS_transaction_timeout),
          Role::PUBLISHER);
      auto exec = co_await folly::coro::co_current_executor;
      controlReadLoop().scheduleOn(exec).start();

      // Announce
      auto annResp = co_await moqClient_.moqSession_->announce(std::move(ann));
      if (annResp.hasValue()) {
        trackNamespace_ = annResp->trackNamespace;

        publishLoop()
            .scheduleOn(folly::getGlobalIOExecutor())
            .start()
            .via(evb_)
            .thenTry([this](auto&&) { stop(); });
      } else {
        XLOG(INFO) << "Announce error trackNamespace="
                   << annResp->trackNamespace
                   << " code=" << annResp.error().errorCode
                   << " reason=" << annResp.error().reasonPhrase;
      }
    } catch (const std::exception& ex) {
      XLOG(ERR) << ex.what();
      co_return;
    }
    XLOG(INFO) << __func__ << " done";
  }

  void stop() {
    XLOG(INFO) << __func__;
    if (moqClient_.moqSession_) {
      moqClient_.moqSession_->unannounce({trackNamespace_});
      moqClient_.moqSession_->close();
    }
  }

  folly::coro::Task<void> publishLoop() {
    XLOG(INFO) << __func__;
    auto g =
        folly::makeGuard([func = __func__] { XLOG(INFO) << "exit " << func; });

    FlvSequentialReader flvSeqReader(FLAGS_input_flv_file);
    while (moqClient_.moqSession_) {
      auto item = flvSeqReader.getNextItem();
      if (item == nullptr) {
        XLOG(ERR) << "Error reading FLV file";
        break;
      }
      for (auto& sub : subscriptions_) {
        XLOG(DBG1) << "Sending item: " << item->id
                   << ", type: " << folly::to_underlying(item->type)
                   << ", to subID-TrackAlias: " << sub.second.subscribeID << "-"
                   << sub.second.trackAlias;

        if (sub.second.fullTrackName == fullVideoTrackName_) {
          auto videoPriority = calculatePriotity(latestVideo_, false);

          if (item->isEOF &&
              MoQFlvStreamerClient::isAnyElementSent(latestVideo_)) {
            // EOF detected and an some video element was sent, close group
            auto objHeaderEndOfGroup = createObjectHeaderEndOfGroup(
                sub.second.trackAlias,
                latestVideo_.group,
                latestVideo_.group,
                latestVideo_.object++,
                videoPriority,
                ForwardPreference::Subgroup);
            XLOG(DBG1) << "Closing group because EOF. objHeader: "
                       << objHeaderEndOfGroup;
            moqClient_.getEventBase()->runInEventBaseThread(
                [this,
                 objHeaderEndOfGroup,
                 subscribeID(sub.second.subscribeID)] {
                  moqClient_.moqSession_->publishStatus(
                      objHeaderEndOfGroup, subscribeID);
                });
          }

          if (item->type == FlvSequentialReader::MediaType::VIDEO) {
            // Video
            if (item->isIdr &&
                MoQFlvStreamerClient::isAnyElementSent(latestVideo_)) {
              // Close group
              auto objHeaderEndOfGroup = createObjectHeaderEndOfGroup(
                  sub.second.trackAlias,
                  latestVideo_.group,
                  0, // subgroup per group
                  latestVideo_.object++,
                  videoPriority,
                  ForwardPreference::Subgroup);
              XLOG(DBG1) << "Closing group because IDR. objHeader: "
                         << objHeaderEndOfGroup;
              moqClient_.getEventBase()->runInEventBaseThread(
                  [this,
                   objHeaderEndOfGroup,
                   subscribeID(sub.second.subscribeID)] {
                    moqClient_.moqSession_->publishStatus(
                        objHeaderEndOfGroup, subscribeID);
                  });

              // Start new group
              latestVideo_.group++;
              latestVideo_.object = 0;
            }
            moqClient_.getEventBase()->runInEventBaseThread(
                [this,
                 item,
                 latestVideo(latestVideo_),
                 videoPriority,
                 trackAlias(sub.second.trackAlias),
                 subscribeID(sub.second.subscribeID)] {
                  auto objPayload = encodeToMoQMi(item);
                  if (!objPayload) {
                    XLOG(ERR) << "Failed to encode video frame";
                  } else {
                    ObjectHeader objHeader = ObjectHeader{
                        trackAlias,
                        latestVideo.group,
                        latestVideo.group,
                        latestVideo.object,
                        videoPriority,
                        ForwardPreference::Subgroup,
                        ObjectStatus::NORMAL,
                        objPayload->computeChainDataLength()};

                    XLOG(DBG1) << "Sending video frame" << objHeader
                               << ", payload size: "
                               << objPayload->computeChainDataLength();

                    moqClient_.moqSession_->publish(
                        objHeader,
                        subscribeID,
                        0,
                        std::move(objPayload),
                        false);
                  }
                });

            latestVideo_.object++;
          }
        }
        if (sub.second.fullTrackName == fullAudioTrackName_ &&
            item->type == FlvSequentialReader::MediaType::AUDIO) {
          // Audio
          auto audioPriority = calculatePriotity(latestAudio_, true);

          if (item->data) {
            // Send audio data in a thread (stream per object)
            ObjectHeader objHeader = ObjectHeader{
                sub.second.trackAlias,
                latestAudio_.group++,
                /*subgroup=*/0,
                latestAudio_.object,
                audioPriority,
                ForwardPreference::Subgroup,
                ObjectStatus::NORMAL};

            moqClient_.getEventBase()->runInEventBaseThread(
                [this, objHeader, item, subscribeID(sub.second.subscribeID)] {
                  auto objPayload = encodeToMoQMi(item);
                  if (!objPayload) {
                    XLOG(ERR) << "Failed to encode audio frame";
                  } else {
                    XLOG(DBG1) << "Sending audio frame" << objHeader
                               << ", payload size: "
                               << objPayload->computeChainDataLength();
                    moqClient_.moqSession_->publishStreamPerObject(
                        objHeader, subscribeID, 0, std::move(objPayload), true);
                  }
                });
          }
        }
      }
      if (item->isEOF) {
        XLOG(INFO) << "FLV file EOF";
        break;
      }
    }
    co_return;
  }

  folly::coro::Task<void> controlReadLoop() {
    class ControlVisitor : public MoQSession::ControlVisitor {
     public:
      explicit ControlVisitor(MoQFlvStreamerClient& client) : client_(client) {}

      void operator()(Announce announce) const override {
        XLOG(WARN) << "Announce ns=" << announce.trackNamespace;
        // text client doesn't expect server or relay to announce anything,
        // but announce OK anyways
        client_.moqClient_.moqSession_->announceOk({announce.trackNamespace});
      }

      void operator()(SubscribeRequest subscribeReq) const override {
        XLOG(INFO) << "SubscribeRequest";
        AbsoluteLocation latest_;
        // Track not available
        if (subscribeReq.fullTrackName == client_.fullVideoTrackName_) {
          latest_ = client_.latestVideo_;
        } else if (subscribeReq.fullTrackName == client_.fullAudioTrackName_) {
          latest_ = client_.latestAudio_;
        } else {
          client_.moqClient_.moqSession_->subscribeError(
              {subscribeReq.subscribeID, 404, "Full trackname NOT available"});
          return;
        }
        // Location mode not supported
        if (subscribeReq.locType != LocationType::LatestObject) {
          client_.moqClient_.moqSession_->subscribeError(
              {subscribeReq.subscribeID,
               403,
               "Only location LatestObject mode supported"});
          return;
        }
        // Save subscribe
        client_.subscriptions_[subscribeReq.subscribeID.value] = subscribeReq;
        XLOG(INFO) << "Subscribed " << subscribeReq.subscribeID;

        client_.moqClient_.moqSession_->subscribeOk(
            {subscribeReq.subscribeID,
             std::chrono::milliseconds(0),
             MoQSession::resolveGroupOrder(
                 GroupOrder::OldestFirst, subscribeReq.groupOrder),
             latest_,
             {}});
        return;
      }

      void operator()(SubscribeDone /* subscribeDone */) const override {
        // Not expecxted to receive this
        XLOG(INFO) << "SubscribeDone";
      }

      void operator()(Unsubscribe unSubs) const override {
        XLOG(INFO) << "Unsubscribe";
        // Delete subscribe
        client_.subscriptions_.erase(unSubs.subscribeID.value);

        XLOG(INFO) << "Unsubscribed " << unSubs.subscribeID;
      }

      virtual void operator()(Goaway) const override {
        XLOG(WARN) << "Goaway - NOT IMPLEMENTED";
      }

     private:
      MoQFlvStreamerClient& client_;
    };
    XLOG(INFO) << __func__;
    auto g =
        folly::makeGuard([func = __func__] { XLOG(INFO) << "exit " << func; });
    ControlVisitor visitor(*this);
    MoQSession::ControlVisitor* vptr(&visitor);
    while (auto msg =
               co_await moqClient_.moqSession_->controlMessages().next()) {
      boost::apply_visitor(*vptr, msg.value());
    }

    XLOG(INFO) << "Session closed";
  }

 private:
  static const uint64_t PRIORITY_AUDIO_OFFSET =
      std::numeric_limits<uint64_t>::max() / 2;

  static bool isAnyElementSent(const AbsoluteLocation& loc) {
    if (loc.group == 0 && loc.object == 0) {
      return false;
    }
    return true;
  }

  static uint64_t calculatePriotity(
      const AbsoluteLocation& latest,
      bool isAudio) {
    return latest.group + (isAudio ? PRIORITY_AUDIO_OFFSET : 0);
  }

  static ObjectHeader createObjectHeaderEndOfGroup(
      TrackIdentifier trackIdentifier,
      uint64_t group,
      uint64_t subgroup,
      uint64_t id,
      uint64_t priority,
      ForwardPreference forwardPreference) {
    return {
        trackIdentifier,
        group,
        subgroup,
        id,
        priority,
        forwardPreference,
        ObjectStatus::END_OF_GROUP};
  }

  static std::unique_ptr<folly::IOBuf> encodeToMoQMi(
      std::shared_ptr<FlvSequentialReader::MediaItem> item) {
    if (item->type == FlvSequentialReader::MediaType::VIDEO) {
      auto dataToEncode = std::make_unique<MoQMi::VideoH264AVCCWCPData>(
          item->id,
          item->pts,
          item->timescale,
          item->duration,
          item->wallclock,
          std::move(item->data),
          std::move(item->metadata),
          item->dts);

      return MoQMi::toObjectPayload(std::move(dataToEncode));

    } else if (item->type == FlvSequentialReader::MediaType::AUDIO) {
      auto dataToEncode = std::make_unique<MoQMi::AudioAACMP4LCWCPData>(
          item->id,
          item->pts,
          item->timescale,
          item->duration,
          item->wallclock,
          std::move(item->data),
          item->sampleFreq,
          item->numChannels);
      return MoQMi::toObjectPayload(std::move(dataToEncode));
    }
    return nullptr;
  }

  MoQClient moqClient_;
  folly::EventBase* evb_;
  TrackNamespace trackNamespace_;
  FullTrackName fullVideoTrackName_;
  FullTrackName fullAudioTrackName_;

  AbsoluteLocation latestVideo_{0, 0};
  AbsoluteLocation latestAudio_{0, 0};

  std::map<uint64_t, SubscribeRequest> subscriptions_;
};
} // namespace

using namespace moxygen;
namespace {

} // namespace

int main(int argc, char* argv[]) {
  folly::Init init(&argc, &argv, false);
  folly::EventBase eventBase;
  proxygen::URL url(FLAGS_connect_url);
  if (!url.hasHost() || !url.isValid()) {
    XLOGF(ERR, "Invalid url: {}", FLAGS_connect_url);
    return 1;
  }
  if (!std::filesystem::exists(FLAGS_input_flv_file)) {
    XLOGF(ERR, "The input file {} does not exists", FLAGS_input_flv_file);
    return 1;
  }

  TrackNamespace ns =
      TrackNamespace(FLAGS_track_namespace, FLAGS_track_namespace_delimiter);
  MoQFlvStreamerClient streamerClient(
      &eventBase,
      std::move(url),
      moxygen::FullTrackName({ns, FLAGS_video_track_name}),
      moxygen::FullTrackName({ns, FLAGS_audio_track_name}));

  class SigHandler : public folly::AsyncSignalHandler {
   public:
    explicit SigHandler(folly::EventBase* evb, std::function<void(int)> fn)
        : folly::AsyncSignalHandler(evb), fn_(std::move(fn)) {
      registerSignalHandler(SIGTERM);
      registerSignalHandler(SIGINT);
    }
    void signalReceived(int signum) noexcept override {
      XLOG(INFO) << __func__ << " signum=" << signum;
      fn_(signum);
      unreg();
    }

    void unreg() {
      unregisterSignalHandler(SIGTERM);
      unregisterSignalHandler(SIGINT);
    }

   private:
    std::function<void(int)> fn_;
  };

  // TODO this does NOT work, we do not get the signal
  SigHandler handler(
      &eventBase, [&streamerClient](int) mutable { streamerClient.stop(); });

  streamerClient.run({{std::move(ns)}, {}})
      .scheduleOn(&eventBase)
      .start()
      .via(&eventBase)
      .thenTry([&handler](auto) { handler.unreg(); });
  if (!eventBase.loop()) {
    return 1;
  }
  return 0;
}

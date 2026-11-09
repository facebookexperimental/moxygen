/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <fmt/core.h>
#include <folly/coro/BlockingWait.h>
#include <folly/coro/Task.h>
#include <folly/io/async/EventBase.h>
#include <folly/io/async/ScopedEventBaseThread.h>
#include <folly/portability/GMock.h>
#include <folly/portability/GTest.h>
#include <folly/portability/Sockets.h>
#include <array>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "moxygen/MoQVersions.h"
#include "moxygen/events/MoQFollyExecutorImpl.h"
#include "moxygen/test/MoQSwitchOrigin.h"
#include "moxygen/test/MoQTestLinks.h"

namespace moxygen::test { namespace {

using namespace testing;
using namespace std::chrono_literals;

const FullTrackName kTrack{TrackNamespace{{"live"}}, "video"};

TEST(ParseTrackNameTest, TheLastPartIsTheNameAndTheRestTheNamespace) {
  const FullTrackName nested{TrackNamespace{{"a", "b"}}, "c"};
  EXPECT_EQ(parseTrackName("live/video"), kTrack);
  EXPECT_EQ(parseTrackName("a/b/c"), nested);
  EXPECT_EQ(parseTrackName("video"), std::nullopt);
  EXPECT_EQ(parseTrackName("live//video"), std::nullopt);
  EXPECT_EQ(parseTrackName("live/"), std::nullopt);
  EXPECT_EQ(parseTrackName(""), std::nullopt);
}

TEST(ParseTrackNamesTest, EveryTrackOfTheListMustBeValidAndNew) {
  const std::vector<FullTrackName> both{
      kTrack, FullTrackName{TrackNamespace{{"live"}}, "audio"}};
  EXPECT_EQ(parseTrackNames("live/video,live/audio"), both);
  EXPECT_EQ(parseTrackNames("live/video, live/audio "), both);
  EXPECT_EQ(parseTrackNames("live/video,"), std::vector{kTrack});
  EXPECT_EQ(parseTrackNames("live/video,live/video"), std::nullopt);
  EXPECT_EQ(parseTrackNames("live/video,audio"), std::nullopt);
  EXPECT_EQ(parseTrackNames(""), std::nullopt);
}

TEST(ParseStatusCodeTest, OnlyCodesTheDraftDefinesAreValid) {
  EXPECT_EQ(
      parsePublishDoneStatusCode("2"), PublishDoneStatusCode::TRACK_ENDED);
  EXPECT_EQ(
      parsePublishDoneStatusCode("18"), PublishDoneStatusCode::MALFORMED_TRACK);
  EXPECT_EQ(parsePublishDoneStatusCode("7"), std::nullopt);
  EXPECT_EQ(parsePublishDoneStatusCode("ended"), std::nullopt);
  EXPECT_EQ(parseResetStreamErrorCode("1"), ResetStreamErrorCode::CANCELLED);
  EXPECT_EQ(
      parseResetStreamErrorCode("3"), ResetStreamErrorCode::SESSION_CLOSED);
  EXPECT_EQ(parseResetStreamErrorCode("8"), std::nullopt);
}

TEST(ParseDeliveryTest, NamesEachDelivery) {
  EXPECT_EQ(
      parseDelivery("subgroups"), SwitchTrackShape::Delivery::SubgroupStreams);
  EXPECT_EQ(
      parseDelivery("objects"), SwitchTrackShape::Delivery::ObjectStreams);
  EXPECT_EQ(parseDelivery("datagrams"), SwitchTrackShape::Delivery::Datagrams);
  EXPECT_EQ(parseDelivery("streams"), std::nullopt);
}

// Groups last 2s and hold 4 objects, one every 500ms: group 5 runs from 10s
// to 12s on the shared clock.
class SwitchClockPublisherTest : public Test {
 protected:
  std::unique_ptr<SwitchClockPublisher> makePublisher(
      std::shared_ptr<SwitchTestHub> hub,
      std::chrono::milliseconds offset = 0ms) {
    return std::make_unique<SwitchClockPublisher>(
        std::move(hub),
        kObjectsPerGroup,
        SwitchClockPublisher::Options{
            .tracks = {kTrack}, .groupDuration = 2s, .offset = offset});
  }

  static std::set<AbsoluteLocation> objects(
      uint64_t group,
      uint64_t endObject) {
    std::set<AbsoluteLocation> locations;
    for (uint64_t object = 0; object < endObject; object++) {
      locations.insert({group, object});
    }
    return locations;
  }

  static std::set<AbsoluteLocation> wholeGroups(uint64_t first, uint64_t end) {
    std::set<AbsoluteLocation> locations;
    for (auto group = first; group < end; group++) {
      locations.merge(objects(group, kObjectsPerGroup));
    }
    return locations;
  }

  static constexpr uint64_t kObjectsPerGroup{4};

  std::shared_ptr<SwitchOrigin> origin_{std::make_shared<SwitchOrigin>(
      SwitchTrackShape{.objectsPerGroup = kObjectsPerGroup})};
  std::shared_ptr<SwitchTestHub> hub_{
      std::make_shared<SwitchTestHub>(0, origin_)};
};

TEST_F(
    SwitchClockPublisherTest,
    ItStartsWithTheGroupNowFallsInAndPublishesEachObjectAsItFallsDue) {
  auto publisher = makePublisher(hub_);

  publisher->publishUntil(11'200ms);
  EXPECT_EQ(origin_->published(kTrack), objects(5, 3));
  publisher->publishUntil(11'499ms);
  EXPECT_EQ(origin_->published(kTrack), objects(5, 3));
  publisher->publishUntil(12'600ms);

  auto expected = wholeGroups(5, 6);
  expected.merge(objects(6, 2));
  EXPECT_EQ(origin_->published(kTrack), expected);
}

TEST_F(
    SwitchClockPublisherTest,
    OriginsStartedAtDifferentMomentsPublishTheSameGroups) {
  auto laterOrigin = std::make_shared<SwitchOrigin>(
      SwitchTrackShape{.objectsPerGroup = kObjectsPerGroup});
  auto early = makePublisher(hub_);
  auto later = makePublisher(std::make_shared<SwitchTestHub>(1, laterOrigin));

  early->publishUntil(8'100ms);
  early->publishUntil(12'600ms);
  later->publishUntil(11'800ms);
  later->publishUntil(12'600ms);

  auto fromGroup5 = wholeGroups(5, 6);
  fromGroup5.merge(objects(6, 2));
  EXPECT_EQ(laterOrigin->published(kTrack), fromGroup5);
  auto fromGroup4 = wholeGroups(4, 5);
  fromGroup4.merge(std::move(fromGroup5));
  EXPECT_EQ(origin_->published(kTrack), fromGroup4);
}

TEST_F(SwitchClockPublisherTest, AnOffsetShiftsTheClock) {
  auto ahead = makePublisher(hub_, 1s);

  ahead->publishUntil(10'250ms);

  EXPECT_EQ(origin_->published(kTrack), objects(5, 3));
}

TEST_F(SwitchClockPublisherTest, TimesBeforeTheEpochCountAsTheEpoch) {
  auto behind = makePublisher(hub_, -10s);

  behind->publishUntil(1'000ms);

  EXPECT_EQ(origin_->published(kTrack), objects(0, 1));
}

TEST_F(SwitchClockPublisherTest, AStalledOriginPublishesWhatFellDueOnResume) {
  auto publisher = makePublisher(hub_);
  publisher->publishUntil(10'000ms);
  publisher->stall();

  publisher->publishUntil(12'100ms);
  EXPECT_EQ(origin_->published(kTrack), objects(5, 1));
  publisher->resume();
  publisher->publishUntil(12'100ms);

  auto expected = wholeGroups(5, 6);
  expected.merge(objects(6, 1));
  EXPECT_EQ(origin_->published(kTrack), expected);
}

class GoawayRecordingViewer : public CheckingViewer {
 public:
  using CheckingViewer::CheckingViewer;

  void goaway(Goaway goaway) override {
    goaways.push_back(std::move(goaway));
  }

  std::vector<Goaway> goaways;
};

// The session GOAWAYs the client gets.
class SessionGoawayRecorder : public Subscriber {
 public:
  void goaway(Goaway goaway) override {
    goaways.push_back(std::move(goaway));
  }

  std::vector<Goaway> goaways;
};

// A viewer subscribed to an origin on draft 18, so GOAWAY timeouts go on the
// wire.
class SwitchOriginControlTest : public Test {
 protected:
  void SetUp() override {
    run([&]() -> folly::coro::Task<void> {
      link_ = co_await links_.connect(hub_);
      link_->client->setSubscribeHandler(sessionGoaways_);
      hub_->attach(link_->server);
      auto subscribed = co_await link_->client->subscribe(
          SubscribeRequest::make(kTrack), viewer_);
      EXPECT_TRUE(subscribed.hasValue());
      if (subscribed.hasValue()) {
        handle_ = std::move(subscribed.value());
      }
    });
  }

  void TearDown() override {
    links_.closeClients();
    drive();
  }

  void run(folly::Function<folly::coro::Task<void>()> body) {
    folly::coro::blockingWait(body(), &eventBase_);
    drive();
  }

  void drive() {
    for (int i = 0; i < 20; i++) {
      eventBase_.loopOnce(EVLOOP_NONBLOCK);
    }
  }

  std::string command(std::string_view line) {
    auto reply = control_.run(line);
    drive();
    return reply;
  }

  folly::EventBase eventBase_;
  std::shared_ptr<MoQFollyExecutorImpl> executor_{
      std::make_shared<MoQFollyExecutorImpl>(&eventBase_)};
  MoQTestLinks links_{kVersionDraft18, executor_};
  std::shared_ptr<SwitchOrigin> origin_{std::make_shared<SwitchOrigin>()};
  std::shared_ptr<SwitchTestHub> hub_{
      std::make_shared<SwitchTestHub>(0, origin_)};
  SwitchClockPublisher publisher_{
      hub_,
      origin_->shape().objectsPerGroup,
      SwitchClockPublisher::Options{.tracks = {kTrack}}};
  SwitchOriginControl control_{hub_, publisher_};
  std::shared_ptr<GoawayRecordingViewer> viewer_{
      std::make_shared<GoawayRecordingViewer>(origin_, kTrack)};
  std::shared_ptr<SessionGoawayRecorder> sessionGoaways_{
      std::make_shared<SessionGoawayRecorder>()};
  MoQTestLink* link_{nullptr};
  std::shared_ptr<Publisher::SubscriptionHandle> handle_;
};

TEST_F(SwitchOriginControlTest, GoawayReachesTheOriginsSessions) {
  EXPECT_EQ(command("goaway 5000 https://next/moq"), "ok");

  ASSERT_EQ(sessionGoaways_->goaways.size(), 1);
  EXPECT_EQ(sessionGoaways_->goaways[0].timeout, 5000);
  EXPECT_EQ(sessionGoaways_->goaways[0].newSessionUri, "https://next/moq");
}

TEST_F(SwitchOriginControlTest, ASecondGoawayIsAnError) {
  EXPECT_EQ(command("goaway 5000"), "ok");

  EXPECT_THAT(command("goaway 5000"), StartsWith("error: no session took it"));
}

TEST_F(SwitchOriginControlTest, RequestGoawayReachesTheTracksSubscriptions) {
  EXPECT_EQ(command("request-goaway live/video 3000"), "ok");

  ASSERT_EQ(viewer_->goaways.size(), 1);
  EXPECT_EQ(viewer_->goaways[0].timeout, 3000);
  EXPECT_THAT(sessionGoaways_->goaways, IsEmpty());
  EXPECT_THAT(
      command("request-goaway live/video"),
      StartsWith("error: no subscription took it"));
}

// Mid-group, so the group's subgroup stream is open.
TEST_F(SwitchOriginControlTest, PublishDoneResetsOpenStreamsAndEndsTheTrack) {
  publisher_.publishUntil(10'600ms);
  viewer_->expectedEnd = PublishDoneStatusCode::TRACK_ENDED;
  viewer_->expectedReset = ResetStreamErrorCode::CANCELLED;
  EXPECT_EQ(command("subscriptions live/video"), "1");

  EXPECT_EQ(command("publish-done live/video 2"), "ok");

  ASSERT_TRUE(viewer_->done().has_value());
  EXPECT_EQ(viewer_->done()->statusCode, PublishDoneStatusCode::TRACK_ENDED);
  EXPECT_THAT(
      viewer_->resets(),
      ElementsAre(
          CheckingViewer::Reset{5, 0, ResetStreamErrorCode::CANCELLED}));
  EXPECT_EQ(command("subscriptions live/video"), "0");
}

TEST_F(SwitchOriginControlTest, PublishDoneResetsWithTheCodeGiven) {
  publisher_.publishUntil(10'600ms);
  drive();
  viewer_->expectedEnd = PublishDoneStatusCode::GOING_AWAY;
  viewer_->expectedReset = ResetStreamErrorCode::GOING_AWAY;

  EXPECT_EQ(command("publish-done live/video 4 4"), "ok");

  EXPECT_THAT(
      viewer_->resets(),
      ElementsAre(
          CheckingViewer::Reset{5, 0, ResetStreamErrorCode::GOING_AWAY}));
  EXPECT_THAT(viewer_->violations(), IsEmpty());
}

TEST_F(SwitchOriginControlTest, FinishEndsTheTrackOnceOpenStreamsFinish) {
  publisher_.publishUntil(10'600ms);
  viewer_->expectedEnd = PublishDoneStatusCode::TRACK_ENDED;

  EXPECT_EQ(command("finish live/video 2"), "ok");
  publisher_.publishUntil(11'999ms);
  drive();

  ASSERT_TRUE(viewer_->done().has_value());
  EXPECT_EQ(viewer_->done()->statusCode, PublishDoneStatusCode::TRACK_ENDED);
  EXPECT_THAT(viewer_->resets(), IsEmpty());
  EXPECT_THAT(viewer_->violations(), IsEmpty());
  EXPECT_THAT(viewer_->missing(), IsEmpty());
}

TEST_F(SwitchOriginControlTest, CommandsForATrackNobodyWatchesAreErrors) {
  for (const auto* line :
       {"request-goaway live/audio",
        "publish-done live/audio 2",
        "finish live/audio 2"}) {
    EXPECT_EQ(command(line), "error: no subscriptions to live/audio") << line;
  }
  EXPECT_THAT(viewer_->goaways, IsEmpty());
  EXPECT_FALSE(viewer_->done().has_value());
}

TEST_F(SwitchOriginControlTest, CloseClosesTheOriginsSessions) {
  EXPECT_EQ(command("close"), "ok");

  EXPECT_TRUE(link_->client->isClosed());
  EXPECT_EQ(command("close"), "error: no sessions to close");
  EXPECT_THAT(command("goaway"), StartsWith("error: no session took it"));
}

TEST_F(SwitchOriginControlTest, StallAndResumeHoldBackAndReleasePublishing) {
  publisher_.publishUntil(10'000ms);
  const auto before = origin_->published(kTrack);

  EXPECT_EQ(command("stall"), "ok");
  publisher_.publishUntil(12'000ms);
  EXPECT_EQ(origin_->published(kTrack), before);
  EXPECT_EQ(command("resume"), "ok");
  publisher_.publishUntil(12'000ms);

  EXPECT_GT(origin_->published(kTrack).size(), before.size());
}

TEST_F(SwitchOriginControlTest, MalformedCommandsAreErrors) {
  for (const auto* line :
       {"",
        "nope",
        "goaway soon",
        "goaway 1 uri extra",
        "request-goaway",
        "request-goaway video",
        "publish-done live/video",
        "publish-done live/video ended",
        "publish-done live/video 7",
        "publish-done live/video 2 8",
        "publish-done live/video 2 1 extra",
        "finish live/video",
        "finish live/video 2 1",
        "close now",
        "subscriptions"}) {
    EXPECT_THAT(command(line), StartsWith("error: ")) << line;
  }
  EXPECT_THAT(sessionGoaways_->goaways, IsEmpty());
  EXPECT_FALSE(link_->client->isClosed());
  EXPECT_FALSE(viewer_->done().has_value());
}

// The control server on loopback, and a client that writes raw bytes and
// reads reply lines.
class SwitchOriginControlServerTest : public Test {
 protected:
  void SetUp() override {
    evb_.getEventBase()->runInEventBaseThreadAndWait([&] {
      server_ = std::make_unique<SwitchOriginControlServer>(
          evb_.getEventBase(), control_);
      server_->start(folly::SocketAddress("::1", 0));
    });
    client_ = connect();
  }

  void TearDown() override {
    folly::netops::close(client_);
    stopServer();
  }

  folly::NetworkSocket connect() {
    auto client = folly::netops::socket(AF_INET6, SOCK_STREAM, 0);
    EXPECT_NE(client, folly::NetworkSocket());
    timeval timeout{.tv_sec = 5, .tv_usec = 0};
    folly::netops::setsockopt(
        client, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
    // Small, so replies the client hasn't read back up into the server.
    const int receiveBuffer = 4096;
    folly::netops::setsockopt(
        client, SOL_SOCKET, SO_RCVBUF, &receiveBuffer, sizeof(receiveBuffer));
    sockaddr_storage address{};
    const auto length = server_->address().getAddress(&address);
    EXPECT_EQ(
        folly::netops::connect(
            client, reinterpret_cast<sockaddr*>(&address), length),
        0);
    return client;
  }

  void stopServer() {
    evb_.getEventBase()->runInEventBaseThreadAndWait([&] { server_.reset(); });
  }

  static void send(folly::NetworkSocket client, std::string_view bytes) {
    while (!bytes.empty()) {
      const auto sent =
          folly::netops::send(client, bytes.data(), bytes.size(), 0);
      ASSERT_GT(sent, 0);
      bytes.remove_prefix(sent);
    }
  }

  void send(std::string_view bytes) {
    send(client_, bytes);
  }

  // The next count reply lines, fewer if the server stops answering.
  std::vector<std::string> replies(size_t count) {
    std::vector<std::string> lines;
    std::array<char, 65536> buffer{};
    while (lines.size() < count) {
      const auto received =
          folly::netops::recv(client_, buffer.data(), buffer.size(), 0);
      if (received <= 0) {
        break;
      }
      pending_.append(buffer.data(), received);
      for (auto end = pending_.find('\n'); end != std::string::npos;
           end = pending_.find('\n')) {
        lines.push_back(pending_.substr(0, end));
        pending_.erase(0, end + 1);
      }
    }
    return lines;
  }

  folly::ScopedEventBaseThread evb_;
  std::shared_ptr<SwitchTestHub> hub_{
      std::make_shared<SwitchTestHub>(0, std::make_shared<SwitchOrigin>())};
  SwitchClockPublisher publisher_{
      hub_,
      SwitchTrackShape{}.objectsPerGroup,
      SwitchClockPublisher::Options{.tracks = {kTrack}}};
  SwitchOriginControl control_{hub_, publisher_};
  std::unique_ptr<SwitchOriginControlServer> server_;
  folly::NetworkSocket client_;
  std::string pending_;
};

TEST_F(SwitchOriginControlServerTest, AnswersEachLineInOrderHoweverItArrives) {
  send("stall\nsubscri");
  send("ptions live/video\r\nresume\n");

  EXPECT_THAT(replies(3), ElementsAre("ok", "0", "ok"));
}

// One line arrives whole, the other over many reads.
TEST_F(SwitchOriginControlServerTest, ALineTooLongIsAnErrorAndDropped) {
  const std::string tooLong(SwitchOriginControlServer::kMaxLineLength + 1, 'x');
  send(tooLong + "\nstall\n");
  EXPECT_THAT(replies(2), ElementsAre("error: line too long", "ok"));

  send(tooLong + tooLong);
  send("x\nresume\n");
  EXPECT_THAT(replies(2), ElementsAre("error: line too long", "ok"));
}

TEST_F(SwitchOriginControlServerTest, StoppingWithAClientConnectedClosesIt) {
  send("stall\n");
  EXPECT_THAT(replies(1), ElementsAre("ok"));

  stopServer();

  std::array<char, 16> buffer{};
  EXPECT_EQ(folly::netops::recv(client_, buffer.data(), buffer.size(), 0), 0);
}

// Each client resets its connection with replies unread, so some replies fail
// while the server is still answering the client's lines.
TEST_F(SwitchOriginControlServerTest, ClientsThatResetLeaveTheServerServing) {
  for (int i = 0; i < 20; i++) {
    auto client = connect();
    send(client, "stall\nresume\nsubscriptions live/video\nstall\n");
    linger reset{.l_onoff = 1, .l_linger = 0};
    folly::netops::setsockopt(
        client, SOL_SOCKET, SO_LINGER, &reset, sizeof(reset));
    folly::netops::close(client);
  }

  send("resume\n");

  EXPECT_THAT(replies(1), ElementsAre("ok"));
}

// Megabytes of replies queue in the server while the client isn't reading;
// each one still arrives whole.
TEST_F(SwitchOriginControlServerTest, RepliesSurviveAClientThatIsNotReading) {
  constexpr size_t kCommands = 4000;
  std::vector<std::string> expected;
  std::string commands;
  for (size_t i = 0; i < kCommands; i++) {
    auto command = fmt::format("nope{}-{}", i, std::string(3000, 'x'));
    expected.push_back("error: unknown command: " + command);
    commands += command + "\n";
  }

  send(commands);

  EXPECT_EQ(replies(kCommands), expected);
}

}} // namespace moxygen::test

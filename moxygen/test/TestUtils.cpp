#include "moxygen/test/TestUtils.h"

#include "moxygen/MoQFramer.h"

namespace moxygen::test {

std::unique_ptr<folly::IOBuf> writeAllMessages() {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  auto res = writeClientSetup(
      writeBuf,
      ClientSetup(
          {{1},
           {
               {folly::to_underlying(SetupKey::ROLE),
                "",
                folly::to_underlying(Role::CLIENT)},
               {folly::to_underlying(SetupKey::PATH), "/foo", 0},
           }}));
  res = writeServerSetup(
      writeBuf,
      ServerSetup(
          {1,
           {
               {folly::to_underlying(SetupKey::ROLE),
                "",
                folly::to_underlying(Role::CLIENT)},
               {folly::to_underlying(SetupKey::PATH), "/foo", 0},
           }}));
  res = writeSubscribeRequest(
      writeBuf,
      SubscribeRequest(
          {0,
           0,
           FullTrackName({"hello", "world"}),
           Location({LocationType::RelativePrevious, 0}),
           Location({LocationType::Absolute, 0}),
           Location({LocationType::RelativeNext, 0}),
           Location({LocationType::None, 0}),
           {{folly::to_underlying(TrackRequestParamKey::AUTHORIZATION),
             "binky"}}}));
  res = writeSubscribeOk(
      writeBuf, SubscribeOk({0, std::chrono::milliseconds(0)}));
  res = writeSubscribeError(
      writeBuf, SubscribeError({0, 404, "not found", folly::none}));
  res = writeUnsubscribe(
      writeBuf,
      Unsubscribe({
          0,
      }));
  res = writeSubscribeFin(writeBuf, SubscribeFin({0, 0, 0}));
  res = writeSubscribeRst(writeBuf, SubscribeRst({0, 404, "not found", 0, 0}));
  res = writeAnnounce(
      writeBuf,
      Announce(
          {"hello",
           {{folly::to_underlying(TrackRequestParamKey::AUTHORIZATION),
             "binky"}}}));
  res = writeAnnounceOk(writeBuf, AnnounceOk({"hello"}));
  res = writeAnnounceError(
      writeBuf, AnnounceError({"hello", 500, "server error"}));
  res = writeUnannounce(
      writeBuf,
      Unannounce({
          "hello",
      }));
  res = writeGoaway(writeBuf, Goaway({"new uri"}));

  res = writeStreamHeader(
      writeBuf,
      ObjectHeader({
          0,
          1,
          2,
          3,
          4,
          ForwardPreference::Track,
          folly::none,
      }));
  res = writeObject(
      writeBuf,
      ObjectHeader({0, 1, 2, 3, 4, ForwardPreference::Track, 0}),
      nullptr);

  return writeBuf.move();
}

} // namespace moxygen::test

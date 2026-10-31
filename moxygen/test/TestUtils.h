/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/io/IOBuf.h>
#include <moxygen/MoQFramer.h>

#include <deque>
#include <string>
#include <vector>

namespace moxygen::test {

enum class TestControlMessages { CLIENT, SERVER, BOTH };
std::unique_ptr<folly::IOBuf> writeAllControlMessages(
    TestControlMessages in,
    const MoQFrameWriter& moqFrameWriter,
    uint64_t version);
// A parsed frame and the request ID its callback carries. For REQUEST_UPDATE
// that is the update's own ID, and the stream's ID is the existing request.
struct ExpectedFrame {
  FrameType type;
  RequestID requestID;
};

// One draft 18+ request stream, carrying the same messages that
// writeAllControlMessages writes onto the draft 16 control stream.
struct TestRequestExchange {
  std::string name;
  RequestID requestID;
  // The requester's codec admits okType first, then postTerminal.
  FrameType okType;
  std::vector<FrameType> postTerminal;
  std::deque<RequestID> pendingUpdateIDs;
  std::vector<FrameType> responderAllowedFrames;
  std::unique_ptr<folly::IOBuf> toResponder;
  std::vector<ExpectedFrame> atResponder;
  std::unique_ptr<folly::IOBuf> toRequester;
  std::vector<ExpectedFrame> atRequester;
};

// Draft 18+ only. Excludes frames that draft 18 replaced with stream resets.
std::vector<TestRequestExchange> writeAllRequestStreamMessages(
    const MoQFrameWriter& moqFrameWriter,
    uint64_t version);

// Draft 18+ control stream contents for one direction: SETUP, then GOAWAY.
std::unique_ptr<folly::IOBuf> writeControlStreamMessages(
    TestControlMessages in,
    const MoQFrameWriter& moqFrameWriter,
    uint64_t version);

std::unique_ptr<folly::IOBuf> writeAllObjectMessages(
    const MoQFrameWriter& moqFrameWriter);
std::unique_ptr<folly::IOBuf> writeAllFetchMessages(
    const MoQFrameWriter& moqFrameWriter);

inline std::unique_ptr<folly::IOBuf> writeAllMessages(
    const MoQFrameWriter& moqFrameWriter,
    uint64_t version) {
  auto buf = writeAllControlMessages(
      TestControlMessages::BOTH, moqFrameWriter, version);
  buf->appendToChain(writeAllObjectMessages(moqFrameWriter));
  buf->appendToChain(writeAllFetchMessages(moqFrameWriter));
  return buf;
}

// Writes a subgroup stream carrying a single object.  Only tests build these,
// so the derivation of the header options lives here rather than on
// MoQFrameWriter.
WriteResult writeSingleObjectStream(
    const MoQFrameWriter& moqFrameWriter,
    folly::IOBufQueue& writeBuf,
    TrackAlias trackAlias,
    const ObjectHeader& objectHeader,
    std::unique_ptr<folly::IOBuf> objectPayload,
    bool endOfGroup = false);

std::unique_ptr<folly::IOBuf> makeBuf(uint32_t size = 10);

std::vector<Extension> getTestExtensions();
std::vector<Parameter> getTestTrackRequestParams(
    const MoQFrameWriter& moqFrameWriter);

} // namespace moxygen::test

/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/io/IOBuf.h>
#include <moxygen/MoQFramer.h>

namespace moxygen::test {

enum class TestControlMessages { CLIENT, SERVER, BOTH };
std::unique_ptr<folly::IOBuf> writeAllControlMessages(
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

std::unique_ptr<folly::IOBuf> makeBuf(uint32_t size = 10);

std::vector<Extension> getTestExtensions();
std::vector<Parameter> getTestTrackRequestParams(
    const MoQFrameWriter& moqFrameWriter);
std::vector<Parameter> getTestPublisherTrackRequestParams(
    const MoQFrameWriter& moqFrameWriter);

} // namespace moxygen::test

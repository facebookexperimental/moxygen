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
std::unique_ptr<folly::IOBuf> writeAllControlMessages(TestControlMessages in);
std::unique_ptr<folly::IOBuf> writeAllObjectMessages();
std::unique_ptr<folly::IOBuf> writeAllFetchMessages();
std::unique_ptr<folly::IOBuf> writeAllDatagramMessages();

inline std::unique_ptr<folly::IOBuf> writeAllMessages() {
  auto buf = writeAllControlMessages(TestControlMessages::BOTH);
  buf->appendToChain(writeAllObjectMessages());
  buf->appendToChain(writeAllFetchMessages());
  buf->appendToChain(writeAllDatagramMessages());
  return buf;
}

std::unique_ptr<folly::IOBuf> makeBuf(uint32_t size = 10);

std::vector<Extension> getTestExtensions();

} // namespace moxygen::test

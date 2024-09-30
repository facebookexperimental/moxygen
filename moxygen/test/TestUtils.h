/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/io/IOBuf.h>

namespace moxygen::test {

std::unique_ptr<folly::IOBuf> writeAllControlMessages();
std::unique_ptr<folly::IOBuf> writeAllObjectMessages();

inline std::unique_ptr<folly::IOBuf> writeAllMessages() {
  auto buf = writeAllControlMessages();
  buf->appendToChain(writeAllObjectMessages());
  return buf;
}

} // namespace moxygen::test

/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/MoQCodec.h"
#include "moxygen/test/TestUtils.h"

#include <folly/portability/GTest.h>

using namespace moxygen;

TEST(MoQCodec, All) {
  auto allMsgs = moxygen::test::writeAllMessages();
  MoQCodec codec(MoQCodec::Direction::CLIENT, nullptr);

  codec.onIngress(std::move(allMsgs), true);
}

TEST(MoQCodec, EmptyObjectPayload) {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  writeObject(
      writeBuf,
      {0, 1, 2, 3, 4, ForwardPreference::Object, folly::none},
      nullptr);
  MoQCodec codec(MoQCodec::Direction::CLIENT, nullptr);
  codec.onIngress(writeBuf.move(), false);
  codec.onIngress(std::unique_ptr<folly::IOBuf>(), true);
}

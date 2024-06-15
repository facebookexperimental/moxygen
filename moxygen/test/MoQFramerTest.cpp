/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/MoQFramer.h"
#include <folly/portability/GTest.h>
#include "moxygen/test/TestUtils.h"

using namespace moxygen;

TEST(SerializeAndParse, All) {
  auto allMsgs = moxygen::test::writeAllMessages();
  folly::io::Cursor cursor(allMsgs.get());
  cursor.skip(2);
  EXPECT_TRUE(parseClientSetup(cursor));
  cursor.skip(2);
  EXPECT_TRUE(parseServerSetup(cursor));
  cursor.skip(1);
  EXPECT_TRUE(parseSubscribeRequest(cursor));
  cursor.skip(1);
  EXPECT_TRUE(parseSubscribeOk(cursor));
  cursor.skip(1);
  EXPECT_TRUE(parseSubscribeError(cursor));
  cursor.skip(1);
  EXPECT_TRUE(parseUnsubscribe(cursor));
  cursor.skip(1);
  EXPECT_TRUE(parseSubscribeDone(cursor));
  cursor.skip(1);
  EXPECT_TRUE(parseSubscribeDone(cursor));
  cursor.skip(1);
  EXPECT_TRUE(parseAnnounce(cursor));
  cursor.skip(1);
  EXPECT_TRUE(parseAnnounceOk(cursor));
  cursor.skip(1);
  EXPECT_TRUE(parseAnnounceError(cursor));
  cursor.skip(1);
  EXPECT_TRUE(parseAnnounceCancel(cursor));
  cursor.skip(1);
  EXPECT_TRUE(parseUnannounce(cursor));
  cursor.skip(1);
  EXPECT_TRUE(parseGoaway(cursor));
  auto res = parseStreamHeader(cursor, FrameType::STREAM_HEADER_TRACK);
  EXPECT_TRUE(res);
  // cursor.skip(1);
  EXPECT_TRUE(parseMultiObjectHeader(
      cursor, FrameType::STREAM_HEADER_TRACK, res.value()));
  cursor.skip(1);
}

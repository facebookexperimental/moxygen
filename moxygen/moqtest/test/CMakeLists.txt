# Copyright (c) Meta Platforms, Inc. and affiliates.
# This source code is licensed under the Apache 2.0 license found in the
# LICENSE file in the root directory of this source tree.

if(NOT BUILD_TESTS)
    return()
endif()

moxygen_add_test(TARGET MoQTrackServerTests
  PREFIX "MoQTrackServer"
  SOURCES
    MoQTrackServerTest.cpp
  DEPENDS
    moxygen_moq_test_server
    moqtestutils
    testmain
    Folly::folly_coro_gtest_helpers
)

# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

if(NOT BUILD_TESTS)
    return()
endif()

moxygen_add_test(TARGET MoQCacheTests
  PREFIX "MoQCache"
  SOURCES
    MoQCacheTests.cpp
  DEPENDS
    moxygen_relay_moq_cache
    moqtestutils
    testmain
    Folly::folly_coro_gtest_helpers
    Folly::folly_io_async_scoped_event_base_thread
)

moxygen_add_test(TARGET MoQRelayTest
  PREFIX "MoQRelay"
  SOURCES
    MoQRelayTest.cpp
  DEPENDS
    moxygen_relay_moq_relay
    moqtestutils
    moxygen_events_moq_folly_executor_impl
    testmain
)

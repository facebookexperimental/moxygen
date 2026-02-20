# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

if(NOT BUILD_TESTS)
    return()
endif()

add_library(testmain TestMain.cpp)
target_include_directories(
    testmain PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
)
target_link_libraries(testmain PUBLIC
    Folly::folly_init_init
    GTest::gmock
    GTest::gtest
)
target_compile_options(
    testmain PRIVATE
    ${_MOXYGEN_COMMON_COMPILE_OPTIONS}
)

add_library(moqtestutils TestUtils.cpp)
target_include_directories(
    moqtestutils PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
    $<BUILD_INTERFACE:${MOXYGEN_FBCODE_ROOT}>
)
target_compile_options(
    moqtestutils PRIVATE
    ${_MOXYGEN_COMMON_COMPILE_OPTIONS}
)
target_link_libraries(
    moqtestutils PUBLIC
    moxygen_moq
    moxygen_moq_codec
    moxygen_moq_consumers
    moxygen_moq_framer
    moxygen_events_moq_folly_executor_impl
    GTest::gmock
)

add_library(moqsessiontestcommon MoQSessionTestCommon.cpp)
target_include_directories(
    moqsessiontestcommon PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
    $<BUILD_INTERFACE:${MOXYGEN_FBCODE_ROOT}>
)
target_compile_options(
    moqsessiontestcommon PRIVATE
    ${_MOXYGEN_COMMON_COMPILE_OPTIONS}
)
target_link_libraries(
    moqsessiontestcommon PUBLIC
    moqtestutils
    moxygen_moq_relay_session
    moxygen_events_moq_folly_executor_impl
    moxygen_moqclient
    moxygen_relay_moq_relay_client
    Folly::folly_coro_gtest_helpers
    GTest::gmock
    GTest::gtest
)

moxygen_add_test(TARGET MoQTests
  PREFIX "MoQ"
  SOURCES
    MoQFramerTest.cpp
    MoQCodecTest.cpp
  DEPENDS
    moqtestutils
    moxygen_moq_framer
    moxygen_moq_codec
    testmain
)

moxygen_add_test(TARGET MoQLocationTests
  PREFIX "MoQLocation"
  SOURCES
    MoQLocationTest.cpp
  DEPENDS
    moxygen_moq_location
    testmain
)

moxygen_add_test(TARGET MoQSessionTests
  PREFIX "MoQSession"
  SOURCES
    MoQSessionFetchTests.cpp
    MoQSessionObjectDeliveryTests.cpp
    MoQSessionPublishNamespaceTests.cpp
    MoQSessionPublishTests.cpp
    MoQSessionSubscribeNamespaceTests.cpp
    MoQSessionSubscribeTests.cpp
    MoQSessionTests.cpp
    MoQSessionTrackStatusTests.cpp
  DEPENDS
    moqsessiontestcommon
    testmain
)

moxygen_add_test(TARGET MoQDeliveryTimerTests
  PREFIX "MoQDeliveryTimer"
  SOURCES
    MoQDeliveryTimerTest.cpp
  DEPENDS
    moqtestutils
    moxygen_events_moq_delivery_timer
    moxygen_events_moq_folly_executor_impl
    testmain
)

moxygen_add_test(TARGET MoQDeliveryTimeoutManagerTests
  PREFIX "MoQDeliveryTimeoutManager"
  SOURCES
    MoQDeliveryTimeoutManagerTest.cpp
  DEPENDS
    moxygen_events_moq_delivery_timeout_manager
    testmain
)

moxygen_add_test(TARGET MoQTokenCacheTests
  PREFIX "MoQTokenCache"
  SOURCES
    MoQTokenCacheTest.cpp
  DEPENDS
    moxygen_moq_token_cache
    testmain
)

moxygen_add_test(TARGET ObjectReceiverTests
  PREFIX "ObjectReceiver"
  SOURCES
    ObjectReceiverTest.cpp
  DEPENDS
    moxygen_moq_obj_receiver
    testmain
)

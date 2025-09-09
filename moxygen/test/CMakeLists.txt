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
    ${LIBGMOCK_INCLUDE_DIR}
    ${LIBGTEST_INCLUDE_DIR}
)
target_link_libraries(testmain PUBLIC Folly::folly)
target_compile_options(
    testmain PRIVATE
    ${_MOXYGEN_COMMON_COMPILE_OPTIONS}
)

add_library(moqtestutils TestUtils.cpp)
target_include_directories(
    moqtestutils PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
    ${LIBGMOCK_INCLUDE_DIR}
    ${LIBGTEST_INCLUDE_DIR}
)
target_compile_options(
    moqtestutils PRIVATE
    ${_MOXYGEN_COMMON_COMPILE_OPTIONS}
)
target_link_libraries(moqtestutils PUBLIC moxygen)


moxygen_add_test(TARGET MoQTests
  PREFIX "MoQ"
  SOURCES
    MoQFramerTest.cpp
    MoQCodecTest.cpp
  DEPENDS
    moqtestutils
    testmain
)

moxygen_add_test(TARGET MoQSessionTests
  PREFIX "MoQSession"
  SOURCES
    MoQSessionTest.cpp
  DEPENDS
    moqtestutils
    moqfollyexecutorimpl
    moxygenclient
    testmain
)

# Copyright (c) Meta Platforms, Inc. and affiliates.
# This source code is licensed under the Apache 2.0 license found in the
# LICENSE file in the root directory of this source tree.

if(NOT BUILD_TESTS)
    return()
endif()

moxygen_add_test(TARGET DeJitterTests
  PREFIX "DeJitter"
  SOURCES
    DeJitterTest.cpp
  DEPENDS
    moxygen_dejitter
    testmain
)

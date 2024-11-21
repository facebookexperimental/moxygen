# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

if(NOT BUILD_TESTS)
    return()
endif()

moqmi_add_test(TARGET MoQMiTests
  SOURCES
    MoQMiTest.cpp
  DEPENDS
    moqmi
    Folly
)

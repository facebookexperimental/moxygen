# Copyright (c) Meta Platforms, Inc. and affiliates.
# This source code is licensed under the Apache 2.0 license found in the
# LICENSE file in the root directory of this source tree.

if(NOT BUILD_TESTS)
    return()
endif()

moxygen_add_test(TARGET MoQMiTests
  PREFIX "MoQMi"
  SOURCES
    MoQMiTest.cpp
  DEPENDS
    moxygen_moq_mi
    moxygen_flv_parser_flv_reader
    proxygen::proxygen_utils_logging
    testmain
)

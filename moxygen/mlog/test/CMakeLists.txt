# Copyright (c) Meta Platforms, Inc. and affiliates.
# This source code is licensed under the Apache 2.0 license found in the
# LICENSE file in the root directory of this source tree.

if(NOT BUILD_TESTS)
    return()
endif()

moxygen_add_test(TARGET MLoggerTests
  PREFIX "MLogger"
  SOURCES
    FileMLoggerTest.cpp
    FileMLoggerFactoryTest.cpp
    SamplingMLoggerFactoryTest.cpp
  DEPENDS
    moxygen::moxygen_mlog_file_mlogger
    moxygen::moxygen_mlog_file_mlogger_factory
    moxygen::moxygen_mlog_sampling_mlogger_factory
    Folly::folly_executors_manual_executor
    mvfst::mvfst_codec_types
    GTest::gtest_main
)

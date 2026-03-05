# Copyright (c) Meta Platforms, Inc. and affiliates.
# This source code is licensed under the Apache 2.0 license found in the
# LICENSE file in the root directory of this source tree.

if(NOT BUILD_TESTS)
    return()
endif()

moxygen_add_test(TARGET FlvReaderTests
  PREFIX "FlvReader"
  SOURCES
    FlvReaderTest.cpp
  DEPENDS
    moxygen_flv_parser_flv_reader
    testmain
)

moxygen_add_test(TARGET FlvSequentialReaderTests
  PREFIX "FlvSequentialReader"
  SOURCES
    FlvSequentialReaderTest.cpp
  DEPENDS
    moxygen_flv_parser_flv_reader
    moxygen_flv_parser_flv_stream_parser
    testmain
)

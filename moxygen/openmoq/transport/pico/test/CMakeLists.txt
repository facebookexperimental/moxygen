# Copyright (c) Meta Platforms, Inc. and affiliates.
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

if(NOT BUILD_TESTS)
    return()
endif()

moxygen_add_test(TARGET PicoH3WebTransportTests
  PREFIX "PicoH3WebTransport"
  SOURCES
    PicoH3WebTransportTest.cpp
  DEPENDS
    openmoq_pico_server_base
    Folly::folly_io_iobuf
)

moxygen_add_test(TARGET PicoWebTransportConfigTests
  PREFIX "PicoWebTransportConfig"
  SOURCES
    PicoWebTransportConfigTest.cpp
  DEPENDS
    openmoq_pico_server_base
)

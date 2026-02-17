# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# FLV Streamer Client
add_executable(
  moqflvstreamerclient
  MoQFlvStreamerClient.cpp
)
set_target_properties(
  moqflvstreamerclient
  PROPERTIES
    BUILD_RPATH ${DEPS_LIBRARIES_DIR}
    INSTALL_RPATH ${DEPS_LIBRARIES_DIR}
)
target_include_directories(
  moqflvstreamerclient PUBLIC $<BUILD_INTERFACE:${MOXYGEN_FBCODE_ROOT}>
)
target_compile_options(
  moqflvstreamerclient PRIVATE
  ${_MOXYGEN_COMMON_COMPILE_OPTIONS}
)
target_link_libraries(
  moqflvstreamerclient PUBLIC
  moxygen_flv_parser_flv_reader
  moxygen_moq_mi
  moxygen_moq_webtransport_client
  moxygen_relay_moq_relay_client
  Folly::folly_init_init
  Folly::folly_io_async_async_signal_handler
)

install(
    TARGETS moqflvstreamerclient
    EXPORT moxygen-exports
    ARCHIVE DESTINATION ${LIB_INSTALL_DIR}
    LIBRARY DESTINATION ${LIB_INSTALL_DIR}
)

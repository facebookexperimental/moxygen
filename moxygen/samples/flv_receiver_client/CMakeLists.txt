# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# FLV Receiver Client
add_executable(
  moqflvreceiverclient
  MoQFlvReceiverClient.cpp
)
set_target_properties(
  moqflvreceiverclient
  PROPERTIES
    BUILD_RPATH ${DEPS_LIBRARIES_DIR}
    INSTALL_RPATH ${DEPS_LIBRARIES_DIR}
)
target_include_directories(
  moqflvreceiverclient PUBLIC $<BUILD_INTERFACE:${MOXYGEN_FBCODE_ROOT}>
)
target_compile_options(
  moqflvreceiverclient PRIVATE
  ${_MOXYGEN_COMMON_COMPILE_OPTIONS}
)
target_link_libraries(
  moqflvreceiverclient PUBLIC
  moxygen_dejitter
  moxygen_flv_parser_flv_writer
  moxygen_moq_mi
  moxygen_moq_mi_to_flv
  moxygen_moq_webtransport_client
  moxygen_moqclient
  Folly::folly_init_init
  Folly::folly_io_async_async_signal_handler
)

install(
    TARGETS moqflvreceiverclient
    EXPORT moxygen-exports
    ARCHIVE DESTINATION ${LIB_INSTALL_DIR}
    LIBRARY DESTINATION ${LIB_INSTALL_DIR}
)

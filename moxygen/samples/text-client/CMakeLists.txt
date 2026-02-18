# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# Text Client
add_executable(
  moqtextclient
  MoQTextClient.cpp
)
set_target_properties(
  moqtextclient
  PROPERTIES
    BUILD_RPATH ${DEPS_LIBRARIES_DIR}
    INSTALL_RPATH ${DEPS_LIBRARIES_DIR}
)
target_include_directories(
  moqtextclient PUBLIC $<BUILD_INTERFACE:${MOXYGEN_FBCODE_ROOT}>
)
target_compile_options(
  moqtextclient PRIVATE
  ${_MOXYGEN_COMMON_COMPILE_OPTIONS}
)
target_link_libraries(
  moqtextclient PUBLIC
  moxygen_mlog_file_mlogger
  moxygen_mlog_mlogger
  moxygen_moq_webtransport_client
  moxygen_relay_moq_relay_client
  Folly::folly_init_init
  Folly::folly_io_async_async_signal_handler
)

install(
    TARGETS moqtextclient
    EXPORT moxygen-exports
    ARCHIVE DESTINATION ${LIB_INSTALL_DIR}
    LIBRARY DESTINATION ${LIB_INSTALL_DIR}
)

# Note: Python bindings (moq_client_pybinding, moq_simple_publisher_pybinding,
# py_moq_client_tester) are not built in the OSS CMake build.

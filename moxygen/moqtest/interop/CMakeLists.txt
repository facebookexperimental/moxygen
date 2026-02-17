# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# moq_interop_client
add_executable(
  moq_interop_client
  MoQInteropClient.cpp
  MoQInteropClientMain.cpp
)
set_target_properties(
  moq_interop_client
  PROPERTIES
    BUILD_RPATH ${DEPS_LIBRARIES_DIR}
    INSTALL_RPATH ${DEPS_LIBRARIES_DIR}
)
target_include_directories(
  moq_interop_client PUBLIC $<BUILD_INTERFACE:${MOXYGEN_FBCODE_ROOT}>
)
target_compile_options(
  moq_interop_client PRIVATE
  ${_MOXYGEN_COMMON_COMPILE_OPTIONS}
)
target_link_libraries(
  moq_interop_client PUBLIC
  moxygen_events_moq_folly_executor_impl
  moxygen_moq_relay_session
  moxygen_moq_webtransport_client
  moxygen_moqclient
  Folly::folly_coro_blocking_wait
  Folly::folly_coro_collect
  Folly::folly_coro_sleep
  Folly::folly_coro_task
  Folly::folly_coro_timeout
  Folly::folly_init_init
)

install(
    TARGETS moq_interop_client
    EXPORT moxygen-exports
    ARCHIVE DESTINATION ${LIB_INSTALL_DIR}
    LIBRARY DESTINATION ${LIB_INSTALL_DIR}
)

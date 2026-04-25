# Copyright (c) Meta Platforms, Inc. and affiliates.
# This source code is licensed under the Apache 2.0 license found in the
# LICENSE file in the root directory of this source tree.

# Manually maintained - has MoQRelayServer binary

moxygen_add_library(moxygen_relay_moq_forwarder
  SRCS
    MoQForwarder.cpp
  EXPORTED_DEPS
    moxygen_moq
    moxygen_moq_location
    moxygen_moq_types
    Folly::folly_container_f14_hash
    Folly::folly_hash_hash
)

moxygen_add_library(moxygen_relay_moq_cache
  SRCS
    MoQCache.cpp
  EXPORTED_DEPS
    moxygen_moq
    moxygen_moq_consumers
    moxygen_moq_framer
    moxygen_util_bidi_iterator
    moxygen_util_fetch_interval_set
    Folly::folly_container_f14_hash
    Folly::folly_coro_baton
    Folly::folly_coro_task
    Folly::folly_logging_logging
)

moxygen_add_library(moxygen_relay_moq_relay
  SRCS
    MoQRelay.cpp
  EXPORTED_DEPS
    moxygen_moq
    moxygen_moq_consumers
    moxygen_moq_types
    moxygen_relay_moq_cache
    moxygen_relay_moq_forwarder
    Folly::folly_container_f14_hash
    Folly::folly_coro_shared_promise
)

moxygen_add_library(moxygen_relay_moq_relay_client
  EXPORTED_DEPS
    moxygen_moq_relay_session
    moxygen_moqclient
    mvfst::mvfst_state_transport_settings
    Folly::folly_coro_sleep
    Folly::folly_logging_logging
)

# MoQRelayServer binary
add_executable(
  moqrelayserver
  MoQRelayServer.cpp
)
set_target_properties(
  moqrelayserver
  PROPERTIES
    BUILD_RPATH ${DEPS_LIBRARIES_DIR}
    INSTALL_RPATH ${DEPS_LIBRARIES_DIR}
)
target_include_directories(
  moqrelayserver PUBLIC $<BUILD_INTERFACE:${MOXYGEN_FBCODE_ROOT}>
)
target_compile_options(
  moqrelayserver PRIVATE
  ${_MOXYGEN_COMMON_COMPILE_OPTIONS}
)
target_link_libraries(
  moqrelayserver PUBLIC
  moxygen_relay_moq_relay
  moxygen_moq_relay_session
  moxygen_moq_server
  Folly::folly_init_init
  Folly::folly_io_async_async_signal_handler
)

install(
    TARGETS moqrelayserver
    EXPORT moxygen-exports
    ARCHIVE DESTINATION ${LIB_INSTALL_DIR}
    LIBRARY DESTINATION ${LIB_INSTALL_DIR}
)

add_subdirectory(test)

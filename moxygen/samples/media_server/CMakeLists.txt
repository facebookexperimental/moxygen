# Copyright (c) Meta Platforms, Inc. and affiliates.
# This source code is licensed under the Apache 2.0 license found in the
# LICENSE file in the root directory of this source tree.

# Manually maintained - has moq_media_server + moq_mp4_receiver binaries

moxygen_add_library(moxygen_samples_media_server_media_catalog
  SRCS
    MediaCatalog.cpp
  DEPS
    Folly::folly_dynamic
    Folly::folly_logging_logging
  EXPORTED_DEPS
    Folly::folly_range
)

moxygen_add_library(moxygen_samples_media_server
  SRCS
    MoQBroadcast.cpp
    MoQBroadcastDispatcher.cpp
    MoQBroadcastFactory.cpp
    PublishLoop.cpp
    sources/FileMediaSourceResolver.cpp
    sources/Fmp4MediaSource.cpp
  DEPS
    moxygen_moq_consumers
    moxygen_moq_publish_error
    Folly::folly_base64
    Folly::folly_coro_sleep
    Folly::folly_coro_with_cancellation
  EXPORTED_DEPS
    moxygen_samples_media_server_media_catalog
    moxygen_moq
    moxygen_moq_server
    moxygen_moq_types
    moxygen_relay_moq_forwarder
    Folly::folly_cancellation_token
    Folly::folly_container_f14_hash
    Folly::folly_coro_async_generator
    Folly::folly_coro_baton
    Folly::folly_coro_shared_promise
    Folly::folly_coro_task
    Folly::folly_executor
    Folly::folly_function
    Folly::folly_io_iobuf
    Folly::folly_logging_logging
)

# MoQMediaServer binary
add_executable(
  moq_media_server
  MoQMediaServerMain.cpp
)
set_target_properties(
  moq_media_server
  PROPERTIES
    BUILD_RPATH ${DEPS_LIBRARIES_DIR}
    INSTALL_RPATH ${DEPS_LIBRARIES_DIR}
)
target_include_directories(
  moq_media_server PUBLIC $<BUILD_INTERFACE:${MOXYGEN_FBCODE_ROOT}>
)
target_compile_options(
  moq_media_server PRIVATE
  ${_MOXYGEN_COMMON_COMPILE_OPTIONS}
)
target_link_libraries(
  moq_media_server PUBLIC
  moxygen_samples_media_server
  moxygen_moq_server
  moxygen_moq_versions
  moxygen_util_signal_handler
  proxygen::proxygen_hq_server
  Folly::folly_coro_task
  Folly::folly_init_init
  Folly::folly_io_async_async_base
  Folly::folly_io_async_scoped_event_base_thread
  Folly::folly_logging_logging
  Folly::folly_network_address
)

install(
    TARGETS moq_media_server
    EXPORT moxygen-exports
    ARCHIVE DESTINATION ${LIB_INSTALL_DIR}
    LIBRARY DESTINATION ${LIB_INSTALL_DIR}
)

# MoQMp4Receiver test client binary
add_executable(
  moq_mp4_receiver
  MoQMp4Receiver.cpp
)
set_target_properties(
  moq_mp4_receiver
  PROPERTIES
    BUILD_RPATH ${DEPS_LIBRARIES_DIR}
    INSTALL_RPATH ${DEPS_LIBRARIES_DIR}
)
target_include_directories(
  moq_mp4_receiver PUBLIC $<BUILD_INTERFACE:${MOXYGEN_FBCODE_ROOT}>
)
target_compile_options(
  moq_mp4_receiver PRIVATE
  ${_MOXYGEN_COMMON_COMPILE_OPTIONS}
)
target_link_libraries(
  moq_mp4_receiver PUBLIC
  moxygen_samples_media_server_media_catalog
  moxygen_events_moq_folly_executor_impl
  moxygen_moq_obj_receiver
  moxygen_moq_versions
  moxygen_relay_moq_relay_client
  moxygen_samples_util_qmux_relay_utils
  moxygen_util_insecure_verifier_dangerous_do_not_use_in_production
  Folly::folly_base64
  Folly::folly_conv
  Folly::folly_coro_task
  Folly::folly_coro_timeout
  Folly::folly_init_init
  Folly::folly_io_async_async_base
  Folly::folly_io_async_async_signal_handler
  Folly::folly_logging_logging
  Folly::folly_portability_gflags
)

install(
    TARGETS moq_mp4_receiver
    EXPORT moxygen-exports
    ARCHIVE DESTINATION ${LIB_INSTALL_DIR}
    LIBRARY DESTINATION ${LIB_INSTALL_DIR}
)

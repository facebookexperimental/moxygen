# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# Relay
add_library(moqcache MoQCache.cpp)
target_include_directories(
  moqcache PUBLIC
  $<BUILD_INTERFACE:${MOXYGEN_FBCODE_ROOT}>
)
target_compile_options(
  moqcache PRIVATE
  ${_MOXYGEN_COMMON_COMPILE_OPTIONS}
)
target_link_libraries(
  moqcache PUBLIC
  moxygen
)

install(
    TARGETS moqcache
    EXPORT moxygen-exports
    ARCHIVE DESTINATION ${LIB_INSTALL_DIR}
    LIBRARY DESTINATION ${LIB_INSTALL_DIR}
)

add_library(moqrelay MoQRelay.cpp MoQCache.cpp)
target_include_directories(
  moqrelay PUBLIC
  $<BUILD_INTERFACE:${MOXYGEN_FBCODE_ROOT}>
)
target_compile_options(
  moqrelay PRIVATE
  ${_MOXYGEN_COMMON_COMPILE_OPTIONS}
)
target_link_libraries(
  moqrelay PUBLIC
  Folly::folly
  wangle::wangle
  proxygen::proxygen
  proxygen::proxygenhqserver
  moxygen
)

install(
    TARGETS moqrelay
    EXPORT moxygen-exports
    ARCHIVE DESTINATION ${LIB_INSTALL_DIR}
    LIBRARY DESTINATION ${LIB_INSTALL_DIR}
)

# MoQRelayServer
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
  moqrelayserver PUBLIC
  $<BUILD_INTERFACE:${MOXYGEN_FBCODE_ROOT}>
)
target_compile_options(
  moqrelayserver PRIVATE
  ${_MOXYGEN_COMMON_COMPILE_OPTIONS}
)
target_link_libraries(
  moqrelayserver PUBLIC
  Folly::folly
  moqrelay
  moxygenserver
  mlogger
)

install(
    TARGETS moqrelayserver
    EXPORT moxygen-exports
    ARCHIVE DESTINATION ${LIB_INSTALL_DIR}
    LIBRARY DESTINATION ${LIB_INSTALL_DIR}
)

add_subdirectory(test)

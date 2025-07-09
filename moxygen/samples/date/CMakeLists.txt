# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# Date
add_executable(
  moqdateserver
  MoQDateServer.cpp
)
set_target_properties(
  moqdateserver
  PROPERTIES
    BUILD_RPATH ${DEPS_LIBRARIES_DIR}
    INSTALL_RPATH ${DEPS_LIBRARIES_DIR}
)
target_include_directories(
  moqdateserver PUBLIC $<BUILD_INTERFACE:${MOXYGEN_FBCODE_ROOT}>
)
target_compile_options(
  moqdateserver PRIVATE
  ${_MOXYGEN_COMMON_COMPILE_OPTIONS}
)
target_link_libraries(
  moqdateserver PUBLIC
  Folly::folly
  moqrelay
  moxygenserver
  moxygenwtclient
  mlogger
)

install(
    TARGETS moqdateserver
    EXPORT moxygen-exports
    ARCHIVE DESTINATION ${LIB_INSTALL_DIR}
    LIBRARY DESTINATION ${LIB_INSTALL_DIR}
)

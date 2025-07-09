# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# MoQMi
add_library(mlogger
    MLogger.cpp
    MLogEvents.cpp
    MLogTypes.cpp
)

target_include_directories(
    mlogger PUBLIC
    $<BUILD_INTERFACE:${MOXYGEN_FBCODE_ROOT}>
)
target_compile_options(
    mlogger PRIVATE
    ${_MOXYGEN_COMMON_COMPILE_OPTIONS}
)
target_link_libraries(
    mlogger PUBLIC
    Folly::folly
    moxygen
)

install(
    TARGETS mlogger
    EXPORT moxygen-exports
    ARCHIVE DESTINATION ${LIB_INSTALL_DIR}
    LIBRARY DESTINATION ${LIB_INSTALL_DIR}
)

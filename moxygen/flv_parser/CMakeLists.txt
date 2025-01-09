# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# flvparser
add_library(flvparser
    FlvCommon.cpp
    FlvReader.cpp
    FlvWriter.cpp
    FlvSequentialReader.cpp
)

target_include_directories(
    flvparser PUBLIC
    $<BUILD_INTERFACE:${MOXYGEN_FBCODE_ROOT}>
)
target_compile_options(
    flvparser PRIVATE
    ${_MOXYGEN_COMMON_COMPILE_OPTIONS}
)

target_link_libraries(
    flvparser PUBLIC
    Folly::folly
)

install(
    TARGETS flvparser
    EXPORT moxygen-exports
    ARCHIVE DESTINATION ${LIB_INSTALL_DIR}
    LIBRARY DESTINATION ${LIB_INSTALL_DIR}
)

add_subdirectory(test)

# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# MoQMi
add_library(moqmi
    MoQMi.cpp
)

target_include_directories(
    moqmi PUBLIC
    $<BUILD_INTERFACE:${MOXYGEN_FBCODE_ROOT}>
)
target_compile_options(
    moqmi PRIVATE
    ${_MOXYGEN_COMMON_COMPILE_OPTIONS}
)
target_link_libraries(
    moqmi PUBLIC
    Folly::folly
    mvfst::mvfst_codec_types
    mvfst::mvfst_folly_utils
)

install(
    TARGETS moqmi
    EXPORT moxygen-exports
    ARCHIVE DESTINATION ${LIB_INSTALL_DIR}
    LIBRARY DESTINATION ${LIB_INSTALL_DIR}
)

add_subdirectory(test)

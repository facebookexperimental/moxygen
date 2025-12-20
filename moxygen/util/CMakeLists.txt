# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

add_library(moqinteger
    MoQInteger.cpp
)

target_include_directories(
    moqinteger PUBLIC
    $<BUILD_INTERFACE:${MOXYGEN_FBCODE_ROOT}>
)

target_compile_options(
    moqinteger PRIVATE
    ${_MOXYGEN_COMMON_COMPILE_OPTIONS}
)

target_link_libraries(
    moqinteger PUBLIC
    Folly::folly
)

add_subdirectory(test)

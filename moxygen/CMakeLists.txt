# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# MOQ
add_library(moxygen
    MoQFramer.cpp
    MoQCodec.cpp
    MoQSession.cpp
    MoQServer.cpp
    MoQClient.cpp
    util/QuicConnector.cpp)

target_include_directories(
    moxygen PUBLIC
    $<BUILD_INTERFACE:${MOXYGEN_FBCODE_ROOT}>
)
target_compile_options(
    moxygen PRIVATE
    ${_MOXYGEN_COMMON_COMPILE_OPTIONS}
)
target_link_libraries(
    moxygen PUBLIC
    Folly::folly
    wangle::wangle
    proxygen::proxygen
    proxygen::proxygenhqserver
    proxygen::quicwebtransport
)

install(
    TARGETS moxygen
    EXPORT moxygen-exports
    ARCHIVE DESTINATION ${LIB_INSTALL_DIR}
    LIBRARY DESTINATION ${LIB_INSTALL_DIR}
)

add_subdirectory(relay)
# TODO: Fails with compiler error in ubuntu P1361252588
# add_subdirectory(samples/chat)
add_subdirectory(samples/text-client)
add_subdirectory(samples/date)
add_subdirectory(test)

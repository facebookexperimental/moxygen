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
    MoQTokenCache.cpp
    events/MoQDeliveryTimer.cpp
)

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
    mvfst::mvfst_events
    mlogger
)

install(
    TARGETS moxygen
    EXPORT moxygen-exports
    ARCHIVE DESTINATION ${LIB_INSTALL_DIR}
    LIBRARY DESTINATION ${LIB_INSTALL_DIR}
)

add_library(moqfollyexecutorimpl
    events/MoQFollyExecutorImpl.cpp
)

target_include_directories(
    moqfollyexecutorimpl PUBLIC
    $<BUILD_INTERFACE:${MOXYGEN_FBCODE_ROOT}>
)

target_compile_options(
    moqfollyexecutorimpl PRIVATE
    ${_MOXYGEN_COMMON_COMPILE_OPTIONS}
)

target_link_libraries(
    moqfollyexecutorimpl PUBLIC
    Folly::folly
    mvfst::mvfst_events
)

add_library(moxygenserver
    MoQServer.cpp)

target_include_directories(
    moxygenserver PUBLIC
    $<BUILD_INTERFACE:${MOXYGEN_FBCODE_ROOT}>
)

target_compile_options(
    moxygenserver PRIVATE
    ${_MOXYGEN_COMMON_COMPILE_OPTIONS}
)

target_link_libraries(
    moxygenserver PUBLIC
    moxygen
    proxygen::proxygenhqserver
    proxygen::quicwebtransport
    moqfollyexecutorimpl
)

add_library(moxygenclient
    MoQClient.cpp
    MoQClientBase.cpp
    util/QuicConnector.cpp
)

target_include_directories(
    moxygenclient PUBLIC
    $<BUILD_INTERFACE:${MOXYGEN_FBCODE_ROOT}>
)

target_compile_options(
    moxygenclient PRIVATE
    ${_MOXYGEN_COMMON_COMPILE_OPTIONS}
)

target_link_libraries(
    moxygenclient PUBLIC
    moxygen
    proxygen::proxygenhqserver
    proxygen::quicwebtransport
    moqfollyexecutorimpl
)

add_library(moxygenwtclient
    MoQWebTransportClient.cpp
)

target_include_directories(
    moxygenwtclient PUBLIC
    $<BUILD_INTERFACE:${MOXYGEN_FBCODE_ROOT}>
)

target_compile_options(
    moxygenwtclient PRIVATE
    ${_MOXYGEN_COMMON_COMPILE_OPTIONS}
)

target_link_libraries(
    moxygenwtclient PUBLIC
    moxygenclient
    proxygen::proxygenhqserver
)


add_subdirectory(mlog)
add_subdirectory(relay)
# TODO: Fails with compiler error in ubuntu P1361252588
# add_subdirectory(samples/chat)
add_subdirectory(samples/text-client)
add_subdirectory(samples/date)
add_subdirectory(samples/flv_streamer_client)
add_subdirectory(samples/flv_receiver_client)
add_subdirectory(moq_mi)
add_subdirectory(flv_parser)
add_subdirectory(test)

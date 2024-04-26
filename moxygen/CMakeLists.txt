# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

cmake_minimum_required(VERSION 3.10)
include(CheckCXXCompilerFlag)
include(CMakePrintHelpers)

project(
    moqt
)

# Needs 20 for co routines
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# MoqFramer
add_library(MoqFramer MoQFramer.cpp)
target_include_directories(MoqFramer PRIVATE ${DEPS_INCLUDES_DIR})

# MoqCodec
add_library(MoqCodec MoQCodec.cpp)
target_include_directories(MoqCodec PRIVATE ${DEPS_INCLUDES_DIR})

# MoqSession
add_library(MoqSession MoqSession.cpp)
target_include_directories(MoqSession PRIVATE ${DEPS_INCLUDES_DIR})

# MoqServer
add_library(MoqServer MoqServer.cpp)
target_include_directories(MoqServer PRIVATE ${DEPS_INCLUDES_DIR})
target_include_directories(MoqServer PRIVATE ${PROXYGEN_INCLUDES})

# MoqClient
add_library(MoqClient MoqClient.cpp)
target_include_directories(MoqClient PRIVATE ${DEPS_INCLUDES_DIR})
target_include_directories(MoqClient PRIVATE ${PROXYGEN_INCLUDES})

add_subdirectory(relay)
add_subdirectory(samples/chat)
add_subdirectory(samples/text-client)

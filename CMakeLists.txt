# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

cmake_minimum_required(VERSION 3.10)
include(CheckCXXCompilerFlag)

project(
  moxygen
)

# Needs 20 for co routines
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(CMAKE_MODULE_PATH
  "${CMAKE_CURRENT_SOURCE_DIR}/cmake"
  # for in-fbsource builds
  "${CMAKE_CURRENT_SOURCE_DIR}/../opensource/fbcode_builder/CMake"
  # For shipit-transformed builds
  "${CMAKE_CURRENT_SOURCE_DIR}/build/fbcode_builder/CMake"
  ${CMAKE_MODULE_PATH})

option(BUILD_SHARED_LIBS
  "If enabled, build moxygen as a shared library.  \
  This is generally discouraged, since moxygen does not commit to having \
  a stable ABI."
  OFF
)

# Mark BUILD_SHARED_LIBS as an "advanced" option, since enabling it
# is generally discouraged.
mark_as_advanced(BUILD_SHARED_LIBS)

include(FBBuildOptions)
fb_activate_static_library_option()

set(MOXYGEN_FBCODE_ROOT ${CMAKE_CURRENT_SOURCE_DIR})

# Dependencies libs
set (DEPS_LIBRARIES_DIR ${MOXYGEN_FBCODE_ROOT}/_build/deps/lib)

# Dependencies
#
# IMPORTANT: If you change this, make the analogous update in:
#   cmake/moxygen-config.cmake.in
find_package(folly REQUIRED)
find_package(wangle REQUIRED)
find_package(mvfst REQUIRED)
find_package(proxygen REQUIRED)
find_package(Glog REQUIRED)
find_package(gflags REQUIRED)
find_package(Zstd REQUIRED)

list(APPEND
  _MOXYGEN_COMMON_COMPILE_OPTIONS
  -Wall
  -Wextra
)

CHECK_CXX_COMPILER_FLAG(-Wnoexcept-type COMPILER_HAS_W_NOEXCEPT_TYPE)
if (COMPILER_HAS_W_NOEXCEPT_TYPE)
  list(APPEND _MOXYGEN_COMMON_COMPILE_OPTIONS -Wno-noexcept-type)
endif()
CHECK_CXX_COMPILER_FLAG(-Wunused-parameter COMPILER_HAS_W_UNUSED_PARAMETER)
if (COMPILER_HAS_W_UNUSED_PARAMETER)
  list(APPEND _MOXYGEN_COMMON_COMPILE_OPTIONS -Wno-unused-parameter)
endif()
CHECK_CXX_COMPILER_FLAG(-Wmissing-field-initializers COMPILER_HAS_W_MISSING_FIELD_INITIALIZERS)
if (COMPILER_HAS_W_MISSING_FIELD_INITIALIZERS)
  list(APPEND _MOXYGEN_COMMON_COMPILE_OPTIONS -Wno-missing-field-initializers)
endif()
CHECK_CXX_COMPILER_FLAG(-Wnullability-completeness COMPILER_HAS_W_NULLABILITY_COMPLETENESS)
if (COMPILER_HAS_W_NULLABILITY_COMPLETENESS)
    list(APPEND _MOXYGEN_COMMON_COMPILE_OPTIONS -Wno-nullability-completeness)
endif()
CHECK_CXX_COMPILER_FLAG(-Wdeprecated-register COMPILER_HAS_W_DEPRECATED_REGISTER)
if (COMPILER_HAS_W_DEPRECATED_REGISTER)
    list(APPEND _MOXYGEN_COMMON_COMPILE_OPTIONS -Wno-deprecated-register)
endif()
CHECK_CXX_COMPILER_FLAG(-Wregister COMPILER_HAS_W_REGISTER)
if (COMPILER_HAS_W_REGISTER)
    list(APPEND _MOXYGEN_COMMON_COMPILE_OPTIONS -Wno-register)
endif()
CHECK_CXX_COMPILER_FLAG(-Wunused-value COMPILER_HAS_W_UNUSED_VALUE)
if (COMPILER_HAS_W_UNUSED_VALUE)
    list(APPEND _MOXYGEN_COMMON_COMPILE_OPTIONS -Wno-unused-value)
endif()

include(MoxygenTest)

add_subdirectory(moxygen)

if (NOT DEFINED LIB_INSTALL_DIR)
    set(LIB_INSTALL_DIR "lib")
endif()

if (NOT DEFINED CMAKE_INSTALL_DIR)
    set(CMAKE_INSTALL_DIR "${LIB_INSTALL_DIR}/cmake/moxygen/")
endif()

install(
    EXPORT moxygen-exports
    FILE moxygen-targets.cmake
    NAMESPACE moxygen::
    DESTINATION ${CMAKE_INSTALL_DIR}
)
include(CMakePackageConfigHelpers)
configure_package_config_file(
    cmake/moxygen-config.cmake.in
    ${CMAKE_CURRENT_BINARY_DIR}/moxygen-config.cmake
    INSTALL_DESTINATION ${CMAKE_INSTALL_DIR}
)
install(
    FILES ${CMAKE_CURRENT_BINARY_DIR}/moxygen-config.cmake
    DESTINATION ${CMAKE_INSTALL_DIR}
)

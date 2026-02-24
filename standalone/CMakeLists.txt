# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

cmake_minimum_required(VERSION 3.16)
project(moxygen-standalone VERSION 1.0.0 LANGUAGES CXX C)

# Disable CMake user package registry to prevent picking up stale getdeps builds
set(CMAKE_FIND_USE_PACKAGE_REGISTRY OFF)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Required for glog 0.7+ compatibility
add_compile_definitions(GLOG_USE_GLOG_EXPORT)

# =============================================================================
# Options
# =============================================================================
option(BUILD_TESTS "Build tests" ON)
option(BUILD_SHARED_LIBS "Build shared libraries (discouraged)" OFF)

# =============================================================================
# Linking preferences
# =============================================================================
# Use static Boost (required by folly). Other system libraries use platform
# defaults (shared on Linux) to avoid PIE issues with non-PIC static libs.
# FetchContent deps are built from source with BUILD_SHARED_LIBS=OFF so
# they are already static.
set(Boost_USE_STATIC_LIBS ON CACHE BOOL "Use static Boost libraries" FORCE)
set(BOOST_LINK_STATIC ON CACHE BOOL "Link Boost statically (for folly)" FORCE)

find_package(ZLIB REQUIRED)
find_library(ZSTD_LIBRARY NAMES zstd)

# =============================================================================
# Read pinned commit hashes from github_hashes (same as getdeps uses)
# =============================================================================
set(GITHUB_HASHES_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../build/deps/github_hashes")

# Helper function to read commit hash from rev.txt file
function(read_rev_hash ORG PROJECT OUT_VAR)
    set(REV_FILE "${GITHUB_HASHES_DIR}/${ORG}/${PROJECT}-rev.txt")
    if(EXISTS "${REV_FILE}")
        file(READ "${REV_FILE}" REV_CONTENT)
        # Format: "Subproject commit <40-char-hash>"
        string(
            REGEX MATCH "Subproject commit ([a-fA-F0-9]+)"
            _ "${REV_CONTENT}"
        )
        if(CMAKE_MATCH_1)
            set(${OUT_VAR} "${CMAKE_MATCH_1}" PARENT_SCOPE)
            message(STATUS "Using pinned ${PROJECT} rev: ${CMAKE_MATCH_1}")
        else()
            message(FATAL_ERROR "Failed to parse rev from ${REV_FILE}")
        endif()
    else()
        message(FATAL_ERROR "Rev file not found: ${REV_FILE}\n"
            "Make sure you cloned moxygen with build/deps/github_hashes/")
    endif()
endfunction()

# Read all pinned revisions
read_rev_hash("facebook" "folly" FOLLY_REV)
read_rev_hash("facebookincubator" "fizz" FIZZ_REV)
read_rev_hash("facebook" "wangle" WANGLE_REV)
read_rev_hash("facebook" "mvfst" MVFST_REV)
read_rev_hash("facebook" "proxygen" PROXYGEN_REV)

# =============================================================================
# System Dependencies (must be pre-installed)
# =============================================================================
list(APPEND CMAKE_MODULE_PATH
    "${CMAKE_CURRENT_SOURCE_DIR}/../build/fbcode_builder/CMake"
)
find_package(Threads REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(gflags CONFIG REQUIRED)
find_package(glog CONFIG REQUIRED)
find_package(fmt CONFIG REQUIRED)
find_package(double-conversion CONFIG REQUIRED)
find_package(Boost 1.70 REQUIRED COMPONENTS
    context filesystem program_options regex)
find_package(ZLIB REQUIRED)
find_package(Zstd REQUIRED)
find_package(LibEvent REQUIRED)
find_package(Sodium REQUIRED)

# Check for libunwind to decide if we need lzma for symbolization
find_package(LibUnwind QUIET)

# =============================================================================
# FetchContent: Meta OSS Dependencies (using pinned hashes)
# =============================================================================
include(FetchContent)
set(FETCHCONTENT_QUIET OFF)

# googletest 1.12+ required for move-only WillOnce support
# (Ubuntu 22.04 ships 1.11 which doesn't support this)
if(BUILD_TESTS)
    # Try to find system googletest first
    find_package(GTest 1.12.1 QUIET)

    if(GTest_FOUND)
        message(STATUS "Using system googletest ${GTest_VERSION}")
        include(GoogleTest)
    else()
        message(STATUS
            "System googletest not found or too old, using FetchContent"
        )
        FetchContent_Declare(googletest
            GIT_REPOSITORY https://github.com/google/googletest.git
            GIT_TAG release-1.12.1
        )
        set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
        # Required on Linux where GCC defaults to PIE executables;
        # without this, static libgmock.a/libgtest.a have non-PIC
        # relocations that fail to link into PIE binaries.
        set(CMAKE_POSITION_INDEPENDENT_CODE ON)
        FetchContent_MakeAvailable(googletest)
        # Ensure FetchContent googletest headers take precedence over homebrew
        target_include_directories(gtest BEFORE INTERFACE
            $<BUILD_INTERFACE:${googletest_SOURCE_DIR}/googletest/include>)
        target_include_directories(gmock BEFORE INTERFACE
            $<BUILD_INTERFACE:${googletest_SOURCE_DIR}/googlemock/include>)
        # Provide GoogleTest module (for gtest_add_tests) since
        # find_package(GTest/GMock) is skipped for FetchContent-provided deps.
        include(GoogleTest)
    endif()
endif()

# Save original BUILD_TESTS value, then disable for fetched dependencies
set(_MOXYGEN_BUILD_TESTS ${BUILD_TESTS})
set(BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)

message(STATUS "Fetching Meta OSS dependencies using pinned revisions...")

# fast_float is a header-only library required by folly
FetchContent_Declare(fast_float
    GIT_REPOSITORY https://github.com/fastfloat/fast_float.git
    GIT_TAG v8.0.0
)
FetchContent_GetProperties(fast_float)
if(NOT fast_float_POPULATED)
    FetchContent_Populate(fast_float)
endif()
set(FASTFLOAT_INCLUDE_DIR
    "${fast_float_SOURCE_DIR}/include" CACHE PATH "" FORCE)

# Allow overriding folly repo/rev via -DFOLLY_REPO_OVERRIDE and
# -DFOLLY_REV_OVERRIDE
set(FOLLY_REPO_OVERRIDE "" CACHE STRING "Override folly git repository URL")
set(FOLLY_REV_OVERRIDE "" CACHE STRING "Override folly git revision")
if(FOLLY_REPO_OVERRIDE)
    set(_FOLLY_REPO "${FOLLY_REPO_OVERRIDE}")
else()
    set(_FOLLY_REPO "https://github.com/facebook/folly.git")
endif()
if(FOLLY_REV_OVERRIDE)
    set(_FOLLY_REV "${FOLLY_REV_OVERRIDE}")
else()
    set(_FOLLY_REV "${FOLLY_REV}")
endif()

FetchContent_Declare(folly
    GIT_REPOSITORY ${_FOLLY_REPO}
    GIT_TAG ${_FOLLY_REV}
)

# Allow overriding fizz repo/rev via -DFIZZ_REPO_OVERRIDE and
# -DFIZZ_REV_OVERRIDE
set(FIZZ_REPO_OVERRIDE "" CACHE STRING "Override fizz git repository URL")
set(FIZZ_REV_OVERRIDE "" CACHE STRING "Override fizz git revision")
if(FIZZ_REPO_OVERRIDE)
    set(_FIZZ_REPO "${FIZZ_REPO_OVERRIDE}")
else()
    set(_FIZZ_REPO "https://github.com/facebookincubator/fizz.git")
endif()
if(FIZZ_REV_OVERRIDE)
    set(_FIZZ_REV "${FIZZ_REV_OVERRIDE}")
else()
    set(_FIZZ_REV "${FIZZ_REV}")
endif()

FetchContent_Declare(fizz
    GIT_REPOSITORY ${_FIZZ_REPO}
    GIT_TAG ${_FIZZ_REV}
)

FetchContent_Declare(wangle
    GIT_REPOSITORY https://github.com/facebook/wangle.git
    GIT_TAG ${WANGLE_REV}
)

FetchContent_Declare(mvfst
    GIT_REPOSITORY https://github.com/facebook/mvfst.git
    GIT_TAG ${MVFST_REV}
)

# Allow overriding proxygen repo/rev via -DPROXYGEN_REPO_OVERRIDE and
# -DPROXYGEN_REV_OVERRIDE
set(PROXYGEN_REPO_OVERRIDE ""
    CACHE STRING "Override proxygen git repository URL")
set(PROXYGEN_REV_OVERRIDE "" CACHE STRING "Override proxygen git revision")
if(PROXYGEN_REPO_OVERRIDE)
    set(_PROXYGEN_REPO "${PROXYGEN_REPO_OVERRIDE}")
else()
    set(_PROXYGEN_REPO "https://github.com/facebook/proxygen.git")
endif()
if(PROXYGEN_REV_OVERRIDE)
    set(_PROXYGEN_REV "${PROXYGEN_REV_OVERRIDE}")
else()
    set(_PROXYGEN_REV "${PROXYGEN_REV}")
endif()

FetchContent_Declare(proxygen
    GIT_REPOSITORY ${_PROXYGEN_REPO}
    GIT_TAG ${_PROXYGEN_REV}
)

# Populate and add dependencies with EXCLUDE_FROM_ALL so only moxygen
# targets are in 'all'
# Override find_package for deps we provide via FetchContent, since sub-projects
# (fizz, wangle, mvfst, proxygen) call find_package(folly),
# find_package(fizz), etc.
# which would fail because these aren't installed — they're in-tree targets.
# Also skip optional compression codecs not needed for moxygen.
# Skip lzma unless libunwind is available (lzma is used for symbolization).
set(_FETCHCONTENT_PROVIDED_DEPS folly fizz wangle mvfst proxygen GTest GMock
    BZip2 LZ4 Snappy)
if(NOT LibUnwind_FOUND)
    list(APPEND _FETCHCONTENT_PROVIDED_DEPS LibLZMA)
    message(STATUS "LibUnwind not found, disabling lzma")
else()
    message(STATUS "LibUnwind found, enabling lzma for symbolization")
endif()
macro(find_package PKG_NAME)
    list(FIND _FETCHCONTENT_PROVIDED_DEPS "${PKG_NAME}" _fc_idx)
    if(_fc_idx GREATER -1)
        # Skip — already provided by FetchContent add_subdirectory
    else()
        _find_package(${ARGV})
    endif()
endmacro()

FetchContent_GetProperties(folly)
if(NOT folly_POPULATED)
    FetchContent_Populate(folly)
    add_subdirectory(${folly_SOURCE_DIR} ${folly_BINARY_DIR} EXCLUDE_FROM_ALL)
endif()

FetchContent_GetProperties(fizz)
if(NOT fizz_POPULATED)
    FetchContent_Populate(fizz)
    add_subdirectory(
        ${fizz_SOURCE_DIR}/fizz ${fizz_BINARY_DIR} EXCLUDE_FROM_ALL)
endif()

FetchContent_GetProperties(wangle)
if(NOT wangle_POPULATED)
    FetchContent_Populate(wangle)
    add_subdirectory(
        ${wangle_SOURCE_DIR}/wangle ${wangle_BINARY_DIR} EXCLUDE_FROM_ALL)
endif()

FetchContent_GetProperties(mvfst)
if(NOT mvfst_POPULATED)
    FetchContent_Populate(mvfst)
    add_subdirectory(${mvfst_SOURCE_DIR} ${mvfst_BINARY_DIR} EXCLUDE_FROM_ALL)
endif()

FetchContent_GetProperties(proxygen)
if(NOT proxygen_POPULATED)
    FetchContent_Populate(proxygen)
    add_subdirectory(
        ${proxygen_SOURCE_DIR} ${proxygen_BINARY_DIR} EXCLUDE_FROM_ALL)
endif()

# =============================================================================
# Moxygen
# =============================================================================
# Re-enable tests for moxygen itself
set(BUILD_TESTS ${_MOXYGEN_BUILD_TESTS} CACHE BOOL "" FORCE)

# Set MOXYGEN_FBCODE_ROOT to match what main CMakeLists.txt expects
set(MOXYGEN_FBCODE_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/..)

# Include the compile options setup
include(CheckCXXCompilerFlag)
list(APPEND _MOXYGEN_COMMON_COMPILE_OPTIONS -Wall -Wextra)
CHECK_CXX_COMPILER_FLAG(-Wno-unused-parameter COMPILER_HAS_W_UNUSED_PARAMETER)
if(COMPILER_HAS_W_UNUSED_PARAMETER)
    list(APPEND _MOXYGEN_COMMON_COMPILE_OPTIONS -Wno-unused-parameter)
endif()

# Add moxygen cmake modules to path and include test helper
list(APPEND CMAKE_MODULE_PATH
    "${CMAKE_CURRENT_SOURCE_DIR}/../cmake"
    "${CMAKE_CURRENT_SOURCE_DIR}/../build/fbcode_builder/CMake"
)
include(MoxygenFunctions)
if(BUILD_TESTS)
    include(MoxygenTest)
endif()

# Include moxygen's main CMakeLists.txt
add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/../moxygen moxygen)

# =============================================================================
# Installation (mirrors main CMakeLists.txt)
# =============================================================================
set(LIB_INSTALL_DIR "lib" CACHE STRING "Library install directory")
set(CMAKE_INSTALL_DIR
    "${LIB_INSTALL_DIR}/cmake/moxygen/"
    CACHE STRING "CMake config install directory")

install(
    EXPORT moxygen-exports
    FILE moxygen-targets.cmake
    NAMESPACE moxygen::
    DESTINATION ${CMAKE_INSTALL_DIR}
)

include(CMakePackageConfigHelpers)
configure_package_config_file(
    ${CMAKE_CURRENT_SOURCE_DIR}/../cmake/moxygen-config.cmake.in
    ${CMAKE_CURRENT_BINARY_DIR}/moxygen-config.cmake
    INSTALL_DESTINATION ${CMAKE_INSTALL_DIR}
)
install(
    FILES ${CMAKE_CURRENT_BINARY_DIR}/moxygen-config.cmake
    DESTINATION ${CMAKE_INSTALL_DIR}
)

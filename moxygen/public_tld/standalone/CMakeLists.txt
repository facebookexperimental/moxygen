# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

cmake_minimum_required(VERSION 3.16)
project(moxygen-standalone VERSION 1.0.0 LANGUAGES CXX C)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# =============================================================================
# Options
# =============================================================================
option(BUILD_TESTS "Build tests" ON)
option(BUILD_SHARED_LIBS "Build shared libraries (discouraged)" OFF)

# =============================================================================
# Prefer static linking for system libraries
# =============================================================================
set(CMAKE_FIND_LIBRARY_SUFFIXES ".a" ".dylib" ".so")
set(Boost_USE_STATIC_LIBS ON CACHE BOOL "Use static Boost libraries" FORCE)
set(BOOST_LINK_STATIC ON CACHE BOOL "Link Boost statically (for folly)" FORCE)

# Find system libs before the static preference breaks them (macOS has no static versions)
set(_saved_suffixes ${CMAKE_FIND_LIBRARY_SUFFIXES})
set(CMAKE_FIND_LIBRARY_SUFFIXES ".dylib" ".tbd" ".so" ".a")
find_package(ZLIB REQUIRED)
find_library(ZSTD_LIBRARY NAMES zstd)
set(CMAKE_FIND_LIBRARY_SUFFIXES ${_saved_suffixes})

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
        string(REGEX MATCH "Subproject commit ([a-fA-F0-9]+)" _ "${REV_CONTENT}")
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
list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/../build/fbcode_builder/CMake")
find_package(Threads REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(gflags CONFIG REQUIRED)
find_package(glog CONFIG REQUIRED)
find_package(fmt CONFIG REQUIRED)
find_package(double-conversion CONFIG REQUIRED)
find_package(Boost 1.70 REQUIRED COMPONENTS context filesystem program_options)
find_package(ZLIB REQUIRED)
find_package(Zstd REQUIRED)
find_package(Libevent CONFIG REQUIRED)
find_package(sodium REQUIRED)

if(BUILD_TESTS)
    find_package(GTest 1.10 CONFIG REQUIRED)
endif()

# =============================================================================
# FetchContent: Meta OSS Dependencies (using pinned hashes)
# =============================================================================
include(FetchContent)
set(FETCHCONTENT_QUIET OFF)

# Save original BUILD_TESTS value, then disable for fetched dependencies
set(_MOXYGEN_BUILD_TESTS ${BUILD_TESTS})
set(BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)

message(STATUS "Fetching Meta OSS dependencies using pinned revisions...")

FetchContent_Declare(folly
    GIT_REPOSITORY https://github.com/facebook/folly.git
    GIT_TAG ${FOLLY_REV}
)

FetchContent_Declare(fizz
    GIT_REPOSITORY https://github.com/facebookincubator/fizz.git
    GIT_TAG ${FIZZ_REV}
)

FetchContent_Declare(wangle
    GIT_REPOSITORY https://github.com/facebook/wangle.git
    GIT_TAG ${WANGLE_REV}
)

FetchContent_Declare(mvfst
    GIT_REPOSITORY https://github.com/facebook/mvfst.git
    GIT_TAG ${MVFST_REV}
)

FetchContent_Declare(proxygen
    GIT_REPOSITORY https://github.com/facebook/proxygen.git
    GIT_TAG ${PROXYGEN_REV}
)

# Populate and add dependencies with EXCLUDE_FROM_ALL so only moxygen targets are in 'all'
FetchContent_GetProperties(folly)
if(NOT folly_POPULATED)
    FetchContent_Populate(folly)
    add_subdirectory(${folly_SOURCE_DIR} ${folly_BINARY_DIR} EXCLUDE_FROM_ALL)
endif()

FetchContent_GetProperties(fizz)
if(NOT fizz_POPULATED)
    FetchContent_Populate(fizz)
    add_subdirectory(${fizz_SOURCE_DIR}/fizz ${fizz_BINARY_DIR} EXCLUDE_FROM_ALL)
endif()

FetchContent_GetProperties(wangle)
if(NOT wangle_POPULATED)
    FetchContent_Populate(wangle)
    add_subdirectory(${wangle_SOURCE_DIR}/wangle ${wangle_BINARY_DIR} EXCLUDE_FROM_ALL)
endif()

FetchContent_GetProperties(mvfst)
if(NOT mvfst_POPULATED)
    FetchContent_Populate(mvfst)
    add_subdirectory(${mvfst_SOURCE_DIR} ${mvfst_BINARY_DIR} EXCLUDE_FROM_ALL)
endif()

FetchContent_GetProperties(proxygen)
if(NOT proxygen_POPULATED)
    FetchContent_Populate(proxygen)
    add_subdirectory(${proxygen_SOURCE_DIR} ${proxygen_BINARY_DIR} EXCLUDE_FROM_ALL)
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
set(CMAKE_INSTALL_DIR "${LIB_INSTALL_DIR}/cmake/moxygen/" CACHE STRING "CMake config install directory")

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

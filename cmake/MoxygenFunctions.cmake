# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# Allow cross-directory target_link_libraries
cmake_policy(SET CMP0079 NEW)

# Initialize global properties for tracking targets and deferred dependencies
set_property(GLOBAL PROPERTY MOXYGEN_COMPONENT_TARGETS)
set_property(GLOBAL PROPERTY MOXYGEN_DEFERRED_DEPS)

# moxygen_add_library(<name>
#   SRCS file1.cpp file2.cpp        # Source files
#   DEPS internal_dep                # Private dependencies
#   EXPORTED_DEPS Folly::folly       # Public dependencies (propagated)
# )
function(moxygen_add_library _target_name)
  cmake_parse_arguments(
    MOXYGEN_LIB
    ""                              # Options (boolean flags)
    ""                              # Single-value args
    "SRCS;DEPS;EXPORTED_DEPS"       # Multi-value args
    ${ARGN}
  )

  set(_sources ${MOXYGEN_LIB_SRCS})
  if(NOT _sources)
    # Legacy support: if no SRCS keyword, treat remaining args as sources
    set(_sources ${MOXYGEN_LIB_UNPARSED_ARGUMENTS})
  endif()

  # Separate moxygen internal deps from external deps
  set(_immediate_deps "")
  set(_moxygen_deps "")
  foreach(_dep IN LISTS MOXYGEN_LIB_EXPORTED_DEPS)
    if(_dep MATCHES "^moxygen_")
      list(APPEND _moxygen_deps ${_dep})
    else()
      list(APPEND _immediate_deps ${_dep})
    endif()
  endforeach()

  set(_private_immediate "")
  set(_private_moxygen "")
  foreach(_dep IN LISTS MOXYGEN_LIB_DEPS)
    if(_dep MATCHES "^moxygen_")
      list(APPEND _private_moxygen ${_dep})
    else()
      list(APPEND _private_immediate ${_dep})
    endif()
  endforeach()

  # Skip if no sources (header-only library)
  list(LENGTH _sources _src_count)
  if(_src_count EQUAL 0)
    # Header-only: create INTERFACE library
    add_library(${_target_name} INTERFACE)
    target_include_directories(${_target_name}
      INTERFACE
        $<BUILD_INTERFACE:${MOXYGEN_FBCODE_ROOT}>
        $<INSTALL_INTERFACE:include/>
    )

    # Link external deps immediately for INTERFACE libraries
    if(_immediate_deps)
      target_link_libraries(${_target_name} INTERFACE ${_immediate_deps})
    endif()

    # Defer moxygen internal deps for INTERFACE libraries
    if(_moxygen_deps)
      list(JOIN _moxygen_deps "," _deps_str)
      set_property(GLOBAL APPEND PROPERTY MOXYGEN_DEFERRED_DEPS
        "${_target_name}|INTERFACE|${_deps_str}"
      )
    endif()

    install(TARGETS ${_target_name} EXPORT moxygen-exports)
    add_library(moxygen::${_target_name} ALIAS ${_target_name})
    return()
  endif()

  # Create STATIC library
  add_library(${_target_name} STATIC ${_sources})

  if(DEFINED PACKAGE_VERSION)
    set_property(TARGET ${_target_name} PROPERTY VERSION ${PACKAGE_VERSION})
  endif()

  if(BUILD_SHARED_LIBS)
    set_property(TARGET ${_target_name} PROPERTY POSITION_INDEPENDENT_CODE ON)
  endif()

  target_include_directories(${_target_name}
    PUBLIC
      $<BUILD_INTERFACE:${MOXYGEN_FBCODE_ROOT}>
      $<INSTALL_INTERFACE:include/>
  )

  target_compile_options(${_target_name}
    PRIVATE
    ${_MOXYGEN_COMMON_COMPILE_OPTIONS}
  )

  target_compile_features(${_target_name} PUBLIC cxx_std_20)

  # Link external deps immediately
  if(_immediate_deps)
    target_link_libraries(${_target_name} PUBLIC ${_immediate_deps})
  endif()

  # Defer moxygen internal deps
  if(_moxygen_deps)
    list(JOIN _moxygen_deps "," _deps_str)
    set_property(GLOBAL APPEND PROPERTY MOXYGEN_DEFERRED_DEPS
      "${_target_name}|PUBLIC|${_deps_str}"
    )
  endif()

  # Private deps
  if(_private_immediate)
    target_link_libraries(${_target_name} PRIVATE ${_private_immediate})
  endif()

  if(_private_moxygen)
    list(JOIN _private_moxygen "," _deps_str)
    set_property(GLOBAL APPEND PROPERTY MOXYGEN_DEFERRED_DEPS
      "${_target_name}|PRIVATE|${_deps_str}"
    )
  endif()

  # Track target
  set_property(GLOBAL APPEND PROPERTY MOXYGEN_COMPONENT_TARGETS ${_target_name})

  # Install
  install(
    TARGETS ${_target_name}
    EXPORT moxygen-exports
    ARCHIVE DESTINATION ${LIB_INSTALL_DIR}
    LIBRARY DESTINATION ${LIB_INSTALL_DIR}
  )

  # Create alias
  add_library(moxygen::${_target_name} ALIAS ${_target_name})
endfunction()

# Resolve deferred dependencies after all targets are defined
# Call this after all add_subdirectory() calls
function(moxygen_resolve_deferred_dependencies)
  # Allow linking targets defined in other directories
  cmake_policy(SET CMP0079 NEW)

  get_property(_deferred_deps GLOBAL PROPERTY MOXYGEN_DEFERRED_DEPS)

  foreach(_spec IN LISTS _deferred_deps)
    # Parse the spec: "target|visibility|dep1,dep2,..."
    string(REPLACE "|" ";" _parts "${_spec}")
    list(LENGTH _parts _len)
    if(_len LESS 3)
      continue()
    endif()

    list(GET _parts 0 _target)
    list(GET _parts 1 _visibility)
    list(GET _parts 2 _deps_str)

    # Split deps by comma
    string(REPLACE "," ";" _deps "${_deps_str}")

    # Filter to only existing targets (skip deps that weren't generated)
    set(_valid_deps "")
    foreach(_dep IN LISTS _deps)
      if(TARGET ${_dep})
        list(APPEND _valid_deps ${_dep})
      endif()
    endforeach()

    if(_valid_deps)
      target_link_libraries(${_target} ${_visibility} ${_valid_deps})
    endif()
  endforeach()
endfunction()

# Install headers preserving directory structure
function(moxygen_install_headers rootName rootDir)
  file(
    GLOB_RECURSE MOXYGEN_HEADERS_TOINSTALL
    RELATIVE ${rootDir}
    ${rootDir}/*.h
  )
  list(FILTER MOXYGEN_HEADERS_TOINSTALL EXCLUDE REGEX test/)
  list(FILTER MOXYGEN_HEADERS_TOINSTALL EXCLUDE REGEX facebook/)
  foreach(header ${MOXYGEN_HEADERS_TOINSTALL})
    get_filename_component(header_dir ${header} DIRECTORY)
    install(FILES ${rootDir}/${header}
            DESTINATION include/${rootName}/${header_dir})
  endforeach()
endfunction()

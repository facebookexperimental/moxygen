#  Copyright (c) 2018, Facebook, Inc.
#  All rights reserved.
#
#  This source code is licensed under the BSD-style license found in the
#  LICENSE file in the root directory of this source tree.

# This module sets the following variables:
#   moxygen_FOUND
#   moxygen_INCLUDE_DIRS
#
# This module exports the following target:
#    moxygen::moxygen
#
# which can be used with target_link_libraries() to pull in the moxygen
# library.


####### Expanded from @PACKAGE_INIT@ by configure_package_config_file() #######
####### Any changes to this file will be overwritten by the next CMake run ####
####### The input file was moxygen-config.cmake.in                            ########

get_filename_component(PACKAGE_PREFIX_DIR "${CMAKE_CURRENT_LIST_DIR}/../../../" ABSOLUTE)

macro(set_and_check _var _file)
  set(${_var} "${_file}")
  if(NOT EXISTS "${_file}")
    message(FATAL_ERROR "File or directory ${_file} referenced by variable ${_var} does not exist !")
  endif()
endmacro()

macro(check_required_components _NAME)
  foreach(comp ${${_NAME}_FIND_COMPONENTS})
    if(NOT ${_NAME}_${comp}_FOUND)
      if(${_NAME}_FIND_REQUIRED_${comp})
        set(${_NAME}_FOUND FALSE)
      endif()
    endif()
  endforeach()
endmacro()

####################################################################################

include(CMakeFindDependencyMacro)
find_dependency(folly)
find_dependency(wangle)
find_dependency(mvfst)
find_dependency(proxygen)

if(NOT TARGET moxygen::moxygen)
    include("${CMAKE_CURRENT_LIST_DIR}/moxygen-targets.cmake")
    get_target_property(moxygen_INCLUDE_DIRS moxygen::moxygen INTERFACE_INCLUDE_DIRECTORIES)
endif()

if(NOT moxygen_FIND_QUIETLY)
    message(STATUS "Found moxygen: ${PACKAGE_PREFIX_DIR}")
endif()

set(moxygen_LIBRARIES
  moxygen::moxygen
  moxygen::moqrelay
  moxygen::moqrelayserver
  moxygen::moqtextclient
  moxygen::moqchatclient
  moxygen::moqchatserver
  moxygen::moqdateserver
)

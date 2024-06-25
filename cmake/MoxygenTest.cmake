# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

option(BUILD_TESTS  "Enable tests" OFF)
include(CTest)
if(BUILD_TESTS)
  find_package(GMock 1.10.0 MODULE REQUIRED)
  find_package(GTest 1.10.0 MODULE REQUIRED)
endif()

function(moxygen_add_test)
    if(NOT BUILD_TESTS)
        return()
    endif()

    set(options)
    set(one_value_args TARGET WORKING_DIRECTORY PREFIX)
    set(multi_value_args SOURCES DEPENDS INCLUDES EXTRA_ARGS)
    cmake_parse_arguments(PARSE_ARGV 0 MOXYGEN_TEST "${options}" "${one_value_args}" "${multi_value_args}")

    if(NOT MOXYGEN_TEST_TARGET)
      message(FATAL_ERROR "The TARGET parameter is mandatory.")
    endif()

    if(NOT MOXYGEN_TEST_SOURCES)
      set(MOXYGEN_TEST_SOURCES "${MOXYGEN_TEST_TARGET}.cpp")
    endif()

    add_executable(${MOXYGEN_TEST_TARGET}
      "${MOXYGEN_TEST_SOURCES}"
    )

    set_property(TARGET ${MOXYGEN_TEST_TARGET} PROPERTY ENABLE_EXPORTS true)

    target_include_directories(${MOXYGEN_TEST_TARGET} PUBLIC
      "${MOXYGEN_TEST_INCLUDES}"
      ${LIBGMOCK_INCLUDE_DIR}
      ${LIBGTEST_INCLUDE_DIRS}
    )

    target_compile_definitions(${MOXYGEN_TEST_TARGET} PUBLIC
      ${LIBGMOCK_DEFINES}
    )

    target_link_libraries(${MOXYGEN_TEST_TARGET} PUBLIC
      "${MOXYGEN_TEST_DEPENDS}"
      ${LIBGMOCK_LIBRARIES}
      ${GLOG_LIBRARY}
    )

    target_compile_options(${MOXYGEN_TEST_TARGET} PRIVATE
      "${_MOXYGEN_COMMON_COMPILE_OPTIONS}"
    )

    gtest_add_tests(TARGET ${MOXYGEN_TEST_TARGET}
                    EXTRA_ARGS "${MOXYGEN_TEST_EXTRA_ARGS}"
                    WORKING_DIRECTORY ${MOXYGEN_TEST_WORKING_DIRECTORY}
                    TEST_PREFIX ${MOXYGEN_TEST_PREFIX}
                    TEST_LIST MOXYGEN_TEST_CASES)

    set_tests_properties(${MOXYGEN_TEST_CASES} PROPERTIES TIMEOUT 120)
endfunction()

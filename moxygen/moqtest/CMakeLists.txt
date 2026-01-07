# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# Relay
add_library(moqtest_utils Utils.cpp)
target_include_directories(
  moqtest_utils PUBLIC
  $<BUILD_INTERFACE:${MOXYGEN_FBCODE_ROOT}>
)
target_compile_options(
  moqtest_utils PRIVATE
  ${_MOXYGEN_COMMON_COMPILE_OPTIONS}
)
target_link_libraries(
  moqtest_utils PUBLIC
  moxygen
)

install(
    TARGETS moqtest_utils
    EXPORT moxygen-exports
    ARCHIVE DESTINATION ${LIB_INSTALL_DIR}
    LIBRARY DESTINATION ${LIB_INSTALL_DIR}
)



# moqtest_client
add_executable(
  moqtest_client
  MoQTestClient.cpp
  MoQTestClientMain.cpp
)
set_target_properties(
  moqtest_client
  PROPERTIES
    BUILD_RPATH ${DEPS_LIBRARIES_DIR}
    INSTALL_RPATH ${DEPS_LIBRARIES_DIR}
)
target_include_directories(
  moqtest_client PUBLIC $<BUILD_INTERFACE:${MOXYGEN_FBCODE_ROOT}>
)
target_compile_options(
  moqtest_client PRIVATE
  ${_MOXYGEN_COMMON_COMPILE_OPTIONS}
)
target_link_libraries(
  moqtest_client PUBLIC
  moqtest_utils
  moxygenwtclient
  moqrelaysession
)

install(
    TARGETS moqtest_client
    EXPORT moxygen-exports
    ARCHIVE DESTINATION ${LIB_INSTALL_DIR}
    LIBRARY DESTINATION ${LIB_INSTALL_DIR}
)


# moqtest_server
add_executable(
  moqtest_server
  MoQTestServer.cpp
  MoQTestServerMain.cpp
)
set_target_properties(
  moqtest_server
  PROPERTIES
    BUILD_RPATH ${DEPS_LIBRARIES_DIR}
    INSTALL_RPATH ${DEPS_LIBRARIES_DIR}
)
target_include_directories(
  moqtest_server PUBLIC
  $<BUILD_INTERFACE:${MOXYGEN_FBCODE_ROOT}>
)
target_compile_options(
  moqtest_server PRIVATE
  ${_MOXYGEN_COMMON_COMPILE_OPTIONS}
)
target_link_libraries(
  moqtest_server PUBLIC
  moqtest_utils
  moxygenserver
  moqrelaysession
  moxygenwtclient
)

install(
    TARGETS moqtest_server
    EXPORT moxygen-exports
    ARCHIVE DESTINATION ${LIB_INSTALL_DIR}
    LIBRARY DESTINATION ${LIB_INSTALL_DIR}
)


# moqperf_test_client
add_executable(
  moqperf_test_client
  MoQPerfTestClient.cpp
  MoQPerfTestClientMain.cpp
)
set_target_properties(
  moqperf_test_client
  PROPERTIES
    BUILD_RPATH ${DEPS_LIBRARIES_DIR}
    INSTALL_RPATH ${DEPS_LIBRARIES_DIR}
)
target_include_directories(
  moqperf_test_client PUBLIC $<BUILD_INTERFACE:${MOXYGEN_FBCODE_ROOT}>
)
target_compile_options(
  moqperf_test_client PRIVATE
  ${_MOXYGEN_COMMON_COMPILE_OPTIONS}
)
target_link_libraries(
  moqperf_test_client PUBLIC
  moqtest_utils
  moxygenwtclient
)

install(
    TARGETS moqperf_test_client
    EXPORT moxygen-exports
    ARCHIVE DESTINATION ${LIB_INSTALL_DIR}
    LIBRARY DESTINATION ${LIB_INSTALL_DIR}
)

#add_subdirectory(test)

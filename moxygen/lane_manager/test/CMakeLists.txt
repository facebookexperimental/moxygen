# Copyright (c) Meta Platforms, Inc. and affiliates.
# This source code is licensed under the Apache 2.0 license found in the
# LICENSE file in the root directory of this source tree.

if(NOT BUILD_TESTS)
    return()
endif()

moxygen_add_test(TARGET CatalogReaderTest
  PREFIX "CatalogReader"
  SOURCES
    CatalogReaderTest.cpp
  DEPENDS
    moxygen_lane_manager_catalog_reader
    moqtestutils
    testmain
)

moxygen_add_test(TARGET LaneManagerTest
  PREFIX "LaneManager"
  SOURCES
    LaneManagerTest.cpp
  DEPENDS
    moxygen_lane_manager_moq_lane_manager
    moxygen_lane_manager_moq_lane_router
    moqtestutils
    testmain
)

moxygen_add_test(TARGET SwitchExecutorTest
  PREFIX "SwitchExecutor"
  SOURCES
    SwitchExecutorTest.cpp
  DEPENDS
    moxygen_lane_manager_switch_executor
    testmain
)
